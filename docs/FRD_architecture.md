# FRD — Phase 1: Silver Layer Unification & Hierarchical Extraction

**Status:** Settled design. Supersedes `FRD_architecture.md`.  
**Scope:** Unify Silver EAV storage, relocate the endpoint catalog into `etfportfolio/core`, introduce a three-tier Endpoints → Families → Metrics hierarchy, replace the dual extractor DTO with `Observation`, and adapt pipeline / panel / catalog to `silver.observations`.  
**Out of scope:** Redesigning LOCF caps, winsorization, debt clustering, theme gating, factor series, or frontier math. Ingestion HTTP behavior (gating, freshness, session) is unchanged except import paths.

A later implementer has the repo, this document, and the project overview / structure / code guidelines. They do not have the design conversation. Do not consult the superseded FRD for decisions; it is wrong on several points this document corrects (USD guessing, persisting null AUM codes, skipping enum-unknown ESG nodes, leaving ratios/industries open, dual Silver tables).

---

## 1. Why this change

Silver today is two isomorphic EAV tables:

| Table | Identity | Extra |
|---|---|---|
| `silver.product_metrics` | `(product_id, source, metric_id, effective_date)` | `currency` |
| `silver.product_dimensions` | `(product_id, dimension_type, dimension_name, effective_date)` | `dimension_code` |

`source` ≈ HTTP endpoint name. `dimension_type` ≈ sleeve (asset class, country, …). One holdings snapshot writes both. Prep stages two inserts. Panel and `scripts/catalog_silver.py` `UNION ALL` the two tables.

Defects:

1. **Structural isomorphism.** The tables are the same EAV shape. `dimension_type` ↔ `source`, `dimension_name` ↔ `metric_id`, `dimension_code` ↔ `currency`. Two staging paths, two upserts, two downstream scans.
2. **Blind string coupling.** Extractors and panel share ad-hoc literals (`"total_expense_ratio"`, `"tresgs"`, …). A renamed extractor metric silently matches zero panel rows.
3. **Package boundary.** `prep/pipeline.py` imports `ingest/endpoints.py`. Shared logic belongs in `etfportfolio/core/`.
4. **AUM currency guessing.** `disambiguate_aum_currency()` infers USD/CAD from `$`, exchange, and country, mislabeling non-US funds.

---

## 2. Decision log (normative)

These are the locked choices. If an implementation disagrees, the implementation is wrong.

| # | Decision | Rationale |
|---|---|---|
| D1 | One table `silver.observations`. Drop `product_metrics` and `product_dimensions`. No compatibility views or shims. | Isomorphic EAV; shims freeze the split. |
| D2 | `family` is the **analytical group**, not the endpoint name. | Holdings sleeves share names like `"Other"`. Same-family PK would overwrite. |
| D3 | PK is `(product_id, family, metric, effective_date)`. `code` is not in the PK. | Matches both legacy PKs. |
| D4 | Cut over by rebuilding from bronze: `uv run main.py prep --force`. No SQL UNION of old Silver rows. | Currency rules and DTO change; old rows are not a source of truth. |
| D5 | `schema.sql` must **DROP** the two legacy tables. Do **not** `DELETE` watermarks inside `apply_schema` (it runs on every ingest connection). If `processed_snapshots` is non-empty and `observations` is empty, log a warning to run `prep --force`. | `CREATE TABLE IF NOT EXISTS` alone would leave orphan tables. Auto-wiping watermarks from `apply_schema` is a foot-gun. |
| D6 | Hierarchy lives in **one** module: `etfportfolio/core/endpoints.py`. | Ingest and prep both key off Endpoint. One file is the catalog. |
| D7 | Extractor **bodies** stay in `prep/extractors.py`. The **registry slot** lives in `core/endpoints.py`. `@register_extractor("holdings")`. Core does not import prep. Ingest does not import extractors. | Avoids a core→prep cycle. Ingest stays fetch-only. |
| D8 | `Observation` lives in `prep/utils.py`. It is a Silver row DTO, not HTTP. Delete `MetricRow`, `DimensionRow`, `ExtractionResult`. | Prep-only type. |
| D9 | Closed **StrEnums** for consistent vocabularies. **Raise** if a payload value does not match an enum the family uses. Open strings for vocabularies that grow (countries, currencies, Lipper tags, theme names, debt-type vendor names at extract time). | Silent skip hides vendor drift. Open enums would be brittle for 100+ countries. |
| D10 | `FamilyDefinition.partition_of_unity` marks sleeve families whose metrics must sum to 1.0 per `(product_id, effective_date)`. | Country / industry / asset-class weights are a simplex, not independent scalars. |
| D11 | AUM: `symbol → {currencies}` plus `bronze.contracts.currency` only. No exchange, no country, no USD default, no hardcoded ISO allowlist. Prefix allowlist = `SELECT DISTINCT currency FROM bronze.contracts`. Unresolved → **skip the AUM observation** (do not persist `code=NULL`). | Null AUM would still enter panel; panel does not read currency today, so mixed units would LOCF. Missing row = missing feature. |
| D12 | Panel Phase 1 is a **mechanical** `FROM silver.observations` rewrite. Preserve LOCF-family CASE, UNIONs, caps, winsor, debt clusters, style-box 12-cell expansion, theme gate. Extractors do not encode panel time semantics. | `observations.family` ≠ panel LOCF `family` (see §9). |
| D13 | Catalog queries `silver.observations` natively. No references to the dropped tables. | Same unification. |
| D14 | Where a StrEnum exists, Python literals in panel/catalog/extractors use the enum (`.value` in SQL). | Closes blind coupling for closed vocabularies. |
| D15 | Delete `etfportfolio/ingest/endpoints.py` after the move. | Single source of truth. |

---

## 3. Target architecture

```
core/endpoints.py          ingest/*                  prep/*
┌─────────────────────┐    fetch bronze snapshots    extract silver.observations
│ StrEnum metrics     │         ▲                          ▲
│ FamilyDefinition    │         │ HTTP fields only         │ @register_extractor
│ Endpoint            │─────────┘                          │ Observation
│ EXTRACTORS dict     │────────────────────────────────────┘
│ ENDPOINTS lists     │
└─────────────────────┘
        │
        ▼
silver.observations  →  prep/panel.py (LOCF / flatten)  →  silver.monthly_panel
                     →  scripts/catalog_silver.py
```

Placement (project guidelines):

- Used in one module → that module.
- Shared within a package → `etfportfolio/<pkg>/utils.py`.
- Shared across packages → `etfportfolio/core/*`.

HTTP catalog + metric ontology are shared by ingest and prep → `core/endpoints.py`.  
Parsers used only by extractors (`parse_percentage`, AUM, …) stay in `prep/utils.py`.

---

## 4. Schema (`etfportfolio/core/schema.sql`)

`apply_schema` is idempotent and runs on every DuckDB connection. Order matters because `silver.products` currently depends on the legacy tables.

```sql
CREATE TABLE IF NOT EXISTS silver.observations (
    product_id             INTEGER NOT NULL,
    family                 VARCHAR NOT NULL,
    metric                 VARCHAR NOT NULL,
    code                   VARCHAR,
    effective_date         DATE NOT NULL,
    effective_date_source  VARCHAR NOT NULL,
    fetched_at             TIMESTAMP WITH TIME ZONE NOT NULL,
    value                  DOUBLE NOT NULL,
    raw_value              VARCHAR NOT NULL,
    PRIMARY KEY (product_id, family, metric, effective_date)
);

CREATE OR REPLACE VIEW silver.products AS
SELECT
    c.product_id,
    c.name,
    c.symbol,
    c.local_symbol,
    c.sec_type,
    COALESCE(c.exchange_id, 'SMART') AS exchange_id,
    c.primary_exchange_id,
    c.currency,
    c.trading_class,
    c.valid_exchanges,
    c.stock_type,
    c.isin,
    c.cusip,
    c.time_zone_id,
    c.min_tick,
    c.created_at,
    c.updated_at
FROM bronze.contracts c
WHERE c.product_id IN (SELECT DISTINCT product_id FROM bronze.prices)
  AND c.product_id IN (SELECT DISTINCT product_id FROM silver.observations);

DROP TABLE IF EXISTS silver.product_metrics;
DROP TABLE IF EXISTS silver.product_dimensions;
```

Create `observations` **before** replacing the view; drop the legacy tables **after**. Leave `silver.processed_snapshots` and `silver.monthly_panel` in place (force-rebuild clears them in Python, not in `apply_schema`).

### Column semantics

| Column | Meaning |
|---|---|
| `family` | Analytical group (`profile`, `asset_class`, `country`, …). See §5.2. |
| `metric` | Canonical metric name or sleeve slice (`total_expense_ratio`, `Equity`, `United States`, …). |
| `code` | Optional ISO / slug / currency. **Not** a second identity column. |
| `effective_date_source` | Exactly `payload` \| `item` \| `snapshot`. |
| `fetched_at` | Bronze snapshot `created_at` (timestamptz). |
| `value` | Parsed float. Finite, NOT NULL. |
| `raw_value` | Vendor string, NOT NULL. |

`code` usage:

| Family / metric | `code` |
|---|---|
| `profile` / `total_net_assets_local` | ISO-4217. Row is omitted if unresolved. |
| `country` | ISO-2 (`US`, `DE`) or `NULL` (Unidentified / remap-to-None). |
| `maturity` | Slug (`mat_lt_1y`, …). |
| `style_box`, `style_box_hist` | Cell slug (`large_core`). |
| `theme` | Theme id from payload `key`. |
| `credit_rating` | Same as `metric` (cleaned grade). |
| Everything else | `NULL`. |

---

## 5. `etfportfolio/core/endpoints.py`

Move `ingest/endpoints.py` here and extend it. Delete the ingest module. Update imports in:

- `etfportfolio/ingest/details.py`
- `etfportfolio/ingest/landing.py`
- `etfportfolio/ingest/snapshots.py`
- `etfportfolio/ingest/pipeline.py` (only if it referenced endpoints)
- `etfportfolio/prep/pipeline.py`

Ingest continues to use `Endpoint.name`, `url_prefix`, `slug_template`, `gated`, `resolve()`, `ENDPOINTS_BY_NAME`, `DETAILS_ENDPOINTS`, `GATED_ENDPOINTS`, `UNGATED_ENDPOINTS`. Those derived lists stay as they are today (landing excluded from details).

### 5.1 Canonical metric `StrEnum`s

`StrEnum` members **are** the stored `metric` strings (usable in SQL and dict keys). Values must be unique across **all** enums (panel `feature_id` collisions) except where a family namespace already separates them in Silver; still keep enum **values** globally unique to avoid panel map mistakes.

#### `ProfileMetric`

```
total_expense_ratio
total_net_assets_local
is_passive
manager_tenure_years
audited_net_expense_ratio
management_expense_ratio
non_management_expense_ratio
```

#### `HoldingsMetric`

```
portfolio_top_10_concentration
```

(Sleeve slices of the holdings **payload** are other families, not this enum.)

#### `EsgMetric`

Exact stored slugs (today’s `sanitize_metric_id` output):

```
esg_coverage
tresgs
tresgcs
tresgccs
tresgens
tresgenrrs
tresgeners
tresgenpis
tresgsos
tresgsowos
tresgsohrs
tresgsocos
tresgsoprs
tresgcgs
tresgcgbds
tresgcgsrs
tresgcgvss
```

Confirm against `DISTINCT metric_id FROM silver.product_metrics WHERE source = 'esg'` (or current tests/fixtures) before freezing. If silver contains extra slugs, **add them to the enum** — do not drop data. After freeze, unknown sanitized names **raise**.

#### `MstarMetric`

```
mstar_medalist_rating
mstar_analyst_coverage_pct
mstar_morningstar_rating
mstar_sustainability_rating
mstar_people_analyst
mstar_people_quant
mstar_process_analyst
mstar_process_quant
mstar_parent_analyst
mstar_parent_quant
```

#### `ThemeMetric`

```
theme_coverage
```

Theme **children** (`family='theme'`) are open: `metric` = vendor name, `code` = theme id.

#### `RatiosMetric`

Closed over **every** ratio `name_tag` currently extracted, not only the /100 set.

Build the member list from:

1. Existing `_RATIOS_PERCENTAGE_METRICS` in `prep/extractors.py`.
2. `DISTINCT metric_id FROM silver.product_metrics WHERE source = 'ratios'` (and extractor tests/fixtures).
3. Panel `CROSS_SECTION_WINSOR_FEATURES` leverage ids if they are ratios (`total_assets_total_equity`, `total_debt_total_capital`, `total_debt_total_equity`, `lt_debt_shareholders_equity`, `ebit_to_interest`, `sales_to_total_assets`).

**Percentage / unit-interval subset** (divide payload value by 100). Store as a class-level collection on the enum, e.g. `RatiosMetric.PERCENTAGE` — not a second parallel frozenset of raw strings:

```
eps_growth_1yr
eps_growth_3yr
eps_growth_5yr
sales_growth_1_year
sales_growth_3_year
sales_growth_5_yr
sales_per_share_growth_1_year
sales_per_share_growth_3_year
operating_cash_flow_growth_rate_3yr
return_on_assets_1yr
return_on_assets_3yr
return_on_equity_1yr
return_on_equity_3yr
return_on_investment_1yr
return_on_investment_3yr
return_on_capital
return_on_capital_3yr
dividend_yield_weighted_average
dividendpayoutratio5yr
dividend_per_share_1yr
dividend_per_share_3yr
yield_to_maturity
average_coupon
relative_strength
```

Lookup: `metric = RatiosMetric(sanitize_metric_id(tag))` — `ValueError` on unknown (that is the required raise). If `metric in RatiosMetric.PERCENTAGE`, `value /= 100.0`.

#### `AssetClassMetric`

Stored `metric` strings (panel maps these to `asset_class_*` feature ids):

```
Equity
Fixed Income
Cash
Other
```

#### `IndustryMetric`

Stored `metric` strings = holdings industry **display names after remap**. Build from:

```sql
SELECT DISTINCT dimension_name
FROM silver.product_dimensions
WHERE dimension_type = 'industry';
```

plus `_INDUSTRY_NAME_REMAPS` targets and `INDUSTRY_FEATURE_OVERRIDES` keys in `panel.py`:

- `Telecommunication Services-Discontinued eff 09/19/2020` remaps to `Communication Services` **before** enum lookup.
- `Not Classified - Non Equity`
- `Non Classified Equity`

Unknown industry name after remap → **raise**.

#### `CreditRatingMetric`

Cleaned grade names (after `clean_credit_rating`). Seed from catalog’s documented set and `DISTINCT` silver `credit_rating` names:

```
AAA, AA, A, BBB, BB, B, CCC, CC, C, D, Not Rated, Not Available
```

Extend from live distinct values, then freeze. Unknown → raise.

#### `MaturityMetric`

**Codes** (stored in `code`; `metric` remains the vendor display string `"% Maturity Less than 1 Year"`, etc.):

```
mat_lt_1y
mat_1_to_3y
mat_3_to_5y
mat_5_to_10y
mat_10_to_20y
mat_20_to_30y
mat_gt_30y
mat_other
```

`_MATURITY_CODE_MAP` stays the display→code map. Unknown display name → raise (do not emit a maturity row with `code=NULL`).

### 5.2 Open vocabularies (not enums — do not raise)

| Family | `metric` | `code` | Notes |
|---|---|---|---|
| `country` | Vendor country name | ISO-2 or `NULL` | New regions appear (Isle of Man, …). Keep `_COUNTRY_CODE_REMAPS`. |
| `theme` | Vendor theme name | Theme id | Taxonomy evolves. |
| `lipper` | `lipper_{sanitize_metric_id(tag)}_{horizon}` | `NULL` | Horizons: `overall`, `3yr`, `5yr`, `10yr`. Skip items without tag/value as today. |
| `debt_type` | Vendor debt name | payload `code` if present | Closed **panel** catalog `DEBT_CLUSTERS` still raises at flatten if unmapped. Not a core enum (110 names, already owned by panel). |
| AUM currency | (not a family) | ISO on `total_net_assets_local` | Open; unresolved skips the row. |

`style_box` / `style_box_hist`: keep current axis-tag validation (`value|core|growth` × `large|multi|mid|small`) and **raise** on unknown tags / OOB coords. `metric` = `"Large Core"` (title-cased), `code` = `"large_core"`. Not a separate StrEnum unless it simplifies tests; the axis sets are already closed.

### 5.3 `FamilyDefinition`

```python
@dataclass(frozen=True, slots=True)
class FamilyDefinition:
    name: str
    description: str
    metrics: type[StrEnum] | None          # None = open vocabulary
    partition_of_unity: bool = False       # True: weights sum to 1.0 per (product_id, effective_date)
```

`FAMILIES: dict[str, FamilyDefinition]` — key == `name`.

| name | metrics enum | partition_of_unity | Description |
|---|---|---|---|
| `profile` | `ProfileMetric` | False | Fees, tenure, net assets, passive flag |
| `holdings` | `HoldingsMetric` | False | Portfolio concentration scalars |
| `asset_class` | `AssetClassMetric` | **True** | Sleeve weights |
| `country` | `None` | **True** | Geographic weights |
| `industry` | `IndustryMetric` | **True** | Sector weights |
| `credit_rating` | `CreditRatingMetric` | **True** | FI credit weights |
| `debt_type` | `None` | **True** | FI structure weights (panel clusters later) |
| `maturity` | `MaturityMetric` (codes) | **True** | FI maturity weights |
| `style_box` | `None` | False at Silver (sparse 1.0 cells; panel expands to 12-cell simplex) | Current style coordinates |
| `style_box_hist` | `None` | False at Silver | Historical style coordinates |
| `ratios` | `RatiosMetric` | False | Fundamentals / valuation |
| `esg` | `EsgMetric` | False | LSEG pillar scores |
| `mstar` | `MstarMetric` | False | Medalist / pillars / stars |
| `lipper` | `None` | False | Lipper Leader ratings |
| `theme` | `None` | False | Thematic weights (not a 100% simplex; coverage gated in panel) |
| `theme_weights` | `ThemeMetric` | False | `theme_coverage` scalar |

Phase 1 **enforcement** of `partition_of_unity`:

- Panel keeps the existing `asset_class` sum-to-1 assert (`PARTITION_TOLERANCE = 0.0001`).
- Catalog keeps sleeve sum profiling.
- Do **not** add new hard fails for country/industry/credit/debt/maturity sums in this phase (data may not be a perfect simplex). The flag is metadata for tests and later gates.

### 5.4 `Endpoint`

```python
@dataclass(frozen=True, slots=True)
class Endpoint:
    name: str
    url_prefix: str
    slug_template: str
    gated: bool
    families: tuple[FamilyDefinition, ...]

    @property
    def url_template(self) -> str:
        return f"{self.url_prefix}{self.slug_template}"

    def resolve(self, **kwargs: Any) -> tuple[str, str, str]:
        slug = self.slug_template.format(**kwargs)
        return self.url_prefix, slug, f"{self.url_prefix}{slug}"
```

No extractor callable on the frozen dataclass.

HTTP fields are **unchanged**:

| name | url_prefix | slug_template | gated | families |
|---|---|---|---|---|
| `landing` | `/tws.proxy/fundamentals/landing/` | `{product_id}?widgets=objective,keyProfile,lipper_ratings,holdings,mf_key_ratios,mstar&lang=en` | False | `()` |
| `holdings` | `/tws.proxy/fundamentals/mf_holdings/` | `{product_id}?lang=en` | True | holdings, asset_class, country, industry, credit_rating, debt_type, maturity |
| `profile` | `/tws.proxy/fundamentals/mf_profile_and_fees/` | `{product_id}?lang=en` | True | profile, style_box, style_box_hist |
| `ratios` | `/tws.proxy/fundamentals/mf_ratios_fundamentals/` | `{product_id}?lang=en` | True | ratios |
| `esg` | `/tws.proxy/impact/esg/` | `{product_id}?accounts={account_id}&lang=en` | False | esg |
| `mstar` | `/tws.proxy/mstar/fund/detail?conid=` | `{product_id}&lang=en` | True | mstar |
| `lipper` | `/tws.proxy/fundamentals/mf_lip_ratings/` | `{product_id}?lang=en` | True | lipper |
| `theme_weights` | `/tws.proxy/knowledge-graph/ui/fund?conid=` | `{product_id}&max=999999999&lang=en` | False | theme_weights, theme |

Also export:

```python
ENDPOINTS_BY_NAME: dict[str, Endpoint]
ENDPOINTS_BY_PREFIX: dict[str, Endpoint]
DETAILS_ENDPOINTS  # all except landing
GATED_ENDPOINTS
UNGATED_ENDPOINTS
```

### 5.5 Extractor registry (DI)

```python
EXTRACTORS: dict[str, Callable] = {}

def register_extractor(endpoint_name: str):
    """Decorator used in prep/extractors.py. Does not import prep."""
    def deco(fn):
        if endpoint_name not in ENDPOINTS_BY_NAME:
            raise KeyError(f"Unknown endpoint {endpoint_name!r}")
        if endpoint_name in EXTRACTORS:
            raise ValueError(f"Duplicate extractor for {endpoint_name!r}")
        EXTRACTORS[endpoint_name] = fn
        return fn
    return deco

def extractor_for(endpoint: Endpoint) -> Callable:
    try:
        return EXTRACTORS[endpoint.name]
    except KeyError as e:
        raise KeyError(f"No extractor registered for {endpoint.name!r}") from e
```

Landing is not registered. Every `DETAILS_ENDPOINTS` name **must** be registered once `prep.extractors` is imported.

Uniform extractor signature:

```python
def extract_*(
    product_id: int,
    payload: dict[str, Any],
    fetched_at: datetime,
    contract_currency: str | None = None,
    known_currencies: set[str] | frozenset[str] | tuple[str, ...] = (),
) -> list[Observation]:
```

Only `extract_profile` uses the last two arguments. Others accept them (defaults) and ignore them.

---

## 6. `Observation` and AUM (`prep/utils.py`)

```python
@dataclass(frozen=True, slots=True)
class Observation:
    product_id: int
    family: str
    metric: str
    code: str | None
    effective_date: date
    effective_date_source: str
    fetched_at: datetime
    value: float
    raw_value: str

    def to_row(self) -> tuple[int, str, str, str | None, date, str, datetime, float, str]:
        return (
            self.product_id,
            self.family,
            self.metric,
            self.code,
            self.effective_date,
            self.effective_date_source,
            self.fetched_at,
            self.value,
            self.raw_value,
        )
```

Delete `MetricRow`, `MetricTuple`, `DimensionRow`, `DimensionTuple`, `ExtractionResult`.

Keep `decompress_payload`, `parse_effective_date`, `parse_net_assets`, `parse_manager_tenure`, `parse_percentage`, `clean_credit_rating`, `sanitize_metric_id` as they are (except AUM currency).

### 6.1 `disambiguate_aum_currency`

Replace the current implementation. Drop `_AUM_ISO_PREFIXES`, `_CAD_EXCHANGES`, `_AUM_SYMBOL_TO_ISO`, and the `listing_exchange` / `country` parameters.

```python
SYMBOL_TO_CURRENCIES: dict[str, set[str]] = {
    "$": {"USD", "CAD", "AUD", "MXN", "SGD", "HKD", "NZD", "TWD"},
    "¥": {"JPY", "CNY", "CNH"},
    "£": {"GBP", "EGP", "LBP"},
    "₩": {"KRW", "KPW"},
    "€": {"EUR"},
    "₹": {"INR"},
}

def disambiguate_aum_currency(
    raw_value: str,
    product_currency: str | None,
    known_currencies: set[str] | frozenset[str] | tuple[str, ...],
) -> str | None:
    ...
```

`known_currencies` is the run-scoped set of `bronze.contracts.currency` (non-null, stripped, uppercased). **Not** a module-level allowlist.

Algorithm (one glyph rule, no unambiguous override):

1. Strip `raw_value`. Empty → `None`.
2. If it starts with a 3-letter alphabetic token (word boundary): uppercased token ∈ `known_currencies` → that token. (May differ from this product’s contract.)
3. If the first character is a key of `SYMBOL_TO_CURRENCIES`: let `candidates` be that set. If `product_currency` (stripped, upper) ∈ `candidates` → `product_currency`. Else → `None`.
4. Otherwise (bare number): if `product_currency` is a 3-letter alpha code ∈ `known_currencies` → `product_currency`. Else → `None`.

Do not guess USD. Do not inspect exchange or country.

When this returns `None`, **`extract_profile` omits the AUM observation**. Other profile metrics are still emitted.

---

## 7. Extractors (`prep/extractors.py`)

Every public extractor returns `list[Observation]` and is decorated with `@register_extractor("<endpoint.name>")`. Delete `EXTRACTOR_REGISTRY`.

Empty payload → `[]` (caller still watermarks the snapshot).

Use enum members for `metric=` (and `family=` via `FAMILIES[...].name` or the literal that matches `FamilyDefinition.name`).

Raise on unknown **enum** lookups. Keep existing raises: missing mstar `id`, unknown management approach, unknown style-box axes/coords, unknown mstar rating string / pillar.

### 7.1 `extract_profile`

- Expenses: `management_expense_ratio`, `non_management_expense_ratio` (ratios as provided; already fractions in payload `ratio`).
- Fund profile: `total_expense_ratio` via `parse_percentage`; `is_passive` 1.0/0.0; `total_net_assets_local` via `parse_net_assets` then `disambiguate_aum_currency` — **skip if currency is None**; `manager_tenure_years`.
- Annual report: `audited_net_expense_ratio`.
- Nested `mstar` style box → `family=style_box|style_box_hist`, `metric=dim_name`, `code=dim_code`, `value=1.0`.
- Product currency from **`bronze.contracts.currency` only** (not `bronze.products`).

### 7.2 `extract_holdings`

- Scalar: `family=holdings`, `HoldingsMetric.PORTFOLIO_TOP_10_CONCENTRATION` via `parse_percentage`.
- Breakdowns (`_HOLDINGS_BREAKDOWNS`) become **separate families** (`asset_class`, `country`, `industry`, `credit_rating`, `debt_type`, `maturity`), `metric=dim_name`, `code` as today (country ISO, maturity slug, credit cleaned name, debt payload code, else None).
- Weights: `float(weight)/100.0` as today.
- Asset class / industry / credit / maturity: enum-validate (raise).
- Country: open; apply `_COUNTRY_CODE_REMAPS`.
- Debt type: open at extract; panel still raises if unmapped.
- Keep intra-extractor coalesce on `(product_id, family, metric, effective_date)` (last wins), same as today’s dimension coalesce.

### 7.3 `extract_esg`

- `coverage` → `EsgMetric.ESG_COVERAGE`.
- Nodes and children: `sanitize_metric_id(name)` then `EsgMetric(...)` — **raise if not a member**. Do not passthrough unknown pillars.

### 7.4 `extract_mstar`

Behavior unchanged, but stored ids come from `MstarMetric`. Still raise on unrecognized pillar id / rating string. Still emit `mstar_analyst_coverage_pct` when foundational pillars were seen. Skip `category` / `category_index`. Skip empty / under-review tokens as today.

### 7.5 `extract_ratios`

For each `name_tag` in sections `dividend`, `financials`, `fixed_income`, `ratios`, `zscore`: `RatiosMetric(sanitize_metric_id(tag))` (raises if unknown). Scale if member of `PERCENTAGE`. `family=ratios`, `code=None`.

### 7.6 `extract_lipper`

Unchanged selection of universe / horizons. `family=lipper`, `metric` still `lipper_{tag}_{horizon}`. Not an enum.

### 7.7 `extract_theme_weights`

- `ThemeMetric.THEME_COVERAGE` in family `theme_weights`.
- Each theme: family `theme`, `metric=name`, `code=theme_id`, value = `rank_adjusted_weight` as today.

---

## 8. Pipeline (`prep/pipeline.py`)

```python
from etfportfolio.core.endpoints import (
    DETAILS_ENDPOINTS,
    ENDPOINTS_BY_PREFIX,
    extractor_for,
)
from etfportfolio.prep import extractors as _extractors  # fills EXTRACTORS
from etfportfolio.prep.utils import Observation, decompress_payload
```

The `_extractors` import is required for registration side effects.

### 8.1 Force reset

```sql
BEGIN TRANSACTION;
DELETE FROM silver.monthly_panel;
DELETE FROM silver.processed_snapshots;
DELETE FROM silver.observations;
COMMIT;
```

### 8.2 Pending snapshots

Same query as today (`bronze.snapshots` anti-join `silver.processed_snapshots`), `BATCH_SIZE = 100`.

If zero pending: `"All snapshots up to date."` → return 0.

If pending is 0 but that is because everything is watermarked **and** `observations` is empty: log a warning that a schema cutover needs `prep --force`.

### 8.3 Per batch

1. Load blobs; decompress (failure still raises).
2. Empty payload or landing prefix → watermark, continue.
3. `ep = ENDPOINTS_BY_PREFIX[url_prefix]`. Unknown prefix → raise `ValueError` (same as today’s unregistered extractor).
4. Load `known_currencies = {row[0] for row in conn.execute("SELECT DISTINCT currency FROM bronze.contracts WHERE currency IS NOT NULL")}` once per run (not per batch is fine).
5. Load `contract_currency` per `product_id` from `bronze.contracts` only. Drop `_load_product_meta`’s products/exchange/country merge.
6. `rows = extractor_for(ep)(product_id, data, created_at, contract_currency=..., known_currencies=...)`.
7. Stage in `dict[tuple[int,str,str,date], Observation]` keyed `(product_id, family, metric, effective_date)`. On collision keep the observation with greater `fetched_at`.
8. Single upsert:

```sql
INSERT INTO silver.observations (
    product_id, family, metric, code, effective_date, effective_date_source,
    fetched_at, value, raw_value
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (product_id, family, metric, effective_date) DO UPDATE SET
    code = EXCLUDED.code,
    value = EXCLUDED.value,
    raw_value = EXCLUDED.raw_value,
    effective_date_source = EXCLUDED.effective_date_source,
    fetched_at = EXCLUDED.fetched_at;
```

Use `Observation.to_row()`. Watermark snapshot ids in the same transaction as today.

Do **not** runtime-assert `Observation.family in ep.families` on every snapshot. Tests cover the static mapping (§11).

---

## 9. Panel (`prep/panel.py`) — mechanical rewrite only

`run_panel()` always full-rebuilds `silver.monthly_panel`. Extractors must not learn LOCF caps.

**Critical:** panel’s internal column `family` is a **LOCF group**, not `silver.observations.family`.

Current metrics CASE (preserve this logic):

- `source='esg'` → LOCF family `esg`
- `ratios` / `lipper` → those names
- `holdings` → `holdings_scalar`
- `theme_weights` → `theme` (merged with theme **children** so coverage and weights share a cap)
- else (`profile` / mstar metrics) → **the metric id itself** (`is_passive` perpetual, TER 540d, AUM 180d, tenure 365d)

If you naively `SELECT family FROM silver.observations` as the LOCF family, profile caps collapse and theme gating breaks.

### 9.1 Table/column map

| Old | New |
|---|---|
| `silver.product_metrics` | `silver.observations` restricted to scalar families: `profile`, `holdings`, `esg`, `mstar`, `lipper`, `ratios`, `theme_weights` |
| `m.source` | `o.family` |
| `m.metric_id` | `o.metric` |
| `m.currency` | `o.code` (panel still does not need it for flatten) |
| `silver.product_dimensions` | `silver.observations` restricted by `family` ∈ {`asset_class`, `country`, `industry`, `credit_rating`, `debt_type`, `maturity`, `theme`, `style_box`, `style_box_hist`} |
| `d.dimension_type` | `o.family` |
| `d.dimension_name` | `o.metric` |
| `d.dimension_code` | `o.code` |

Keep the same UNION ALL projections (asset_class join `asset_class_map`, country `country_{code}`, industry slug, credit slug, maturity `code` as feature_id, debt `GROUP BY` cluster, theme children + parent rollups, style-box 12-cell expansion). Same empty-input behavior (`DELETE monthly_panel` if no observations). Same `_assert_closed_sets` (debt map, asset class) — point them at `observations`. Same spine bounds `MIN/MAX(effective_date)`.

Python maps (`ESG_FEATURE_MAP` keys, winsor ids, style feature ids, profile metric ids in the cap CASE) should use enum `.value` where an enum exists.

Do not redesign `_apply_locf`, `_synthesize`, `_apply_theme_gate`, `_winsorize`, `_attach_dominant_asset_class`.

---

## 10. Catalog (`scripts/catalog_silver.py`)

Replace every `silver.product_metrics` / `silver.product_dimensions` reference.

- PK uniqueness: `(product_id, family, metric, effective_date)`.
- Cross-source collision check: `COUNT(DISTINCT family)` on `(product_id, metric, effective_date)` only if you still want it; families are supposed to namespace metrics. Prefer “no duplicate PK” + watchlists.
- Null/non-finite `value`/`raw_value`; `effective_date <= fetched_at::DATE`; `effective_date_source ∈ {payload,item,snapshot}`; product_id ∈ `bronze.contracts`.
- Watchlists: `family='asset_class'` metrics vs `AssetClassMetric`; AUM `code` / raw prefix vs known currencies; `family='credit_rating'` vs enum; `family='maturity'` codes vs `MaturityMetric`; style-box codes vs the 12-cell grid.
- Pending/migration checks that referred to `product_metrics.currency` or dual tables: rewrite to `observations` / drop as landed.
- SQLite export: replace `metrics` + `dimensions` tables with one observations-profile table (family, metric, n_obs, …) plus `dimensions_summary`-style grouping by family. Keep cadence / dwell / LOCF / findings tables.

---

## 11. Tests

Guideline: every `etfportfolio/<pkg>/<mod>.py` has `tests/<pkg>/test_<mod>.py`. Reuse `tests/conftest.py`. Move `tests/ingest/test_endpoints.py` → `tests/core/test_endpoints.py` if it exists. Update imports; delete obsolete tests (no shims).

### `tests/core/test_endpoints.py`

- Every endpoint `resolve(product_id=1, account_id="U123")` produces the expected prefix/slug/url (same templates as today).
- `DETAILS` / `GATED` / `UNGATED` membership.
- Landing `families == ()`.
- Holdings/profile/theme_weights `families` match §5.4.
- All StrEnum values non-empty; no duplicates within each enum.
- After `import etfportfolio.prep.extractors`: every `DETAILS_ENDPOINTS` name is in `EXTRACTORS`; landing is not.
- `register_extractor("nope")` raises.

### `tests/prep/test_utils.py`

AUM (use an explicit `known={"USD","CAD","AUD","EUR","GBP","JPY","INR","HKD",...}` in tests):

| raw | product_currency | expected |
|---|---|---|
| `CAD 50M` | anything | `CAD` if CAD in known |
| `AUD 1.2B` | anything | `AUD` |
| `HKD 800M` | anything | `HKD` |
| `$ 1.2B` | `CAD` | `CAD` |
| `$ 1.2B` | `USD` | `USD` |
| `$ 1.2B` | `EUR` | `None` |
| `$ 1.2B` | `None` | `None` |
| `¥ 500M` | `JPY` | `JPY` |
| `¥ 500M` | `CNY` | `CNY` |
| `£ 100M` | `GBP` | `GBP` |
| `€ 100M` | `EUR` | `EUR` |
| `€ 100M` | `USD` | `None` (glyph set is `{EUR}`, contract not in set) |
| `₹ 500M` | `INR` | `INR` |
| `50000000` | `EUR` | `EUR` if EUR in known |
| `50000000` | `None` | `None` |

No exchange/country parameters.

### `tests/prep/test_extractors.py`

- Each extractor returns `list[Observation]` with valid fields.
- Profile AUM: `code` set when resolvable; **observation absent** when not.
- Profile style box: `family` in `{style_box, style_box_hist}`, `code` like `large_core`.
- Holdings countries: `code='US'` for United States; industries enum-validated; maturity `code='mat_lt_1y'`.
- ESG unknown node name → `ValueError`.
- Ratios unknown `name_tag` → `ValueError`; percentage members scaled by 1/100.
- Mstar unknown pillar still raises.
- Unknown industry name raises; remapped discontinued telecom name is accepted as Communication Services.

### `tests/prep/test_pipeline.py`

- End-to-end snapshot → `silver.observations`.
- Intra-batch collision on `(product_id, family, metric, effective_date)` keeps greater `fetched_at`.
- `force=True` clears `observations`, `processed_snapshots`, `monthly_panel`.
- Landing / empty payload watermarked without rows.

### `tests/prep/test_panel.py`

- `run_panel()` builds `silver.monthly_panel` from `silver.observations`.
- Existing LOCF / synth / partition-of-unity behaviours still hold (update fixtures to write `observations` instead of the two old tables).

---

## 12. Cutover (operator)

```bash
uv run main.py prep --force
uv run scripts/catalog_silver.py
```

`--force` is required once after pull: dropping the legacy tables leaves watermarks pointing at snapshots whose Silver rows no longer exist. Incremental `prep` would process nothing.

Invariants: catalog scorecard 0 FAIL on structural checks.

---

## 13. Implementation sequence

1. `schema.sql`: create `observations`, replace `silver.products`, drop legacy tables.
2. Write `core/endpoints.py` (enums, families, Endpoint, registry, ENDPOINTS). Delete `ingest/endpoints.py`. Fix ingest imports.
3. `prep/utils.py`: `Observation`; replace AUM helper; delete dual DTOs.
4. `prep/extractors.py`: `@register_extractor`, `list[Observation]`, enum lookups, AUM skip.
5. `prep/pipeline.py`: prefix lookup, one upsert, contracts-only currency, `known_currencies`.
6. `prep/panel.py`: mechanical FROM/column map; enum literals in Python maps.
7. `scripts/catalog_silver.py`: native `observations`.
8. Tests as §11.
9. Operator: `prep --force`, then catalog.

Do not add compatibility shims, dual-write, or views named `product_metrics`.

---

## 14. Explicit non-goals

- Changing ingest gating, freshness hours, landing preview hashing, or session login.
- Putting LOCF caps, winsor bounds, or debt clusters on `FamilyDefinition` / extractors.
- Persisting unresolved AUM with `code=NULL`.
- Enumerating countries, currencies, Lipper tags, or theme names.
- Per-endpoint subclasses (`HoldingsEndpoint`, …).
- Attaching callables onto frozen `Endpoint` instances.
- Auto-resetting watermarks inside `apply_schema`.
- Collapsing panel’s UNION flatten into one `CASE family` mega-query.

---

## 15. File checklist

| Action | Path |
|---|---|
| Create | `etfportfolio/core/endpoints.py` |
| Delete | `etfportfolio/ingest/endpoints.py` |
| Edit | `etfportfolio/core/schema.sql` |
| Edit | `etfportfolio/prep/utils.py` |
| Edit | `etfportfolio/prep/extractors.py` |
| Edit | `etfportfolio/prep/pipeline.py` |
| Edit | `etfportfolio/prep/panel.py` (not `panels.py`) |
| Edit | `etfportfolio/ingest/details.py`, `landing.py`, `snapshots.py` |
| Edit | `scripts/catalog_silver.py` |
| Create/move | `tests/core/test_endpoints.py` |
| Edit | `tests/prep/test_utils.py`, `test_extractors.py`, `test_pipeline.py`, `test_panel.py` |

CLI (`main.py prep`) is unchanged: observations then panel.