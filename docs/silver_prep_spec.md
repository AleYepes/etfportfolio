# Silver Prep Specification

**Status:** Settled implementation spec (observations + monthly panel).  
**Audience:** An implementer with the current `etfportfolio` tree and no prior design conversation.  
**Supersedes:** Dual EAV tables (`silver.product_metrics` / `silver.product_dimensions`), hardcoded LOCF day-caps, analyst/quant mstar splits, debt-type clustering, partition-of-unity asserts, panel winsorization, theme coverage gates, `is_leveraged`, `management_fee_rate`, mstar `pillar_*` synthesis, `monthly_panel.asset_class`, earliest-`fetched_at` collision ties, CAD/exchange AUM guessing, and any “mechanical retarget of the old panel.”

This document is the **only** design authority for this work. If current code conflicts with it, change the code.

---

## 0. Goal and phase split

Prep produces factor-ready fundamentals. Cleaning is split by *kind of work*:

| Phase | Module | Responsibility |
|---|---|---|
| **1. Observations** | `prep/extractors.py`, `prep/utils.py`, `prep/pipeline.py` | Per-snapshot JSON → clean point-in-time rows in `silver.observations`. Micro-clean only: parse, snake-case, units, closed-vocab checks, simplex clip/normalize/drop-residuals, strict AUM currency, lexical dates. |
| **2. Monthly panel** | `prep/panel.py` | Align observations onto a per-product month-end **trading** spine. Series-macro only: last-value interpolation with per-series p99 caps, snapshot replacement **with stored zeros** for composition/dummy families, in-month AUM→USD. |
| **Later (out of scope)** | — | Pivot to wide, completeness/partition by asset class, parent-theme collapse, winsor/outlier research, factor returns, regressions, frontiers. |

Extractors must not do time-series work (no LOCF, no month-end spine, no FX averaging, no densifying across dates). The panel must not re-parse vendor JSON.

`main.py prep [--force]` runs phase 1 then phase 2, as today.

---

## 1. Design principles (non-negotiable)

1. **No shims.** Drop obsolete tables, columns, metric aliases, and extractor DTOs. No compatibility views for `product_metrics` / `product_dimensions`.
2. **Two interpolation grains, one math.** p99-of-positive-gaps, with “no positive gaps ⇒ uncapped over the trading spine,” is the only cap rule. **Grain** differs by family class (§6): scalars interpolate per `(product, metric)`; default-0 families interpolate per `(product, family)` snapshot so omitted buckets can be stored as 0 without leaking the previous snapshot’s values.
3. **Default-0 families must not leak omitted buckets.** Jan `cash=0.2` must not survive a Feb snapshot that omitted Cash. The tall panel **stores** the zeros; a future pivot must not be required to get this right.
4. **No lookahead for multi-observation series.** Do not paint months *before* the first observation of a series that has two or more points. The **singleton** exception (§6.3 / §6.4) is deliberate: one snapshot behaves like the old MVP (use it across the whole trading history).
5. **Do not invent a 0/0/0 portfolio.** Stored zeros exist only on months when a default-0 family snapshot is **live**. Stale/absent families emit **no rows**, not a full-zero sleeve.
6. **Simple over clever.** Prefer one SQL pattern that is correct to a second special case that is slightly tighter. No cap-fallback table, no backfill for multi-obs series, no per-metric exceptions inside default-0 families.
7. **Package boundary.** Ingest never imports prep. Prep never imports ingest. Shared catalog lives in `etfportfolio/core/`. Core must not hold extractor callables.

---

## 2. Decision log

| ID | Decision |
|---|---|
| D1 | One table `silver.observations`. Drop `silver.product_metrics` and `silver.product_dimensions`. |
| D2 | PK `(product_id, family, metric, effective_date)`. `code` is never part of the PK. |
| D3 | `metric` is `lower_snake_case` **without** a family prefix. Panel `feature_id = f"{family}_{metric}"`. |
| D4 | `code` is contextual only: ISO-4217 for AUM, ISO-2 for country, `theme_id` for themes. `NULL` otherwise. Never use `code` as a panel identity. |
| D5 | **14 observation families** (§4). `debt_type` is dropped. `style_box` and `style_box_hist` are both stored; the panel merges them into 13 panel families. |
| D6 | Simplex order: validate vocab (incl. residuals) → clip negatives to 0 → normalize to sum 1.0 → **drop residuals**. Do not re-normalize survivors. Sum of remaining buckets may be `< 1`. If post-clip sum `≤ 0`, emit nothing for that sleeve. |
| D7 | Collision on the same PK: **greater `date_source_depth` wins**; if equal, **later `fetched_at` wins** (restatements beat first-seen). Same rule in-memory and in the SQL upsert. |
| D8 | Unknown closed-vocab keys, unknown ratio tags, unknown ESG pillars, unknown mstar ids/ratings, unknown style axes: **raise**. Do not watermark that snapshot; continue the batch; rerun retries it. |
| D9 | Open vocabs: `country`, `theme`, `rank_adj_theme`. Closed vocabs raise on unknown names. |
| D10 | AUM currency is a strict match against `bronze.contracts.currency` via the glyph dictionary. Unresolved → omit the AUM row. Never persist `code=NULL`. Never guess USD/CAD from exchange or country. |
| D11 | `profile` absorbs `top_10_weight`, `theme_coverage`, `esg_coverage`, `mstar_coverage`. |
| D12 | Dual theme extract: `family='theme'` (payload `weight` as given) and `family='rank_adj_theme'` (payload `rank_adjusted_weight` as given). **Do not divide by 100.** Coverage is a profile scalar. No 0.7 gate. No parent rollup in this work. |
| D13 | Mstar analyst and quant are the **same** metrics (`people`, `process`, `parent`, …). Ignore `q` / `q_` for naming. Do not emit `category` / `category_index`. |
| D14 | Endpoint catalog + metric maps live in `core/endpoints.py`. Extractor **functions** and `EXTRACTOR_REGISTRY` live in prep. |
| D15 | Panel table is four columns: `(product_id, as_of_date, feature_id, value)`. **No `asset_class` column.** Dominant-class partitioning is a later phase. |
| D16 | No panel winsor, no `is_leveraged`, no `management_fee_rate`, no `pillar_*`, no debt clusters, no partition-of-unity, no cap-fallback table, no first-obs backfill for multi-obs series. |
| D17 | Default-0 families **store zeros** in the tall panel via snapshot densify (§6.4). Do **not** rely on a later `fillna(0)`. Do **not** two-step INSERT-zeros-then-UPDATE; implement as one overlay (`LEFT JOIN` + `COALESCE`). |
| D18 | Local AUM is converted to USD **before** in-month averaging, using that **observation month’s** average FX, then interpolated as a USD stock. Panel emits `profile_total_net_assets_usd` only. Do not re-FX after LOCF. |
| D19 | Caps are computed **per product-series**, not globally per family name, and **not** from a static fallback. `n_gaps = 0` ⇒ uncapped. There is no `< 10 transitions` branch. |
| D20 | Spine bounds are **per product** from `bronze.prices` (`first_price` / `last_price`), never the global min/max of the prices table. |
| D21 | Isolated extract failures do not fail the run or the rest of the batch. Unregistered extractors and schema errors still raise. |

---

## 3. Schema

`etfportfolio/core/schema.sql` is applied idempotently on every connection (`apply_schema` in `etfportfolio/core/db.py`). Create `silver.observations` **before** replacing views; drop legacy tables **after**.

Do **not** `CREATE` `silver.product_metrics` or `silver.product_dimensions` anymore.

```sql
CREATE TABLE IF NOT EXISTS silver.observations (
    product_id             INTEGER NOT NULL,
    family                 VARCHAR NOT NULL,
    metric                 VARCHAR NOT NULL,
    code                   VARCHAR,
    effective_date         DATE NOT NULL,
    date_source_depth      INTEGER NOT NULL,
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

### `silver.monthly_panel` migration

The existing PK is already `(product_id, as_of_date, feature_id)`. `asset_class` is a leftover **column**, not part of the PK. Do **not** `DROP TABLE` on every connection (that would wipe the panel during ingest). Migrate idempotently:

```sql
CREATE TABLE IF NOT EXISTS silver.monthly_panel (
    product_id   INTEGER NOT NULL,
    as_of_date   DATE NOT NULL,
    feature_id   VARCHAR NOT NULL,
    value        DOUBLE NOT NULL,
    PRIMARY KEY (product_id, as_of_date, feature_id)
);

ALTER TABLE silver.monthly_panel DROP COLUMN IF EXISTS asset_class;
```

If DuckDB on the target version cannot `DROP COLUMN IF EXISTS`, then in `apply_schema` (Python) inspect `information_schema.columns` and, only when `asset_class` is present, `DROP TABLE silver.monthly_panel` and recreate the four-column table. Never drop it unconditionally.

Keep `silver.processed_snapshots` unchanged.

**Operator note:** the first deploy after this schema lands drops the legacy EAV tables. Silver is empty until `uv run main.py prep --force`.

### Column semantics (`silver.observations`)

| Column | Rules |
|---|---|
| `family` | One of the 14 names in §4. |
| `metric` | `lower_snake_case`, no family prefix (`equity`, `people`, `total_return_3yr`, `tresgs`). |
| `code` | AUM ISO-4217, country ISO-2, theme `theme_id`; else `NULL`. |
| `effective_date` | Resolved reporting date. Invariant: `effective_date <= fetched_at::DATE`. If a parsed date is after `fetched_at.date()`, treat it as unparsed (do not push `DateContext`). |
| `date_source_depth` | 0 snapshot, 1 payload, 2 container, 3+ item/leaf. |
| `fetched_at` | Bronze snapshot `created_at` (the row’s ingest time). |
| `value` | Finite IEEE 754 (`not isnan`, `not isinf`). May be negative where the domain allows (fee waivers, shorts). Simplex weights are post-clean fractions in `[0, 1]`. |
| `raw_value` | Exact vendor string (unscaled display). |

Two themes that snake-case to the same `metric` on the same date collide on the PK; D7 keeps one. That is accepted. Do not add `code` to the PK. Catalog may count such collisions.

---

## 4. Families, representations, vocabularies

Define maps in `etfportfolio/core/endpoints.py`. Residual maps are `dict[str, bool]` (`True` = residual, dropped after simplex normalize).

Also export:

```python
DEFAULT_ZERO_FAMILIES = frozenset({
    "asset_class",
    "country",
    "industry",
    "credit_rating",
    "maturity",
    "theme",
    "rank_adj_theme",
    "style_box",  # panel name after hist merge; observations also have style_box_hist
})

SCALAR_FAMILIES = frozenset({
    "ratios",
    "esg",
    "mstar",
    "lipper",
    "profile",
})

STYLE_CELLS: tuple[str, ...] = tuple(
    f"{size}_{style}"
    for size in ("large", "multi", "mid", "small")
    for style in ("value", "core", "growth")
)
```

`metric` is the map key. Panel `feature_id` is `{family}_{metric}`.

### 4.1 Default-0 families (STRICT_SIMPLEX + style + themes)

Snapshot-replaced and densified to 0 at panel time (§6.4). **No exceptions** in this list.

#### `asset_class` (from `holdings.allocation_self`)

```python
ASSET_CLASS_METRICS = {
    "equity": False,
    "fixed_income": False,
    "cash": False,
    "other": True,  # residual
}
```

#### `country` (from `holdings.investor_country`, open vocab)

- `metric = to_lower_snake_case(name)`
- `code` = ISO-2 after remaps `{"Croatia": "HR", "Bulgaria": "BG", "Guam": "GU", "Uzbekistan": "UZ"}`; else payload `country_code`
- Residual: `unidentified` (drop post-normalize). `"Unidentified"` snakes to `unidentified`.
- Unknown country names are allowed (`open_vocab=True`)

#### `industry` (from `holdings.industry`)

Remap `"Telecommunication Services-Discontinued eff 09/19/2020"` → `"Communication Services"` **before** snake-case.

```python
INDUSTRY_METRICS = {
    "academic_and_educational_services": False,
    "basic_materials": False,
    "communication_services": False,
    "consumer_cyclicals": False,
    "consumer_non_cyclicals": False,
    "energy": False,
    "financials": False,
    "healthcare": False,
    "industrials": False,
    "real_estate": False,
    "technology": False,
    "utilities": False,
    "non_classified_equity": True,
    "not_classified_non_equity": True,
}
```

#### `credit_rating` (from `holdings.debtor`)

Strip `"% Quality/"`, `"% Quality "`, `"% Quality-"` then snake-case.

```python
CREDIT_RATING_METRICS = {
    "aaa": False, "aa": False, "a": False, "bbb": False,
    "bb": False, "b": False, "ccc": False, "cc": False,
    "c": False, "d": False,
    "not_rated": True, "not_available": True,
}
```

#### `maturity` (from `holdings.maturity`)

`to_lower_snake_case` of the vendor display is the metric (`"% Maturity Less than 1 Year"` → `maturity_less_than_1_year`). Do not keep old `mat_lt_1y` slugs.

```python
MATURITY_METRICS = {
    "maturity_less_than_1_year": False,
    "maturity_1_to_3_years": False,
    "maturity_3_to_5_years": False,
    "maturity_5_to_10_years": False,
    "maturity_10_to_20_years": False,
    "maturity_20_to_30_years": False,
    "maturity_greater_than_30_years": False,
    "maturity_other": True,
}
```

#### `theme` and `rank_adj_theme` (from `theme_weights`)

Open vocab. `metric = to_lower_snake_case(name)`, `code = theme_id`. Values are payload floats **as given** (no `/100`).

#### `style_box` and `style_box_hist` (from `profile.mstar`)

Categorical dummies. Valid X `{"value","core","growth"}`, Y `{"large","multi","mid","small"}`.  
Extract **active cells only**, `value=1.0`, `metric=f"{y}_{x}"` (e.g. `mid_core`), `code=NULL`.  
Do not assert presence of `large_growth` / `large_value` / `mid_value` in extractor tests (empirical data may have none). The **panel** still densifies all 12 cells on live style months.

### 4.2 Scalar families (no stored zeros; missing = no row)

#### `ratios` (UNBOUNDED_FLOATS)

```python
RATIOS_PERCENTAGE_METRICS = frozenset({
    "eps_growth_1yr", "eps_growth_3yr", "eps_growth_5yr",
    "sales_growth_1_year", "sales_growth_3_year", "sales_growth_5_yr",
    "sales_per_share_growth_1_year", "sales_per_share_growth_3_year",
    "operating_cash_flow_growth_rate_3yr",
    "return_on_assets_1yr", "return_on_assets_3yr",
    "return_on_equity_1yr", "return_on_equity_3yr",
    "return_on_investment_1yr", "return_on_investment_3yr",
    "return_on_capital", "return_on_capital_3yr",
    "dividend_yield_weighted_average", "dividendpayoutratio5yr",
    "dividend_per_share_1yr", "dividend_per_share_3yr",
    "yield_to_maturity", "average_coupon", "relative_strength",
})
RATIOS_STANDARD_METRICS = frozenset({
    "price_earnings", "price_book", "price_sales", "price_cash", "price_to_dividend",
    "average_final_composite_zscore", "latest_composite_z_score",
    "latest_dividend_yield_zscore", "latest_price_sales_zscore",
    "latest_price_to_book_zscore", "latest_price_to_earnings_zscore",
    "latest_return_on_equity_zscore", "latest_sps_growth_zscore",
    "weighted_final_composite_zscore",
    "average_quality", "effective_maturity", "nominal_maturity",
    "total_assets_total_equity", "total_debt_total_capital",
    "total_debt_total_equity", "lt_debt_shareholders_equity",
    "ebit_to_interest", "sales_to_total_assets",
})
ALL_RATIOS_METRICS = RATIOS_PERCENTAGE_METRICS | RATIOS_STANDARD_METRICS
```

Unknown `name_tag` after snake-case → `ValueError`. Percentage metrics: `value / 100.0`. Omit `average_quality` when `raw_value == "-"` (micro-clean in the extractor, not the panel).

#### `esg` (0–100 vendor scale; leave as-is)

```python
ESG_METRICS = frozenset({
    "tresgs", "tresgcs", "tresgccs", "tresgens", "tresgenrrs",
    "tresgeners", "tresgenpis", "tresgsos", "tresgsowos", "tresgsohrs",
    "tresgsocos", "tresgsoprs", "tresgcgs", "tresgcgbds", "tresgcgsrs",
    "tresgcgvss",
})
```

Unknown pillar → `ValueError`. `coverage` is **not** an esg metric; it is `profile.esg_coverage`. Keep zero scores (`0` is a real score; it is not “missing”).

#### `mstar` (ordinal 1–5)

Metrics: `people`, `process`, `parent`, `medalist_rating`, `morningstar_rating`, `sustainability_rating`.

Maps (lowercase; treat `-` and `_` and spaces as equivalent when looking up):

- medalist: Gold=5, Silver=4, Bronze=3, Neutral=2, Negative=1
- people/process/parent: High=5, Above Average=4, Average=3, Below Average=2, Low=1
- morningstar_rating: `"1"`…`"5"`
- sustainability: `"1"`…`"5"` **or** High…Low as above

Skip `category`, `category_index`. Skip empty / `-` / `under_review` / `not_applicable` / `n/a` / `na` for the **score**, but see coverage below.  
Strip a leading `q_` from `id` to get the metric name; do **not** put `quant`/`analyst` in the metric. Missing `id` or unknown id/rating → `ValueError`.

`profile.mstar_coverage` = `k / 3.0` where `k` is the number of summary items whose id (after stripping `q_`) is in `{people, process, parent}` — **count the item even if the score was skipped** (`under_review` still means the pillar is present). If `k == 0`, omit coverage (do not emit `0.0`).

#### `lipper` (ordinal 1–5)

Horizons: `overall`, `3_year`→`3yr`, `5_year`→`5yr`, `10_year`→`10yr`.  
Select the universe with maximum peer count (max integer parsed from `rating.name`); tie-break `("United States","Germany","UK","Canada","Japan","Australia")` (earlier in that list wins).  
`metric = f"{to_lower_snake_case(name_tag)}_{horizon}"` (e.g. `total_return_3yr`).  
`feature_id` = `lipper_total_return_3yr`. Do **not** put a second `lipper_` into `metric`.

#### `profile`

```python
PROFILE_METRICS = frozenset({
    "total_expense_ratio", "total_net_assets_local", "is_passive",
    "manager_tenure_years", "audited_net_expense_ratio",
    "management_expense_ratio", "non_management_expense_ratio",
    "top_10_weight", "theme_coverage", "esg_coverage", "mstar_coverage",
})
```

- expense allocation ratios: vendor fractions as given (0–1 typical; do not clip here)
- `total_expense_ratio`, `audited_net_expense_ratio`: `parse_percentage`
- `is_passive`: Passive=1, Active=0; skip pending tokens (`under_review`, `n/a`, `-`, empty, … same skip set as today’s extractors); else `ValueError`
- `total_net_assets_local`: `parse_net_assets` + `disambiguate_aum_currency`; omit if currency is `None`
- `manager_tenure_years`: `parse_manager_tenure` at the observation’s `effective_date`. Panel **does not** grow tenure along the spine in this phase; it LOCF’s the extracted scalar.
- `top_10_weight`: `parse_percentage` from holdings `top_10_weight`

---

## 5. Phase 1 — Observations

### 5.1 Package layout

| Path | Role |
|---|---|
| **Create** `etfportfolio/core/endpoints.py` | Move the existing `Endpoint` dataclass, `ENDPOINTS` list, `ENDPOINTS_BY_NAME`, `DETAILS_ENDPOINTS`, `GATED_ENDPOINTS`, `UNGATED_ENDPOINTS` **unchanged**. Add every metric map / frozenset in §4, `DEFAULT_ZERO_FAMILIES`, `SCALAR_FAMILIES`, `STYLE_CELLS`, country/industry remaps. |
| **Delete** `etfportfolio/ingest/endpoints.py` | Import sites switch to `etfportfolio.core.endpoints`: `ingest/details.py`, `ingest/landing.py`, `ingest/snapshots.py`, `ingest/pipeline.py`, and any ingest tests. |
| `etfportfolio/prep/utils.py` | `Observation`, `DateContext`, `clean_simplex`, `to_lower_snake_case`, new `disambiguate_aum_currency`. Keep `parse_net_assets`, `parse_percentage`, `parse_manager_tenure`, `clean_credit_rating`, `decompress_payload`. **Delete** `MetricRow`, `DimensionRow`, `MetricTuple`, `DimensionTuple`, `ExtractionResult`, `parse_effective_date`, `sanitize_metric_id`, and the old CAD-guessing `disambiguate_aum_currency`. |
| `etfportfolio/prep/extractors.py` | Seven functions returning `list[Observation]`. `EXTRACTOR_REGISTRY` here. Delete `EXTRACTOR_REGISTRY`’s old `ExtractionResult` contract. |
| `etfportfolio/prep/pipeline.py` | Batch extract, isolate errors, depth/`fetched_at` staging, upsert into `silver.observations`. Stop reading/writing the two legacy EAV tables. Stop importing `etfportfolio.ingest.endpoints`. |

Keep ingest HTTP behavior (gating, freshness, sessions, snapshot changelog) unchanged except import paths.

Every `etfportfolio/<pkg>/<mod>.py` still has `tests/<pkg>/test_<mod>.py`. Moving endpoints to core means **create** `tests/core/test_endpoints.py` (adapt `tests/ingest/test_endpoints.py`) and delete or re-export the ingest copy so there is not a stale ingest test importing a deleted module.

### 5.2 `Observation` and snake-case

```python
@dataclass(frozen=True, slots=True)
class Observation:
    product_id: int
    family: str
    metric: str
    code: str | None
    effective_date: date
    date_source_depth: int
    fetched_at: datetime
    value: float
    raw_value: str

    def to_row(self) -> tuple[int, str, str, str | None, date, int, datetime, float, str]:
        return (
            self.product_id, self.family, self.metric, self.code,
            self.effective_date, self.date_source_depth, self.fetched_at,
            self.value, self.raw_value,
        )
```

```python
def to_lower_snake_case(s: str) -> str:
    cleaned = s.replace("&", "and")
    return re.sub(r"[^a-z0-9]+", "_", cleaned.lower()).strip("_")
```

Call `to_lower_snake_case` everywhere a metric is derived from a vendor string. Do not keep `sanitize_metric_id`.

Do not emit an observation if `value` is non-finite.

### 5.3 `DateContext`

Lexical scope stack. No date leakage across JSON branches (a nephew date must not affect a sibling).

```python
@dataclass(frozen=True)
class DateScope:
    effective_date: date
    depth: int

class DateContext:
    def __init__(self, snapshot_date: date):
        self._stack: list[DateScope] = [DateScope(snapshot_date, 0)]

    @property
    def current(self) -> DateScope:
        return self._stack[-1]

    @contextmanager
    def scope(self, raw_date: Any, depth: int) -> Iterator[DateScope]:
        parsed = self.parse_date(raw_date)
        if parsed is not None and parsed <= self._stack[0].effective_date:
            self._stack.append(DateScope(parsed, depth))
        else:
            self._stack.append(self.current)
        try:
            yield self.current
        finally:
            self._stack.pop()
```

Causality bound is the constructor’s `snapshot_date` (`fetched_at.date()`). A parsed date **after** that is treated as unparsed and the scope inherits `current`.

`parse_date` returns `None` on failure:

- `None`, empty/whitespace string, numeric `<= 0` → `None`
- `int`/`float` or all-digit string **longer than 8**: `datetime.fromtimestamp(val/1000, tz=UTC).date()`; catch `ValueError`, `OverflowError`, `OSError`
- 8-digit string: `%Y%m%d`
- contains `-`: first 10 chars `%Y-%m-%d`
- contains `/`: first 10 chars `%Y/%m/%d`

Use `contextlib.suppress(ValueError)` on the strptime branches. Do not return a source string; depth is the provenance.

The pipeline constructs `DateContext(fetched_at.date())` once per snapshot and passes it in. Every emitted row uses `date_ctx.current.effective_date` and `date_ctx.current.depth`.

Depth convention:

| Depth | Source |
|---|---|
| 0 | Snapshot `fetched_at.date()` (constructor) |
| 1 | Payload `as_of_date` / `asOfDate` |
| 2 | Container (annual report, Lipper universe) |
| 3+ | Leaf (mstar `publish_date`, AUM embedded `(YYYY/MM/DD)`) |

### 5.4 `clean_simplex`

Pure function. Holdings sleeves only.

```python
def clean_simplex(
    raw_items: list[dict[str, Any]],
    name_extractor: Callable[[dict[str, Any]], str | None],
    weight_extractor: Callable[[dict[str, Any]], float | None],
    metric_map: dict[str, bool] | None,
    code_extractor: Callable[[dict[str, Any]], str | None] | None = None,
    open_vocab: bool = False,
    residual_names: set[str] | None = None,
) -> list[tuple[str, str | None, float, str]]:
    ...
```

Algorithm:

1. For each item: `raw_name = name_extractor(item)`, `raw_w = weight_extractor(item)`. Skip if either is `None`. `metric = to_lower_snake_case(raw_name)`. `code = code_extractor(item) if code_extractor else None`. `raw = str(item.get("formatted_weight", raw_w))`.
2. Closed vocab: if `not open_vocab` and `metric_map` is not `None` and `metric not in metric_map` → `ValueError(f"Unrecognized metric '{metric}' in simplex")`.
3. Clip `weight = max(0.0, float(weight))`.
4. `total = sum(weights)`; if `total <= 0`: return `[]`.
5. Divide each weight by `total`.
6. Drop residuals (`metric_map[metric] is True` when present in map, or `metric in residual_names`).
7. Return `(metric, code, value, raw)` for survivors.

Holdings vendor weights are **percentage points** (99.8 = 99.8%). Convert **in the weight_extractor** (`float(w) / 100.0`) so `clean_simplex` sees fractions. After normalize, survivors are fractions of the **including-residual** total.

### 5.5 AUM currency

Replace the current guesser. New signature:

```python
SYMBOL_TO_CURRENCIES: dict[str, set[str]] = {
    "$": {"USD", "CAD", "AUD", "MXN", "SGD", "HKD", "NZD", "TWD"},
    "€": {"EUR"},
    "£": {"GBP", "EGP", "LBP"},
    "¥": {"JPY", "CNY", "CNH"},
    "₩": {"KRW", "KPW"},
    "₹": {"INR"},
}

def disambiguate_aum_currency(
    raw_value: str,
    contract_currency: str | None,
    known_currencies: set[str],
) -> str | None:
    s = raw_value.strip()
    if not s:
        return None
    m = re.match(r"^([A-Za-z]{3})\b", s)
    if m:
        token = m.group(1).upper()
        return token if token in known_currencies else None
    if s[0] in SYMBOL_TO_CURRENCIES:
        prod = (contract_currency or "").strip().upper()
        return prod if prod in SYMBOL_TO_CURRENCIES[s[0]] else None
    prod = (contract_currency or "").strip().upper()
    if prod in known_currencies:
        return prod
    return None
```

Keep `parse_net_assets` / `parse_percentage` / `parse_manager_tenure` / `clean_credit_rating` / `decompress_payload` as they exist today (they are still the right parsers). Delete `parse_effective_date` (it is also invalid Python 3 `except` syntax in the current tree — do not port it).

### 5.6 Extractors

Unified signature (unused kwargs stay defaulted so the pipeline can always pass currency):

```python
def extract_*(
    product_id: int,
    payload: dict[str, Any],
    fetched_at: datetime,
    date_ctx: DateContext,
    contract_currency: str | None = None,
    known_currencies: set[str] | None = None,
) -> list[Observation]:
```

Empty / `None` / non-dict payload → `[]`.

```python
EXTRACTOR_REGISTRY: dict[str, Callable[..., list[Observation]]] = {
    "holdings": extract_holdings,
    "profile": extract_profile,
    "ratios": extract_ratios,
    "esg": extract_esg,
    "mstar": extract_mstar,
    "lipper": extract_lipper,
    "theme_weights": extract_theme_weights,
}
```

Do **not** put this registry in `core/`.

Helper to build rows (optional, keep it local if used once):

```python
def _obs(..., date_ctx, fetched_at, value, raw, code=None) -> Observation:
    scope = date_ctx.current
    return Observation(..., effective_date=scope.effective_date,
                       date_source_depth=scope.depth, fetched_at=fetched_at, ...)
```

#### `extract_holdings`

- `with date_ctx.scope(payload.get("as_of_date"), 1):` around the whole extract
- `top_10_weight` → family `profile`, metric `top_10_weight` via `parse_percentage`
- `allocation_self` → `clean_simplex(..., metric_map=ASSET_CLASS_METRICS)` → family `asset_class`
- `investor_country` → `clean_simplex(..., open_vocab=True, residual_names={"unidentified"})` with code remaps from §4.1
- `industry` → remap discontinued telecom in `name_extractor`, `INDUSTRY_METRICS`
- `debtor` → `clean_credit_rating` then snake-case inside simplex, `CREDIT_RATING_METRICS`
- `maturity` → snake-case display names, `MATURITY_METRICS`
- **Do not extract** `debt_type`, `currency`, `geographic`, `top_10` constituent names

Each simplex tuple becomes one `Observation` at `date_ctx.current`.

#### `extract_profile`

The profile payload has **no** reliable root `as_of_date`. Leave depth 0 except the two explicit pushes below.

- `expenses_allocation`: names `"Management Expenses"` / `"Non-Management Expenses"` → `management_expense_ratio` / `non_management_expense_ratio` at depth 0
- `fund_and_profile` tags, same matching as today’s extractor (`Total_Expense_Ratio` / `"Total Expense Ratio"`, `Management_Approach`, `Total_Net_Assets_*` / name startswith `"Total Net Assets"`, `Manager_Tenure`)
- AUM: `parse_net_assets`; `with date_ctx.scope(embedded_date_string_or_parsed, 3):` around that **one** observation; `code = disambiguate_aum_currency(raw, contract_currency, known_currencies or set())`; **skip the row** if `code is None`
- Annual report `name == "Annual Report"`: `with date_ctx.scope(report["as_of_date"], 2):` for `audited_net_expense_ratio`
- Style boxes from `payload["mstar"]`: families `style_box` / `style_box_hist`; raise on missing/unknown axes or out-of-bounds coords (keep today’s validation). Metric is `{y}_{x}`, value `1.0`, raw `str(coord)`
- Only this extractor needs `contract_currency` / `known_currencies`

#### `extract_ratios`

- `with date_ctx.scope(payload.get("as_of_date"), 1):`
- Sections `dividend`, `financials`, `fixed_income`, `ratios`, `zscore`
- Skip `value is None` or missing `name_tag`
- `metric = to_lower_snake_case(name_tag)` must be in `ALL_RATIOS_METRICS` else `ValueError`
- Divide by 100 if metric in `RATIOS_PERCENTAGE_METRICS`
- `raw_value = str(item.get("value_fmt") if item.get("value_fmt") is not None else value)`
- Skip `average_quality` when that raw string is `"-"`

#### `extract_esg`

- `with date_ctx.scope(payload.get("asOfDate"), 1):`
- `coverage` → family `profile`, metric `esg_coverage`
- Walk `content` and each node’s `children`; slug = `to_lower_snake_case(name)`; must be in `ESG_METRICS` else `ValueError`
- Keep `value == 0`

#### `extract_mstar`

- `with date_ctx.scope(payload.get("as_of_date"), 1):` for the payload
- For each `summary` item: skip `category` / `category_index`; require `id`; strip `q_` for the metric; map scores as §4.2
- `with date_ctx.scope(pillar.get("publish_date"), 3):` around each **emitted score** (inherit payload date when publish_date is missing/unparsed)
- Coverage uses the payload-level scope (not the leaf publish_date), emitted as `profile.mstar_coverage`

#### `extract_lipper`

- Keep the current universe picker and `_regex_int` peer-count helper
- `with date_ctx.scope(universe.get("as_of_date"), 2):` around emitted rows
- `raw_value` may remain `"4 (United States: 1500 funds)"`

#### `extract_theme_weights`

- No payload date: stay at depth 0
- `coverage` → `profile.theme_coverage` (float as given)
- For each theme with `key` and `name`:
  - emit `theme` if `weight is not None`
  - emit `rank_adj_theme` if `rank_adjusted_weight is not None`
  - missing one does not block the other
- No `/100`

### 5.7 Pipeline (`prep/pipeline.py`)

`BATCH_SIZE = 100`.

Import `ENDPOINTS` from `etfportfolio.core.endpoints` (not ingest). Import `EXTRACTOR_REGISTRY` from `etfportfolio.prep.extractors`.

**Force** (`main.py prep --force`):

```sql
BEGIN;
DELETE FROM silver.monthly_panel;
DELETE FROM silver.processed_snapshots;
DELETE FROM silver.observations;
COMMIT;
```

Pending snapshots: `bronze.snapshots` left-join `silver.processed_snapshots` where processed is null, ordered by `snapshot_id`.

Preload once per run:

```python
known_currencies = {
    row[0]
    for row in conn.execute(
        "SELECT DISTINCT currency FROM bronze.contracts WHERE currency IS NOT NULL"
    ).fetchall()
}
```

Per product, `contract_currency` is **only** `bronze.contracts.currency`. Do **not** merge `bronze.products` for AUM. Load contracts for the chunk’s product ids (missing contract → `None`).

`URL_PREFIX_TO_NAME = {ep.url_prefix: ep.name for ep in ENDPOINTS}`. Skip endpoint name `landing` (watermark, emit nothing). Unknown prefix → `ValueError(f"Unregistered extractor for url_prefix: {url_prefix}")` — **fail the run** (this is a code bug, not a dirty payload).

**Per snapshot inside a chunk, isolate payload/extract failures:**

1. Decompress. On failure: log, do **not** watermark, continue.
2. Empty payload (`{}` / empty): watermark, emit nothing.
3. `date_ctx = DateContext(created_at.date())` (use the snapshot’s `created_at` as `fetched_at`).
4. Call extractor. On any `Exception`: log `(product_id, snapshot_id, endpoint, exception)`, do **not** watermark, continue.
5. Stage successful observations; mark snapshot succeeded.

A failure must **not** roll back sibling successes in the same chunk. Stage in memory, then one transaction for the chunk: upsert staged rows + watermark **succeeded** ids only.

**In-memory staging** keyed by `(product_id, family, metric, effective_date)`:

```
if pk not in staged or obs.date_source_depth > existing.date_source_depth:
    staged[pk] = obs
elif (obs.date_source_depth == existing.date_source_depth
      and obs.fetched_at > existing.fetched_at):
    staged[pk] = obs
```

**Upsert** (same rule versus rows already in DuckDB):

```sql
INSERT INTO silver.observations (
    product_id, family, metric, code, effective_date, date_source_depth,
    fetched_at, value, raw_value
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (product_id, family, metric, effective_date) DO UPDATE SET
    code = EXCLUDED.code,
    value = EXCLUDED.value,
    raw_value = EXCLUDED.raw_value,
    date_source_depth = EXCLUDED.date_source_depth,
    fetched_at = EXCLUDED.fetched_at
WHERE EXCLUDED.date_source_depth > date_source_depth
   OR (EXCLUDED.date_source_depth = date_source_depth
       AND EXCLUDED.fetched_at > fetched_at);
```

After the run, log watermarked / failed / empty. Do **not** raise at the end solely because some snapshots failed; the operator reruns `prep` to retry unwatermarked rows.

`run_observations` returns the number of snapshots **watermarked** this run (successes + empty payloads). Document that in the docstring: failed snapshots remain pending.

---

## 6. Phase 2 — Monthly panel

Always a full rebuild (`DELETE` then `INSERT`). LOCF needs complete history.

If `silver.observations` is empty, `DELETE FROM silver.monthly_panel` and return 0.

Reuse `month_end_spine(start, end)` already in `prep/panel.py`.

### 6.1 Feature identity

```
feature_id = f"{family}_{metric}"
```

Examples: `asset_class_equity`, `country_united_states`, `industry_technology` (not `sector_technology`), `credit_aaa`, `maturity_less_than_1_year`, `theme_discount_retail`, `rank_adj_theme_discount_retail`, `style_box_mid_core`, `profile_is_passive`, `profile_total_net_assets_usd`, `mstar_people`, `lipper_total_return_3yr`, `esg_tresgs`, `ratios_price_earnings`.

Never put `code` into `feature_id`. After style merge, family is `style_box` (no `style_box_hist_*` features).

### 6.2 Product trading spine

Per product that appears in **both** `silver.observations` and `bronze.prices`:

```
first_price = MIN(bronze.prices.date)::DATE
last_price  = MAX(bronze.prices.date)::DATE
```

Month-end calendar: last calendar day of each month `t` with `first_price <= t <= last_price` (`month_end_spine(first_price, last_price)`).

Do **not** use global min/max across all products. Do **not** pad +540 days. Products with observations but no prices contribute **no** panel rows.

### 6.3 Scalar interpolation

Families: `ratios`, `lipper`, `esg`, `mstar`, `profile` (except that AUM is pre-aggregated in §6.5 into a USD series, then interpolated **as a scalar**).

Grain: `(product_id, family, metric)` ≡ `(product_id, feature_id)`.

**Do not** snapshot-replace these. AUM months and TER months must coexist. Missing scalar metrics are **absent rows**, never 0.

**Cap (no fallback table, no global-per-name cap):**

```sql
-- gaps per (product_id, family, metric)
gap_days = date_diff('day',
    LAG(effective_date) OVER (
        PARTITION BY product_id, family, metric
        ORDER BY effective_date
    ),
    effective_date
)
-- keep gap_days > 0
cap = quantile_cont(gap_days, 0.99)::INTEGER
n_gaps = COUNT(*) of those gaps
```

Rules, for each `(product, feature)` and spine month `t ∈ [first_price, last_price]`:

| `n_gaps` | What is written |
|---|---|
| `0` (singleton) | The one value on **every** `t` in the spine, including `t < effective_date`. This is the MVP “one snapshot, all trading history” rule. |
| `≥ 1` | Let `obs` be the observation with `effective_date <= t` of latest `effective_date`, then latest `fetched_at`. Write it iff `t >= first_obs` **and** `t - obs.effective_date <= cap`. No row if `t` is before the first point, after `last_obs + cap`, or beyond `last_price`. |

`is_passive` is **not** a special perpetual flag. A singleton series is already uncapped via `n_gaps = 0`.

Do **not** grow `manager_tenure_years` along the spine in this phase.

### 6.4 Default-0 families — densified snapshot replacement

Families (panel names): `asset_class`, `country`, `industry`, `credit_rating`, `maturity`, `theme`, `rank_adj_theme`, `style_box`. **No exceptions.**

This is the only “split,” and it is the same boolean as `DEFAULT_ZERO_FAMILIES`:

- These families are **compositions or dummy grids**. The vendor sends a *snapshot of the whole sleeve*. An omitted bucket means 0, not “keep last month.”
- Per-metric LOCF would leak Jan Cash into a Feb snapshot that omitted Cash. A later wide `fillna(0)` cannot fix a leaked **number**.
- Therefore the interpolator’s grain is the **family snapshot date**, and we **store** zeros for the universe of that family on months when that snapshot is live.

#### Conceptual model (what the SQL must mean)

For every **live** `(product_id, family, as_of_date)` and every `metric` in that family’s universe, the panel value is:

```
COALESCE(observation.value at the chosen snapshot date, 0.0)
```

That is “start at 0, overlay the snapshot.” Implement it as **one** `CROSS JOIN universe LEFT JOIN observations … COALESCE`, not as `INSERT` all zeros then `UPDATE`.

Do **not** emit that universe on months when the family is **not** live. A full-zero sleeve is a fake empty portfolio.

#### Style merge (in a temp relation; do not mutate `silver.observations`)

Treat `style_box` and `style_box_hist` as one family named `style_box`.

On a given `(product_id, effective_date)`: if any `style_box` rows exist, use only those; else use `style_box_hist`. Metrics already share the 12-cell names. After this merge, hist does not exist as a panel family.

#### Family snapshot dates

```
family_dates(product_id, family, effective_date)
  = DISTINCT dates from the (merged) observation stream
```

#### Family cap (same singleton/p99 math, grain = snapshot dates)

```
family_gaps over (product_id, family) ORDER BY effective_date
  among DISTINCT effective_date
```

| `n_family_gaps` | Live months on the product spine |
|---|---|
| `0` | **All** `t ∈ [first_price, last_price]`. Chosen snapshot = that only date, even when `t < date`. Overlay still zeros metrics the snapshot omitted. |
| `≥ 1` | `family_cap = p99(family_gaps)`. For month `t`: `snap = max(effective_date \| effective_date <= t)`. If `snap` is null (`t` before first snapshot) → **emit nothing**. If `t - snap > family_cap` → **emit nothing**. Else densify `snap` against the universe. |

Do **not** apply per-metric p99 inside these families. A Cash bucket that appears in only some snapshots is handled by overlay-to-0, not by a Cash-specific cap.

#### Universe (this is the set of zeros)

| Family | Universe |
|---|---|
| `asset_class` | `{equity, fixed_income, cash}` — **never** `other`. Canonical even if this product never reported `cash`. |
| `industry` | Non-residual keys of `INDUSTRY_METRICS` |
| `credit_rating` | The ten letter grades, not `not_rated` / `not_available` |
| `maturity` | Seven non-residual keys |
| `style_box` | All 12 `STYLE_CELLS` |
| `country`, `theme`, `rank_adj_theme` | `SELECT DISTINCT metric FROM silver.observations WHERE family = … AND product_id = …` (**per-product** history, all time). Do **not** cross-join every theme in the book onto every ETF. A metric that appears later in the product’s history is in the universe of **earlier live** snapshots and overlays as 0 there. |

Closed universes come from the maps, **not** from `DISTINCT` of what the product happened to emit. If you built the universe from `DISTINCT` only, a product that never reported Cash would never get `asset_class_cash=0`, and the leak bug would return in disguise.

Open universes are per-product so the tall table does not explode (~1000 book-wide themes). After a later pivot, a country the product never held is simply not a column for that product; that is the later phase’s problem.

#### Overlay

```
value      = COALESCE(obs.value, 0.0)
feature_id = family || '_' || metric
```

Join observations at `(product_id, family, metric, effective_date = snap)`. Do **not** re-normalize after overlay. Residual-drop survivors may sum to `< 1`. Zeros do not change that.

#### Sketch (normative behavior, not required table names)

```sql
-- live(product_id, family, as_of_date, snap)
-- universe(family, metric)  -- canonical or per-product
-- obs_merged: observations after style hist merge, family rewritten to style_box

SELECT
    live.product_id,
    live.as_of_date,
    live.family || '_' || u.metric AS feature_id,
    COALESCE(o.value, 0.0) AS value
FROM live
JOIN universe u
  ON u.family = live.family
 AND (u.product_id = live.product_id OR u.product_id IS NULL)  -- NULL for canonical
LEFT JOIN obs_merged o
  ON o.product_id = live.product_id
 AND o.family = live.family
 AND o.metric = u.metric
 AND o.effective_date = live.snap;
```

### 6.5 AUM → USD (only remaining derived **value**)

Observations keep `profile` / `total_net_assets_local` / `code=ISO`.

Panel feature: `profile_total_net_assets_usd` **only**. Never write `profile_total_net_assets_local` to `monthly_panel`. Never write USD AUM into `silver.observations`.

**Order:** convert each local tick with **that tick’s calendar month’s** average FX → average those USD values inside the month → interpolate the monthly USD series with §6.3 (singleton / p99 on **this aggregated series**, not on raw local ticks).

Do not LOCF local amounts across months and then convert. Do not use daily FX. Do not re-convert a LOCF’d USD value with a later month’s FX (that would inject FX volatility into a stale AUM).

```sql
CREATE TEMP TABLE monthly_fx AS
SELECT
    source_currency AS currency,
    LAST_DAY(date::DATE) AS month_end,
    AVG(close) AS rate_to_usd
FROM bronze.fx
WHERE target_currency = 'USD'
GROUP BY 1, 2;

CREATE TEMP TABLE aum_usd_monthly AS
SELECT
    o.product_id,
    LAST_DAY(o.effective_date) AS effective_month,
    AVG(
        o.value * CASE
            WHEN o.code = 'USD' THEN 1.0
            ELSE fx.rate_to_usd
        END
    ) AS value
FROM silver.observations o
LEFT JOIN monthly_fx fx
  ON fx.currency = o.code
 AND fx.month_end = LAST_DAY(o.effective_date)
WHERE o.family = 'profile'
  AND o.metric = 'total_net_assets_local'
  AND (o.code = 'USD' OR fx.rate_to_usd IS NOT NULL)
GROUP BY o.product_id, LAST_DAY(o.effective_date);
```

Treat `aum_usd_monthly` as observations of feature `profile_total_net_assets_usd` with `effective_date = effective_month`. Then §6.3.

Operator: `bronze.fx` must cover contract currencies (`uv run main.py ingest fx`). A tick with missing FX is dropped from the average. A month with no convertible ticks has no AUM average (then §6.3 may fill later months from a previous month’s USD average).

### 6.6 What the panel must not do

- Winsorize or clamp
- Emit `is_leveraged`, `management_fee_rate`, `pillar_*`, `*_is_quant`, `mstar_people_analyst`, …
- Cluster debt or emit `debt_*`
- Assert simplex sums to 1
- Theme coverage gate
- Parent-theme rollups (do not join `bronze.themes`)
- Stamp a dominant asset class
- Pivot / `fillna`
- Re-normalize sleeves
- Grow manager tenure
- Backfill multi-obs series before `first_obs`
- 0-fill scalar families
- 0-fill default-0 families on **non-live** months

### 6.7 Write path

Build temp `panel_final(product_id, as_of_date, feature_id, value)` as the union of (1) densified default-0 families (2) scalar LOCF including USD AUM. Then:

```sql
BEGIN;
DELETE FROM silver.monthly_panel;
INSERT INTO silver.monthly_panel SELECT * FROM panel_final;
COMMIT;
```

Return `COUNT(*)`. Keep the existing console / progress logging style.

---

## 7. Catalog and tests

### 7.1 `scripts/catalog_silver.py`

Point at `silver.observations` / `silver.monthly_panel`. If the script still queries the legacy EAV tables, rewrite it.

Observations:

- 0 duplicate PKs on `(product_id, family, metric, effective_date)`
- `effective_date <= fetched_at::DATE`
- finite `value`
- theme referential check (informational, must not crash prep):

```sql
SELECT COUNT(*) FROM silver.observations o
LEFT JOIN bronze.themes t ON o.code = t.theme_id
WHERE o.family IN ('theme', 'rank_adj_theme') AND t.theme_id IS NULL;
```

Panel:

- 0 rows with `feature_id = 'profile_total_net_assets_local'`
- `profile_total_net_assets_usd` may exist
- no `asset_class` column
- no `debt_*` cluster features, no `style_box_hist_*`, no `asset_class_other`, no `country_unidentified`, no `mat_lt_1y`, no `esg_score` aliases

### 7.2 Tests to rewrite

Keep `tests/conftest.py` and `load_fixture`. Fixture payloads under `tests/fixtures/` still run through the new extractors.

| File | Must cover |
|---|---|
| `tests/core/test_endpoints.py` | Registry names, `resolve`, gated split, metric maps (residuals flagged, no `debt_type`), `DEFAULT_ZERO_FAMILIES` / `STYLE_CELLS` |
| `tests/prep/test_utils.py` | `to_lower_snake_case` (`&` → `and`), `DateContext` (branch isolation, epoch/ISO/8-digit, **future date ignored**), `clean_simplex` (clip, normalize-then-drop residual so 10%+90% other → equity 0.10 not 1.0, zero-sum omit, closed raise, open residual), `disambiguate_aum_currency` (token, glyph+contract match, omit on miss — **no** TSE/Canada/`listing_exchange` guessing), remaining parsers |
| `tests/prep/test_extractors.py` | Snake metrics, holdings simplex + **no** `debt_type`, `top_10_weight` in `profile`, profile AUM omitted on unresolved currency, esg coverage in `profile`, mstar metrics **without** analyst/quant suffix and without `mstar_` in `metric`, theme dual families unscaled, fixture parametrization still valid (assert `Observation` fields, not 9-tuples) |
| `tests/prep/test_pipeline.py` | Watermark; isolation (one bad snapshot does not block the batch or watermark itself); collision deeper-depth wins; equal-depth **later** `fetched_at` wins; `--force` wipes observations + panel + watermarks; unregistered prefix raises; `run_observations` return = watermarked count |
| `tests/prep/test_panel.py` | Four-column schema; `feature_id` shape; singleton spans full **per-product** price range including before `effective_date`; multi-obs no pre-first fill; p99 cap drops stale tail; Cash omitted on later snapshot → `asset_class_cash=0` not 0.2; live asset_class densifies `fixed_income`/`cash` to 0; **no rows** for a family after `last_snap + cap` (not a 0/0/0 sleeve); residual families absent; style 12 cells with hist losing to selected; AUM convert-then-average with monthly FX; no local AUM feature; no prices → no panel rows; country/theme zeros only for that product’s distinct metrics |
| Ingest tests | Import `etfportfolio.core.endpoints` |
| `tests/core/test_db.py` | Assert `silver.observations` exists and `monthly_panel` has no `asset_class` column; do **not** assert `product_metrics` / `product_dimensions` |

Delete tests that require `debt_type`, live `asset_class_other`, `country_unidentified`, `mat_lt_1y`, `esg_score` aliases, `pillar_people`, winsor, partition-of-unity, `monthly_panel.asset_class`, CAD guessing, earliest-`fetched_at` wins, `ExtractionResult` / `DimensionRow`.

---

## 8. File action checklist

| Action | Path |
|---|---|
| Create | `etfportfolio/core/endpoints.py` |
| Delete | `etfportfolio/ingest/endpoints.py` |
| Edit | `etfportfolio/core/schema.sql` (and `apply_schema` only if column drop needs Python) |
| Edit | `etfportfolio/prep/utils.py` |
| Edit | `etfportfolio/prep/extractors.py` |
| Edit | `etfportfolio/prep/pipeline.py` |
| Edit | `etfportfolio/prep/panel.py` |
| Edit | ingest `details.py`, `landing.py`, `snapshots.py`, `pipeline.py` (imports) |
| Edit | `scripts/catalog_silver.py` (create if missing, otherwise rewrite sources) |
| Create | `tests/core/test_endpoints.py` (adapt from `tests/ingest/test_endpoints.py`; delete the ingest copy) |
| Edit | `tests/prep/test_utils.py`, `test_extractors.py`, `test_pipeline.py`, `test_panel.py` |
| Edit | `tests/core/test_db.py`, any ingest test that imported `etfportfolio.ingest.endpoints` |

`prep/cli.py` and `main.py` stay as the phase-1-then-panel driver.

---

## 9. Implementation sequence

1. Schema + products view + drop legacy tables + monthly_panel column drop; fix `test_db.py`.
2. Move endpoints to core (catalog + maps); fix ingest imports and ingest tests.
3. Prep utils (`Observation`, `DateContext`, `clean_simplex`, strict AUM).
4. Extractors + registry; extractor tests (including fixtures).
5. Pipeline staging/upsert/isolation/force; pipeline tests.
6. Panel rewrite; panel tests.
7. Catalog script.
8. `uv run pytest` until green. Operator cutover on a real DB is `uv run main.py prep --force` (out of scope for unit tests).

Do not implement later-phase work (wide pivot, parent themes, winsor, factor returns) in this change.

---

## 10. Worked examples (implementer checks)

**Simplex residual.** Payload asset_class Equity 10, Other 90. After clip/normalize, drop `other`. Observation: `asset_class` / `equity` = `0.10` only. Live panel month: `asset_class_equity=0.10`, `asset_class_fixed_income=0`, `asset_class_cash=0`. Sum `0.10` is correct. Do not scale equity to `1.0`.

**Omitted cash.** Jan snapshot equity 0.8, cash 0.2. Feb snapshot equity 1.0 only. Feb panel: `asset_class_equity=1.0`, `asset_class_cash=0.0`, `asset_class_fixed_income=0.0`. Not cash=`0.2`.

**Stale sleeve.** Same product, last holdings snapshot 2024-01-31, family p99 = 31 days, prices continue through 2024-06-30. January month-end has the densified sleeve. From 2024-03-31 (59 days later) **no** `asset_class_*` rows (not three zeros).

**Singleton ESG.** One `tresgs` on 2025-06-30, prices 2020–2026. `n_gaps=0` → `esg_tresgs` on every month-end from first price through last price, including 2020.

**Two ratios.** P/E on 2024-01-31 and 2024-03-31 (59-day gap), p99=59. Month 2024-02-29 carries January. Month 2024-05-31 is 61 days after March → omit. Month 2023-12-31 is before first obs → omit.

**Singleton vs multi on the same family class.** `is_passive` once → paints the whole spine. `total_expense_ratio` monthly → p99 cap, no pre-first fill. Both are `profile` scalars; they do **not** snapshot-replace each other.

**AUM mixed month.** 15 Jul CAD 10_000_000 and 31 Jul USD 8_000_000; July avg CADUSD=0.75. July USD average = `(10e6*0.75 + 8e6*1)/2 = 7.75e6` → `profile_total_net_assets_usd`. August without AUM may LOCF that **USD** figure (no FX restatement).

**Unresolved AUM.** `$10M` and contract currency `EUR` (EUR not in the `$` candidate set) → no observation, no panel AUM.

**Collision.** Same PK, depth 3 vs depth 1 → keep depth 3. Same depth, fetched 10:00 vs 14:00 → keep 14:00.

**Bad industry name.** Snapshot raises, is not watermarked, sibling snapshots in the batch still commit.

**Theme open densify.** Product has ever emitted `discount_retail` and `cloud_computing`. A later snapshot only has `discount_retail`. Live month stores `theme_discount_retail=<value>` and `theme_cloud_computing=0`. A second product that never held those themes does not get those rows.

**Style hist vs selected.** Same date, `style_box` has `mid_core` and hist has `large_value`. Panel: `style_box_mid_core=1`, the other 11 cells `0` (including `style_box_large_value=0`). Hist-only date: `style_box_large_value=1`, other 11 cells `0`.