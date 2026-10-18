# FRD: Country-Weighted Macroeconomic Indicator Collapse

**Status:** Draft  
**Scope:** New `bronze.country_indicators` table + new `ingest/country_indicators.py` module + new collapse step in `prep/panel.py`

---

## 1. Background and Motivation

### 1.1 What the country family currently contains

`silver.observations` stores per-ETF country weight breakdown observations under `family = 'country'`. Each row records the fractional portfolio weight (`value`) for one country in one product at one point in time. The `metric` column is the slugified country name (e.g. `united_states`) and the `code` column holds the ISO 3166-1 alpha-2 country code (e.g. `US`). As of this writing, there are ~100 distinct (metric, code) country pairings observed across the universe.

These per-country weight columns are the raw, high-cardinality representation of a portfolio's geographic exposure. They are ingested from IBKR holdings data and preprocessed by `prep/pipeline.py` into `silver.observations` unchanged.

### 1.2 The problem: per-country weights as panel features are not useful

The current `panel.py` materialises all surviving country observations directly as individual `country_<country_name>` features in `silver.monthly_panel`. This means the panel grows by one column per country ever observed in any product, which is both high-dimensional and economically uninterpretable. A portfolio's 3% allocation to Japan is not a useful panel feature; what matters is what characteristics that 3% Japan exposure brings to the portfolio — growth, trade volume, GDP, etc.

### 1.3 What the MVP did

The legacy MVP fetched macroeconomic indicator time-series for every country in the dataset via the World Bank API (`wbgapi`), computed country-level indicator snapshots (level + YoY growth rate), and then collapsed the per-country weight columns into a fixed set of **portfolio-level** macroeconomic features by taking the weighted average of each indicator across all countries held by each ETF:

```
portfolio_gdp_stat = Σ (country_weight_i × country_gdp_level_i)
portfolio_gdp_growth_rate = Σ (country_weight_i × country_gdp_yoy_growth_i)
```

This produced a small, fixed set of economically meaningful scalar features that capture the macro environment of each ETF's geographic footprint.

### 1.4 Goals of this FRD

Describe all the functional requirements needed to replicate and improve this logic in the current medallion architecture, without coupling `ingest` and `prep` packages or introducing unnecessary complexity.

---

## 2. Existing Architecture Context

### 2.1 Package boundaries

The codebase enforces a strict layering rule: packages must not import from each other (only from `etfportfolio/core/`). Specifically:

- **`etfportfolio/ingest/`** — fetches and stores raw data into the bronze layer. No knowledge of `prep`. Currently contains: `fx.py`, `themes.py`, `prices.py`, `products.py`, `contracts.py`, `details.py`, `snapshots.py`, `landing.py`, `session.py`, `series.py`, `clean.py`, `gateway.py`, `utils.py`, `pipeline.py`.
- **`etfportfolio/prep/`** — reads from bronze/silver layers and produces cleaned silver/gold outputs. No knowledge of `ingest`. Currently contains: `pipeline.py` (observations), `panel.py` (monthly panel), `extractors.py`, `utils.py`, `cli.py`.
- **`etfportfolio/core/`** — shared utilities consumed by both. Currently contains: `db.py`, `config.py`, `endpoints.py`, `schema.sql`, `logging.py`, `progress.py`.

### 2.2 Analogous prior art: `ingest/fx.py` and `ingest/themes.py`

The project already has two examples of ingesting non-IBKR reference data into the bronze layer:

- `ingest/fx.py` fetches FX rates from `yfinance` for all distinct contract currencies. It resolves the set of currencies needed by querying `bronze.contracts`, then stores time-series in `bronze.fx`.
- `ingest/themes.py` fetches the global theme taxonomy from an IBKR knowledge-graph API endpoint and stores it in `bronze.themes`, which is subsequently consumed by `prep/panel.py` to collapse child themes into parent theme features.

The proposed World Bank country indicator ingestion follows exactly the same pattern as `fx.py`/`themes.py`: query existing bronze data to determine what to fetch, then store results in a new bronze table.

### 2.3 Why country indicator ingestion belongs in `ingest/`, not `prep/`

The user raised the question of whether this ingestion belongs in `ingest/` (given that it requires knowing which countries appear in `silver.observations`, a prep-layer output). The answer is: **it still belongs in `ingest/`**, for the following reasons:

1. `bronze.country_indicators` is a static, non-IBKR reference table — it is logically the same kind of data as `bronze.fx` or `bronze.themes`. Its consumers (currently only `panel.py`) should read from a pre-populated bronze table rather than triggering API calls during panel construction.
2. The country codes can equally be derived from `bronze.snapshots` / `bronze.payload_blobs` or from `silver.observations`. Since country codes are already present in `silver.observations.code`, they are convenient to read from there at ingest time, just as `fx.py` reads from `bronze.contracts` to resolve its currency universe. This does not create a hard prep→ingest dependency; it merely queries a silver read-model that ingest already has access to via `core/db.py`.
3. Keeping all external API calls in `ingest/` preserves the invariant that `prep/` is a pure transformation layer that can run offline without any network access.

### 2.4 The `clean_simplex` / `_country_code` path in `extractors.py`

Country observations are extracted in `extract_holdings` via `clean_simplex(..., open_vocab=True, residual_names={"unidentified"})`. The `open_vocab=True` flag means any country name that clears the simplex normalisation is accepted — there is no closed whitelist for countries. The `_country_code` function first checks `COUNTRY_CODE_REMAPS` (a small hand-maintained dict in `endpoints.py`) and falls back to the `country_code` field from the raw JSON payload.

Crucially: **country observations are not filtered out for missing codes**. If `country_code` is absent from the raw payload and the name is not in `COUNTRY_CODE_REMAPS`, the observation is stored with `code = NULL`. For the indicator collapsing logic to work, every country observation that will participate in the collapse must have a non-null ISO alpha-2 code, because that code is the join key to the World Bank API. Therefore:

- The current `COUNTRY_CODE_REMAPS` dict should be kept up to date for any countries whose codes are missing from the raw IBKR payload.
- Country observations with a null code cannot be collapsed and should be treated as zero weight for the purpose of the indicator collapse (they still survive as raw country weight features if the low-count pruning retains them).

---

## 3. Functional Requirements

### 3.1 New bronze table: `bronze.country_indicators`

A new table must be added to `etfportfolio/core/schema.sql` to store World Bank macroeconomic indicator time-series at annual country-year granularity.

**Schema (logical):**

| Column | Type | Notes |
|---|---|---|
| `country_code` | `VARCHAR` | ISO 3166-1 alpha-2 (e.g. `US`, `DE`) |
| `indicator_code` | `VARCHAR` | World Bank indicator ID (e.g. `NY.GDP.MKTP.CD`) |
| `year` | `INTEGER` | Calendar year |
| `value` | `DOUBLE` | Indicator value (post-transformation; see §3.2.4) |
| `fetched_at` | `TIMESTAMP WITH TIME ZONE` | When this row was written |
| Primary key | | `(country_code, indicator_code, year)` |

A separate status table `bronze.country_indicators_status` should track freshness per `(country_code, indicator_code)`, following the same pattern as `bronze.fx_status` and `bronze.price_status`.

### 3.2 New ingest module: `etfportfolio/ingest/country_indicators.py`

This module is responsible for fetching and storing country indicator data from the World Bank API via the `wbgapi` Python library. It follows the same structural pattern as `ingest/fx.py`.

#### 3.2.1 Country universe resolution

The set of countries to fetch must be derived at runtime from the distinct ISO alpha-2 codes present in `silver.observations` where `family = 'country'` and `code IS NOT NULL`. This is the current-architecture equivalent of the MVP's `coco.CountryConverter()` lookup — the codes are already present in the observations table, so no country name conversion library is needed.

The resolved codes must be validated against the World Bank economy list (`wb.economy.list()`) to filter out codes the API does not recognise (e.g. `GG` for Guernsey, `JE` for Jersey, offshore financial centres, and non-sovereign territories like `PR`, `GU`, `VI` which are not independent World Bank economies). Unrecognised codes should be logged at INFO level and skipped — they contribute zero to the indicator collapse for any ETF holding them.

#### 3.2.2 Indicator set

The following World Bank indicators must be fetched. This list is intentionally narrower than the MVP's, as several MVP indicators were only intermediate values used to compute derived indicators.

| WB Indicator ID | Canonical name | Notes |
|---|---|---|
| `NY.GDP.MKTP.CD` | GDP (current USD) | Stored as share of global GDP (see §3.2.4) |
| `NY.GDP.PCAP.CD` | GDP per capita (current USD) | Raw value; null-filled from `GDP / population` |
| `SP.POP.TOTL` | Total population | Stored as raw value |
| `NE.IMP.GNFS.ZS` | Imports of goods and services (% of GDP) | Used to derive share of global trade; dropped from final table |
| `NE.EXP.GNFS.ZS` | Exports of goods and services (% of GDP) | Used to derive share of global trade; dropped from final table |
| `BX.KLT.DINV.WD.GD.ZS` | FDI net inflows (% of GDP) | Stored as raw value |

The indicator list should be defined as a named constant in the module (or in `core/endpoints.py` if it needs to be shared), not hardcoded inline.

#### 3.2.3 Fetch window

The fetch must cover from 5 years before the earliest year in `bronze.prices` through to the current calendar year. This 5-year lookback mirrors the MVP's approach and ensures that YoY growth rate computation (which requires multiple prior years) is possible for the oldest products in the universe.

Fetching should be done in chunks of ≤ 40 country codes per API call (World Bank API limitation). Failed chunks should be logged and skipped without aborting the overall run.

#### 3.2.4 Derived indicators and transformations

Before storing into `bronze.country_indicators`, the following transformations must be applied annually (i.e. per year, not per product):

1. **Share of global trade**: For each year, compute `(imports_pct_gdp + exports_pct_gdp)` for each country, sum across all countries in the universe to get total trade, then divide each country's total by the universe total. Store the result as a new indicator `SH.TRD.VOL` (share of global trade volume). The raw import and export indicators are not stored.

2. **Share of global GDP**: For each year, divide each country's GDP (`NY.GDP.MKTP.CD`) by the sum of GDP across all countries in the universe. Store this as the transformed value for `NY.GDP.MKTP.CD`. (This converts an absolute dollar figure into a relative share, making it comparable across years and portfolios.)

3. **GDP per capita gap-fill**: If `NY.GDP.PCAP.CD` is null for a given country-year, compute it as `NY.GDP.MKTP.CD (raw, pre-transformation) / SP.POP.TOTL`. Apply this before the GDP normalisation step.

4. **Temporal interpolation and extrapolation**: World Bank data has structural annual gaps and missing recent years. For each indicator and country, apply Akima spline interpolation across the year axis to fill interior gaps. For exterior gaps (before the first observed year or after the last observed year), use a linear regression fit on the 3 nearest observed years to extrapolate. Any remaining nulls after these two steps should be filled with the cross-country mean for that indicator-year.

These transformations are applied at ingest time before writing to `bronze.country_indicators`, so that `prep/panel.py` can read clean, ready-to-use values without any additional computation.

#### 3.2.5 Freshness and incremental updates

Because World Bank data is annual, a full refetch should be triggered no more than once per day. The `bronze.country_indicators_status` table tracks the last successful fetch timestamp per indicator. The module's `sync()` entry point should accept a `force: bool` parameter, following the pattern of `fx.sync()`.

#### 3.2.6 Integration into `ingest/pipeline.py`

A new phase must be inserted into the `_run_full` pipeline in `ingest/pipeline.py`, after Phase 8 (data cleaning), since `country_indicators` requires `silver.observations` to be populated in order to resolve the country code universe. This is the only case where an ingest phase reads from the silver layer; it is analogous to how `fx.py` reads from `bronze.contracts` rather than from raw IBKR payloads. It must also be exposed as a standalone `Ingest.country_indicators()` method on the `cli` object.

> [!NOTE]
> An alternative placement would be before the details phase (Phase 7), using country codes derived directly from raw `bronze.snapshots` payloads rather than `silver.observations`. This would avoid reading from the silver layer in `ingest/`, but it requires duplicating country code extraction logic that already exists in `prep/extractors.py`. Given that the project rules prohibit cross-package imports, and that `silver.observations` is a stable read-model, reading from it in `ingest/country_indicators.py` is the simpler and preferred approach.

### 3.3 Changes to `prep/panel.py`: country indicator collapse step

After the existing Step 4 (theme aggregation and clean observations assembly) and **before** Step 5 (global metric low-count pruning), a new **country indicator collapse** step must be inserted. This step reads from `bronze.country_indicators` and replaces the raw per-country weight observations in `clean_observations` with a small set of portfolio-weighted macro scalar features.

#### 3.3.1 What the collapse produces

For each ETF product, for each panel month, and for each retained indicator, the collapse computes two values:

1. **Level** (`_stat`): the portfolio-weighted average of the indicator's current level across all countries held.
2. **Growth rate** (`_growth_rate`): the portfolio-weighted average of the indicator's year-over-year growth rate across all countries held.

Growth rate for indicator `I` in country `c` at year `y` is defined as the 3-year rolling mean of annual changes: `mean(I[c,y] - I[c,y-1], I[c,y-1] - I[c,y-2], I[c,y-2] - I[c,y-3]) / I[c,y-3]`. This is applied as a post-query computation at panel build time using `bronze.country_indicators` values, not pre-stored.

The output features are named `country_{indicator_slug}_{suffix}` where `indicator_slug` is a fixed mapping from WB indicator code to a short name, and `suffix` is `stat` or `growth_rate`. For example:
- `country_gdp_stat`
- `country_gdp_growth_rate`
- `country_gdp_pcap_stat`
- `country_gdp_pcap_growth_rate`
- `country_population_stat`
- `country_population_growth_rate`
- `country_fdi_stat`
- `country_fdi_growth_rate`
- `country_share_trade_volume_stat`
- `country_share_trade_volume_growth_rate`

These are scalar features (one value per product-month) and belong to the `profile` family in the panel, or to a new dedicated `country_macro` family. The family choice determines whether they are LOCF'd as scalars (with gap-cap) or treated as default-0 compositional features; as scalars they should follow the scalar LOCF path in section 7–8 of `panel.py`.

#### 3.3.2 Panel-date to indicator-year alignment

World Bank data is annual. For a panel month `as_of_date`, the indicator value used is the most recently completed calendar year: `year = as_of_date.year - 1` if `as_of_date.month < 6`, else `year = as_of_date.year`. This rule prevents look-ahead bias (annual data for year Y is typically published mid-way through Y+1).

#### 3.3.3 Handling missing country codes

Country observations in `clean_observations` with `code IS NULL` cannot be mapped to an indicator value. They should be treated as zero weight — i.e., excluded from both the numerator and denominator of the weighted average. This means the collapsed indicator value reflects only the share of the portfolio whose country code is known and has WB data.

#### 3.3.4 Handling countries not covered by the World Bank API

Some codes in `silver.observations` are not recognised by the World Bank (e.g. `GG`, `JE`, `KY`, `BM`). These are typically offshore financial centres and non-sovereign territories. They must be treated the same as null codes: excluded from the weighted sum but counted in the total portfolio weight for normalisation purposes.

> [!IMPORTANT]
> The effective coverage of the collapse for a given product-month is the sum of weights for countries that *do* have WB data. This coverage fraction should be stored as an additional feature: `country_macro_coverage` (analogous to `profile_esg_coverage` and `profile_theme_coverage`). Products where macro coverage is very low (e.g. < 50%) will produce collapse features of limited interpretive value.

#### 3.3.5 Disposition of raw country weight observations

The raw `country_*` weight features (one per country) must **still be built and survive** the low-count pruning if they have sufficient observations. The country indicator collapse features are **additional** features, not replacements. The geographic weight breakdown retains value as a standalone set of features (e.g. for a `country_united_states_stat` feature showing US portfolio weight). The collapse adds a separate set of macro-economic derived features on top.

This means the collapse step writes its output into `clean_observations` as new rows under the designated family, alongside the existing country weight rows, before pruning runs.

---

## 4. Non-Goals

- **`coco.CountryConverter()`**: Not required. ISO alpha-2 codes are already available in `silver.observations.code`. The small number of countries whose codes are absent from the raw IBKR payload are handled by keeping `COUNTRY_CODE_REMAPS` in `core/endpoints.py` up to date.
- **Additional WB indicators**: The indicator set in §3.2.2 covers the economically relevant indicators from the MVP. Commented-out MVP indicators (trade surplus, services/goods ratio) are explicitly excluded.
- **Second derivative** (`_acceleration`): The MVP had a commented-out second derivative feature. It is not included here.
- **Intra-year WB data updates**: World Bank data refreshes annually. There is no requirement to track intra-year revisions or restatements; a simple overwrite on refetch is sufficient (no `cold_storage` archive needed).

---

## 5. New Files Summary

| File | Purpose |
|---|---|
| `etfportfolio/ingest/country_indicators.py` | World Bank API fetch, transform, and store into `bronze.country_indicators` |
| `tests/ingest/test_country_indicators.py` | Unit tests for the above |

### Modified files

| File | Change |
|---|---|
| `etfportfolio/core/schema.sql` | Add `bronze.country_indicators` and `bronze.country_indicators_status` table definitions |
| `etfportfolio/core/endpoints.py` | Add `WB_INDICATOR_MAP` constant mapping WB indicator codes to slugified names |
| `etfportfolio/ingest/pipeline.py` | Add Phase 9 calling `country_indicators.sync()`; add `Ingest.country_indicators()` method |
| `etfportfolio/prep/panel.py` | Add country indicator collapse step between theme aggregation (Step 4) and low-count pruning (Step 5) |
| `tests/prep/test_panel.py` | Add tests for the collapse step |

---

## 6. Open Questions

1. **Family for collapse features**: Should the collapsed indicator features be added to the `profile` family (making them LOCF scalars) or to a dedicated `country_macro` family? Using `profile` is the path of least resistance since the scalar LOCF machinery already handles it. A dedicated family would be more semantically precise and easier to filter/inspect.

2. **Cross-country normalisation domain**: The global GDP share and global trade share computations in §3.2.4 are currently defined over "all countries in the universe" (i.e. all countries present in `silver.observations`). Should they instead be normalised over all World Bank economies? Using a consistent global denominator (all WB economies) makes the indicator values comparable over time; using only the universe countries makes them relative to the covered subset.

3. **Collapse step timing in the pipeline**: The proposed position (after theme aggregation, before low-count pruning) means the collapse output rows are subject to the same low-count pruning as all other metrics. This is desirable — if only one product has any country data, the macro indicators would be pruned. Confirm this behaviour is intentional.
