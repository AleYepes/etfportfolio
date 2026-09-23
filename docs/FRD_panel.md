# Document 2: FRD — Phase 2: Factor Panel Alignment, Dynamic Cadence & FX Standardization

**Target Components:**
- `etfportfolio/prep/panel.py`

**Test Components:**
- `tests/prep/test_panel.py`

---

## 1. Executive Summary & Problem Rationale

Following the unification of the Silver layer in Phase 1, all fundamental observations reside in `silver.observations(product_id, family, metric, code, effective_date, effective_date_source, fetched_at, value, raw_value)`.

However, the empirical audit in `scripts/catalog_silver.py` surfaced four critical methodological defects in the factor panel generation process (`prep/panel.py`):
1. **Severe ESG Under-Coverage**: A hardcoded 90-day LOCF cap dropped **95.6%** of eligible ETF months because vendor ESG updates experience multi-year hiatuses between universe overhauls ($p_{95} = p_{99} = 1,470\text{ days}$).
2. **First-Observation Reporting Lags**: 97.8%–100% of ETFs trade on exchanges for years before fundamental data is first indexed (median latency 1,400–1,800 days, with maximum gaps exceeding 10,000 days), leaving early trading histories completely vacant of factor exposures.
3. **Currency Incoherence**: `total_net_assets_local` spans 26 distinct currencies without USD standardization. Cross-sectional factor sorting and cap-weighted factor portfolios cannot mix local nominal currencies.
4. **Rigid Staleness Caps**: Hardcoded staleness caps (e.g. 180d, 540d) fail to match empirical inter-arrival cadences across distinct families.

### Phase 2 Objectives
- Expand month-end spine generation to the absolute inception of exchange pricing and apply product-specific price floors (`as_of_date >= first_price_date`).
- Replace hardcoded staleness caps with an empirical dynamic $p_{99}$ inter-arrival cadence engine with catalog-derived static fallbacks.
- Implement bounded backward-fill from each product's earliest fundamental observation down to its price inception date.
- Standardize local AUM to `total_net_assets_usd` using calendar-month average exchange rates from `bronze.fx`.
- Completely replace `total_net_assets_local` with `total_net_assets_usd` in `silver.monthly_panel`.

---

## 2. Detailed Technical Requirements

### 2.1 Price-Aware Month-End Spine & Price Floor (`etfportfolio/prep/panel.py`)

#### Problem Analysis
In the legacy implementation, `_load_month_ends` bounded the spine by `SELECT MIN(d), MAX(d)` across Silver observation dates. Because fundamental reporting begins years after exchange trading inception, historical spine months prior to the first fundamental observation did not exist in `month_ends`, preventing backward-fill from populating early trading history.

#### Specification
1. **Determine Global Bounds**:
   Query the minimum date across both price history and Silver observations:
   ```sql
   WITH price_bounds AS (
       SELECT MIN(date::DATE) AS min_price_date FROM bronze.prices
   ),
   obs_bounds AS (
       SELECT MIN(effective_date) AS min_eff_date, MAX(effective_date) AS max_eff_date
       FROM silver.observations
   )
   SELECT
       LEAST(COALESCE(p.min_price_date, o.min_eff_date), o.min_eff_date) AS global_start_date,
       o.max_eff_date AS global_end_date
   FROM obs_bounds o
   LEFT JOIN price_bounds p ON 1=1;
   ```
2. **Month-End Spine Generation**:
   Construct `month_ends` starting at `global_start_date` and extending through `global_end_date + INTERVAL 540 DAY`.
3. **Per-Product Price Floor Isolation**:
   Query each product's first trading date from `bronze.prices`:
   ```sql
   CREATE TEMP TABLE first_prices AS
   SELECT product_id, MIN(date::DATE) AS first_price_date
   FROM bronze.prices
   GROUP BY product_id;
   ```
4. **Candidate Product Spine**:
   Form the product spine strictly bounded by trading inception:
   ```sql
   CREATE TEMP TABLE spine AS
   SELECT p.product_id, m.as_of_date
   FROM (SELECT DISTINCT product_id FROM silver.observations) p
   INNER JOIN first_prices fp ON fp.product_id = p.product_id
   CROSS JOIN month_ends m
   WHERE m.as_of_date >= fp.first_price_date;
   ```

---

### 2.2 Dynamic $p_{99}$ Cadence Engine with Empirical Fallbacks (`etfportfolio/prep/panel.py`)

#### Specification
Staleness caps are calculated dynamically per family inside DuckDB during `run_panel()`:

1. **Empirical Static Baseline Table**:
   Register static fallback caps derived from the catalog audit:
   ```sql
   CREATE TEMP TABLE cadence_fallbacks (
       family VARCHAR PRIMARY KEY,
       fallback_cap_days INTEGER NOT NULL
   );
   INSERT INTO cadence_fallbacks VALUES
       ('asset_class', 31),
       ('country', 31),
       ('credit_rating', 34),
       ('debt_type', 34),
       ('industry', 31),
       ('maturity', 34),
       ('style_box', 9),
       ('style_box_hist', 16),
       ('theme', 16),
       ('esg', 1470),
       ('holdings', 31),
       ('lipper', 31),
       ('mstar', 297),
       ('profile', 365),
       ('ratios', 62),
       ('theme_weights', 16);
   ```

2. **Dynamic Computation Query**:
   Compute empirical inter-arrival transitions directly from `silver.observations`:
   ```sql
   WITH transitions AS (
       SELECT
           family,
           product_id,
           effective_date,
           date_diff('day', LAG(effective_date) OVER (
               PARTITION BY product_id, family ORDER BY effective_date
           ), effective_date) AS gap_days
       FROM (SELECT DISTINCT product_id, family, effective_date FROM silver.observations)
   ),
   family_p99 AS (
       SELECT
           family,
           COUNT(*) AS n_transitions,
           quantile_cont(gap_days, 0.99)::INTEGER AS dyn_cap
       FROM transitions
       WHERE gap_days IS NOT NULL AND gap_days > 0
       GROUP BY family
   )
   CREATE TEMP TABLE active_family_caps AS
   SELECT
       fb.family,
       CASE
           WHEN p.n_transitions >= 10 AND p.dyn_cap IS NOT NULL THEN p.dyn_cap
           ELSE fb.fallback_cap_days
       END AS cap_days
   FROM cadence_fallbacks fb
   LEFT JOIN family_p99 p ON fb.family = p.family;
   ```

3. **Perpetual Exemption**:
   In `_flatten_observations`, `metric = 'is_passive'` retains `cap_days = NULL` (infinite carry-forward).

---

### 2.3 Bounded Backward-Fill from Earliest Observation (`etfportfolio/prep/panel.py`)

#### Specification
Extend `_apply_locf` to bridge early inception reporting lags:

1. **Earliest Family Observation Isolation (`rn = 1`)**:
   Isolate the single earliest observation per product and family:
   ```sql
   CREATE TEMP TABLE first_family_obs AS
   SELECT product_id, family, effective_date, fetched_at, cap_days
   FROM (
       SELECT
           product_id, family, effective_date, fetched_at, cap_days,
           ROW_NUMBER() OVER (
               PARTITION BY product_id, family
               ORDER BY effective_date ASC, fetched_at DESC
           ) AS rn
       FROM obs
   )
   WHERE rn = 1;
   ```

2. **Spine Alignment (Forward LOCF + Backward Fill)**:
   In `locf_family`:
   - **Forward LOCF Branch**: Match spine months where:
     $$s.\text{as\_of\_date} \ge fd.\text{effective\_date} \quad \text{AND} \quad \left(fd.\text{cap\_days IS NULL} \lor (s.\text{as\_of\_date} - fd.\text{effective\_date}) \le fd.\text{cap\_days}\right)$$
   - **Backward Fill Branch**: Match spine months where $s.\text{as\_of\_date} < fo.\text{effective\_date}$, subject to:
     - `date_diff('day', s.as_of_date, fo.effective_date) <= fo.cap_days` (bounded by active family $p_{99}$ cap).
     - `s.as_of_date >= fp.first_price_date` (price floor strictly respected).
     - Joins strictly on `first_family_obs` (`rn = 1`), preventing lookahead leakage from subsequent observations.

3. **Deduplication**:
   Combine candidates from both branches and deduplicate:
   ```sql
   QUALIFY ROW_NUMBER() OVER (
       PARTITION BY s.product_id, s.as_of_date, family
       ORDER BY abs(date_diff('day', effective_date, s.as_of_date)) ASC, fetched_at DESC
   ) = 1
   ```

---

### 2.4 Monthly Aggregated FX Normalization for AUM (`etfportfolio/prep/panel.py`)

#### Context
`total_net_assets_local` is reported across 26 distinct currencies. Without USD standardization, cross-sectional factor sorting and cap-weighted factor portfolio derivation are invalid. Normalization occurs post-LOCF along the month-end spine using calendar-month average exchange rates from `bronze.fx` (`target_currency = 'USD'`), preventing day-to-day FX fluctuations from injecting spurious noise into fund size rankings.

#### Specification
1. **Calendar-Month Average FX Rates**:
   Compute monthly mean conversion rates directly from `bronze.fx`:
   ```sql
   CREATE TEMP TABLE monthly_fx AS
   SELECT
       source_currency AS currency,
       LAST_DAY(date::DATE) AS as_of_date,
       AVG(close) AS rate_to_usd
   FROM bronze.fx
   WHERE target_currency = 'USD'
   GROUP BY source_currency, LAST_DAY(date::DATE);
   ```

2. **Post-LOCF Currency Standardization**:
   In `_synthesize`, standardize local AUM to USD along the month-end spine:
   ```sql
   SELECT
       m.product_id,
       m.as_of_date,
       'total_net_assets_usd' AS feature_id,
       m.value * CASE
           WHEN m.code = 'USD' THEN 1.0
           ELSE fx.rate_to_usd
       END AS value
   FROM panel_locf m
   LEFT JOIN monthly_fx fx
       ON fx.currency = m.code
      AND fx.as_of_date = m.as_of_date
   WHERE m.feature_id = 'total_net_assets_local'
     AND (m.code = 'USD' OR fx.rate_to_usd IS NOT NULL);
   ```

3. **Total Feature Replacement**:
   In `silver.monthly_panel`, `total_net_assets_local` is **completely replaced** by `total_net_assets_usd`. Local AUM observations with unverified currencies (`code IS NULL`) or missing monthly FX rates are omitted from the panel.

---

### 2.5 Invariant & Quality Gate Preservation (`etfportfolio/prep/panel.py`)

All existing panel synthesis logic and invariant checks must be preserved:
1. **Theme Coverage Gate**: Filter thematic weights where `theme_coverage < 0.70`.
2. **Winsorization**: Apply cross-sectional 1st/99th percentile winsorization across profitability and leverage ratios, and clamp expense ratios to `[-1.0, 2.0]`.
3. **Dominant Asset Class Assignment**: Assign each product-month its dominant asset class (`Equity`, `Fixed Income`, `Cash`, `Other`).
4. **Partition of Unity Assertion**: Assert that sleeve weights sum to $1.0 \pm 0.0001$.

---

## 3. Operational Migration Sequence

1. Verify that `bronze.fx` contains daily quotes for all portfolio contract currencies:
   ```bash
   uv run main.py ingest fx
   ```
2. Update `prep/panel.py` with:
   - Price-aware month-end spine logic.
   - Dynamic $p_{99}$ cadence engine and static fallbacks.
   - Bounded backward-fill from earliest observations.
   - Post-LOCF calendar-month FX conversion.
3. Rebuild `silver.monthly_panel`:
   ```bash
   uv run main.py prep
   ```
4. Validate with `scripts/catalog_silver.py`:
   - `silver.monthly_panel` contains `total_net_assets_usd` and 0 instances of `total_net_assets_local`.
   - ESG coverage increases from ~4.4% to ~70.6%.
   - Invariants pass with 0 errors.

---

## 4. Test Suite Requirements

In `tests/prep/test_panel.py`:

1. **Dynamic Cadence Fallback**:
   - Verify that test environments with <10 transitions default to `cadence_fallbacks` (e.g. 1,470d for ESG).
2. **Backward-Fill Bounded by First Price**:
   - Mock a fund with `first_price_date = 2024-01-31` and first ESG observation at `2025-06-30`.
   - Verify that months from `2024-01-31` through `2025-05-31` receive backward-filled ESG values.
   - Verify that months prior to `2024-01-31` receive **no** ESG exposure (price inception floor strictly respected).
3. **FX Normalization**:
   - Mock a GBP fund reporting £10,000,000 AUM and a CAD fund reporting $20,000,000 AUM.
   - Mock monthly FX rates in `bronze.fx` (`GBP->USD = 1.30`, `CAD->USD = 0.75`).
   - Verify `total_net_assets_usd` reflects the monthly average exchange rate conversion ($13,000,000 and $15,000,000).
   - Verify `total_net_assets_local` is omitted from `silver.monthly_panel`.