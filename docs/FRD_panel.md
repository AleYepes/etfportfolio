# Document 2: Silver Preprocessing & Factor Panel Alignment

**Status:** Approved for Implementation  
**Scope:** `etfportfolio/prep/`, `tests/prep/`  
**Target Modules:**
- `etfportfolio/prep/utils.py` (strict currency disambiguation)
- `etfportfolio/prep/panel.py` (dynamic p99 cadence, bounded backward fill, monthly FX aggregation, USD AUM)
- `etfportfolio/prep/pipeline.py` (observation extraction pipeline)
- `tests/prep/test_utils.py` (updated)
- `tests/prep/test_panel.py` (updated)

---

## 1. Executive Summary & Empirical Motivations

An empirical audit of `data/silver_catalog.sqlite` against `etfportfolio/prep/panel.py` identified four critical methodological defects in panel construction:
1. **Severe ESG Under-coverage**: `ESG_CAP_DAYS = 90` drops **95.6%** of eligible LOCF months because vendor updates occur weekly during active windows but experience ~4-year hiatuses (p95 = p99 = 1 470 days).
2. **First-Observation Reporting Lags**: 97.8%–100% of ETFs trade on exchanges for years before fundamental data is first indexed (median gap 1 400–1 800 days), leaving early trading histories completely vacant of factor exposures.
3. **AUM Currency Incoherence & Heuristic Contamination**: `total_net_assets_local` spans 26 distinct currencies without USD standardization. Furthermore, `disambiguate_aum_currency()` in `prep/utils.py` was defaulting to USD or guessing CAD whenever `$` was present, misclassifying thousands of EUR-, GBP-, and MXN-denominated funds.
4. **Rigid Staleness Caps**: Hardcoded LOCF caps failed to match empirical arrival cadences across distinct metric and dimension families.

---

## 2. Detailed Technical Requirements

### 2.1. Strict Contract-Centric AUM Currency Disambiguation (`prep/utils.py`)

#### Problem Analysis
In vendor fundamental snapshots, `$` is frequently used on European or Latin American fund pages, or local funds report parent fund AUM in USD while trading in EUR/GBP. Previously, `disambiguate_aum_currency` assumed `$` was USD (or guessed CAD if the exchange was Canadian), mislabeling over 6 000 products.

#### Specification
Refactor `disambiguate_aum_currency(raw_value, product_currency, listing_exchange, country)`:
1. **Explicit 3-Letter ISO Code**: If `raw_value` begins with an ISO prefix (e.g. `CAD 50M`, `AUD 1.2B`), return that code.
2. **Strict Contract Matching for Ambiguous Symbols**:
   Define candidate currencies per ambiguous symbol:
   ```python
   AMBIGUOUS_CURRENCY_SYMBOLS: dict[str, frozenset[str]] = {
       "$": frozenset({"USD", "CAD", "AUD", "MXN", "SGD", "HKD", "NZD", "TWD"}),
       "¥": frozenset({"JPY", "CNY", "CNH"}),
       "£": frozenset({"GBP", "EGP", "LBP"}),
       "₩": frozenset({"KRW", "KPW"}),
       "€": frozenset({"EUR"}),
       "₹": frozenset({"INR"}),
   }
   ```
   - If the raw string begins with symbol $S$:
     - Check `contract_currency = (product_currency or "").strip().upper()`.
     - **If `contract_currency in AMBIGUOUS_CURRENCY_SYMBOLS[S]`**: return `contract_currency`.
     - **If it does not match**: return `None`.
3. **Clean Fallback**:
   - If no symbol or prefix matches, and `contract_currency` is a valid 3-letter alphabetic string, return `contract_currency`.
   - Otherwise, return `None`.
4. **Zero-Guessing Invariant**: **Never default to `"USD"`**. If currency cannot be verified against the broker contract, return `None`. Products with unverified currencies will have `currency = NULL` in `silver.product_metrics` and will be excluded from downstream USD AUM calculations.

---

### 2.2. Dynamic p99 Cadence Engine with Empirical Fallbacks (`prep/panel.py`)

#### Specification
Staleness caps must be calculated dynamically per family inside DuckDB during `run_panel()`:

1. **Family Partitioning (`source_updated`)**:
   Align the family partitioning across all observations in `_flatten_observations`:
   - For `silver.product_metrics`: `family = m.source` (e.g. `esg, holdings, lipper, mstar, profile, ratios, theme_weights`).
   - For `silver.product_dimensions`: `family = d.dimension_type` (e.g. `asset_class, country, credit_rating, debt_type, industry, maturity, style_box, style_box_hist, theme`).
2. **Empirical Static Baseline Table**:
   Register a static fallback table derived from the catalog audit:
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
3. **Dynamic Computation Query**:
   Compute empirical inter-arrival transitions:
   ```sql
   WITH transitions AS (
       SELECT
           source AS family,
           product_id,
           effective_date,
           date_diff('day', LAG(effective_date) OVER (
               PARTITION BY product_id, source ORDER BY effective_date
           ), effective_date) AS gap_days
       FROM (SELECT DISTINCT product_id, source, effective_date FROM silver.product_metrics)
       UNION ALL
       SELECT
           dimension_type AS family,
           product_id,
           effective_date,
           date_diff('day', LAG(effective_date) OVER (
               PARTITION BY product_id, dimension_type ORDER BY effective_date
           ), effective_date) AS gap_days
       FROM (SELECT DISTINCT product_id, dimension_type, effective_date FROM silver.product_dimensions)
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
4. **Perpetual Exemption**:
   In `_flatten_observations`, `is_passive` explicitly retains `cap_days = NULL` (infinite carry-forward).

---

### 2.3. Bounded Backward-Fill from First Observation (`prep/panel.py`)

#### Specification
Extend `_apply_locf` in `prep/panel.py` to fill pre-inception reporting gaps:
1. **First-Price Floor**:
   Query each product's first trading date from `bronze.prices`:
   ```sql
   CREATE TEMP TABLE first_prices AS
   SELECT product_id, MIN(date::DATE) AS first_price_date
   FROM bronze.prices
   GROUP BY product_id;
   ```
2. **Earliest Observation Isolation (`rn = 1`)**:
   Isolate the single earliest observation per fund and family:
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
3. **Spine Extension (Forward LOCF + Backward Fill)**:
   In `locf_family`:
   - **Forward LOCF Branch**: Match spine months where $s.\text{as\_of\_date} \ge fd.\text{effective\_date}$ and $(s.\text{as\_of\_date} - fd.\text{effective\_date}) \le fd.\text{cap\_days}$.
   - **Backward Fill Branch**: Match spine months where $s.\text{as\_of\_date} < fo.\text{effective\_date}$, subject to:
     - `date_diff('day', s.as_of_date, fo.effective_date) <= fo.cap_days` (bounded by family p99 cap).
     - `s.as_of_date >= fp.first_price_date` (strictly prevented from filling prior to exchange trading).
     - Joins strictly on `first_family_obs` (`rn = 1`), guaranteeing zero lookahead leakage from subsequent observations.
4. **Deduplication**:
   Deduplicate forward and backward candidates with `QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, as_of_date, family ORDER BY date_diff('day', effective_date, as_of_date) ASC) = 1`.

---

### 2.4. Monthly Aggregated FX Normalization for AUM (`prep/panel.py`)

#### Problem & Rationale
Day-to-day foreign exchange fluctuations add unnecessary noise to fund size sorting and cap-weighted factor portfolio construction. Normalization must occur at panel frequency using the monthly mean rate of the respective month.

#### Specification
1. **Monthly Mean Exchange Rate Aggregate**:
   Compute the calendar month-end average exchange rate for all qualified CASH pairs:
   ```sql
   CREATE TEMP TABLE monthly_fx AS
   SELECT
       c.symbol AS currency,
       LAST_DAY(f.date::DATE) AS as_of_date,
       AVG(f.close) AS rate_to_usd
   FROM bronze.fx_rates f
   JOIN bronze.contracts c ON c.product_id = f.product_id
   WHERE c.sec_type = 'CASH' AND c.currency = 'USD'
   GROUP BY c.symbol, LAST_DAY(f.date::DATE);
   ```
2. **Panel Synthesis & Conversion**:
   In `_synthesize`, convert `total_net_assets_local` to `total_net_assets_usd`:
   ```sql
   SELECT
       m.product_id,
       m.as_of_date,
       'total_net_assets_usd' AS feature_id,
       m.value * CASE
           WHEN m.currency = 'USD' THEN 1.0
           ELSE fx.rate_to_usd
       END AS value
   FROM panel_locf m
   LEFT JOIN monthly_fx fx
       ON fx.currency = m.currency
      AND fx.as_of_date = m.as_of_date
   WHERE m.feature_id = 'total_net_assets_local'
     AND (m.currency = 'USD' OR fx.rate_to_usd IS NOT NULL);
   ```
3. **Total Feature Replacement**:
   `total_net_assets_local` is **completely replaced** by `total_net_assets_usd` in `silver.monthly_panel`. Rows where the currency is unverified (`NULL`) or where no matching monthly FX rate exists are omitted.

---

## 3. Operational Migration Sequence

1. Execute schema updates (`bronze.fx_rates`, `bronze.fx_status`, `cold_storage.fx_rates`).
2. Run ingestion to populate FX rates:
   ```bash
   uv run main.py ingest contracts
   uv run main.py ingest fx
   ```
3. Re-extract Silver observations with the strict currency disambiguator and rebuild the panel:
   ```bash
   uv run main.py prep --force
   ```
4. Verify catalog scorecards via `scripts/catalog_silver.py`:
   - All 8 structural invariants pass.
   - `total_net_assets_usd` is present; `total_net_assets_local` is absent.
   - ESG coverage increases from ~4.4% to ~70.6%.

---

## 4. Test Suite Requirements

Update and expand tests in `tests/prep/`:
1. **`tests/prep/test_utils.py`**:
   - `test_disambiguate_aum_currency_matching_contract`: `"$ 1.2B"` with `product_currency='CAD'` returns `'CAD'`.
   - `test_disambiguate_aum_currency_mismatched_symbol`: `"$ 1.2B"` with `product_currency='EUR'` returns `None`.
   - `test_disambiguate_aum_currency_yen_cny`: `"¥ 500M"` with `product_currency='CNY'` returns `'CNY'`; with `'JPY'` returns `'JPY'`.
   - `test_disambiguate_aum_currency_no_usd_fallback`: Unknown symbol with no clean contract currency returns `None`.
2. **`tests/prep/test_panel.py`**:
   - `test_dynamic_cadence_fallback`: Database with <10 transitions defaults to empirical baseline values (e.g. 1 470d for ESG).
   - `test_backward_fill_bounded_by_price_floor`: Fund with first trading date on 2024-01-31 and first fundamental on 2025-06-30 receives backward-filled factor exposures from 2024-01-31 through 2025-05-31, but **zero** exposure in 2023.
   - `test_backward_fill_isolates_earliest_obs`: Multiple fundamental updates across history only backward-fill the value of `rn = 1`.
   - `test_total_net_assets_usd_monthly_mean`: Local EUR AUM is multiplied by the January average of EUR.USD daily closes, producing `total_net_assets_usd`. Verify `total_net_assets_local` does not appear in the panel.