# Comprehensive Review & Audit Critique: FRD Phase 1 Silver Layer Architecture

**Target Document:** `FRD_architecture.md` (Phase 1 Settled Design)  
**Empirical Baseline:** `silver_catalog.log` (Audit of 9,977,797 Silver rows across 20,435 priced funds / 22,737 contracts in `data/etf.duckdb`)  
**Schema Context:** Pre-FRD `schema.sql`  

---

## Executive Summary

The architectural direction of the FRD—unifying the isomorphic `silver.product_metrics` and `silver.product_dimensions` tables into a single `silver.observations` EAV table, establishing a clean package boundary in `etfportfolio/core/endpoints.py`, and replacing dual DTOs with `Observation`—is **empirically validated** by the catalog audit:
- The unified primary key `(product_id, family, metric, effective_date)` produces **exactly 0 collisions** across all ~10 million legacy rows.
- Data integrity invariants (temporal causality, foreign key resolution to `bronze.contracts`, provenance enumerations) passed at 100%.

However, the audit reveals **two ingestion-breaking P0 bugs**, **two architectural/data-loss P1 issues**, and several domain realities (shorts, fee subsidies, simplex failures) that require explicit specification fixes in the FRD before implementation.

---

## 1. Critical Ingestion-Breaking Flaws (P0)

Under FRD Decision **D9**, extractors must use closed `StrEnum`s and **raise a `ValueError`** on any payload tag not present in the enum. If implemented as currently written, the pipeline will crash immediately on live data due to missing members in two core enums.

### 1.1 `RatiosMetric` Enum Omissions (17 Missing Tags)
* **FRD §5.1 Gap:** The FRD lists only 24 percentage metrics and mentions 6 leverage metrics (30 total).
* **Empirical Reality (§10.2 of log):** The database contains **47 distinct ratio metrics** across 817,680 observations. Under the current FRD, 17 extracted ratio metrics will trigger a runtime `ValueError`.
* **Missing Tags to Add to `RatiosMetric`:**
  1. **Standard Valuation Multiples (Standard floats; DO NOT divide by 100):**
     * `price_earnings` (19,242 obs)
     * `price_book` (19,227 obs)
     * `price_sales` (19,170 obs)
     * `price_cash` (19,159 obs)
     * `price_to_dividend` (19,100 obs)
  2. **Cross-Sectional Composite Z-Scores (Standard floats; do NOT divide by 100):**
     * `average_final_composite_zscore` (16,487 obs)
     * `latest_composite_z_score` (16,487 obs)
     * `latest_dividend_yield_zscore` (16,487 obs)
     * `latest_price_sales_zscore` (16,483 obs)
     * `latest_price_to_book_zscore` (16,487 obs)
     * `latest_price_to_earnings_zscore` (16,485 obs)
     * `latest_return_on_equity_zscore` (16,487 obs)
     * `latest_sps_growth_zscore` (16,487 obs)
     * `weighted_final_composite_zscore` (16,487 obs)
  3. **Fixed Income & Quality Metrics (Standard floats; do NOT divide by 100):**
     * `average_quality` (7,032 obs)
     * `effective_maturity` (8,031 obs)
     * `nominal_maturity` (8,031 obs)
* **Actionable FRD Fix:** Expand `RatiosMetric` from 30 to **47 members**. Explicitly maintain `RatiosMetric.PERCENTAGE` containing strictly the 24 unit-interval metrics (`eps_growth_*`, `sales_growth_*`, `return_on_*`, `yield_to_maturity`, `average_coupon`, etc.).

---

### 1.2 `IndustryMetric` Enum Drift & Vendor Taxonomy
* **FRD §5.1 Gap:** The FRD references standard GICS sectors, mentioning overrides in `panel.py` and discontinued telecom. It omits the exact closed set.
* **Empirical Reality (§10.3 of log):** Holdings industry exposures do **not** follow GICS; they follow the Refinitiv/Lipper sector taxonomy. There are **14 distinct categories** across 151,723 observations.
* **Critical Missing Value:**
  * `Academic & Educational Services` (447 obs across 277 funds). Without this, any fund holding education stocks will fail ingestion.
* **Actionable FRD Fix:** Lock `IndustryMetric` to the exact 14 vendor values:
  ```python
  class IndustryMetric(StrEnum):
      ACADEMIC_EDUCATIONAL_SERVICES = "Academic & Educational Services"
      BASIC_MATERIALS = "Basic Materials"
      COMMUNICATION_SERVICES = "Communication Services"
      CONSUMER_CYCLICALS = "Consumer Cyclicals"
      CONSUMER_NON_CYCLICALS = "Consumer Non-Cyclicals"
      ENERGY = "Energy"
      FINANCIALS = "Financials"
      HEALTHCARE = "Healthcare"
      INDUSTRIALS = "Industrials"
      NON_CLASSIFIED_EQUITY = "Non Classified Equity"
      NOT_CLASSIFIED_NON_EQUITY = "Not Classified - Non Equity"
      REAL_ESTATE = "Real Estate"
      TECHNOLOGY = "Technology"
      UTILITIES = "Utilities"
  ```
  *(Note: Apply the existing remap `Telecommunication Services-Discontinued eff 09/19/2020` → `Communication Services` before enum lookup).*

---

## 2. High-Impact Architectural & Modeling Issues (P1)

### 2.1 AUM Disambiguation Drops 19.8% of Fund AUM (FRD D11 & §6.1)
* **The Rule:** FRD §6.1 matches currency glyphs (`$`, `€`, `£`, `¥`) to a fixed set of candidate currencies, and checks whether `bronze.contracts.currency` is in that candidate set. If unresolved, FRD D11 commands: *"skip the AUM observation (do not persist `code=NULL`)"*.
* **Empirical Reality (§5 of log):**
  * `total_net_assets_local` has 34,137 legacy observations across 19,223 funds.
  * Running the FRD §6.1 simulation results in **6,772 dropped AUM observations (19.84% data loss across ~3,000+ funds)**.
  * **Root Cause:** In European/UCITS markets, an ETF’s trading contract currency (`bronze.contracts.currency`) is the share-class trading currency (e.g., `GBP`, `EUR`, `CHF`), but the master fund reports total portfolio AUM in `USD` (prefixed with `$`) or `EUR` (prefixed with `€`).
    * `raw='$364.33M'`, `contract='EUR'` → **2,554 rows dropped** (1,287 funds)
    * `raw='$23.16B'`, `contract='GBP'` → **2,364 rows dropped** (1,186 funds)
    * `raw='€5.79B'`, `contract='GBP'` → **628 rows dropped** (315 funds)
    * `raw='$547.67M'`, `contract='CHF'` → **452 rows dropped** (452 funds)
    * `raw='€4.68M'`, `contract='USD'` → **190 rows dropped** (108 funds)
* **Critique & Actionable FRD Fix:**
  1. *Option A (Contract-strict, data loss accepted):* Document explicitly in the FRD decision log that ~20% of AUM observations on non-US cross-listed share classes will be omitted by design because `bronze.contracts.currency` does not represent fund master reporting currency.
  2. *Option B (Recommended - Precision Disambiguation):* Notice that glyph `€` is unambiguous: it maps to exactly one currency (`EUR`), which is already in `known_currencies`. If a raw string starts with `€`, it is `EUR` regardless of whether the fund's London listing trades in `GBP` (saving ~1,000 observations). For `$`, if `contract` is European (`GBP`, `EUR`, `CHF`) and the fund is an international cross-listing, evaluate whether `USD` can be resolved via `bronze.products.currency` or if master fund currency is tracked elsewhere.

---

### 2.2 Unleveraged Database Asset: `bronze.themes` Reference Catalog
* **FRD §5.2 / §5.3 Gap:** The FRD marks `family='theme'` as an open vocabulary (`metrics=None`) with no schema validation, stating *"Taxonomy evolves"*.
* **Empirical Reality (§9 & schema):**
  * Themes are not a minor sleeve: they constitute **7,431,078 observations — 74.5% of the entire Silver layer!**
  * The pre-FRD schema already maintains `bronze.themes (theme_id, num_id, name, parent_id)`.
* **Critique & Actionable FRD Fix:**
  * While hardcoding 491 themes into a static Python `StrEnum` is undesirable, leaving 75% of Silver data completely unverified is an architectural antipattern.
  * **Solution:** Treat `bronze.themes` as a **dynamic database-backed contract**. In `Observation`, `code` stores `theme_id` and `metric` stores `name`. Add a step in `prep/pipeline.py` (or as a catalog integrity check in `scripts/catalog_silver.py`) asserting:
    ```sql
    SELECT COUNT(*) FROM silver.observations o
    LEFT JOIN bronze.themes t ON o.code = t.theme_id
    WHERE o.family = 'theme' AND t.theme_id IS NULL;
    ```
    This guarantees referential integrity without requiring hundreds of static Python enums.

---

### 2.3 Simplex Reality vs `partition_of_unity = True` (FRD D10 & §5.3)
* **The Rule:** FRD D10 flags sleeve families whose metrics must sum to 1.0: `asset_class`, `country`, `industry`, `credit_rating`, `debt_type`, `maturity`.
* **Empirical Reality (§7 of log):**
  * `asset_class`: **100.0% sum to 1.0000** (30,307 / 30,307 snapshots).
  * `country`: **62.99%** sum to 1.0 (5,934 snapshots under-sum <0.95; median 1.0, max 2.13).
  * `industry`: **19.45%** sum to 1.0 (median 0.9962; min -0.43, max 3.388).
  * `credit_rating`: **13.82%** sum to 1.0 (max 17.35).
  * `debt_type`: **9.78%** sum to 1.0 (max 17.35).
  * `maturity`: **13.67%** sum to 1.0 (max 17.35).
* **Critique & Actionable FRD Fix:**
  * Fixed income breakdowns (credit, debt type, maturity) and industry breakdowns do not sum to 1.0 in multi-asset funds because they represent sleeve weights conditional on asset class (e.g., fixed income is only 20% of the fund), or they contain derivative notions summing to multiples of 1.0.
  * Setting `partition_of_unity = True` uniformly across all six families is conceptually inaccurate.
  * **Fix:** Update FRD §5.3 to distinguish between:
    1. **Strict Simplex (`partition_of_unity = True`):** Strictly `asset_class`.
    2. **Conditional / Unnormalized Sleeves (`partition_of_unity = False` or `conditional_sleeve = True`):** `country`, `industry`, `credit_rating`, `debt_type`, `maturity`. Explicitly document that sum-to-one tests must only be applied to `asset_class`.

---

## 3. Domain Invariants & Schema Clarifications

### 3.1 Negative Values are Legitimate Domain Representations
* **Empirical Reality (§7 & §9 of log):**
  * `asset_class`: 7,400 snapshots contain short positions (min `Cash`: -4.1666, min `Other`: -420.9745).
  * `country`: 3,280 snapshots contain shorts (min `United States`: -3.7040, min `Unidentified`: -420.2214).
  * `theme`: 4,547 observations are negative (rank-adjusted weights reflect short thematic factor exposure).
  * `profile`: 280 snapshots have **negative expense ratios** (`management_expense_ratio` down to -95.0531, `non_management_expense_ratio` down to -0.9584). These represent vendor fee waivers/reimbursements where the sponsor subsidizes fund operation.
  * `ratios`: `dividend_yield_weighted_average` has negative values (-0.0008), `effective_maturity` has negative values (-5.1251, reflecting interest rate swap duration hedging), `nominal_maturity` (-0.9624), and `return_on_capital` (-9.0157).
* **Actionable FRD Fix:** In FRD §4 and §11, explicitly document that `value` is bounded only by IEEE 754 numeric finiteness (`IS NOT NULL AND NOT is_nan(value) AND NOT is_infinite(value)`). Validation rules must **not** assert non-negativity (`value >= 0`).

---

### 3.2 Expense Ratio Allocation Structure
* **Empirical Reality (§7 of log):**
  * In 100% of paired snapshots (12,419 / 12,419), `management_expense_ratio` + `non_management_expense_ratio` = 1.0000.
  * In 6,420 snapshots, non-management fee is 0.0 and management fee is 1.0.
* **Domain Clarification:** These two profile fields in the vendor payload are **allocation weights** of the total fee burden, not basis-point expense ratios. The actual expense ratio in basis points is `total_expense_ratio`. The FRD correctly specifies keeping them as provided fractions (do not divide by 100).

---

### 3.3 Style Box 9-Cell Empirical Sparsity
* **Empirical Reality (§9 & §10 of log):**
  * Out of the 12 theoretical cells (`value|core|growth` × `large|multi|mid|small`), **3 cells have zero observations**: `large_growth`, `large_value`, and `mid_value`.
  * 100% of observed funds map to the other 9 cells (`large_core`, `mid_core`, `mid_growth`, `multi_core`, `multi_growth`, `multi_value`, `small_core`, `small_growth`, `small_value`).
* **Actionable FRD Fix:** In FRD §11 (Tests), clarify that while the 12-cell Cartesian product must be supported by the coordinate validator and panel expansion, test assertions on database contents must not expect rows in `large_growth`, `large_value`, or `mid_value`.

---

## 4. Downstream Implications for Panel LOCF (Phase 2 Alignment)

While FRD §2 Decision **D12** locks Phase 1 as a mechanical SQL rewrite of `prep/panel.py`, the audit uncovered empirical cadences that directly impact panel quality:

### 4.1 The ESG 90-Day Staleness Paradox
* **Empirical Lag & Cadence (§2, §3, §6 of log):**
  * LSEG ESG scores have a median reporting lag of 8 days, but a **p75/p95 reporting lag of 1,491 days (~4.1 years)**.
  * The empirical inter-arrival update interval per fund is **1,470 days** (p90/p95).
  * Today's panel applies a **90-day LOCF cap** to ESG (`source='esg'`).
  * **Consequence (§6 simulation):** The 90-day cap results in an abysmal **4.4% monthly panel fill rate** (37,834 valid fund-months out of 869,081). If the cap is widened to the empirical p95 (1,470 days), coverage surges to **39.7%**; combined with backward fill, coverage reaches **70.6%**.
* **Actionable FRD Note:** Ensure the mechanical rewrite in Phase 1 preserves the existing 90-day cap without regression, but add an explicit note in FRD §9 highlighting that the panel ESG LOCF cap must be raised in Phase 2 to prevent discarding ~90% of valid ESG signals.

---

## 5. Validated FRD Architectural Decisions

The catalog audit provides strong empirical confirmation for the following core decisions:

| FRD Decision | Empirical Verification | Audit Log Evidence |
|---|---|---|
| **D1 & D3 (Unified PK)** | `(product_id, family, metric, effective_date)` | **PASS:** 0 duplicate groups across 9,977,797 combined observations. |
| **D5 (Schema Cleanliness)** | Drop legacy tables, verify causality | **PASS:** 0 future-dated rows (`effective_date <= fetched_at::DATE`), 0 orphan products. |
| **D9 (Credit Ratings Enum)** | 12 cleaned credit grades | **PASS:** Exact 12/12 match (`AAA`, `AA`, `A`, `BBB`, `BB`, `B`, `CCC`, `CC`, `C`, `D`, `Not Rated`, `Not Available`). |
| **D9 (Maturity Slugs Enum)** | 8 `mat_*` code slugs | **PASS:** Exact 8/8 match (`mat_lt_1y` through `mat_gt_30y` and `mat_other`). |
| **D9 (Debt Clusters)** | 110 vendor debt types mapped to 9 clusters | **PASS:** All 110 vendor debt types map cleanly to `DEBT_CLUSTERS` without unmapped values. |
| **D9 (EsgMetric Enum)** | 17 LSEG pillar slugs | **PASS:** Exact 17/17 match with stored database slugs. |
| **D9 (MstarMetric Enum)** | 10 Morningstar metric slugs | **PASS:** Exact 10/10 match (`mstar_medalist_rating`, pillars, stars, coverage pct). |

---

## 6. Actionable Specification Checklist (Document Diff)

Before handing off the FRD to implementation, apply the following surgical adjustments:

1. **FRD §5.1 (`RatiosMetric`):**
   * Replace the 30-member list with all **47 canonical tags**.
   * Define `RatiosMetric.PERCENTAGE` strictly as the 24 growth/return/yield metrics.
   * Explicitly designate the remaining 23 metrics as standard floating-point quantities (no 1/100 scaling).
2. **FRD §5.1 (`IndustryMetric`):**
   * Define `IndustryMetric` as the closed set of **14 Refinitiv/Lipper sectors**, including `Academic & Educational Services`.
3. **FRD §5.2 & §5.3 (`Theme` Family):**
   * Document referential integrity against `bronze.themes` via `Observation.code = bronze.themes.theme_id`.
4. **FRD §5.3 (`FamilyDefinition`):**
   * Change `partition_of_unity` to `True` **only** for `asset_class`. Set to `False` for `country`, `industry`, `credit_rating`, `debt_type`, and `maturity`.
5. **FRD §6.1 (`disambiguate_aum_currency`):**
   * Document the expected ~19.8% AUM drop rate on cross-listed UCITS share classes as an intentional trade-off under D11, or permit unambiguous glyph matching (`€` → `EUR`).
6. **FRD §4 & §11 (Invariants & Tests):**
   * Clarify that `value` supports negative floats (shorts, fee waivers, swap durations).
   * Note the empirical 9-cell sparsity of style box observations.