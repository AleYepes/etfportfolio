# Prep & Panel Architecture and Exploration Roadmap

- **Status:** Approved
- **Scope:** Silver Prep (`etfportfolio/prep`), Database Schema (`etfportfolio/core/schema.sql`), and Monthly Factor Panel Pipeline
- **Functional Requirements Document:** [docs/FRD.md](file:///Users/alex/Documents/etfportfolio/docs/FRD.md)
- **Priority Hierarchy:** Data Accuracy / Correctness >> Codebase Simplicity / Clarity > Storage Efficiency > Runtime Performance >> Auth Security

---

## 1. Architecture Overview & Pipeline Stages

This document defines the architectural strategy for transforming raw Bronze ETF snapshot payloads into a clean Silver Medallion layer, and subsequently building a unified monthly factor panel (`silver.monthly_panel`) for quantitative factor research and portfolio optimization.

The pipeline establishes a strict separation of concerns across two stages:

### Stage 1: Observations (Single-Pass Extract-and-Clean)
- **Inputs:** Compressed JSON blobs in `bronze.snapshots` and `bronze.payload_blobs`.
- **Outputs:** Long-format records in `silver.product_metrics` and `silver.product_dimensions`.
- **Responsibilities (Syntactic & Operable Cleaning):**
  - Parse raw strings into operable numeric values (`value DOUBLE`).
  - Standardize percentages to decimal fractions (e.g., `15.5%` -> `0.155`).
  - Extract and standardize explicit ISO currency codes (`currency VARCHAR`).
  - Enforce report date precedence (`as_of_date` / item date over snapshot timestamp).
  - Map categorical ordinal scores to numeric scales (e.g., `"gold"` -> `5.0`).
  - Drop obsolete granular records (`top_holding` constituent stocks) to prevent storage bloat.

### Stage 2: Panel Construction (Comparative & Time-Series Cleaning)
- **Inputs:** Clean observations in `silver.product_metrics` and `silver.product_dimensions`.
- **Outputs:** Physical long table `silver.monthly_panel` (`product_id`, `as_of_date`, `asset_class`, `feature_id`, `value`).
- **Responsibilities (Cross-Sectional & Time-Series Cleaning):**
  - Generate a unified month-end calendar spine.
  - Forward-fill observations via LOCF with metric-specific empirical staleness caps.
  - Flatten active dimensions (`industry` sectors, `country` ISO codes, `maturity` duration buckets, `credit_rating` quality tiers, `style_box` coordinates, and `theme` weights) into panel features.
  - Verify sum-to-1.0 invariants across allocation dimensions and isolate residual categories (`Other`, `Unidentified`) to eliminate multicollinearity.
  - Assign domain defaults (e.g., unassigned themes default to `0.0` exposure).
  - Cross-sectional outlier winsorization and bounds across peer groups.

### Downstream Post-Processing (In-Memory Analytics in Python / RAM)
- Pivot `silver.monthly_panel` into wide matrices on demand (`PIVOT silver.monthly_panel ON feature_id`).
- Filter by `asset_class = 'Equity'` or `'Fixed Income'`.
- Calculate factor return series (weighted cross-sectional regressions).
- Prune multicollinear factors via correlation matrices and Variance Inflation Factor (VIF) clustering.
- Execute portfolio optimization (Mean-Variance, CVaR, Black-Litterman) using NumPy/SciPy/CVXPY.

---

## 2. Settled Architectural Decisions

### 2.1. Unified Extract-and-Clean (No Table Duplication)
- Ingestion from Bronze blobs into Silver tables performs all syntactic and operable cleaning in a single pass.
- To prevent DuckDB WAL bloat and file fragmentation, the pipeline avoids row-wise in-place `UPDATE` operations and duplicate raw/clean tables.
- [extractors.py](file:///Users/alex/Documents/etfportfolio/etfportfolio/prep/extractors.py) and [prep/utils.py](file:///Users/alex/Documents/etfportfolio/etfportfolio/prep/utils.py) act as the single source of truth for converting payload fields into clean numeric values and explicit units, while preserving raw strings in `raw_value` for auditability.

### 2.2. Schema Enhancement: Explicit Currency
- Monetary metrics (e.g., `total_net_assets_local`) require explicit currency provenance to support cross-sectional size comparisons and log(AUM) calculations.
- `silver.product_metrics` in [schema.sql](file:///Users/alex/Documents/etfportfolio/etfportfolio/core/schema.sql) is amended to include `currency VARCHAR`.
- Extractors populate ISO-4217 currency codes (`USD`, `EUR`, `CAD`, etc.), resolving ambiguous `$` symbols against `silver.products.currency`. Unitless ratios leave `currency` as `NULL`.

### 2.3. Outright Removal of Granular Stock Holdings
- The `top_holding` dimension records over 30,000 distinct stock names, producing extreme sparsity across the ETF universe without factor modeling value.
- Extraction of `top_holding` in `extract_holdings()` is removed entirely.
- Overall fund concentration remains tracked via the scalar metric `portfolio_top_10_concentration` in `silver.product_metrics`.

### 2.4. Dimension Flattening & Normalization Rules
When building `silver.monthly_panel`, active dimensions from `silver.product_dimensions` are flattened into factor features:
1. **`asset_class`**: 4 categories (`Equity`, `Fixed Income`, `Cash`, `Other`). Serves as the primary partition/filter attribute on `silver.monthly_panel`. Normalizes to sum to 1.0; 'Other' is dropped during factor modeling.
2. **`country`**: 107 categories mapped to 103 `dimension_code` values (2-letter ISO). Sum to 1.0 per `(product_id, effective_date)`. 'Unidentified' is dropped during post-processing to eliminate multicollinearity.
3. **`industry`**: 14 economic sectors. Sum to 1.0 per `(product_id, effective_date)`.
4. **`style_box` & `style_box_hist`**: Unified into 9 boolean coordinate features (`large_core`, `mid_growth`, etc.), giving precedence to `style_box` when both exist for a product and date.
5. **`credit_rating`**: 12 credit tiers (plus Not Available / Not Rated). Sum to 1.0 per `(product_id, effective_date)`.
6. **`maturity`**: 8 duration buckets (plus % Maturity Other). Sum to 1.0 per `(product_id, effective_date)`.
7. **`debt_type`**: 110 categories. Candidate for reduction/clustering into a compact set of standardized debt codes summing to 1.0.
8. **`theme`**: All 491 thematic factor weights are preserved, with missing values defaulting to `0.0` (zero exposure) in the panel. The 19 parent themes in `bronze.themes` serve as an umbrella hierarchy.

### 2.5. Panel Storage: Physical Long Table, In-Memory Dynamic Pivot
- **On Disk (`silver.monthly_panel`)**: Stored as a physical long table `(product_id, as_of_date, asset_class, feature_id, value)`. This avoids dynamic column migrations and utilizes DuckDB columnar compression.
- **In Memory (Python / Analytics)**: Pivoted into wide 2D matrices on demand using DuckDB's vectorized `PIVOT` or Polars/Pandas. Multicollinearity pruning and factor filtering occur dynamically in RAM.

---

## 3. Investigation Protocol for Payload Exploration Sessions

To investigate and specify all subtle quirks, units, and outliers across the 82 metrics and 9 active dimensions, the exploration work is divided into 5 sequential agent sessions.

### 3.1. Operating Rules for Exploration Sessions
1. **No Fragmented Code Changes:** Exploration agents do not patch extractors or schema files independently.
2. **Single Deliverable per Session:** Each session agent audits payloads, investigates DuckDB distributions, and documents explicit functional requirements in [docs/FRD.md](file:///Users/alex/Documents/etfportfolio/docs/FRD.md).
3. **Empirical Cadence Analysis:** Each session reviews the empirical distribution of update delays (`effective_date` deltas) in DuckDB and specifies the recommended LOCF staleness threshold in the FRD.

### 3.2. Exploration Sequence & Scopes

#### Session 1: `profile` & Fees
- **Endpoint:** `/tws.proxy/fundamentals/mf_profile_and_fees/`
- **Scope:** `total_net_assets_local`, `total_expense_ratio`, `management_expense_ratio`, `non_management_expense_ratio`, `audited_net_expense_ratio`, `manager_tenure_years`, `is_passive`.
- **Objectives:** Document currency extraction logic (`$`, `€`, `CAD`, etc.), fee ratio scaling, negative management fee handling, manager tenure date formulas, and passive flag validation.
- **Output:** Populate Section 4.1 of [docs/FRD.md](file:///Users/alex/Documents/etfportfolio/docs/FRD.md).

#### Session 2: `ratios` Fundamentals
- **Endpoint:** `/tws.proxy/fundamentals/mf_ratios_fundamentals/`
- **Scope:** 47 fundamental metrics across Valuation Multiples, Growth Rates, Financial Health, Profitability, and Fixed Income.
- **Objectives:** Document extreme ratio bounds (e.g., leverage multipliers, sales growth), negative value handling on positive valuation multiples, and payload `as_of_date` verification.
- **Output:** Populate Section 4.2 of [docs/FRD.md](file:///Users/alex/Documents/etfportfolio/docs/FRD.md).

#### Session 3: `holdings` Allocations
- **Endpoint:** `/tws.proxy/fundamentals/mf_holdings/`
- **Scope:** `asset_class`, `country`, `industry`, `credit_rating`, `debt_type`, `maturity`, and scalar `portfolio_top_10_concentration`.
- **Objectives:** Formalize removal of `top_holding` constituent extraction, verify sum-to-1.0 normalization rules, audit negative cash/collateral and leveraged gross weights (> 100%), and standardize country ISO codes.
- **Output:** Populate Section 4.3 of [docs/FRD.md](file:///Users/alex/Documents/etfportfolio/docs/FRD.md).

#### Session 4: `mstar` & `lipper` Ratings
- **Endpoints:** `/tws.proxy/mstar/fund/detail?conid=` and `/tws.proxy/fundamentals/mf_lip_ratings/`
- **Scope:** Morningstar Medalist, Pillar, Star, and Sustainability ratings; 568 Lipper metric variants across 37 geographic peer universes.
- **Objectives:** Formalize analyst vs. quantitative model segregation in Morningstar, define geographic universe selection for Lipper to prevent factor explosion, and establish rating scale maps.
- **Output:** Populate Section 4.4 of [docs/FRD.md](file:///Documents/etfportfolio/docs/FRD.md).

#### Session 5: `themes` & `esg` Evaluative Signals
- **Endpoints:** `/tws.proxy/knowledge-graph/ui/fund?conid=` and `/tws.proxy/impact/esg/`
- **Scope:** 491 thematic factor weights, 17 Refinitiv ESG scores.
- **Objectives:** Formalize sparse default rules (`NULL` -> `0.0`), document umbrella mapping using the 19 parent themes in `bronze.themes`, and specify weekly vs. snapshot date mechanics.
- **Output:** Populate Section 4.5 of [docs/FRD.md](file:///Users/alex/Documents/etfportfolio/docs/FRD.md).

---

## 4. Master Implementation Phase

Once all 5 exploration sessions have completed their audits and populated [docs/FRD.md](file:///Users/alex/Documents/etfportfolio/docs/FRD.md), a final implementation agent executes the consolidated changes:

1. **Schema Update:** Update `etfportfolio/core/schema.sql` to add `currency VARCHAR` to `silver.product_metrics`.
2. **Extractor Refactoring:** Implement all operable cleaning and currency parsing rules across [extractors.py](file:///Users/alex/Documents/etfportfolio/etfportfolio/prep/extractors.py) and [prep/utils.py](file:///Users/alex/Documents/etfportfolio/etfportfolio/prep/utils.py), and remove `top_holding` extraction.
3. **Unit Tests:** Add comprehensive unit test fixtures and assertions in `tests/prep/test_extractors.py`.
4. **Database Re-ingestion:** Run `main.py prep observations --force` to rebuild `silver.product_metrics` and `silver.product_dimensions`.
5. **Panel Construction:** Implement `etfportfolio/prep/panel.py` implementing `silver.monthly_panel` with dimensional flattening, sum-to-1.0 checks, and empirical LOCF staleness caps.