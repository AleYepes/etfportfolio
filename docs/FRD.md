# Functional Requirements Document (FRD): Silver Prep & Factor Panel

- **Document Status:** In Progress (Living Specification)
- **Parent Architecture:** [docs/Roadmap.md](file:///Users/alex/Documents/etfportfolio/docs/Roadmap.md)
- **Scope:** Silver Prep (`etfportfolio/prep`), Database Schema (`etfportfolio/core/schema.sql`), and Factor Panel (`silver.monthly_panel`)
- **Guiding Priority Hierarchy:** Data Accuracy / Correctness >> Codebase Simplicity / Clarity > Storage Efficiency > Runtime Performance >> Auth Security

---

## 1. System Overview & Core Invariants

This document serves as the formal living functional specification for data extraction, operable cleaning, and monthly panel construction in the `etfportfolio` repository.

### 1.1. Operating Protocol
1. **Exploration Sessions Accumulate Requirements:** During each of the 5 payload exploration sessions, the dedicated agent investigates raw payloads, audits DuckDB distributions, and appends explicit functional rules to Section 4 of this document. Exploration agents do not write one-off code patches.
2. **Single Implementation Pass:** Once all 5 payload explorations are completed, a single implementation agent executes all schema adjustments, extractor refactors, and test additions in one unified pass.
3. **Living Audit Trail:** Every cleaning rule or metric override added to this document must include empirical justification derived from raw payload data or DuckDB distributions.

### 1.2. Core Architectural Invariants
1. **Single-Pass Operable Extraction:** Extractors in `etfportfolio/prep/extractors.py` must convert raw string values into clean, operable numeric values (`value DOUBLE`) and standard ISO currency codes (`currency VARCHAR`). The exact display string is retained in `raw_value` for auditability.
2. **Precedence of Report Dates:** Effective dates must always extract the explicit item/payload `as_of_date` or report date over snapshot fetch timestamps. Snapshot timestamp is strictly a fallback when no report date exists.
3. **Allocation Sum-to-1.0 Invariant:** `asset_class` is the **strict partition-of-unity invariant**; 100.0% of product-dates must sum to $1.0 \pm 0.0001$. Sub-allocations (`country`, `industry`, `credit_rating`, `debt_type`, `maturity`) represent partial, sleeve-specific, or truncated distributions (e.g. top-country truncation, equity-only sectors omitting bond sleeves, cash repo offsets). These are audited for bounds in Silver and renormalized or assigned residual factor columns (`Unidentified`, `Other`, `Not Rated`) in Stage 2 panel construction.
4. **Physical Long Storage on Disk:** Storage on disk is strictly Long. Dynamic wide pivoting occurs in memory (via DuckDB `PIVOT` or Polars/Pandas in RAM) for factor regression and portfolio optimization.

---

## 2. Target Database Schemas

### 2.1. `silver.product_metrics` (Amended with `currency`)
```sql
CREATE TABLE silver.product_metrics (
    product_id             INTEGER NOT NULL,                  -- Links to silver.products.product_id
    source                 VARCHAR NOT NULL,                  -- Ingestion endpoint ('ratios', 'profile', etc.)
    metric_id              VARCHAR NOT NULL,                  -- Canonical metric identifier
    effective_date         DATE NOT NULL,                     -- Point-in-time assessment date
    effective_date_source  VARCHAR NOT NULL,                  -- 'payload', 'item', or 'snapshot'
    fetched_at             TIMESTAMP WITH TIME ZONE NOT NULL, -- Blob fetch timestamp
    value                  DOUBLE NOT NULL,                   -- Standardized numeric value for analytics
    raw_value              VARCHAR NOT NULL,                  -- Formatted display string from raw payload
    currency               VARCHAR,                           -- Standard ISO-4217 currency code ('USD', 'EUR', etc.) or NULL
    PRIMARY KEY (product_id, source, metric_id, effective_date)
);
```

### 2.2. `silver.product_dimensions`
```sql
CREATE TABLE silver.product_dimensions (
    product_id             INTEGER NOT NULL,                  -- Links to silver.products.product_id
    dimension_type         VARCHAR NOT NULL,                  -- 'asset_class', 'country', 'industry', etc.
    dimension_name         VARCHAR NOT NULL,                  -- Category or feature name
    dimension_code         VARCHAR,                           -- Standard code: 2-letter ISO, style slug, or theme UUID
    effective_date         DATE NOT NULL,                     -- Point-in-time assessment date
    effective_date_source  VARCHAR NOT NULL,                  -- 'payload', 'item', or 'snapshot'
    fetched_at             TIMESTAMP WITH TIME ZONE NOT NULL, -- Blob fetch timestamp
    value                  DOUBLE NOT NULL,                   -- Decimal weight (1.0 = 100%) or coordinate flag
    raw_value              VARCHAR NOT NULL,                  -- Formatted percentage or coordinate string
    PRIMARY KEY (product_id, dimension_type, dimension_name, effective_date)
);
```
*Note: `dimension_type = 'top_holding'` is dropped outright during extraction to eliminate over 30,000 sparse individual security records.*

### 2.3. `silver.monthly_panel` (Physical Long Table)
```sql
CREATE TABLE silver.monthly_panel (
    product_id   INTEGER NOT NULL,                  -- Links to silver.products.product_id
    as_of_date   DATE NOT NULL,                     -- Month-end calendar spine date
    asset_class  VARCHAR NOT NULL,                  -- Primary partition ('Equity', 'Fixed Income', etc.)
    feature_id   VARCHAR NOT NULL,                  -- Metric ID or flattened dimension feature
    value        DOUBLE NOT NULL,                   -- Cleaned, LOCF-carried factor value
    PRIMARY KEY (product_id, as_of_date, feature_id)
);
```

---

## 3. Dimension Flattening & Normalization Specifications

When `silver.monthly_panel` is constructed, dimensional rows from `silver.product_dimensions` are flattened into panel features according to the following specifications:

| Dimension Type | Input Distinct Names | Panel Target Feature Representation | Normalization & Collinearity Handling |
| :--- | :--- | :--- | :--- |
| `asset_class` | 4 categories (`Equity`, `Fixed Income`, `Cash`, `Other`) | Primary partition/filter attribute on `silver.monthly_panel` | Strict partition-of-unity invariant: 100.0% of product-dates sum to $1.0 \pm 0.0001$. 'Other' is isolated/dropped during factor modeling. |
| `country` | 107 categories (103 ISO codes + `Unidentified`) | Flattened by 2-letter ISO `dimension_code` (e.g., `country_us`, `country_jp`) | Weights sum to ~1.0 (63.6% exact $\pm 0.1\%$, 74.2% $\pm 2\%$). Standard ISO-3166-1 remaps applied (`HR`, `BG`, `GU`, `UZ`). `Unidentified` retains `dimension_code = NULL` (acting as short swap/derivative offset) and is dropped during factor regressions to prevent multicollinearity. |
| `industry` | 14 sectors | 14 sector weight features (e.g., `sector_technology`, `sector_financials`) | Weights sum to ~1.0 (19.8% exact $\pm 0.1\%$, 75.7% $\pm 2\%$). Discontinued telecom category is remapped to `Communication Services`. Unclassified residual buckets (`Not Classified - Non Equity`, `Non Classified Equity`) handled explicitly. |
| `style_box` & `style_box_hist` | 9 observed coordinates (out of 12 grid cells) | 12 canonical boolean indicator features (`style_{size}_{style}`) | All 12 theoretical Morningstar coordinates are emitted in `silver.monthly_panel` to prevent schema drift in downstream regressions. The 3 unobserved cells (`style_large_growth`, `style_large_value`, `style_mid_value`) default to `0.0`. Precedence given to `style_box` over `style_box_hist` when both exist for a given `(product_id, effective_date)`. |
| `credit_rating` | 12 tiers | 12 standardized credit quality factor weights (`AAA` through `D`, plus `Not Rated` and `Not Available`) | Weights sum to ~1.0 (14.8% exact $\pm 0.1\%$, 53.8% $\pm 2\%$). Dimension codes already standardized to cleaned letter grades (`[APPLIED]`). Other-like categories isolated. |
| `maturity` | 8 buckets | 8 standardized duration bucket features using concise slugs (`mat_lt_1y`, `mat_1_to_3y`, `mat_3_to_5y`, `mat_5_to_10y`, `mat_10_to_20y`, `mat_20_to_30y`, `mat_gt_30y`, `mat_other`) | Weights sum to ~1.0 (14.6% exact $\pm 0.1\%$, 53.6% $\pm 2\%$). Standardized slugs eliminate verbose `% Maturity` display strings. |
| `debt_type` | 110 categories | 9 standardized macroeconomic debt cluster features (`debt_sovereign`, `debt_agency_supranational`, `debt_municipal`, `debt_corporate_senior`, `debt_corporate_subordinated`, `debt_securitized_mbs`, `debt_securitized_abs`, `debt_unsecured_general`, `debt_specialty_derivatives`) | All 110 raw types map deterministically into the 9 macroeconomic clusters with 0 unmapped types (verified §4.3.2.6). Resolves regression matrix rank deficiency. |
| `theme` | 491 themes | 491 sparse thematic exposure features (`theme_{uuid}`) + 19 parent rollups (`theme_parent_{uuid}`) | Missing values default to `0.0` (zero exposure). Themes are non-exclusive and overlapping; no sum-to-1.0 constraint applied. Quality-gated via `theme_coverage >= 0.7`. |
| `top_holding` | 30,128 names | **Dropped outright** | Excluded from extraction (eliminates 263,186 sparse rows; 52.1% are single-fund singletons). Concentration captured via scalar `portfolio_top_10_concentration`. |

---

## 4. Payload Exploration & Requirement Specifications

*This section is populated and expanded sequentially by the 5 payload exploration agent sessions.*

### 4.1. Session 1 Specification: `profile` & Fees

- **Target Endpoint:** `/tws.proxy/fundamentals/mf_profile_and_fees/`
- **Extractor:** `extract_profile` in `etfportfolio/prep/extractors.py`
- **Test Fixtures:** `tests/fixtures/profile_*.json` (`profile_complete.json`, `profile_equity.json`, `profile_bond.json`, `profile_empty.json`, `profile_error.json`)
- **Scope of Ingested Entities:**
  - **7 Scalar Metrics** (`silver.product_metrics`):
    1. `total_net_assets_local`: Fund AUM denominated in local fund reporting currency.
    2. `total_expense_ratio`: Prospectus/profile total expense ratio (TER) as a decimal fraction of AUM.
    3. `management_expense_ratio`: Management fee proportion of total fund expenses (allocation fraction).
    4. `non_management_expense_ratio`: Operational and administrative proportion of total fund expenses (allocation fraction).
    5. `audited_net_expense_ratio`: Audited net expense ratio from the fund's official Annual Report.
    6. `manager_tenure_years`: Length of lead portfolio manager's tenure in fractional years.
    7. `is_passive`: Binary flag (`1.0` = Passive index tracker, `0.0` = Active management).
  - **2 Style Dimensions** (`silver.product_dimensions`):
    1. `style_box`: Morningstar 2D investment style box current coordinate (`selected`).
    2. `style_box_hist`: Morningstar 2D investment style box historical coordinate (`hist`).

---

#### 4.1.1. Empirical Database & Cadence Audit Findings

Across **42,406** raw Bronze snapshots of the `profile` endpoint in `data/etf.duckdb`, the empirical audit of live data revealed the following distribution characteristics and update frequencies:

##### A. Metric Statistical Distributions & Data Quality

| Metric ID | Table Source Key | Obs Count | Unique ETFs | Date Source | Min | P25 | Median | P75 | Max | Anomalies & Outliers |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `total_net_assets_local` | `fund_and_profile` (`Total_Net_Assets_Month_End`) | 33,625 | 19,176 | `item` (100%) | 0.0 | 2.22e+07 | 1.31e+08 | 7.84e+08 | 3.36e+13 | 1 zero (liquidated fund `CAD0`); min positive is \$19.08 (seed capital); mega-AUMs in local currency include ¥33.64T JPY (NEXT FUNDS TOPIX ETF 1306), ₩24.92T KRW (SAMSUNG KODEX 200 ETF 069500), and ¥17.15T JPY (NF NIKKEI 225 ETF 1321). Dollar-prefixed records (`$`) reach \$1.05T. |
| `total_expense_ratio` | `fund_and_profile` (`Total_Expense_Ratio`) | 16,806 | 6,189 | `snapshot` (100%) | 0.0000 | 0.0031 | 0.0055 | 0.0079 | 0.1349 | 35 zero-fee ETFs (100% fee waivers, e.g. HODL); Max values of 13.49% (PBDC) and 12.44% (FBDC) verified as valid SEC Acquired Fund Fees and Expenses (AFFE) for BDCs. |
| `management_expense_ratio` | `expenses_allocation` (`Management Expenses`) | 12,213 | 4,560 | `snapshot` (100%) | -95.0531 | 0.9785 | 1.0000 | 1.0000 | 1.9584 | 191 negative ratios down to -95.05 (-9505.31%) caused by advisor fee waivers/reimbursements exceeding gross fees on low-net-expense funds (e.g. FPAS, TAGS). |
| `non_management_expense_ratio` | `expenses_allocation` (`Non-Management Expenses`) | 12,213 | 4,560 | `snapshot` (100%) | -0.9584 | 0.0000 | 0.0000 | 0.0215 | 96.0531 | 78 negative ratios (down to -95.84%); 6,330 zero ratios; compensating positive spikes up to 96.05 (+9605.31%) ensuring sum with management equals 1.0. |
| `audited_net_expense_ratio` | `reports` (`Annual Report` -> `Total Net Expense`) | 4,830 | 4,597 | `item` (100%) | 0.0000 | 0.0039 | 0.0069 | 0.0105 | 0.0509 | 37 zero ratios (full waivers); max is 5.0911% (specialized leveraged/alternative fund). Reflects official audited annual reports. |
| `manager_tenure_years` | `fund_and_profile` (`Manager_Tenure`) | 23,277 | 11,372 | `snapshot` (100%) | 0.0192 | 1.4565 | 3.6715 | 8.6708 | 33.9493 | 100% parseable dates (`YYYY/MM/DD`); min tenure ~7 days; max tenure 33.95 years (lead manager since October 1992). |
| `is_passive` | `fund_and_profile` (`Management_Approach`) | 39,694 | 20,024 | `snapshot` (100%) | 0.0 | 0.0 | 1.0 | 1.0 | 1.0 | 24,087 Passive (60.7%), 15,607 Active (39.3%). No unknown categories. |

##### B. Style Box Dimension Coverage

From the `mstar` object in `profile` payloads, coordinates map across 3 value tiers (`value`, `core`, `growth`) and 4 market cap tiers (`large`, `multi`, `mid`, `small`), producing exactly 9 observed distinct features across 23,888 total dimension rows (the remaining 3 coordinates—`large_growth`, `large_value`, `mid_value`—are unassigned across the universe):
- `mid_core`: 1,301 selected / 7,834 hist (5,010 distinct ETFs in union)
- `multi_core`: 527 selected / 3,151 hist (2,147 distinct ETFs in union)
- `mid_growth`: 716 selected / 3,213 hist (2,074 distinct ETFs in union)
- `multi_growth`: 405 selected / 2,578 hist (1,663 distinct ETFs in union)
- `large_core`: 184 selected / 2,556 hist (1,550 distinct ETFs in union)
- `multi_value`: 71 selected / 599 hist (379 distinct ETFs in union)
- `small_growth`: 375 selected / 378 hist (346 distinct ETFs in union)
- `small_value`: 99 selected / 266 hist (184 distinct ETFs in union)
- `small_core`: 112 selected / 123 hist (108 distinct ETFs in union)

##### C. Empirical Cadence & Update Delta Analysis

By analyzing consecutive point-in-time deltas per `product_id` across historical snapshots:

| Metric Group | Delta Sample Size ($N$) | Min Delta | 25th % | Median Delta | 75th % | 95th % | Max Delta | Empirical Update Frequency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `total_net_assets_local` | 14,449 pairs | 14 days | 31 days | **31.0 days** | 31 days | 31 days | 31 days | **Strict Monthly Cadence** (12,898 delta=31d, 1,549 delta=28d). |
| `audited_net_expense_ratio` | 233 pairs | 214 days | 365 days | **365.0 days** | 365 days | 365 days | 792 days | **Annual Cadence** (225 of 233 pairs exactly 365d). |
| Snapshot Attributes (`TER`, `Tenure`, `Approach`) | ~10,000–20,000 pairs | 2 days | 2 days | **7.0–7.3 days** | 9 days | 10 days | 10 days | **Weekly Crawler Snapshot Cadence** (reflects recurring ingestion). |

---

#### 4.1.2. Exact Functional Cleaning Rules & Transformation Specifications

##### 1. AUM Currency Extraction & Scaling (`total_net_assets_local`)
- **Payload Location:** `fund_and_profile` entry with `name_tag = "Total_Net_Assets_Month_End"` or `name` beginning with `"Total Net Assets"`.
- **Date Precedence:**
  - Extract the parenthesized date string `(YYYY/MM/DD)` via regex `(\d{4}[-/]\d{2}[-/]\d{2}|\d{8})`.
  - Assign as `effective_date` with `effective_date_source = 'item'`.
  - If no parenthesized date exists, fallback to `snapshot_created_at.date()` with `effective_date_source = 'snapshot'`.
- **Numeric Scaling:**
  - Normalize European comma/dot separators: if both `,` and `.` appear, the earlier separator is treated as a thousands grouping separator.
  - Scale matched magnitude suffix:
    $$\text{Multiplier} = \begin{cases} 10^3 & \text{suffix } \in \{'k', 'K'\} \\ 10^6 & \text{suffix } \in \{'m', 'M'\} \\ 10^9 & \text{suffix } \in \{'b', 'B'\} \\ 10^{12} & \text{suffix } \in \{'t', 'T'\} \\ 1.0 & \text{otherwise} \end{cases}$$
  - $\text{value} = \text{base\_value} \times \text{Multiplier}$.
- **ISO-4217 Currency Disambiguation Rules:**
  Across all 42,406 snapshots, exactly 26 distinct currency prefix tokens appear. They must be resolved into ISO codes as follows:
  1. **Standard Currency Symbols:**
     - `€` $\rightarrow$ `EUR`
     - `£` $\rightarrow$ `GBP`
     - `¥` $\rightarrow$ `JPY`
     - `₹` $\rightarrow$ `INR`
  2. **Explicit ISO Prefix Strings:**
     - Match directly to `currency`: `CAD`, `AUD`, `CNY`, `TWD`, `HKD`, `CHF`, `BRL`, `SGD`, `MXN`, `KRW`, `MYR`, `CNH`, `AED`, `SEK`, `ZAR`, `ILS`, `SAR`, `NOK`, `HUF`, `DKK`, `VND`.
  3. **Disambiguation of Dollar Sign (`$`):**
     - If `silver.products.currency == 'CAD'` (or listing exchange is `TSE` / Canadian) $\rightarrow$ `CAD` (local Canadian reporting line; 23 snapshots).
     - Otherwise $\rightarrow$ `USD`. This correctly handles:
       - US-domiciled ETFs (14,986 snapshots).
       - European UCITS ETFs (Ireland, Luxembourg) trading on European/UK/Mexican exchanges in local trade currency (2,552 EUR, 2,364 GBP, 1,028 MXN, 452 CHF) whose underlying fund base reporting currency is USD.
       - Asian cross-listings (SEHK, SGX, TASE) reporting AUM in USD base currency.
     *(Note: Australian ETFs in the dataset uniformly report AUM with an explicit `AUD` prefix across all 877 snapshots; zero `$`-prefixed records carry an AUD contract currency).*
- **Zero & Outlier Policy:**
  - $\text{value} = 0.0$ (e.g. `CAD0 (2020/08/31)`) is retained in Silver as a valid observation of a defunct/liquidated fund, and masked out during factor weighting.
  - Micro-AUM values (e.g. \$19.08) are retained.

##### 2. Total Expense Ratio (`total_expense_ratio`)
- **Payload Location:** `fund_and_profile` entry with `name_tag = "Total_Expense_Ratio"`.
- **Transformation Formula:**
  $$\text{value} = \frac{\text{float}(\text{raw\_value.replace}('\%', ''))}{100.0}$$
- **Validation Bounds:**
  - Valid range: $[0.0000, 0.2000]$ (0.0% to 20.0%).
  - `0.0%` is legitimate (zero-fee promotional waivers, e.g. VanEck Bitcoin ETF HODL).
  - Ratios exceeding 5.0% (e.g. Putnam BDC Income ETF PBDC at 13.49%, First Trust BDC ETF FBDC at 12.44%) are **valid** SEC-mandated Acquired Fund Fees and Expenses (AFFE) for Business Development Companies. They must not be clamped or rejected.

##### 3. Expense Breakdown Ratios (`management_expense_ratio`, `non_management_expense_ratio`)
- **Payload Location:** `payload["expenses_allocation"]` list entries (`name = "Management Expenses"` and `name = "Non-Management Expenses"`).
- **Financial Semantics:**
  - These values represent the **allocation breakdown of total expenses** ($\text{Management Fee} / \text{Total Net Expense}$), **NOT** an absolute percentage of fund AUM.
  - Invariant: $\text{management\_expense\_ratio} + \text{non\_management\_expense\_ratio} = 1.0$.
- **Fee Waiver / Subsidy Mechanics & Negative Ratios:**
  - Ratios can legitimately become negative (down to $-95.0531$) or exceed $1.0$ (up to $+96.0531$) when an advisor reimburses fund expenses or waives fees on a fund with near-zero total net expenses.
  - **Silver Layer:** Retain the exact unconstrained float `ratio` for auditability and reconstruction of raw fee allocations.
  - **Factor Panel Layer (`silver.monthly_panel`):**
    - Winsorize allocation weights to $[-1.0, 2.0]$ to prevent extreme regression leverage, OR
    - Construct an operable absolute fee metric:
      $$\text{management\_fee\_rate} = \text{total\_expense\_ratio} \times \text{management\_expense\_ratio}$$

##### 4. Audited Net Expense Ratio (`audited_net_expense_ratio`)
- **Payload Location:** `payload["reports"]` where `name = "Annual Report"`, field `name = "Total Net Expense"`.
- **Date Precedence:**
  - Report `as_of_date` (Unix epoch millisecond timestamp converted to UTC date) assigned as `effective_date` with `effective_date_source = 'item'`.
  - Fallback to snapshot date only if `as_of_date` is missing or $0$.
- **Transformation Formula:**
  $$\text{value} = \frac{\text{float}(\text{raw\_value.replace}('\%', ''))}{100.0}$$

##### 5. Manager Tenure (`manager_tenure_years`)
- **Payload Location:** `fund_and_profile` entry with `name_tag = "Manager_Tenure"`.
- **Parsing:** Date string in `%Y/%m/%d` (all 23,277 live cases) or `%Y-%m-%d` / `%Y`.
- **Snapshot Observation Formula:**
  $$\text{tenure\_years} = \text{round}\left(\max\left(0.0, \frac{\text{snapshot\_date} - \text{start\_date}}{365.25}\right), 4\right)$$
- **Panel Construction Rule:**
  - `silver.product_metrics.value` stores snapshot-frozen tenure in fractional years. To advance tenure dynamically along the monthly panel calendar spine:
    - Parse manager start date from `silver.product_metrics.raw_value` (date string formatted as `YYYY/MM/DD`).
    - $\text{panel\_tenure\_years} = \max\left(0.0, \frac{\text{as\_of\_date} - \text{start\_date}}{365.25}\right)$.
    - If `raw_value` is not a parseable date string, fallback to standard LOCF: $\text{value} + \frac{\text{as\_of\_date} - \text{effective\_date}}{365.25}$.

##### 6. Passive Management Flag (`is_passive`)
- **Payload Location:** `fund_and_profile` entry with `name_tag = "Management_Approach"`.
- **Discrete Value Map:**
  $$\text{value} = \begin{cases} 1.0 & \text{if text.lower()} = \text{'passive'} \\ 0.0 & \text{if text.lower()} = \text{'active'} \end{cases}$$
- **Invariant:** Raise `ValueError` if an unexpected approach string is encountered.

##### 7. Style Box Coordinates (`style_box`, `style_box_hist`)
- **Payload Location:** `payload["mstar"]` dictionary (`selected` $\rightarrow$ `style_box`, `hist` $\rightarrow$ `style_box_hist`).
- **Coordinate Mapping:**
  - X-axis (`x_axis_tag`): `[0: 'value', 1: 'core', 2: 'growth']`.
  - Y-axis (`y_axis_tag`): `[0: 'large', 1: 'multi', 2: 'mid', 3: 'small']`.
  - Standard `dimension_code`: `"{y_tag}_{x_tag}"` (e.g. `mid_core`, `large_core`).
  - Standard `dimension_name`: `"{y_tag.title()} {x_tag.title()}"` (e.g. `Mid Core`, `Large Core`).
  - Value: `1.0` (active indicator).
- **Panel Flattening & Precedence:**
  - Flatten into 9 binary indicator features in `silver.monthly_panel`.
  - Precedence: When both `style_box` and `style_box_hist` exist for a given `(product_id, effective_date)`, `style_box` overrides `style_box_hist`.

---

#### 4.1.3. Recommended LOCF Staleness Caps for Factor Panel

Based on the empirical cadence audit of reporting delays:

| Feature / Metric Name | Empirical Update Cadence | Typical Reporting Delay | Recommended Panel Staleness Cap | Operational Rationale |
| :--- | :--- | :--- | :--- | :--- |
| `total_net_assets_local` | Monthly (median 31d) | 15–45 days | **6 Months (180 days)** | 99.9% of funds report within 60 days. Gaps exceeding 6 months indicate halted trading, liquidation, or abandoned reporting. |
| `total_expense_ratio` | Annual (Prospectus updates) | 30–90 days | **18 Months (540 days)** | Prospectuses update annually; 18 months accommodates 12-month fiscal cycle plus a 6-month regulatory filing buffer. |
| `audited_net_expense_ratio` | Annual (median 365d) | 60–120 days | **18 Months (540 days)** | Audited annual reports are published once per fiscal year; an 18-month threshold prevents dropouts during normal filing lags. |
| `management_expense_ratio` | Annual (with TER) | 30–90 days | **18 Months (540 days)** | Corresponds to prospectus expense allocation schedule. |
| `non_management_expense_ratio` | Annual (with TER) | 30–90 days | **18 Months (540 days)** | Corresponds to prospectus expense allocation schedule. |
| `manager_tenure_years` | Continuous | N/A | **12 Months (365 days)** | Computed continuously from `start_date`. Re-verification required if fund profile is not refreshed within 12 months. |
| `is_passive` | Invariant fund mandate | N/A | **Perpetual Carry** | Investment strategy mandate changes require shareholder vote and are exceedingly rare. |
| `style_box` / `style_box_hist` | Semi-annual / Annual | 30–90 days | **12 Months (365 days)** | Portfolio style box shifts slowly; annual carry provides continuous style exposure without introducing stale portfolio drifts. |

---

### 4.2. Session 2 Specification: `ratios` Fundamentals

- **Target Endpoint:** `/tws.proxy/fundamentals/mf_ratios_fundamentals/`
- **Extractor:** `extract_ratios` in [extractors.py](file:///Users/alex/Documents/etfportfolio/etfportfolio/prep/extractors.py#L75-L107)
- **Test Fixtures:** `tests/fixtures/ratios_complete.json`, `ratios_equity.json`, `ratios_bond.json`, `ratios_empty.json`, `ratios_error.json`
- **Bronze Snapshot Count:** 42,055 snapshots across 20,024 unique products.
- **Silver Record Count:** 748,780 metric rows across 16,538 unique products (47 distinct `metric_id` values).
- **Scope of Ingested Entities:**
  - **47 Scalar Metrics** (`silver.product_metrics`), organized into 5 payload sections:
    1. `ratios` section (22 metrics): Valuation multiples, EPS growth, profitability returns, and financial leverage.
    2. `financials` section (6 metrics): Revenue and cash flow growth rates.
    3. `fixed_income` section (5 metrics): Yield, maturity, coupon, and credit quality.
    4. `dividend` section (5 metrics): Yield, payout, DPS growth, price-to-dividend.
    5. `zscore` section (9 metrics): Composite and component cross-sectional z-scores.
  - **0 Dimensions** (`silver.product_dimensions`): This endpoint produces no dimensional data.

---

#### 4.2.1. Empirical Database & Cadence Audit Findings

Across **42,055** raw Bronze snapshots of the `ratios` endpoint in `data/etf.duckdb`, the empirical audit revealed the following:

##### A. Payload Structure & Date Provenance

- **Payload `as_of_date`:** Present in 38,064 of 42,055 snapshots (90.5%). The remaining 3,991 snapshots that lack `as_of_date` contain **zero metric items** across all 5 sections — they are effectively empty payloads. Therefore, **100% of metric-bearing payloads have a valid `as_of_date`.**
- **`as_of_date` Format:** Unix epoch millisecond timestamp (e.g., `1785470400000` → `2026-07-31 UTC`).
- **`effective_date_source`:** 100% of Silver records use `'payload'` — no snapshot-date fallback has been needed for this endpoint.
- **Effective Date Distribution:** All effective dates fall on month-end business days. 39 distinct dates observed, from `2015-01-31` to `2026-08-31`. The vast majority of data concentrates in the most recent months (14,316 products on `2026-07-31`, 7,568 on `2026-08-31`).
- **Error Payloads:** The `ratios_error.json` fixture demonstrates `IDENTIFICATION_PROBLEM` error responses (`{"type": "IDENTIFICATION_PROBLEM", ...}`). Zero such payloads exist in the current Bronze store. The extractor's `if not payload: return result` guard handles these via the empty-dict check.
- **Empty Payloads:** The `ratios_empty.json` fixture demonstrates payloads with all empty sections. 3,991 such payloads exist in Bronze (no `as_of_date`, all section lists empty). These correctly produce zero Silver rows.
- **`title_vs` Field:** A peer group label string (e.g., `"Large-Cap Growth Funds"`, `"Short Investment Grade Debt Funds"`). Present in 15,949 of 42,055 snapshots. **Not extracted** — informational only; the peer comparison columns (`vs`, `min`, `max`, `avg`, `percentile`) within each item are similarly not extracted.

##### B. Metric Extraction Mechanics

The current extractor iterates over 5 sections (`dividend`, `financials`, `fixed_income`, `ratios`, `zscore`) and for each item:
- Reads `item["value"]` (a `float`) directly as the operable numeric value — **no percentage division or scaling is applied**.
- Reads `item["value_fmt"]` (a `str`) as `raw_value` for display/auditability.
- Derives `metric_id` from `item["name_tag"]` via `sanitize_metric_id()`, which lowercases and replaces non-alphanumeric characters with underscores (e.g., `EPS_growth_1yr` → `eps_growth_1yr`, `LT_Debt_Shareholders_Equity` → `lt_debt_shareholders_equity`).
- Items with `value = None` or missing `name_tag` are silently skipped.
- **No null values or missing tags exist** in the entire Bronze store — verified across all 42,055 snapshots.
- **`currency` is always `NULL`** — all ratios metrics are unitless dimensionless quantities.

##### C. Metric Statistical Distributions & Data Quality

###### C.1. Valuation Multiples (from `ratios` section)

| Metric ID | Human Name | Obs | Unique ETFs | Min | P25 | Median | P75 | Max | Neg | Zero | Provider Cap | Anomalies |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `price_sales` | Price/Sales | 17,540 | 11,998 | 0.09 | 3.88 | 6.12 | 8.77 | **50.00** | 0 | 0 | 50.0 | 7 records at exact cap. |
| `price_cash` | Price/Cash | 17,529 | 11,993 | 1.70 | 15.77 | 20.97 | 26.19 | **60.00** | 0 | 0 | 60.0 | 29 records at exact cap. |
| `price_book` | Price/Book | 17,587 | 12,027 | 0.50 | 4.26 | 6.31 | 10.15 | **25.00** | 0 | 0 | 25.0 | 43 records at exact cap. |
| `price_earnings` | Price/Earnings | 17,602 | 12,041 | 0.68 | 23.95 | 28.52 | 32.85 | **60.00** | 0 | 0 | 60.0 | 70 records at exact cap. |
| `price_to_dividend` | Price to Dividend | 17,470 | 11,961 | 7.29 | 87.98 | 158.90 | 354.07 | 5,018.75 | 0 | 0 | None | No cap; extreme highs from near-zero dividend funds. |
| `relative_strength` | Relative Strength | 17,628 | 12,056 | -49.17 | 0.01 | 2.20 | 4.61 | 155.44 | 4,391 | 0 | None | Signed metric; large negatives/positives during market dislocations. |

**Key Finding — Provider-Side Caps on Valuation Multiples:** The data provider (Morningstar via IBKR) applies hard upper bounds on P/S (50), P/Cash (60), P/B (25), and P/E (60). No negative values exist for any valuation multiple in the entire Bronze store. These caps are applied upstream before delivery. **No negative valuation multiples are observed or possible** — the provider clips them at source.

###### C.2. EPS & Revenue Growth Rates (from `ratios` and `financials` sections)

| Metric ID | Human Name | Obs | Min | P25 | Median | P75 | Max | Neg | Provider Caps |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `eps_growth_1yr` | EPS Growth 1yr | 17,624 | **-50.00** | 14.82 | 22.72 | 29.92 | **100.00** | 511 | [-50, 100] (26 at -50, 61 at 100) |
| `eps_growth_3yr` | EPS Growth 3yr | 17,548 | **-50.00** | 9.72 | 15.86 | 21.76 | **100.00** | 1,064 | [-50, 100] (4 at -50, 17 at 100) |
| `eps_growth_5yr` | EPS Growth 5yr | 17,561 | -43.65 | 14.64 | 19.57 | 24.44 | **100.00** | 177 | [~, 100] (5 at 100) |
| `sales_growth_1_year` | Sales Growth 1yr | 17,608 | -18.46 | 7.72 | 13.10 | 17.42 | **100.00** | 218 | [~, 100] (5 at 100) |
| `sales_growth_3_year` | Sales Growth 3yr | 17,621 | -19.45 | 6.23 | 10.80 | 15.69 | **100.00** | 453 | [~, 100] (15 at 100) |
| `sales_growth_5_yr` | Sales Growth 5yr | 17,617 | -7.46 | 9.77 | 13.68 | 16.95 | 74.85 | 14 | None observed |
| `sales_per_share_growth_1_year` | SPS Growth 1yr | 17,608 | -28.59 | 8.68 | 14.59 | 20.46 | **19,862.17** | 290 | None (uncapped) |
| `sales_per_share_growth_3_year` | SPS Growth 3yr | 17,621 | -26.95 | 6.76 | 10.79 | 16.03 | 249.16 | 554 | None (uncapped) |
| `operating_cash_flow_growth_rate_3yr` | OCF Growth 3yr | 17,553 | -39.60 | 12.07 | 19.37 | 26.53 | 237.53 | 515 | None |

**Key Finding — Percentage Point Scale:** All growth rates are stored as **percentage points** (e.g., `18.13` means 18.13% growth), NOT decimal fractions. This is the provider's native representation. The extractor passes the `value` float through unchanged. **Decision: Convert to decimal fractions** (`value / 100.0`) during extraction for consistency with the profile endpoint's expense ratios and the Roadmap's percentage-to-decimal convention (`15.5%` → `0.155`).

**Key Finding — Provider-Side Caps on EPS Growth:** EPS growth rates are clipped to `[-50, 100]` percentage points by the provider. Sales growth and SPS growth are NOT consistently capped — `sales_per_share_growth_1_year` has an extreme outlier at 19,862% (GROW, Schroder Real Return Fund, Aug 2021, likely a corporate restructuring artefact).

###### C.3. Profitability & Return Metrics (from `ratios` section)

| Metric ID | Human Name | Obs | Min | P01 | Median | P99 | Max | Neg | Anomalies |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `return_on_assets_1yr` | ROA 1yr | 17,624 | -91.94 | -10.15 | 8.57 | 31.57 | 102.13 | 617 | Negatives from loss-making biotech/cannabis holdings. |
| `return_on_assets_3yr` | ROA 3yr | 17,548 | -58.35 | — | 7.50 | — | 104.59 | 672 | |
| `return_on_equity_1yr` | ROE 1yr | 17,624 | **-2,103.39** | -30.36 | 24.01 | 372.54 | **1,865.97** | 703 | Extreme tails from highly leveraged split corp structures. |
| `return_on_equity_3yr` | ROE 3yr | 17,603 | -170.92 | — | 19.16 | — | **2,971,172.92** | 709 | SPLT (Brompton Split Corp Pref) dominates extremes. |
| `return_on_investment_1yr` | ROI 1yr | 17,622 | -197.56 | — | 14.55 | — | 993.60 | 535 | |
| `return_on_investment_3yr` | ROI 3yr | 17,546 | -65.54 | — | 12.77 | — | 1,397.67 | 612 | |
| `return_on_capital` | ROC | 17,606 | -901.57 | — | 16.39 | — | 4,277.95 | 57 | |
| `return_on_capital_3yr` | ROC 3yr | 17,418 | -174.30 | — | 15.32 | — | 235.48 | 16 | |

**Key Finding — Extreme Return Metrics:** ROE 3yr reaches 2,971,173% for SPLT (Brompton Split Corp Preferred ETF) due to near-zero book equity in split-corp/leveraged structures. These are mathematically correct given the extreme leverage ($\text{Total Assets} / \text{Total Equity} = 263{,}821\times$) but are severe regression outliers. **Decision: Winsorize profitability metrics at the panel layer (Stage 2)**, not in the Silver extractor. Store raw values for auditability.

###### C.4. Financial Health / Leverage Ratios (from `ratios` section)

| Metric ID | Human Name | Obs | Min | P25 | Median | P75 | Max | Neg | Anomalies |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `ebit_to_interest` | EBIT/Interest | 17,582 | -5,235.26 | 26.12 | 76.69 | 138.77 | **1,303,456.78** | 500 | Max from Chinese small-cap ETF; negative from unprofitable drone/biotech ETFs. |
| `lt_debt_shareholders_equity` | LT Debt/SE | 17,587 | 0.0008 | 0.51 | 0.75 | 0.95 | 25.98 | 0 | |
| `total_assets_total_equity` | TA/TE | 17,622 | 1.01 | 3.17 | 4.25 | 5.65 | **263,821.09** | 0 | SPLT (Brompton Split Corp) at 263,821×; mechanically correct. |
| `total_debt_total_capital` | TD/TC | 17,604 | 0.002 | 0.69 | 0.95 | 1.22 | 26.14 | 0 | Values > 1.0 when negative equity. |
| `total_debt_total_equity` | TD/TE | 17,609 | 0.002 | 0.33 | 0.39 | 0.45 | 3.83 | 0 | |
| `sales_to_total_assets` | Sales/TA | 17,622 | 0.008 | 0.53 | 0.65 | 0.75 | 3.94 | 0 | |

**Key Finding — Unbounded Leverage:** Financial leverage ratios (TA/TE, EBIT/Interest) can be astronomically large for leveraged split-corp and Chinese small-cap ETFs. These are not data errors. **Decision: Preserve raw values in Silver; winsorize at panel layer.**

###### C.5. Dividend Metrics (from `dividend` section)

| Metric ID | Human Name | Obs | Min | Median | Max | Neg | Zero | Anomalies |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dividend_yield_weighted_average` | Div Yield (Wtd Avg) | 15,091 | -0.08 | 1.57 | 15.79 | 1 | 21 | 1 negative (PGRX, -0.08%); 21 zeros from non-dividend biotech/cannabis/crypto ETFs. |
| `dividendpayoutratio5yr` | Div Payout Ratio 5yr | 17,474 | 0.61 | 50.34 | **4,473.22** | 0 | 0 | Extreme highs from clean energy ETFs (TAN: 2,195%, ACES: 2,664%) where payout vastly exceeds earnings. |
| `price_to_dividend` | Price/Dividend | 17,470 | 7.29 | 158.90 | 5,018.75 | 0 | 0 | Extreme highs from near-zero-yield ETFs. |
| `dividend_per_share_1yr` | DPS Growth 1yr | 17,427 | -100.00 | 15.17 | 228.24 | 589 | 0 | Min at -100% (complete dividend elimination). Units: percentage points. |
| `dividend_per_share_3yr` | DPS Growth 3yr | 17,384 | -37.03 | 13.13 | 172.69 | 281 | 0 | |

**Note:** `Dividend_Yield_Weighted_Average` appears exclusively in the `ratios_equity.json` fixture — absent from `ratios_complete.json`. It is present on 15,091 product-dates (same count as zscore metrics), indicating it is only reported for products within the Morningstar equity peer comparison universe.

###### C.6. Fixed Income Metrics (from `fixed_income` section)

| Metric ID | Human Name | Obs | Unique ETFs | Min | Median | Max | Neg | Anomalies |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `yield_to_maturity` | Yield to Maturity | 7,460 | 4,830 | -8.73 | 4.36 | **10.00** | 24 | Provider cap at 10.0 (15 records exactly at 10.0, e.g. IPRV, BLTN). Negative YTM from convertible bond ETFs (CVRT: -8.73%, MCVT: -4.78%). |
| `nominal_maturity` | Nominal Maturity (years) | 7,466 | 4,834 | -0.96 | 7.44 | 96.25 | 4 | Negative from crypto/derivatives ETFs (SETH, BETE, BETH at -0.96yr). Units: years. |
| `effective_maturity` | Effective Maturity (years) | 7,466 | 4,834 | -5.13 | 6.90 | 58.97 | 5 | Negative from Fubon S&P US Pref Stock ETF (-5.13yr) and crypto ETFs. Units: years. |
| `average_coupon` | Average Coupon (%) | 6,673 | 4,349 | 0.13 | 4.17 | 14.38 | 0 | Units: percentage points. |
| `average_quality` | Average Quality | 6,592 | 4,320 | 3.00 | 7.16 | **10.00** | 0 | **Non-numeric `value_fmt`** — the only metric where `value_fmt` is a credit rating letter grade. |

**Key Finding — `average_quality` Dual Representation:** This metric has a **numeric continuous score** in `value` (range 3.0–10.0) and a **letter-grade string** in `value_fmt`/`raw_value` (one of `AAA`, `AA`, `A`, `BBB`, `BB`, `B`, `CCC`, `CC`, `-`). The numeric score is a weighted portfolio-level average that does NOT fall on integer boundaries. The raw_value to value mapping is:

| `raw_value` (Letter Grade) | Numeric Range | Nominal Integer Midpoint |
| :--- | :--- | :--- |
| `-` (Not Rated / N/A) | 10.0 | 10 |
| `AAA` | 9.0 | 9 |
| `AA` | [8.0, 9.0) | 8 |
| `A` | [7.0, 8.0) | 7 |
| `BBB` | [6.0, 7.0) | 6 |
| `BB` | [5.0, 6.0) | 5 |
| `B` | [4.0, 5.0) | 4 |
| `CCC` | [3.0, 4.0) | 3 |
| `CC` | [2.99, 3.0) | — |

The letter grade in `raw_value` is simply the floor of the numeric score mapped to a credit tier. The provider already computes the continuous numeric score — the extractor stores this numeric score directly in `value`. **Decision: Retain the provider's continuous numeric score as-is.** No additional ordinal mapping is needed. The `raw_value` letter grade is preserved for auditability.

**Key Finding — `-` Grade (Not Rated):** 33 records use `raw_value = '-'` with `value = 10.0`. These belong to commodity/cryptocurrency ETFs (e.g., IGLN iShares Physical Gold, GOLD, Bitcoin ETFs) that hold no rated fixed-income securities. The provider assigns the maximum score of 10.0 to these. **Decision: Retain value 10.0 in Silver; flag for special handling at panel layer** — these should either be excluded from fixed-income quality factor regressions or mapped to `NULL`.

**Key Finding — Fixed Income Metrics on Equity ETFs (Balanced / Multi-Asset):** 
When evaluating funds on their point-in-time latest asset allocation snapshot (`dimension_name = 'Equity'` and `value > 0.5`), exactly **205 to 254 unique equity-dominant ETFs** report fixed-income metrics:
- `average_coupon`: 205 ETFs
- `average_quality`: 206 ETFs
- `yield_to_maturity`: 253 ETFs
- `effective_maturity`: 254 ETFs
- `nominal_maturity`: 254 ETFs

*(Methodology Note: A historical query summing equity sleeve weights across all dates yielded 305 ETFs due to multi-snapshot accumulation; evaluating per point-in-time snapshot confirms the true cross-asset cohort is 205–254 ETFs).* These represent balanced, target-date, and multi-asset ETFs with minority debt sleeves. They are legitimate cross-asset observations and must be retained in Silver.

###### C.7. Z-Score Metrics (from `zscore` section)

| Metric ID | Human Name | Obs | Min | P25 | Median | P75 | Max |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `average_final_composite_zscore` | Avg Final Composite Z | 15,091 | -1.35 | -0.16 | -0.01 | 0.14 | 1.74 |
| `latest_composite_z_score` | Latest Composite Z (explicit underscores) | 15,091 | -1.47 | -0.19 | -0.01 | 0.13 | 1.74 |
| `latest_dividend_yield_zscore` | Div Yield Z | 15,091 | -6.65 | -0.24 | -0.06 | 0.25 | 1.66 |
| `latest_price_to_book_zscore` | P/B Z | 15,091 | -1.33 | -0.25 | -0.09 | 0.09 | 3.03 |
| `latest_price_to_earnings_zscore` | P/E Z | 15,089 | -1.33 | -0.17 | -0.02 | 0.21 | 2.36 |
| `latest_price_sales_zscore` | P/S Z | 15,087 | -1.80 | -0.27 | -0.13 | 0.05 | 7.03 |
| `latest_return_on_equity_zscore` | ROE Z | 15,091 | -2.96 | -0.23 | -0.07 | 0.05 | 4.15 |
| `latest_sps_growth_zscore` | SPS Growth Z | 15,091 | -2.61 | -0.19 | -0.07 | 0.06 | 3.74 |
| `weighted_final_composite_zscore` | Wtd Final Composite Z | 15,091 | -1.35 | -0.17 | -0.01 | 0.13 | 1.74 |

**Key Finding — Z-Scores Metric Naming & Peer-Group Dropouts:** 
1. **Explicit Tag Identifier:** Note that `latest_composite_z_score` contains underscores between all words (`_z_score`), whereas all other component metrics terminate in `_zscore`. Queries filtering on `LIKE '%zscore'` silently omit `latest_composite_z_score`.
2. **Peer Universe Dropouts:** 7 of the 9 z-scores report exactly 15,091 observations, precisely matching `dividend_yield_weighted_average` (the Morningstar equity peer comparison universe). `latest_price_to_earnings_zscore` drops 2 records (15,089 obs) and `latest_price_sales_zscore` drops 4 records (15,087 obs) where constituent metrics cannot be calculated.

##### D. Product-Date Metric Completeness Profiles

The number of metrics reported per `(product_id, effective_date)` clusters into three natural cohorts:

| Cohort | Metric Count | Product-Dates | Description |
| :--- | :--- | :--- | :--- |
| Bond-only | 5 | 6,105 | Fixed-income ETFs with only the 5 `fixed_income` section metrics. |
| Equity (no z-scores) | 32 | 1,801 | Equity ETFs with `ratios` + `financials` + `dividend` (no `zscore` or FI). |
| Equity (full) | 42 | 14,899 | Equity ETFs with all sections including `zscore` + `dividend_yield_weighted_average`. |
| Mixed / Balanced | 37–47 | ~560 | Multi-asset ETFs with partial FI + full equity metrics. |

##### E. Empirical Cadence & Update Delta Analysis

| Analysis Dimension | Value |
| :--- | :--- |
| Total product-date pairs with prior observation | 7,992 |
| Min delta | 30 days |
| P5 / P25 / **Median** / P75 / P95 | 31 / 31 / **31.0 days** / 31 / 31 |
| Max delta | 853 days |
| Mode | 31 days (7,925 of 7,992 = 99.2%) |

The reporting cadence is **strict monthly** — 99.2% of consecutive observations are exactly 31 days apart (month-end to month-end). Long-tail gaps include:
- 62 days (19 pairs): Skipped one month-end.
- 91–92 days (12 pairs): Skipped two months.
- 822–853 days (~2.3 years, 4 pairs): Funds with prolonged reporting gaps (likely halted/reorganized products).

---

#### 4.2.2. Exact Functional Cleaning Rules & Transformation Specifications

##### 1. Date Extraction (`as_of_date`)
- **Payload Location:** Top-level `payload["as_of_date"]` — a Unix epoch millisecond timestamp.
- **Date Precedence:**
  - Convert to UTC date: `datetime.fromtimestamp(as_of_date / 1000.0, tz=UTC).date()`.
  - Assign `effective_date_source = 'payload'`.
  - If `as_of_date` is `None`, `0`, or absent, fallback to `snapshot_created_at.date()` with `effective_date_source = 'snapshot'`.
- **Empirical Validation:** 100% of metric-bearing payloads have valid `as_of_date`. The fallback path has never been exercised for a payload with actual metric data.
- **Current Implementation:** Already correct in `extract_ratios()` via `parse_effective_date()`. No change needed.

##### 2. Metric ID Normalization
- **Current Implementation:** `sanitize_metric_id(tag)` lowercases and replaces non-alphanumeric sequences with `_`, stripping leading/trailing underscores.
- **Examples:** `EPS_growth_1yr` → `eps_growth_1yr`, `LT_Debt_Shareholders_Equity` → `lt_debt_shareholders_equity`, `DividendPayoutRatio5yr` → `dividendpayoutratio5yr`.
- **No change needed.** All 47 tags normalize cleanly to unique lowercase identifiers.

##### 3. Value Extraction & Percentage-Point Normalization
- **Current Behavior:** The extractor reads `item["value"]` (a float) directly as `val_float = float(value)` and stores it as `value DOUBLE`.
- **Percentage-Point Scale Normalization:** Growth rates, return metrics, yields, coupons, dividend payout, and relative strength are delivered by the provider in **percentage points** (e.g., `18.13` means 18.13%, `2.20` means +2.20% outperformance).
- **Required Transformation — Decimal Fraction Conversion:** Exactly **24 metrics** must be divided by 100.0 during extraction to produce operable decimal fractions consistent with `profile` expense ratios and the Roadmap convention:

  $$\text{value} = \frac{\text{item["value"]}}{100.0}$$

  **The 24 Metrics Requiring `/100.0` Conversion (percentage points → decimal fraction):**

  | Category | Metric IDs | Count |
  | :--- | :--- | :---: |
  | EPS Growth | `eps_growth_1yr`, `eps_growth_3yr`, `eps_growth_5yr` | 3 |
  | Sales Growth | `sales_growth_1_year`, `sales_growth_3_year`, `sales_growth_5_yr` | 3 |
  | SPS Growth | `sales_per_share_growth_1_year`, `sales_per_share_growth_3_year` | 2 |
  | OCF Growth | `operating_cash_flow_growth_rate_3yr` | 1 |
  | Profitability / Return | `return_on_assets_1yr`, `return_on_assets_3yr`, `return_on_equity_1yr`, `return_on_equity_3yr`, `return_on_investment_1yr`, `return_on_investment_3yr`, `return_on_capital`, `return_on_capital_3yr` | 8 |
  | Dividend Metrics | `dividend_yield_weighted_average`, `dividendpayoutratio5yr`, `dividend_per_share_1yr`, `dividend_per_share_3yr` | 4 |
  | Fixed Income Yield/Coupon | `yield_to_maturity`, `average_coupon` | 2 |
  | Relative Return | `relative_strength` | 1 |
  | **Total Scaled** | | **24** |

  **The 23 Metrics Stored as Pure Ratios, Multiples, or Dimensionless Scores (NO `/100` conversion):**

  | Category | Metric IDs | Count |
  | :--- | :--- | :---: |
  | Valuation Multiples | `price_sales`, `price_cash`, `price_book`, `price_earnings`, `price_to_dividend` | 5 |
  | Financial Health / Leverage | `ebit_to_interest`, `lt_debt_shareholders_equity`, `total_assets_total_equity`, `total_debt_total_capital`, `total_debt_total_equity`, `sales_to_total_assets` | 6 |
  | Fixed Income Tenor / Grade | `nominal_maturity` (years), `effective_maturity` (years), `average_quality` (continuous score 3.0–10.0) | 3 |
  | Morningstar Peer Z-Scores | `average_final_composite_zscore`, `latest_composite_z_score`, `latest_dividend_yield_zscore`, `latest_price_sales_zscore`, `latest_price_to_book_zscore`, `latest_price_to_earnings_zscore`, `latest_return_on_equity_zscore`, `latest_sps_growth_zscore`, `weighted_final_composite_zscore` | 9 |
  | **Total Unscaled** | | **23** |

##### 4. `raw_value` Handling
- **Current Behavior:** `raw_value = str(item.get("value_fmt") if item.get("value_fmt") is not None else value)`.
- **No change needed.** The `value_fmt` string is the provider's display-formatted value (e.g., `"7.86"`, `"BBB"`, `"-5.13"`). It is stored as-is for auditability.

##### 5. `average_quality` — Non-Numeric `raw_value`
- **Unique Quirk:** This is the **only metric** across all 47 where `value_fmt` is not a numeric string. It contains credit rating letter grades: `AAA`, `AA`, `A`, `BBB`, `BB`, `B`, `CCC`, `CC`, `-`.
- **No special handling needed in the extractor:** The numeric `value` field already contains the provider's continuous credit quality score (3.0–10.0). The extractor stores this float directly. The letter-grade `value_fmt` is preserved in `raw_value`.
- **Panel Layer Note:** Products with `raw_value = '-'` and `value = 10.0` (commodity/crypto ETFs with no rated bonds) should be treated as `NULL` quality during fixed-income factor regression rather than a real "best quality" signal.

##### 6. Provider-Side Caps — Silver Retention Policy
- **Valuation Multiples (P/E ≤ 60, P/B ≤ 25, P/S ≤ 50, P/Cash ≤ 60, YTM ≤ 10):** These are pre-applied by the data provider. Records at exact cap values are valid ceiling-censored observations. **Retain in Silver as-is.** During panel-layer factor regression, consider right-censored treatment (Tobit models or indicator dummies) for records at exact cap values.
- **EPS Growth Caps ([-50, 100]):** Provider-side clipping on EPS growth rates only. SPS growth and OCF growth are uncapped. **Retain in Silver as-is.**
- **No additional Silver-layer capping or clamping.** All extreme values (ROE 3yr at 2,971,173%, TA/TE at 263,821×, EBIT/Interest at 1,303,457×) are retained for auditability. Winsorization occurs at the panel layer (Stage 2).

##### 7. Error & Empty Payload Handling
- **Error payloads** (`{"type": "IDENTIFICATION_PROBLEM", ...}`): Contain no section lists. The extractor's `if not payload: return result` handles them when the payload is falsy, and the section-iteration loop produces zero items when the keys are absent.
- **Empty payloads** (`{"ratios":[], "financials":[], ...}`): Contain empty lists in all sections. The extractor correctly produces zero Silver rows.
- **No additional error handling is needed.** The current implementation is robust to both cases.

---

#### 4.2.3. Recommended LOCF Staleness Caps for Factor Panel

Based on the empirical cadence audit (99.2% of deltas exactly 31 days):

| Feature / Metric Group | Empirical Update Cadence | Recommended Panel Staleness Cap | Operational Rationale |
| :--- | :--- | :--- | :--- |
| **All 22 `ratios` section metrics** (valuation multiples, profitability, leverage) | Monthly (median 31d) | **6 Months (180 days)** | 99%+ report within 62 days. Gaps exceeding 6 months indicate fund restructuring or data cessation. |
| **All 6 `financials` section metrics** (growth rates) | Monthly (median 31d) | **6 Months (180 days)** | Co-reported with `ratios` section on the same `as_of_date`. |
| **All 5 `fixed_income` section metrics** (YTM, maturity, coupon, quality) | Monthly (median 31d) | **6 Months (180 days)** | Co-reported on the same `as_of_date`. Fixed-income characteristics shift slowly. |
| **All 5 `dividend` section metrics** (yield, payout, DPS growth) | Monthly (median 31d) | **6 Months (180 days)** | Co-reported on the same `as_of_date`. |
| **All 9 `zscore` section metrics** (composite & component z-scores) | Monthly (median 31d) | **6 Months (180 days)** | Cross-sectional z-scores are peer-relative and recalculated monthly. Stale z-scores beyond 6 months lose peer relevance. |

**Uniform 6-month cap for all ratios metrics:** Unlike the `profile` endpoint which has a mix of monthly, annual, and perpetual cadences, the `ratios` endpoint reports **all metrics simultaneously on a single `as_of_date`**. There is no metric-level cadence variation — all 47 metrics share the same monthly reporting cycle. A uniform 6-month LOCF staleness cap is appropriate across the board.

---

### 4.3. Session 3 Specification: `holdings` Allocations

- **Target Endpoint:** `/tws.proxy/fundamentals/mf_holdings/`
- **Extractor:** `extract_holdings` in [extractors.py](file:///Users/alex/Documents/etfportfolio/etfportfolio/prep/extractors.py#L505-L604)
- **Test Fixtures:** `tests/fixtures/holdings_*.json` (`holdings_complete.json`, `holdings_equity.json`, `holdings_bond.json`, `holdings_empty.json`, `holdings_error.json`)
- **Bronze Snapshot Count:** 47,401 snapshots across 22,634 unique products.
- **Silver Record Counts:**
  - **1 Scalar Metric** (`silver.product_metrics`): `portfolio_top_10_concentration` (27,919 rows across 18,793 unique products).
  - **6 Active Allocation Dimensions** (`silver.product_dimensions`):
    1. `asset_class`: 76,011 rows across 18,792 unique products (4 distinct categories: `Cash`, `Other`, `Equity`, `Fixed Income`).
    2. `country`: 181,379 rows across 18,792 unique products (107 distinct country names mapped to 103 ISO codes + `Unidentified`).
    3. `industry`: 137,885 rows across 13,026 unique products (14 distinct sector categories).
    4. `credit_rating`: 38,379 rows across 5,691 unique products (12 distinct credit tiers: `AAA` through `D`, plus `Not Rated` and `Not Available`).
    5. `debt_type`: 55,331 rows across 5,691 unique products (110 distinct debt structure categories).
    6. `maturity`: 40,755 rows across 5,687 unique products (8 distinct maturity duration buckets).
  - **1 Deprecated Granular Dimension** (`silver.product_dimensions`): `top_holding` (263,186 rows across 18,793 unique products, 30,128 distinct security names) — **targeted for outright removal**.

---

#### 4.3.1. Empirical Database & Cadence Audit Findings

Across **47,401** raw Bronze snapshots of the `holdings` endpoint in `data/etf.duckdb`, the empirical audit revealed the following findings:

##### A. Payload Structure & Date Provenance

- **Payload `as_of_date`:** Present in 27,919 of 47,401 snapshots (58.9%). All 27,919 populated payloads contain a positive Unix epoch millisecond timestamp (e.g., `1785470400000` $\rightarrow$ `2026-07-31 UTC`).
- **Empty / Incomplete Snapshots:** The remaining 19,482 snapshots lack `as_of_date` or contain `{"as_of_date": 0}` (as demonstrated in `holdings_empty.json`) and have empty allocation lists. These correctly yield zero Silver rows.
- **`effective_date_source`:** 100% of populated Silver records in both `silver.product_metrics` (27,919 rows) and `silver.product_dimensions` (529,740 non-theme rows) use `'payload'`. No snapshot-date fallback is utilized for metric/dimension-bearing payloads.
- **Effective Date Range & Alignment:** Effective dates range from `2015-01-31` to `2026-08-31`, concentrated on calendar month-ends. Date alignment between metrics and dimensions is nearly 100% congruent (only 2 product-dates have concentration without dimensions, and 0 product-dates have dimensions without concentration).
- **Discarded Payload Breakdowns:**
  - `currency`: Contains currency breakdown list (e.g. `US Dollar: 94.42%`, `<No Currency>: 0.67%`). Not extracted to avoid multicollinearity with `country` and fund reporting currency.
  - `geographic`: Contains regional geographic breakdown dictionary (`mena`, `eu`, `us`, etc.). Not extracted to prevent multicollinearity with individual `country` exposures.

##### B. Scalar Metric: `portfolio_top_10_concentration`

| Metric ID | Table Source Key | Obs Count | Unique ETFs | Min | P01 | P25 | Median | P75 | P99 | Max |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `portfolio_top_10_concentration` | `payload["top_10_weight"]` | 27,919 | 18,793 | 0.0078 | 0.0256 | 0.2509 | 0.4477 | 0.7971 | 1.3245 | **17.6872** |

- **Percentage Scaling:** The raw payload presents strings like `"30.47%"`. The extractor converts this to decimal fractions: $30.47\% \rightarrow 0.3047$.
- **Values Exceeding 1.0 (> 100%):** Exactly **990 observations** (across 671 unique ETFs, 3.5% of total) exhibit concentration $> 1.0$, with extreme outliers reaching **17.6872** ($1{,}768.72\%$).
- **Root Cause of Extreme Outliers:**
  1. **Leveraged & Inverse ETFs:** Direxion Daily 3X Bull ETFs (`EDC` at 3.93, `MEXX` at 3.58, `YINN` at 3.52) hold total return swap contracts with notional gross exposures of 300% of net assets.
  2. **Active Overlay & Gross Collateral Structures:** Products such as `JMXT` / `JMEX` (Janus Henderson Mexican Sovereign, 17.69) and `THFA` / `TFGD` (JH FA ESG Active Core, 7.38) report gross notional positions in underlying sovereign debt, repos, and collateral offsetting forward currency hedges.
- **Zero & Bounds Policy:** Minimum observed is $0.0078$ (0.78% for broad equal-weighted indices). No negative or zero concentrations exist.
- **Silver Layer Policy:** Retain the exact unconstrained decimal value in Silver for data fidelity and auditability.
- **Factor Panel Layer Policy:** Winsorize or cap at $1.0$ for standard unleveraged factor models, or isolate a `is_leveraged` indicator feature to control for gross swap exposure.

##### C. Allocation Dimensions Sum-to-1.0 & Distribution Audit

All allocation categories represent percentage breakdowns of fund net assets. The empirical audit examined the sum of weights per `(product_id, effective_date)` across all 6 active dimensions:

| Dimension Type | Snapshots | Unique ETFs | Exact $\pm 0.1\%$ | Valid $\pm 2.0\%$ | Lev $>105\%$ | Under $<95\%$ | Min Sum | Max Sum | Dates w/ Shorts |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `asset_class` | 27,917 | 18,792 | **100.0%** | **100.0%** | 0 | 0 | 1.0000 | 1.0000 | 6,792 (24.3%) |
| `country` | 27,917 | 18,792 | **63.58%** | **74.18%** | 87 | 5,365 | 0.0297 | 2.1306 | 3,052 (10.9%) |
| `industry` | 19,156 | 13,026 | **19.81%** | **75.72%** | 524 | 2,532 | -0.4331 | 3.3748 | 41 (0.2%) |
| `credit_rating` | 8,839 | 5,691 | **14.79%** | **53.76%** | 192 | 2,922 | -0.3330 | 17.3540 | 36 (0.4%) |
| `debt_type` | 8,839 | 5,691 | **10.44%** | **45.51%** | 159 | 3,315 | -0.3330 | 17.3540 | 27 (0.3%) |
| `maturity` | 8,835 | 5,687 | **14.63%** | **53.63%** | 190 | 2,917 | -0.3330 | 17.3540 | 38 (0.4%) |

###### Detailed Dimensional Findings:

1. **`asset_class` (Strict Invariant):**
   - **100.0% of all product-dates sum to exactly $1.000000 \pm 0.000015$**, confirming the provider enforces mathematical balance across the 4 asset classes:
     - `Cash`: Present in 23,805 dates. 2,080 dates (7.4%) have **negative cash** (down to $-4.17$, e.g., borrowing/overdraft or repo commitments); 429 dates exceed $+1.0$ (up to $+420.87$ in inverse leveraged ETFs holding 100% cash + short swaps).
     - `Other`: Present in 20,637 dates. 5,037 dates (18.0%) have negative values (down to $-420.97$, representing the mark-to-market liability of short derivative swap positions).
     - `Equity`: Present in 18,283 dates. Median weight 0.9960. 17 dates have small negative short equity hedges (down to $-1.12$); 1,173 dates exceed $1.0$ (up to $2.16$ in leveraged long equity).
     - `Fixed Income`: Present in 13,286 dates. Median weight 0.7700. 20 dates have negative bond short hedges; 889 dates exceed $1.0$ (up to $17.35$ in leveraged bond funds like `UDN`).
   - **Leveraged Short Exemplar:** In `RKLZ` (Defiance Daily Target 2X Short RKLB ETF, 2026-08-31), `Cash = +420.87`, `Fixed Income = +1.11`, `Other = -420.97`, precisely summing to $1.000000$.

2. **`country` (Truncation & ISO Code Mapping):**
   - **Sum Behavior:** Median sum is $1.000000$. 70.3% sum to exactly $1.0$. The remaining 29.7% sum to less than $1.0$ (P01: 0.68) because the provider's payload only returns top countries or truncates negligible weights.
   - **Negative Country Weights:** 3,052 dates exhibit negative country weights (primarily in `Unidentified`, down to $-420.22$, which acts as the offsetting derivative bucket for currency/equity short futures).
   - **Country Codes & Names Audit (107 names):**
     - 104 countries have mapped codes, 3 names have `dimension_code = NULL`:
       - `Unidentified`: 17,546 rows. Represents multi-asset derivatives, synthetic swaps, or unassigned holdings.
       - `Guam`: 121 rows (median weight 0.31%). Standard ISO-3166-1 alpha-2 is `GU`.
       - `Uzbekistan`: 6 rows (median weight 1.26%). Standard ISO-3166-1 alpha-2 is `UZ`.
     - **ISO Code Collision:** `dimension_code = 'CR'` is assigned to both `Costa Rica` and `Croatia`. In ISO-3166-1 alpha-2:
       - `Costa Rica` $\rightarrow$ `CR`
       - `Croatia` $\rightarrow$ `HR` (Croatia's native code is `HR` / Hrvatska; `CR` was mistakenly emitted by the provider).
     - **3-Letter Alpha-3 Code:** `Bulgaria` has `dimension_code = 'BGR'`. In ISO-3166-1 alpha-2, Bulgaria is `BG`.
     - **Non-Standard Names:** `Korea` (code `KR`) denotes South Korea (Republic of Korea); `Virgin Islands (U.S.)` (code `VI`) denotes US Virgin Islands.

3. **`industry` (14 Economic Sectors):**
   - **Reporting Scope:** Populated for 19,156 product-dates (all equity and mixed ETFs; pure bond ETFs have no industry breakdown).
   - **Sum Behavior:** Median sum is $0.9963$. 64.6% sum to $1.0 \pm 0.01$. Long-tail sums below $1.0$ occur due to non-equity or unclassified portions.
   - **Residual Categories:**
     - `Not Classified - Non Equity`: 5,117 rows (median 2.26%, max 3.37). Represents the bond/cash/commodity portion of multi-asset ETFs.
     - `Non Classified Equity`: 1,445 rows (median 0.85%, max 1.25). Represents unclassified equity stocks.
   - **Discontinued Sector Category:** `Telecommunication Services-Discontinued eff 09/19/2020` has 29 rows (historical snapshots prior to GICS reclassification into Communication Services).

4. **`credit_rating` (12 Quality Tiers):**
   - **Reporting Scope:** Populated for 8,839 product-dates (all fixed-income ETFs).
   - **String Normalization:** Raw payload names like `"% Quality/AAA"`, `"% Quality AA"`, and `"% Quality-A"` are cleaned by `clean_credit_rating()` to standard letter grades: `AAA`, `AA`, `A`, `BBB`, `BB`, `B`, `CCC`, `CC`, `C`, `D`, plus `Not Rated` and `Not Available`.
   - **Sum Behavior:** Median sum is $0.9861$. 39.6% sum to $1.0 \pm 0.01$. Residual sums $< 1.0$ reflect cash/equity components within balanced funds.
   - **High-Yield & Default Exposure:** `D` (Default) appears in 725 product-dates; `C` in 239 product-dates; `CC` in 431 product-dates.

5. **`maturity` (8 Duration Buckets):**
   - **Reporting Scope:** Populated for 8,835 product-dates (matching `credit_rating`).
   - **Categories:** Exactly 8 standardized maturity buckets:
     1. `% Maturity Less than 1 Year` (7,148 rows)
     2. `% Maturity 1 to 3 Years` (7,157 rows)
     3. `% Maturity 3 to 5 Years` (5,523 rows)
     4. `% Maturity 5 to 10 Years` (5,464 rows)
     5. `% Maturity 10 to 20 Years` (4,705 rows)
     6. `% Maturity 20 to 30 Years` (4,497 rows)
     7. `% Maturity Greater than 30 Years` (3,652 rows)
     8. `% Maturity Other` (2,609 rows)
   - **Sum Behavior:** Median sum is $0.9860$. 39.6% sum to $1.0 \pm 0.01$.

6. **`debt_type` (110 Structural Categories):**
   - **Reporting Scope:** Populated for 8,839 product-dates.
   - **Cardinality Problem:** 110 distinct debt types are recorded without standardization (e.g. `Bundesanleihen`, `Dutch State Loan`, `Gilt Treasury Stock`, `Sovereign Bond`, `CORP`, `Corporate Medium Term Notes`, `ABSY`, `CMO Whole Loan`).
   - **Clustering Requirement:** Leaving 110 distinct sparse features causes regression matrix rank deficiency. A structured mapping into **9 macroeconomic debt clusters** resolves all 110 categories with 100% coverage (see Section 4.3.2.5).

##### D. Impact of Granular `top_holding` Constituents

- **Database Burden:** `top_holding` accounts for **263,186 rows** in `silver.product_dimensions` across 18,793 ETFs and 30,128 distinct security names.
- **Sparsity:** The vast majority of stock/bond names appear in only 1 or 2 ETFs across the universe, providing zero statistical value for cross-sectional factor models while creating significant memory bloat during panel pivoting.
- **Verification of Removal:** In Roadmap Section 2.3 and FRD Section 3, `top_holding` extraction is slated for complete removal. Portfolio concentration is comprehensively preserved by `portfolio_top_10_concentration` in `silver.product_metrics`.

##### E. Empirical Cadence & Update Delta Analysis

By analyzing consecutive point-in-time deltas per `product_id` across historical snapshots:

| Analysis Dimension | Metric / Dimension Delta Distribution |
| :--- | :--- |
| Delta Sample Size ($N$) | 9,126 consecutive product-date pairs |
| Minimum Delta | 30 days |
| 1st / 5th / 25th Percentile | 31.0 / 31.0 / 31.0 days |
| **Median Delta** | **31.0 days** |
| 75th / 95th / 99th Percentile | 31.0 / 31.0 / 31.0 days |
| Maximum Delta | 853 days |
| Mode Delta | **31 days (9,044 of 9,126 pairs = 99.1%)** |
| Long-Tail Gaps | 62 days (23 pairs, skipped 1 mo); 91–92 days (17 pairs, skipped 2 mo); 822–853 days (4 pairs, prolonged fund reorganizations). |

**Conclusion:** Holdings breakdowns and portfolio concentration follow a **strict monthly cadence**. Over 99.1% of all updates occur on exact 31-day intervals corresponding to month-end reporting cycles.

---

#### 4.3.2. Exact Functional Cleaning Rules & Transformation Specifications

##### 1. Date Extraction (`as_of_date`)
- **Payload Location:** `payload["as_of_date"]` — Unix epoch millisecond timestamp.
- **Date Precedence:**
  - Convert to UTC date: `datetime.fromtimestamp(as_of_date / 1000.0, tz=UTC).date()`.
  - Assign `effective_date_source = 'payload'`.
  - If `as_of_date` is missing, $0$, or negative, fallback to `snapshot_created_at.date()` with `effective_date_source = 'snapshot'`.
- **Validation:** 100% of populated holdings payloads have valid positive millisecond timestamps.

##### 2. Outright Removal of `top_holding` Granular Extraction
- **Specification:** Delete the `for item in payload.get("top_10", []): ...` loop in `extract_holdings()` in `etfportfolio/prep/extractors.py`.
- **Rationale:** Eliminates 263,186 sparse individual security rows from `silver.product_dimensions` and speeds up database re-ingestion, adhering to Roadmap Section 2.3.
- **Replacement:** Fund concentration remains captured via scalar metric `portfolio_top_10_concentration`.

##### 3. Scalar Concentration Metric (`portfolio_top_10_concentration`)
- **Payload Location:** `payload.get("top_10_weight")` (e.g. `"30.47%"`).
- **Transformation Formula:**
  $$\text{value} = \frac{\text{float}(\text{raw\_value.replace}('\%', ''))}{100.0}$$
- **Silver Layer Retention:** Retain the exact float value (even if $> 1.0$) for auditability.
- **Panel Construction Rule:** In `silver.monthly_panel`, clip or winsorize concentration to $[0.0, 1.0]$ for standard factor models, or isolate an indicator feature `is_leveraged = 1.0` when concentration $> 1.0$.

##### 4. Allocation Dimension Scaling & Normalization
- **Payload Fields:** `allocation_self` $\rightarrow$ `asset_class`, `investor_country` $\rightarrow$ `country`, `industry` $\rightarrow$ `industry`, `debtor` $\rightarrow$ `credit_rating`, `debt_type` $\rightarrow$ `debt_type`, `maturity` $\rightarrow$ `maturity`.
- **Transformation Formula:**
  $$\text{weight} = \frac{\text{float}(item[\text{"weight"}])}{100.0}$$
  $$\text{raw\_str} = str(item.get(\text{"formatted\_weight"}, f\text{"}\{weight\_val\}\%"\}))$$
- **Negative Weights & Short Collateral:**
  - Silver Layer: Retain exact negative weights (e.g., negative cash $-4.17$, negative other $-420.97$, negative country $-420.22$) for mathematical accounting consistency and auditability.
  - Panel Layer: When creating long-only factor exposure weights, zero-floor negative exposures ($\max(0.0, w)$) and renormalize remaining weights to sum to 1.0, or create dedicated `short_exposure` features.

##### 5. Country ISO-3166 Standardizations & Code Remapping Rules
Across all 27,917 country breakdown product-dates, exactly 107 country names appear. During extraction, `dimension_code` must be standardized as follows:
- **ISO-3166-1 Alpha-2 Remaps (133 rows total):**
  - `Croatia`: Provider mistakenly emits code `CR` (colliding with Costa Rica). Remap `dimension_code` to `'HR'` (1 row).
  - `Bulgaria`: Provider emits 3-letter code `BGR`. Remap `dimension_code` to `'BG'` (5 rows).
  - `Guam`: Provider emits `NULL`. Remap `dimension_code` to `'GU'` (121 rows).
  - `Uzbekistan`: Provider emits `NULL`. Remap `dimension_code` to `'UZ'` (6 rows).
- **Residual Multi-Asset Bucket (`Unidentified`):**
  - 17,546 rows. Represents unclassified cash, currency swaps, and derivative offsets (weights down to -420.22).
  - `dimension_code` must strictly be set to `NULL` (`dimension_name = 'Unidentified'`).
- **Preserved Codes:**
  - `Costa Rica`: Retains `dimension_code = 'CR'` (25 rows).
  - `Korea`: Denotes South Korea, retains `'KR'`.
  - `Virgin Islands (U.S.)`: Retains `'VI'`.

##### 6. Industry Sector Standardization & Discontinued Category Handling
- **Payload Location:** `payload["industry"]` list entries.
- **Closed Set:** Exactly 14 categories emitted in raw Bronze.
- **Historical Discontinued Category Remap (29 rows):**
  - Raw name: `"Telecommunication Services-Discontinued eff 09/19/2020"`.
  - Cleaning Rule: Remap `dimension_name` to `"Communication Services"` to ensure longitudinal factor continuity and prevent creating a sparse, single-period factor column in `silver.monthly_panel`.

##### 7. Debt Type Cluster Mapping (110 $\rightarrow$ 9 Macroeconomic Clusters)
When building `silver.monthly_panel`, the 110 raw `debt_type` categories must be aggregated into **9 standardized fixed-income structural factors**:

| Cluster Feature ID | Cluster Description | Constituent Raw Debt Types (All 110 Covered) |
| :--- | :--- | :--- |
| `debt_sovereign` | Direct sovereign government debt, treasuries, and state loans | `Sovereign Bond`, `Bundesanleihen`, `Dutch State Loan`, `Gilt Treasury Stock`, `Irish Govt Bond`, `Japanese Govt Bond`, `Danish Govt Bond`, `Notas do Tesouro Nacional F`, `Obligaciones del Estado`, `Obligation Assimilable du Tresor`, `Oblig Assim Tresor Indexee I'Indice`, `Oblig Assim Tresor Indexee I'Inflation`, `Obligation Lineaire`, `Obrigacoes do Tesouro`, `Titulos de Tesoreria TES B`, `Treasury Bills`, `Treasury Notes/Bonds`, `Treasury STRIPS`, `MXBONO`, `UDIBONO`, `OMAN`, `Govt Guaranteed`, `Government other` |
| `debt_agency_supranational` | Government agencies, development banks, and sponsored enterprises | `Agencies`, `Small Business Administration` |
| `debt_municipal` | Local, state, and municipal tax-backed obligations | `MUNI`, `Certificates of Obligation`, `Certificates of Participation`, `Grant Antic Notes`, `Tax And Rev Antic Notes`, `Tax Antic Notes`, `Unknown Antic Types` |
| `debt_corporate_senior` | Senior, secured, covered, and investment-grade corporate obligations | `CORP`, `Corporate Medium Term Notes`, `Senior Note`, `Senior Debenture`, `Senior Bank Note`, `Senior Secured`, `Secured Bond`, `Secured Note`, `First Mortgage Bond`, `First Mortgage Note`, `First & Refunding Mortgage Bond`, `Covered Bond`, `Hypothekenpfandbrief`, `Pfandbrief Anleihe`, `Oeffentliche Pfandbrief`, `HPF Jumbo`, `Jumbo Landesschatzanweisung`, `Sakerstallda Obligationer`, `Obligations Foncieres`, `Collateral Trust`, `Collateral Debt`, `Collateralized Notes` |
| `debt_corporate_subordinated` | Subordinated, mezzanine, and hybrid capital instruments | `Subordinated Note`, `Senior Subordinated Note`, `Subordinated Bank Note`, `Subordinated Debenture`, `Senior Subordinated Debenture`, `Junior Subordinated Note`, `Junior Subordinated Debenture`, `Mezzanine Debt`, `Trust Preferred Security`, `Participaciones Preferentes` |
| `debt_securitized_mbs` | Residential and commercial mortgage-backed securities | `Mortgage Pools`, `Mortgages`, `Mortgage Bond`, `Mortgage Note`, `Second Mortgage Bond`, `Commercial Mortgage-Backed Security`, `Collateralized Mortgage Obligation`, `CMOs`, `CMO Whole Loan`, `CMO Agricultural MBS`, `TBA`, `Pass Through Certificate` |
| `debt_securitized_abs` | Consumer, auto, aircraft, equipment, and credit card asset-backed debt | `ABSY`, `Asset Backed Tranches`, `Credit Card Receivables`, `Auto/Installment Loans`, `Auto Lease Loans`, `Auto Floorplan/Wholesale Loans`, `Equipment Backed Loan`, `Aircraft Lease`, `Student Loan` |
| `debt_unsecured_general` | General unassigned debt, bank debt, notes, and depositary receipts | `Bond`, `Note`, `Unsecured Note`, `Debenture`, `Fixed Income`, `Global Bonds`, `Inhaberschuldverschreibung`, `Certificate`, `Certificates Of Indebtness`, `Other Certificates`, `Deposit Note`, `Depositary Share`, `Depository Receipts (Thailand)`, `Bank Debt`, `Bankers Acceptance`, `Trust` |
| `debt_specialty_derivatives` | Inflation-linked, Islamic sukuk, structured derivatives, warrants, and equity hedges | `Index Linked Security`, `Index-Linked Gilt`, `Islamic Sukuk`, `Derivative`, `Interest only`, `Principal only`, `Warrants`, `Preferred Stock`, `OTHER` |

##### 8. Credit Rating & Maturity Bucket Normalization
- **Credit Rating Standard Code (`[APPLIED]`):** `dimension_code` is already cleanly populated with the cleaned letter grade (`AAA`, `AA`, `A`, `BBB`, `BB`, `B`, `CCC`, `CC`, `C`, `D`, `Not Rated`, `Not Available`). Invariant: `dimension_code == dimension_name`.
- **Maturity Duration Standard Code (`[PENDING]` — 40,755 rows):** Map verbose bucket names to concise slugs in `dimension_code`:
  - `% Maturity Less than 1 Year` $\rightarrow$ `mat_lt_1y`
  - `% Maturity 1 to 3 Years` $\rightarrow$ `mat_1_to_3y`
  - `% Maturity 3 to 5 Years` $\rightarrow$ `mat_3_to_5y`
  - `% Maturity 5 to 10 Years` $\rightarrow$ `mat_5_to_10y`
  - `% Maturity 10 to 20 Years` $\rightarrow$ `mat_10_to_20y`
  - `% Maturity 20 to 30 Years` $\rightarrow$ `mat_20_to_30y`
  - `% Maturity Greater than 30 Years` $\rightarrow$ `mat_gt_30y`
  - `% Maturity Other` $\rightarrow$ `mat_other`

##### 9. Error & Empty Payload Handling
- Payloads with `as_of_date: 0` or missing `as_of_date` contain empty breakdown lists and correctly produce zero rows.
- Error payloads (`{"type": "IDENTIFICATION_PROBLEM", ...}`) contain no breakdown keys and produce zero rows.

---

#### 4.3.3. Recommended LOCF Staleness Caps for Factor Panel

Based on the empirical cadence audit (median delta: 31.0 days, 99.1% of updates at 31 days):

| Feature / Dimension Group | Empirical Update Cadence | Typical Reporting Delay | Recommended Panel Staleness Cap | Operational Rationale |
| :--- | :--- | :--- | :--- | :--- |
| `portfolio_top_10_concentration` | Monthly (median 31d) | 15–45 days | **6 Months (180 days)** | Concentration changes with monthly portfolio rebalancing. Gaps > 6 months signal inactive reporting. |
| `asset_class` allocation | Monthly (median 31d) | 15–45 days | **6 Months (180 days)** | High-level asset allocation is rebalanced monthly. |
| `country` allocation | Monthly (median 31d) | 15–45 days | **6 Months (180 days)** | Country allocations update monthly; 6-month cap covers normal quarterly rebalancing cycles. |
| `industry` allocation | Monthly (median 31d) | 15–45 days | **6 Months (180 days)** | Sector weights track underlying equities on a monthly cadence. |
| `credit_rating` quality | Monthly (median 31d) | 15–45 days | **6 Months (180 days)** | Fixed-income credit profiles update monthly. |
| `debt_type` clusters | Monthly (median 31d) | 15–45 days | **6 Months (180 days)** | Debt structure allocations update monthly. |
| `maturity` duration buckets | Monthly (median 31d) | 15–45 days | **6 Months (180 days)** | Maturity schedules update monthly as bonds mature or roll. |

**Uniform 6-month cap for all holdings entities:** All breakdowns within `mf_holdings` are reported synchronously on the exact same monthly `as_of_date`. A uniform 6-month (180-day) LOCF staleness threshold guarantees consistency across all allocation factors in `silver.monthly_panel`.

---

---

### 4.4. Session 4 Specification: `mstar` & `lipper` Evaluative Ratings

- **Target Endpoints:**
  - Morningstar: `/tws.proxy/mstar/fund/detail?conid=`
  - Lipper: `/tws.proxy/fundamentals/mf_lip_ratings/`
- **Extractors:** `extract_mstar` and `extract_lipper` in [extractors.py](file:///Users/alex/Documents/etfportfolio/etfportfolio/prep/extractors.py#L385-L503)
- **Test Fixtures:**
  - Morningstar: `tests/fixtures/mstar_*.json` (`mstar_equity.json`, `mstar_bond.json`, `mstar_empty.json`, `mstar_error.json`)
  - Lipper: `tests/fixtures/lipper_*.json` (`lipper_equity.json`, `lipper_bond.json`, `lipper_empty.json`)
- **Bronze Snapshot Counts:**
  - `mstar`: 35,072 snapshots across 22,634 unique products (32,541 valid metric-bearing, 2,531 empty, 0 HTTP errors).
  - `lipper`: 25,544 snapshots across 22,634 unique products (14,519 valid metric-bearing, 11,025 empty).
- **Silver Record Counts (Current Baseline):**
  - `mstar`: 84,221 metric rows across 16,454 unique products (10 distinct `metric_id` values).
  - `lipper`: 1,064,394 metric rows across 11,650 unique products (568 distinct `metric_id` values across 37 geographic universes).
- **Scope of Ingested Entities:**
  - **Morningstar (8 Evaluative Metrics):**
    1. `mstar_medalist_rating`: Forward-looking qualitative/quantitative conviction score (1.0 to 5.0).
    2. `mstar_analyst_coverage_pct`: Proportion of the 3 foundational pillars evaluated by human analysts ($0.0, 0.3333, 0.6667, 1.0$).
    3. `mstar_morningstar_rating`: Backward-looking mathematical risk-adjusted performance stars (1.0 to 5.0).
    4. `mstar_sustainability_rating`: Corporate portfolio ESG risk globe rating (1.0 to 5.0).
    5. `mstar_people_analyst` / `mstar_people_quant`: People pillar evaluation (1.0 to 5.0).
    6. `mstar_process_analyst` / `mstar_process_quant`: Process pillar evaluation (1.0 to 5.0).
    7. `mstar_parent_analyst` / `mstar_parent_quant`: Parent asset management firm evaluation (1.0 to 5.0).
  - **Lipper (20 Canonical Evaluative Metrics across 4 Horizons):**
    1. `lipper_total_return_{horizon}`: Overall historical return vs peers (1.0 to 5.0).
    2. `lipper_consistent_return_{horizon}`: Risk-adjusted return consistency vs peers (1.0 to 5.0).
    3. `lipper_preservation_{horizon}`: Downside risk / capital preservation vs broad asset class (1.0 to 5.0).
    4. `lipper_expense_{horizon}`: Fee competitiveness vs open-end mutual funds and ETFs in peer group (1.0 to 5.0).
    5. `lipper_tax_efficiency_{horizon}`: After-tax return retention vs peers (1.0 to 5.0, US products only).
    - Horizons: `overall`, `3yr`, `5yr`, `10yr`.
  - **Dimensions (`silver.product_dimensions`):** 0 active dimensions. (Morningstar Style Box dimensions are ingested separately via `extract_profile` from the `profile` endpoint, documented in Section 4.1).

---

#### 4.4.1. Empirical Database & Cadence Audit Findings

Across **35,072** Morningstar snapshots and **25,544** Lipper snapshots in `data/etf.duckdb`, the empirical audit revealed the following findings:

##### A. Morningstar Payload Architecture & Ingestion Audit

###### A.1. Payload Structure & Date Provenance
- **Top-Level Keys:** Valid payloads contain `['as_of_date', 'commentary', 'q_full_report_id', 'summary']`.
- **Top-Level `as_of_date`:** Present in 17,157 snapshots as an 8-digit date string (e.g., `"20260731"` $\rightarrow$ `2026-07-31`). Absent in 15,384 valid payloads.
- **Pillar-Level `publish_date`:** In `summary`, each rating item carries its own `publish_date` string (e.g. `"20260427"`, `"20250716"`). In `silver.product_metrics`, **99.996% (84,218 of 84,221 rows)** use `effective_date_source = 'item'`, with only 3 rows falling back to snapshot date.
- **Asynchronous Pillar Dates:** Different pillars on the exact same fund and snapshot carry vastly different publication dates. For example, in `mstar_equity.json` (SPY):
  - `morningstar_rating`: `2026-07-31` (monthly recalculation)
  - `sustainability_rating`: `2026-06-30` (monthly recalculation)
  - `medalist_rating`: `2026-04-27` (analyst report publication)
  - `process`: `2026-04-27` (analyst report publication)
  - `parent`: `2025-07-16` (firm-level annual review conducted 9 months earlier)
- **Discarded Non-Metric Payload Fields:**
  - `commentary`: Long narrative qualitative text reviews written by analysts (e.g. Brendan McCann) or automated text generators (`"Morningstar Automated Analysis"`). Contains no structured numerical factor data.
  - `q_full_report_id`: Hexadecimal report identifier string (e.g. `"63723206e26a17cb57bfd41a"`).
  - `category` (in `summary`): 512 distinct text classifications (e.g., `"Large Blend"`, `"Japan Large-Cap Equity"`). Skipped as an informative text classification rather than a factor score.
  - `category_index` (in `summary`): 415 distinct benchmark index names (e.g., `"Morningstar US Large-Mid TR USD"`). Skipped.
- **Special/Non-Numeric String Values:**
  - `medalist_rating = 'Under_Review'`: Observed 2 times across the database.
  - `people = 'Not_Applicable'`: Observed 2 times.
  - `process = 'Not_Applicable'`: Observed 2 times.
  - The current extractor skips these via `if norm_val in ("under_review", "not_applicable", ...): continue`. This correctly prevents runtime crashes and avoids corrupting numeric scores.

###### A.2. The Medalist Analyst vs. Quantitative Rating Bug & Pillar Composition
In Morningstar's 2023 unified framework, the **Medalist Rating** (`id = "medalist_rating"`) serves as the overall 5-tier forward-looking rating (Gold, Silver, Bronze, Neutral, Negative).
- **The Extractor Bug:** In raw payloads, `medalist_rating` always arrives with `q: false` (or `q` omitted). The legacy extractor checked:
  ```python
  is_quant = bool(pillar.get("q") is True or pillar_key.startswith("q_"))
  ```
  Because `pillar.get("q")` is `False`, the extractor mislabeled **10,755 medalist ratings as `mstar_medalist_rating_analyst`**, while only 4 records were labeled `mstar_medalist_rating_quant` (from an obsolete `quantitative_rating` tag used in early 2023).
- **Empirical Pillar Composition of Medalist Funds:**
  Across historical snapshots (18,881 snapshots) and when evaluated strictly on the **latest snapshot per fund** (10,746 unique ETFs with Medalist ratings), the empirical distribution is:

| Profile (`People-Process-Parent`) | Category | Latest Fund Count | % of Medalist Universe | Historical Snapshot Count | % Snapshots | Nature of Rating |
| :---: | :--- | :---: | :---: | :---: | :---: | :--- |
| `A-Q-A` | Hybrid | 4,119 | **38.3%** | 7,138 | 37.8% | Analyst People & Parent, Quant Process |
| `Q-Q-A` | Hybrid | 2,603 | **24.2%** | 4,711 | 25.0% | Analyst Parent, Quant People & Process |
| `A-A-A` | Pure Analyst (3A) | 2,404 | **22.4%** | 4,124 | 21.8% | 100% Human Analyst Coverage |
| `Q-Q-Q` | Pure Quant (3Q) | 1,414 | **13.2%** | 2,567 | 13.6% | 100% Machine Learning Derived |
| `Q-A-A` | Hybrid | 137 | **1.3%** | 234 | 1.2% | Analyst Process & Parent, Quant People |
| `Q-A-Q` | Hybrid | 46 | **0.4%** | 69 | 0.4% | Analyst Process, Quant People & Parent |
| `A-A-Q` | Hybrid | 12 | **0.1%** | 21 | 0.1% | Analyst People & Process, Quant Parent |
| `A-Q-Q` | Hybrid | 11 | **0.1%** | 17 | 0.1% | Analyst People, Quant Process & Parent |
| **Total** | | **10,746** | **100.0%** | **18,881** | **100.0%** | **64.4% Hybrid, 22.4% Analyst, 13.2% Quant** |

- **Significance for Quantitative Factor Modeling:**
  Only **22.4%** of Medalist ETFs are fully covered by human analysts. **13.2%** are 100% machine-learning derived, and **64.4%** are hybrid structures. This predominance of hybrid ratings occurs because institutional parents (BlackRock, Vanguard, State Street) receive firm-level qualitative reviews, while index replication processes are evaluated algorithmically.

##### B. Morningstar Statistical Distributions & Rating Scales

###### B.1. Statistical Summary Across Current Silver Metrics

| Metric ID | Description | Obs Count | Unique ETFs | Min | P05 | P25 | Median | P75 | P95 | Max |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `mstar_medalist_rating_analyst` | Medalist Rating (Mislabeled) | 10,755 | 10,742 | 1.0 | 2.0 | 2.0 | **3.0** | 4.0 | 5.0 | 5.0 |
| `mstar_medalist_rating_quant` | Medalist Rating (Legacy tag) | 4 | 4 | 3.0 | 3.2 | 3.8 | **4.0** | 4.3 | 4.9 | 5.0 |
| `mstar_morningstar_rating` | Star Rating (1..5 Stars) | 14,067 | 9,233 | 1.0 | 1.0 | 3.0 | **3.0** | 4.0 | 5.0 | 5.0 |
| `mstar_sustainability_rating` | Sustainability (1..5 Globes) | 27,128 | 15,742 | 1.0 | 1.0 | 2.0 | **3.0** | 4.0 | 5.0 | 5.0 |
| `mstar_people_analyst` | People Pillar (Analyst) | 6,557 | 6,546 | 2.0 | 3.0 | 4.0 | **4.0** | 4.0 | 4.0 | 5.0 |
| `mstar_people_quant` | People Pillar (Quant) | 4,202 | 4,202 | 1.0 | 2.0 | 3.0 | **3.0** | 3.0 | 4.0 | 5.0 |
| `mstar_process_analyst` | Process Pillar (Analyst) | 2,610 | 2,599 | 1.0 | 2.0 | 3.0 | **4.0** | 4.0 | 5.0 | 5.0 |
| `mstar_process_quant` | Process Pillar (Quant) | 8,149 | 8,149 | 1.0 | 1.0 | 2.0 | **3.0** | 3.0 | 4.0 | 5.0 |
| `mstar_parent_analyst` | Parent Pillar (Analyst) | 9,266 | 9,264 | 2.0 | 2.0 | 3.0 | **3.0** | 4.0 | 5.0 | 5.0 |
| `mstar_parent_quant` | Parent Pillar (Quant) | 1,483 | 1,483 | 1.0 | 1.0 | 2.0 | **2.0** | 3.0 | 4.0 | 5.0 |

###### B.2. Frequency Distribution by Rating Category

```
mstar_medalist_rating:
  Gold     (5.0):  1,773 ( 9.4%)  ███
  Silver   (4.0):  1,371 ( 7.3%)  ██
  Bronze   (3.0):  2,716 (14.4%)  █████
  Neutral  (2.0):  4,391 (23.3%)  ████████
  Negative (1.0):    508 ( 2.7%)  █
  [Unrated/None]:  8,122 (43.0%)  (Active/uncovered funds)

mstar_morningstar_rating:
  5 Stars  (5.0):  1,794 (12.8%)  ████
  4 Stars  (4.0):  4,025 (28.6%)  █████████
  3 Stars  (3.0):  4,888 (34.7%)  ███████████
  2 Stars  (2.0):  2,368 (16.8%)  █████
  1 Star   (1.0):    992 ( 7.1%)  ██

mstar_sustainability_rating:
  High          (5.0):  3,022 (11.1%)  ████
  Above Average (4.0):  5,653 (20.8%)  ███████
  Average       (3.0): 11,088 (40.9%)  ██████████████
  Below Average (2.0):  5,260 (19.4%)  ███████
  Low           (1.0):  2,105 ( 7.8%)  ███
```

- **Pillar Distribution Asymmetry:**
  - Human analysts evaluate **People** overwhelmingly as `Above_Average` (5,600 of 6,557 = 85.4%), reflecting favorable views of institutional trading desks at BlackRock, Vanguard, and State Street. Quant People scores center symmetrically on `Average` (65.5%).
  - Human analysts evaluate **Process** at a median of `4.0` (`Above_Average`), while Quant Process scores center at `3.0` (`Average`) with a heavy tail at `Below_Average` (31.3%).
  - Minimum observed for analyst People and Parent is `2.0` (`Below_Average`), whereas quant scores extend to `1.0` (`Low`).

##### C. Lipper Payload Architecture & Factor Explosion Audit

###### C.1. Multi-Universe Cardinality & The Factor Explosion Problem
In Lipper Leader payloads (`mf_lip_ratings`), data is grouped by geographic peer universes:
- **Universe Cardinality per Payload:**
  - 1 Universe: 5,522 products (47.6%) — predominantly domestic US, Canadian, Japanese, and Australian funds.
  - 2 to 10 Universes: 1,220 products (10.5%).
  - 11 to 22 Universes: 4,865 products (41.9%) — European UCITS funds cross-registered across multiple European Union member states.
- **Factor Explosion in Silver:**
  Because the legacy extractor iterated over every universe in `payload["universes"]` and formed `metric_id = f"lipper_{tag}_{horizon}_{country_name}"`:
  - **37 distinct geographic universes** were created (`germany`, `uk`, `italy`, `luxembourg`, `france`, `netherlands`, `spain`, `sweden`, `austria`, `finland`, `switzerland`, `denmark`, `norway`, `united_states`, `singapore`, `chile`, `canada`, `belgium`, `czech_republic`, `gcc`, `slovakia`, `poland`, `peru`, `australia`, `china`, `japan`, `taiwan`, `hong_kong`, `india`, `brazil`, etc.).
  - Multiplied by 5 metric tags and 4 horizons, this resulted in **568 distinct `metric_id`s** and **1,064,394 rows** in `silver.product_metrics`.
  - Storing 568 distinct Lipper features in a wide regression matrix results in extreme column sparsity (each country column is empty for ~80–90% of the ETF universe) and severe multicollinearity among European cross-listings.

###### C.2. Cross-Universe Rating Consistency
For the 6,231 multi-universe snapshots (UCITS and cross-listed ETFs):
- **51.4% (3,205 snapshots)** exhibit **identical ratings** across all registered countries for the exact same metric and horizon.
- **39.1% (2,439 snapshots)** differ by only $\pm 1$ notch (e.g. rating 4 in Germany vs. 5 in UK), caused by minor differences in peer cutoffs between national fund registration lists.
- **7.0%** differ by 2 notches, and only **1.1%** differ by 3 or 4 notches (almost exclusively occurring when comparing a large liquid domestic market like the US against a tiny satellite registration like Peru with only 15 funds).

###### C.3. Statistical Distributions of Canonical Lipper Metrics

| Metric Tag | Canonical Meaning | Obs Count | Unique ETFs | Min | P25 | Median | P75 | Max | % Score = 5 ("Leader") | % Score = 1 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `total_return` | 3/5/10yr and overall historical return vs peers | 80,485 | 11,481 | 1.0 | 2.0 | **3.0–4.0** | 5.0 | 5.0 | 28.6%–34.2% | 9.7%–14.6% |
| `consistent_return` | Risk-adjusted return consistency vs peers | 80,485 | 11,481 | 1.0 | 2.0 | **3.0–4.0** | 5.0 | 5.0 | 28.9%–35.5% | 14.0%–16.6% |
| `preservation` | Downside risk / capital preservation vs asset class | 83,635 | 11,650 | 1.0 | 2.0 | **3.0** | 4.0 | 5.0 | 24.4%–26.6% | 19.4%–24.1% |
| `expense` | Fee competitiveness vs peer group | 77,786 | 9,363 | 1.0 | 5.0 | **5.0** | 5.0 | 5.0 | **82.1%–85.7%** | 0.4%–0.6% |
| `tax_efficiency` | After-tax yield retention (US products only) | 6,323 | 3,458 | 1.0 | 2.0 | **4.0** | 5.0 | 5.0 | 37.6%–43.8% | 10.0%–14.6% |

###### C.4. Specific Lipper Metric Anomalies:
1. **The Expense Score Clustering Anomaly:** Over **82% to 85%** of all ETFs receive the maximum Lipper Leader score of `5.0` for Expense. This is mathematically valid because Lipper peer universes combine active open-end mutual funds with ETFs. Because ETFs possess structural fee advantages over traditional mutual funds, almost all ETFs land in the cheapest quintile of their Lipper classification. In factor regressions, `lipper_expense` exhibits very low cross-sectional variance among ETFs; `total_expense_ratio` from `profile` provides vastly superior granularity.
2. **Tax Efficiency Restriction:** `tax_efficiency` is calculated exclusively for US-domiciled products (3,458 unique ETFs). Non-US universes contain empty lists for tax efficiency.
3. **Preservation Baseline:** `preservation` is evaluated relative to the broad asset class rather than the narrow category, giving equity funds lower scores (median 3.0, 24% at 1.0) and fixed-income funds higher scores.

##### D. Empirical Cadence & Update Delta Analysis

By analyzing consecutive point-in-time observations per `product_id` across historical snapshots:

| Endpoint & Metric Group | Delta Pairs ($N$) | Min Delta | 25th % | Median Delta | 75th % | 95th % | Max Delta | Mode Delta | Empirical Update Cadence |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **All Lipper Metrics** (`lipper_*`) | 45,739 pairs | 31 days | 31 days | **31.0 days** | 31 days | 31 days | 31 days | **31 days (100.0%)** | **Strict Monthly Cadence** (Exact month-end recalculation). |
| `mstar_morningstar_rating` | 4,834 pairs | 31 days | 31 days | **31.0 days** | 31 days | 31 days | 31 days | **31 days (100.0%)** | **Strict Monthly Cadence** (Mathematical month-end recalculation). |
| `mstar_sustainability_rating` | 11,386 pairs | 31 days | 31 days | **31.0 days** | 31 days | 31 days | 31 days | 243 days | **31 days (99.8%)** | **Strict Monthly Cadence** (Monthly ESG portfolio refresh). |
| `mstar_medalist_rating_analyst` | 13 pairs | 39 days | 129 days | **130.0 days** | 136 days | 136 days | 136 days | 136 days | **Event-Driven / Annual** (Tied to analyst report publications). |
| `mstar_process_analyst` / `people` | 11 pairs | 129 days | 129 days | **136.0 days** | 136 days | 136 days | 136 days | 136 days | **Event-Driven / Annual** (Tied to analyst report publications). |
| `mstar_parent_analyst` | 2 pairs | 22 days | 22 days | **22.0 days** | 22 days | 22 days | 22 days | 22 days | **Annual Firm Review** (Updated when firm coverage changes). |

**Key Finding — Single-Shot Country Variants vs Longitudinal Primary Metrics:**
Across the 568 Lipper metric variants, **only 20 variants** (specifically the 5 metric tags $\times$ 4 horizons in the deepest primary domestic universe) possess longitudinal history ($\ge 2$ observations on any product). The remaining 548 variants are single-shot cross-registration snapshots. Consequently, LOCF staleness carry is mathematically ineffective on the unreduced table. Applying the **Max-Peer-Count Selection Algorithm** collapses the 568 sparse variants into the 20 canonical metrics, instantly enabling full longitudinal depth across the 45,739 monthly transitions.

---

#### 4.4.2. Exact Functional Cleaning Rules & Transformation Specifications

##### 1. Morningstar Date Extraction & Precedence
- **Pillar-Level Date Precedence:**
  - For each summary item in `payload.get("summary", [])`:
    1. If `pillar.get("publish_date")` is present: parse date via regex `(\d{4}[-/]?\d{2}[-/]?\d{2})` and assign `effective_date_source = 'item'`.
    2. Else if top-level `payload.get("as_of_date")` is present: parse date and assign `effective_date_source = 'payload'`.
    3. Else: fallback to `snapshot_created_at.date()` with `effective_date_source = 'snapshot'`.
- **Empirical Validation:** 99.996% of Morningstar observations carry an explicit item-level `publish_date`.

##### 2. Morningstar Discrete Rating Scale Maps
Raw rating strings in `payload["summary"]` must be mapped to numeric values $[1.0, 5.0]$:

###### A. Morningstar Medalist Rating (`_MSTAR_MEDALIST_MAP`)
$$\text{value} = \begin{cases} 5.0 & \text{'gold'} \\ 4.0 & \text{'silver'} \\ 3.0 & \text{'bronze'} \\ 2.0 & \text{'neutral'} \\ 1.0 & \text{'negative'} \end{cases}$$

###### B. Morningstar Pillar Ratings (`_MSTAR_PILLAR_MAP`)
Applies to `people`, `process`, and `parent`:
$$\text{value} = \begin{cases} 5.0 & \text{'high'} \\ 4.0 & \text{'above_average'} \\ 3.0 & \text{'average'} \\ 2.0 & \text{'below_average'} \\ 1.0 & \text{'low'} \end{cases}$$

###### C. Morningstar Star Rating (`_MSTAR_STAR_MAP`)
Applies to `morningstar_rating`:
$$\text{value} = \text{float}(\text{raw\_value}) \in \{1.0, 2.0, 3.0, 4.0, 5.0\}$$

###### D. Morningstar Sustainability Rating (`_MSTAR_SUSTAINABILITY_MAP`)
Applies to `sustainability_rating` (Globe rating):
$$\text{value} = \begin{cases} 5.0 & \text{'high' or '5'} \\ 4.0 & \text{'above_average' or '4'} \\ 3.0 & \text{'average' or '3'} \\ 2.0 & \text{'below_average' or '2'} \\ 1.0 & \text{'low' or '1'} \end{cases}$$

###### E. String Normalization & Exclusion Policy:
- Prior to lookup, normalize strings via `str(raw_val).strip().lower()`.
- Explicitly skip non-evaluative / pending tokens without raising exceptions:
  $$\text{tokens to skip} \in \{\text{'under_review'}, \text{'under review'}, \text{'not_applicable'}, \text{'not applicable'}, \text{'na'}, \text{'n/a'}, \text{'-'}, \text{''}\}$$
- Skip informative non-numeric tags: `pillar_key in ('category', 'category_index')`.
- If an unrecognized non-empty rating string is encountered, raise `ValueError` to prevent silent corruption.

##### 3. Morningstar Analyst vs. Quantitative Segregation Architecture

###### A. Silver Layer Extraction Policy
In `extract_mstar()`:
1. **Unified Medalist Metric (`mstar_medalist_rating`):**
   - Extract `medalist_rating` (or legacy `quantitative_rating`) as canonical `metric_id = "mstar_medalist_rating"`.
   - Store the provider rating string (`"Gold"`, `"Silver"`, etc.) in `raw_value`.
2. **Analyst Coverage Metric (`mstar_analyst_coverage_pct`):**
   - Inspect the summary items for the 3 foundational pillars:
     $$\text{analyst\_count} = \sum_{p \in \{\text{'people'}, \text{'process'}, \text{'parent'}\}} \mathbb{I}(p \in \text{summary})$$
     $$\text{coverage} = \frac{\text{analyst\_count}}{3.0}$$
   - Emit `metric_id = "mstar_analyst_coverage_pct"` with `value = coverage`, `raw_value = f"{analyst_count}/3 analyst pillars"`.
3. **Pillar Segregation:**
   - Continue extracting individual pillars as `mstar_{pillar}_{'quant' if is_quant else 'analyst'}` (`mstar_people_analyst`, `mstar_people_quant`, etc.), maintaining strict provenance in Silver.

###### B. Factor Panel Construction Policy (`silver.monthly_panel`)
When flattening Morningstar features into `silver.monthly_panel`:
1. **Pillar Score Synthesis:** Construct unified pillar features (`pillar_people`, `pillar_process`, `pillar_parent`) using analyst priority with quantitative fallback:
   $$\text{pillar\_score} = \begin{cases} \text{score}_{\text{analyst}} & \text{if analyst rating is available} \\ \text{score}_{\text{quant}} & \text{if algorithmic rating is available} \\ \text{NULL} & \text{otherwise} \end{cases}$$
2. **Pillar Quant Indicators:** Provide binary indicator features (`pillar_people_is_quant`, `pillar_process_is_quant`, `pillar_parent_is_quant`) taking value `1.0` if the active score was derived algorithmically, and `0.0` if human analyst-driven.
3. **Medalist Segmentation:** The researcher can evaluate `mstar_medalist_rating` conditioned on `mstar_analyst_coverage_pct = 1.0` (pure analyst), `= 0.0` (pure quant), or as a continuous interaction term.

##### 4. Lipper Max-Peer-Count Geographic Universe Selection Algorithm

To eliminate the 568-factor explosion and resolve multi-jurisdiction UCITS redundancy, the pipeline selects a **single primary geographic peer universe per product** using the **Max-Peer-Count Selection Algorithm**:

###### A. Selection Algorithm Definition
Given `payload["universes"]`:
1. If `len(universes) == 1`: Select that single universe directly.
2. If `len(universes) > 1`:
   - For each universe $U_i \in \text{universes}$, calculate the maximum peer group fund count reported across its rating items:
     $$N(U_i) = \max_{\text{item} \in U_i} \left( \text{regex\_int}(\text{item.rating.name}) \right)$$
   - Select the universe with the highest statistical depth:
     $$U^* = \arg\max_{U_i} N(U_i)$$
   - In case of a tie in peer count, prioritize standard primary markets: `['United States', 'Germany', 'UK', 'Canada', 'Japan', 'Australia']`.

###### B. Empirical Selection Validation
Across all 11,611 products with Lipper data:
- **US Cross-Listings:** For US funds registered in Latin America / Asia (e.g. SPY registered in US, Chile, Peru), the algorithm selects `United States` ($N \approx 1,500$ vs $N \approx 27$ in Chile and $15$ in Peru).
- **European UCITS:** For UCITS funds registered across 16 European nations, the algorithm selects `Germany` ($N \approx 2,780$) or `Luxembourg` ($N \approx 2,806$), providing the deepest and most stable European peer universe.
- **Single-Market Funds:** Canada ($N \approx 1,083$), Japan ($N \approx 220$), Australia ($N \approx 313$), China ($N \approx 297$), Taiwan ($N \approx 187$), and India ($N \approx 159$) are preserved with 100% fidelity.

###### C. Target Canonical Lipper Metrics (20 Features in `silver.monthly_panel`)
Extract or flatten only the 20 canonical metrics for the selected primary universe $U^*$:

| Metric Tag | Panel Target Feature IDs (4 Horizons Each) | Operable Value | Raw Value Format |
| :--- | :--- | :--- | :--- |
| `total_return` | `lipper_total_return_{overall\|3yr\|5yr\|10yr}` | Float $[1.0, 5.0]$ | `"5 (Germany, 2781 funds)"` |
| `consistent_return` | `lipper_consistent_return_{overall\|3yr\|5yr\|10yr}` | Float $[1.0, 5.0]$ | `"4 (Germany, 2781 funds)"` |
| `preservation` | `lipper_preservation_{overall\|3yr\|5yr\|10yr}` | Float $[1.0, 5.0]$ | `"3 (Germany, 14890 funds)"` |
| `expense` | `lipper_expense_{overall\|3yr\|5yr\|10yr}` | Float $[1.0, 5.0]$ | `"5 (Germany, 1362 funds)"` |
| `tax_efficiency` | `lipper_tax_efficiency_{overall\|3yr\|5yr\|10yr}` | Float $[1.0, 5.0]$ | `"5 (United States, 114 funds)"` (US only) |

###### D. Database Storage Decision:
- **Silver Observations Layer (`extract_lipper`):** Implement primary universe selection in `extract_lipper()`, emitting canonical metric IDs (`lipper_{tag}_{horizon}`) while storing the selected universe and fund count in `raw_value` (e.g. `f"{val} ({universe_name}: {fund_count} funds)"`). This eliminates over 860,000 redundant duplicate rows from `silver.product_metrics` and eliminates table fragmentation.

##### 5. Discrete Integer Scale Verification
- All Lipper scores are strictly discrete integer ratings $v \in \{1, 2, 3, 4, 5\}$.
- No decimal scaling or percentage division ($\div 100$) is applied.
- `currency` is always `NULL` (dimensionless ordinal scores).

---

#### 4.4.3. Recommended LOCF Staleness Caps for Factor Panel

Based on the empirical cadence audit (Lipper: 100% 31-day deltas; Morningstar Stars/ESG: 100% 31-day deltas; Morningstar Medalist/Pillars: event-driven annual review cycle):

| Feature / Metric Group | Empirical Update Cadence | Typical Reporting Delay | Recommended Panel Staleness Cap | Operational Rationale |
| :--- | :--- | :--- | :--- | :--- |
| **All 20 Canonical Lipper Metrics** (`lipper_*`) | Monthly (median 31d, mode 31d) | 15–45 days | **6 Months (180 days)** | 100% of consecutive historical updates occur on exact 31-day month-ends. A 6-month cap prevents dropouts during occasional reporting lapses while dropping dead/liquidated funds. |
| `mstar_morningstar_rating` | Monthly (median 31d, mode 31d) | 15–45 days | **6 Months (180 days)** | Star ratings are recalculated mathematically every month-end. |
| `mstar_sustainability_rating` | Monthly (median 31d, mode 31d) | 30–60 days | **6 Months (180 days)** | Portfolio ESG risk globes are refreshed monthly as underlying holdings are disclosed. |
| `mstar_medalist_rating` | Event-driven / Annual (median 130d) | 30–90 days | **18 Months (540 days)** | Analyst reports and qualitative evaluations update annually. An 18-month threshold covers the 12-month review cycle plus a 6-month buffer. |
| `mstar_analyst_coverage_pct` | Event-driven / Annual | 30–90 days | **18 Months (540 days)** | Tracks pillar coverage composition; updates synchronously with Medalist ratings. |
| `mstar_people_*`, `mstar_process_*`, `mstar_parent_*` | Event-driven / Annual (median 136d) | 30–90 days | **18 Months (540 days)** | Individual pillar scores persist until the next analyst or quantitative model review. |

---

---

### 4.5. Session 5 Specification: `themes` & `esg` Evaluative Signals

- **Target Endpoints:**
  - Themes: `/tws.proxy/knowledge-graph/ui/fund?conid=`
  - ESG: `/tws.proxy/impact/esg/`
- **Extractors:** [`extract_theme_weights`](file:///Users/alex/Documents/etfportfolio/etfportfolio/prep/extractors.py#L607-L644) and [`extract_esg`](file:///Users/alex/Documents/etfportfolio/etfportfolio/prep/extractors.py#L316-L382) in [extractors.py](file:///Users/alex/Documents/etfportfolio/etfportfolio/prep/extractors.py)
- **Test Fixtures:** [theme_weights.json](file:///Users/alex/Documents/etfportfolio/tests/fixtures/theme_weights.json) and [esg.json](file:///Users/alex/Documents/etfportfolio/tests/fixtures/esg.json)
- **Bronze Snapshot Counts:**
  - Themes: 27,631 snapshots across 5 fetch dates (Sep 3–13, 2026); 15,786 contain data, 11,845 are empty `{}`.
  - ESG: 43,437 snapshots across 5 fetch dates (Sep 3–13, 2026); 23,789 contain data, 19,648 are empty `{}`.
- **Silver Layer Counts:**
  - Themes: 5,096,954 rows in `silver.product_dimensions` (`dimension_type = 'theme'`), 10,884 unique products, 491 distinct child themes.
  - ESG: 404,404 rows in `silver.product_metrics` (`source = 'esg'`), 11,627 unique products, 17 distinct `metric_id` values.
- **Product Overlap:**
  - 904 products have ESG scores but no theme data (predominantly fixed-income: 860 FI + 9 Other + 4 None + 1 Cash vs 0 Equity).
  - 161 products have theme data but no ESG scores.

---

#### 4.5.1. Theme Weights Payload Architecture & Extraction Rules

##### A. Payload Schema

The `/tws.proxy/knowledge-graph/ui/fund?conid=` endpoint returns:
```json
{
  "conid": 52197301,
  "name": "Vanguard Total World Stock Index Fund In",
  "symbol": "VT",
  "exchange": "SMART",
  "assetType": "STK",
  "coverage": 0.925019,
  "themes": [
    {
      "key": "dd8b04fb-8529-47df-b179-4c68d78f34e5",
      "name": "AI Inference",
      "weight": 0.267545,
      "rank_adjusted_weight": 0.208054786666667
    }
  ]
}
```

- **Top-level keys (invariant):** `conid`, `name`, `symbol`, `exchange`, `assetType`, `coverage`, `themes`.
- **Theme item keys (invariant):** `key` (UUID), `name`, `weight`, `rank_adjusted_weight`.
- **Empty payloads:** 11,845 of 27,631 snapshots (42.9%) return `{}` for products with no thematic coverage (non-equity products). These produce zero rows correctly.

##### B. Date Provenance

- **No explicit date field exists in the theme payload.** The payload contains no `asOfDate`, `as_of_date`, or any date-like key.
- **Effective date strategy:** `effective_date = snapshot_created_at.date()`, `effective_date_source = "snapshot"`.
- **Implication:** Theme weights are point-in-time snapshots tied to the fetch timestamp. There is no independent provider-reported recalculation date.

##### C. Weight Field Selection: `rank_adjusted_weight` vs `weight`

Two weight fields exist per theme:
- `weight` — raw portfolio-weighted thematic exposure (range $[0.0, 1.0]$ per theme).
- `rank_adjusted_weight` — rank-penalized weight that depresses themes where the fund's coverage of that theme is relatively low compared to the universe median.

**Empirical comparison** across 7,353 theme instances from 20 sample payloads:
- $\text{ratio} = \frac{\text{rank\_adjusted\_weight}}{\text{weight}}$: min $= 0.0133$, median $= 0.5604$, max $= 1.0000$.
- `rank_adjusted_weight` is always $\leq$ `weight`.

**Decision:** The extractor correctly stores `rank_adjusted_weight` as the canonical `value`. This is the appropriate choice because it penalizes themes where a fund has negligible coverage of the theme's constituent universe, producing more discriminating cross-sectional factor loadings. The raw `weight` is discarded to prevent multicollinearity (both weights are monotonically correlated and differ only in scaling).

##### D. Theme Cardinality & Sparsity

- **Total child themes in `bronze.themes`:** 491 (across 19 parent categories).
- **Themes per `(product_id, effective_date)`:**
  - Min: 1, Median: 370, Max: 491, Mean: 322.88.
  - Products with $\leq 3$ themes: sparse edge cases (10 products), typically single-stock leveraged ETPs.
  - VT (broad world index) carries all 491 themes; thematic ETFs like BOTZ carry ~430–540 themes (across cross-listings).

##### E. Theme Coverage Ratio

The payload field `coverage` represents the fraction of the fund's portfolio holdings that have thematic classification data:
- **Present in:** 15,786 of 15,786 non-empty payloads (always present when themes exist).
- **Range:** min $= 0.700$, P01 $= 0.711$, P25 $= 0.871$, median $= 0.945$, P75 $= 0.982$, P99 $= 1.219$, max $= 3.844$.
- **Anomaly — Coverage $> 1.0$:** Observed in 139 products (P99 $= 1.175$), exclusively leveraged ETPs (2× Leverage Shares, yield-enhanced structures). The coverage ratio exceeds 1.0 because the fund's effective equity exposure exceeds its NAV due to embedded leverage.
- **Current status:** Theme coverage is **not extracted** into `silver.product_metrics`. The extractor emits only dimension rows.
- **Specification:** Extract `coverage` as a scalar metric in `silver.product_metrics` with `source = "theme_weights"`, `metric_id = "theme_coverage"`, `value = float(coverage)`, `raw_value = str(coverage)`. This enables quality-gating in the panel layer (e.g., exclude products with coverage $< 0.7$).

##### F. Negative Theme Weights

- **Count:** 3,153 rows (0.062% of all theme rows) from 20 distinct products.
- **Cause:** All 20 products are long/short or alternative strategy ETFs (e.g., `LBAY` Leatherback Long/Short, `HDGE` Accelerate Absolute Return, `NLSI` NEOS Long/Short Equity Income, `FLSE` Fidelity Global Opportunities Long/Short).
- **Range of negative weights:** $[-0.0852, 0)$.
- **Interpretation:** Negative `rank_adjusted_weight` indicates the fund has net short thematic exposure — it is underweight (shorting) the constituent stocks of that theme relative to the market.
- **Silver layer rule:** Retain exact negative weights for mathematical consistency. The sign carries factor modeling value (distinguishing long/short strategies from long-only).
- **Panel layer rule:** In `silver.monthly_panel`, retain negative weights as-is. In downstream factor regressions, the researcher can optionally zero-floor ($\max(0, w)$) for long-only analyses.

##### G. Theme Weights $> 1.0$ (Leveraged ETPs)

- **Count:** 1,877 rows (0.037%) from 94 distinct products.
- **Cause:** All 94 products are leveraged ETPs (Leverage Shares 2× single-stock, Purpose Yield Shares, Enhanced High Income structures). The `rank_adjusted_weight` scales with the fund's effective exposure, so a 2× leveraged Amazon ETP has `rank_adjusted_weight ≈ 2.0` for Amazon-related themes.
- **Maximum observed:** $2.0007$ (Leverage Shares 2× Goldman Sachs).
- **Silver layer rule:** Retain exact values $> 1.0$ for auditability.
- **Panel layer rule:** In `silver.monthly_panel`, retain raw values. Downstream factor analysis may winsorize to $[0.0, 1.0]$ or isolate leveraged products via an indicator feature.

##### H. Sum of Theme Weights (Non-Normalization)

Theme weights do **not** sum to 1.0 per `(product_id, effective_date)`:
- Sum distribution: min $= 0.052$, median $= 7.303$, max $= 110.865$, mean $= 9.636$.
- **Interpretation:** Each theme weight independently measures the fund's exposure to that theme (as a fraction of the portfolio). Themes are overlapping — a holding like NVIDIA contributes simultaneously to "AI Inference", "Semiconductor Chips", "Accelerated Computing", etc. Therefore, unlike allocation dimensions (`country`, `industry`), theme weights are **not subject to a sum-to-1.0 invariant**.
- **Panel layer rule:** Theme weights are sparse features, not partition-of-unity allocations. Missing themes default to $0.0$. No normalization or sum constraint is applied.

##### I. Hierarchical Parent Theme Rollup

`bronze.themes` defines a 2-level hierarchy: 19 parent themes → 491 child themes. Each child maps to exactly one parent via `parent_id`. Parent themes are NOT present in `silver.product_dimensions` (only children).

| Parent Theme Name | Children | Median Rollup Weight | Max Rollup Weight |
| :--- | :---: | :---: | :---: |
| Technology and Innovation | 60 | 1.358 | 10.037 |
| Technology Hardware and Semiconductors | 20 | 0.791 | 3.332 |
| Consumer Goods and Retail | 44 | 0.504 | 1.805 |
| Healthcare and Biotechnology | 50 | 0.478 | 1.546 |
| Energy and Utilities | 48 | 0.429 | 1.533 |
| Entertainment and Media | 24 | 0.413 | 2.430 |
| Financial Services and FinTech | 40 | 0.409 | 1.262 |
| Telecommunications and Connectivity | 14 | 0.387 | 1.217 |
| Automotive and Mobility | 24 | 0.332 | 1.221 |
| Transportation and Logistics | 26 | 0.234 | 0.722 |
| Industrial Products and Services | 19 | 0.187 | 0.560 |
| Environmental and Sustainability Solutions | 17 | 0.176 | 0.561 |
| Aerospace and Defense | 15 | 0.103 | 0.402 |
| Construction and Infrastructure | 11 | 0.095 | 0.375 |
| Real Estate and Property Management | 22 | 0.077 | 0.311 |
| Food and Beverage | 14 | 0.069 | 0.231 |
| Mining and Metals | 18 | 0.044 | 0.317 |
| Hospitality and Leisure | 13 | 0.040 | 0.168 |
| Agriculture and Food Production | 12 | 0.029 | 0.142 |

**Panel layer specification:** In `silver.monthly_panel`, provide **both** representations:
1. **Granular child theme features** (`theme_{theme_id}`): All 491 child themes as sparse features, defaulting to $0.0$ for absent themes.
2. **Aggregated parent theme features** (`theme_parent_{parent_id}`): 19 rolled-up features computed as $\sum_{\text{child} \in \text{parent}} \text{child\_weight}$.

The parent rollup serves as a dimensionality-reduced alternative for factor regressions where 491 sparse features create estimation instability.

##### J. `dimension_code` Integrity

- All 491 distinct `dimension_code` values in `silver.product_dimensions` are verified to exist in `bronze.themes` (0 orphans).
- No parent theme IDs appear in `silver.product_dimensions` (0 parent matches) — only child themes are stored.
- `dimension_code` stores the UUID `key` from the payload; `dimension_name` stores the human-readable theme name.

---

#### 4.5.2. ESG Payload Architecture & Extraction Rules

##### A. Payload Schema

The `/tws.proxy/impact/esg/` endpoint returns Refinitiv (LSEG) weighted-average ESG scores:
```json
{
  "title": "ESG",
  "asOfDate": "20260815",
  "coverage": 0.980016,
  "source": "CALCULATED",
  "symbol": "SPY",
  "no_settings": true,
  "content": [
    { "name": "TRESGS", "value": 6 },
    { "name": "TRESGCS", "value": 4 },
    { "name": "TRESGCCS", "value": 3 },
    { "name": "TRESGENS", "value": 6, "children": [
        { "name": "TRESGENRRS", "value": 8 },
        { "name": "TRESGENERS", "value": 7 },
        { "name": "TRESGENPIS", "value": 4 }
    ]},
    { "name": "TRESGSOS", "value": 6, "children": [
        { "name": "TRESGSOWOS", "value": 6 },
        { "name": "TRESGSOHRS", "value": 7 },
        { "name": "TRESGSOCOS", "value": 7 },
        { "name": "TRESGSOPRS", "value": 6 }
    ]},
    { "name": "TRESGCGS", "value": 7, "children": [
        { "name": "TRESGCGBDS", "value": 7 },
        { "name": "TRESGCGSRS", "value": 6 },
        { "name": "TRESGCGVSS", "value": 8 }
    ]}
  ]
}
```

- **Top-level keys (invariant across 23,789 non-empty payloads):** `asOfDate`, `content`, `coverage`, `no_settings`, `source`, `symbol`, `title`.
- **`source`:** Always `"CALCULATED"` (100% of non-empty payloads). Refinitiv computes portfolio-level weighted-average ESG scores from constituent company scores.
- **`title`:** Always `"ESG"`.
- **Empty payloads:** 19,648 of 43,437 snapshots (45.2%) return `{}`. These are products without ESG coverage (leveraged, commodity, currency ETPs).

##### B. ESG Metric Taxonomy & Score Hierarchy

The ESG payload contains a 3-level tree structure with 16 score nodes plus 1 coverage metric:

| Metric ID | Full Name | Level | Parent | Scale | Observed Range |
| :--- | :--- | :---: | :--- | :--- | :--- |
| `esg_coverage` | Portfolio ESG Holdings Coverage | — | — | Continuous $[0, \infty)$ | $[0.700, 2.680]$ |
| `tresgs` | ESG Score | L0 | — | Integer $[0, 10]$ | $[1, 8]$ |
| `tresgcs` | ESG Combined Score | L0 | — | Integer $[0, 10]$ | $[1, 8]$ |
| `tresgccs` | ESG Controversies Score | L0 | — | Integer $[0, 10]$ | $[0, 10]$ |
| `tresgens` | Environmental Pillar | L1 | — | Integer $[0, 10]$ | $[0, 9]$ |
| `tresgenrrs` | Resource Use Score | L2 | `tresgens` | Integer $[0, 10]$ | $[0, 9]$ |
| `tresgeners` | Emissions Score | L2 | `tresgens` | Integer $[0, 10]$ | $[0, 9]$ |
| `tresgenpis` | Environmental Innovation Score | L2 | `tresgens` | Integer $[0, 10]$ | $[0, 9]$ |
| `tresgsos` | Social Pillar | L1 | — | Integer $[0, 10]$ | $[1, 9]$ |
| `tresgsowos` | Workforce Score | L2 | `tresgsos` | Integer $[0, 10]$ | $[0, 9]$ |
| `tresgsohrs` | Human Rights Score | L2 | `tresgsos` | Integer $[0, 10]$ | $[0, 9]$ |
| `tresgsocos` | Community Score | L2 | `tresgsos` | Integer $[0, 10]$ | $[0, 9]$ |
| `tresgsoprs` | Product Responsibility Score | L2 | `tresgsos` | Integer $[0, 10]$ | $[0, 9]$ |
| `tresgcgs` | Governance Pillar | L1 | — | Integer $[0, 10]$ | $[0, 9]$ |
| `tresgcgbds` | Management Score | L2 | `tresgcgs` | Integer $[0, 10]$ | $[0, 9]$ |
| `tresgcgsrs` | Shareholders Score | L2 | `tresgcgs` | Integer $[0, 10]$ | $[0, 9]$ |
| `tresgcgvss` | CSR Strategy Score | L2 | `tresgcgs` | Integer $[0, 10]$ | $[0, 9]$ |

##### C. Date Provenance

- **`asOfDate` format:** `"YYYYMMDD"` string (e.g., `"20260815"`, `"20260822"`, `"20260829"`).
- **Present in:** 23,789 of 23,789 non-empty payloads (100%). Missing only in the 19,648 empty `{}` payloads.
- **Extraction strategy:** `parse_effective_date(payload.get("asOfDate"), ...)` correctly parses `YYYYMMDD` via the `_DATE_REGEX` pattern and yields `effective_date_source = "payload"`.
- **Distinct `effective_date` values observed in Silver:** 3 dates (`2026-08-22`, `2026-08-29`, `2026-09-05`), all Fridays — confirming weekly recalculation cadence.

##### D. ESG Score Scale & Discrete Integer Verification

- All 16 ESG scores are **discrete integer values** on a $[0, 10]$ integer scale (Refinitiv decile scoring).
- No fractional scores observed. All `value` fields are integer-typed in the JSON payload.
- No percentage scaling ($\div 100$) is applied — scores are stored as-is.
- `currency` is always `NULL` (dimensionless ordinal scores).

**Score Interpretation:** Higher scores indicate better ESG performance, **except** `tresgccs` (Controversies), where higher scores indicate *fewer* controversies (i.e., better). All scores are directionally consistent: higher = better.

##### E. ESG Coverage Ratio Anomaly

`esg_coverage` represents the fraction of the fund's portfolio holdings that have Refinitiv ESG ratings:
- **Distribution:** min $= 0.7000$, P05 $= 0.7592$, P25 $= 0.8826$, median $= 0.9419$, P75 $= 0.9740$, P95 $= 0.9982$, P99 $= 1.1749$, max $= 2.6804$.
- **Coverage $> 1.0$:** 281 observations from 139 distinct products (1.18% of coverage records). These are exclusively leveraged ETPs whose effective equity exposure exceeds NAV.
- **9 snapshot payloads have `coverage = NULL` (spanning 3 unique products):** The top-level `coverage` key is missing from 9 individual payloads that otherwise contain valid `content` score trees (`tresgs` obs = 23,789 vs `esg_coverage` obs = 23,780). These 3 products are retained in Silver; at the panel layer, their scores are filtered out if a strict `esg_coverage >= 0.7` gate is enforced.
- **Silver layer rule:** Retain exact `esg_coverage` value, including values $> 1.0$, for auditability. Do not clip.
- **Panel layer rule:** Use `esg_coverage` as a quality-gate filter. Recommended minimum: $\geq 0.7$ (all observed records pass this threshold). Coverage $> 1.0$ is an informational anomaly, not an error.

##### F. Zero-Score Edge Cases

Several sub-pillar scores can be exactly $0$ (the theoretical floor):

| Metric | Zeros | % of Obs | Interpretation |
| :--- | :---: | :---: | :--- |
| `tresgenpis` (Env. Innovation) | 375 | 1.58% | Fund constituents have negligible eco-innovation. |
| `tresgccs` (Controversies) | 155 | 0.65% | Maximum controversies — worst decile. |
| `tresgcgsrs` (Shareholders) | 80 | 0.34% | Weakest shareholder governance decile. |
| `tresgcgbds` (Management) | 41 | 0.17% | Weakest management score. |
| `tresgsohrs` (Human Rights) | 37 | 0.16% | Worst human rights compliance. |
| `tresgcgvss` (CSR Strategy) | 31 | 0.13% | No CSR strategy score. |
| `tresgenrrs` (Resource Use) | 30 | 0.13% | Lowest resource efficiency decile. |
| `tresgens` (Environmental) | 27 | 0.11% | Lowest environmental pillar. |
| `tresgeners` (Emissions) | 20 | 0.08% | Worst emissions management. |
| `tresgcgs` (Governance) | 11 | 0.05% | Lowest governance pillar. |
| `tresgsowos` (Workforce) | 10 | 0.04% | Worst workforce practices. |
| `tresgsocos` (Community) | 2 | 0.01% | Lowest community engagement. |
| `tresgsoprs` (Product Resp.) | 2 | 0.01% | Worst product responsibility. |

- **Silver layer rule:** Retain $0$ values as valid (they represent the lowest decile, not missing data).
- **Panel layer rule:** $0$ is a legitimate score in the $[0, 10]$ integer scale. No imputation needed.
- **Note:** The aggregate scores `tresgs` and `tresgcs` never reach $0$ (min $= 1$), nor do `tresgsos` (min $= 1$). Only sub-pillar and controversies scores reach the floor.

##### G. Statistical Distributions of ESG Metrics in Silver

| Metric ID | Description | Obs | Unique ETFs | Min | P05 | P25 | Median | P75 | P95 | Max | Mean | Std |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `esg_coverage` | Portfolio Coverage Ratio | 23,780 | 11,624 | 0.70 | 0.76 | 0.88 | **0.94** | 0.97 | 1.00 | 2.68 | 0.92 | 0.10 |
| `tresgs` | ESG Score | 23,789 | 11,627 | 1 | 4 | 6 | **6** | 7 | 7 | 8 | 6.06 | 0.92 |
| `tresgcs` | ESG Combined Score | 23,789 | 11,627 | 1 | 4 | 4 | **5** | 6 | 6 | 8 | 4.91 | 0.88 |
| `tresgccs` | Controversies Score | 23,789 | 11,627 | 0 | 3 | 4 | **5** | 7 | 9 | 10 | 5.46 | 2.09 |
| `tresgens` | Environmental Pillar | 23,789 | 11,627 | 0 | 3 | 5 | **6** | 7 | 7 | 9 | 5.79 | 1.24 |
| `tresgenrrs` | Resource Use | 23,789 | 11,627 | 0 | 4 | 6 | **7** | 8 | 8 | 9 | 6.74 | 1.43 |
| `tresgeners` | Emissions | 23,789 | 11,627 | 0 | 4 | 6 | **7** | 7 | 8 | 9 | 6.48 | 1.25 |
| `tresgenpis` | Env. Innovation | 23,789 | 11,627 | 0 | 1 | 4 | **4** | 5 | 6 | 9 | 4.20 | 1.42 |
| `tresgsos` | Social Pillar | 23,789 | 11,627 | 1 | 4 | 6 | **6** | 7 | 8 | 9 | 6.25 | 0.99 |
| `tresgsowos` | Workforce | 23,789 | 11,627 | 0 | 4 | 6 | **6** | 7 | 8 | 9 | 6.38 | 1.25 |
| `tresgsohrs` | Human Rights | 23,789 | 11,627 | 0 | 3 | 6 | **6** | 7 | 8 | 9 | 5.96 | 1.31 |
| `tresgsocos` | Community | 23,789 | 11,627 | 0 | 5 | 6 | **7** | 7 | 8 | 9 | 6.73 | 0.87 |
| `tresgsoprs` | Product Responsibility | 23,789 | 11,627 | 0 | 4 | 5 | **6** | 6 | 7 | 9 | 5.67 | 1.08 |
| `tresgcgs` | Governance Pillar | 23,789 | 11,627 | 0 | 5 | 6 | **6** | 7 | 7 | 9 | 6.03 | 0.90 |
| `tresgcgbds` | Management | 23,789 | 11,627 | 0 | 5 | 6 | **6** | 7 | 7 | 9 | 6.18 | 1.02 |
| `tresgcgsrs` | Shareholders | 23,789 | 11,627 | 0 | 4 | 5 | **5** | 6 | 6 | 9 | 5.17 | 0.96 |
| `tresgcgvss` | CSR Strategy | 23,789 | 11,627 | 0 | 4 | 6 | **7** | 8 | 8 | 9 | 6.69 | 1.29 |

##### H. Score Distribution Patterns

```
tresgs (ESG Score) — Concentrated around 6:
  1:     14 ( 0.1%)
  2:     49 ( 0.2%)
  3:    253 ( 1.1%)  █
  4:  1,359 ( 5.7%)  ███
  5:  2,855 (12.0%)  ██████
  6: 11,474 (48.2%)  ████████████████████████
  7:  7,606 (32.0%)  ████████████████
  8:    179 ( 0.8%)

tresgccs (Controversies) — Wide symmetric distribution:
  0:    155 ( 0.7%)
  1:    181 ( 0.8%)
  2:    832 ( 3.5%)  ██
  3:  3,840 (16.1%)  ████████
  4:  3,701 (15.6%)  ████████
  5:  3,493 (14.7%)  ███████
  6:  3,910 (16.4%)  ████████
  7:  3,074 (12.9%)  ██████
  8:  2,335 ( 9.8%)  █████
  9:  2,067 ( 8.7%)  ████
  10:   201 ( 0.8%)
```

- **Aggregate scores (`tresgs`, `tresgcs`) cluster tightly** (std $< 1.0$) around 5–6, reflecting diversified ETF portfolios averaging constituent scores.
- **Sub-pillar scores have wider spread** (std $1.0$–$1.4$), providing more cross-sectional discriminating power for factor modeling.
- **Controversies (`tresgccs`) has the widest spread** (std $= 2.09$, full 0–10 range), making it the most discriminating single ESG signal.

---

#### 4.5.3. Extraction Rule Specifications

##### 1. `extract_theme_weights` Rules

| Rule | Current Behavior | Required Change |
| :--- | :--- | :--- |
| **Weight field** | Extracts `rank_adjusted_weight` as `value` | ✅ Correct — no change. |
| **`weight` field** | Discarded (not extracted) | ✅ Correct — prevents multicollinearity. |
| **`dimension_type`** | `"theme"` | ✅ Correct. |
| **`dimension_name`** | `name.strip()` | ✅ Correct. |
| **`dimension_code`** | `str(theme_id).strip()` (UUID) | ✅ Correct — matches `bronze.themes.theme_id`. |
| **`effective_date`** | `snapshot_created_at.date()` | ✅ Correct — no payload date exists. |
| **`effective_date_source`** | `"snapshot"` | ✅ Correct. |
| **`coverage` metric** | **Not extracted** | ⚠️ **Add:** Emit `theme_coverage` metric to `silver.product_metrics`. |
| **Negative weights** | Stored as-is | ✅ Correct — intentional for long/short ETFs. |
| **Empty payloads** | Returns empty `ExtractionResult` | ✅ Correct. |

**Required addition — `theme_coverage` metric extraction:**
```python
coverage = payload.get("coverage")
if coverage is not None:
    result.metrics.append((
        product_id,
        "theme_weights",        # source
        "theme_coverage",       # metric_id
        eff_date,
        "snapshot",             # effective_date_source
        snapshot_created_at,
        float(coverage),        # value
        str(coverage),          # raw_value
    ))
```

##### 2. `extract_esg` Rules

| Rule | Current Behavior | Required Change |
| :--- | :--- | :--- |
| **Date parsing** | `parse_effective_date(payload.get("asOfDate"))` → `"payload"` | ✅ Correct — parses `YYYYMMDD` format. |
| **`coverage` metric** | Extracted as `esg_coverage` | ✅ Correct. |
| **Score iteration** | Iterates `content[]` nodes and their `children[]` | ✅ Correct — traverses 2-level tree. |
| **`metric_id` generation** | `sanitize_metric_id(node_name)` → lowercase TRESG tags | ✅ Correct — produces `tresgs`, `tresgcs`, etc. |
| **Score scaling** | `float(node_val)` — stored as raw integers | ✅ Correct — no division or rescaling. |
| **`currency`** | Not emitted (current schema lacks currency on esg) | ✅ Correct — ESG scores are dimensionless. |
| **Empty payloads** | Returns empty `ExtractionResult` | ✅ Correct. |

**No changes required to `extract_esg`.** The current implementation correctly handles all observed payload structures and edge cases.

##### 3. Human-Readable `metric_id` Alias Table

For panel feature naming, the following canonical mapping from Refinitiv TRESG codes to human-readable feature IDs is recommended:

| Raw `metric_id` (Silver) | Panel `feature_id` (Monthly Panel) | Description |
| :--- | :--- | :--- |
| `tresgs` | `esg_score` | Overall ESG Score |
| `tresgcs` | `esg_combined_score` | ESG Combined Score (incl. controversies) |
| `tresgccs` | `esg_controversies` | ESG Controversies Score |
| `tresgens` | `esg_environmental` | Environmental Pillar |
| `tresgenrrs` | `esg_resource_use` | Resource Use Sub-Score |
| `tresgeners` | `esg_emissions` | Emissions Sub-Score |
| `tresgenpis` | `esg_env_innovation` | Environmental Innovation Sub-Score |
| `tresgsos` | `esg_social` | Social Pillar |
| `tresgsowos` | `esg_workforce` | Workforce Sub-Score |
| `tresgsohrs` | `esg_human_rights` | Human Rights Sub-Score |
| `tresgsocos` | `esg_community` | Community Sub-Score |
| `tresgsoprs` | `esg_product_responsibility` | Product Responsibility Sub-Score |
| `tresgcgs` | `esg_governance` | Governance Pillar |
| `tresgcgbds` | `esg_management` | Management Sub-Score |
| `tresgcgsrs` | `esg_shareholders` | Shareholders Sub-Score |
| `tresgcgvss` | `esg_csr_strategy` | CSR Strategy Sub-Score |
| `esg_coverage` | `esg_coverage` | Portfolio ESG Holdings Coverage |

**Implementation note:** The alias mapping applies only in the panel construction layer (`silver.monthly_panel`). The Silver observation tables retain the raw Refinitiv `metric_id` tags for provenance.

---

#### 4.5.4. Recommended LOCF Staleness Caps for Factor Panel

Based on the empirical cadence audit:

**ESG cadence:** Weekly. All consecutive `effective_date` deltas in Silver are exactly 7 days (100% of 12,162 observed deltas). The provider (`asOfDate`) recalculates every Friday.

**Theme cadence:** Snapshot-driven (no provider date). Bronze snapshot deltas range from 2–10 days depending on fetch scheduling. Since theme weights have no explicit recalculation date, freshness depends entirely on fetch frequency.

| Feature / Metric Group | Empirical Update Cadence | Typical Reporting Delay | Recommended Panel Staleness Cap | Operational Rationale |
| :--- | :--- | :--- | :--- | :--- |
| **All 16 ESG scores** (`tresgs`, `tresgcs`, `tresgccs`, `tresgens`, `tresgsos`, `tresgcgs`, and 10 sub-pillars) | Weekly (7-day deltas, 100%) | 0–7 days (Fridays) | **3 Months (90 days)** | Weekly recalculation means a 3-month cap covers 12+ missed weeks before declaring stale. Shorter cap than monthly-reporting metrics because weekly data should never have large gaps unless the product is delisted. |
| `esg_coverage` | Weekly (co-reported with scores) | 0–7 days | **3 Months (90 days)** | Quality-gate metric; co-moves with ESG scores. |
| `theme_coverage` | Snapshot-driven (2–10 day fetch deltas) | N/A (snapshot-bound) | **6 Months (180 days)** | No provider recalculation date; coverage is stable and only changes when fund holdings shift materially. |
| **All 491 theme weights** (`dimension_type = 'theme'`) | Snapshot-driven (2–10 day fetch deltas) | N/A (snapshot-bound) | **6 Months (180 days)** | Theme weights change gradually as portfolio rebalances. A 6-month cap covers normal quarterly rebalancing while dropping defunct products. Default unassigned themes to $0.0$. |

---

#### 4.5.5. Panel Construction Rules for Themes & ESG

##### A. Theme Panel Features

1. **Granular child themes (491 features):**
   - Feature ID format: `theme_{dimension_code}` (UUID-based, e.g., `theme_dd8b04fb-8529-47df-b179-4c68d78f34e5`).
   - Value: `rank_adjusted_weight` from `silver.product_dimensions`.
   - Default for missing themes: $0.0$ (no exposure).
   - **No sum-to-1.0 normalization** — themes are overlapping, not mutually exclusive partitions.

2. **Aggregated parent themes (19 features):**
   - Feature ID format: `theme_parent_{parent_theme_id}` (UUID-based).
   - Value: $\sum_{\text{child} \in \text{parent}} \text{child\_rank\_adjusted\_weight}$ via JOIN to `bronze.themes`.
   - Default for missing parent rollup: $0.0$.

3. **Coverage quality gate:**
   - Products with `theme_coverage < 0.7` may be excluded from theme factor regressions (all current products pass this threshold, but the guard should be implemented for future robustness).

##### B. ESG Panel Features

1. **Direct score features (17 features):**
   - Feature IDs: Use the human-readable aliases from Section 4.5.3 (e.g., `esg_score`, `esg_environmental`, `esg_controversies`).
   - Values: Integer scores on $[0, 10]$, carried forward as DOUBLE.
   - No rescaling or percentage conversion.

2. **Coverage quality gate:**
   - Products with `esg_coverage < 0.7` may be excluded from ESG factor regressions.

3. **Asset class coverage (Empirical Audit):**
   Across the live database universe, coverage by dominant asset class (latest snapshot $> 60\%$ allocation) is:

| Asset Class | Total Products | Products w/ Themes | % Themes | Products w/ ESG | % ESG |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Equity** | 11,753 | 10,005 | **85.1%** | 10,658 | **90.7%** |
| **Fixed Income** | 4,495 | 873 | **19.4%** | 961 | **21.4%** |

   - **Cross-Sectional Overlap:** Exactly **904 products** report ESG scores but have zero theme data (predominantly fixed-income funds lacking equity thematic classifications). Exactly **161 products** report theme weights but lack ESG scores.
   - **Panel implication:** Theme and ESG features are primarily populated for Equity-class products. For Fixed Income products, these features will be predominantly `0.0` (theme default) or `NULL` (ESG), and should be filtered out from fixed-income-specific factor regressions.

---

## 5. Master Implementation Phase Checklist

The 13 functional specification rules tracked by the Silver Layer Auditor (`scripts/catalog_silver_tables.py`) are categorized below. The implementation phase must resolve the 12 pending spec items in a single unified refactoring pass:

### 5.1. Database Schema & Storage
- [ ] **Schema Migration (`currency` column):** Amend `silver.product_metrics` in `etfportfolio/core/schema.sql` to include `currency VARCHAR`. *(Status: PENDING)*
- [ ] **AUM Currency Disambiguation:** Populate ISO-4217 currency codes (`USD`, `EUR`, `CAD`, etc.) on `total_net_assets_local` in `silver.product_metrics`, disambiguating `$` against contract currency (`TSE`/`CAD` $\rightarrow$ `CAD`, all others $\rightarrow$ `USD`). Ensure non-AUM unitless metrics remain `NULL`. *(Status: PENDING)*
- [ ] **Outright Pruning of `top_holding`:** Delete `top_holding` extraction loop in `extract_holdings()`. Drops 263,186 sparse rows across 30,128 distinct security names. Overall fund concentration remains tracked by `portfolio_top_10_concentration`. *(Status: PENDING)*

### 5.2. Operable Numeric Cleaning & Metric Scaling
- [ ] **Ratios Decimal Conversion (/100.0):** Divide the **24 growth, return, yield, and coupon metrics** by 100.0 during extraction in `extract_ratios()` to convert percentage points to operable decimal fractions. Retain the **23 unscaled valuation multiples, leverage ratios, maturities, and z-scores** as native floats. *(Status: PENDING)*
- [ ] **Unified Morningstar Medalist Metric (`mstar_medalist_rating`):** Extract canonical `mstar_medalist_rating` (1.0 to 5.0) in `extract_mstar()`, resolving the bug where `q: false` caused all ratings to be classified as analyst-driven. *(Status: PENDING)*
- [ ] **Morningstar Analyst Coverage Proportion (`mstar_analyst_coverage_pct`):** Emit `mstar_analyst_coverage_pct` ($\text{analyst\_count} / 3.0$) measuring the proportion of human coverage across the People, Process, and Parent pillars. *(Status: PENDING)*
- [ ] **Lipper Max-Peer-Count Universe Reduction:** Implement primary universe selection in `extract_lipper()`, collapsing 568 sparse geographic variants into the **20 canonical metrics** (`lipper_{tag}_{horizon}`). Store selected universe and peer count in `raw_value`. *(Status: PENDING)*
- [ ] **Theme Coverage Extraction (`theme_coverage`):** Extract top-level `payload["coverage"]` as scalar metric `theme_coverage` in `silver.product_metrics` (`source = 'theme_weights'`). *(Status: PENDING)*

### 5.3. Dimensional Standardization & Sector Remaps
- [x] **Credit Rating Cleaned Codes:** Map credit rating `dimension_code` to standardized letter grades (`AAA`, `AA`, `A`, `BBB`, `BB`, `B`, `CCC`, `CC`, `C`, `D`, `Not Rated`, `Not Available`). *(Status: APPLIED — verified across all 38,379 rows)*
- [ ] **Country ISO Code Standardizations:** Standardize 133 unmapped country anomaly rows: remap Croatia `CR` $\rightarrow$ `HR`, Bulgaria `BGR` $\rightarrow$ `BG`, Guam `NULL` $\rightarrow$ `GU`, and Uzbekistan `NULL` $\rightarrow$ `UZ`. Retain `dimension_code = NULL` for `Unidentified`. *(Status: PENDING)*
- [ ] **Industry Discontinued Category Remap:** Remap 29 historical rows of `"Telecommunication Services-Discontinued eff 09/19/2020"` to `"Communication Services"`. *(Status: PENDING)*
- [ ] **Maturity Slugs (`mat_*`):** Standardize 40,755 maturity rows by populating `dimension_code` with concise slugs (`mat_lt_1y`, `mat_1_to_3y`, `mat_3_to_5y`, `mat_5_to_10y`, `mat_10_to_20y`, `mat_20_to_30y`, `mat_gt_30y`, `mat_other`). *(Status: PENDING)*

### 5.4. Factor Panel Construction (`silver.monthly_panel`)
- [ ] **Panel Construction Pipeline (`etfportfolio/prep/panel.py`):** Build physical long table `silver.monthly_panel` (`product_id`, `as_of_date`, `asset_class`, `feature_id`, `value`) featuring:
  - Unified month-end calendar spine.
  - Forward-filling via LOCF with empirical staleness caps (Weekly ESG $\rightarrow$ 90d; Monthly Ratios/Holdings/Lipper/Stars $\rightarrow$ 180d; Annual Medalist/Fees $\rightarrow$ 540d; Perpetual `is_passive`).
  - Flattening of the 9 style box coordinates (defaulting the 3 unobserved cells to `0.0`).
  - Aggregation of the 110 raw debt types into the 9 macroeconomic clusters.
  - Aggregation of the 491 child themes into 19 parent rollups via `bronze.themes`.
  - Partition-of-unity sum-to-1.0 validation on `asset_class`, and handling of residual buckets (`Other`, `Unidentified`, `Not Rated`). *(Status: PENDING)*
