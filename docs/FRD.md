# Functional Design Document (FDD): Silver Observations Preprocessing Engine

**Status:** Final / Approved  
**Layer:** Silver (Medallion Architecture)  
**Target Module:** `etfportfolio.observations`  
**CLI Surface:** `uv run python main.py prep [--force]`

---

## 1. Executive Summary & Design Rationale

The **Silver Observations Preprocessing Engine** is responsible for transforming raw, immutable Bronze JSON snapshot blobs (`bronze.payload_blobs` joined with `bronze.snapshots`) into strongly typed, normalized, and deterministic relational tables in DuckDB.

Downstream Gold modules consume these Silver tables to construct monthly Last-Observation-Carried-Forward (LOCF) factor panels, estimate factor loadings, and execute asset-allocation models.

```
                   ┌─────────────────────────────────────────┐
                   │           bronze.snapshots /            │
                   │          bronze.payload_blobs           │
                   └────────────────────┬────────────────────┘
                                        │
                           [Observations Extraction]
                             (python main.py prep)
                                        │
          ┌─────────────────────────────┴─────────────────────────────┐
          ▼                                                           ▼
┌──────────────────────────────────┐        ┌───────────────────────────────────┐
│      silver.product_metrics      │        │    silver.product_dimensions      │
├──────────────────────────────────┤        ├───────────────────────────────────┤
│ product_id            INTEGER    │        │ product_id            INTEGER     │
│ source                VARCHAR    │        │ dimension_type        VARCHAR     │
│ metric_id             VARCHAR    │        │ dimension_name        VARCHAR     │
│ effective_date        DATE       │        │ dimension_code        VARCHAR     │
│ effective_date_source VARCHAR    │        │ effective_date        DATE        │
│ fetched_at            TIMESTAMPTZ│        │ effective_date_source VARCHAR     │
│ value                 DOUBLE     │        │ fetched_at            TIMESTAMPTZ │
│ raw_value             VARCHAR    │        │ value                 DOUBLE      │
│                                  │        │ raw_value             VARCHAR     │
│ PRIMARY KEY:                     │        │                                   │
│ (product_id, source, metric_id,  │        │ PRIMARY KEY:                      │
│  effective_date)                 │        │ (product_id, dimension_type,      │
└──────────────────────────────────┘        │  dimension_name, effective_date)  │
                                            └───────────────────────────────────┘
```

### Core Architecture Decisions (Settled)
1. **Geometric Table Separation:**
   - **`silver.product_metrics`**: Zero-dimensional scalar metrics (financial ratios, growth rates, ESG scores, qualitative ratings, AUM, expense ratios, concentration).
   - **`silver.product_dimensions`**: One-dimensional exposure vectors, constituent breakdowns, and categorical memberships (asset classes, industries, countries, credit ratings, debt types, maturities, thematic loadings, top-10 holdings, and current/historical style box assignments).
2. **Standardized Extractor Interface (`ExtractionResult`):**
   - Every extractor returns a uniform dataclass `ExtractionResult(metrics=[...], dimensions=[...])`. This completely decouples `pipeline.py` from endpoint-specific branching.
3. **Point-in-Time Provenance Tracking (`effective_date_source`):**
   - Dates are resolved strictly via a 3-tier hierarchy:
     $$\text{Item-level embedded date} \longrightarrow \text{Payload top-level date} \longrightarrow \text{Snapshot ingestion date}$$
   - The column `effective_date_source` explicitly records this origin using a controlled vocabulary: `'item'`, `'payload'`, or `'snapshot'`.
4. **Multicollinearity Elimination:**
   - Discards overlapping raw `weight` in themes; ingests only `rank_adjusted_weight`.
   - Excludes `currency` and `geographic` allocations from holdings, preserving `investor_country` as the sole geographic exposure breakdown.
5. **In-Memory Batch Staging & Deduplication:**
   - DuckDB throws hard `Constraint Error` exceptions when duplicate keys appear within the same `executemany` batch.
   - Snapshots are processed in 500-snapshot batches staged in memory. Intra-payload items are naturally distinct; inter-snapshot key collisions within the batch are resolved by overwriting with the newer observation (`fetched_at`).
6. **Strict Validation vs. Graceful Skipping:**
   - **Missing optional sections** (e.g., absent `Manager Tenure`, absent `Annual Report`, or omitted debt breakdowns) are gracefully skipped without creating empty rows.
   - **Present but unmapped or malformed categorical values** (e.g., an unrecognized Morningstar pillar string or invalid date format) throw immediate hard errors (`ValueError`) to alert developers to vendor schema changes.
7. **Purity of Silver Storage:**
   - Numeric fields are cleanly parsed and typecast to `DOUBLE` while preserving the exact unparsed text in `raw_value`. Outlier clipping, winsorization, and statistical cleaning are deferred to Gold.

---

## 2. DuckDB Schema DDL

In `etfportfolio/core/schema.sql`, the Silver layer is defined as follows (note: legacy tables `silver.metric_observations`, `silver.portfolio_allocations`, and `silver.theme_exposures` are dropped manually by the operator):

```sql
CREATE SCHEMA IF NOT EXISTS silver;

-- 1. Fund-Level Scalar Observations
CREATE TABLE IF NOT EXISTS silver.product_metrics (
    product_id            INTEGER NOT NULL,
    source                VARCHAR NOT NULL,       -- 'ratios', 'profile', 'esg', 'mstar', 'lipper', 'holdings'
    metric_id             VARCHAR NOT NULL,       -- lower_snake_case identifier
    effective_date        DATE NOT NULL,          -- Point-in-time observation/publication date
    effective_date_source VARCHAR NOT NULL,       -- 'item', 'payload', or 'snapshot'
    fetched_at            TIMESTAMP WITH TIME ZONE NOT NULL, -- Snapshot creation timestamp
    value                 DOUBLE NOT NULL,        -- Clean numeric float for matrix math
    raw_value             VARCHAR NOT NULL,       -- Exact source string representation
    PRIMARY KEY (product_id, source, metric_id, effective_date)
);

-- 2. Fund Portfolio Allocations, Dimensions, and Loadings
CREATE TABLE IF NOT EXISTS silver.product_dimensions (
    product_id            INTEGER NOT NULL,
    dimension_type        VARCHAR NOT NULL,       -- 'asset_class', 'industry', 'country', 'credit_rating', 'debt_type', 'maturity', 'theme', 'top_holding', 'style_box', 'style_box_hist'
    dimension_name        VARCHAR NOT NULL,       -- Constituent or bucket display name
    dimension_code        VARCHAR,                -- Standardized code (ISO country, theme UUID, rating, conids, or style box slug)
    effective_date        DATE NOT NULL,          -- Point-in-time observation date
    effective_date_source VARCHAR NOT NULL,       -- 'item', 'payload', or 'snapshot'
    fetched_at            TIMESTAMP WITH TIME ZONE NOT NULL, -- Snapshot creation timestamp
    value                 DOUBLE NOT NULL,        -- Normalized decimal proportion [0.0, 1.0] or binary indicator (1.0)
    raw_value             VARCHAR NOT NULL,       -- Exact source string representation
    PRIMARY KEY (product_id, dimension_type, dimension_name, effective_date)
);

-- 3. Incremental Processing Watermark
CREATE TABLE IF NOT EXISTS silver.processed_snapshots (
    snapshot_id           BIGINT PRIMARY KEY,
    processed_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

---

## 3. Data Types and Extractor Contracts

### 3.1 Dataclass Definitions (`etfportfolio/observations/utils.py`)

All extractors return an instance of `ExtractionResult`:

```python
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

@dataclass(slots=True)
class MetricRow:
    product_id: int
    source: str
    metric_id: str
    effective_date: date
    effective_date_source: str  # 'item' | 'payload' | 'snapshot'
    fetched_at: datetime
    value: float
    raw_value: str

    def as_tuple(self) -> tuple:
        return (
            self.product_id,
            self.source,
            self.metric_id,
            self.effective_date,
            self.effective_date_source,
            self.fetched_at,
            self.value,
            self.raw_value,
        )

@dataclass(slots=True)
class DimensionRow:
    product_id: int
    dimension_type: str
    dimension_name: str
    dimension_code: str | None
    effective_date: date
    effective_date_source: str  # 'item' | 'payload' | 'snapshot'
    fetched_at: datetime
    value: float
    raw_value: str

    def as_tuple(self) -> tuple:
        return (
            self.product_id,
            self.dimension_type,
            self.dimension_name,
            self.dimension_code,
            self.effective_date,
            self.effective_date_source,
            self.fetched_at,
            self.value,
            self.raw_value,
        )

@dataclass(slots=True)
class ExtractionResult:
    metrics: list[MetricRow] = field(default_factory=list)
    dimensions: list[DimensionRow] = field(default_factory=list)
```

---

## 4. Date and String Parsing Utility Specifications

All parsing utilities reside in `etfportfolio/observations/utils.py`.

### 4.1 Hierarchical Date Parsing (`parse_effective_date`)
```python
def parse_effective_date(
    val: Any,
    fallback_date: date,
    default_source: str = "payload",
) -> tuple[date, str]:
    """Parses date from epoch ms (int/float), YYYYMMDD, YYYY-MM-DD, or YYYY/MM/DD string.

    Returns:
        (resolved_date, effective_date_source)
        where effective_date_source is default_source if val is successfully parsed,
        or 'snapshot' if val is None, <= 0, unparseable, or invalid.
    """
```
- Supported formats:
  - Integer / Float: epoch milliseconds (e.g., `1785470400000` $\to$ `2026-07-31`). If $\le 0$, fallback to `'snapshot'`.
  - 8-digit numeric string: `YYYYMMDD` (e.g., `"20260731"` $\to$ `2026-07-31`).
  - Delimited string: `YYYY-MM-DD` or `YYYY/MM/DD`.
- If parsing fails or input is null: returns `(fallback_date, 'snapshot')`.

### 4.2 AUM and Embedded Date Parsing (`parse_net_assets`)
```python
def parse_net_assets(
    raw_val: Any,
    fallback_date: date,
) -> tuple[float, str, date, str] | None:
    """Parses total net assets from strings like '$78.63B (2026/07/31)', '€500M (2025/12/31)', or '2,5B'.

    Returns:
        (numeric_value, raw_value_str, effective_date, effective_date_source)
        or None if missing or completely unparseable.
    """
```
- **Date Extraction:** Searches for `\(([0-9]{4}[-/][0-9]{2}[-/][0-9]{2})\)`. If found, parses as `date` with source `'item'`. If absent, uses `fallback_date` with source `'snapshot'`.
- **Amount Extraction:**
  1. Captures numeric portion and magnitude unit: `r'([0-9.,]+)\s*([kKmMbBtT]?)'`.
  2. Number normalization: Handles European comma decimals vs. US thousands separators:
     - If string contains `,` and no `.`, and comma is followed by 1 or 2 digits, replace `,` with `.` (e.g., `"2,5"` $\to$ `2.5`).
     - Otherwise, strip commas as thousands separators (e.g., `"1,250.5"` $\to$ `1250.5`).
  3. Magnitude multipliers:
     - `k` or `K`: $\times 10^3$
     - `m` or `M`: $\times 10^6$
     - `b` or `B`: $\times 10^9$
     - `t` or `T`: $\times 10^{12}$
     - No unit: $\times 1.0$
  4. Returns `(float_value, str(raw_val).strip(), effective_date, effective_date_source)`.

### 4.3 Manager Tenure Duration (`parse_manager_tenure`)
```python
def parse_manager_tenure(
    tenure_val: Any,
    ref_date: date,
) -> tuple[float, str] | None:
    """Computes tenure in years relative to ref_date from start date string (e.g. '2013/01/01').

    Returns:
        (tenure_years, raw_value_str)
        or None if tenure_val is missing, empty, or None.
    Raises:
        ValueError if tenure_val is present but cannot be parsed as a valid date.
    """
```
- If `tenure_val` is `None` or whitespace: return `None`.
- Parse date from string using `YYYY/MM/DD`, `YYYY-MM-DD`, or `YYYYMMDD`. If invalid, raise `ValueError(f"Invalid Manager Tenure date string: '{tenure_val}'")`.
- Calculation:
  $$\text{tenure\_years} = \max\left(0.0, \frac{(\text{ref\_date} - \text{start\_date}).\text{days}}{365.25}\right)$$
- Return `(round(tenure_years, 4), str(tenure_val).strip())`.

### 4.4 Tag Sanitization and Credit Rating Cleaning
- `sanitize_metric_id(tag: str) -> str`: Converts any string to `lower_snake_case` stripping non-alphanumeric characters. (e.g., `"Price/Earnings"` $\to$ `"price_earnings"`, `"TRESGS"` $\to$ `"tresgs"`).
- `clean_credit_rating(raw_name: str) -> str`: Strips leading `"% Quality/"` or `"% Quality "` prefixes (e.g., `"% Quality/BBB"` $\to$ `"BBB"`, `"% Quality Not Available"` $\to$ `"Not Available"`).

---

## 5. Extractor Routing and Detailed Specifications

Routing table matching Bronze snapshot URL prefixes:

| Bronze URL Prefix | Extractor | Target Silver Tables |
| :--- | :--- | :--- |
| `/tws.proxy/fundamentals/mf_ratios_fundamentals/` | `extract_ratios` | `silver.product_metrics` |
| `/tws.proxy/fundamentals/mf_profile_and_fees/` | `extract_profile` | `silver.product_metrics` |
| `/tws.proxy/impact/esg/` | `extract_esg` | `silver.product_metrics` |
| `/tws.proxy/mstar/fund/detail?conid=` | `extract_mstar` | `silver.product_metrics` |
| `/tws.proxy/fundamentals/mf_lip_ratings/` | `extract_lipper` | `silver.product_metrics` |
| `/tws.proxy/fundamentals/mf_holdings/` | `extract_holdings` | `silver.product_metrics` & `silver.product_dimensions` |
| `/tws.proxy/knowledge-graph/ui/fund?conid=` | `extract_theme_weights` | `silver.product_dimensions` |

---

### 5.1 Fundamentals & Multiples (`extract_ratios`)
- **Endpoint Prefix:** `/tws.proxy/fundamentals/mf_ratios_fundamentals/`
- **Output:** `ExtractionResult.metrics`
- **Source Identifier:** `'ratios'`
- **Date Resolution:** Top-level `payload.get("as_of_date")` (epoch ms). Source = `'payload'`; fallback = `'snapshot'`.
- **Sections Processed:** `["dividend", "financials", "fixed_income", "ratios", "zscore"]`.
- **Extraction Rules:**
  - Iterate through all items in the processed sections.
  - Skip items where `item.get("value") is None`. Ignore peer comparison fields (`avg`, `min`, `max`, `vs`, `percentile`).
  - `metric_id`: `sanitize_metric_id(item["name_tag"])`.
  - `value`: `float(item["value"])`.
  - `raw_value`: `str(item.get("value_fmt") if item.get("value_fmt") is not None else item["value"])`.

---

### 5.2 Fund Profile, Fees & Style (`extract_profile`)
- **Endpoint Prefix:** `/tws.proxy/fundamentals/mf_profile_and_fees/`
- **Output:** `ExtractionResult.metrics` and `ExtractionResult.dimensions`
- **Source Identifier:** `'profile'`
- **Fallback Date:** `snapshot_created_at.date()`.
- **Extraction Sections:**
  1. **Expenses Allocation (`expenses_allocation`):**
     - Iterate through items. If `item.get("ratio") is None`, skip.
     - `"Management Expenses"` $\to$ `metric_id = 'management_expense_ratio'`, `value = float(ratio)`, `raw_value = str(item.get("value", ratio))`, date source = `'snapshot'`.
     - `"Non-Management Expenses"` $\to$ `metric_id = 'non_management_expense_ratio'`, `value = float(ratio)`, `raw_value = str(item.get("value", ratio))`, date source = `'snapshot'`.
  2. **Fund Profile Scalars (`fund_and_profile`):**
     - Search by `name_tag` or `name`:
     - **Total Expense Ratio (`Total_Expense_Ratio`):**
       - Strip `%`, divide by 100.0 (e.g., `"0.06%"` $\to$ `0.0006`).
       - `metric_id = 'total_expense_ratio'`, date source = `'snapshot'`.
     - **Management Approach (`Management_Approach`):**
       - If `"passive"` (case-insensitive) $\to$ `1.0`.
       - If `"active"` (case-insensitive) $\to$ `0.0`.
       - If unrecognized string, raise `ValueError(f"Unknown Management Approach: '{val}'")`.
       - `metric_id = 'is_passive'`, `raw_value = str(val)`, date source = `'snapshot'`.
     - **Total Net Assets (`Total Net Assets (Month End)`):**
       - Parse using `parse_net_assets(val, snapshot_created_at.date())`.
       - If valid, emits `metric_id = 'total_net_assets_local'`.
     - **Manager Tenure (`Manager Tenure`):**
       - Parse using `parse_manager_tenure(val, snapshot_created_at.date())`.
       - If valid, emits `metric_id = 'manager_tenure_years'`, date source = `'snapshot'`.
  3. **Audited Annual Reports (`reports`):**
     - Iterate over *all* entries in `reports` where `report.get("name") == "Annual Report"`.
     - Resolve report date from `report.get("as_of_date")` (epoch ms). Source = `'item'`.
     - Locate field where `field.get("name") == "Total Net Expense"`.
     - Strip `%`, divide by 100.0 (e.g., `"0.0564%"` $\to$ `0.000564`).
     - Emit `metric_id = 'audited_net_expense_ratio'`, `raw_value = str(val)`.
  4. **Morningstar Style Box (`mstar`):**
     - Discard `mstar["name"]` entirely.
     - Evaluate coordinate tuples in `mstar.get("selected", [])` and `mstar.get("hist", [])`. If both are empty or absent, cleanly emit nothing.
     - Validate axis tags against accepted taxonomy:
       - `x_axis_tag` (or `x_axis`): `{"value", "core", "growth"}`
       - `y_axis_tag` (or `y_axis`): `{"large", "multi", "mid", "small"}`
       - If unexpected tags are present or coordinates are out of bounds, raise `ValueError`.
     - Output goes directly to `silver.product_dimensions` (no scalar style metrics in `silver.product_metrics`):
       - `dimension_type`: `'style_box'` for `selected` coordinates; `'style_box_hist'` for `hist` coordinates.
       - `dimension_name`: `f"{y_label.title()} {x_label.title()}"` (e.g., `"Large Core"`, `"Mid Growth"`).
       - `dimension_code`: `f"{y_label.lower()}_{x_label.lower()}"` (e.g., `"large_core"`, `"mid_growth"`).
       - `effective_date`: `snapshot_created_at.date()`, `effective_date_source`: `'snapshot'`.
       - `value`: `1.0` (sparse indicator: each assigned quadrant generates a row; unassigned quadrants have no rows).
       - `raw_value`: string coordinate representation (e.g., `"[1, 1]"`).

---

### 5.3 Refinitiv ESG Scores (`extract_esg`)
- **Endpoint Prefix:** `/tws.proxy/impact/esg/`
- **Output:** `ExtractionResult.metrics`
- **Source Identifier:** `'esg'`
- **Date Resolution:** `payload.get("asOfDate")` (`YYYYMMDD` string). Source = `'payload'`; fallback = `'snapshot'`.
- **Extraction Rules:**
  - **Portfolio Coverage:** If `payload.get("coverage") is not None`, emit:
    - `metric_id = 'esg_coverage'`, `value = float(coverage)`, `raw_value = str(coverage)`.
  - **Score Hierarchy:** Recursively traverse `content` and all nested `children`:
    - For every node having non-null `name` and `value`:
      - `metric_id = sanitize_metric_id(node["name"])` (e.g., `TRESGS` $\to$ `tresgs`, `TRESGENRRS` $\to$ `tresgenrrs`).
      - `value = float(node["value"])`.
      - `raw_value = str(node["value"])`.

---

### 5.4 Morningstar Ratings & Pillars (`extract_mstar`)
- **Endpoint Prefix:** `/tws.proxy/mstar/fund/detail?conid=`
- **Output:** `ExtractionResult.metrics`
- **Source Identifier:** `'mstar'`
- **Commentary:** Discard the `commentary` array completely.
- **Date Resolution:**
  - For each summary item: item `publish_date` (`YYYYMMDD` or `YYYY-MM-DD`, source = `'item'`) $\to$ top-level `payload.get("as_of_date")` (source = `'payload'`) $\to$ fallback `snapshot_created_at.date()` (source = `'snapshot'`).
- **Ordinal Mappings:**
  ```python
  MEDALIST_MAP = {
      "gold": 5.0, "silver": 4.0, "bronze": 3.0, "neutral": 2.0, "negative": 1.0
  }
  PILLAR_MAP = {
      "high": 5.0, "above_average": 4.0, "average": 3.0, "below_average": 2.0, "low": 1.0
  }
  STAR_MAP = {
      "1": 1.0, "2": 2.0, "3": 3.0, "4": 4.0, "5": 5.0
  }
  SUSTAINABILITY_MAP = {
      "1": 1.0, "2": 2.0, "3": 3.0, "4": 4.0, "5": 5.0,
      "high": 5.0, "above_average": 4.0, "average": 3.0, "below_average": 2.0, "low": 1.0
  }
  ```
- **Disambiguation Logic:**
  - Skip non-rating items: `category`, `category_index`.
  - Skip unrated/review statuses: `"under_review"`, `"not_applicable"`, `"under review"`, `"na"`, `"n/a"`, `""`, `"-"`.
  - Check quantitative status:
    ```python
    is_quant = bool(item.get("q") is True or str(item.get("id", "")).startswith("q_"))
    ```
  - Strip leading `q_` from `item["id"]`.
  - If `base_metric == "quantitative_rating"`, base becomes `"medalist_rating"`.
  - Suffix assignment:
    - If base metric is `medalist_rating`, `people`, `process`, or `parent`:
      - Append `_quant` if `is_quant` else `_analyst` (e.g., `mstar_process_analyst`, `mstar_medalist_rating_quant`).
    - If `morningstar_rating`: `mstar_morningstar_rating` (no suffix).
    - If `sustainability_rating`: `mstar_sustainability_rating` (no suffix).
  - Value mapping:
    - If lowercase string is not in the respective mapping dictionary, **raise `ValueError(f"Unrecognized rating string '{raw_val}' for pillar '{pillar_id}'")`**.
    - Emit mapped numeric `float` and original `raw_val`.

---

### 5.5 Lipper Ratings by Country (`extract_lipper`)
- **Endpoint Prefix:** `/tws.proxy/fundamentals/mf_lip_ratings/`
- **Output:** `ExtractionResult.metrics`
- **Source Identifier:** `'lipper'`
- **Disambiguation Rule:** Do **not** average universes. Embed sanitized country name in `metric_id`.
- **Horizon Mapping:**
  - `overall` $\to$ `overall`
  - `3_year` $\to$ `3yr`
  - `5_year` $\to$ `5yr`
  - `10_year` $\to$ `10yr`
- **Extraction Rules:**
  - Iterate through all universes in `payload.get("universes", [])`.
  - Date: universe `as_of_date` (epoch ms). Source = `'item'`; fallback = `'snapshot'`.
  - Universe Country: `country = sanitize_metric_id(u.get("name") or "global")`.
  - For each horizon:
    - Iterate through ratings items (`consistent_return`, `total_return`, `preservation`, `tax_efficiency`, `expense`):
      - `tag = sanitize_metric_id(item["name_tag"])`
      - `metric_id = f"lipper_{tag}_{horizon_suffix}_{country}"`
      - `value = float(item["rating"]["value"])`
      - `raw_value = str(item["rating"]["value"])`

---

### 5.6 Holdings & Portfolio Dimensions (`extract_holdings`)
- **Endpoint Prefix:** `/tws.proxy/fundamentals/mf_holdings/`
- **Output:** Populates **both** `ExtractionResult.metrics` and `ExtractionResult.dimensions`!
- **Date Resolution:** Top-level `payload.get("as_of_date")` (epoch ms). Source = `'payload'`; fallback = `'snapshot'`.
- **A. Concentration Scalar Metric (`silver.product_metrics`):**
  - If `payload.get("top_10_weight")` is present and non-empty:
    - Strip `%`, divide by 100.0 (e.g., `"30.47%"` $\to$ `0.3047`).
    - `source = 'holdings'`, `metric_id = 'portfolio_top_10_concentration'`, `value = 0.3047`, `raw_value = "30.47%"`.
- **B. Allocation Dimensions (`silver.product_dimensions`):**
  - **Explicit Exclusions:** Discard `currency` and `geographic` completely (eliminates collinearity).
  - **Breakdown Mappings:**
    | Payload Key | `dimension_type` | `dimension_name` Rule | `dimension_code` Rule | `value` Rule |
    | :--- | :--- | :--- | :--- | :--- |
    | `allocation_self` | `'asset_class'` | `item["name"].strip()` | `None` | `weight / 100.0` |
    | `investor_country` | `'country'` | `item["name"].strip()` | `item.get("country_code")` | `weight / 100.0` |
    | `industry` | `'industry'` | `item["name"].strip()` | `None` | `weight / 100.0` |
    | `debtor` | `'credit_rating'` | `clean_credit_rating(item["name"])` | `clean_credit_rating(item["name"])` | `weight / 100.0` |
    | `debt_type` | `'debt_type'` | `item["name"].strip()` | `None` | `weight / 100.0` |
    | `maturity` | `'maturity'` | `item["name"].strip()` | `None` | `weight / 100.0` |
    | `top_10` | `'top_holding'` | `f"{ticker} - {name}"` (or `{name}`) | Comma-separated `conids` | `assets_pct / 100.0` |
  - **Top-10 Holdings Detail:**
    - For each holding in `payload.get("top_10", [])`:
      - `dimension_name`: If `item.get("ticker")` is present and non-empty:
        ```python
        f"{item['ticker'].strip()} - {item['name'].strip()}"
        ```
        Otherwise: `item["name"].strip()`.
      - `dimension_code`: `",".join(str(c) for c in item.get("conids", [])) or None`.
      - `value`: Strip `<`, strip `%`, divide by 100.0. If `assets_pct` is non-numeric (`"-"`, `"N/A"`), skip item.
      - `raw_value`: `str(item["assets_pct"])`.

---

### 5.7 Thematic Factor Loadings (`extract_theme_weights`)
- **Endpoint Prefix:** `/tws.proxy/knowledge-graph/ui/fund?conid=`
- **Output:** `ExtractionResult.dimensions`
- **Date Resolution:** Snapshot creation date `snapshot_created_at.date()`. Source = `'snapshot'`.
- **Extraction Rules:**
  - Iterate through `payload.get("themes", [])`.
  - Discard raw `weight`. Ingest only `rank_adjusted_weight`.
  - `dimension_type = 'theme'`.
  - `dimension_name = theme["name"].strip()`.
  - `dimension_code = str(theme["key"]).strip()`.
  - `value = float(theme["rank_adjusted_weight"])`.
  - `raw_value = str(theme["rank_adjusted_weight"])`.

---

## 6. Pipeline Execution & In-Memory Deduplication

### 6.1 Execution Flow (`etfportfolio/observations/pipeline.py`)

1. **Batch Fetching:**
   Fetch unparsed snapshots joined with blobs in chunks of `BATCH_SIZE = 500`:
   ```sql
   SELECT
       s.snapshot_id,
       s.product_id,
       s.url_prefix,
       s.created_at,
       b.payload
   FROM bronze.snapshots s
   JOIN bronze.payload_blobs b ON s.hash = b.hash
   LEFT JOIN silver.processed_snapshots p ON s.snapshot_id = p.snapshot_id
   WHERE p.snapshot_id IS NULL
   ORDER BY s.snapshot_id ASC;
   ```
2. **In-Memory Batch Staging & Deduplication:**
   To prevent DuckDB `Constraint Error: Duplicate key` failures inside `executemany`:
   ```python
   staged_metrics: dict[tuple, tuple] = {}
   staged_dimensions: dict[tuple, tuple] = {}
   processed_ids: list[tuple[int]] = []

   for snapshot_id, product_id, url_prefix, created_at, raw_blob in chunk:
       data = decompress_payload(raw_blob)
       if not data:
           processed_ids.append((snapshot_id,))
           continue

       extractor = EXTRACTOR_REGISTRY.get(url_prefix)
       if extractor is None:
           processed_ids.append((snapshot_id,))
           continue

       result: ExtractionResult = extractor(product_id, data, created_at)

       for m in result.metrics:
           key = (m.product_id, m.source, m.metric_id, m.effective_date)
           # Inter-snapshot collision: latest fetched_at overwrites
           staged_metrics[key] = m.as_tuple()

       for d in result.dimensions:
           key = (d.product_id, d.dimension_type, d.dimension_name, d.effective_date)
           # Inter-snapshot collision: latest fetched_at overwrites
           staged_dimensions[key] = d.as_tuple()

       processed_ids.append((snapshot_id,))
   ```

3. **Atomic Batch Upsert:**
   Wrap database writes in an explicit DuckDB transaction:
   ```sql
   -- silver.product_metrics Upsert
   INSERT INTO silver.product_metrics (
       product_id, source, metric_id, effective_date, effective_date_source, fetched_at, value, raw_value
   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
   ON CONFLICT (product_id, source, metric_id, effective_date) DO UPDATE SET
       value = EXCLUDED.value,
       raw_value = EXCLUDED.raw_value,
       effective_date_source = EXCLUDED.effective_date_source,
       fetched_at = EXCLUDED.fetched_at;

   -- silver.product_dimensions Upsert
   INSERT INTO silver.product_dimensions (
       product_id, dimension_type, dimension_name, dimension_code, effective_date, effective_date_source, fetched_at, value, raw_value
   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
   ON CONFLICT (product_id, dimension_type, dimension_name, effective_date) DO UPDATE SET
       value = EXCLUDED.value,
       dimension_code = EXCLUDED.dimension_code,
       raw_value = EXCLUDED.raw_value,
       effective_date_source = EXCLUDED.effective_date_source,
       fetched_at = EXCLUDED.fetched_at;

   -- Watermark Persistence
   INSERT INTO silver.processed_snapshots (snapshot_id, processed_at)
   VALUES (?, CURRENT_TIMESTAMP);
   ```

4. **Force Reset Behavior:**
   When `run_observations(force=True)` is called:
   ```sql
   BEGIN TRANSACTION;
   DELETE FROM silver.processed_snapshots;
   DELETE FROM silver.product_metrics;
   DELETE FROM silver.product_dimensions;
   COMMIT;
   ```

---

## 7. Concrete End-to-End Extraction Examples

### Example 1: Morningstar Ratings Disambiguation
**Source Snapshot (`mstar/fund/detail?conid=`):**
```json
{
  "as_of_date": "20260731",
  "summary": [
    {"id": "medalist_rating", "value": "Gold", "q": false, "publish_date": "20260427"},
    {"id": "process", "value": "High", "q": false, "publish_date": "20260427"},
    {"id": "q_process", "value": "Below_Average", "q": true, "publish_date": "20260731"},
    {"id": "morningstar_rating", "value": "4", "q": false, "publish_date": "20260731"}
  ]
}
```
**Stored in `silver.product_metrics`:**
| product_id | source | metric_id | effective_date | effective_date_source | value | raw_value |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `52197301` | `'mstar'` | `'mstar_medalist_rating_analyst'` | `2026-04-27` | `'item'` | `5.0` | `'Gold'` |
| `52197301` | `'mstar'` | `'mstar_process_analyst'` | `2026-04-27` | `'item'` | `5.0` | `'High'` |
| `52197301` | `'mstar'` | `'mstar_process_quant'` | `2026-07-31` | `'item'` | `2.0` | `'Below_Average'` |
| `52197301` | `'mstar'` | `'mstar_morningstar_rating'` | `2026-07-31` | `'item'` | `4.0` | `'4'` |

---

### Example 2: Lipper Ratings Preserving Country Universes
**Source Snapshot (`mf_lip_ratings`):**
```json
{
  "universes": [
    {
      "name": "United States",
      "as_of_date": 1785470400000,
      "3_year": [{"name_tag": "consistent_return", "rating": {"value": 4}}]
    },
    {
      "name": "Chile",
      "as_of_date": 1785470400000,
      "3_year": [{"name_tag": "consistent_return", "rating": {"value": 5}}]
    }
  ]
}
```
**Stored in `silver.product_metrics`:**
| product_id | source | metric_id | effective_date | effective_date_source | value | raw_value |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `52197301` | `'lipper'` | `'lipper_consistent_return_3yr_united_states'` | `2026-07-31` | `'item'` | `4.0` | `'4'` |
| `52197301` | `'lipper'` | `'lipper_consistent_return_3yr_chile'` | `2026-07-31` | `'item'` | `5.0` | `'5'` |

---

### Example 3: Holdings Dual Output & Top-10
**Source Snapshot (`mf_holdings`):**
```json
{
  "as_of_date": 1785470400000,
  "top_10_weight": "30.47%",
  "investor_country": [
    {"name": "United States", "country_code": "US", "weight": 71.2451}
  ],
  "top_10": [
    {
      "name": "GUGGENHEIM STRATEGIC OPPORTUNITIES FUND",
      "conids": [86174372],
      "assets_pct": "3.49%"
    },
    {
      "name": "MICROSOFT CORP",
      "ticker": "MSFT",
      "conids": [272093],
      "assets_pct": "<0.01%"
    }
  ]
}
```
**Stored in `silver.product_metrics`:**
| product_id | source | metric_id | effective_date | effective_date_source | value | raw_value |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `52197301` | `'holdings'` | `'portfolio_top_10_concentration'` | `2026-07-31` | `'payload'` | `0.3047` | `'30.47%'` |

**Stored in `silver.product_dimensions`:**
| product_id | dimension_type | dimension_name | dimension_code | effective_date | effective_date_source | value | raw_value |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `52197301` | `'country'` | `'United States'` | `'US'` | `2026-07-31` | `'payload'` | `0.712451` | `'71.25%'` |
| `52197301` | `'top_holding'` | `'GUGGENHEIM STRATEGIC OPPORTUNITIES FUND'` | `'86174372'` | `2026-07-31` | `'payload'` | `0.0349` | `'3.49%'` |
| `52197301` | `'top_holding'` | `'MSFT - MICROSOFT CORP'` | `'272093'` | `2026-07-31` | `'payload'` | `0.0001` | `'<0.01%'` |

---

### Example 4: AUM, Manager Tenure, and Audited Reports
**Source Snapshot (`mf_profile_and_fees` with snapshot creation `2026-08-15`):**
```json
{
  "fund_and_profile": [
    {"name": "Total Expense Ratio", "value": "0.06%"},
    {"name": "Management Approach", "value": "Passive"},
    {"name": "Total Net Assets (Month End)", "value": "$78.63B (2026/07/31)"},
    {"name": "Manager Tenure", "value": "2013/01/01"}
  ],
  "reports": [
    {
      "name": "Annual Report",
      "as_of_date": 1761883200000,
      "fields": [{"name": "Total Net Expense", "value": "0.0564%"}]
    }
  ]
}
```
**Stored in `silver.product_metrics`:**
| product_id | source | metric_id | effective_date | effective_date_source | value | raw_value |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `52197301` | `'profile'` | `'total_expense_ratio'` | `2026-08-15` | `'snapshot'` | `0.0006` | `'0.06%'` |
| `52197301` | `'profile'` | `'is_passive'` | `2026-08-15` | `'snapshot'` | `1.0` | `'Passive'` |
| `52197301` | `'profile'` | `'total_net_assets_local'` | `2026-07-31` | `'item'` | `78630000000.0` | `'$78.63B (2026/07/31)'` |
| `52197301` | `'profile'` | `'manager_tenure_years'` | `2026-08-15` | `'snapshot'` | `13.6208` | `'2013/01/01'` |
| `52197301` | `'profile'` | `'audited_net_expense_ratio'` | `2025-10-31` | `'item'` | `0.000564` | `'0.0564%'` |

---

### Example 5: Style Box Assignments (Current & Historical)
**Source Snapshot (`mf_profile_and_fees` with snapshot creation `2026-08-15`):**
```json
{
  "mstar": {
    "x_axis_tag": ["value", "core", "growth"],
    "y_axis_tag": ["large", "multi", "mid", "small"],
    "selected": [[1, 1], [1, 2]],
    "hist": [[0, 0]]
  }
}
```
**Stored in silver.product_dimensions:**
| product_id | dimension_type | dimension_name | dimension_code | effective_date | effective_date_source | value | raw_value |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `52197301` | `'style_box'` | `'Multi Core'` | `'multi_core'` | `2026-08-15` | `'snapshot'` | `1.0` | `'[1, 1]'` |
| `52197301` | `'style_box'` | `'Mid Core'` | `'mid_core'` | `2026-08-15` | `'snapshot'` | `1.0` | `'[1, 2]'` |
| `52197301` | `'style_box_hist'` | `'Large Value'` | `'large_value'` | `2026-08-15` | `'snapshot'` | `1.0` | `'[0, 0]'` |

---

## 8. Implementation Verification & Acceptance Criteria

1. **Schema Integrity:**
   - Verify `schema.sql` contains correct table DDL for `silver.product_metrics`, `silver.product_dimensions`, and `silver.processed_snapshots`.
   - Running `db_connection()` must create the two new tables without SQL syntax errors.
2. **Extractor Contract:**
   - Every extractor in `EXTRACTOR_REGISTRY` returns `ExtractionResult`.
   - Unit tests pass with mock JSON payloads matching all 7 endpoints.
3. **Pipeline Invariance:**
   - Processing 500 snapshots with duplicate or overlapping keys raises zero DuckDB constraint errors.
   - `python main.py prep` runs to completion and marks snapshots in `silver.processed_snapshots`.
   - `python main.py prep --force` completely clears both silver observation tables and reprocesses from bronze.
4. **Validation Hard-Errors:**
   - Modifying a Morningstar pillar value to `"Nonexistent_Rating"` in a test payload immediately raises `ValueError`.
   - Modifying `Management Approach` to `"Robotic"` raises `ValueError`.
   - Omission of `Manager Tenure` or `reports` cleanly skips without raising an exception.