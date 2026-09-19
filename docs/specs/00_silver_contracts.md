# Silver Layer Contracts & Invariants

- **Status:** Implementation specification (condensed from FRD §1.2, §2)
- **Scope:** `etfportfolio/prep` extractors, `etfportfolio/core/schema.sql`, Silver observation tables
- **Sibling specs:** [01_silver_extractors.md](01_silver_extractors.md) · [02_factor_panel.md](02_factor_panel.md)
- **Empirical audit trail:** [../research/empirical_audit_archive.md](../research/empirical_audit_archive.md)
- **Priority:** Data Accuracy / Correctness >> Codebase Simplicity / Clarity > Storage Efficiency > Runtime Performance >> Auth Security

---

## 1. Core Architectural Invariants

### 1.1 Single-pass operable extraction

Extractors in `etfportfolio/prep/extractors.py` convert each raw payload item into:

| Field | Type | Role |
| :--- | :--- | :--- |
| `value` | `DOUBLE NOT NULL` | Clean operable numeric for analytics |
| `currency` | `VARCHAR` (nullable) | ISO-4217 code, or `NULL` for unitless metrics |
| `raw_value` | `VARCHAR NOT NULL` | Exact provider display string (audit trail) |

No second cleaning pass is permitted. Silver stores the operable number and the original display string in the same row.

### 1.2 Report-date precedence

Effective dates **must** extract the explicit item/payload `as_of_date`, `publish_date`, or report date over snapshot fetch timestamps.

| Rank | Source token (`effective_date_source`) | When used |
| :---: | :--- | :--- |
| 1 | `'item'` | Per-item date (`publish_date`, parenthesized AUM date, report `as_of_date`) |
| 2 | `'payload'` | Top-level payload `as_of_date` / `asOfDate` |
| 3 | `'snapshot'` | `snapshot_created_at.date()` — **fallback only** when no report date exists |

Snapshot timestamp is strictly a fallback. Never prefer fetch time when a report date is present.

### 1.3 Allocation sum-to-1.0 invariant

`asset_class` is the **strict partition-of-unity invariant**: 100.0% of product-dates must sum to \(1.0 \pm 0.0001\).

Sub-allocations (`country`, `industry`, `credit_rating`, `debt_type`, `maturity`) are partial, sleeve-specific, or truncated distributions (top-country truncation, equity-only sectors omitting bond sleeves, cash/repo offsets). They are audited for bounds in Silver and renormalized or assigned residual factor columns (`Unidentified`, `Other`, `Not Rated`) in Stage 2 panel construction.

### 1.4 Physical long storage

Storage on disk is strictly **long**. Dynamic wide pivoting occurs in memory (DuckDB `PIVOT` or Polars/Pandas) for factor regression and portfolio optimization. `silver.monthly_panel` is a physical long table, not a wide matrix.

---

## 2. `ExtractionResult` Interface Contract

Every extractor (`extract_profile`, `extract_ratios`, `extract_holdings`, `extract_mstar`, `extract_lipper`, `extract_theme_weights`, `extract_esg`) returns a single `ExtractionResult`. Empty or error payloads return an empty result (zero rows), never `None`.

```python
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

EffectiveDateSource = Literal["payload", "item", "snapshot"]

# MetricRow aligns 1:1 with silver.product_metrics columns (post-currency migration).
MetricRow = tuple[
    int,                 # product_id
    str,                 # source            e.g. 'profile', 'ratios', 'holdings', 'mstar', 'lipper', 'esg', 'theme_weights'
    str,                 # metric_id         canonical lowercase identifier
    date,                # effective_date
    EffectiveDateSource, # effective_date_source
    datetime,            # fetched_at        snapshot fetch timestamp (tz-aware)
    float,               # value             operable numeric
    str,                 # raw_value         provider display string
    str | None,          # currency          ISO-4217 or NULL
]

# DimensionRow aligns 1:1 with silver.product_dimensions columns.
DimensionRow = tuple[
    int,                 # product_id
    str,                 # dimension_type    e.g. 'asset_class', 'country', 'industry', 'theme'
    str,                 # dimension_name    category / feature name
    str | None,          # dimension_code    ISO, slug, UUID, or NULL
    date,                # effective_date
    EffectiveDateSource, # effective_date_source
    datetime,            # fetched_at
    float,               # value             decimal weight (1.0 = 100%) or coordinate flag
    str,                 # raw_value
]

@dataclass
class ExtractionResult:
    metrics: list[MetricRow] = field(default_factory=list)
    dimensions: list[DimensionRow] = field(default_factory=list)
```

### 2.1 Extractor obligations

1. **Never emit `dimension_type = 'top_holding'`.** That loop is deleted; concentration is the scalar `portfolio_top_10_concentration`.
2. **Always emit `currency`.** ISO-4217 on `total_net_assets_local`; `NULL` on every unitless metric (ratios, ratings, percentages, scores, coverage).
3. **Retain unconstrained floats** in Silver (negatives, values \(> 1.0\), provider caps, split-corp extremes). Winsorization is a panel-layer concern.
4. **Skip, do not crash**, on empty payloads (`{}`, empty section lists) and error payloads (`{"type": "IDENTIFICATION_PROBLEM", ...}`).
5. **Raise `ValueError`** on unrecognized non-empty categorical tokens where a closed map is defined (`is_passive`, Morningstar rating strings). Skip listed pending tokens (`under_review`, `not_applicable`, `n/a`, `-`, `''`).

---

## 3. Target Database Schemas

### 3.1 `silver.product_metrics`

```sql
CREATE TABLE silver.product_metrics (
    product_id             INTEGER NOT NULL,                  -- FK → silver.products.product_id
    source                 VARCHAR NOT NULL,                  -- Ingestion endpoint ('ratios', 'profile', …)
    metric_id              VARCHAR NOT NULL,                  -- Canonical metric identifier
    effective_date         DATE NOT NULL,                     -- Point-in-time assessment date
    effective_date_source  VARCHAR NOT NULL,                  -- 'payload' | 'item' | 'snapshot'
    fetched_at             TIMESTAMP WITH TIME ZONE NOT NULL, -- Blob fetch timestamp
    value                  DOUBLE NOT NULL,                   -- Standardized numeric value for analytics
    raw_value              VARCHAR NOT NULL,                  -- Formatted display string from raw payload
    currency               VARCHAR,                           -- ISO-4217 ('USD', 'EUR', …) or NULL
    PRIMARY KEY (product_id, source, metric_id, effective_date)
);
```

### 3.2 `silver.product_dimensions`

```sql
CREATE TABLE silver.product_dimensions (
    product_id             INTEGER NOT NULL,                  -- FK → silver.products.product_id
    dimension_type         VARCHAR NOT NULL,                  -- 'asset_class', 'country', 'industry', …
    dimension_name         VARCHAR NOT NULL,                  -- Category or feature name
    dimension_code         VARCHAR,                           -- 2-letter ISO, style slug, theme UUID, or NULL
    effective_date         DATE NOT NULL,                     -- Point-in-time assessment date
    effective_date_source  VARCHAR NOT NULL,                  -- 'payload' | 'item' | 'snapshot'
    fetched_at             TIMESTAMP WITH TIME ZONE NOT NULL, -- Blob fetch timestamp
    value                  DOUBLE NOT NULL,                   -- Decimal weight (1.0 = 100%) or coordinate flag
    raw_value              VARCHAR NOT NULL,                  -- Formatted percentage or coordinate string
    PRIMARY KEY (product_id, dimension_type, dimension_name, effective_date)
);
```

`dimension_type = 'top_holding'` is dropped outright during extraction (eliminates ~263k sparse individual-security rows).

### 3.3 `silver.monthly_panel` (physical long table)

```sql
CREATE TABLE silver.monthly_panel (
    product_id   INTEGER NOT NULL,                  -- FK → silver.products.product_id
    as_of_date   DATE NOT NULL,                     -- Month-end calendar spine date
    asset_class  VARCHAR NOT NULL,                  -- Primary partition ('Equity', 'Fixed Income', …)
    feature_id   VARCHAR NOT NULL,                  -- Metric ID or flattened dimension feature
    value        DOUBLE NOT NULL,                   -- Cleaned, LOCF-carried factor value
    PRIMARY KEY (product_id, as_of_date, feature_id)
);
```

Panel construction is specified in [02_factor_panel.md](02_factor_panel.md). PK does **not** include `asset_class`; `feature_id` is unique per `(product_id, as_of_date)`.

---

## 4. Integrity Constraints

### 4.1 Primary-key uniqueness

| Table | Primary key | Implication |
| :--- | :--- | :--- |
| `silver.product_metrics` | `(product_id, source, metric_id, effective_date)` | Same metric on the same report date overwrites; distinct sources may coexist (`profile` TER vs `ratios` yield). |
| `silver.product_dimensions` | `(product_id, dimension_type, dimension_name, effective_date)` | One weight per named category per date. `dimension_code` is **not** in the PK (codes can be remapped without splitting identity). |
| `silver.monthly_panel` | `(product_id, as_of_date, feature_id)` | One value per feature per month-end. Style-box precedence and debt-type clustering must collapse before insert. |

Extractors that would emit duplicate PK tuples for a single snapshot must coalesce (last-write or explicit precedence) before return.

### 4.2 Temporal causality

\[
\text{effective\_date} \le \text{fetched\_at}::\text{DATE}
\]

A report cannot be dated after the blob that contains it was fetched. Violations are extractor bugs (mis-parsed epoch, swapped fields) and must fail validation, not be silently stored.

`fetched_at` is the Bronze snapshot timestamp (tz-aware). `effective_date` is a calendar date in UTC.

### 4.3 Closed `effective_date_source`

`effective_date_source ∈ {'payload', 'item', 'snapshot'}`. No other tokens.

### 4.4 Numeric non-null

Both `value` columns are `DOUBLE NOT NULL`. Items with `value is None` are skipped, not stored as null. Zero (`0.0`) is a valid observation (liquidated AUM, zero-fee waiver, lowest ESG decile) and **must** be retained.

### 4.5 Currency nullability

- `total_net_assets_local`: `currency` **NOT NULL** in practice (always an ISO-4217 code after disambiguation).
- All other metrics: `currency IS NULL` (dimensionless ratios, scores, percentages, coverage, ratings).

---

## 5. Source Token Map

| Extractor | `source` value | Produces metrics | Produces dimensions |
| :--- | :--- | :---: | :---: |
| `extract_profile` | `'profile'` | 7 scalars | `style_box`, `style_box_hist` |
| `extract_ratios` | `'ratios'` | 47 scalars | — |
| `extract_holdings` | `'holdings'` | `portfolio_top_10_concentration` | `asset_class`, `country`, `industry`, `credit_rating`, `debt_type`, `maturity` |
| `extract_mstar` | `'mstar'` | 8 evaluative (+ coverage) | — |
| `extract_lipper` | `'lipper'` | 20 canonical (5 tags × 4 horizons) | — |
| `extract_theme_weights` | `'theme_weights'` | `theme_coverage` | `theme` (491 children) |
| `extract_esg` | `'esg'` | 17 (`esg_coverage` + 16 TRESG scores) | — |

Style-box coordinates are ingested via `extract_profile` from the `profile` payload's `mstar` object, **not** via `extract_mstar`.
