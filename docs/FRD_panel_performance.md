# Functional Requirements Document (FRD): Monthly Panel Performance Optimization, Outlier Preprocessing & Theme Aggregation

**Status:** Approved for Implementation  
**Audience:** Implementer / Core Maintainers  
**Target Modules:** `etfportfolio/prep/panel.py`, `etfportfolio/prep/cli.py`, `tests/prep/test_panel.py`, `tests/prep/test_cli.py`  
**Target Hardware Baseline:** Apple Silicon (Mac Mini M2, 4P + 4E cores, 8 GB Unified Memory)

---

## 1. Executive Summary & Context

The ETF portfolio engine processes point-in-time ETF fundamental observations (`silver.observations`) and daily price trading spines (`bronze.prices`) to construct an aligned, point-in-time monthly factor panel (`silver.monthly_panel`). Downstream, this panel defines the long and short asset sets for cross-sectional factor-series return analytics (analogous to Fama–French HML/SMB factor construction).

During Stage 2 execution (`run_panel`), the original implementation caused CPU performance cores to sustain 100% saturation for tens of minutes or hours, with die temperatures climbing to 90°C–92°C, aggressive thermal fan engagement, and severe disk-swap thrashing.

This document specifies a complete architectural and algorithmic overhaul of `etfportfolio/prep/panel.py` and the CLI interface in `etfportfolio/prep/cli.py`. The overhaul:
1. **Eliminates quadratic and non-equi join bottlenecks** via precomputed validity intervals (`LEAD`) and universe bifurcation (**D-OPT-1 through D-OPT-4**).
2. **Introduces a distribution-aware rolling MAD/IQR time-series outlier preprocessor** on raw continuous scalar observations and child theme weights.
3. **Implements Architecture A (Post-Observation Rollup)** to aggregate 491 granular child themes into 19 macroeconomic parent themes within the canonical closed universe, while preserving raw observations in `silver.observations` for distribution auditing.
4. **Enforces structural invariants**: Fails immediately if an observation matches a root parent theme directly, and strictly drops unmapped child themes.
5. **Applies global metric low-count pruning** using a dynamic threshold ($\max(5, \text{median} - 3 \cdot \text{IQR})$) to discard degenerate, low-frequency metrics.
6. **Materializes `silver.monthly_panel` as a compact, columnar-compressed table** (~250 MB on disk) built in ~12–15 seconds.
7. **Updates the CLI surface** to allow running extraction and panel construction independently (`prep`, `prep obs`, `prep panel`).

---

## 2. Empirical Audit & Root Cause Analysis

Profiling DuckDB execution plans against the production dataset (18,131,078 clean observations across 20,435 products and 1,420,799 product-months) isolated four distinct algorithmic bottlenecks:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                 THE BOTTLENECK CHAIN                                   │
└────────────────────────────────────────────────────────────────────────────────────────┘
                                           │
  1. Correlated Subquery Nested Loop       ▼
     sp JOIN caps (4.65M rows) ──[JOIN LATERAL (DELIM_JOIN)]──> 4.65M table scans (CPU pegged)
                                           │
  2. Non-Equi Universe Join Predicate      ▼
     live JOIN universe ─────────[OR u.product_id IS NULL]────> 120B inner comparisons
                                           │
  3. Intermediate Materialization Blowout  ▼
     panel_default0 (94M) + panel_scalar (6M) + panel_final (100M) ──> >15 GB RAM (Swap thrashing)
                                           │
  4. The 85M-Row Theme Multiplier          ▼
     23.2k snapshots × ~305 themes × 2 families ───────────────> 85M rows of stored zeros
```

1. **The Correlated Subquery Nested Loop (`JOIN LATERAL`):**
   Evaluating `JOIN LATERAL (SELECT MAX(effective_date) ... <= as_of_date)` across 4.65M candidate pairs for default-0 families and ~42M candidate pairs for scalars compiled into `DELIM_JOIN` / correlated scans. Evaluating this subquery millions of times pegged all CPU cores.
2. **The Non-Equi Join in `panel_default0` (~120 Billion Comparisons):**
   The join predicate `ON u.family = live.family AND (u.product_id = live.product_id OR u.product_id IS NULL)` contains a disjunction (`OR`). Relational query optimizers cannot build an equi-join hash table on a composite key containing an `OR` condition. DuckDB hashed **strictly on `family`**. For `theme` and `rank_adj_theme` (3.37M product-theme pairs), every live snapshot (~250k rows) scanned that single 3.37M-row hash bucket:
   $$250{,}000 \times 3{,}366{,}571 \approx \mathbf{120{,}000{,}000{,}000\ (120\text{ billion})\text{ comparisons}}$$
3. **Unfiltered Outer Scan in `style_box` Merge:**
   Style-box deduplication executed self-joins against all 18.1M observations without filtering the outer scan to style-box families.
4. **Intermediate Materialization Memory Blowout (>15 GB RAM on a 6.3 GB Buffer):**
   Staging `panel_default0` (~94M rows), `panel_scalar` (~6M rows), and `panel_final` (~100M rows) required >15 GB RAM, far exceeding DuckDB’s 6.3 GB buffer ceiling and triggering continuous buffer spilling to disk and memory serialization churn.

---

## 3. Settled Decision Log & Architectural Rationale

| Decision ID | Decision | Considered Alternatives | Rationale |
|---|---|---|---|
| **DEC-THEME-1** | **Architecture A: Post-Observation Rollup in `panel.py`** | Architecture B (Pre-aggregation in `extractors.py`); No reduction (Pure SQL). | Preserves raw child observations in `silver.observations` for statistical distribution audits and anomaly inspection. Child weights are aggregated into 19 parent themes in `panel.py`, cutting panel rows by ~80M rows and slashing build time to ~15s without losing raw research fidelity. |
| **DEC-THEME-2** | **Canonical Closed Universe for 19 Parent Themes** | Product-level open universe. | Thematic exposures represent a fixed 19-dimensional basis vector for cross-sectional factor sorting. An ETF with 0% exposure to a theme must receive an explicit `0.0` (placing it in the bottom quantile), rather than a `NULL` which drops it from factor construction. Adds only ~4.7M rows across 10 years (~1.5s build time). |
| **DEC-THEME-3** | **Fail-Fast on Direct Parent Observations** | Ignore / Pass through. | The vendor endpoint (`theme_weights`) only provides child themes. If an observation metric matches a root parent theme (`parent_id IS NULL`), it indicates an unannounced upstream schema break that must be caught immediately rather than silently aggregated. |
| **DEC-THEME-4** | **Strict Drop for Unmapped Child Themes** | Synthetic `other_themes` category; Pass through as unmapped metric. | Ensures only valid, taxonomy-governed thematic factors enter the factor panel. Unmapped artifacts or deprecated vendor keys do not distort the canonical 19-parent factor basis. |
| **DEC-OUTLIER-1** | **Continuity & Skewness-Shifted Log Transformations** | Raw scale only; Parametric normality assumption. | Non-continuous / boolean metrics (`COUNT(DISTINCT) / COUNT(*) < 0.05`) are skipped. Continuous metrics with $|\text{skew}| > 3.0$ are shifted and log-transformed ($z = \ln(v - \min + 1)$ for right-skew, $z = \ln(\max - v + 1)$ for left-skew), making rolling quantile bounds symmetric on heavy-tailed metrics (e.g. AUM, P/E multiples). |
| **DEC-OUTLIER-2** | **Rolling MAD ($5 \cdot \text{IQR}$) with Zero-IQR Guardrail** | Standard deviation ($\sigma$) clipping; Fixed-width bands. | Evaluated over `ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING` (15-obs centered window) per `(product_id, family, metric)`. If $\text{IQR} == 0.0$, outlier trimming is skipped to prevent legitimate flat sequences (e.g. static expense ratios) from being trimmed. |
| **DEC-OUTLIER-3** | **Temp Table Staging with Python Summary Telemetry** | Passing tuple lists to Python; In-line SQL dropping without logging. | Outlier rows are flagged into `temp.outlier_observations` entirely in DuckDB C++ memory, avoiding IPC overhead. Python queries a small aggregated summary table to log diagnostic dropped counts and extreme values. |
| **DEC-PRUNE-1** | **Global Metric Low-Count Pruning with Safety Floor** | Per-family pruning; No floor ($\le 0$ cutoff). | Drops metrics whose total database observation count satisfies $C < \max(5, \text{median\_count} - 3 \cdot \text{IQR\_count})$. Guarantees that degenerate metrics with fewer than 5 observations are always dropped, while adapting to empirical density across the database. |
| **DEC-OPT-1** | **Precomputed Validity Intervals (`LEAD`)** | Correlated subquery (`JOIN LATERAL`). | Sparse snapshot dates (162k rows) are converted into non-overlapping `[valid_from, valid_to]` intervals via windowed `LEAD` in 0.04s. Range join onto `product_spine` completes in 0.02s (45,000x speedup). |
| **DEC-OPT-2** | **Bifurcate Canonical and Product Universe Joins** | Composite join with `(u.product_id = live.product_id OR u.product_id IS NULL)`. | Splits static canonical universes (82 metrics) from open product universes (`country`). Eliminates the disjunctive `OR` predicate, enabling strict single-key and composite hash equi-joins (120B comparisons reduced to 1.4s). |
| **DEC-OPT-3** | **Single-Pass Style Merge via `QUALIFY`** | Unfiltered self-join on 18.1M observations. | Window function `QUALIFY family = CASE WHEN bool_or(family = 'style_box') ...` completes the merge in 0.38s in a single table scan. |
| **DEC-OPT-4** | **Direct Streaming Appends to `silver.monthly_panel`** | Staging intermediate temp tables (`panel_default0`, `panel_scalar`, `panel_final`). | Eliminates 100M-row intermediate table allocations. Queries stream directly into `silver.monthly_panel`, keeping resident memory under 1.5 GB. |
| **DEC-OPT-5** | **Default OS & DuckDB Thread Governance** | Dynamic P-core detection (`sysctl`); Hardcoded `SET threads = 4`. | Because query optimizations drop build time to ~15s, multi-minute core pegging is eliminated. Native thread scheduling across all cores executes cleanly without thermal rise or fragile platform-specific code. |
| **DEC-STORAGE-1** | **Materialize `silver.monthly_panel` Table** | Dynamic SQL View. | Storing the table consumes only ~250 MB on disk and decouples asynchronous time-series preparation from factor analytics. Downstream Gold factor queries execute in 0.05s instead of paying a 15s re-evaluation penalty on every read. |
| **DEC-CLI-1** | **Three-Way CLI Dispatch (`prep`, `prep obs`, `prep panel`)** | Monolithic `prep` command only. | Enables independent execution of Stage 1 (observation extraction) and Stage 2 (panel construction) via Python Fire while preserving full backward compatibility. |

---

## 4. System Architecture & Execution Flow

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                             STAGE 2: `_build_panel` FLOW                                │
└────────────────────────────────────────────────────────────────────────────────────────┘
                                           │
  1. Trading Spine Construction            ▼
     Scan bronze.prices ─────────> CREATE TEMP TABLE product_spine (product_id, as_of_date)
                                           │
  2. Invariant Validation & Mapping        ▼
     Verify 0 direct parent obs ──> CREATE TEMP TABLE theme_mapping (child_code -> parent)
                                           │
  3. Outlier Preprocessing                 ▼
     Continuous scalars & themes ─> Skew-shift & ln() ─> Rolling MAD (5*IQR) ─> temp.outlier_obs
                                           │
  4. Theme Rollup & Clean Obs View         ▼
     Aggregate child obs to parent ─> Exclude outliers ─> CREATE TEMP TABLE clean_observations
                                           │
  5. Global Low-Count Metric Drop          ▼
     Filter clean_observations ──> COUNT(*) >= MAX(5, median - 3*IQR) ─> surviving_observations
                                           │
  6. Default-0 Sleeves (LEAD Intervals)    ▼
     Compute [valid_from, valid_to] ──> Bifurcated Equi-Joins ──> STREAM INSERT monthly_panel
     • Canonical Closed: 53 base metrics + 19 theme + 19 rank_adj_theme
     • Product Open: country
                                           │
  7. Scalar Sleeves & FX Conversion        ▼
     AUM USD conversion via FX ──> LEAD intervals ──────────────> STREAM INSERT monthly_panel
                                           │
                                           ▼
                             `silver.monthly_panel` Committed
```

---

## 5. Detailed Functional Specifications

### 5.1 Step 1: Trading Spine Construction

Determine per-product active trading bounds from `bronze.prices` for products present in `silver.observations`:
1. Query minimum and maximum dates per product from `bronze.prices`.
2. Generate inclusive month-end series via `month_end_spine(first_p, last_p)`:
   ```python
   conn.execute("CREATE TEMP TABLE product_spine (product_id INTEGER NOT NULL, as_of_date DATE NOT NULL)")
   conn.executemany("INSERT INTO product_spine VALUES (?, ?)", spine_rows)
   ```

---

### 5.2 Step 2: Invariant Validation & Theme Mapping

#### 5.2.1 Invariant: Direct Parent Theme Check
Inspect `silver.observations` for `family IN ('theme', 'rank_adj_theme')`.
Verify that no record's `metric` matches `to_lower_snake_case(name)` of any root parent theme (`parent_id IS NULL` in `bronze.themes`):
```sql
SELECT COUNT(*)
FROM silver.observations o
JOIN bronze.themes p
  ON p.parent_id IS NULL
 AND o.family IN ('theme', 'rank_adj_theme')
 AND o.metric = regexp_replace(lower(replace(trim(p.name), '&', 'and')), '[^a-z0-9]+', '_', 'g');
```
* **Failure Condition:** If `COUNT(*) > 0`, **raise `ValueError` immediately** with diagnostic details. Direct parent theme observations violate vendor contract invariants.

#### 5.2.2 Theme Mapping Table
Construct a temporary lookup mapping child themes to their parent theme:
```sql
CREATE TEMP TABLE theme_mapping AS
SELECT
    c.theme_id AS child_code,
    regexp_replace(lower(replace(trim(c.name), '&', 'and')), '[^a-z0-9]+', '_', 'g') AS child_metric,
    regexp_replace(lower(replace(trim(p.name), '&', 'and')), '[^a-z0-9]+', '_', 'g') AS parent_metric
FROM bronze.themes c
JOIN bronze.themes p ON c.parent_id = p.theme_id
WHERE c.parent_id IS NOT NULL;
```
* **Unmapped Child Rule:** Any child theme in `silver.observations` that cannot be joined to `theme_mapping` is strictly excluded from rollup.

---

### 5.3 Step 3: Distribution-Aware Time-Series Outlier Detection

Outlier detection executes on raw observations before child themes are collapsed.

#### 5.3.1 Target Families & Exclusions
* **Target Families:** `'ratios'`, `'profile'`, `'esg'`, `'theme'`, `'rank_adj_theme'`.
* **Explicit Exclusions:**
  - Discrete/indicator metrics: `style_box`, `style_box_hist`, `mstar`, `lipper`, `asset_class`, `industry`, `credit_rating`, `maturity`.
  - Binary/discrete scalars: `profile.is_passive`.

#### 5.3.2 Continuity / Categorical Encoding Test
For each candidate metric within the target families, compute:
```sql
CREATE TEMP TABLE candidate_metrics AS
SELECT
    family,
    metric,
    COUNT(*)::FLOAT AS n_total,
    COUNT(DISTINCT value)::FLOAT / COUNT(*)::FLOAT AS distinct_ratio,
    skewness(value) AS skew_val,
    MIN(value) AS min_val,
    MAX(value) AS max_val
FROM silver.observations
WHERE family IN ('ratios', 'profile', 'esg', 'theme', 'rank_adj_theme')
  AND metric NOT IN ('is_passive')
GROUP BY family, metric
HAVING COUNT(DISTINCT value) > 10
   AND (COUNT(DISTINCT value)::FLOAT / COUNT(*)::FLOAT) >= 0.05;
```
Metrics with `COUNT(DISTINCT value) <= 10` or `distinct_ratio < 0.05` are classified as discrete and skipped.

#### 5.3.3 Skewness-Shifted Log Transformation
For continuous metrics in `candidate_metrics`, compute transformed value $z$:
* **Heavy Right Skew ($\text{skew\_val} > 3.0$):**
  $$z = \ln(\text{value} - \text{min\_val} + 1.0)$$
* **Heavy Left Skew ($\text{skew\_val} < -3.0$):**
  $$z = \ln(\text{max\_val} - \text{value} + 1.0)$$
* **Moderate Skew ($|\text{skew\_val}| \le 3.0$):**
  $$z = \text{value}$$

#### 5.3.4 Rolling MAD / IQR Filter
Compute rolling median and IQR of $z$ across a 15-observation centered window partitioned by product and metric:
```sql
CREATE TEMP TABLE outlier_observations AS
WITH transformed AS (
    SELECT
        s.product_id,
        s.family,
        s.metric,
        s.effective_date,
        s.value,
        s.raw_value,
        CASE
            WHEN m.skew_val > 3.0 THEN ln(s.value - m.min_val + 1.0)
            WHEN m.skew_val < -3.0 THEN ln(m.max_val - s.value + 1.0)
            ELSE s.value
        END AS z
    FROM silver.observations s
    JOIN candidate_metrics m
      ON s.family = m.family AND s.metric = m.metric
),
stats AS (
    SELECT
        product_id,
        family,
        metric,
        effective_date,
        value,
        raw_value,
        z,
        quantile_cont(z, 0.5) OVER w AS med_z,
        quantile_cont(z, 0.25) OVER w AS q25_z,
        quantile_cont(z, 0.75) OVER w AS q75_z
    FROM transformed
    WINDOW w AS (
        PARTITION BY product_id, family, metric
        ORDER BY effective_date
        ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING
    )
)
SELECT
    product_id,
    family,
    metric,
    effective_date,
    value,
    raw_value
FROM stats
WHERE (q75_z - q25_z) > 0.0
  AND abs(z - med_z) > 5.0 * (q75_z - q25_z);
```
* **Zero-IQR Rule:** If $q_{75} - q_{25} == 0.0$, the row is **not** an outlier (skipped).

#### 5.3.5 Python Telemetry
Python queries summary statistics from `outlier_observations` and logs them:
```python
outlier_summary = conn.execute(
    """
    SELECT family, metric, COUNT(*) AS n_dropped, MIN(value) AS min_val, MAX(value) AS max_val
    FROM outlier_observations
    GROUP BY family, metric
    ORDER BY n_dropped DESC
    """
).fetchall()

n_total_outliers = sum(r[2] for r in outlier_summary)
if n_total_outliers > 0:
    logger.info("Outlier preprocessor flagged %d observations across %d metrics", n_total_outliers, len(outlier_summary))
    for fam, met, cnt, mn, mx in outlier_summary[:10]:
        logger.debug("Outlier drop: %s.%s (%d rows, range [%s, %s])", fam, met, cnt, mn, mx)
```

---

### 5.4 Step 4: Theme Aggregation & Clean Observations Assembly

Assemble `clean_observations`, excluding outliers and rolling up child themes to parent themes:
```sql
CREATE TEMP TABLE clean_observations AS
-- 1. Non-theme observations (excluding outliers)
SELECT
    s.product_id,
    s.family,
    s.metric,
    s.code,
    s.effective_date,
    s.date_source_depth,
    s.fetched_at,
    s.value
FROM silver.observations s
LEFT JOIN outlier_observations outl
  ON s.product_id = outl.product_id
 AND s.family = outl.family
 AND s.metric = outl.metric
 AND s.effective_date = outl.effective_date
WHERE s.family NOT IN ('theme', 'rank_adj_theme')
  AND outl.product_id IS NULL

UNION ALL

-- 2. Theme observations aggregated to parent themes (excluding outliers and unmapped child themes)
SELECT
    s.product_id,
    s.family,
    m.parent_metric AS metric,
    NULL AS code,
    s.effective_date,
    MAX(s.date_source_depth) AS date_source_depth,
    MAX(s.fetched_at) AS fetched_at,
    SUM(s.value) AS value
FROM silver.observations s
JOIN theme_mapping m
  ON (s.code IS NOT NULL AND s.code = m.child_code)
  OR s.metric = m.child_metric
LEFT JOIN outlier_observations outl
  ON s.product_id = outl.product_id
 AND s.family = outl.family
 AND s.metric = outl.metric
 AND s.effective_date = outl.effective_date
WHERE s.family IN ('theme', 'rank_adj_theme')
  AND outl.product_id IS NULL
GROUP BY s.product_id, s.family, m.parent_metric, s.effective_date;
```

---

### 5.5 Step 5: Global Metric Low-Count Pruning

Filter `clean_observations` to discard metrics with degenerate global counts:
1. Count observations per distinct `(family, metric)` across `clean_observations`.
2. Compute global quantiles across all metrics and apply the safety floor:
   $$\text{cutoff} = \max(5, \text{median\_count} - 3 \cdot (\text{q75\_count} - \text{q25\_count}))$$
3. Retain surviving rows:
```sql
CREATE TEMP TABLE metric_counts AS
SELECT family, metric, COUNT(*) AS obs_count
FROM clean_observations
GROUP BY family, metric;

CREATE TEMP TABLE global_cutoff AS
SELECT
    GREATEST(5, (
        quantile_cont(obs_count, 0.5) - 3.0 * (quantile_cont(obs_count, 0.75) - quantile_cont(obs_count, 0.25))
    ))::INTEGER AS min_threshold
FROM metric_counts;

CREATE TEMP TABLE surviving_observations AS
SELECT o.*
FROM clean_observations o
JOIN metric_counts mc
  ON o.family = mc.family AND o.metric = mc.metric
JOIN global_cutoff gc
  ON mc.obs_count >= gc.min_threshold;
```
Python logs the computed threshold and any pruned metrics.

---

### 5.6 Step 6: Default-0 Densification via Validity Intervals & Bifurcation

#### 5.6.1 Style Merge (`QUALIFY`, D-OPT-3)
Extract default-0 observations with single-pass style box deduplication:
```sql
CREATE TEMP TABLE default0_obs AS
SELECT
    product_id,
    family,
    metric,
    effective_date,
    date_source_depth,
    fetched_at,
    value
FROM surviving_observations
WHERE family IN (
    'asset_class', 'country', 'industry', 'credit_rating',
    'maturity', 'theme', 'rank_adj_theme'
)

UNION ALL

SELECT
    product_id,
    'style_box' AS family,
    metric,
    effective_date,
    date_source_depth,
    fetched_at,
    value
FROM surviving_observations
WHERE family IN ('style_box', 'style_box_hist')
QUALIFY family = CASE
    WHEN bool_or(family = 'style_box') OVER (PARTITION BY product_id, effective_date)
    THEN 'style_box'
    ELSE 'style_box_hist'
END;
```

#### 5.6.2 Snapshot Caps & Validity Intervals (`LEAD`, D-OPT-1)
1. Determine snapshot dates and gaps:
```sql
CREATE TEMP TABLE default0_snaps AS
SELECT DISTINCT product_id, family, effective_date
FROM default0_obs;

CREATE TEMP TABLE default0_gaps AS
SELECT
    product_id,
    family,
    effective_date,
    date_diff('day',
        LAG(effective_date) OVER (PARTITION BY product_id, family ORDER BY effective_date),
        effective_date
    ) AS gap_days
FROM default0_snaps;

CREATE TEMP TABLE default0_caps AS
SELECT
    product_id,
    family,
    COUNT(gap_days) AS n_gaps,
    quantile_cont(gap_days, 0.99)::INTEGER AS cap,
    MIN(effective_date) AS min_snap
FROM default0_gaps
WHERE gap_days IS NULL OR gap_days > 0
GROUP BY product_id, family;
```
2. Build `default0_live` via non-overlapping intervals:
```sql
CREATE TEMP TABLE default0_live AS
-- Singleton case: valid across the product's entire price spine
SELECT
    sp.product_id,
    sp.as_of_date,
    c.family,
    c.min_snap AS snap
FROM product_spine sp
JOIN default0_caps c
  ON c.product_id = sp.product_id
 AND c.n_gaps = 0

UNION ALL

-- Multi-obs case: precomputed validity intervals
SELECT
    sp.product_id,
    sp.as_of_date,
    inv.family,
    inv.snap
FROM product_spine sp
JOIN (
    SELECT
        s.product_id,
        s.family,
        s.effective_date AS snap,
        s.effective_date AS valid_from,
        CASE
            WHEN LEAD(s.effective_date) OVER w IS NOT NULL
            THEN LEAST(s.effective_date + c.cap, LEAD(s.effective_date) OVER w - 1)
            ELSE s.effective_date + c.cap
        END AS valid_to
    FROM default0_snaps s
    JOIN default0_caps c
      ON c.product_id = s.product_id
     AND c.family = s.family
     AND c.n_gaps >= 1
    WINDOW w AS (PARTITION BY s.product_id, s.family ORDER BY s.effective_date)
) inv
  ON sp.product_id = inv.product_id
 AND sp.as_of_date >= inv.valid_from
 AND sp.as_of_date <= inv.valid_to;
```

#### 5.6.3 Bifurcated Universe Registration (D-OPT-2)
1. **`canonical_universe` (Static, Closed):**
   - 3 asset classes (excluding `other`).
   - 12 industries (excluding non-classified residuals).
   - 10 credit ratings (excluding unrated/unavailable).
   - 7 maturities (excluding other).
   - 12 style box cells (`large_value` through `small_growth`).
   - **19 parent themes** for `family = 'theme'`.
   - **19 parent themes** for `family = 'rank_adj_theme'`.
   - Total = **82 canonical metrics**.
   Populate via `CREATE TEMP TABLE canonical_universe (family VARCHAR, metric VARCHAR)`.
2. **`product_universe` (Open):**
   - Populated exclusively for `country`:
   ```sql
   CREATE TEMP TABLE product_universe AS
   SELECT DISTINCT family, metric, product_id
   FROM surviving_observations
   WHERE family = 'country';
   ```

#### 5.6.4 Direct Streaming Inserts into `silver.monthly_panel` (D-OPT-4)
Execute streaming appends directly into `silver.monthly_panel`:
```sql
-- 1. Canonical closed universes (strict join on family)
INSERT INTO silver.monthly_panel (product_id, as_of_date, feature_id, value)
SELECT
    live.product_id,
    live.as_of_date,
    live.family || '_' || u.metric AS feature_id,
    COALESCE(o.value, 0.0) AS value
FROM default0_live live
JOIN canonical_universe u
  ON u.family = live.family
LEFT JOIN default0_obs o
  ON o.product_id = live.product_id
 AND o.family = live.family
 AND o.metric = u.metric
 AND o.effective_date = live.snap;

-- 2. Open per-product universes (strict equi-join on product_id AND family)
INSERT INTO silver.monthly_panel (product_id, as_of_date, feature_id, value)
SELECT
    live.product_id,
    live.as_of_date,
    live.family || '_' || u.metric AS feature_id,
    COALESCE(o.value, 0.0) AS value
FROM default0_live live
JOIN product_universe u
  ON u.product_id = live.product_id
 AND u.family = live.family
LEFT JOIN default0_obs o
  ON o.product_id = live.product_id
 AND o.family = live.family
 AND o.metric = u.metric
 AND o.effective_date = live.snap;
```

---

### 5.7 Step 7: Scalar Interpolation & USD AUM Conversion

1. Monthly FX aggregation and USD AUM conversion:
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
FROM surviving_observations o
LEFT JOIN monthly_fx fx
  ON fx.currency = o.code
 AND fx.month_end = LAST_DAY(o.effective_date)
WHERE o.family = 'profile'
  AND o.metric = 'total_net_assets_local'
  AND (o.code = 'USD' OR fx.rate_to_usd IS NOT NULL)
GROUP BY o.product_id, LAST_DAY(o.effective_date);
```

2. Assemble scalar observations, distinct per `(product_id, feature_id, effective_date)`:
```sql
CREATE TEMP TABLE scalar_obs AS
SELECT
    product_id,
    family,
    metric,
    family || '_' || metric AS feature_id,
    effective_date,
    fetched_at,
    value
FROM surviving_observations
WHERE family IN ('ratios', 'lipper', 'esg', 'mstar', 'profile')
  AND NOT (family = 'profile' AND metric = 'total_net_assets_local')

UNION ALL

SELECT
    product_id,
    'profile' AS family,
    'total_net_assets_usd' AS metric,
    'profile_total_net_assets_usd' AS feature_id,
    effective_month AS effective_date,
    effective_month::TIMESTAMP WITH TIME ZONE AS fetched_at,
    value
FROM aum_usd_monthly;

CREATE TEMP TABLE scalar_obs_distinct AS
SELECT
    product_id,
    family,
    metric,
    feature_id,
    effective_date,
    fetched_at,
    value
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id, feature_id, effective_date
            ORDER BY fetched_at DESC
        ) AS rn
    FROM scalar_obs
)
WHERE rn = 1;
```

3. Calculate scalar caps:
```sql
CREATE TEMP TABLE scalar_gaps AS
SELECT
    product_id,
    feature_id,
    effective_date,
    date_diff('day',
        LAG(effective_date) OVER (PARTITION BY product_id, feature_id ORDER BY effective_date),
        effective_date
    ) AS gap_days
FROM scalar_obs_distinct;

CREATE TEMP TABLE scalar_caps AS
SELECT
    product_id,
    feature_id,
    COUNT(gap_days) AS n_gaps,
    quantile_cont(gap_days, 0.99)::INTEGER AS cap,
    MIN(effective_date) AS min_date
FROM scalar_gaps
WHERE gap_days IS NULL OR gap_days > 0
GROUP BY product_id, feature_id;
```

4. Stream scalar observations directly into `silver.monthly_panel` via precomputed `LEAD` intervals:
```sql
INSERT INTO silver.monthly_panel (product_id, as_of_date, feature_id, value)
-- Singleton scalars: full spine coverage
SELECT
    sp.product_id,
    sp.as_of_date,
    sc.feature_id,
    so.value
FROM product_spine sp
JOIN scalar_caps sc
  ON sc.product_id = sp.product_id
 AND sc.n_gaps = 0
JOIN scalar_obs_distinct so
  ON so.product_id = sc.product_id
 AND so.feature_id = sc.feature_id

UNION ALL

-- Multi-obs scalars: valid within [valid_from, valid_to]
SELECT
    sp.product_id,
    sp.as_of_date,
    inv.feature_id,
    inv.value
FROM product_spine sp
JOIN (
    SELECT
        so.product_id,
        so.feature_id,
        so.value,
        so.effective_date AS valid_from,
        CASE
            WHEN LEAD(so.effective_date) OVER w IS NOT NULL
            THEN LEAST(so.effective_date + sc.cap, LEAD(so.effective_date) OVER w - 1)
            ELSE so.effective_date + sc.cap
        END AS valid_to
    FROM scalar_obs_distinct so
    JOIN scalar_caps sc
      ON sc.product_id = so.product_id
     AND sc.feature_id = so.feature_id
     AND sc.n_gaps >= 1
    WINDOW w AS (PARTITION BY so.product_id, so.feature_id ORDER BY so.effective_date)
) inv
  ON sp.product_id = inv.product_id
 AND sp.as_of_date >= inv.valid_from
 AND sp.as_of_date <= inv.valid_to;
```

---

### 5.8 CLI Dispatch Interface

Update `ObservationsCLI` in `etfportfolio/prep/cli.py`:
```python
from __future__ import annotations

from etfportfolio.core.logging import console
from etfportfolio.prep.panel import run_panel
from etfportfolio.prep.pipeline import run_observations


class ObservationsCLI:
    """CLI surface for preprocessing: `main.py prep`."""

    def __call__(self, force: bool = False) -> None:
        """Runs full prep pipeline: observations extraction then monthly panel."""
        self.obs(force=force)
        self.panel()

    def obs(self, force: bool = False) -> int:
        """Runs Silver observations extraction only."""
        console.info("=== Starting Silver Observations Extraction ===")
        processed_count = run_observations(force=force)
        console.info(f"=== Observations Complete. Processed {processed_count} snapshots. ===")
        return processed_count

    def panel(self) -> int:
        """Rebuilds silver.monthly_panel from current Silver observations."""
        console.info("=== Starting Factor Panel Construction ===")
        n_rows = run_panel()
        console.info(f"=== Panel Complete. Wrote {n_rows} rows. ===")
        return n_rows


cli = ObservationsCLI()
```
* Supported commands via Fire:
  - `uv run main.py prep [--force]`
  - `uv run main.py prep obs [--force]`
  - `uv run main.py prep panel`

---

## 6. Testing, Verification & Migration Guide

### 6.1 Existing Test Suite Compatibility & Updates

In `tests/prep/test_panel.py`:
1. **`test_open_vocab_densify_per_product_only`**:
   The existing test used dummy themes (`theme_a`, `theme_b`, `theme_c`). Under Architecture A, themes are collapsed to parent themes and unmapped themes are dropped, while `country` is now the open per-product universe. Update this test to use `country` (e.g. `country_us` vs `country_ca`) to verify per-product open universe densification without unmapped theme interference.
2. **`bronze.themes` Population in Fixtures**:
   Tests that verify theme processing must ensure `bronze.themes` contains root parents (`parent_id IS NULL`) and children (`parent_id IS NOT NULL`).

### 6.2 New Test Specifications in `tests/prep/test_panel.py`

1. **`test_direct_parent_theme_observation_raises_error`**:
   Insert a root parent theme in `bronze.themes` (e.g. `theme_id='P1', name='Artificial Intelligence', parent_id=NULL`) and an observation with `family='theme', metric='artificial_intelligence'`. Assert that `run_panel()` raises `ValueError`.
2. **`test_unmapped_child_theme_strictly_dropped`**:
   Insert an observation for a theme that does not exist in `bronze.themes`. Verify it produces zero rows in `silver.monthly_panel`.
3. **`test_child_themes_aggregated_to_parent_theme`**:
   Create parent `P1` ("Technology") and two child themes `C1` ("Hardware", weight 0.3) and `C2` ("Software", weight 0.5) for the same product and date. Verify `silver.monthly_panel` contains `theme_technology` with value `0.8`.
4. **`test_all_19_parent_themes_densified_in_canonical_universe`**:
   For an ETF with a single theme reported, verify that all 19 parent themes are emitted in `silver.monthly_panel` (unreported parent themes evaluate to `0.0`).
5. **`test_rolling_mad_outlier_detection_and_iqr_zero_guardrail`**:
   - Provide a 15-point time series where 14 points are ~20.0 and 1 point is 1500.0 (assert 1500.0 is trimmed).
   - Provide a 15-point flat series where all points are 0.0020 (IQR == 0.0, assert no points are dropped).
6. **`test_metric_low_count_pruning`**:
   Insert a metric with only 3 observations total across all products. Assert it is pruned by the global cutoff ($\max(5, \dots)$).

### 6.3 Test Specifications in `tests/prep/test_cli.py`

1. **`test_cli_subcommands`**:
   - Verify `cli.obs(force=False)` invokes `run_observations(force=False)` only.
   - Verify `cli.panel()` invokes `run_panel()` only.
   - Verify `cli(force=True)` invokes both sequentially with `force=True`.

### 6.4 Acceptance Benchmarks (Mac Mini M2, 8 GB RAM)

| Metric | Target | Original Implementation |
|---|---|---|
| **Monthly Panel Runtime** | $\le$ 30 seconds (expected ~12–15s) | Hangs / > 1 hour |
| **Peak CPU Die Temperature** | $\le$ 65°C | 90°C–92°C (Thermal throttling) |
| **Peak Resident RAM** | $\le$ 2.0 GB | > 15 GB (Swap thrashing) |
| **Storage Footprint (`monthly_panel`)** | $\le$ 300 MB | ~12–16 GB uncompressed |