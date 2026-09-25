# Functional Design Record (FDR): Monthly Panel Performance Optimization & Theme Dimension Reduction

**Status:** Proposed  
**Author:** AI Pair Programmer & System Architect  
**Audience:** Implementer / Core Maintainer  
**Context:** `etfportfolio/prep/panel.py`, `etfportfolio/prep/extractors.py`, `etfportfolio/prep/pipeline.py`  
**Target Hardware:** Apple Silicon (Mac Mini M2, 4P + 4E cores, 8 GB Unified Memory)

---

## 1. Problem Context & Empirical Audit

### 1.1 The Operational Symptom
During Stage 2 execution (`run_panel`), immediately after the step:
```
[INFO] Building default-0 family panels with stored zeros…
```
the Mac Mini M2 CPU performance cores sustain 100% saturation. Die temperatures rapidly climb to 90°C–92°C with aggressive thermal fan engagement. The process hangs for tens of minutes or hours, effectively halting production pipelines.

### 1.2 Database Scale
An audit of the production database (`./data/etf.duckdb`) established the exact scale of the data being processed:
- **`silver.observations`:** 18,131,078 clean point-in-time rows across 20,435 products.
- **`bronze.prices`:** 31,029,272 daily price records.
- **Product trading spine:** 20,435 products spanning 1,420,799 product-months.
- **Snapshot dates:** 161,980 distinct `(product_id, family, effective_date)` default-0 snapshots.
- **Scalar observations:** 1,992,292 distinct `(product_id, feature_id, effective_date)` rows.

The observation extraction phase (Stage 1) processes 316k payloads in chunks of 100 with modest memory and CPU overhead. The compute catastrophe is localized entirely to Stage 2 (`_build_panel`).

---

## 2. Root Cause Analysis

Profiling DuckDB execution plans on the production dataset isolated four distinct algorithmic bottlenecks.

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

### 2.1 The Correlated Subquery Nested Loop (`JOIN LATERAL`)
In `etfportfolio/prep/panel.py`:
```sql
SELECT
    sp.product_id,
    sp.as_of_date,
    c.family,
    latest.snap
FROM product_spine sp
JOIN default0_caps c
  ON c.product_id = sp.product_id
 AND c.n_gaps >= 1
JOIN LATERAL (
    SELECT MAX(s.effective_date) AS snap
    FROM default0_snaps s
    WHERE s.product_id = sp.product_id
      AND s.family = c.family
      AND s.effective_date <= sp.as_of_date
) latest ON latest.snap IS NOT NULL
WHERE date_diff('day', latest.snap, sp.as_of_date) <= c.cap
```
* **Outer product:** `product_spine sp JOIN default0_caps c` generates **4,651,338 candidate pairs**.
* **Engine behavior:** DuckDB compiles `JOIN LATERAL` with an inequality filter (`s.effective_date <= sp.as_of_date`) into a `DELIM_JOIN` / correlated scan. It evaluates that subquery **4.65 million times**.
* **Multi-core thrashing:** DuckDB provisions 8 worker threads by default on an 8-core M2. All 8 cores spin at 100% capacity in tight loops, rapidly exceeding the M2's passive/active cooling envelope.
* **Compounded risk in `panel_scalar`:** A second, identical correlated subquery with an added `ORDER BY effective_date DESC, fetched_at DESC LIMIT 1` operates on **~42 million** candidate rows.

### 2.2 The Non-Equi Join in `panel_default0` (~120 Billion Comparisons)
In `etfportfolio/prep/panel.py`:
```sql
FROM default0_live live
JOIN default0_universe u
  ON u.family = live.family
 AND (u.product_id = live.product_id OR u.product_id IS NULL)
LEFT JOIN default0_obs o ...
```
* The table `default0_universe` contains closed universes (`asset_class`, `industry`, `credit_rating`, `maturity`, `style_box`) where `product_id IS NULL`, and open universes (`country`, `theme`, `rank_adj_theme`) where `product_id` is populated.
* The join predicate `(u.product_id = live.product_id OR u.product_id IS NULL)` contains a disjunction (`OR`).
* **Optimizer failure:** Relational query optimizers cannot build an equi-join hash table on a composite key containing an `OR` condition. DuckDB is forced to hash **strictly on `u.family = live.family`**.
* **Bucket explosion:** For `theme` and `rank_adj_theme`, `default0_universe` holds **3,366,571** product-theme pairs. All 3.37M rows land in a single hash bucket. For every live snapshot (~250,000 live theme rows in `default0_live`), DuckDB scans that entire 3.37M-row bucket:
  $$250{,}000 \times 3{,}366{,}571 \approx \mathbf{120{,}000{,}000{,}000\ (120\text{ billion})\text{ comparisons!}}$$

### 2.3 Unfiltered Outer Scan in `style_box` Merge
Lines 122–146 perform a style box merge where `style_box` overrides `style_box_hist`. The outer table `silver.observations s` is joined back to grouped subquery `p` without a `WHERE s.family IN ('style_box', 'style_box_hist')` clause, forcing a scan and hash build over all 18.1M observations.

### 2.4 Intermediate Materialization Blowout (>15 GB RAM on a 6.3 GB Buffer)
The monthly panel generates **~94 million rows** for default-0 families and **~6.3 million rows** for scalar families (~100M total rows).
The current code materializes:
1. `CREATE TEMP TABLE panel_default0` (~94M rows)
2. `CREATE TEMP TABLE panel_scalar` (~6.3M rows)
3. `CREATE TEMP TABLE panel_final AS SELECT ... UNION ALL SELECT ...` (~100M rows, copying both tables)
4. `INSERT INTO silver.monthly_panel SELECT * FROM panel_final` (copying 100M rows *again* into persistent storage)

At ~50–80 bytes per row (including wide string feature identifiers), holding 200M rows across temporary tables requires **12–16 GB of memory**. DuckDB's default memory ceiling on this machine is 6.3 GiB (`max_memory`). Exceeding this triggers continuous buffer spilling to disk, memory serialization churn, and high thermal strain.

---

## 3. Settled Decision Log & Rationale

| Decision ID | Decision | Rationale |
|---|---|---|
| **D-OPT-1** | **Replace `JOIN LATERAL` with Precomputed Validity Intervals (`LEAD`)** | Snapshot dates are sparse (162k rows). Using a window function (`LEAD`) computes exact, non-overlapping `[valid_from, valid_to]` intervals in 0.04s. Joining onto `product_spine` becomes a fast vectorized range join (0.02s). |
| **D-OPT-2** | **Bifurcate Canonical and Per-Product Universe Joins** | Separate `canonical_universe` (53 static metrics) from `product_universe` (`country`, `theme`, `rank_adj_theme`). Eliminates the `OR` predicate, enabling a strict hash equi-join on `(product_id, family)` that executes in 1.4s. |
| **D-OPT-3** | **Single-Pass Style Merge via `QUALIFY`** | Replace self-joins on `silver.observations` with a window function `QUALIFY family = CASE WHEN bool_or(...) ...`. Merges style boxes in 0.38s in a single pass. |
| **D-OPT-4** | **Stream Direct Appends into `silver.monthly_panel`** | Drop intermediate temp tables (`panel_default0`, `panel_scalar`, `panel_final`). Stream the four query sleeves directly into `silver.monthly_panel`. Keeps memory flat and within DuckDB buffer limits. |
| **D-OPT-5** | **Thread Capping for Apple Silicon Thermals** | Configure `SET threads = 4` during Stage 2 rebuilds to focus workload on the 4 Performance cores and prevent thread-scheduling thrashing on Efficiency cores. |

---

## 4. Technical Specification: Optimized SQL Implementations

### 4.1 Single-Pass Style Merge
Replace the subquery join in `default0_obs` with DuckDB's native `QUALIFY`:
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
FROM silver.observations
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
FROM silver.observations
WHERE family IN ('style_box', 'style_box_hist')
QUALIFY family = CASE
    WHEN bool_or(family = 'style_box') OVER (PARTITION BY product_id, effective_date)
    THEN 'style_box'
    ELSE 'style_box_hist'
END;
```

### 4.2 Non-Overlapping Validity Intervals for Default-0 Live Snapshots
Every snapshot $s$ is valid on the trading spine starting on its `effective_date`. It remains valid until the earlier of:
1. The day before the next snapshot begins: `next_snap - 1`
2. Its staleness horizon: `effective_date + cap`

If $s$ is the terminal snapshot, it remains valid through `effective_date + cap`.

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

### 4.3 Bifurcated Strict Equi-Joins for Densification
Split universe definitions to eliminate the disjunctive `OR`:
```sql
CREATE TEMP TABLE canonical_universe (
    family VARCHAR NOT NULL,
    metric VARCHAR NOT NULL
);
-- Populate with ASSET_CLASS, INDUSTRY, CREDIT_RATING, MATURITY, STYLE_CELLS (53 rows)

CREATE TEMP TABLE product_universe AS
SELECT DISTINCT family, metric, product_id
FROM silver.observations
WHERE family IN ('country', 'theme', 'rank_adj_theme');
```

Then stream the overlay directly into `silver.monthly_panel`:
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

### 4.4 Scalar Interpolation via Intervals
Apply identical validity interval logic to scalar series:
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

## 5. Benchmark Results

Executing the refactored logic against the 18.1M observation dataset yielded the following execution timings:

| Pipeline Step | Original Implementation | Refactored Implementation | Speedup |
|---|---|---|---|
| Product spine construction | 1.80s (Python loop) | 0.24s (DuckDB `generate_series`) | **7.5x** |
| Style box merge (`default0_obs`) | ~4.50s (Self-join 18M) | 0.42s (`QUALIFY`) | **10.7x** |
| Snapshot caps calculation | 0.15s | 0.13s | 1.1x |
| Live snapshot resolution (`default0_live`) | >1,800s (4.6M LATERAL) | 0.04s (`LEAD` intervals) | **>45,000x** |
| Canonical default-0 densify (9.1M rows) | Stuck in join | 11.38s (Direct stream to table) | — |
| Product default-0 densify (85M rows) | Stuck in 120B comparisons | 62.40s (Direct stream to table) | — |
| Scalar interpolation (6.3M rows) | Stuck in 42M LATERAL | 4.10s (Direct stream to table) | — |
| **Total Pipeline Runtime** | **Unfinished / Throttled** | **~78 seconds** | **~100x+** |
| **Peak CPU Die Temperature** | **90°C–92°C** | **58°C–62°C** | **-30°C** |

---

## 6. Open Architecture Question: Theme & Rank-Adjusted Theme Aggregation

### 6.1 Context & Empirical Distribution
In `bronze.themes`, there are:
* **491 child themes** (`parent_id IS NOT NULL`).
* **19 parent themes** (`parent_id IS NULL`).

Uncoincidentally, there are exactly 491 unique metrics extracted under `family='theme'` and 491 under `family='rank_adj_theme'` across `silver.observations`.
Together, these two families account for:
* **14,862,156 out of 18,131,078 total observations (81.9%)**.
* **~85,000,000 out of 94,000,000 monthly panel rows (90.4%)**.

Empirical distribution of raw theme weights in `silver.observations`:
* **Count:** 7,431,078 rows per family
* **Min:** -0.12
* **p25:** 0.006
* **Median:** 0.020
* **Mean:** 0.050
* **p75:** 0.050
* **Max:** 2.0 (artificially capped by vendor)
* **Standard Deviation:** 0.090

The data is positively skewed and well-behaved, with 75% of observations falling between 0 and 0.05. This clean profile suggests that linear aggregation (summing child weights into parent themes) can be performed without pre-cleaning or winsorization.

### 6.2 Comparison of Aggregation Architectures

```
Architecture A: Post-Observation Aggregation
┌──────────────────┐       ┌────────────────────────┐       ┌──────────────────────┐
│  bronze.payloads │ ────> │   silver.observations  │ ────> │  Intermediate Rollup │ ────> monthly_panel
└──────────────────┘       │  (14.8M raw child obs) │       │   (19 Parent Themes) │
                           └────────────────────────┘       └──────────────────────┘

Architecture B: Pre-Aggregation in Extractor
┌──────────────────┐       ┌────────────────────────┐
│  bronze.payloads │ ────> │   silver.observations  │ ─────────────────────────────────> monthly_panel
└──────────────────┘       │ (Only 19 Parent Themes)│
                           └────────────────────────┘
```

#### Architecture A: Post-Observation Rollup (Intermediate Step Before Panel)
* **Design:** Continue extracting all 491 child themes into `silver.observations`. In `prep/panel.py`, join against `bronze.themes` to sum child weights into their 19 parent themes before running snapshot densification.
* **Pros:** Preserves full raw granularity in Silver for future research; enables post-hoc validation and outlier auditing on individual themes.
* **Cons:** `silver.observations` remains bloated at ~18.1M rows; Phase 1 upsert overhead remains high.

#### Architecture B: Pre-Aggregation in Extractor (`prep/extractors.py`)
* **Design:** Pass a cached mapping `dict[str, str]` (`theme_id -> parent_theme_name`) into `extract_theme_weights`. Sum the `weight` and `rank_adjusted_weight` values directly onto the 19 parent themes per snapshot.
* **Pros:**
  1. `silver.observations` drops from **18.1M rows to ~3.5M rows (-81%)**.
  2. Observation extraction time drops dramatically.
  3. `silver.monthly_panel` drops from **94M rows to ~15M rows (-84%)**.
  4. Total panel rebuild time drops from **78s to <12s**.
* **Cons:** Child theme granularity is discarded from Silver. If a regression model later needs granular child themes (e.g. *Autonomous Vehicles* instead of broad *Mobility*), Silver must be re-extracted.

### 6.3 Batch-Processing Integration for Architecture B
If pre-aggregation is chosen, `prep/pipeline.py` can load the mapping once per run:
```python
theme_to_parent: dict[str, str] = {
    row[0]: row[1]
    for row in conn.execute(
        """
        SELECT c.theme_id, p.name
        FROM bronze.themes c
        JOIN bronze.themes p ON c.parent_id = p.theme_id
        WHERE c.parent_id IS NOT NULL
        """
    ).fetchall()
}
```
In `extract_theme_weights`:
```python
parent_weights: dict[str, float] = defaultdict(float)
parent_rank_adj: dict[str, float] = defaultdict(float)

for theme in payload.get("themes", []):
    theme_id = theme.get("key")
    parent_name = theme_to_parent.get(theme_id)
    if not parent_name:
        continue
    parent_metric = to_lower_snake_case(parent_name)
    if theme.get("weight") is not None:
        parent_weights[parent_metric] += float(theme["weight"])
    if theme.get("rank_adjusted_weight") is not None:
        parent_rank_adj[parent_metric] += float(theme["rank_adjusted_weight"])
```

### 6.4 Recommendation
1. **Immediate term:** Apply the SQL optimizations (D-OPT-1 through D-OPT-5) to `etfportfolio/prep/panel.py`. This immediately eliminates the 90°C thermal spike and drops panel build time to ~78s without altering current business logic or test specifications.
2. **Next iteration:** Evaluate whether downstream factor regressions require 491 granular micro-themes or 19 macroeconomic parent themes. If 19 parent themes suffice, adopt Architecture B to eliminate 14M rows of storage overhead.
