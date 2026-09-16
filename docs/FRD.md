# Functional Requirements Document (FRD): Resilient Historical Price Ingestion, Overlap Validation & Cold Storage Lifecycle Management

**Status:** Approved for Implementation  
**Target Modules:**  
- `etfportfolio/ingest/prices.py`
- `etfportfolio/clean/` (`pipeline.py`, `prices.py`, `blobs.py`, `__init__.py`)
- `main.py`
- `scripts/restore_truncated_prices.py`
- `tests/test_prices.py`, `tests/test_clean.py`, `tests/test_products.py`, `tests/test_utils.py`  
**Layer:** Bronze, Cold Storage & Maintenance (Medallion Architecture)  
**Author:** Quantitative Architecture & Engineering  
**Date:** September 2026  

---

## 1. Executive Summary & Problem Context

### 1.1 Ingestion Pipeline Architecture
The `etfportfolio` pipeline ingests daily historical OHLCV prices across 22,500+ global ETFs from Interactive Brokers (IBKR Gateway `clientId=2`) into DuckDB (`bronze.prices`). When corporate actions, splits, or structural restatements occur, superseded series are archived into `cold_storage.prices`.

Incremental updates proceed as follows:
1. **Initial Fill:** Fetches a 30-year baseline (`duration="30 Y"`).
2. **Incremental Update:** For products with existing history ending on $t_{\text{last}}$, fetches the elapsed window plus an overlap window $W$ and a 2-day fetch margin.
3. **Overlap Validation:** Validates incoming bars in $W$ against stored bronze bars.
   - **Pass:** The overlap window and incremental tail ($t \ge t_{\text{overlap\_start}}$) are upserted into `bronze.prices` via `upsert_series`.
   - **Fail (Mismatch):** Refetches complete 30-year history (`"30 Y"`), archives existing bronze rows to `cold_storage.prices`, and replaces `bronze.prices`.

---

### 1.2 The Production Incidents & Iteration History

- **Run 1 (Baseline Fill):** Ingested all 22,500+ global ETF price series.
- **Run 2 (First Incremental Update — Pre-FRD):**
  - Out of 5,501 products processed before manual pipeline interruption, **604 products (11.0%)** triggered full 30-year refetches:
    - **601 products** failed due to `value_mismatch`.
    - **3 products** failed due to `date_mismatch` (`229325937`, `236798101`, `332383610`), triggering catastrophic 1-bar truncation.
- **Run 3 (Second Incremental Update — Post-Partial FRD):**
  - Tolerances were tightened to a single strict threshold (`REL_TOL = 1e-4`, `ABS_TOL = 0.01`), and seam-check logic collapsed into the same tight bound.
  - While refetches dropped to **6.41%**, this remained unacceptably high. Across 22.5k products, a 6.4% refetch rate demands ~1,440 full 30-year IBKR queries, severely exhausting IBKR pacing limits (60 req / 10 min) and bloating runtime by dozens of hours.

---

### 1.3 Empirical Database Audit (`data/etf.duckdb`)

An audit of the live 3.8 GB database (`data/etf.duckdb`, containing 27,410,990 bronze price bars and 1,906,184 cold storage price bars across 1,288 products) uncovered the following distribution among products archived for `value_mismatch`:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│              Empirical Breakdown of cold_storage.prices (1,288 Products)        │
├────────────────────────────┬───────────┬────────────┬───────────────────────────┤
│ Failure Classification     │ Products  │ Percentage │ Underlying Root Cause     │
├────────────────────────────┼───────────┼────────────┼───────────────────────────┤
│ 1. Zero-Diff Identical     │ 577       │ 44.8%      │ Volume & VWAP drift       │
│ 2. Isolated Recent Shift   │ 210       │ 16.3%      │ Exchange settlement drift │
│    - Seam bar (Day 0 / Fri)│ 187       │ 14.5%      │ Closing cross auction     │
│    - Pre-seam (Day 1 / Thu)│ 20        │ 1.6%       │ T+1/T+2 clearing breaks   │
│    - Day 2 to 6            │ 3         │ 0.2%       │ Clearing breaks / jitter  │
│ 3. True Corporate Actions  │ 485       │ 37.6%      │ Multi-year splits & divs  │
│ 4. Ancient Jitter (>7d)    │ 16        │ 1.2%       │ 1.0001 bp float jitter    │
└────────────────────────────┴───────────┴────────────┴───────────────────────────┘
```

#### Key Empirical Insights:
1. **False-Positive Archival Bloat (61.1% of cold storage):** 
   577 products in `cold_storage.prices` have **100% identical OHLC prices** to `bronze.prices` across their entire 30-year history. Another 210 products differed only on the most recent 1–2 days by a few cents (median shift: \$0.06 / 0.11%). Both categories were needlessly archived, bloating the database by over 1.1 million rows.
2. **Settlement Adjustments Extend Beyond Day 0:**
   While 187 products had discrepancies strictly on the latest seam date ($t_{\text{last}}$), 20 products had adjustments on the preceding trading date ($t_{\text{last}} - 1$). Continuous market closes occur at 16:00, but official primary exchange closing cross settlement, off-exchange Form T prints, and trade busts settle over T+1/T+2. A single-day seam check is too narrow; the overlap evaluation must accommodate bounded settlement adjustments across the recent tail of the window.
3. **Multi-Bar Uniformity Governs Corporate Actions:**
   When a genuine split or dividend occurs under `WHAT_TO_SHOW = "ADJUSTED_LAST"`, IBKR scales past prices by a uniform factor $R = \frac{P_{\text{new}}}{P_{\text{old}}}$. Across the overlap window, every single bar shifts by the exact same ratio $R$ ($\text{std}(R_d) \approx 0$). In contrast, settlement drift is localized strictly to recent dates ($d \ge t_{\text{last}} - 2\text{d}$) and does not affect historical bars.

---

## 2. Architectural Objectives & Governing Principles

### 2.1 Settled Objective Hierarchy
When design tradeoffs arise, the pipeline strictly adheres to:

$$\textbf{Correctness} \gg \textbf{Storage Efficiency} > \textbf{Ingestion Runtime}$$

- **Correctness:** Zero tolerance for undetected corporate actions or corrupted/truncated price histories. A product must never lose historical bars or miss a split.
- **Storage Efficiency:** Bloated tables in `bronze` and `cold_storage` severely degrade downstream preprocessing, panel construction, and factor regression runtime. Identical or redundant series must never be archived.
- **Ingestion Runtime:** Pacing sustainability is paramount. Eliminating false-positive refetches keeps IBKR requests well within gateway limits (60 req / 10 min), transforming a multi-day ordeal into a fast weekly incremental sync.

### 2.2 Core Architectural Principles
- **Simplicity:** Clear, simple, testable code. No speculative abstractions or unneeded metadata layers. Module-specific logic stays inside the module.
- **Two-Tier Defense:** Prevent redundant writes at ingestion time (write-time guard) AND provide an offline maintenance tool to prune legacy bloat (clean module).
- **Replacement Over Deprecation:** Obsolete composite tests and dead routines are deleted cleanly.
- **Medallion Discipline:** Bronze stores raw data; Cold Storage acts as an audit trail of structural data evolutions, not an indiscriminate dump of transient API noise.

---

## 3. High-Level Flowcharts

### 3.1 Overlap Validation & Ingestion Decision Tree (`prices.py`)

```
                          Incoming Incremental Bars
                                     │
                     ┌───────────────┴───────────────┐
                     ▼                               ▼
               Bars Returned?                 No Bars Returned
                     │                               │
                     │                        Record 'no_data'
                     ▼
         14-Day Overlap Validation [W]
                     │
       ┌─────────────┼───────────────────────────────┐
       │             │                               │
   Clean Pass    Bounded Settlement Drift     Structural Corporate Action
 (0 diffs in W)  (Diffs strictly in last 2d;  (Diffs on d < last_date - 2d,
       │          rel_diff <= 1% or abs <= $0.10) or ratio std <= 1e-3 across W)
       │             │                               │
       └──────┬──────┘                               ▼
              │                              IBKR 30Y Refetch
              │                                      │
              ▼                              ┌───────┴───────┐
        upsert_series                        ▼               ▼
     (Overlap W + Tail)               len < 90% exist   len >= 90% exist
  [Overwrites settlement                     │               │
    prints in-place]                   ABORT REPLACE         ▼
              │                       Preserve Bronze  Pre-Archive Check
              │                       Record 'error'   (Historical d < W_start)
              │                                              │
              │                                      ┌───────┴───────┐
              │                                      ▼               ▼
              │                               Hist Bars Match  Hist Bars Differ
              │                                      │               │
              │                                SKIP ARCHIVE    Archive to Cold
              │                                Replace Bronze  Replace Bronze
              │                                      │               │
              └──────────────────────────────┬───────┴───────────────┘
                                             ▼
                                        Record 'ok'
```

---

### 3.2 Cold Storage & Payload Blob Maintenance (`clean/`)

```
                           etfportfolio clean
                                   │
              ┌────────────────────┴────────────────────┐
              ▼                                         ▼
     clean_cold_storage                       clean_payload_blobs
              │                                         │
  Scan (product_id, run_id) in               Find blobs with hash NOT IN
      cold_storage.prices                     (bronze.snapshots UNION
              │                                bronze.snapshot_previews)
  Compare OHLC bars against                             │
      current bronze.prices                             ▼
              │                               DELETE from payload_blobs
      ┌───────┴───────┐                                 │
      ▼               ▼                                 │
 100% Match     Genuine Diff                            │
(or superset)   (Splits/Divs)                           │
      │               │                                 │
   DELETE          PRESERVE                             │
      │               │                                 │
      └───────┬───────┘                                 │
              │                                         │
              └────────────────────┬────────────────────┘
                                   ▼
                         DuckDB CHECKPOINT (VACUUM)
```

---

## 4. Detailed Implementation Specifications

### Spec 1: Decouple Price Validation from Volume & VWAP Telemetry
**Location:** `etfportfolio/ingest/prices.py`

Restrict `value_columns` strictly to pricing fields (`open`, `high`, `low`, `close`):

```python
PRICES_SPEC = SeriesSpec(
    bronze_table="bronze.prices",
    cold_table="cold_storage.prices",
    columns=("open", "high", "low", "close", "volume", "average", "bar_count"),
    value_columns=("open", "high", "low", "close"),  # Exclude volume, average, bar_count
)
```

*Rationale:* Prevents post-market volume adjustments from triggering spurious 30-year refetches. Fresh volume and average telemetry are still written to DuckDB during `upsert_series`.

---

### Spec 2: Calibrated Constants & 14-Day Overlap Window
**Location:** `etfportfolio/ingest/prices.py`

```python
OVERLAP_CALENDAR_DAYS = 14  # Expanded from 7 to 14 days (~10 trading bars)
FETCH_MARGIN_DAYS = 2

PRICE_REL_TOL = 1e-4        # 1 basis point (0.01%) for baseline equality
PRICE_ABS_TOL = 0.01        # $0.01 (1 cent) for baseline equality

SETTLEMENT_REL_TOL = 0.01   # 1.00% max tolerance for recent settlement drift
SETTLEMENT_ABS_TOL = 0.10   # $0.10 max absolute drift for recent settlement prints

MIN_REFETCH_RETENTION_RATIO = 0.90  # Refetch must contain >= 90% of existing bars
```

*Rationale:* 
- A 14-day calendar window yields ~10 trading bars. IBKR treats durations under 365 days as `"D"`, so fetching 23–30 days consumes the exact same single sub-second API request and quota as 7 days.
- 10 trading bars provide ample sample points to reliably calculate ratio standard deviation $\text{std}(R)$ and robustly isolate 1-to-2 day settlement adjustments from corporate actions.

---

### Spec 3: Systematic Window Shift Test (`validate_overlap`)
**Location:** `etfportfolio/ingest/prices.py`

Refactor `validate_overlap` to distinguish benign exchange settlement drift from structural corporate actions across window $W = [t_{\text{last}} - 14\text{d}, t_{\text{last}}]$.

#### Rules & Algorithm:
1. **Date Set Equality:** If `existing_dates != new_dates` in $W \implies$ return `(False, "date_mismatch")`.
2. **Strict Baseline Comparison:** Compare `spec.value_columns` using `PRICE_REL_TOL` and `PRICE_ABS_TOL`. Collect all differing dates into `mismatched_dates`.
3. **Clean Pass:** If `len(mismatched_dates) == 0` $\implies$ return `(True, None)`.
4. **Determine Recent Settlement Horizon:** 
   Sort the existing dates in $W$. The settlement horizon consists of the last 2 available trading dates:
   $$T_{\text{recent}} = \{ \text{sorted\_dates}[-1], \text{sorted\_dates}[-2] \}$$
5. **Corporate Action Discrimination:**
   - **Interior Mismatch:** If any date in `mismatched_dates` is older than the last 2 trading dates ($d \notin T_{\text{recent}}$) $\implies$ return `(False, "corporate_action")`.
   - **Multi-Bar Uniformity Test:** If $\ge 2$ dates differ in $W$, compute price ratios $R_d = \frac{P_{\text{new}, d, \text{close}}}{P_{\text{old}, d, \text{close}}}$. If the standard deviation $\text{std}(R_d) \le 10^{-3}$ and $|\text{mean}(R_d) - 1| > 10^{-3}$ $\implies$ return `(False, "corporate_action")`.
6. **Bounded Settlement Drift Envelope:**
   If all differing dates are within $T_{\text{recent}}$, verify that **every** pricing field on those dates satisfies:
   $$\text{abs\_diff} \le \text{SETTLEMENT\_ABS\_TOL} \quad \text{OR} \quad \text{rel\_diff} \le \text{SETTLEMENT\_REL\_TOL}$$
   - If all differing fields satisfy this envelope: log `INFO` notice and return `(True, None)`. `upsert_series` will automatically overwrite these bars in `bronze.prices` with the official settlement prints.
   - If any field exceeds this envelope: return `(False, "value_mismatch")`.

```python
def validate_overlap(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    product_id: int,
    new_points: dict[datetime, dict[str, Any]],
    last_date: datetime,
) -> tuple[bool, str | None]:
    start = overlap_start_for(last_date)
    col_sql = ", ".join(("date", *spec.columns))
    existing_rows = conn.execute(
        f"""
        SELECT {col_sql}
        FROM {spec.bronze_table}
        WHERE product_id = $1 AND date >= $2 AND date <= $3
        ORDER BY date ASC
        """,
        [product_id, start, last_date],
    ).fetchall()

    existing_dates = [row[0] for row in existing_rows]
    existing_set = set(existing_dates)
    existing_vals = {row[0]: {col: row[i + 1] for i, col in enumerate(spec.columns)} for row in existing_rows}

    new_in_w = {d: vals for d, vals in new_points.items() if start <= d <= last_date}
    new_set = set(new_in_w)

    if existing_set != new_set:
        return False, "date_mismatch"

    mismatched_dates: list[datetime] = []
    for d in sorted(new_in_w):
        old_vals = existing_vals[d]
        new_vals = new_in_w[d]
        diff_found = False
        for key in spec.value_columns:
            v1, v2 = old_vals.get(key), new_vals.get(key)
            if v1 is not None and v2 is not None:
                if not math.isclose(float(v1), float(v2), rel_tol=PRICE_REL_TOL, abs_tol=PRICE_ABS_TOL):
                    diff_found = True
                    break
        if diff_found:
            mismatched_dates.append(d)

    if not mismatched_dates:
        return True, None

    # Identify the last 2 trading dates in W
    recent_dates = set(existing_dates[-2:]) if len(existing_dates) >= 2 else set(existing_dates)

    # Discrepancies prior to the last 2 trading days indicate historical structural adjustment
    if any(d not in recent_dates for d in mismatched_dates):
        return False, "corporate_action"

    # Multi-bar ratio uniformity check
    if len(mismatched_dates) >= 2:
        ratios = []
        for d in mismatched_dates:
            c_old = float(existing_vals[d]["close"])
            c_new = float(new_in_w[d]["close"])
            if c_old > 0:
                ratios.append(c_new / c_old)
        if len(ratios) >= 2:
            mean_r = sum(ratios) / len(ratios)
            variance = sum((r - mean_r) ** 2 for r in ratios) / len(ratios)
            std_r = math.sqrt(variance)
            if std_r <= 1e-3 and abs(mean_r - 1.0) > 1e-3:
                return False, "corporate_action"

    # Verify all differences in recent_dates fall within the bounded settlement envelope
    for d in mismatched_dates:
        old_bar = existing_vals[d]
        new_bar = new_in_w[d]
        for key in spec.value_columns:
            v1, v2 = old_bar.get(key), new_bar.get(key)
            if v1 is not None and v2 is not None:
                f1, f2 = float(v1), float(v2)
                if not math.isclose(f1, f2, rel_tol=PRICE_REL_TOL, abs_tol=PRICE_ABS_TOL):
                    abs_diff = abs(f2 - f1)
                    rel_diff = abs_diff / abs(f1) if f1 != 0 else float("inf")
                    if abs_diff > SETTLEMENT_ABS_TOL and rel_diff > SETTLEMENT_REL_TOL:
                        return False, "value_mismatch"

    logger.info(
        "Product %d: accepted bounded settlement revision on %s; overwriting with official prints.",
        product_id,
        [d.strftime("%Y-%m-%d") for d in mismatched_dates],
    )
    return True, None
```

---

### Spec 4: Pre-Archive Historical Verification Guard
**Location:** `etfportfolio/ingest/prices.py`

When a 30-year refetch occurs following a validation failure, **verify whether historical bars ($d < \text{overlap\_start}$) actually changed** before archiving existing bronze rows to `cold_storage.prices`.

#### Helper Function:
```python
def _has_historical_price_change(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    product_id: int,
    new_bars: dict[datetime, dict[str, Any]],
    cutoff_date: datetime,
) -> bool:
    """Returns True if any historical bar (date < cutoff_date) differs between bronze and new_bars."""
    col_sql = ", ".join(("date", *spec.value_columns))
    rows = conn.execute(
        f"""
        SELECT {col_sql}
        FROM {spec.bronze_table}
        WHERE product_id = $1 AND date < $2
        """,
        [product_id, cutoff_date],
    ).fetchall()

    for row in rows:
        d = row[0]
        new_val = new_bars.get(d)
        if new_val is None:
            return True  # A bar was dropped or date changed
        for i, col in enumerate(spec.value_columns):
            v_old = row[i + 1]
            v_new = new_val.get(col)
            if v_old is not None and v_new is not None:
                if not math.isclose(float(v_old), float(v_new), rel_tol=PRICE_REL_TOL, abs_tol=PRICE_ABS_TOL):
                    return True
    return False
```

#### Guard Integration in `_fetch_and_store`:
```python
    if not valid:
        logger.warning(
            "Product %d: %s detected. Refetching full 30Y history...",
            product.product_id,
            mismatch_type,
        )
        existing_count = await worker.submit(_get_series_count, product.product_id)
        full_bars_raw = await _fetch_historical(ib, product, "30 Y", end_datetime="")
        full_bars = _extract_bars(full_bars_raw, max_date=yesterday)

        # Spec 5: Anti-Truncation Guard
        min_expected_bars = (
            max(1, math.floor(existing_count * MIN_REFETCH_RETENTION_RATIO))
            if existing_count > 5
            else 1
        )
        if len(full_bars) < min_expected_bars:
            err_msg = f"Truncated refetch: got {len(full_bars)} bars, expected >= {min_expected_bars} (existing: {existing_count})"
            logger.error("Product %d: %s. Aborting replace to preserve bronze rows.", product.product_id, err_msg)
            await worker.submit(_record_price_status, product.product_id, "error", err_msg)
            return

        if full_bars:
            overlap_start = overlap_start_for(last_date)
            # Spec 4: Pre-Archive Historical Checksum
            hist_changed = await worker.submit(
                _has_historical_price_change,
                PRICES_SPEC,
                product.product_id,
                full_bars,
                overlap_start,
            )

            if hist_changed:
                await worker.submit(
                    replace_series,
                    PRICES_SPEC,
                    product.product_id,
                    full_bars,
                    archive=True,
                    reason=mismatch_type,
                )
                logger.info("Product %d: corporate action confirmed; archived old series and replaced (%d bars)", product.product_id, len(full_bars))
            else:
                await worker.submit(
                    replace_series,
                    PRICES_SPEC,
                    product.product_id,
                    full_bars,
                    archive=False,
                )
                logger.info("Product %d: historical bars identical; replaced bronze WITHOUT cold storage archival (%d bars)", product.product_id, len(full_bars))

            await worker.submit(_record_price_status, product.product_id, "ok", None)
        else:
            await worker.submit(_record_price_status, product.product_id, "no_data", None)
        return
```

---

### Spec 5: Anti-Truncation Safety Guard
**Location:** `etfportfolio/ingest/prices.py`

Preserve the anti-truncation guard: if IBKR returns a transient incomplete response losing $> 10\%$ of historical data (`len(full_bars) < floor(existing_count * 0.90)`), the pipeline:
1. Immediately aborts replacement.
2. Preserves all existing `bronze.prices` rows intact.
3. Records `status="error"` in `bronze.price_status` so the product is retried on subsequent runs.
4. Remains active even under `--force`.

---

### Spec 6: Dedicated Cleaning & Maintenance Package (`etfportfolio/clean/`)
**Target Package:** `etfportfolio/clean/`

Create a dedicated maintenance package to eliminate historical bloat in `cold_storage.prices` and `bronze.payload_blobs`.

#### Directory Layout:
```
etfportfolio/clean/
├── __init__.py
├── pipeline.py
├── prices.py
└── blobs.py
```

#### 1. `etfportfolio/clean/prices.py` (`clean_cold_storage`):
Scans all archived runs in `cold_storage.prices`. Compares each run against the current `bronze.prices` table for the same `product_id`.
- If **100% of archived bars** match `bronze.prices` within `PRICE_REL_TOL` and `PRICE_ABS_TOL`: the archived run is a redundant duplicate. Delete all rows for that `(product_id, run_id)` from `cold_storage.prices`.
- If bars differ (corporate action restatement): keep the archived run.

```python
def clean_cold_storage(conn: duckdb.DuckDBPyConnection) -> int:
    """Purges redundant runs in cold_storage.prices where OHLC prices match bronze.prices.
    
    Returns the number of deleted rows.
    """
    # Fetch distinct product_id, run_id pairs
    runs = conn.execute("SELECT DISTINCT product_id, run_id FROM cold_storage.prices").fetchall()
    deleted_rows = 0

    for pid, run_id in runs:
        # Check if any OHLC bar in cold storage differs from current bronze
        mismatches = conn.execute(
            """
            SELECT count(*)
            FROM cold_storage.prices c
            LEFT JOIN bronze.prices b ON c.product_id = b.product_id AND c.date = b.date
            WHERE c.product_id = $1 AND c.run_id = $2
            AND (
                b.date IS NULL OR
                abs(c.open - b.open) > 0.01 OR abs(c.open - b.open) / nullif(c.open, 0) > 1e-4 OR
                abs(c.high - b.high) > 0.01 OR abs(c.high - b.high) / nullif(c.high, 0) > 1e-4 OR
                abs(c.low - b.low) > 0.01 OR abs(c.low - b.low) / nullif(c.low, 0) > 1e-4 OR
                abs(c.close - b.close) > 0.01 OR abs(c.close - b.close) / nullif(c.close, 0) > 1e-4
            )
            """,
            [pid, run_id],
        ).fetchone()[0]

        if mismatches == 0:
            # 100% match: redundant archive
            count = conn.execute(
                "DELETE FROM cold_storage.prices WHERE product_id = $1 AND run_id = $2",
                [pid, run_id],
            ).fetchone()[0]
            deleted_rows += count
            logger.info("Product %d (run %s): deleted %d redundant cold storage rows.", pid, run_id, count)

    return deleted_rows
```

#### 2. `etfportfolio/clean/blobs.py` (`clean_unreferenced_blobs`):
Identifies and deletes orphaned rows in `bronze.payload_blobs` whose `hash` is not referenced by any record in `bronze.snapshots` or `bronze.snapshot_previews`.

```python
def clean_unreferenced_blobs(conn: duckdb.DuckDBPyConnection) -> int:
    """Deletes unreferenced payload blobs from bronze.payload_blobs.
    
    Returns the number of deleted blobs.
    """
    res = conn.execute(
        """
        DELETE FROM bronze.payload_blobs
        WHERE hash NOT IN (
            SELECT hash FROM bronze.snapshots
            UNION
            SELECT hash FROM bronze.snapshot_previews
        )
        """
    ).fetchone()
    deleted_count = res[0] if res else 0
    return deleted_count
```

#### 3. `etfportfolio/clean/pipeline.py` & CLI Integration:
Orchestrates cleaning phases and runs DuckDB `CHECKPOINT` to compact file storage.

```python
class Clean:
    """CLI surface for maintenance and storage pruning: `main.py clean <subcommand>`."""

    def __call__(self) -> None:
        """Run full cleanup (cold storage deduplication + unreferenced blobs + checkpoint)."""
        self.prices()
        self.blobs()
        self.checkpoint()

    def prices(self) -> None:
        with db_connection(settings.db_path) as conn:
            deleted = clean_cold_storage(conn)
            console.info(f"Cold storage cleanup complete. {deleted} redundant rows purged.")

    def blobs(self) -> None:
        with db_connection(settings.db_path) as conn:
            deleted = clean_unreferenced_blobs(conn)
            console.info(f"Payload blob cleanup complete. {deleted} orphaned blobs purged.")

    def checkpoint(self) -> None:
        with db_connection(settings.db_path) as conn:
            conn.execute("CHECKPOINT")
            console.info("Database checkpoint and storage compaction complete.")

cli = Clean()
```

Expose in `main.py`:
```python
from etfportfolio.clean import pipeline as clean_pipeline
...
fire.Fire(
    {
        "ingest": ingest_pipeline.cli,
        "prep": prep_pipeline.cli,
        "clean": clean_pipeline.cli,
    },
    command=argv,
)
```

---

### Spec 7: Standalone Migration Script for Truncated Products
**Location:** `scripts/restore_truncated_prices.py`

Preserve the idempotent standalone restoration script for products `229325937`, `236798101`, and `332383610` to restore historical rows from `cold_storage.prices` back to `bronze.prices` before setting status to `'error'` for proper incremental extension.

---

### Spec 8: Domain Test Suite Reorganization & Acceptance Criteria

1. **Reorganize Test Files:**
   - Delete obsolete/unmigrated test files.
   - Separate cleanly: `test_prices.py`, `test_clean.py`, `test_products.py`, `test_utils.py`.
2. **Acceptance Scenarios in `tests/test_prices.py`:**
   - `test_format_duration_14_days`: Duration string formatting accounts for 14-day overlap.
   - `test_validate_overlap_settlement_drift_accepted`: Price shifts $\le \$0.10$ on $t_{\text{last}}$ or $t_{\text{last}} - 1$ return `(True, None)`.
   - `test_validate_overlap_interior_shift_triggers_corporate_action`: Discrepancy on $d < t_{\text{last}} - 2\text{d}$ returns `(False, "corporate_action")`.
   - `test_validate_overlap_ratio_uniformity_triggers_corporate_action`: Uniform multiplicative shift ($P_{\text{new}} = 0.5 \times P_{\text{old}}$) across $W$ returns `(False, "corporate_action")`.
   - `test_fetch_and_store_pre_archive_check_skips_archive_when_historical_identical`: Full refetch whose historical bars match bronze replaces bronze with `archive=False`.
   - `test_fetch_and_store_pre_archive_check_archives_when_historical_differs`: Full refetch with altered historical prices writes to `cold_storage.prices` with `archive=True`.
   - `test_fetch_and_store_anti_truncation_guard`: Prevents 1-bar replacement when existing count is > 5.
3. **Acceptance Scenarios in `tests/test_clean.py`:**
   - `test_clean_cold_storage_removes_identical_runs`: Identical archived run is deleted; genuine split run is preserved.
   - `test_clean_unreferenced_blobs`: Orphaned blob is deleted; referenced blob is preserved.
   - `test_clean_cli_execution`: `Clean` pipeline runs end-to-end against DuckDB.

---

## 5. Verification & Acceptance Checklist

| Step | Verification Command | Acceptance Standard |
| :--- | :--- | :--- |
| **1. Regression Tests** | `.venv/bin/pytest tests/` | 100% tests pass cleanly across `test_prices.py`, `test_clean.py`, `test_products.py`, `test_utils.py`. |
| **2. Code Linting** | `.venv/bin/ruff check .` | Zero lint errors or formatting warnings. |
| **3. Cold Storage Pruning** | `python main.py clean prices` | Safely purges ~1.1M redundant rows from `cold_storage.prices` while preserving all 485 corporate action runs. |
| **4. Orphan Blob Pruning** | `python main.py clean blobs` | Safely removes unreferenced payload blobs and compacts DuckDB via `CHECKPOINT`. |
| **5. Incremental Fetch Test** | `python main.py ingest prices` | Refetch rate drops from 6.41% to legitimate corporate actions only (~1–2%), with 0 redundant series archived. |
