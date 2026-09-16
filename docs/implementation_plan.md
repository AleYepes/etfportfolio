# Implementation Plan: Resilient Historical Price Ingestion, Overlap Validation & Ingest Cleaning

## Executive Summary & Objective

The `etfportfolio` pipeline ingests daily historical OHLCV prices across 22,500+ global ETFs from Interactive Brokers (IBKR Gateway `clientId=2`) into DuckDB (`bronze.prices`). When corporate actions, splits, or structural restatements occur, superseded series are archived into `cold_storage.prices`.

In previous production runs:
1. **Spurious 30-Year Refetches (6.4%–11.0% of universe):** Minor exchange settlement drift (closing cross auction prices adjusting by pennies on $T_0$ to $T_4$) and volume/VWAP drift triggered thousands of full 30-year IBKR queries, consuming gateway pacing limits (60 req / 10 min) and bloating runtimes.
2. **Cold Storage Archival Bloat:** Over 60% of archived runs in `cold_storage.prices` (~1.2M rows) were false positives identical or near-identical to bronze.
3. **Catastrophic Truncations:** Pacing or API hiccups returned 1-bar series, risking silent truncation of 20+ year price histories.

This plan details the full functional, algorithmic, and architectural refactoring to achieve zero-loss, minimal-refetch, storage-efficient price ingestion and offline maintenance.

---

## Architecture & Governance

When tradeoffs arise, the implementation strictly adheres to:

$$\textbf{Correctness} \gg \textbf{Storage Efficiency} > \textbf{Ingestion Runtime}$$

- **Correctness:** Zero tolerance for undetected corporate actions or corrupted/truncated price histories.
- **Storage Efficiency:** Redundant series must never be archived to `cold_storage.prices`.
- **Ingestion Runtime:** Eliminate false-positive refetches, keeping IBKR requests within pacing limits (~1–2% refetch rate maximum).
- **Module Discipline:** 
  - Cleaning routines for bronze tables reside in `etfportfolio/ingest/clean.py` and are exposed via `main.py ingest clean`.
  - Testing structure begins mirroring module organization: `tests/ingest/test_prices.py` and `tests/ingest/test_clean.py`.

---

## User Review Required

> [!IMPORTANT]
> **Key Settled Decisions:**
> 1. **Overlap Window:** 14 calendar days (`OVERLAP_CALENDAR_DAYS = 14`), providing ~10 trading days.
> 2. **Settlement Horizon:** Last 4–5 trading bars before $t_{\text{last}}$.
> 3. **Split / Corporate Action Check:** Discrepancies in historical core bars ($d < t_{\text{last}} - 4\text{ bars}$) trigger refetch as corporate actions. Ratio uniformity ($\text{std}(R) \le 10^{-3}$) confirms splits without distortion from settlement jitter.
> 4. **Settlement Drift Tolerance:** Within the last 4–5 trading bars, shifts $\le \$0.10$ OR $\le 1.00\%$ are accepted as benign settlement adjustments and overwritten in bronze in-place.
> 5. **Anti-Truncation Guard:** `MIN_REFETCH_RETENTION_RATIO = 0.90` (refetch must have $\ge 90\%$ of existing bronze bar count).
> 6. **Pre-Archive Guard:** On 30Y refetch, compare historical bars ($d < t_{\text{overlap\_start}}$). If historical bars are identical to bronze, replace bronze with `archive=False` to prevent cold storage bloat.
> 7. **Clean Subcommand:** Single command `main.py ingest clean` executing cold storage pruning, unreferenced blob deletion, and DuckDB `CHECKPOINT`.
> 8. **Cold Storage Pruning Rule:** Purge runs from `cold_storage.prices` where all bars older than 5 trading days match `bronze.prices` strictly.

---

## Proposed Changes

### Component 1: Ingest Price Validation & Anti-Truncation (`etfportfolio/ingest/prices.py`)

#### [MODIFY] [prices.py](file:///Users/alex/Documents/etfportfolio/etfportfolio/ingest/prices.py)

1. **Constants:**
   ```python
   WHAT_TO_SHOW = "ADJUSTED_LAST"
   BAR_SIZE = "1 day"
   OVERLAP_CALENDAR_DAYS = 14  # Expanded from 7 to 14 days (~10 trading bars)
   FETCH_MARGIN_DAYS = 2

   PRICE_REL_TOL = 1e-4        # 1 basis point (0.01%) for baseline equality
   PRICE_ABS_TOL = 0.01        # $0.01 (1 cent) for baseline equality

   SETTLEMENT_REL_TOL = 0.01   # 1.00% max tolerance for recent settlement drift
   SETTLEMENT_ABS_TOL = 0.10   # $0.10 max absolute drift for recent settlement prints
   SETTLEMENT_TRADING_DAYS = 5 # Horizon for recent exchange settlement drift

   MIN_REFETCH_RETENTION_RATIO = 0.90  # Refetch must contain >= 90% of existing bars
   ```

2. **Series Layout:**
   Ensure `PRICES_SPEC.value_columns` strictly tracks pricing fields:
   ```python
   PRICES_SPEC = SeriesSpec(
       bronze_table="bronze.prices",
       cold_table="cold_storage.prices",
       columns=("open", "high", "low", "close", "volume", "average", "bar_count"),
       value_columns=("open", "high", "low", "close"),  # Excludes volume, average, bar_count
   )
   ```

3. **Window Partitioning & Overlap Validation (`validate_overlap`):**
   - Query bronze rows for $W = [t_{\text{last}} - 14\text{d}, t_{\text{last}}]$.
   - Strict Date Set Equality: If `existing_dates != new_dates` in $W \implies$ return `(False, "date_mismatch")`.
   - Identify all dates with pricing discrepancies using `PRICE_REL_TOL` and `PRICE_ABS_TOL`.
   - If no discrepancies $\implies$ return `(True, None)`.
   - Sort existing trading dates. Partition into:
     - **Settlement Horizon:** $T_{\text{recent}} = \text{sorted\_dates}[-5:]$ (the last 4–5 trading days).
     - **Historical Core:** $W_{\text{core}} = \{d \in \text{sorted\_dates} \mid d \notin T_{\text{recent}}\}$.
   - **Historical Core Evaluation:**
     - If any date in `mismatched_dates` belongs to $W_{\text{core}}$, check ratio uniformity on $W_{\text{core}}$:
       If $\ge 2$ dates differ in $W_{\text{core}}$, compute ratios $R_d = \frac{P_{\text{new}, d, \text{close}}}{P_{\text{old}, d, \text{close}}}$.
       If $\text{std}(R_d) \le 10^{-3}$ and $|\text{mean}(R_d) - 1| > 10^{-3} \implies$ return `(False, "corporate_action")`.
       Any other difference in $W_{\text{core}}$ also returns `(False, "corporate_action")` (historical restatement).
   - **Settlement Horizon Evaluation ($T_{\text{recent}}$):**
     - If all mismatched dates are within $T_{\text{recent}}$:
       - Multi-bar uniformity check on $T_{\text{recent}}$: if $\ge 2$ dates differ and $\text{std}(R_d) \le 10^{-3}$ and $|\text{mean}(R_d) - 1| > 10^{-3} \implies$ return `(False, "corporate_action")`.
       - Check bounded settlement envelope: for every differing date and pricing field, check:
         $$\text{abs\_diff} \le \text{SETTLEMENT\_ABS\_TOL} \quad \text{OR} \quad \text{rel\_diff} \le \text{SETTLEMENT\_REL\_TOL}$$
       - If all fields satisfy the envelope: log info notice and return `(True, None)`. (Incoming bars overwrite bronze in `upsert_series`).
       - If any field exceeds envelope: return `(False, "value_mismatch")`.

4. **Pre-Archive Historical Verification Guard (`_has_historical_price_change`):**
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
           f"SELECT {col_sql} FROM {spec.bronze_table} WHERE product_id = $1 AND date < $2",
           [product_id, cutoff_date],
       ).fetchall()
       for row in rows:
           d = row[0]
           new_val = new_bars.get(d)
           if new_val is None:
               return True
           for i, col in enumerate(spec.value_columns):
               v_old = row[i + 1]
               v_new = new_val.get(col)
               if v_old is not None and v_new is not None:
                   if not math.isclose(float(v_old), float(v_new), rel_tol=PRICE_REL_TOL, abs_tol=PRICE_ABS_TOL):
                       return True
       return False
   ```

5. **Integration in `_fetch_and_store`:**
   - On validation failure: fetch 30Y history.
   - **Anti-Truncation Guard:**
     `min_expected_bars = max(1, math.floor(existing_count * MIN_REFETCH_RETENTION_RATIO)) if existing_count > 5 else 1`
     If `len(full_bars) < min_expected_bars`: log error, abort replacement, preserve bronze, record `status='error'`.
   - **Pre-Archive Check:**
     If `_has_historical_price_change(..., cutoff_date=overlap_start_for(last_date))`:
       `replace_series(PRICES_SPEC, product.product_id, full_bars, archive=True, reason=mismatch_type)`
     Else:
       `replace_series(PRICES_SPEC, product.product_id, full_bars, archive=False)`

---

### Component 2: Ingestion Maintenance & Pruning (`etfportfolio/ingest/clean.py`)

#### [NEW] [clean.py](file:///Users/alex/Documents/etfportfolio/etfportfolio/ingest/clean.py)

Implement clean functions and the CLI controller:
1. `clean_cold_storage(conn: duckdb.DuckDBPyConnection) -> int`:
   - Scans distinct `(product_id, run_id)` in `cold_storage.prices`.
   - For each run, queries cold storage bars for that run. Identifies $t_{\text{max}} = \max(\text{date})$.
   - Evaluates historical bars where $\text{date} \le t_{\text{max}} - 5\text{ days}$.
   - Checks if any historical bar differs from `bronze.prices` (using `PRICE_ABS_TOL` and `PRICE_REL_TOL`).
   - If 0 historical differences $\implies$ delete all rows for `(product_id, run_id)` from `cold_storage.prices`.
   - Returns count of deleted rows.
2. `clean_payload_blobs(conn: duckdb.DuckDBPyConnection) -> int`:
   - Deletes rows in `bronze.payload_blobs` where `hash` is not in `bronze.snapshots` or `bronze.snapshot_previews`.
   - Returns count of deleted blobs.
3. `checkpoint(conn: duckdb.DuckDBPyConnection) -> None`:
   - Executes `conn.execute("CHECKPOINT")`.
4. `run_clean() -> None`:
   - Connects to DuckDB, runs `clean_cold_storage`, `clean_payload_blobs`, and `checkpoint`.
   - Emits console progress messages.

---

### Component 3: CLI Integration (`etfportfolio/ingest/pipeline.py`)

#### [MODIFY] [pipeline.py](file:///Users/alex/Documents/etfportfolio/etfportfolio/ingest/pipeline.py)

Add `clean` method to `Ingest` class:
```python
class Ingest:
    ...
    def clean(self) -> None:
        from etfportfolio.ingest import clean
        clean.run_clean()
```
User runs `python main.py ingest clean` to perform complete database hygiene.

---

### Component 4: Test Suite Restructuring (`tests/ingest/`)

#### [NEW] [test_clean.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_clean.py)
- `test_clean_cold_storage_deletes_redundant_runs` (verifies runs with identical historical bars $> 5\text{d}$ are purged).
- `test_clean_cold_storage_preserves_corporate_actions` (verifies genuine historical splits are retained in cold storage).
- `test_clean_unreferenced_blobs` (verifies orphaned blobs deleted while referenced blobs preserved).
- `test_run_clean_end_to_end` (runs complete cleanup and checkpoint).

#### [NEW] [test_prices.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_prices.py) (migrated from `tests/test_prices.py`)
- `test_format_duration_14_days`
- `test_validate_overlap_exact_match`
- `test_validate_overlap_date_mismatch`
- `test_validate_overlap_settlement_drift_accepted_within_5_days`
- `test_validate_overlap_settlement_drift_rejected_when_exceeds_envelope`
- `test_validate_overlap_historical_core_mismatch_triggers_corporate_action`
- `test_validate_overlap_ratio_uniformity_detects_split`
- `test_fetch_and_store_pre_archive_guard_skips_archive_when_historical_identical`
- `test_fetch_and_store_pre_archive_guard_archives_when_historical_differs`
- `test_fetch_and_store_anti_truncation_guard_prevents_replacement`

#### [DELETE] [test_prices.py](file:///Users/alex/Documents/etfportfolio/tests/test_prices.py) (reorganized into `tests/ingest/test_prices.py`).

---

## Documentation Synchronization

Update `docs/FRD.md` to reflect:
1. `etfportfolio/ingest/clean.py` and `main.py ingest clean` (incorporating package-scoped design).
2. Settlement horizon of 4–5 trading days with ratio uniformity check partitioned between historical core and recent seam.
3. Test suite structure organized under `tests/ingest/`.

---

## Verification Plan

### Automated Tests
Run full test suite:
```bash
.venv/bin/pytest tests/
```
Verify 100% pass across all tests, including new `tests/ingest/test_clean.py` and `tests/ingest/test_prices.py`.

### Lint & Formatting
```bash
.venv/bin/ruff check .
```
Ensure zero lint or style violations.

### Live Database Cleanup Verification
Test dry-run or live run of `python main.py ingest clean`:
- Verify ~1.2M redundant rows safely deleted from `cold_storage.prices`.
- Verify 556 genuine corporate action runs preserved.
- Verify DuckDB file compacted without errors.
