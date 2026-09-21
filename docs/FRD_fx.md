# Functional Design Review (FDR): Foreign Exchange (FX) Rate Ingestion & Series Engine Generalization

**Target Components:** `etfportfolio/core/schema.sql`, `etfportfolio/ingest/series.py` (new), `etfportfolio/ingest/fx.py` (new), `etfportfolio/ingest/prices.py`, `etfportfolio/ingest/clean.py`, `etfportfolio/ingest/pipeline.py`, `pyproject.toml`  
**Test Components:** `tests/ingest/test_series.py` (new), `tests/ingest/test_fx.py` (new), `tests/ingest/test_clean.py`, `tests/ingest/test_prices.py`

---

## 1. Executive Summary & Objectives

The platform requires historical daily exchange rate series against USD to convert non-USD product fundamentals, market caps, and asset values into USD. While contract qualification and ETF price series are sourced from Interactive Brokers (via IB Gateway), IBKR does not provide uniform historical FX coverage across all required fund base currencies.

This specification details:
1. **Extraction of timeseries mechanics** from `etfportfolio/ingest/prices.py` into a shared, reusable, synchronous engine: `etfportfolio/ingest/series.py`.
2. **Generalization of `SeriesSpec`** to support single-column entity keys (`product_id`) and composite entity keys (`source_currency, target_currency`), with explicit, configurable drift tolerances and archival policies.
3. **Implementation of `etfportfolio/ingest/fx.py`** using `yfinance` to fetch direct quote exchange rates against USD (`{CUR}USD=X`) for all non-USD currencies qualified in `bronze.contracts`.
4. **Integration of the FX phase** into the CLI (`main.py ingest fx`) and the full ingestion sequence (`main.py ingest`) immediately following contract qualification.
5. **Generalization of cold-storage cleanup** in `etfportfolio/ingest/clean.py` across all series tables governed by a `SeriesSpec`.

---

## 2. Architectural Decisions & Rationales

### 2.1 External FX Data Provider: `yfinance`
- **Decision:** Use `yfinance` to fetch daily historical exchange rates via direct quotes against USD (`f"{symbol}USD=X"`).
- **Rationale:** Free, requires no API registration or developer keys, imposes no monthly request caps, and provides over 20 years of daily history for 28/29 target currencies present in `bronze.contracts`.
- **Special Case (CNH vs. CNY):** Yahoo Finance does not supply historical daily bars for offshore Yuan (`CNHUSD=X`, returning only the current day's quote), but provides complete history for onshore Yuan (`CNYUSD=X`). The ingestion layer maps `CNH` to `CNY` when querying `yfinance`, but writes the resulting series into `bronze.fx` under `source_currency = 'CNH'`. If both `CNH` and `CNY` are qualified contracts, both are fetched independently and persisted under their respective codes. This isolates external provider quirks at the ingestion boundary and allows downstream models to join directly on `contracts.currency` without alias tables.

### 2.2 Quoting Convention & Table Naming
- **Decision:** Quote directly in USD per 1 unit of foreign currency (e.g. `EURUSD = 1.08 USD per 1 EUR`).
- **Rationale:** Converting foreign portfolio values and market caps into USD becomes a simple, fast multiplication: $\text{Value}_{\text{USD}} = \text{Value}_{\text{FX}} \times \text{Rate}$.
- **Tables:** Store active bars in `bronze.fx`, archive restatements in `cold_storage.fx`, and track entity status in `bronze.fx_status`, matching the convention of `bronze.prices` / `cold_storage.prices` / `bronze.price_status`.

### 2.3 Execution Model: Synchronous DB Engine with Decoupled Callers
- **Decision:** Keep all DuckDB series operations (`validate_overlap`, `replace_series`, `upsert_series`, `load_series_status`, `record_series_status`, `clean_series_cold_storage`) strictly synchronous in `etfportfolio/ingest/series.py`, accepting `conn: duckdb.DuckDBPyConnection`.
- **Rationale:**
  - `yfinance` is synchronous and blocking. `fx.py` runs synchronously using `with db_connection(settings.db_path) as conn:`, mirroring `clean.run_clean()`. When invoked inside the top-level async pipeline, `_run_full()` bridges it cleanly using `await asyncio.to_thread(fx.sync, force=force)`.
  - `prices.py` retains its battle-tested `ib_async` execution model without modification, offloading the synchronous series calls via `await worker.submit(func, conn, ...)`.
  - This avoids invasive surgery on `ib_async` while eliminating code duplication.

### 2.4 Renaming & Parameterizing `SeriesSpec`
- **Decision:** Replace ambiguous properties `columns` and `value_columns` with unambiguous attributes:
  - `entity_columns`: Tuple of primary key columns identifying the entity (`("product_id",)` or `("source_currency", "target_currency")`).
  - `data_columns`: All data payload columns stored in the table (`("open", "high", "low", "close", ...)`).
  - `tolerance_columns`: Subset of data columns evaluated in overlap drift tests (`("open", "high", "low", "close")`).
  - Configurable tolerances and flags (`rel_tol`, `abs_tol`, `settlement_rel_tol`, `settlement_abs_tol`, `settlement_trading_days`, `detect_splits`).
- **FX Tolerances:** Because FX units like JPY and KRW trade at tiny fractions of a dollar (~$0.0065 and ~$0.00075), an absolute tolerance of `$0.01` would mask a 150%+ error. FX tolerances rely strictly on relative tolerance (`rel_tol = 1e-4`, `settlement_rel_tol = 0.01`) with absolute tolerance scaled down to machine epsilon (`abs_tol = 1e-8`, `settlement_abs_tol = 1e-6`). Furthermore, `detect_splits` is disabled for FX since foreign currencies do not undergo stock splits or dividend restatements.

---

## 3. Database Schema Additions (`etfportfolio/core/schema.sql`)

Append the following DDL to `etfportfolio/core/schema.sql`:

```sql
CREATE TABLE IF NOT EXISTS bronze.fx (
    source_currency   VARCHAR NOT NULL,
    target_currency   VARCHAR NOT NULL,
    date              TIMESTAMP NOT NULL,
    open              DOUBLE,
    high              DOUBLE,
    low               DOUBLE,
    close             DOUBLE NOT NULL,
    updated_at        TIMESTAMP NOT NULL,
    PRIMARY KEY (source_currency, target_currency, date)
);

CREATE TABLE IF NOT EXISTS bronze.fx_status (
    source_currency   VARCHAR NOT NULL,
    target_currency   VARCHAR NOT NULL,
    last_checked_at   TIMESTAMP NOT NULL,
    status            VARCHAR NOT NULL,
    error_message     VARCHAR,
    PRIMARY KEY (source_currency, target_currency)
);

CREATE TABLE IF NOT EXISTS cold_storage.fx (
    source_currency   VARCHAR NOT NULL,
    target_currency   VARCHAR NOT NULL,
    run_id            TIMESTAMP NOT NULL,
    date              TIMESTAMP NOT NULL,
    open              DOUBLE,
    high              DOUBLE,
    low               DOUBLE,
    close             DOUBLE,
    reason            VARCHAR,
    PRIMARY KEY (source_currency, target_currency, run_id, date)
);
```

---

## 4. Shared Series Engine Specification (`etfportfolio/ingest/series.py`)

Create `etfportfolio/ingest/series.py` by extracting and generalizing the timeseries logic from `prices.py`.

### 4.1 Data Structures & Constants

```python
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

logger = logging.getLogger(__name__)

OVERLAP_CALENDAR_DAYS = 14
FETCH_MARGIN_DAYS = 2
MIN_REFETCH_RETENTION_RATIO = 0.90


@dataclass(frozen=True)
class SeriesSpec:
    bronze_table: str
    cold_table: str
    status_table: str
    entity_columns: tuple[str, ...]
    data_columns: tuple[str, ...]
    tolerance_columns: tuple[str, ...]
    rel_tol: float = 1e-4
    abs_tol: float = 0.01
    settlement_rel_tol: float = 0.01
    settlement_abs_tol: float = 0.10
    settlement_trading_days: int = 5
    detect_splits: bool = True


@dataclass(frozen=True)
class SeriesStatus:
    last_date: datetime | None
    last_updated: datetime | None
    last_checked_at: datetime | None
    status: str | None  # 'ok', 'no_data', 'error', or None
```

### 4.2 Helper Utilities
- **`coerce_entity_key(entity_id: Any) -> tuple[Any, ...]`**: Normalizes a scalar (e.g. `int` or `str`) to a 1-tuple `(entity_id,)` if it is not already a tuple.
- **`build_entity_where(entity_columns: tuple[str, ...], prefix: str = "", start_param: int = 1) -> tuple[str, int]`**:
  Generates `f"{prefix}{col} = ${start_param + i}"` joined by ` AND `, returning the clause string and next parameter index.
- **`overlap_start_for(last_date: datetime, calendar_days: int = OVERLAP_CALENDAR_DAYS) -> datetime`**:
  Returns `last_date - timedelta(days=calendar_days)`.
- **`is_series_fresh(status: SeriesStatus | None, target_date: datetime, hours: float) -> bool`**:
  - `status is None` $\rightarrow$ `False`.
  - `status.last_date is not None and status.last_date >= target_date` $\rightarrow$ `True`.
  - `status.status in ("ok", "no_data")` $\rightarrow$ `is_fresh(status.last_checked_at, hours) or is_fresh(status.last_updated, hours)` (using `etfportfolio.ingest.utils.is_fresh`).
  - Status `'error'` or `None` $\rightarrow$ `False`.

### 4.3 Core Operations
All functions are synchronous and accept `conn: duckdb.DuckDBPyConnection`.

1. **`get_last_date(conn: duckdb.DuckDBPyConnection, spec: SeriesSpec, entity_id: Any) -> datetime | None`**:
   - Computes `MAX(date)` from `spec.bronze_table` matching `spec.entity_columns`.

2. **`get_series_count(conn: duckdb.DuckDBPyConnection, spec: SeriesSpec, entity_id: Any) -> int`**:
   - Computes `COUNT(*)` from `spec.bronze_table` matching `spec.entity_columns`.

3. **`validate_overlap(conn: duckdb.DuckDBPyConnection, spec: SeriesSpec, entity_id: Any, new_points: dict[datetime, dict[str, Any]], last_date: datetime) -> tuple[bool, str | None]`**:
   - `entity_vals = coerce_entity_key(entity_id)`.
   - Queries `date` and `spec.data_columns` from `spec.bronze_table` in range `[start, last_date]` matching `spec.entity_columns`.
   - Performs set-equality check on dates in window $W = [\text{start}, \text{last\_date}]$. Returns `(False, "date_mismatch")` if date sets differ.
   - Evaluates tolerance differences across `spec.tolerance_columns` using `math.isclose(v1, v2, rel_tol=spec.rel_tol, abs_tol=spec.abs_tol)`.
   - If no values differ: returns `(True, None)`.
   - Partitions mismatches into recent settlement horizon (last `spec.settlement_trading_days`) and historical core.
   - If historical core mismatches exist:
     - If `spec.detect_splits` is `True` and the ratio uniformity test passes: return `(False, "corporate_action")`.
     - Otherwise return `(False, "corporate_action" if spec.detect_splits else "restatement")`.
   - If mismatches are isolated to recent settlement horizon:
     - If `spec.detect_splits` is `True` and ratio uniformity passes: return `(False, "corporate_action")`.
     - If any field exceeds both `spec.settlement_abs_tol` and `spec.settlement_rel_tol`: return `(False, "value_mismatch")`.
     - Otherwise accept bounded settlement revision and return `(True, None)`.

4. **`replace_series(conn: duckdb.DuckDBPyConnection, spec: SeriesSpec, entity_id: Any, points: dict[datetime, dict[str, Any]], *, archive: bool = False, reason: str | None = None) -> None`**:
   - `entity_vals = coerce_entity_key(entity_id)`.
   - Executes inside a transaction (`BEGIN TRANSACTION` ... `COMMIT` / `ROLLBACK`).
   - If `archive=True`: copies existing rows from `spec.bronze_table` into `spec.cold_table`, stamping `run_id = now` and `reason`.
   - Deletes existing rows from `spec.bronze_table` for `entity_vals`.
   - Inserts `points` with `updated_at = now`.

5. **`upsert_series(conn: duckdb.DuckDBPyConnection, spec: SeriesSpec, entity_id: Any, points: dict[datetime, dict[str, Any]]) -> None`**:
   - Executes inside a transaction.
   - Performs `INSERT INTO {spec.bronze_table} ... ON CONFLICT ({spec.entity_columns}, date) DO UPDATE SET ...` updating `spec.data_columns` and `updated_at = now`.

6. **`record_series_status(conn: duckdb.DuckDBPyConnection, spec: SeriesSpec, entity_id: Any, status: str, error_message: str | None = None) -> None`**:
   - Upserts into `spec.status_table` with `last_checked_at = now`, `status`, and truncated `error_message[:500]`.

7. **`load_series_status(conn: duckdb.DuckDBPyConnection, spec: SeriesSpec) -> dict[Any, SeriesStatus]`**:
   - Executes dynamic full outer join between `spec.bronze_table` and `spec.status_table` grouped by `spec.entity_columns`:
     ```sql
     SELECT
         {coalesced_entity_cols},
         MAX(b.date) AS last_date,
         MAX(b.updated_at) AS last_updated,
         MAX(s.last_checked_at) AS last_checked_at,
         MAX(s.status) AS status
     FROM {spec.bronze_table} b
     FULL OUTER JOIN {spec.status_table} s
       ON {join_conditions}
     GROUP BY {group_by_indices}
     ```
   - If `len(spec.entity_columns) == 1`: returns `dict[Any, SeriesStatus]` keyed by the scalar identifier (e.g. `product_id`).
   - If `len(spec.entity_columns) > 1`: returns `dict[tuple[Any, ...], SeriesStatus]` keyed by tuple (e.g. `(source_currency, target_currency)`).

8. **`has_historical_series_change(conn: duckdb.DuckDBPyConnection, spec: SeriesSpec, entity_id: Any, new_bars: dict[datetime, dict[str, Any]], cutoff_date: datetime) -> bool`**:
   - Returns `True` if any bar in `spec.bronze_table` with `date < cutoff_date` is absent from `new_bars` or differs beyond `spec.rel_tol` / `spec.abs_tol`.

9. **`clean_series_cold_storage(conn: duckdb.DuckDBPyConnection, spec: SeriesSpec) -> int`**:
   - Queries distinct runs from `spec.cold_table`: `SELECT DISTINCT {entity_columns}, run_id FROM {spec.cold_table}`.
   - For each run: finds `cutoff_date = MAX(date) - timedelta(days=spec.settlement_trading_days + 2)`.
   - Checks if any historical bar (`date <= cutoff_date`) differs from `spec.bronze_table` beyond tolerances.
   - If 0 mismatches found: deletes redundant run from `spec.cold_table` and returns total deleted rows.

---

## 5. FX Ingestion Specification (`etfportfolio/ingest/fx.py`)

Create `etfportfolio/ingest/fx.py`.

### 5.1 Specification & Configuration

```python
from etfportfolio.ingest.series import SeriesSpec

FX_SPEC = SeriesSpec(
    bronze_table="bronze.fx",
    cold_table="cold_storage.fx",
    status_table="bronze.fx_status",
    entity_columns=("source_currency", "target_currency"),
    data_columns=("open", "high", "low", "close"),
    tolerance_columns=("open", "high", "low", "close"),
    rel_tol=1e-4,  # 1 basis point
    abs_tol=1e-8,  # Machine epsilon for FX rates
    settlement_rel_tol=0.01,  # 1.00%
    settlement_abs_tol=1e-6,
    settlement_trading_days=5,
    detect_splits=False,  # Currencies do not undergo stock splits
)

DEFAULT_TARGET_CURRENCY = "USD"
FX_TICKER_OVERRIDES = {
    "CNH": "CNY",  # yfinance lacks CNH history; substitute with CNY
}
```

### 5.2 Currency Discovery
```python
def resolve_target_currencies(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Select unique non-USD contract currencies."""
    total_row = conn.execute("SELECT COUNT(*) FROM bronze.contracts").fetchone()
    total_contracts = total_row[0] if total_row else 0
    if total_contracts == 0:
        raise RuntimeError("bronze.contracts is empty. Run 'ingest contracts' first to qualify products.")

    query = """
    SELECT DISTINCT currency
    FROM bronze.contracts
    WHERE currency IS NOT NULL
      AND currency != 'USD'
      AND sec_type != 'CASH'
    ORDER BY currency
    """
    rows = conn.execute(query).fetchall()
    return [row[0] for row in rows]
```

### 5.3 Fetching & Parsing via `yfinance`
- **Ticker Mapping:**
  ```python
  fetch_symbol = FX_TICKER_OVERRIDES.get(source_currency, source_currency)
  ticker_symbol = f"{fetch_symbol}{target_currency}=X"
  ```
- **DataFrame Extraction (`extract_fx_bars`):**
  - Accepts a `pandas.DataFrame` from `yf.Ticker(ticker_symbol).history(...)` and an optional `max_date: datetime`.
  - Normalizes index timestamps: converts tz-aware or naive index timestamps to naive UTC midnight (`d.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)`).
  - Normalizes column names to lowercase (`open`, `high`, `low`, `close`).
  - Drops rows where `close` is `NaN` or `None`.
  - Filters out any bars with `date > max_date` (where `max_date = yesterday`).
  - Returns `dict[datetime, dict[str, float]]`.

### 5.4 Ingestion & Overlap Lifecycle (`_fetch_and_store_fx`)
For a given `entity_id = (source_currency, target_currency)`:
1. Query `last_date = get_last_date(conn, FX_SPEC, entity_id)`.
2. Compute `today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)` and `yesterday = today - timedelta(days=1)`.
3. **First Fill (`last_date is None`):**
   - Call `yf.Ticker(ticker_symbol).history(period="max", auto_adjust=False)`.
   - Extract bars bounded by `max_date = yesterday`.
   - If empty: call `record_series_status(conn, FX_SPEC, entity_id, "no_data")`.
   - If bars present: call `replace_series(conn, FX_SPEC, entity_id, new_bars, archive=False)` and `record_series_status(conn, FX_SPEC, entity_id, "ok")`.
4. **Incremental Fill (`last_date` exists):**
   - Calculate `start_date = overlap_start_for(last_date) - timedelta(days=FETCH_MARGIN_DAYS)`.
   - Call `yf.Ticker(ticker_symbol).history(start=start_date.strftime("%Y-%m-%d"), auto_adjust=False)`.
   - Extract bars bounded by `max_date = yesterday`.
   - If empty: call `record_series_status(conn, FX_SPEC, entity_id, "no_data")`.
   - Call `valid, mismatch_type = validate_overlap(conn, FX_SPEC, entity_id, new_bars, last_date)`.
   - **If Valid:**
     - Filter `points_to_store = {d: pt for d, pt in new_bars.items() if d >= overlap_start_for(last_date)}`.
     - Call `upsert_series(conn, FX_SPEC, entity_id, points_to_store)` and `record_series_status(conn, FX_SPEC, entity_id, "ok")`.
   - **If Mismatch:**
     - Query existing row count via `get_series_count(conn, FX_SPEC, entity_id)`.
     - Fetch full history: `yf.Ticker(ticker_symbol).history(period="max", auto_adjust=False)`.
     - Extract bars bounded by `yesterday`.
     - Check retention threshold: if `len(full_bars) < floor(existing_count * MIN_REFETCH_RETENTION_RATIO)` (when `existing_count > 5`), abort replace, preserve existing rows, and call `record_series_status(conn, FX_SPEC, entity_id, "error", err_msg)`.
     - Call `has_historical_series_change(conn, FX_SPEC, entity_id, full_bars, overlap_start_for(last_date))`.
     - If true: `replace_series(conn, FX_SPEC, entity_id, full_bars, archive=True, reason=mismatch_type)`.
     - If false: `replace_series(conn, FX_SPEC, entity_id, full_bars, archive=False)`.
     - Call `record_series_status(conn, FX_SPEC, entity_id, "ok")`.

### 5.5 Entry Point
```python
def sync(force: bool = False, target_currency: str = DEFAULT_TARGET_CURRENCY) -> int:
    """Synchronous entry point for FX rate ingestion."""
```
- Open DB: `with db_connection(settings.db_path) as conn:`.
- Resolve non-USD currencies from `bronze.contracts`. If empty, return 0.
- Check freshness via `status_cache = load_series_status(conn, FX_SPEC)`.
- Filter `to_process`: currencies where `force=True` or `not is_series_fresh(status_cache.get((cur, target_currency)), yesterday, settings.freshness_window_hours)`.
- Loop through currencies requiring update under `progress_bar(len(to_process), desc="FX Rates", unit="currency")`:
  - Execute `_fetch_and_store_fx(conn, currency, target_currency)`.
  - Wrap per-currency execution in `try ... except Exception as e:` $\rightarrow$ log error and `record_series_status(conn, FX_SPEC, (currency, target_currency), "error", str(e))` to avoid crashing the batch loop.
- Return number of processed currencies.

---

## 6. Refactoring Existing Components

### 6.1 `etfportfolio/ingest/prices.py`
- Define `PRICES_SPEC` using `SeriesSpec`:
  ```python
  PRICES_SPEC = SeriesSpec(
      bronze_table="bronze.prices",
      cold_table="cold_storage.prices",
      status_table="bronze.price_status",
      entity_columns=("product_id",),
      data_columns=("open", "high", "low", "close", "volume", "average", "bar_count"),
      tolerance_columns=("open", "high", "low", "close"),
      rel_tol=PRICE_REL_TOL,
      abs_tol=PRICE_ABS_TOL,
      settlement_rel_tol=SETTLEMENT_REL_TOL,
      settlement_abs_tol=SETTLEMENT_ABS_TOL,
      settlement_trading_days=SETTLEMENT_TRADING_DAYS,
      detect_splits=True,
  )
  ```
- Remove duplicated functions (`validate_overlap`, `replace_series`, `upsert_series`, `record_series_status`, `is_series_fresh`, `_has_historical_price_change`, `_get_last_date`, `_get_series_count`) and import them directly from `etfportfolio.ingest.series`.
- Refactor `_load_price_series_status(conn)` to call `load_series_status(conn, PRICES_SPEC)`.
- Keep existing IB Gateway connection management (`client_id=2`) and `_fetch_historical` intact.

### 6.2 `etfportfolio/ingest/clean.py`
- Generalize `clean_cold_storage`:
  ```python
  def clean_cold_storage(conn: duckdb.DuckDBPyConnection) -> int:
      """Purges redundant runs in cold_storage.prices and cold_storage.fx."""
      from etfportfolio.ingest.fx import FX_SPEC
      from etfportfolio.ingest.prices import PRICES_SPEC
      from etfportfolio.ingest.series import clean_series_cold_storage

      deleted_prices = clean_series_cold_storage(conn, PRICES_SPEC)
      deleted_fx = clean_series_cold_storage(conn, FX_SPEC)
      
      logger.info("Cold storage purged: %d prices rows, %d fx rows.", deleted_prices, deleted_fx)
      return deleted_prices + deleted_fx
  ```
- Keeps the exact `clean_cold_storage(conn) -> int` return signature for 100% backward compatibility with existing tests and scripts.

### 6.3 `etfportfolio/ingest/pipeline.py` & CLI
- Add `_run_fx(force: bool = False) -> int`:
  ```python
  async def _run_fx(force: bool = False) -> int:
      from etfportfolio.ingest import fx
      return await asyncio.to_thread(fx.sync, force=force)
  ```
- Expose `fx` in `Ingest` CLI:
  ```python
  class Ingest:
      ...
      def fx(self, force: bool = False) -> None:
          from etfportfolio.ingest import fx
          count = fx.sync(force=force)
          console.info(f"FX sync complete. {count} currencies processed.")
  ```
- Update `_run_full`:
  - Phase 1: Product discovery (`products.sync`)
  - Phase 2: Contract qualification (`contracts.sync`)
  - **Phase 3: FX rate series** (`await asyncio.to_thread(fx.sync, force=force)`)
  - Phase 4: Price series (`prices.sync`)
  - Phase 5: Session validation (`session.ensure_session`)
  - Phase 6: Theme taxonomy (`themes.sync`)
  - Phase 7: Product details (`_run_details_phase`)
  - Phase 8: Data cleaning (`clean.run_clean()`)

---

## 7. Testing & Verification Requirements

Per project guidelines, every module in `etfportfolio/<pkg>/<mod>.py` must have a corresponding test suite in `tests/<pkg>/test_<mod>.py`.

### 7.1 `tests/ingest/test_series.py`
Unit test all shared operations against an in-memory DuckDB connection (`duckdb.connect(":memory:")` with `schema.sql` applied):
1. **Overlap Validation:**
   - Exact match returns `(True, None)`.
   - Missing/extra date returns `(False, 'date_mismatch')`.
   - Core value drift returns `(False, 'corporate_action')` when `detect_splits=True`, or `(False, 'restatement')` when `detect_splits=False`.
   - Multiplicative ratio shift detection on close prices (`detect_splits=True`).
   - Bounded recent settlement revision returns `(True, None)`.
   - Excessive recent settlement drift returns `(False, 'value_mismatch')`.
2. **Replace & Upsert:**
   - Atomic replacement deletes old records and inserts new records.
   - `archive=True` writes prior state to cold storage table with `reason`.
   - Upsert preserves untouched historical bars and overwrites modified overlap bars.
3. **Composite Entity Keys:**
   - Verify `replace_series`, `upsert_series`, `validate_overlap`, and `clean_series_cold_storage` work with composite keys like `("source_currency", "target_currency")`.
4. **Status Dampening:**
   - Verify `is_series_fresh` returns `True` for recent bars, `True` for dampened `'no_data'`/`'ok'`, and `False` for `'error'` or missing status.
5. **Cold Storage Cleanup:**
   - Redundant run matching bronze within tolerance is deleted.
   - Run containing true historical restatements is preserved.

### 7.2 `tests/ingest/test_fx.py`
Unit test FX ingestion completely offline using mocks (`unittest.mock.patch` on `yfinance.Ticker`):
1. **Currency Resolution:**
   - Querying `bronze.contracts` returns distinct sorted non-USD currencies, excluding `sec_type = 'CASH'`.
   - Empty contracts table raises `RuntimeError`.
2. **Data Transformation & Normalization:**
   - `extract_fx_bars` correctly normalizes tz-aware pandas timestamps to UTC naive midnight and strips bars > `yesterday`.
   - Rows with `NaN` in `close` are omitted.
3. **CNH / Override Mapping:**
   - Ticker resolution routes `CNH` to `CNYUSD=X`, but writes into `bronze.fx` with `source_currency = 'CNH'`.
   - If both `CNH` and `CNY` exist in contracts, both are processed independently.
4. **Sync Workflow (Mocked `yf.Ticker`):**
   - Initial fill populates `bronze.fx` and marks status `'ok'`.
   - Incremental fill with matching overlap updates tail bars.
   - Restatement triggers cold storage archival and replacement.
   - Network failure records status `'error'` without crashing the batch loop.

---

## 8. Step-by-Step Implementation Sequence

1. **Dependency:** Add `yfinance` to `pyproject.toml` dependencies and run `uv sync`.
2. **Schema:** Append DDL for `bronze.fx`, `bronze.fx_status`, and `cold_storage.fx` to `etfportfolio/core/schema.sql`.
3. **Shared Series Engine:** Implement `etfportfolio/ingest/series.py` and unit tests `tests/ingest/test_series.py`.
4. **Prices Refactor:** Update `etfportfolio/ingest/prices.py` to use `series.py`; verify with existing tests in `tests/ingest/test_prices.py`.
5. **FX Ingestion:** Implement `etfportfolio/ingest/fx.py` and unit tests `tests/ingest/test_fx.py`.
6. **Cleaning Update:** Update `etfportfolio/ingest/clean.py` and `tests/ingest/test_clean.py` to clean both prices and FX cold storage.
7. **Pipeline & CLI:** Wire `fx` into `etfportfolio/ingest/pipeline.py` and verify `main.py ingest fx`.
8. **Final Test Suite:** Run `uv run pytest` and linting checks across the entire repository.