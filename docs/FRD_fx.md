# Document 1: Ingestion Refactoring & FX Rates Engine

**Status:** Approved for Implementation  
**Scope:** `etfportfolio/ingest/`, `etfportfolio/core/schema.sql`  
**Target Modules:**
- `etfportfolio/core/schema.sql` (schema migrations)
- `etfportfolio/ingest/series.py` (new shared module)
- `etfportfolio/ingest/fx_rates.py` (new FX ingestion engine)
- `etfportfolio/ingest/prices.py` (refactored to consume `series.py`)
- `etfportfolio/ingest/pipeline.py` (wiring Phase 2b and CLI)
- `tests/ingest/test_series.py` (new)
- `tests/ingest/test_fx_rates.py` (new)
- `tests/ingest/test_prices.py` (updated)

---

## 1. Executive Summary & Problem Statement

Downstream quantitative factor modeling and cap-weighted portfolio construction require cross-sectional asset size parity across global ETFs. Currently, `bronze.contracts` contains **29 distinct trading currencies** across global funds (`USD, HKD, EUR, GBP, CHF, SEK, AUD, JPY, MXN, INR, CAD, KRW, SGD, TWD, NOK, HUF, PLN, ILS, RUB, DKK, ZAR, CNH, CNY, MYR, SAR, BRL, AED, RON, CZK`). However, the Bronze layer only ingests equity price series (`sec_type IN ('STK', 'ETF', 'FUND')`), leaving no exchange rate time-series in the database.

This initiative accomplishes three goals:
1. **Keeps Ingestion Self-Contained**: FX rates are market time-series fetched via broker API. Ingesting them as an official stage of `main.py ingest` preserves the invariant that Bronze ingestion is completely self-contained and `etfportfolio/prep/` remains a purely offline DuckDB transformation pipeline. `gateway.py` remains strictly within `ingest/`.
2. **Eliminates Code Duplication (DRY Series Engine)**: Decouples the shared historical time-series validation, 14-day overlap checksums, incremental upserts, and cold storage archival logic from `ingest/prices.py` into a reusable module: `ingest/series.py`.
3. **Implements `ingest/fx_rates.py`**: Dynamically qualifies and ingests daily foreign exchange historical midpoint bars from Interactive Brokers Gateway (`sec_type='CASH'`, `exchange='IDEALPRO'`) for all non-USD currencies present in `bronze.contracts`.

---

## 2. Architectural Principles & Invariants

1. **No Speculative Abstractions or Inverse Quoting**: We do not build speculative inverse-rate calculation math ($1/P$) or manual pair-mapping dictionaries. In accordance with strict simplicity, the engine attempts dynamic qualification of `Forex(f"{curr}USD")`. Only pairs where `contract.symbol == curr` and `contract.currency == 'USD'` are ingested. If a currency is not quoted directly against USD on IDEALPRO (or IBKR returns empty contract details), it is treated as untradable: marked as `'no_data'` in status tracking and omitted.
2. **Fail-Hard Freshness Architecture**: `fx_rates.py` mirrors the exact freshness and error handling of `prices.py`. If existing FX series are fresh within `settings.freshness_window_hours`, broker connection is bypassed. If series are stale and IB Gateway is unreachable, the phase immediately raises `IBConnectionError`. Downstream factor weights cannot run on missing or stale currency conversions.
3. **Client ID Allocation**:
   - `clientId=1`: `ingest/contracts.py`
   - `clientId=2`: `ingest/prices.py`
   - `clientId=3`: `ingest/fx_rates.py`
4. **Bronze Isolation**: Qualified Forex contracts are stored in `bronze.contracts` with `sec_type = 'CASH'`. The existing `silver.products` view filter (`WHERE c.product_id IN (SELECT DISTINCT product_id FROM bronze.prices)`) strictly prevents cash contracts from leaking into ETF product analytics.

---

## 3. Database Schema Modifications (`etfportfolio/core/schema.sql`)

Append the following DDL to `schema.sql`:

```sql
-- Bronze: Primary daily FX series (BarData layout matching bronze.prices)
CREATE TABLE IF NOT EXISTS bronze.fx_rates (
    product_id   INTEGER NOT NULL,
    date         TIMESTAMP NOT NULL,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE NOT NULL,
    volume       DOUBLE,
    average      DOUBLE,
    bar_count    INTEGER,
    updated_at   TIMESTAMP NOT NULL,
    PRIMARY KEY (product_id, date)
);

-- Bronze: Freshness and operational status tracking for FX pairs
CREATE TABLE IF NOT EXISTS bronze.fx_status (
    product_id      INTEGER PRIMARY KEY,
    last_checked_at TIMESTAMP NOT NULL,
    status          VARCHAR NOT NULL,
    error_message   VARCHAR
);

-- Cold Storage: Series archival on corporate actions or structural baseline shifts
CREATE TABLE IF NOT EXISTS cold_storage.fx_rates (
    product_id   INTEGER NOT NULL,
    run_id       TIMESTAMP NOT NULL,
    date         TIMESTAMP NOT NULL,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE,
    volume       DOUBLE,
    average      DOUBLE,
    bar_count    INTEGER,
    reason       VARCHAR,
    PRIMARY KEY (product_id, run_id, date)
);
```

---

## 4. Module Specifications

### 4.1. `etfportfolio/ingest/series.py` (New Shared Engine)

Extract the generic time-series mechanics from `ingest/prices.py` into `series.py`:

```python
@dataclass(frozen=True)
class SeriesSpec:
    bronze_table: str
    cold_table: str
    status_table: str
    columns: tuple[str, ...]
    value_columns: tuple[str, ...]

PRICES_SPEC = SeriesSpec(
    bronze_table="bronze.prices",
    cold_table="cold_storage.prices",
    status_table="bronze.price_status",
    columns=("open", "high", "low", "close", "volume", "average", "bar_count"),
    value_columns=("open", "high", "low", "close"),
)

FX_SPEC = SeriesSpec(
    bronze_table="bronze.fx_rates",
    cold_table="cold_storage.fx_rates",
    status_table="bronze.fx_status",
    columns=("open", "high", "low", "close", "volume", "average", "bar_count"),
    value_columns=("open", "high", "low", "close"),
)
```

Export the following pure functions and database helpers:
- `format_duration(days: int) -> str`: Produces IB duration strings (`"X D"` or `"Y Y"`).
- `overlap_start_for(last_date: datetime) -> datetime`: Returns `last_date - timedelta(days=14)`.
- `validate_overlap(conn, spec: SeriesSpec, product_id: int, new_points: dict, last_date: datetime) -> tuple[bool, str | None]`: Validates continuity over $[last\_date - 14d, last\_date]$.
- `replace_series(conn, spec: SeriesSpec, product_id: int, points: dict, *, archive: bool = False, reason: str | None = None) -> None`: Atomic deletion and re-insertion, archiving to `cold_table` when `archive=True`.
- `upsert_series(conn, spec: SeriesSpec, product_id: int, points: dict) -> None`: Overwrite-in-place upsert for overlap window and new tail dates.
- `extract_bars(bars: list[BarData], max_date: datetime | None = None) -> dict[datetime, dict[str, Any]]`: Converts `ib_async.BarData` objects into naive UTC midnight-keyed dictionaries.
- `has_historical_change(conn, spec: SeriesSpec, product_id: int, new_bars: dict, cutoff_date: datetime) -> bool`: Checks if any bar prior to `cutoff_date` was modified.
- `record_series_status(conn, spec: SeriesSpec, product_id: int, status: str, error_message: str | None = None) -> None`: Upserts into `spec.status_table`.
- `load_series_status(conn, spec: SeriesSpec) -> dict[int, PriceSeriesStatus]`: Loads `(last_date, last_updated, last_checked_at, status)` per `product_id`.
- `is_series_fresh(status: PriceSeriesStatus | None, target_date: datetime, hours: float) -> bool`: Returns `True` if latest bar reaches `target_date` (yesterday) or checked recently with status `'ok'` / `'no_data'`.

### 4.2. `etfportfolio/ingest/fx_rates.py` (New FX Engine)

1. **Target Currency Discovery**:
   Query distinct non-USD contract currencies:
   ```sql
   SELECT DISTINCT currency
   FROM bronze.contracts
   WHERE currency IS NOT NULL 
     AND currency != 'USD' 
     AND sec_type != 'CASH'
   ORDER BY currency;
   ```
2. **Contract Qualification (`clientId=3`)**:
   For each target currency string `curr`:
   - Declare contract `contract = Forex(f"{curr}USD")`.
   - Execute `details = await ib.reqContractDetailsAsync(contract)`.
   - Validate qualification: verify `len(details) > 0`, `details[0].contract.symbol == curr`, and `details[0].contract.currency == 'USD'`.
   - If qualified:
     - Extract `conId = details[0].contract.conId`.
     - Upsert contract into `bronze.contracts` using `ingest.contracts.upsert_contract(conn, conId, details[0])`.
   - If not qualified (or empty):
     - Log a warning: `f"Forex pair {curr}USD is not available on IDEALPRO; skipping."`
     - Record `no_data` in `bronze.fx_status` (using a deterministic synthetic negative ID or hash if `conId` is unavailable).
3. **Historical Data Polling**:
   For each qualified Forex contract `cd`:
   - Set `whatToShow = "MIDPOINT"` (required for CASH/IDEALPRO; `ADJUSTED_LAST` is invalid for Forex).
   - Set `barSizeSetting = "1 day"`, `useRTH = True`.
   - Use `SeriesSpec = FX_SPEC`.
   - Initial fetch: `duration = "30 Y"`.
   - Incremental fetch: calculate duration with 14-day overlap + 2-day margin.
   - Run `validate_overlap`, handle cold storage archiving on mismatch, and upsert bars.
4. **Freshness & Status Dampening**:
   Update `bronze.fx_status` with `last_checked_at`, `'ok'`, `'no_data'`, or `'error'`.

### 4.3. `etfportfolio/ingest/prices.py` (Refactor)

Refactor `ingest/prices.py` to remove duplicate helper implementations:
- Replace local definitions of `validate_overlap`, `replace_series`, `upsert_series`, `format_duration`, `overlap_start_for`, `_extract_bars`, `_has_historical_price_change`, `_record_price_status`, `_load_price_series_status`, and `is_series_fresh` with imports from `etfportfolio.ingest.series`.
- Retain equity-specific logic (`resolve_target_products`, `WHAT_TO_SHOW = "ADJUSTED_LAST"`, `sec_type` mapping).

### 4.4. Pipeline Integration (`etfportfolio/ingest/pipeline.py`)

1. Wire `fx_rates.sync(force=force)` into master orchestration immediately after `contracts.sync()` and prior to `prices.sync()`:
   ```python
   console.info("=== Phase 2: Contract qualification ===")
   await contracts.sync(force=force)

   console.info("=== Phase 2b: Foreign exchange rates ===")
   try:
       count = await fx_rates.sync(force=force)
       console.info(f"FX sync complete. {count} currency pairs processed.")
   except Exception as e:
       logger.error("FX sync failed: %s", e)
       raise

   console.info("=== Phase 3: Price series ===")
   await prices.sync(force=force)
   ```
2. Expose CLI command on `Ingest` class:
   ```python
   def fx(self, force: bool = False) -> None:
       count = asyncio.run(fx_rates.sync(force=force))
       console.info(f"FX sync complete. {count} currency pairs processed.")
   ```

---

## 5. Verification & Test Plan

1. **`tests/ingest/test_series.py`**:
   - Verify `validate_overlap` passes on matching prints and bounded settlement revisions, and returns `corporate_action` or `value_mismatch` on structural discrepancies.
   - Verify `replace_series` with `archive=True` writes preceding rows to the cold storage table with run timestamp and reason.
   - Verify `upsert_series` executes idempotent inserts and updates overlapping prints in place.
2. **`tests/ingest/test_fx_rates.py`**:
   - Mock IB Gateway returning valid `ContractDetails` for `EURUSD` (`symbol='EUR', currency='USD', conId=12087792`). Assert contract is upserted to `bronze.contracts` with `sec_type='CASH'` and bars are stored in `bronze.fx_rates`.
   - Mock IB Gateway returning empty details for an unsupported pair (e.g., `VNDUSD`). Assert it logs a warning, writes status `'no_data'`, and does not crash the pipeline.
   - Test incremental update: verify overlap bars are updated and status stamped `'ok'`.
3. **Regression Tests (`tests/ingest/test_prices.py`)**:
   - Execute full price test suite to guarantee zero regression after delegating to `series.py`.
