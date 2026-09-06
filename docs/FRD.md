# Technical Specification Document

**Document Title**: Exchange Blacklisting, Dedicated Price Status Tracking, and Ingestion Pipeline Simplification  
**Target Components**: `etfportfolio.core`, `etfportfolio.ingestion`  

---

## 1. Executive Summary & Problem Context

The ingestion pipeline fetches ETF universe data and historical daily price series from Interactive Brokers (IBKR) Gateway (`ib_async`) and persists it into DuckDB following a medallion architecture (`bronze` $\rightarrow$ `silver` $\rightarrow$ `gold`, with `cold_storage` for audit archives).

### Identified Issues
1. **Unsubscribed Exchange Failures**: Market data subscriptions in IBKR are billed per-exchange. Queries for products on unsubscribed foreign exchanges (e.g., `SFB`, `EBS`, `TSEJ`, `B3`, `TWSE`, `FWB`, `SWB`) fail with permission errors or return zero data bars (`HMDS query returned no data`).
2. **Missing Retry Dampening**: When a product returns 0 price bars or errors out, nothing is written to `bronze.prices`. Consequently, the pipeline considers the product perpetually un-updated and re-queries hundreds of unpriceable contracts on *every single execution*, wasting API rate limits and generating log noise.
3. **Invalid Duration Parameter Strings (IB Error 321)**: In incremental fetching, `prices.py` calculates duration as `f"{gap_days + 9} D"`. The IBKR API strictly rejects durations formatted with `"D"` if the count exceeds 365 days. Day counts $> 365$ must use the `"Y"` (years) unit.
4. **Architectural Leakage (Layer Boundary Violation)**: Ingestion scripts currently query `silver.products`. As an ingestion engine responsible strictly for producing Bronze data, `ingestion` must only interact with `bronze` tables. `silver.products` is a downstream consumer view and should not be a prerequisite or target for Bronze ingestion.
5. **CLI and Parameter Clutter**: Unused `--product_ids` and `--limit` flags permeate all CLI commands and internal function signatures, introducing dead parsing code and test maintenance overhead.

---

## 2. Architectural Decisions & Design Principles

All changes in this specification adhere to the project's foundational guidelines:
1. **Bronze-Only Ingestion Boundary**: The `ingestion` module queries and writes strictly to `bronze` schemas (`bronze.products`, `bronze.contracts`, `bronze.prices`, `bronze.price_status`, `bronze.snapshots`, `bronze.payload_blobs`) and `cold_storage`. It must never query `silver.products`.
2. **Module Organization (Rule 2)**:
   - Shared across multiple scripts within `ingestion/` $\rightarrow$ `etfportfolio.ingestion.utils` (e.g., `ProductContract`, `is_fresh`).
   - Used only within `prices.py` $\rightarrow$ stays in `etfportfolio.ingestion.prices` (e.g., `format_duration`, `PRICES_SPEC`, `is_series_fresh`, `validate_overlap`, `replace_series`, `upsert_series`).
3. **Replacement Over Deprecation (Rule 3)**: Replace old queries, parameters, and helpers cleanly; do not accumulate deprecated fallback paths or unused parameter shims.
4. **Differentiated Freshness Dampening**:
   - `status = 'ok'` or `'no_data'`: Dampened by the 24-hour freshness window (`settings.freshness_window_hours`). Products that definitively have no data (e.g., untraded, zero volume, unsubscribed exchange) will not be re-queried for 24 hours.
   - `status = 'error'`: Represents transient infrastructure failures (socket drops, Gateway timeouts, temporary network hiccups). An `'error'` status is **never dampened**; it remains immediately eligible for retry on subsequent pipeline runs.
5. **`--force` Flag Semantics**:
   - `--force` acts strictly as a **freshness bypass**, not an unconditional 30-year database wipe.
   - When `--force` is passed, all qualified target products are selected (bypassing the 24-hour dampening gate).
   - For each target product:
     - If `last_date is None` (no prior price bars): pulls full `"30 Y"`.
     - If `last_date is not None` (prior bars exist): runs an incremental fetch with duration `format_duration(gap_days + 9)`.
     - Data corruption recovery remains fully automated: if the existing price series diverges, `validate_overlap` fails its 7-day checksum, archives the corrupt series to `cold_storage.prices`, and pulls a full 30-year replacement.

---

## 3. Configuration & Schema Specifications

### 3.1 Configuration (`etfportfolio/core/config.py`)

Add `blocked_exchanges` to `Settings`.

```python
class Settings(BaseSettings):
    ...
    blocked_exchanges: list[str] = []
```

- **Default**: `[]` (no exchanges blocked unless explicitly configured).
- **Configuration**: Set in `pyproject.toml` under `[tool.etfportfolio]` or via `.env`.
  ```toml
  [tool.etfportfolio]
  blocked_exchanges = ["SFB", "EBS", "TSEJ", "B3", "TWSE", "FWB", "SWB", "TASE", "MEXI", "VALUE", "WSE"]
  ```

### 3.2 Database Schema (`etfportfolio/core/schema.sql`)

Add the `bronze.price_status` table:

```sql
-- Price series ingestion attempt status and freshness tracking
CREATE TABLE IF NOT EXISTS bronze.price_status (
    product_id      INTEGER PRIMARY KEY,
    last_checked_at TIMESTAMP NOT NULL,
    status          VARCHAR NOT NULL,  -- 'ok', 'no_data', 'error'
    error_message   VARCHAR            -- NULL for 'ok' and 'no_data', populated only on exceptions
);
```

- **Medallion Purity**: Keeps `bronze.prices` exclusively populated with authentic market bars. Zero dummy or sentinel rows (`close = -1`) are written to price tables.
- **Error Transparency**: `error_message` records up to 500 characters of exception details strictly when `status == 'error'`, remaining `NULL` for successful or cleanly empty queries.

---

## 4. Module-by-Module Technical Specifications

```
                     ┌────────────────────────────────┐
                     │   etfportfolio/core/config.py  │
                     │  - blocked_exchanges: list[str]│
                     └───────────────┬────────────────┘
                                     │
                 ┌───────────────────┴───────────────────┐
                 ▼                                       ▼
  ┌──────────────────────────────┐       ┌──────────────────────────────┐
  │ etfportfolio/ingestion/      │       │ etfportfolio/ingestion/      │
  │ utils.py                     │       │ products.py                  │
  │ - ProductContract (DTO)      │◄──────┤ - resolve_target_products()  │
  │ - is_fresh()                 │       │   (queries bronze.contracts, │
  │ - content_address(), blobs   │       │    filters blocked_exchanges)│
  └──────────────┬───────────────┘       └──────────────┬───────────────┘
                 │                                      │
                 │          ┌───────────────────────────┘
                 ▼          ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │ etfportfolio/ingestion/prices.py                                    │
  │ - format_duration()                                                 │
  │ - PriceSeriesStatus (dataclass)                                     │
  │ - is_series_fresh() [evaluates 'error' vs 'no_data' in Python]      │
  │ - _load_price_series_status() [reads bronze.contracts/prices/status]│
  │ - _record_price_status() [upserts bronze.price_status]              │
  │ - SeriesSpec, PRICES_SPEC, validate_overlap, replace, upsert        │
  │ - sync(force: bool = False)                                         │
  └─────────────────────────────────────────────────────────────────────┘
```

---

### 4.1 Ingestion Shared Utilities (`etfportfolio/ingestion/utils.py`)

1. **Move `ProductContract` in**: Since `ProductContract` is a data transfer object (DTO) shared across `products.py`, `prices.py`, and `pipeline.py`, it belongs in `etfportfolio/ingestion/utils.py` per Principle 2.
2. **Move Price-Specific Helpers out**: Move `SeriesSpec`, `PRICES_SPEC`, `OVERLAP_CALENDAR_DAYS`, `FETCH_MARGIN_DAYS`, `overlap_start_for`, `validate_overlap`, `_insert_points`, `replace_series`, `upsert_series`, and `is_series_fresh` out of `utils.py` and into `prices.py`.
3. **Preserve General Snapshot Helpers**: Keep `_TYPE_RANK`, `_sort_key`, `_canonicalize`, `canonical_bytes`, `content_address`, `store_blob`, `gc_preview_blob`, and `is_fresh` in `utils.py`.

```python
@dataclass(frozen=True)
class ProductContract:
    product_id: int
    symbol: str | None = None
    sec_type: str | None = None
    exchange_id: str | None = None
    primary_exchange_id: str | None = None
    currency: str | None = None
    local_symbol: str | None = None
    trading_class: str | None = None
```

---

### 4.2 Target Product Resolution (`etfportfolio/ingestion/products.py`)

1. **Delete Dead Code**: Remove `_parse_product_ids_arg` and `resolve_target_ids`.
2. **Implement `resolve_target_products`**:
   - Query strictly from `bronze.contracts` (never `silver.products`).
   - Filter `blocked_exchanges` against `COALESCE(primary_exchange_id, exchange_id)`.
   - Ensure null-safety in SQL: `WHERE (COALESCE(primary_exchange_id, exchange_id) IS NULL OR COALESCE(primary_exchange_id, exchange_id) NOT IN (...))`.
   - **Fail-Fast Check**: If 0 rows return, check `SELECT COUNT(*) FROM bronze.contracts`.
     - If count is 0: raise `RuntimeError("bronze.contracts is empty. Run 'ingest contracts' first to qualify products.")`.
     - If count is $> 0$: log info (`"All products were excluded by blocked_exchanges."`) and return `[]`.

```python
from etfportfolio.ingestion.utils import ProductContract

def resolve_target_products(conn: duckdb.DuckDBPyConnection) -> list[ProductContract]:
    """Select active qualified products from bronze.contracts, excluding blocked exchanges."""
    blocked = settings.blocked_exchanges
    query = """
    SELECT
        product_id,
        symbol,
        sec_type,
        exchange_id,
        primary_exchange_id,
        currency,
        local_symbol,
        trading_class
    FROM bronze.contracts
    """
    params: list[Any] = []
    if blocked:
        placeholders = ", ".join(f"${i + 1}" for i in range(len(blocked)))
        query += f" WHERE (COALESCE(primary_exchange_id, exchange_id) IS NULL OR COALESCE(primary_exchange_id, exchange_id) NOT IN ({placeholders}))"
        params.extend(blocked)

    query += " ORDER BY product_id"
    rows = conn.execute(query, params).fetchall()

    if not rows:
        total_contracts = conn.execute("SELECT COUNT(*) FROM bronze.contracts").fetchone()[0]
        if total_contracts == 0:
            raise RuntimeError("bronze.contracts is empty. Run 'ingest contracts' first to qualify products.")
        logger.info("All products were excluded by blocked_exchanges.")
        return []

    return [
        ProductContract(
            product_id=row[0],
            symbol=row[1],
            sec_type=row[2],
            exchange_id=row[3],
            primary_exchange_id=row[4],
            currency=row[5],
            local_symbol=row[6],
            trading_class=row[7],
        )
        for row in rows
    ]
```

---

### 4.3 Price Ingestion Refactoring (`etfportfolio/ingestion/prices.py`)

#### A. Duration Formatting
Implement `format_duration(days: int) -> str` to prevent IB Error 321:

```python
import math

def format_duration(days: int) -> str:
    """Format duration string for IB reqHistoricalDataAsync.
    
    Days <= 365 use 'D' (minimum 10 D for margin).
    Days > 365 must use 'Y' (up to 30 Y).
    """
    if days <= 365:
        return f"{max(days, 10)} D"
    years = math.ceil(days / 365.25)
    return f"{min(years, 30)} Y"
```

#### B. Status Tracking & Python Freshness Evaluation
Define `PriceSeriesStatus` and implement status recording and loading:

```python
@dataclass(frozen=True)
class PriceSeriesStatus:
    last_date: datetime | None
    last_updated: datetime | None        # MAX(bronze.prices.updated_at)
    last_checked_at: datetime | None     # MAX(bronze.price_status.last_checked_at)
    status: str | None                   # 'ok', 'no_data', 'error', or None


def _record_price_status(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    status: str,
    error_message: str | None = None,
) -> None:
    now = datetime.now(UTC).replace(tzinfo=None)
    truncated_msg = error_message[:500] if error_message else None
    conn.execute(
        """
        INSERT INTO bronze.price_status (product_id, last_checked_at, status, error_message)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (product_id) DO UPDATE SET
            last_checked_at = EXCLUDED.last_checked_at,
            status = EXCLUDED.status,
            error_message = EXCLUDED.error_message
        """,
        [product_id, now, status, truncated_msg],
    )


def _load_price_series_status(conn: duckdb.DuckDBPyConnection) -> dict[int, PriceSeriesStatus]:
    """Load product_id -> PriceSeriesStatus from bronze tables."""
    rows = conn.execute(
        """
        SELECT
            c.product_id,
            MAX(pr.date) AS last_date,
            MAX(pr.updated_at) AS last_updated,
            MAX(ps.last_checked_at) AS last_checked_at,
            MAX(ps.status) AS status
        FROM bronze.contracts c
        LEFT JOIN bronze.prices pr ON c.product_id = pr.product_id
        LEFT JOIN bronze.price_status ps ON c.product_id = ps.product_id
        GROUP BY c.product_id
        """
    ).fetchall()
    return {
        row[0]: PriceSeriesStatus(
            last_date=row[1],
            last_updated=row[2],
            last_checked_at=row[3],
            status=row[4],
        )
        for row in rows
    }


def is_series_fresh(
    status: PriceSeriesStatus | None,
    target_date: datetime,
    hours: float,
) -> bool:
    """Evaluate price freshness with differentiated status dampening.
    
    1. Fresh if price bars reach target_date (yesterday).
    2. Fresh if checked within hours and status was 'ok' or 'no_data'.
    3. Fresh if last prices were updated within hours (fallback).
    4. An 'error' status is NEVER fresh; retried on subsequent runs.
    """
    if not status:
        return False
    if status.last_date is not None and status.last_date >= target_date:
        return True
    if status.status in ("ok", "no_data") and is_fresh(status.last_checked_at, hours):
        return True
    if status.status != "error" and is_fresh(status.last_updated, hours):
        return True
    return False
```

#### C. Fetch and Store Workflow (`_fetch_and_store`)
Update `_fetch_and_store` to handle both initial and incremental paths, recording status without wiping data on empty responses:

```python
async def _fetch_and_store(
    worker: AsyncDbWorker,
    ib: Any,
    product: ProductContract,
    force: bool = False,
) -> None:
    """Fetch and store prices for one product.
    
    force=True bypasses the freshness gate in target selection; products with
    existing bars still fetch incrementally with overlap validation.
    """
    last_date = await worker.submit(_get_last_date, product.product_id)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    yesterday = today - timedelta(days=1)

    # Initial fetch (no existing bars in bronze.prices)
    if last_date is None:
        duration = "30 Y"
        bars = await _fetch_historical(ib, product, duration, end_datetime="")
        new_bars = _extract_bars(bars, max_date=yesterday)
        if new_bars:
            await worker.submit(replace_series, PRICES_SPEC, product.product_id, new_bars, archive=False)
            await worker.submit(_record_price_status, product.product_id, "ok", None)
            logger.info("Product %d: full price refetch complete (%d bars)", product.product_id, len(new_bars))
        else:
            await worker.submit(_record_price_status, product.product_id, "no_data", None)
            logger.warning("Product %d: no price bars returned", product.product_id)
        return

    # Incremental fetch
    gap_days = (today - last_date).days
    duration = format_duration(gap_days + 7 + 2)
    bars = await _fetch_historical(ib, product, duration, end_datetime="")
    new_bars = _extract_bars(bars, max_date=yesterday)

    if not new_bars:
        # Existing price bars remain intact
        await worker.submit(_record_price_status, product.product_id, "no_data", None)
        logger.info("Product %d: no price bars returned for incremental update", product.product_id)
        return

    valid, mismatch_type = await worker.submit(validate_overlap, PRICES_SPEC, product.product_id, new_bars, last_date)

    if not valid:
        logger.warning(
            "Product %d: %s detected. Replacing with full refetch and archiving...",
            product.product_id,
            mismatch_type,
        )
        full_bars_raw = await _fetch_historical(ib, product, "30 Y", end_datetime="")
        full_bars = _extract_bars(full_bars_raw, max_date=yesterday)
        if full_bars:
            await worker.submit(
                replace_series,
                PRICES_SPEC,
                product.product_id,
                full_bars,
                archive=True,
                reason=mismatch_type,
            )
            await worker.submit(_record_price_status, product.product_id, "ok", None)
            logger.info("Product %d: mismatch refetch archived and replaced (%d bars)", product.product_id, len(full_bars))
        else:
            await worker.submit(_record_price_status, product.product_id, "no_data", None)
            logger.warning("Product %d: full refetch returned no bars after mismatch. Preserving existing rows.", product.product_id)
        return

    # Overlap validated: persist validated window and tail
    overlap_start = overlap_start_for(last_date)
    points_to_store = {d: pt for d, pt in new_bars.items() if d >= overlap_start}
    await worker.submit(upsert_series, PRICES_SPEC, product.product_id, points_to_store)
    await worker.submit(_record_price_status, product.product_id, "ok", None)
    logger.info("Product %d: incremental price update complete (%d bars)", product.product_id, len(points_to_store))
```

#### D. Runner Loop & Error Capture (`_run_price_ingestion`)
Catch unexpected exceptions per-product to record `status = 'error'`, while re-raising `IBConnectionError` immediately to abort if the gateway connection drops:

```python
async def _run_price_ingestion(force: bool = False) -> int:
    async with AsyncDbWorker(settings.db_path) as worker:
        from etfportfolio.ingestion.products import resolve_target_products

        products = await worker.submit(resolve_target_products)
        if not products:
            return 0

        if force:
            to_process = products
        else:
            status_cache = await worker.submit(_load_price_series_status)
            today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
            yesterday = today - timedelta(days=1)
            to_process = [
                p
                for p in products
                if not is_series_fresh(status_cache.get(p.product_id), yesterday, settings.freshness_window_hours)
            ]

        skipped = len(products) - len(to_process)
        if skipped:
            console.info(f"{skipped}/{len(products)} products up to date, {len(to_process)} to process.")
        else:
            console.info(f"Processing {len(products)} product(s)...")

        if not to_process:
            console.info("Done. All products are up to date.")
            return 0

        logger.info("Price ingestion: %d products to process", len(to_process))

        async with ib_connection(client_id=2) as ib:
            with progress_bar(len(to_process), desc="Prices") as bar:
                for product in to_process:
                    bar.set_postfix_str(str(product.product_id))
                    if not ib.isConnected():
                        raise IBConnectionError("IB Gateway connection was lost during price ingestion.")
                    try:
                        await _fetch_and_store(worker, ib, product, force)
                    except IBConnectionError:
                        raise
                    except Exception as e:
                        logger.error("Failed to fetch prices for product %d: %s", product.product_id, e)
                        await worker.submit(_record_price_status, product.product_id, "error", str(e))
                    finally:
                        bar.update(1)

        return len(to_process)
```

---

### 4.4 Contract Qualification Refactoring (`etfportfolio/ingestion/contracts.py`)

Simplify `contracts.py` to remove `limit` and `product_ids`:

1. `_select_target_product_ids(conn: duckdb.DuckDBPyConnection) -> list[int]`:
   ```python
   def _select_target_product_ids(conn: duckdb.DuckDBPyConnection) -> list[int]:
       """Return all product IDs from bronze.products."""
       rows = conn.execute("SELECT product_id FROM bronze.products ORDER BY product_id").fetchall()
       return [row[0] for row in rows]
   ```
2. Update runner functions:
   ```python
   async def _run_contract_qualification(force: bool = False) -> int: ...
   async def sync(force: bool = False) -> int: ...
   ```

---

### 4.5 Pipeline & CLI Simplification (`etfportfolio/ingestion/pipeline.py`)

1. **Details Phase**: Derive target product IDs from `resolve_target_products`:
   ```python
   async def _run_details_only(force: bool = False) -> None:
       async with AsyncDbWorker(settings.db_path) as worker:
           client, account_id = await session.ensure_session()
           console.info(f"Session OK. Active account: {account_id}")
           try:
               target_products = await worker.submit(products.resolve_target_products)
               target_ids = [p.product_id for p in target_products]
               await _run_details_phase(worker, client, account_id, target_ids, force)
           finally:
               await client.aclose()
   ```
2. **Full Pipeline (`_run_full`)**:
   - `Phase 2`: `await contracts.sync(force=force)`
   - `Phase 3`: `await prices.sync(force=force)`
   - `Phase 6`:
     ```python
     async with AsyncDbWorker(settings.db_path) as worker:
         target_products = await worker.submit(products.resolve_target_products)
         target_ids = [p.product_id for p in target_products]
         await _run_details_phase(worker, client, account_id, target_ids, force)
     ```
3. **Ingest CLI Interface**:
   ```python
   class Ingest:
       """CLI surface for the ingestion pipeline: `main.py ingest <phase>`."""

       def __call__(self, force: bool = False) -> None:
           asyncio.run(_run_full(force=force))

       def session(self) -> None:
           account_id = asyncio.run(_run_session())
           console.info(f"Session OK. Active account: {account_id}")

       def products(self, force: bool = False) -> None:
           count = asyncio.run(products.sync(force=force))
           console.info(f"Product sync complete. Total products synced: {count}")

       def contracts(self, force: bool = False) -> None:
           count = asyncio.run(contracts.sync(force=force))
           console.info(f"Contract qualification complete. {count} products processed.")

       def prices(self, force: bool = False) -> None:
           count = asyncio.run(prices.sync(force=force))
           console.info(f"Price series complete. {count} products processed.")

       def themes(self, force: bool = False) -> None:
           p_count, n_count = asyncio.run(_run_themes(force=force))
           console.info(f"Theme taxonomy synced: {p_count} parents, {n_count} nodes.")

       def details(self, force: bool = False) -> None:
           asyncio.run(_run_details_only(force=force))
   ```

---

## 5. Verification & Acceptance Criteria

### 5.1 Verification Scenarios

| Scenario | Input Condition | Expected Behavior |
| :--- | :--- | :--- |
| **Exchange Blacklisting** | `blocked_exchanges = ["SFB", "EBS"]` | Products where `COALESCE(primary_exchange_id, exchange_id)` is `'SFB'` or `'EBS'` are absent from `resolve_target_products()`. Neither `prices` nor `details` executes requests for them. |
| **Fail-Fast Empty Check** | `bronze.contracts` has 0 rows | `resolve_target_products()` raises `RuntimeError("bronze.contracts is empty. Run 'ingest contracts' first to qualify products.")`. |
| **All Blocked Logging** | All contracts match `blocked_exchanges` | `resolve_target_products()` logs `"All products were excluded by blocked_exchanges."` and returns `[]` without raising. |
| **No-Data Dampening** | Product query returns 0 bars | `bronze.price_status` records `status = 'no_data'` and `error_message = NULL`. A second run within 24 hours skips the product. |
| **Preservation of Prices on 0 Bars** | Product has existing bars; incremental fetch returns 0 bars | Existing rows in `bronze.prices` remain unchanged. `bronze.price_status` updates to `'no_data'`. |
| **Error Retry Exemption** | Request raises `Exception("Timeout")` | `bronze.price_status` records `status = 'error'`, `error_message = 'Timeout'`. A second run immediately re-queries this product (no 24h lockout). |
| **IB Duration Format** | Incremental gap is 15 days | Duration sent to IB is `"24 D"`. |
| **IB Duration Yearly Cap** | Incremental gap is 400 days | Duration sent to IB is `"2 Y"` (never `"409 D"`). |
| **Force Flag Freshness Bypass** | `main.py ingest prices --force` | Bypasses 24h freshness check in target selection. Products with existing bars fetch incrementally; products without bars fetch `"30 Y"`. |
| **CLI Signatures** | CLI commands invoked | Only `--force` / `-f` accepted. Invoking with `--limit` or `--product_ids` fails cleanly at CLI parser level. |

### 5.2 Test Import Updates
Any existing unit or integration tests that imported `PRICES_SPEC`, `validate_overlap`, `replace_series`, `upsert_series`, or `is_series_fresh` from `etfportfolio.ingestion.utils` must be updated to import from `etfportfolio.ingestion.prices`. Test references importing `ProductContract` import it from `etfportfolio.ingestion.utils`.