# Functional Design Report (FDR): Refactoring & Alignment for `etfportfolio`

**Document Status:** Final / Approved  
**Scope:** Bronze & Silver Medallion Tiers (`etfportfolio/core`, `etfportfolio/ingest`, `etfportfolio/prep`, `tests/`, `pyproject.toml`, `README.md`)  
**Guiding Priorities:** Data Accuracy & Correctness >> Codebase Simplicity & Clarity > Storage Efficiency > Runtime Performance >> Security  

## 1. Executive Summary & Intent

This document establishes the implementation blueprint for refactoring the Bronze and Silver tiers of the `etfportfolio` data system. It resolves verified concurrency defects, relational view inaccuracies, cold-storage purge flaws, static type errors, and transformation routing tight-coupling.

Per architectural consensus, all designs follow a strict principle of **simplicity over speculative abstraction**. Defenses against edge cases are kept lean and direct. Components deemed functional and stable for the single-user execution model are explicitly bounded and preserved without modification.

## 2. Core Architectural Principles & Scope Boundaries

1. **Bronze & Silver Boundary:** The scope is strictly limited to the ingestion (`bronze`) and observation extraction (`silver`) tiers. Downstream Gold-layer analytical pipelines (monthly LOCF panels, factor return series, cross-sectional factor regressions, and efficient-frontier optimization) are intentionally deferred to a future phase.
2. **Monolithic Ingest Retention:** `etfportfolio/ingest/session.py` and `etfportfolio/ingest/prices.py` remain single files. They must not be split into sub-packages or helper modules.
3. **Network-Paced Database Execution:** Existing row-by-row and chunked database execution loops in `prices.py`, `contracts.py`, and `themes.py` are retained as-is. Upstream Interactive Brokers (IB) Gateway connections are sensitive to socket delays; preserving current execution rhythms prevents connection resets.
4. **Zero Index Overhead on Bronze Snapshots:** No secondary indexes may be added to `bronze.snapshots` or adjacent bronze tables to conserve memory and disk footprint.
5. **Strict Fail-Fast Extraction:** Silver observation extraction will not introduce error-quarantine tables or silent error masking. If an unhandled payload variation or malformed field appears, the extraction process fails fast, logging the exception to disk so data anomalies are surfaced and addressed directly.
6. **Disposable Scripts:** Standalone utility scripts in `scripts/` (`migrate_snapshots_changelog.py`, `migration_price_status.py`, `restore_truncated_prices.py`, `log_check.py`) are temporary, disposable artifacts and must be ignored during refactoring.

## 3. Subsystem Specifications & Implementation Blueprint

### 3.1. Database Concurrency & Worker Lifecycle

#### Files Affected
- `etfportfolio/core/db.py`
- `etfportfolio/ingest/landing.py`

#### Rationale & Mechanism
1. **Thread Affinity Violation:** DuckDB connection handles are thread-local. In the existing code, `AsyncDbWorker.__init__` opened the connection (`duckdb.connect()`) and ran migrations (`apply_schema()`) directly on the calling thread (the asyncio event-loop thread). The connection handle was then passed to `self._thread` to execute queries in `_run()`. This violates DuckDB’s threading model and causes synchronous disk I/O to block the asyncio event loop during initialization.
2. **CPU Serialization on DB Queue:** In `landing.py`, `content_address` (which only computes `xxhash` and `zstd` compression on in-memory JSON) was wrapped in a dummy callable with an unused `conn` parameter and dispatched to `AsyncDbWorker.submit()`. This forced purely in-memory CPU calculations onto the single-threaded FIFO database queue, stalling actual DuckDB reads and writes.

#### Implementation Specification
1. **Move Connection Acquisition and Schema Setup Inside the Worker Thread:**
   - Update `AsyncDbWorker.__init__` to only initialize internal queue and thread objects. It must **not** call `duckdb.connect()` or `apply_schema()`.
   - Update `AsyncDbWorker._run()` so that the connection is opened and schema applied **inside the background thread**:
     ```python
     def _run(self) -> None:
         Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
         self._conn = duckdb.connect(self._db_path)
         apply_schema(self._conn)
         self._ready_event.set()  # Signal ready to asyncio loop

         while True:
             item = self._queue.get()
             if item is None:
                 break
             func, args, kwargs, future = item
             try:
                 result = func(self._conn, *args, **kwargs)
                 if future is not None:
                     self._loop.call_soon_threadsafe(future.set_result, result)
             except Exception as exc:
                 logger.exception("Error in AsyncDbWorker task")
                 if future is not None:
                     self._loop.call_soon_threadsafe(future.set_exception, exc)

         if self._conn is not None:
             self._conn.close()
             self._conn = None
     ```
   - In `AsyncDbWorker.__aenter__`, wait for `self._ready_event` before returning `self`.
2. **Decouple CPU Hashing/Compression from the Database Worker:**
   - In `etfportfolio/ingest/landing.py`, remove the dummy wrapper `_content_address(conn: duckdb.DuckDBPyConnection, payload: dict)`.
   - In `landing.py:fetch_and_gate`, call `content_address(payload or {})` directly in Python:
     ```python
     digest, compressed = content_address(payload or {})
     ```
   - Reserve `AsyncDbWorker.submit()` exclusively for operations interacting with DuckDB.

### 3.2. Relational Modeling: `silver.products` View Realignment

#### Files Affected
- `etfportfolio/core/schema.sql`

#### Rationale & Mechanism
`bronze.products` is a dirty discovery table populated from broad web-search scraping. `bronze.contracts` contains verified, qualified instruments returned by the official IBKR API. However, not all qualified contracts have operable price series or usable snapshot details. Downstream factor analytics in Silver and Gold strictly require instruments that possess both price series and fundamental observations. 

The previous `silver.products` view inner-joined `bronze.products` and `bronze.contracts`, dropping unqualified products prematurely from discovery queries while failing to ensure data-readiness for downstream analytics.

#### Implementation Specification
1. Redefine `silver.products` in `etfportfolio/core/schema.sql` as a view over `bronze.contracts`, filtered strictly by the presence of prices in `bronze.prices` and the presence of extracted observations in either `silver.product_metrics` or `silver.product_dimensions`:
   ```sql
   CREATE OR REPLACE VIEW silver.products AS
   SELECT
       c.product_id,
       c.name,
       c.symbol,
       c.local_symbol,
       c.sec_type,
       COALESCE(c.exchange_id, 'SMART') AS exchange_id,
       c.primary_exchange_id,
       c.currency,
       c.trading_class,
       c.valid_exchanges,
       c.stock_type,
       c.isin,
       c.cusip,
       c.time_zone_id,
       c.min_tick,
       c.created_at,
       c.updated_at
   FROM bronze.contracts c
   WHERE c.product_id IN (SELECT DISTINCT product_id FROM bronze.prices)
     AND (
         c.product_id IN (SELECT DISTINCT product_id FROM silver.product_metrics)
         OR c.product_id IN (SELECT DISTINCT product_id FROM silver.product_dimensions)
     );
   ```
2. **Target Resolution Invariant:** Confirm that candidate resolution for ingestion phases (`prices.py`, `details.py`) continues to query `bronze.contracts` via `resolve_target_products()`, guaranteeing ingestion discovery is decoupled from the downstream data-readiness filter in `silver.products`.

### 3.3. Cold Storage Cleanup: Null-Safe Mismatch Detection

#### Files Affected
- `etfportfolio/ingest/clean.py`

#### Rationale & Mechanism
In `clean_cold_storage`, archived price runs are evaluated against `bronze.prices`. If zero historical bars differ outside of a recent settlement buffer, the cold-storage run is deemed a duplicate archive and deleted.

However, `open`, `high`, `low`, and `volume` are nullable columns. In SQL, any arithmetic expression involving `NULL` (`abs(c.open - b.open) > ?`) evaluates to `NULL` (falsy in a `WHERE` clause). If a data restatement introduced or removed `NULL`s where values previously existed, the mismatch expression evaluated to `NULL`, resulting in a mismatch count of 0. Consequently, valid cold-storage archives of corporate actions were erroneously deleted.

#### Implementation Specification
Update the mismatch check in `etfportfolio/ingest/clean.py` so comparisons explicitly handle `NULL` states using DuckDB's `IS DISTINCT FROM` and explicit null guards:

```sql
SELECT count(*)
FROM cold_storage.prices c
LEFT JOIN bronze.prices b ON c.product_id = b.product_id AND c.date = b.date
WHERE c.product_id = ? AND c.run_id = ? AND c.date <= ?
AND (
    b.date IS NULL
    OR (c.open IS NULL AND b.open IS NOT NULL)
    OR (c.open IS NOT NULL AND b.open IS NULL)
    OR (c.open IS NOT NULL AND b.open IS NOT NULL AND (abs(c.open - b.open) > ? OR abs(c.open - b.open) / nullif(abs(c.open), 0) > ?))
    OR (c.high IS NULL AND b.high IS NOT NULL)
    OR (c.high IS NOT NULL AND b.high IS NULL)
    OR (c.high IS NOT NULL AND b.high IS NOT NULL AND (abs(c.high - b.high) > ? OR abs(c.high - b.high) / nullif(abs(c.high), 0) > ?))
    OR (c.low IS NULL AND b.low IS NOT NULL)
    OR (c.low IS NOT NULL AND b.low IS NULL)
    OR (c.low IS NOT NULL AND b.low IS NOT NULL AND (abs(c.low - b.low) > ? OR abs(c.low - b.low) / nullif(abs(c.low), 0) > ?))
    OR (c.close IS NULL AND b.close IS NOT NULL)
    OR (c.close IS NOT NULL AND b.close IS NULL)
    OR (c.close IS NOT NULL AND b.close IS NOT NULL AND (abs(c.close - b.close) > ? OR abs(c.close - b.close) / nullif(abs(c.close), 0) > ?))
)
```

### 3.4. Extractor Registry: Decoupling from Provider URLs

#### Files Affected
- `etfportfolio/prep/extractors.py`
- `etfportfolio/prep/pipeline.py`

#### Rationale & Mechanism
`prep/extractors.py` used vendor-specific URL paths (e.g. `"/tws.proxy/fundamentals/mf_ratios_fundamentals/"`) as keys in `EXTRACTOR_REGISTRY`. This tightly coupled observation parsing to IBKR's internal routing structure. `etfportfolio/ingest/endpoints.py` already defines canonical endpoint names (`ratios`, `profile`, `esg`, `mstar`, `lipper`, `holdings`, `theme_weights`). Re-keying the registry to canonical names cleanly decouples data transformation from transport URL definitions.

#### Implementation Specification
1. **Re-key Extractor Registry:**
   In `etfportfolio/prep/extractors.py`, key `EXTRACTOR_REGISTRY` by canonical endpoint name:
   ```python
   EXTRACTOR_REGISTRY: dict[str, Callable[[int, dict[str, Any], datetime], ExtractionResult]] = {
       "ratios": extract_ratios,
       "profile": extract_profile,
       "esg": extract_esg,
       "mstar": extract_mstar,
       "lipper": extract_lipper,
       "holdings": extract_holdings,
       "theme_weights": extract_theme_weights,
   }
   ```
2. **Resolve Canonical Name in Prep Pipeline:**
   In `etfportfolio/prep/pipeline.py`, map the snapshot's `url_prefix` to its canonical endpoint name using `ENDPOINTS` from `etfportfolio.ingest.endpoints`:
   ```python
   from etfportfolio.ingest.endpoints import ENDPOINTS

   URL_PREFIX_TO_NAME: dict[str, str] = {ep.url_prefix: ep.name for ep in ENDPOINTS}
   ```
   When dispatching snapshots:
   ```python
   ep_name = URL_PREFIX_TO_NAME.get(url_prefix)
   extractor = EXTRACTOR_REGISTRY.get(ep_name) if ep_name else None
   if extractor is not None:
       result = extractor(product_id, data, created_at)
   ```

### 3.5. Timestamp Evaluation Standardization

#### Files Affected
- `etfportfolio/ingest/contracts.py`

#### Rationale & Mechanism
In `contracts.py:upsert_contract`, `datetime.now(UTC)` was evaluated twice inside a single list comprehension:
```python
values = list(data.values()) + [datetime.now(UTC), datetime.now(UTC)]
```
This causes microsecond timing disparities between `created_at` and `updated_at` on the initial row insertion.

#### Implementation Specification
Evaluate `datetime.now(UTC)` once into a local variable and reuse it for both columns:
```python
now = datetime.now(UTC)
values = list(data.values()) + [now, now]
```

### 3.6. Elimination of Obsolete Compatibility Aliases

#### Files Affected
- `etfportfolio/ingest/prices.py`

#### Rationale & Mechanism
Project policy mandates: *"Delete obsolete logic and tests outright; do not add compatibility shims or fallback layers."* Backward compatibility aliases in `prices.py` violate this rule.

#### Implementation Specification
Remove lines 38–41 in `etfportfolio/ingest/prices.py`:
```python
# REMOVE:
REL_TOL = PRICE_REL_TOL  # Backward compatibility alias
ABS_TOL = PRICE_ABS_TOL  # Backward compatibility alias
```
Verify that all references throughout the package use `PRICE_REL_TOL` and `PRICE_ABS_TOL`.

### 3.7. Static Typing, Dependencies & HTTP Client Hygiene

#### Files Affected
- `pyproject.toml`
- `etfportfolio/ingest/session.py`
- `etfportfolio/ingest/clean.py`
- `tests/core/test_config.py`
- `tests/core/test_db.py`
- `tests/prep/test_pipeline.py`

#### Implementation Specification
1. **Move `tqdm` to Production Dependencies:**
   In `pyproject.toml`, relocate `"tqdm>=4.70.0"` from `[dependency-groups] dev` into `[project] dependencies`. `etfportfolio/core/progress.py` imports `tqdm` unconditionally, which causes `ModuleNotFoundError` in non-dev environments (`uv sync --no-dev`).
2. **Deduplicate Client Construction in `login()`:**
   In `etfportfolio/ingest/session.py:login()`, eliminate lines 610–621 (the manual inline `httpx.AsyncClient` instantiation). Replace with:
   ```python
   async with build_async_client(timeout=10.0) as test_client:
   ```
3. **Resolve `pyright` Type Checker Diagnostics:**
   - **`None` Subscripting:** Guard DuckDB `.fetchone()` calls before subscripting in `clean.py`, `test_db.py`, and `test_pipeline.py`.
     ```python
     row = conn.execute(...).fetchone()
     assert row is not None
     val = row[0]
     ```
   - **Settings Constructor in Tests:** In `tests/core/test_config.py`, replace `Settings(_env_file=None)` with standard Pydantic v2 test configuration patterns (e.g. passing explicit field values or using `monkeypatch.delenv`).

## 4. Architectural Decisions Preserved "As-Is"

The following components were analyzed and explicitly retained in their current design:

1. **Credentials & Account Reconcile Interactivity:**
   - Plaintext credentials (`IBKR_USERNAME`, `IBKR_PASSWORD`, `ACCOUNT_ID`) stored in the uncommitted `.env` file are retained.
   - Synchronous terminal `input()` for interactive account ID confirmation is retained.
   - In-place mutation of the `settings` singleton upon discovering the live account ID during login or probe is retained as necessary startup behavior for the single-user execution model.
2. **Strict Fail-Fast Extraction:**
   - `prep/pipeline.py` will not introduce error quarantine tables (`silver.failed_snapshots`) or catch-and-continue logic. If a payload violates extractor assumptions, it raises immediately, halting the batch to preserve data integrity and prompt an immediate fix.
3. **Price Freshness Calendar:**
   - The calendar calculation `yesterday = today - timedelta(days=1)` in `prices.py` is maintained as-is without trading-calendar libraries.
4. **Currency Handling Scope:**
   - `silver.product_metrics` retains monetary values as local unscaled floats, with the original denomination preserved in `raw_value`. Exchange rate normalization is deferred to future Gold-tier factor preparation.
5. **CLI Surface:**
   - Unimplemented flags (`--limit`, `--product-ids`) documented in `README.md` are removed from the documentation. The Fire CLI signature remains bounded to `force: bool = False`.

## 5. Verification & Acceptance Criteria

To declare the refactoring complete, an implementing agent must verify the following:

1. **Static Typing & Formatting:**
   - `uv run pyright` passes with **0 errors**.
   - `uv run ruff check .` passes with **0 errors**.
2. **Unit & Integration Test Suite:**
   - `uv run pytest` passes cleanly, with tests covering updated `AsyncDbWorker` lifecycle, null-safe cold storage cleanup, and canonical extractor dispatching.
3. **Ingestion and Silver Sanity Execution:**
   - `uv run python main.py ingest --force` runs through product discovery, qualification, prices, and details without DuckDB thread affinity errors or event-loop stalls.
   - `uv run python main.py prep --force` successfully extracts observations into `silver.product_metrics` and `silver.product_dimensions`.
   - `silver.products` view returns only records that possess both price records and observation records.