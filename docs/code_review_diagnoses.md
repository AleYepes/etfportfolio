# Diagnostic Code Review Report: etfportfolio

This report provides a comprehensive, self-contained diagnostic analysis of the `etfportfolio` repository. It synthesizes findings across architectural structure, database interaction, concurrency, data integrity, scraper resilience, and code hygiene.

Per review instructions, this document contains **only diagnoses and rationale** (the exact nature, mechanism, and impact of each issue), with **zero prescriptions, remediation steps, or recommendations**.

---

## Table of Contents

1. [Architecture & Domain Scope](#1-architecture--domain-scope)
   - [Issue 1: Absence of Gold Layer and Downstream Factor Analytics](#issue-1-absence-of-gold-layer-and-downstream-factor-analytics)
   - [Issue 2: Inner Join in `silver.products` View Silently Drops Unqualified Products](#issue-2-inner-join-in-silverproducts-view-silently-drops-unqualified-products)
   - [Issue 3: Loss of Currency Normalization in Financial Metrics](#issue-3-loss-of-currency-normalization-in-financial-metrics)
   - [Issue 4: Extractor Registry Tightly Coupled to Upstream Provider URLs](#issue-4-extractor-registry-tightly-coupled-to-upstream-provider-urls)
   - [Issue 5: Oversized Modules with Tangled Responsibilities](#issue-5-oversized-modules-with-tangled-responsibilities)
2. [Concurrency, Threading & Resource Lifecycle](#2-concurrency-threading--resource-lifecycle)
   - [Issue 6: `AsyncDbWorker` Premature Resource Acquisition in Constructor](#issue-6-asyncdbworker-premature-resource-acquisition-in-constructor)
   - [Issue 7: Cross-Thread Connection Sharing and Event-Loop Blocking in `AsyncDbWorker`](#issue-7-cross-thread-connection-sharing-and-event-loop-blocking-in-asyncdbworker)
   - [Issue 8: CPU-Bound Hashing and Compression Serialized onto the Database Worker](#issue-8-cpu-bound-hashing-and-compression-serialized-onto-the-database-worker)
   - [Issue 9: Blocking Synchronous `input()` Executed Inside Async Event Loop](#issue-9-blocking-synchronous-input-executed-inside-async-event-loop)
   - [Issue 10: Rigid Worker Signature Forcing Dummy `conn` Parameters](#issue-10-rigid-worker-signature-forcing-dummy-conn-parameters)
3. [Database & Query Performance](#3-database--query-performance)
   - [Issue 11: Missing Compound Index on `bronze.snapshots` Leading to Full Table Scans](#issue-11-missing-compound-index-on-bronzesnapshots-leading-to-full-table-scans)
   - [Issue 12: Single-Row SQL Execution Loop for Time-Series Ingestion](#issue-12-single-row-sql-execution-loop-for-time-series-ingestion)
   - [Issue 13: $O(N)$ Serial Thread-Queue Round-Trips for Contract Freshness](#issue-13-on-serial-thread-queue-round-trips-for-contract-freshness)
   - [Issue 14: Manual Existence Checking and Procedural Updates in `themes.py`](#issue-14-manual-existence-checking-and-procedural-updates-in-themespy)
   - [Issue 15: Dual Clock Sources and Mixed Datetime Awareness](#issue-15-dual-clock-sources-and-mixed-datetime-awareness)
   - [Issue 16: Mixed SQL Parameter Placeholder Syntax](#issue-16-mixed-sql-parameter-placeholder-syntax)
4. [Data Integrity & Scraper Resilience](#4-data-integrity--scraper-resilience)
   - [Issue 17: Complete Lack of Error Isolation in Silver Extraction Pipeline](#issue-17-complete-lack-of-error-isolation-in-silver-extraction-pipeline)
   - [Issue 18: Inconsistent Error Handling Contracts Among Utility Parsers](#issue-18-inconsistent-error-handling-contracts-among-utility-parsers)
   - [Issue 19: Null Value Masking in Cold Storage Cleanup SQL](#issue-19-null-value-masking-in-cold-storage-cleanup-sql)
   - [Issue 20: Weekend Calendar Blindness in Price Freshness Calculation](#issue-20-weekend-calendar-blindness-in-price-freshness-calculation)
5. [Static Typing, Dependencies & CLI Contracts](#5-static-typing-dependencies--cli-contracts)
   - [Issue 21: Static Type Checker (`pyright`) Fails with 11 Errors](#issue-21-static-type-checker-pyright-fails-with-11-errors)
   - [Issue 22: Critical Runtime Dependency Misclassified in Development Group](#issue-22-critical-runtime-dependency-misclassified-in-development-group)
   - [Issue 23: CLI Surface Drift: Documented Filtering Flags Unimplemented](#issue-23-cli-surface-drift-documented-filtering-flags-unimplemented)
   - [Issue 24: Broken Diagnostic Script Due to Namespace Mismatch](#issue-24-broken-diagnostic-script-due-to-namespace-mismatch)
   - [Issue 25: Duplicated HTTP Client Construction Logic](#issue-25-duplicated-http-client-construction-logic)
6. [Security & Project Policy Compliance](#6-security--project-policy-compliance)
   - [Issue 26: Plaintext Brokerage Credentials Stored on Disk](#issue-26-plaintext-brokerage-credentials-stored-on-disk)
   - [Issue 27: Retention of Obsolete Logic and Compatibility Aliases](#issue-27-retention-of-obsolete-logic-and-compatibility-aliases)

---

## 1. Architecture & Domain Scope

### Issue 1: Absence of Gold Layer and Downstream Factor Analytics
* **Files**: [`etfportfolio/core/schema.sql:3`](../etfportfolio/core/schema.sql#L3), [`main.py:18-24`](main.py#L18-L24), [`AGENTS.md:3-8`](AGENTS.md#L3-L8)
* **Diagnosis**:
  The repository's formal mandate is to ingest ETF data, build monthly LOCF panels, derive weighted factor returns, run factor regressions, and calculate efficient-frontier portfolios. In `schema.sql`, line 3 executes `CREATE SCHEMA IF NOT EXISTS gold;`. However, the schema definition contains **zero tables, views, or sequences** within `gold`. Furthermore, the application code contains no modules or CLI commands beyond `ingest` and `prep` (silver observations extraction).
* **Rationale**:
  The system currently terminates at the ingestion and extraction of discrete observation points in the silver tier. Downstream analytical components—specifically the temporal alignment into regular monthly panels via Last Observation Carried Forward (LOCF), cross-sectional standardization, regression of returns against factor series, and mean-variance optimization—do not exist in the codebase.

---

### Issue 2: Inner Join in `silver.products` View Silently Drops Unqualified Products
* **Files**: [`etfportfolio/core/schema.sql:153-173`](../etfportfolio/core/schema.sql#L153-L173)
* **Diagnosis**:
  The silver product view is defined as:
  ```sql
  CREATE OR REPLACE VIEW silver.products AS
  SELECT
      p.product_id,
      ...
  FROM bronze.products p
  JOIN bronze.contracts c ON p.product_id = c.product_id;
  ```
* **Rationale**:
  The view uses an `INNER JOIN` between `bronze.products` and `bronze.contracts`. If a product has been discovered during the product search phase but has not yet completed contract qualification (due to gateway timeouts, exchange blocks, pacing violations, or being newly added), that product is completely excluded from `silver.products`. Downstream queries joining against `silver.products` will treat these products as non-existent rather than partially ingested or pending qualification.

---

### Issue 3: Loss of Currency Normalization in Financial Metrics
* **Files**: [`etfportfolio/prep/extractors.py:251-266`](../etfportfolio/prep/extractors.py#L251-L266), [`etfportfolio/prep/utils.py:104-135`](../etfportfolio/prep/utils.py#L104-L135), [`etfportfolio/core/schema.sql:175-185`](../etfportfolio/core/schema.sql#L175-L185)
* **Diagnosis**:
  In `extract_profile`, net asset values (AUM) are parsed by `parse_net_assets`:
  ```python
  amt_clean = re.sub(r"[^\d.,kKmMbBtT]", "", amt_str)
  ...
  final_val = base_val * multiplier
  ```
  This parses amounts like `"$1.5B"`, `"€500M"`, or `"¥50,000M"` into bare floats. The metric is stored in `silver.product_metrics` under `metric_id = 'total_net_assets_local'`. The schema for `silver.product_metrics` has no currency column:
  ```sql
  CREATE TABLE IF NOT EXISTS silver.product_metrics (
      product_id INTEGER NOT NULL,
      source VARCHAR NOT NULL,
      metric_id VARCHAR NOT NULL,
      effective_date DATE NOT NULL,
      effective_date_source VARCHAR NOT NULL,
      fetched_at TIMESTAMP WITH TIME ZONE NOT NULL,
      value DOUBLE NOT NULL,
      raw_value VARCHAR NOT NULL,
      PRIMARY KEY (product_id, source, metric_id, effective_date)
  );
  ```
* **Rationale**:
  `silver.product_metrics` stores monetary values as unscaled numeric floats without recording the currency code. When cross-sectional analytics or size-factor weightings are computed across an international ETF universe, funds denominated in Japanese Yen (JPY), Korean Won (KRW), or British Pence (GBX) will be evaluated directly against US Dollar (USD) or Euro (EUR) figures without any exchange rate translation, causing multi-order-of-magnitude distortions in fund sizing.

---

### Issue 4: Extractor Registry Tightly Coupled to Upstream Provider URLs
* **Files**: [`etfportfolio/prep/extractors.py:647-655`](../etfportfolio/prep/extractors.py#L647-L655)
* **Diagnosis**:
  In `prep/extractors.py`, dispatching is driven by `EXTRACTOR_REGISTRY`:
  ```python
  EXTRACTOR_REGISTRY: dict[str, Callable[[int, dict[str, Any], datetime], ExtractionResult]] = {
      "/tws.proxy/fundamentals/mf_ratios_fundamentals/": extract_ratios,
      "/tws.proxy/fundamentals/mf_profile_and_fees/": extract_profile,
      "/tws.proxy/impact/esg/": extract_esg,
      "/tws.proxy/mstar/fund/detail?conid=": extract_mstar,
      "/tws.proxy/fundamentals/mf_lip_ratings/": extract_lipper,
      "/tws.proxy/fundamentals/mf_holdings/": extract_holdings,
      "/tws.proxy/knowledge-graph/ui/fund?conid=": extract_theme_weights,
  }
  ```
* **Rationale**:
  The transformation layer (`prep`) uses the raw, vendor-specific URL paths of the data provider (Interactive Brokers internal proxy paths) as the discriminator for business data extraction. If IBKR alters an endpoint path, introduces an alternate version, or if supplementary data sources (mandated in `AGENTS.md`) are integrated, the silver processing tier fails to route the payloads even if the payload schema is identical.

---

### Issue 5: Oversized Modules with Tangled Responsibilities
* **Files**: [`etfportfolio/ingest/session.py`](../etfportfolio/ingest/session.py) (646 lines), [`etfportfolio/ingest/prices.py`](../etfportfolio/ingest/prices.py) (652 lines)
* **Diagnosis**:
  Two files in `ingest` encompass divergent, mixed-responsibility subsystems:
  - `session.py` combines: Playwright headless/headed browser management, HTML cookie banner DOM traversal, 2FA prompt polling, credentials retrieval from `.env`, file I/O rewriting `.env` on disk, session health validation via HTTP GETs, account identifier heuristics parsing regexes from cookies and HTML responses, and HTTP retry wrappers.
  - `prices.py` combines: Historical price duration calculations, IBKR Gateway request orchestration, bar extraction, calendar window boundary calculations, 14-day overlap set-equality validation, uniform ratio distribution calculations for split detection, cold storage archiving, database upserts, and status table persistence.
* **Rationale**:
  High coupling within these single modules impedes granular unit testing. For example, testing the mathematical properties of split detection or overlap tolerance validation requires importing the entire gateway client, configuration, and database connection machinery.

---

## 2. Concurrency, Threading & Resource Lifecycle

### Issue 6: `AsyncDbWorker` Premature Resource Acquisition in Constructor
* **Files**: [`etfportfolio/core/db.py:55-66`](../etfportfolio/core/db.py#L55-L66)
* **Diagnosis**:
  `AsyncDbWorker` is designed as an asynchronous context manager (`async with AsyncDbWorker(...) as worker:`). However, resource allocation and background thread execution take place in `__init__`:
  ```python
  def __init__(self, db_path: str):
      self._db_path = db_path
      self._loop = asyncio.get_running_loop()
      self._queue: queue.Queue = queue.Queue()
      self._thread = threading.Thread(target=self._run, daemon=True)
      self._conn: duckdb.DuckDBPyConnection | None = None

      Path(db_path).parent.mkdir(parents=True, exist_ok=True)
      self._conn = duckdb.connect(db_path)
      apply_schema(self._conn)

      self._thread.start()
  ```
  `__aenter__` merely returns `self`.
* **Rationale**:
  If an exception is raised after `AsyncDbWorker` instantiation but prior to entering the `async with` block (or if initialization of another resource fails in a compound setup), `__aexit__` is never called. Consequently, the background thread remains running, the DuckDB file lock is held open indefinitely by `self._conn`, and resources leak. Standard context manager protocol dictates that resource acquisition occurs in `__aenter__`.

---

### Issue 7: Cross-Thread Connection Sharing and Event-Loop Blocking in `AsyncDbWorker`
* **Files**: [`etfportfolio/core/db.py:63-66`](../etfportfolio/core/db.py#L63-L66), [`etfportfolio/core/db.py:68-88`](../etfportfolio/core/db.py#L68-L88)
* **Diagnosis**:
  In `AsyncDbWorker.__init__`, `duckdb.connect(db_path)` and `apply_schema(self._conn)` run directly on the calling thread (the thread executing the `asyncio` event loop). Once initialized, the connection `self._conn` is handed over to `self._thread`, which executes queries inside `_run()`:
  ```python
  result = func(self._conn, *args, **kwargs)
  ```
* **Rationale**:
  1. **Thread affinity**: DuckDB connection handles are thread-local. Creating a connection on Thread A (the event loop) and querying it continuously on Thread B (the worker thread) violates DuckDB's threading guidelines.
  2. **Event loop stall**: Executing `duckdb.connect()` and running the multi-statement schema migration `apply_schema()` synchronously within `__init__` performs disk I/O and acquires file-level locks directly on the asyncio loop, blocking all concurrent asynchronous networking tasks.

---

### Issue 8: CPU-Bound Hashing and Compression Serialized onto the Database Worker
* **Files**: [`etfportfolio/ingest/landing.py:13-14`](../etfportfolio/ingest/landing.py#L13-L14), [`etfportfolio/ingest/landing.py:89`](../etfportfolio/ingest/landing.py#L89)
* **Diagnosis**:
  In `landing.py`:
  ```python
  def _content_address(conn: duckdb.DuckDBPyConnection, payload: dict) -> tuple[int, bytes]:
      return content_address(payload)
  ...
  digest, compressed = await worker.submit(_content_address, payload or {})
  ```
* **Rationale**:
  `content_address` performs canonical sorting of Python dictionary keys, JSON byte encoding via `orjson`, 64-bit integer hashing via `xxhash`, and zstandard compression via `zstd`. None of these operations access the DuckDB connection (`conn` is completely unused). Submitting this purely in-memory, CPU-bound calculation to `AsyncDbWorker` forces it into the single-threaded FIFO database queue, needlessly serializing payload processing and stalling actual DuckDB database operations behind compression workloads.

---

### Issue 9: Blocking Synchronous `input()` Executed Inside Async Event Loop
* **Files**: [`etfportfolio/ingest/session.py:225-245`](../etfportfolio/ingest/session.py#L225-L245), [`etfportfolio/ingest/session.py:315`](../etfportfolio/ingest/session.py#L315), [`etfportfolio/ingest/gateway.py:40`](../etfportfolio/ingest/gateway.py#L40)
* **Diagnosis**:
  In `session.py`:
  ```python
  def reconcile_account_id(account_id: str, env_path: Path | None = None) -> bool:
      ...
      if env_account_id and env_account_id != account_id:
          answer = (
              input(f"Account mismatch: '{account_id}' (new) vs '{env_account_id}' (expected). Continue? [y/N] ")
              .strip()
              .lower()
          )
  ```
  This function is invoked via `probe()` in `ensure_session()`, and via `ib_connection()` in `gateway.py`:
  ```python
  accounts = ib.managedAccounts()
  if accounts:
      reconcile_account_id(accounts[0])
  ```
* **Rationale**:
  `input()` is a synchronous, blocking standard I/O call. Because it is invoked in the middle of active asynchronous coroutines on the main thread, the entire asyncio event loop is frozen waiting for keyboard input from standard in. All network timers, ping/keepalive intervals for IB Gateway (`ib_async`), and ongoing concurrent HTTP requests stop executing. If the pipeline runs in a headless environment, background daemon, or scheduled cron job without a TTY, `input()` immediately raises an unhandled `EOFError`.

---

### Issue 10: Rigid Worker Signature Forcing Dummy `conn` Parameters
* **Files**: [`etfportfolio/ingest/landing.py:13-14`](../etfportfolio/ingest/landing.py#L13-L14), [`etfportfolio/core/db.py:75-77`](../etfportfolio/core/db.py#L75-L77)
* **Diagnosis**:
  `AsyncDbWorker._run()` unconditionally passes `self._conn` as the first argument to every submitted task:
  ```python
  func, args, kwargs, future = item
  result = func(self._conn, *args, **kwargs)
  ```
  To satisfy this, `landing.py` defines:
  ```python
  def _content_address(conn: duckdb.DuckDBPyConnection, payload: dict) -> tuple[int, bytes]:
      return content_address(payload)
  ```
* **Rationale**:
  The database worker enforces a rigid contract where any callable dispatched to the thread must accept a DuckDB connection object, even when the task does not involve database access. This results in artificial wrapper functions with dead arguments and obscures task dependencies.

---

## 3. Database & Query Performance

### Issue 11: Missing Compound Index on `bronze.snapshots` Leading to Full Table Scans
* **Files**: [`etfportfolio/core/schema.sql:96-105`](../etfportfolio/core/schema.sql#L96-L105), [`etfportfolio/ingest/snapshots.py:29-38`](../etfportfolio/ingest/snapshots.py#L29-L38)
* **Diagnosis**:
  In `schema.sql`:
  ```sql
  CREATE TABLE IF NOT EXISTS bronze.snapshots (
      snapshot_id     INTEGER PRIMARY KEY DEFAULT nextval('bronze.snapshots_id_seq'),
      hash            UBIGINT NOT NULL,
      product_id      INTEGER NOT NULL,
      url_prefix      VARCHAR NOT NULL,
      url_slug        VARCHAR,
      created_at      TIMESTAMP NOT NULL,
      last_checked_at TIMESTAMP NOT NULL
  );
  ```
  In `snapshots.py`, `store_snapshot` queries:
  ```sql
  SELECT snapshot_id, hash
  FROM bronze.snapshots
  WHERE product_id = $1 AND url_prefix = $2
  ORDER BY snapshot_id DESC
  LIMIT 1
  ```
* **Rationale**:
  `bronze.snapshots` contains no index on `(product_id, url_prefix)`. Because this lookup is performed on every snapshot fetch for every product across all 7 endpoints, DuckDB must perform a full sequential table scan for every individual snapshot evaluation. As snapshots accumulate across historical ingestion runs, the cost of checking snapshots increases linearly with total historical row count.

---

### Issue 12: Single-Row SQL Execution Loop for Time-Series Ingestion
* **Files**: [`etfportfolio/ingest/prices.py:202-217`](../etfportfolio/ingest/prices.py#L202-L217), [`etfportfolio/ingest/prices.py:284-288`](../etfportfolio/ingest/prices.py#L284-L288)
* **Diagnosis**:
  In `prices.py`:
  ```python
  def _insert_points(conn, spec, product_id, points, now):
      col_sql = ", ".join(("product_id", "date", *spec.columns, "updated_at"))
      placeholders = ", ".join(f"${i + 1}" for i in range(len(spec.columns) + 3))
      sql = f"INSERT INTO {spec.bronze_table} ({col_sql}) VALUES ({placeholders})"
      for bar_date, point in points.items():
          params: list[Any] = [product_id, bar_date]
          params.extend(point.get(col) for col in spec.columns)
          params.append(now)
          conn.execute(sql, params)
  ```
* **Rationale**:
  When performing a full 30-year price fetch or recovering from a corporate action mismatch, a single ETF returns approximately 7,500 daily trading bars. `_insert_points` executes `conn.execute(...)` individually 7,500 times per product. Across a universe of 5,000 ETFs undergoing full ingestion, this executes approximately 37,500,000 individual Python-to-C++ DuckDB calls. This serial per-row query execution consumes hours of runtime purely in query dispatch and parameter marshaling overhead, bypassing DuckDB's vectorized bulk ingestion capabilities.

---

### Issue 13: $O(N)$ Serial Thread-Queue Round-Trips for Contract Freshness
* **Files**: [`etfportfolio/ingest/contracts.py:234-242`](../etfportfolio/ingest/contracts.py#L234-L242)
* **Diagnosis**:
  In `contracts.py`:
  ```python
  to_process = []
  for pid in target_ids:
      if force:
          to_process.append(pid)
      else:
          fresh = await worker.submit(_contract_is_fresh, pid)
          if not fresh:
              to_process.append(pid)
  ```
  Where `_contract_is_fresh` executes:
  ```sql
  SELECT updated_at FROM bronze.contracts WHERE product_id = $1
  ```
* **Rationale**:
  Checking freshness iterates over all `target_ids` sequentially in Python, dispatching an individual SQL query through the asyncio-to-thread queue for each product. For 5,000 products, this performs 5,000 sequential queue submissions, context switches, and individual queries to determine what a single set-difference query could resolve in milliseconds.

---

### Issue 14: Manual Existence Checking and Procedural Updates in `themes.py`
* **Files**: [`etfportfolio/ingest/themes.py:38-87`](../etfportfolio/ingest/themes.py#L38-L87)
* **Diagnosis**:
  `upsert_themes` fetches all existing theme IDs into Python memory:
  ```python
  existing_ids = {row[0] for row in conn.execute("SELECT theme_id FROM bronze.themes").fetchall()}
  ```
  It then iterates over `parents` and `nodes`, executing individual single-row `INSERT` or `UPDATE` queries:
  ```python
  for p in parents:
      ...
      if theme_id in existing_ids:
          conn.execute(update_parent_query, [num_id, name, theme_id])
      else:
          conn.execute(insert_query, [theme_id, num_id, name, None])
          existing_ids.add(theme_id)
  ```
* **Rationale**:
  The function implements manual client-side upsert logic in Python via existence checks and conditional branching. This requires two round-trips per node in Python rather than utilizing declarative database upserts (`INSERT INTO ... ON CONFLICT DO UPDATE`), which are supported by DuckDB and used elsewhere in the project.

---

### Issue 15: Dual Clock Sources and Mixed Datetime Awareness
* **Files**: [`etfportfolio/ingest/prices.py:233`](../etfportfolio/ingest/prices.py#L233), [`etfportfolio/ingest/contracts.py:190`](../etfportfolio/ingest/contracts.py#L190), [`etfportfolio/ingest/products.py:44, 59`](../etfportfolio/ingest/products.py#L44), [`etfportfolio/ingest/themes.py:42`](../etfportfolio/ingest/themes.py#L42), [`etfportfolio/ingest/landing.py:41`](../etfportfolio/ingest/landing.py#L41)
* **Diagnosis**:
  Timestamps written into the database originate from two distinct sources:
  1. **Python clock**: `datetime.now(UTC)` stripped of timezone information (e.g. `prices.py:233`, `snapshots.py:22`) or passed directly as timezone-aware datetime objects (e.g. `contracts.py:190`).
  2. **DuckDB SQL clock**: `(now() AT TIME ZONE 'UTC')` evaluated inside DuckDB engine (e.g. `products.py`, `themes.py`, `landing.py`).
  In `contracts.py:190`, two separate calls to `datetime.now(UTC)` are evaluated in a single list comprehension:
  ```python
  values = list(data.values()) + [datetime.now(UTC), datetime.now(UTC)]
  ```
* **Rationale**:
  During multi-hour or multi-day ingestion runs, Python application server time and database server execution time can drift. In `contracts.py`, invoking `datetime.now(UTC)` twice creates microsecond disparities between `created_at` and `updated_at` on the initial insert. Furthermore, mixing naive UTC datetimes with timezone-aware datetimes across tables leads to type comparison errors during delta and freshness calculations.

---

### Issue 16: Mixed SQL Parameter Placeholder Syntax
* **Files**: [`etfportfolio/ingest/clean.py:48-70`](../etfportfolio/ingest/clean.py#L48-L70), [`etfportfolio/ingest/contracts.py:189`](../etfportfolio/ingest/contracts.py#L189), [`etfportfolio/prep/pipeline.py:19, 30, 41`](../etfportfolio/prep/pipeline.py#L19)
* **Diagnosis**:
  The codebase alternates between two different SQL parameter styles supported by DuckDB:
  - Positional numeric parameters (`$1, $2, $3, ...`): used throughout `contracts.py`, `prices.py`, `products.py`, `landing.py`, `snapshots.py`.
  - Question mark placeholders (`?, ?, ?, ...`): used in `clean.py:32, 48`, `prep/pipeline.py:19, 30, 41`, `tests/core/test_db.py`.
* **Rationale**:
  The absence of a unified parameter convention impairs codebase searchability, cross-module refactoring, and static SQL validation.

---

## 4. Data Integrity & Scraper Resilience

### Issue 17: Complete Lack of Error Isolation in Silver Extraction Pipeline
* **Files**: [`etfportfolio/prep/pipeline.py:109-130`](../etfportfolio/prep/pipeline.py#L109-L130), [`etfportfolio/prep/extractors.py:101, 238, 359, 436, 439, 549`](prep/extractors.py#L101)
* **Diagnosis**:
  In `prep/pipeline.py`, batches of 100 snapshots are decompressed and extracted:
  ```python
  extractor = EXTRACTOR_REGISTRY.get(url_prefix)
  if extractor is not None:
      result = extractor(product_id, data, created_at)
  ```
  There is no `try...except` block surrounding the extractor call.
  Within `extractors.py`, functions perform direct casts and explicit raises:
  - `extract_ratios`: `val_float = float(value)` (line 101)
  - `extract_profile`: `raise ValueError(f"Unknown Management Approach: '{val}'")` (line 238)
  - `extract_esg`: `float(node_val)` (line 359)
  - `extract_mstar`: `raise ValueError(f"Unrecognized mstar pillar id: '{pillar_id}'")` (line 436), `raise ValueError(f"Unrecognized rating string '{raw_val}' for pillar '{pillar_id}'")` (line 439)
  - `extract_holdings`: `weight = float(weight_val) / 100.0` (line 549)
* **Rationale**:
  If a single payload out of tens of thousands contains an unexpected string (e.g. `"N/A"`, a newly introduced Morningstar rating category, or an unexpected management approach), the extractor raises an unhandled exception. This terminates the entire batch loop, triggers a transaction rollback, and aborts the pipeline. Because the offending snapshot ID is never written to `silver.processed_snapshots`, re-running the pipeline will crash on the exact same snapshot, permanently halting data processing.

---

### Issue 18: Inconsistent Error Handling Contracts Among Utility Parsers
* **Files**: [`etfportfolio/prep/utils.py:43-77`](../etfportfolio/prep/utils.py#L43-L77), [`etfportfolio/prep/utils.py:79-137`](../etfportfolio/prep/utils.py#L79-L137), [`etfportfolio/prep/utils.py:139-166`](../etfportfolio/prep/utils.py#L139-L166), [`etfportfolio/prep/utils.py:168-185`](../etfportfolio/prep/utils.py#L168-L185)
* **Diagnosis**:
  Parsing utilities in `prep/utils.py` exhibit conflicting error reporting strategies:
  - `parse_effective_date`: Returns fallback value `(fallback_date, "snapshot")` when encountering malformed or unparseable inputs.
  - `parse_net_assets`: Returns `None` when parsing fails or input is malformed.
  - `parse_manager_tenure`: Raises `ValueError(f"Invalid Manager Tenure date string: '{raw_val}'")` (line 161) when format is unrecognized.
  - `parse_percentage`: Raises `ValueError(f"Cannot parse percentage from '{val}': {e}")` (line 184) when conversion fails.
* **Rationale**:
  There is no standard contract for handling malformed data across the parser suite. Some functions safely yield `None` or fallbacks, while adjacent functions raise exceptions that crash calling extractors.

---

### Issue 19: Null Value Masking in Cold Storage Cleanup SQL
* **Files**: [`etfportfolio/ingest/clean.py:43-70`](../etfportfolio/ingest/clean.py#L43-L70)
* **Diagnosis**:
  In `clean_cold_storage`, historical bars are compared between cold storage and bronze:
  ```sql
  SELECT count(*)
  FROM cold_storage.prices c
  LEFT JOIN bronze.prices b ON c.product_id = b.product_id AND c.date = b.date
  WHERE c.product_id = ? AND c.run_id = ? AND c.date <= ?
  AND (
      b.date IS NULL OR
      abs(c.open - b.open) > ? OR abs(c.open - b.open) / nullif(abs(c.open), 0) > ? OR
      abs(c.high - b.high) > ? OR abs(c.high - b.high) / nullif(abs(c.high), 0) > ? OR
      abs(c.low - b.low) > ? OR abs(c.low - b.low) / nullif(abs(c.low), 0) > ? OR
      abs(c.close - b.close) > ? OR abs(c.close - b.close) / nullif(abs(c.close), 0) > ?
  )
  ```
* **Rationale**:
  In SQL, any arithmetic operation or comparison involving `NULL` yields `NULL`. In `bronze.prices` and `cold_storage.prices`, `open`, `high`, `low`, and `volume` are nullable columns.
  - If `c.open` is `NULL` and `b.open` is `100.0`, `abs(c.open - b.open)` evaluates to `NULL`.
  - The condition `NULL > ?` evaluates to `UNKNOWN` (falsy in a `WHERE` clause).
  - Consequently, if a historical restatement introduced or removed price values where nulls previously existed, the discrepancy is not detected as a mismatch. The mismatch count remains 0, and `clean_cold_storage` permanently purges the cold storage archive under the false assumption that all prices matched.

---

### Issue 20: Weekend Calendar Blindness in Price Freshness Calculation
* **Files**: [`etfportfolio/ingest/prices.py:490-492`](../etfportfolio/ingest/prices.py#L490-L492), [`etfportfolio/ingest/prices.py:343-362`](../etfportfolio/ingest/prices.py#L343-L362), [`etfportfolio/ingest/prices.py:610-616`](../etfportfolio/ingest/prices.py#L610-L616)
* **Diagnosis**:
  In `prices.py`:
  ```python
  today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
  yesterday = today - timedelta(days=1)
  ...
  to_process = [
      p for p in products
      if not is_series_fresh(status_cache.get(p.product_id), yesterday, settings.freshness_window_hours)
  ]
  ```
  And inside `is_series_fresh`:
  ```python
  if status.last_date is not None and status.last_date >= target_date:
      return True
  ```
* **Rationale**:
  The calculation defines the target freshness date strictly as calendar `today - 1 day`. On Mondays, `yesterday` evaluates to Sunday. Financial equity markets do not trade on weekends; the latest available historical trading bar for any standard ETF is Friday (3 calendar days prior). As a result, on every Monday prior to market close and settlement, `last_date >= target_date` evaluates to `False` for the entire product universe.

---

## 5. Static Typing, Dependencies & CLI Contracts

### Issue 21: Static Type Checker (`pyright`) Fails with 11 Errors
* **Files**: [`etfportfolio/ingest/clean.py:43, 73`](../etfportfolio/ingest/clean.py#L43), [`tests/core/test_config.py:6, 23, 30, 39`](tests/core/test_config.py#L6), [`tests/core/test_db.py:44, 49`](tests/core/test_db.py#L44), [`tests/prep/test_pipeline.py:173, 175, 177`](tests/prep/test_pipeline.py#L173)
* **Diagnosis**:
  Executing `uv run pyright` reports 11 typing errors:
  1. `clean.py:43:22`: `Object of type "None" is not subscriptable` on `conn.execute(...).fetchone()[0]`.
  2. `clean.py:73:21`: `Object of type "None" is not subscriptable` on `conn.execute(...).fetchone()[0]`.
  3. `tests/core/test_config.py:6, 23, 30, 39`: `No parameter named "_env_file"` when instantiating `Settings(_env_file=None)`.
  4. `tests/core/test_db.py:44:17, 49:13`: `Object of type "None" is not subscriptable` on `.fetchone()[0]`.
  5. `tests/prep/test_pipeline.py:173:16, 175:15, 177:15`: `Object of type "None" is not subscriptable` on `.fetchone()[0]`.
* **Rationale**:
  The project documentation instructs developers to verify code with `uv run pyright`. DuckDB's Python stub definitions type `.fetchone()` as returning `tuple[Any, ...] | None`. Direct subscripting without `None` guards causes type-checking failures and runtime `TypeError` risks. In addition, `pydantic-settings` 2.x does not declare `_env_file` as an acceptable keyword argument on `BaseSettings.__init__`.

---

### Issue 22: Critical Runtime Dependency Misclassified in Development Group
* **Files**: [`pyproject.toml:7-20`](pyproject.toml#L7-L20), [`pyproject.toml:23-31`](pyproject.toml#L23-L31), [`etfportfolio/core/progress.py:9`](../etfportfolio/core/progress.py#L9), [`etfportfolio/core/logging.py:7`](../etfportfolio/core/logging.py#L7), [`main.py:5`](main.py#L5)
* **Diagnosis**:
  In `pyproject.toml`, `tqdm` is specified exclusively under `[dependency-groups] dev`:
  ```toml
  [dependency-groups]
  dev = [
      "ipykernel>=7.3.0",
      "pandas>=3.0.5",
      "pyright>=1.1.411",
      "pytest>=9.1.1",
      "respx>=0.23.1",
      "ruff>=0.16.4",
      "tqdm>=4.70.0",
  ]
  ```
  However, `etfportfolio/core/progress.py` imports `tqdm` unconditionally:
  ```python
  from tqdm import tqdm
  ```
  And `core/progress.py` is imported by `core/logging.py`, which is imported by `main.py`.
* **Rationale**:
  When the application is installed in a production environment using standard package dependencies (e.g. `pip install .` or `uv sync --no-dev`), `tqdm` is not installed. Invoking `python main.py` immediately fails with `ModuleNotFoundError: No module named 'tqdm'`.

---

### Issue 23: CLI Surface Drift: Documented Filtering Flags Unimplemented
* **Files**: [`README.md:41-44`](README.md#L41-L44), [`etfportfolio/ingest/pipeline.py:176-209`](../etfportfolio/ingest/pipeline.py#L176-L209)
* **Diagnosis**:
  `README.md` documents:
  ```bash
  uv run python main.py ingest --limit 10
  uv run python main.py ingest --product-ids "756733,8335"
  uv run python main.py ingest --force
  ```
  In `ingest/pipeline.py`, the `Ingest` CLI surface is defined as:
  ```python
  class Ingest:
      def __call__(self, force: bool = False) -> None: ...
      def session(self) -> None: ...
      def products(self, force: bool = False) -> None: ...
      def contracts(self, force: bool = False) -> None: ...
      def prices(self, force: bool = False) -> None: ...
      def themes(self, force: bool = False) -> None: ...
      def details(self, force: bool = False) -> None: ...
      def clean(self) -> None: ...
  ```
* **Rationale**:
  The CLI implementation does not define `limit` or `product_ids` parameters on any method. When a user runs the commands documented in the README, `fire` either rejects the unexpected arguments or ignores them, running the full ingestion without bounds.

---

### Issue 24: Broken Diagnostic Script Due to Namespace Mismatch
* **Files**: [`scripts/log_check.py:31, 39, 48, 54`](scripts/log_check.py#L31-L54)
* **Diagnosis**:
  In `scripts/log_check.py`:
  ```python
  if "etfportfolio.ingestion.products" in line:
      ...
  elif "etfportfolio.ingestion.contracts" in line or "ib_async.wrapper" in line:
      ...
  elif "etfportfolio.ingestion.gateway" in line:
      ...
  elif "etfportfolio.ingestion.prices" in line:
      ...
  ```
* **Rationale**:
  The actual Python package is named `etfportfolio.ingest`. Standard logger initialization across the codebase (`logging.getLogger(__name__)`) emits logger names like `etfportfolio.ingest.products` and `etfportfolio.ingest.contracts`. The string literal check for `etfportfolio.ingestion.*` will never match any log lines emitted by the package, causing the script to output zero counts for all ingestion operations.

---

### Issue 25: Duplicated HTTP Client Construction Logic
* **Files**: [`etfportfolio/ingest/session.py:204-222`](../etfportfolio/ingest/session.py#L204-L222), [`etfportfolio/ingest/session.py:610-621`](../etfportfolio/ingest/session.py#L610-L621)
* **Diagnosis**:
  `build_async_client` centrally configures standard HTTP headers, cookies, and base URL:
  ```python
  def build_async_client(timeout: float = 30.0) -> httpx.AsyncClient:
      session_path = Path(settings.session_state_path)
      cookies = _load_cookies_from_storage_state(session_path)
      headers = {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ...",
          "Accept": "application/json, text/plain, */*",
          "Referer": f"{settings.ibkr_base_url}/portal/",
          "X-Requested-With": "XMLHttpRequest",
      }
      return httpx.AsyncClient(...)
  ```
  However, inside `login()`, an identical client configuration is manually re-constructed inline:
  ```python
  async with httpx.AsyncClient(
      base_url=settings.ibkr_base_url.rstrip("/"),
      headers={
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ...",
          "Accept": "application/json, text/plain, */*",
          "Referer": f"{settings.ibkr_base_url}/portal/",
          "X-Requested-With": "XMLHttpRequest",
      },
      cookies=cookies,
      timeout=10.0,
      follow_redirects=True,
  ) as test_client:
  ```
* **Rationale**:
  Duplicating the client instantiation with hardcoded headers creates maintenance drift. Any future updates to user agents, security headers, or proxy configurations applied to `build_async_client` will not take effect during the login verification probe.

---

## 6. Security & Project Policy Compliance

### Issue 26: Plaintext Brokerage Credentials Stored on Disk
* **Files**: [`etfportfolio/ingest/session.py:273-302`](../etfportfolio/ingest/session.py#L273-L302), [`etfportfolio/ingest/session.py:634`](../etfportfolio/ingest/session.py#L634)
* **Diagnosis**:
  In `session.py`:
  ```python
  def _write_credentials(username: str, password: str, env_path: Path | None = None) -> None:
      ...
      replacements = {"IBKR_USERNAME": username, "IBKR_PASSWORD": password}
      ...
      target_env.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
  ```
  When `login()` completes successfully, line 634 executes:
  ```python
  _write_credentials(username, password)
  ```
* **Rationale**:
  The application automatically writes the user's raw, unencrypted Interactive Brokers username and password into the local `.env` text file. Storing financial trading credentials in plaintext on disk exposes the account to credential harvesting by any unauthorized local process, tool, or accidental source control commit.

---

### Issue 27: Retention of Obsolete Logic and Compatibility Aliases
* **Files**: [`AGENTS.md:15`](AGENTS.md#L15), [`etfportfolio/ingest/prices.py:38-41`](../etfportfolio/ingest/prices.py#L38-L41), [`scripts/migrate_snapshots_changelog.py`](scripts/migrate_snapshots_changelog.py), [`scripts/migration_price_status.py`](scripts/migration_price_status.py), [`scripts/restore_truncated_prices.py`](scripts/restore_truncated_prices.py)
* **Diagnosis**:
  1. `AGENTS.md` explicitly mandates:
     > *"Delete obsolete logic and tests outright; do not add compatibility shims or fallback layers."*
  2. In `prices.py:38-41`:
     ```python
     REL_TOL = PRICE_REL_TOL  # Backward compatibility alias
     ABS_TOL = PRICE_ABS_TOL  # Backward compatibility alias
     ```
  3. The `scripts/` directory retains multiple one-time, temporary migration scripts:
     - `migrate_snapshots_changelog.py`: One-time script to convert snapshots to state-transition changelogs.
     - `migration_price_status.py`: "Temporary migration script: backfill bronze.price_status from bronze.prices."
     - `restore_truncated_prices.py`: One-off script containing hardcoded product IDs (`TARGET_PRODUCTS = [229325937, 236798101, 332383610]`).
* **Rationale**:
  Retaining compatibility aliases and completed one-time migration scripts directly contravenes the project's explicit architectural guidelines against compatibility shims and obsolete logic.

---

## 7. Global State & Configuration Coupling

### Issue 28: Import-Time Singleton Instantiation and Mutable Global State
* **Files**: [`etfportfolio/core/config.py:52`](../etfportfolio/core/config.py#L52), [`etfportfolio/ingest/session.py:269, 300-301`](../etfportfolio/ingest/session.py#L269)
* **Diagnosis**:
  In `core/config.py`, settings are instantiated at the module root:
  ```python
  settings = Settings()
  ```
  Throughout the codebase, modules import `from etfportfolio.core.config import settings`.
  Furthermore, `session.py` directly mutates this singleton at runtime:
  ```python
  settings.account_id = account_id
  settings.ibkr_username = username
  settings.ibkr_password = password
  ```
* **Rationale**:
  1. **Side-effects at import time**: Simply importing any module that touches configuration triggers file reads on `.env` and `pyproject.toml`.
  2. **Mutable shared state**: Mutating `settings` properties in-place introduces hidden global state transitions between different asynchronous calls and test executions, violating the core rule in `AGENTS.md`: *"No hidden global state, no speculative abstractions."*
