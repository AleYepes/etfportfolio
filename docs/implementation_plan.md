# Testing Framework Redesign: 1:1 Parity, Behavior Audit & Modernization Plan

This plan establishes strict 1:1 structural parity between `etfportfolio/` and `tests/`, eliminates obsolete test debt, centralizes test fixtures and isolation, and organizes quantitative invariant tests per [TESTING_FRAMEWORK.md](file:///Users/alex/Documents/etfportfolio/docs/TESTING_FRAMEWORK.md).

---

## Architectural Decisions Settled in Interview

1. **Strict 1:1 Mirror Layout**: Every production module under `etfportfolio/<pkg>/<module>.py` maps to `tests/<pkg>/test_<module>.py` (including contract/smoke tests for constant-heavy modules like `endpoints.py` or `progress.py`).
2. **Unified Single Test Files**: Unit calculations and fast in-memory DuckDB integration tests live together in `test_<module>.py`.
3. **Prune Legacy Migration Tests**: Prune one-off script tests from `tests/test_changelog_and_migration.py` and preserve active snapshot changelog logic inside `tests/ingest/test_snapshots.py`.
4. **Phased Package Rollout**: Executed package-by-package (`core/` -> `prep/` -> `ingest/` -> cleanup), keeping tests green at every step.
5. **Centralized Fixture Helper**: Provide `load_fixture(name)` in `tests/conftest.py` for loading JSON files from `tests/fixtures/` dynamically.
6. **Autouse Global Test Environment Guard**: Automatically isolate credentials, session state path, and prevent real network/browser launches.
7. **Decompose `test_prices.py`**: Break the 933-line monolith into `tests/ingest/test_prices.py`, `tests/ingest/test_gateway.py`, and `tests/ingest/test_pipeline.py`.
8. **Strict Domain Boundaries**: Cleanly separate `test_snapshots.py` (blob/changelog), `test_landing.py` (gating/previews), and `test_details.py` (concurrency/freshness caches).
9. **Complete Core Contracts**: Verify schema idempotence, `AsyncDbWorker` thread safety, settings config loading, and progress bar dispatch.
10. **Active Fixture Verification**: Test extractors against complete sample JSON fixtures under `tests/fixtures/`.
11. **Mocked Pipeline Orchestration**: Test `ingest/pipeline.py` and `prep/cli.py` as controllers without redundant end-to-end loops.
12. **Explicit Quantitative Invariants**: Test names and assertions strictly match production math (14-day overlap, 4-5d settlement envelope, $\text{std}(R) \le 10^{-3}$ split detection, 90% retention guard, cold storage redundancy purging).

---

## Target File Map

```
etfportfolio/                             tests/
├── core/                                 ├── conftest.py
│   ├── config.py            ───────►     ├── core/
│   ├── db.py                ───────►     │   ├── test_config.py
│   ├── logging.py           ───────►     │   ├── test_db.py
│   └── progress.py          ───────►     │   ├── test_logging.py
│                                         │   └── test_progress.py
├── prep/                                 ├── prep/
│   ├── cli.py               ───────►     │   ├── test_cli.py
│   ├── extractors.py        ───────►     │   ├── test_extractors.py
│   ├── pipeline.py          ───────►     │   ├── test_pipeline.py
│   └── utils.py             ───────►     │   └── test_utils.py
└── ingest/                               └── ingest/
    ├── clean.py             ───────►         ├── test_clean.py
    ├── contracts.py         ───────►         ├── test_contracts.py
    ├── details.py           ───────►         ├── test_details.py
    ├── endpoints.py         ───────►         ├── test_endpoints.py
    ├── gateway.py           ───────►         ├── test_gateway.py
    ├── landing.py           ───────►         ├── test_landing.py
    ├── pipeline.py          ───────►         ├── test_pipeline.py
    ├── prices.py            ───────►         ├── test_prices.py
    ├── products.py          ───────►         ├── test_products.py
    ├── session.py           ───────►         ├── test_session.py
    ├── snapshots.py         ───────►         ├── test_snapshots.py
    ├── themes.py            ───────►         ├── test_themes.py
    └── utils.py             ───────►         └── test_utils.py
```

---

## Proposed Changes

### 1. Root & Test Infrastructure

#### [NEW] [tests/conftest.py](file:///Users/alex/Documents/etfportfolio/tests/conftest.py)
- Autouse environment isolation fixture:
  - Isolate `settings.session_state_path` to a temporary path.
  - Reset `settings.account_id` to test default.
  - Mock `etfportfolio.ingest.session.login` to avoid Playwright launches.
- Standard reusable fixtures:
  - `db_conn`: in-memory DuckDB with `apply_schema(conn)`.
  - `load_fixture`: helper function taking filename slug (e.g., `"holdings_equity"`) and loading parsed JSON from `tests/fixtures/<name>.json`.

---

### 2. `core` Test Suite (`tests/core/`)

#### [NEW] [tests/core/test_config.py](file:///Users/alex/Documents/etfportfolio/tests/core/test_config.py)
- Test default settings fallback and reading custom settings from `pyproject.toml` `[tool.etfportfolio]`.
- Test environment variable overriding (e.g. `ACCOUNT_ID`, `DB_PATH`).

#### [NEW] [tests/core/test_db.py](file:///Users/alex/Documents/etfportfolio/tests/core/test_db.py)
- Test `apply_schema` creates all required schemas (`bronze`, `silver`, `gold`, `cold_storage`) and tables.
- Test `apply_schema` idempotence (running multiple times on same database does not fail or duplicate sequences).
- Test `AsyncDbWorker`:
  - Worker execution of synchronous DB tasks in thread pool.
  - Exception propagation across worker boundary to caller.
  - Proper shutdown.

#### [NEW] [tests/core/test_logging.py](file:///Users/alex/Documents/etfportfolio/tests/core/test_logging.py)
- Relocate and modernize existing tests from `tests/test_logging.py` (verifying verbose/quiet logging behavior and noisy logger suppression).

#### [NEW] [tests/core/test_progress.py](file:///Users/alex/Documents/etfportfolio/tests/core/test_progress.py)
- Test `iter_progress` and `progress_bar` creation with custom descriptions.
- Test `bars_disabled()` evaluation based on `sys.stderr.isatty()`.
- Test `TqdmLoggingHandler` writing through `tqdm.write` without crashing or corrupting active bars.
- Test progress bar advance, total tracking, and context exit cleanup.

---

### 3. `prep` Test Suite (`tests/prep/`)

#### [NEW] [tests/prep/test_extractors.py](file:///Users/alex/Documents/etfportfolio/tests/prep/test_extractors.py)
- Move extractor unit tests from `tests/test_observations_extractors.py`.
- Add active verification against sample payload fixtures (`ratios_complete.json`, `profile_complete.json`, `esg.json`, `mstar_complete.json`, `lipper_complete.json`, `holdings_complete.json`, `theme_weights.json`) ensuring all real payload variants extract cleanly.

#### [NEW] [tests/prep/test_utils.py](file:///Users/alex/Documents/etfportfolio/tests/prep/test_utils.py)
- Move parsing utility tests (`parse_effective_date`, `clean_credit_rating`, `sanitize_metric_id`, `parse_net_assets`, `parse_manager_tenure`, `parse_percentage`, `decompress_payload`) from `tests/test_observations_extractors.py` and `tests/test_utils.py`.

#### [NEW] [tests/prep/test_pipeline.py](file:///Users/alex/Documents/etfportfolio/tests/prep/test_pipeline.py)
- Move pipeline integration tests from `tests/test_observations_pipeline.py`:
  - `run_observations` execution and incremental watermark tracking in `silver.processed_snapshots`.
  - In-memory deduplication of multiple snapshots for the same metric/dimension.
  - `force=True` complete reprocessing.

#### [NEW] [tests/prep/test_cli.py](file:///Users/alex/Documents/etfportfolio/tests/prep/test_cli.py)
- Test `ObservationsCLI.__call__` dispatching to `run_observations` with `force=True` and `force=False`.

---

### 4. `ingest` Test Suite (`tests/ingest/`)

#### [MODIFY] [tests/ingest/test_clean.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_clean.py)
- Retain existing tests for cold storage pruning and payload blob GC.
- Verify `clean_cold_storage` purges redundant runs ($>5\text{d}$ identical) while preserving genuine corporate actions.

#### [NEW] [tests/ingest/test_contracts.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_contracts.py)
- Relocate and modernize tests from `tests/test_contracts.py`:
  - `_clean_val` string stripping and null conversions.
  - `_flatten_contract_details` attribute flattening.
  - `upsert_contract` inserting and updating DuckDB rows without empty string contamination.

#### [NEW] [tests/ingest/test_products.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_products.py)
- Relocate tests from `tests/test_products.py`:
  - `resolve_target_products` exclusion of `blocked_exchanges`.
  - Empty `bronze.contracts` handling.

#### [NEW] [tests/ingest/test_session.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_session.py)
- Move session-related tests from `tests/test_session_and_series.py`:
  - `fetch_with_retry` 200 success and 404 non-retry behavior using `respx`.
  - `reconcile_account_id` `.env` updates.

#### [NEW] [tests/ingest/test_snapshots.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_snapshots.py)
- Move active snapshot logic from `tests/test_changelog_and_migration.py`:
  - Initial snapshot store inserting row.
  - Identical snapshot payload updating `last_checked_at` in place.
  - Changed snapshot payload inserting new row.
  - Transaction rollback on DuckDB error.

#### [NEW] [tests/ingest/test_landing.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_landing.py)
- Test `fetch_and_gate`: content addressing and hash comparison against `bronze.snapshot_previews`.
- Test `commit_preview`: updating preview hash and garbage collecting old preview blob.
- Test `stamp_last_checked`: conditional timestamping.

#### [NEW] [tests/ingest/test_details.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_details.py)
- Test `load_landing_freshness_cache` and `load_endpoint_freshness_cache`.
- Test `process_product`: skipping fresh landing/endpoints, fetching changed endpoints, handling 404s.

#### [NEW] [tests/ingest/test_gateway.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_gateway.py)
- Extract gateway tests from `tests/ingest/test_prices.py`:
  - `ib_connection` context manager lifecycle (connect, yield, disconnect).
  - Raising `IBConnectionError` on connection refused or timeout.

#### [MODIFY] [tests/ingest/test_prices.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_prices.py)
- Refactor the 933-line file to focus strictly on price engine invariant suites:
  - `test_format_duration`: Duration calculation and 30Y cap.
  - `test_record_and_load_price_status`: Status caching, truncation of error messages.
  - `test_is_series_fresh_differentiated`: Freshness evaluation.
  - `test_validate_overlap_settlement_envelope`: 14-day calendar window, 4-5d settlement envelope ($\le \$0.10$ or $\le 1\%$).
  - `test_validate_overlap_corporate_action`: Divergence outside 5d triggering corporate action flag.
  - `test_uniform_ratio_split_detection`: Multi-bar uniform ratio test ($\text{std}(R) \le 10^{-3}$).
  - `test_anti_truncation_90pct_guard`: Minimum 90% retention guard.
  - `test_upsert_series_and_replace_series`: Database series mutation and cold storage archiving.

#### [NEW] [tests/ingest/test_pipeline.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_pipeline.py)
- Test `Ingest.contracts`, `Ingest.details`, and `Ingest.prices` orchestration:
  - Option parsing, worker initialization, and task dispatch.

#### [NEW] [tests/ingest/test_themes.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_themes.py)
- Test `upsert_themes` hierarchy (inserting parents, then child nodes with conflict resolution).
- Test `sync` taxonomy fetching, freshness skip behavior (unless `force=True`), and database updates under mocked network responses.

#### [NEW] [tests/ingest/test_endpoints.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_endpoints.py)
- Test `Endpoint.resolve` URL construction and path parameter interpolation.

#### [NEW] [tests/ingest/test_utils.py](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_utils.py)
- Move ingest utilities from `tests/test_utils.py`:
  - `is_fresh` calculation.
  - `canonical_bytes` and `content_address` determinism.
  - `store_blob` and `gc_preview_blob`.

---

### 5. Cleanup of Legacy Flat Test Files

Once all replacement modules are active and passing:
- [DELETE] `tests/test_changelog_and_migration.py`
- [DELETE] `tests/test_contracts.py`
- [DELETE] `tests/test_logging.py`
- [DELETE] `tests/test_observations_extractors.py`
- [DELETE] `tests/test_observations_pipeline.py`
- [DELETE] `tests/test_products.py`
- [DELETE] `tests/test_session_and_series.py`
- [DELETE] `tests/test_utils.py`

---

## Verification Plan

### Automated Tests
1. Run test suite at each phase:
   - Phase 1 (`core`): `.venv/bin/pytest tests/core`
   - Phase 2 (`prep`): `.venv/bin/pytest tests/prep`
   - Phase 3 (`ingest`): `.venv/bin/pytest tests/ingest`
2. Full suite run across entire repository:
   - `.venv/bin/pytest -v` (expect 100% passing with 0 warnings)
3. Code formatting & lint validation:
   - `.venv/bin/ruff check .`
   - `.venv/bin/ruff format --check .`
