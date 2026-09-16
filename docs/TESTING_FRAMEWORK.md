# Testing Framework Redesign: 1:1 Structural Parity, Behavior Audit & Test Suite Modernization

**Status:** Draft / Conceptual Proposal  
**Target Areas:** `tests/`, `etfportfolio/`  
**Author:** Quantitative Architecture & Engineering  
**Date:** September 2026  

---

## 1. Context & Motivation

Over several iterations of development across ingestion, bronze/silver schema migrations, and factor analysis, the test suite has accumulated structural and behavioral debt:
1. **Monolithic & Disorganized Test Files:** `tests/test_prices.py` alone grew to 800+ lines mixing unit calculations, integration flows, and edge assertions. Legacy files like `test_session_and_series.py` combine disparate domains.
2. **Structural Drift:** The repository is organized modularly under `etfportfolio/` (`core/`, `ingest/`, `prep/`, etc.), whereas `tests/` remained flat with arbitrary naming conventions.
3. **Behavioral Staleness & Deprecated Assertions:** Some tests still assert against obsolete constraints, deprecated tolerance thresholds, or legacy error structures rather than current production contracts.
4. **Cognitive Overhead:** When modifying a production script (e.g., `etfportfolio/ingest/contracts.py`), finding all corresponding test assertions requires grepping rather than navigating to a predictable test location.

---

## 2. Core Architectural Principles

### 2.1 1:1 Mirror Structural Parity
Every production file under `etfportfolio/<package>/<module>.py` must have an exact 1:1 mirror under `tests/<package>/test_<module>.py`:

```
etfportfolio/
├── core/
│   ├── config.py
│   ├── db.py
│   ├── logging.py
│   └── progress.py
├── ingest/
│   ├── clean.py
│   ├── contracts.py
│   ├── details.py
│   ├── endpoints.py
│   ├── gateway.py
│   ├── pipeline.py
│   ├── prices.py
│   ├── products.py
│   ├── session.py
│   ├── themes.py
│   └── utils.py
└── prep/
    ├── extractors.py
    ├── pipeline.py
    └── utils.py

tests/
├── conftest.py
├── fixtures/
├── core/
│   ├── test_config.py
│   ├── test_db.py
│   ├── test_logging.py
│   └── test_progress.py
├── ingest/
│   ├── test_clean.py
│   ├── test_contracts.py
│   ├── test_details.py
│   ├── test_endpoints.py
│   ├── test_gateway.py
│   ├── test_pipeline.py
│   ├── test_prices.py
│   ├── test_products.py
│   ├── test_session.py
│   ├── test_themes.py
│   └── test_utils.py
└── prep/
    ├── test_extractors.py
    ├── test_pipeline.py
    └── test_utils.py
```

### 2.2 Strict Behavior Audit & Deprecation Pruning
Following the repo's governing rule in `AGENTS.md` (**Replacement Over Deprecation**):
- Tests must assert against **current active behavior only**.
- Delete assertions verifying deprecated fallback modes or dead error paths.
- Each test module must validate:
  1. **Contract Integrity:** Function signatures, return types, and argument validation.
  2. **Domain Invariants:** Core algorithmic logic (e.g. overlap validation, ratio uniformity, extractor parsing).
  3. **Error Boundaries:** Failure modes (timeouts, network drops, data truncation) and transaction rollback guarantees.

---

## 3. Implementation Phases & Interview Agenda

When developing the full specification in subsequent rounds, the interview should systematically address:

### Phase 1: Test Inventory & Audit Mapping
- Enumerate every existing test file in `tests/*.py` and map each test function to its target production module.
- Identify orphaned or obsolete tests with no corresponding production code.

### Phase 2: Behavioral Contract Verification
For each domain, audit the active acceptance criteria:
- **`ingest/prices.py`**:
  - 14-day calendar overlap window (~10 trading bars).
  - Last 4–5 trading days settlement envelope ($\le \$0.10$ or $\le 1\%$).
  - Historical core corporate action triggers ($d < t_{\text{last}} - 4\text{d}$).
  - Multi-bar uniform ratio test ($\text{std}(R) \le 10^{-3}$) isolating stock splits.
  - Pre-archive historical verification guard ($d < t_{\text{overlap\_start}}$).
  - Anti-truncation 90% retention guard.
- **`ingest/clean.py`**:
  - Cold storage pruning of runs where historical bars $> 5\text{d}$ match bronze.
  - Unreferenced payload blob purging.
  - Checkpoint execution.
- **`ingest/products.py` & `contracts.py`**:
  - Qualification rules, exchange filtering, and batch updates.
- **`ingest/session.py` & `details.py`**:
  - Session authentication, landing hash comparison, ungated/gated endpoint caching.
- **`prep/` (Observations & Silver Layer)**:
  - Metric and dimension extraction, payload decompression, watermark tracking.

### Phase 3: Migration Execution & Fixture Hygiene
- Move shared test fixtures (e.g. `db_conn`, mock IB gateway) into root `tests/conftest.py`.
- Ensure JSON payload fixtures under `tests/fixtures/` are organized and referenced cleanly without hardcoded absolute paths.
- Run `pytest` and `ruff` to ensure 100% test coverage with zero regressions.

---

## 4. Key Questions for the Next Interview

1. **Granularity of Unit vs Integration:** Should `test_<module>.py` contain both pure unit tests (mocked DB/network) and in-memory DuckDB integration tests, or should integration tests have dedicated files (e.g. `test_<module>_integration.py`)?
2. **Fixture Strategy:** How should JSON API responses (`fixtures/*.json`) be shared across modules without duplication?
3. **Migration Phasing:** Should the reorganization happen package-by-package (e.g., `core/` then `ingest/` then `prep/`) or in a single comprehensive restructuring?
