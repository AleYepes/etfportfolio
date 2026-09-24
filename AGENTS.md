## Project Overview

This project is intended for ETF factor-series analytics:
- ingest ETF data (IBKR + supplementary sources) in a DuckDB
- build monthly LOCF panels of fundamentals following a medallion architecture
- derive weighted factor-return series from each panel metric
- regress ETF returns on selected factors
- compute efficient-frontier portfolios

## Code Guidelines

- Write simple, lean, testable code; No hidden global state, no speculative abstractions
- Placement guides:
  - Only used in one module: keep it in that module `etfportfolio/<pkg>/<mod>.py`
  - Shared between modules within a package: `etfportfolio/<pkg>/utils.py` (or a new <mod>.py)
  - Shared between modules across packages: `etfportfolio/core/*` (packages must not import from each other)
- Delete obsolete logic and tests outright; do not add compatibility shims or fallback layers

## Testing Guidelines

- Every `etfportfolio/<pkg>/<mod>.py` must have a matching `tests/<pkg>/test_<mod>.py`
- Reuse `tests/conftest.py`