## Overview

This project is intended for ETF factor-series analytics:
- ingest ETF data (IBKR + supplementary sources) in a DuckDB,
- build monthly LOCF panels of fundamentals following a medallion architecture,
- derive weighted factor-return series from each panel metric,
- regress ETF returns on selected factors,
- compute efficient-frontier portfolios.

## Code

- Write simple, testable code; No hidden global state, no speculative abstractions.
- Placement guides:
  - Used in one module: keep in that module `etfportfolio/<pkg>/<mod>.py`.
  - Shared between modules in a package: `etfportfolio/<pkg>/utils.py`.
  - Shared across packages: `etfportfolio/core/*`.
- Delete obsolete logic and tests outright; do not add compatibility shims or fallback layers.

## Tests

- Every `etfportfolio/<pkg>/<mod>.py` must have a matching `tests/<pkg>/test_<mod>.py`.
- Reuse `tests/conftest.py`