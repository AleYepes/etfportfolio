# Agent Guidelines & Overview

This project runs factor-series analyses on ETF data. At a high level, it must:

- fetch ETF data from IBKR and supplementary data from other sources
- follow a medallion architecture via DuckDB schemas (`bronze`, `silver`, `gold`, and `cold_storage`)
- construct monthly LOCF panels for ETF fundamental metrics
- build weighted factor return series from each fundamental metric
- select a subset of factors as independent variables
- regress ETF returns on factor returns
- calculate efficient-frontier portfolios

## Architecture & Coding Principles

- **Simplicity**: Clear, simple, testable code. No hidden global state. No speculative abstractions. Do not build for hypothetical future needs.
- **SQL Separation**: Async functions contain no SQL. All SQL statements live in pure, synchronous helper functions executed via `AsyncDbWorker` (independently callable and testable).
- **Module Organization**:
  - Used in only one script → stay declared in that script.
  - Shared across multiple scripts within a single directory (e.g., `ingestion/`, future `panel/`) → that directory's `utils.py`.
  - Shared across multiple directories under `etfportfolio/` → `core/`.
- **Replacement Over Deprecation**: Prefer replacing old functionality cleanly rather than accumulating deprecated alternatives.