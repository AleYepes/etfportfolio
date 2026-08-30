# Functional Requirements Document: Asynchronous Database Access Architecture for Ingestion Pipeline V2

## 1. Introduction

### 1.1 Purpose
This document defines a comprehensive re-architecture of database access in the ingestion pipeline. The original V1 proposal introduced a background writer thread to eliminate event-loop blocking caused by synchronous DuckDB writes. This blocking caused severe memory issues and performance degradation while running API request loops with Interactive Brokers (IB) Gateway. The revised V2 plan expands that idea into a full-scale, uniform architecture where **all** database access from asynchronous code is offloaded to a dedicated worker thread. This eliminates event-loop blocking, improves clarity, and decouples async orchestration from synchronous persistence logic.

### 1.2 Scope
The refactor applies to **all ingestion modules** that perform database operations inside asynchronous functions:
- `products.py`
- `contracts.py`
- `prices.py`
- `themes.py`
- `details.py`
- `landing.py`
- `snapshots.py`
- `sentiment.py`

The new architecture is designed to be the standard pattern for any future async module that needs persistence.

## 2. Guiding Principles

1. **Asynchronous code must never block** on DuckDB I/O or CPU-bound work.
2. **All DB access in a phase flows through a single worker** that owns a dedicated DuckDB connection.
3. **Async functions contain no direct SQL**; all SQL lives in small synchronous helper functions submitted to the worker.
4. **Each phase owns its worker lifecycle** – top-level functions create and close their own worker; submodules receive it explicitly.
5. **Legacy connection management is removed** – `connect()` and `current()` are deleted.
6. **The codebase remains clear, simple, clean, and testable** – no hidden global state, no speculative abstractions.
7. **No migration or backfill** is required; the database will be recreated after implementation.

## 3. Problem Analysis

### 3.1 Background
The original pipeline used a context-managed connection (`connect()`) passed through async functions. Synchronous DuckDB writes inside `async` functions blocked the event loop, preventing timely acknowledgment of IB Gateway responses. The gateway then accumulated internal “orphan EC” objects, causing memory pressure, slowdowns, and potential crashes.

### 3.2 Beyond Blocking
Even with a write-only background queue, several architectural issues remain:
- Reads and writes mix in async functions, making offloading difficult and error-prone.
- Multiple connections may exist concurrently, risking file access conflicts.
- Hidden connection ownership via `current()` makes the code hard to reason about.
- Inline SQL scattered through async logic obscures the blocking points and reduces testability.

The V2 architecture addresses all of these by making the worker the sole DB access point for a phase and by strictly separating async orchestration from synchronous SQL.

## 4. Proposed Solution: `AsyncDbWorker`

### 4.1 Location and Responsibilities
The worker will be implemented in `etfportfolio/core/db.py`, alongside existing DB helpers (`apply_schema`, `store_blob`, `gc_preview_blob`). It is responsible for:
- Creating and owning a dedicated DuckDB connection (not thread-shared).
- Applying `schema.sql` automatically upon connection creation.
- Executing submitted tasks sequentially in a dedicated thread.
- Providing a non-blocking API to async coroutines.
- Gracefully shutting down after draining all queued work.

### 4.2 API

```python
class AsyncDbWorker:
    def __init__(self, db_path: str):
        """
        Create connection, apply schema, start worker thread.
        Must be instantiated inside an async function so that the running
        event loop can be captured for future result propagation.
        """

    def enqueue(self, func, *args, **kwargs) -> None:
        """Fire-and-forget write task. Exceptions are logged and swallowed."""

    async def submit(self, func, *args, **kwargs):
        """Submit a read/result-returning task. Returns the result or raises the exception."""

    async def close(self) -> None:
        """Signal shutdown, wait for all queued tasks to finish, and join thread."""
```

### 4.3 Implementation Details

- The worker runs a daemon thread with a `queue.Queue`.
- The worker stores the current event loop via `asyncio.get_running_loop()` at initialization. This is required so that `submit` can safely propagate results back to the async caller without blocking.
- The thread loop repeatedly pulls tasks; for each task, it calls `func(conn, *args, **kwargs)` where `conn` is the worker’s own DuckDB connection.
- For `enqueue`, exceptions are caught, logged, and the loop continues.
- For `submit`, the method creates an `asyncio.Future` internally, enqueues the task, and awaits the future. The worker thread sets the result or exception on that future using `loop.call_soon_threadsafe(future.set_result, result)` or `loop.call_soon_threadsafe(future.set_exception, exc)`. This ensures the event loop is never blocked.
- `close()` enqueues a sentinel, then awaits an `asyncio.Future` that the worker sets just before exiting. This ensures all writes are flushed and the connection is closed before the phase returns.
- The worker constructor creates the database file parent directory if needed, opens the connection, and calls `apply_schema(conn)`.

### 4.4 Schema and Helper Functions

- `apply_schema` remains and is called once by the worker.
- `store_blob` and `gc_preview_blob` remain in `core/db.py` and are submitted to the worker when needed.
- `gc_preview_blob` will be fixed to remove the reference to non-existent `bronze.series` (the old series table was a vestige).
- `connect()` and `current()` will be **deleted**.

## 5. Refactored Architecture

### 5.1 Async Orchestration and Sync SQL Helpers

Every ingestion module will be refactored so that:
- Async functions **only** perform API calls, control flow, and worker submissions.
- All `conn.execute` calls are moved into small, named synchronous helper functions (e.g., `select_existing_prices`, `upsert_prices_rows`). These helpers accept `conn` as the first argument and are submitted via `worker.submit` or `worker.enqueue`.
- No inline SQL may appear in async code.
- Reads that produce data needed for decisions are submitted with `submit`; writes are submitted with `enqueue` and not awaited.

### 5.2 Worker Lifecycle and Ownership

- **Top-level entry points** (e.g., `products.sync`, `contracts.sync`, `prices.sync`, `themes.sync`, `details._run_details_phase`) create their own `AsyncDbWorker`, use it for all DB access, and close it before returning.
- **Submodules within a shared phase** (e.g., `landing.fetch_and_gate`, `snapshots.fetch_snapshot`, `sentiment.fetch_incremental`) receive the worker as an explicit parameter. They never create their own.
- No function accepts an external `conn` parameter; the worker is the only DB handle.
- `resolve_target_ids` will be submitted through the details phase’s worker as a read task.

### 5.3 CLI Layer

- CLI methods in `pipeline.py` become thin wrappers that call the appropriate async sync function via `asyncio.run(...)`. No outer `with connect()` block remains.
- `main.py` remains unchanged apart from removing any direct DB connection usage.

## 6. Error Handling

- **API-level errors** (e.g., `SessionInvalidError`) are detected in async code **before** any worker submission and propagate normally, aborting the phase as needed.
- **Write tasks** (`enqueue`) are fire-and-forget: exceptions are logged and swallowed, allowing the phase to continue. The pipeline reports success based on API results, not DB write success.
- **Read tasks** (`submit`) propagate exceptions to the async caller. The caller may decide to abort the phase or handle the error. This distinction preserves fail-fast behavior when a read is critical while avoiding cascading failures from individual write problems.
- All worker task exceptions are logged with full context.

## 7. Cleanup and Migration

- The old DuckDB file will be deleted; the new worker will create a fresh database and apply the current schema. No migration scripts are needed.
- Legacy connection utilities (`connect`, `current`) are removed.
- Any remaining references to `bronze.series` are cleaned up (in `gc_preview_blob`).
- The syntax error in `contracts.py` (`except AttributeError, KeyError:`) will be corrected to use parentheses.

## 8. Success Criteria

- No synchronous DuckDB calls remain inside `async def` functions in ingestion modules.
- All DB access in a phase is routed through a single `AsyncDbWorker`.
- Worker shutdown is non-blocking and guarantees all queued writes are flushed.
- The pipeline no longer blocks the event loop during DB operations, eliminating IB Gateway orphan EC buildup.
- The code is demonstrably cleaner and more testable: async functions contain no SQL, and SQL helpers are independently callable.
- Existing functionality is preserved; all phases still produce the same data outputs.

## 9. Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| DuckDB connection not thread-safe | Worker owns a single connection and uses it exclusively in its own thread. |
| Queue grows faster than worker can process | Worker executes sequentially; individual tasks are fast. If needed, batch SQL can be optimized. |
| Loss of writes on abrupt termination | Sentinels and `close()` draining ensure graceful flush; only the currently executing task might be lost (same as current behavior). |
| Increased complexity from worker bridging | The worker is a small, self-contained utility; all concurrency is encapsulated, leaving callers simple. |
| Testing challenges | Sync SQL helpers can be tested directly; async orchestration can be tested with a mock worker. |