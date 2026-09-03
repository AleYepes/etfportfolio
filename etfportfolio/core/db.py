import asyncio
import logging
import queue
import threading
from pathlib import Path

import duckdb

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"

logger = logging.getLogger(__name__)


def apply_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Applies schema.sql idempotently to the database connection."""
    schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    conn.execute(schema_sql)


class AsyncDbWorker:
    """Runs all DuckDB operations on a dedicated worker thread.

    Instances are created inside async functions (so the running event loop
    is captured) and are used as async context managers:

        async with AsyncDbWorker(settings.db_path) as worker:
            result = await worker.submit(some_sync_helper, arg1, arg2)

    The worker owns a single DuckDB connection, applies the schema on
    startup, and executes submitted tasks sequentially. All tasks must be
    synchronous functions whose first argument is the DuckDB connection.
    """

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

    def _run(self) -> None:
        """Worker thread loop: process tasks until sentinel."""
        while True:
            item = self._queue.get()
            if item is None:  # sentinel
                break

            func, args, kwargs, future = item
            try:
                result = func(self._conn, *args, **kwargs)
                if future is not None:
                    self._loop.call_soon_threadsafe(future.set_result, result)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error in AsyncDbWorker task")
                if future is not None:
                    self._loop.call_soon_threadsafe(future.set_exception, exc)

        if self._conn is not None:
            self._conn.close()
            self._conn = None

    async def submit(self, func, *args, **kwargs):
        """Submit a task and wait for its result."""
        future = self._loop.create_future()
        self._queue.put((func, args, kwargs, future))
        return await future

    async def close(self) -> None:
        """Signal shutdown and wait for all queued tasks to finish."""
        if self._thread.is_alive():
            self._queue.put(None)  # sentinel
            await asyncio.to_thread(self._thread.join)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
