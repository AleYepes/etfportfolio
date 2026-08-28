from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

import duckdb

from etfportfolio.core.config import settings

_current: ContextVar[duckdb.DuckDBPyConnection | None] = ContextVar("_current", default=None)
_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def apply_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Applies schema.sql idempotently to the database connection."""
    schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    conn.execute(schema_sql)


@contextmanager
def connect(db_path: str | None = None):
    """Context manager for DuckDB connection with automatic schema application."""
    target_path = db_path or settings.db_path
    Path(target_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(target_path)
    apply_schema(conn)
    token = _current.set(conn)
    try:
        yield conn
    finally:
        _current.reset(token)
        conn.close()


def current() -> duckdb.DuckDBPyConnection:
    """Returns the currently active DuckDB connection in the context."""
    conn = _current.get()
    if conn is None:
        raise RuntimeError("No active DB connection — call within `with etfportfolio.core.db.connect():`")
    return conn


# --- Content-addressed blob storage -----------------------------------------
# All bronze.payload_blobs read/write/GC logic lives here, alongside the rest
# of the DB layer. snapshots.py, series.py, and landing.py all write blobs
# through store_blob() rather than each carrying their own copy of the SQL.


def store_blob(conn: duckdb.DuckDBPyConnection, digest: int, compressed: bytes) -> None:
    """Ensures a content-addressed payload blob is stored (idempotent on duplicate hash)."""
    conn.execute(
        """
        INSERT INTO bronze.payload_blobs (hash, payload)
        VALUES ($1, $2)
        ON CONFLICT (hash) DO NOTHING
        """,
        [digest, compressed],
    )


def gc_preview_blob(conn: duckdb.DuckDBPyConnection, old_hash: int | None) -> bool:
    """Garbage-collects an old payload blob if it is no longer referenced anywhere in bronze."""
    if old_hash is None:
        return False

    # Check references across snapshots, series, and snapshot_previews
    query = """
    SELECT
        (SELECT COUNT(*) FROM bronze.snapshots WHERE hash = $1) +
        (SELECT COUNT(*) FROM bronze.series WHERE hash = $1) +
        (SELECT COUNT(*) FROM bronze.snapshot_previews WHERE hash = $1) AS ref_count
    """
    res = conn.execute(query, [old_hash]).fetchone()
    ref_count = res[0] if res else 0

    if ref_count == 0:
        conn.execute("DELETE FROM bronze.payload_blobs WHERE hash = $1", [old_hash])
        return True
    return False
