from contextlib import contextmanager
from contextvars import ContextVar
import duckdb

_current: ContextVar[duckdb.DuckDBPyConnection | None] = ContextVar("_current", default=None)

@contextmanager
def connect():
    conn = duckdb.connect(settings.db_path)
    apply_schema(conn)   # executes schema.sql, idempotent
    token = _current.set(conn)
    try:
        yield conn
    finally:
        _current.reset(token)
        conn.close()

def current() -> duckdb.DuckDBPyConnection:
    conn = _current.get()
    if conn is None:
        raise RuntimeError("No active DB connection — call within `with core.db.connect():`")
    return conn