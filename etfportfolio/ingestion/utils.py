"""Ingestion-shared helpers

Content-addressing, snapshot blob store, and the prices/sentiment timeseries
overlap-validate / replace / upsert path (including mismatch-triggered
cold_storage archives).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
import orjson
import xxhash
import zstandard as zstd

OVERLAP_CALENDAR_DAYS = 7
FETCH_MARGIN_DAYS = 2

_TYPE_RANK = {type(None): 0, bool: 1, int: 2, float: 2, str: 3, list: 4, dict: 5}


def _sort_key(value: Any) -> tuple[int, str]:
    return (
        _TYPE_RANK.get(type(value), 6),
        orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode(),
    )


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _canonicalize(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return sorted((_canonicalize(v) for v in value), key=_sort_key)
    return value


def canonical_bytes(payload: Any) -> bytes:
    """Return deterministically sorted canonical JSON bytes."""
    return orjson.dumps(_canonicalize(payload), option=orjson.OPT_SORT_KEYS)


def content_address(payload: Any) -> tuple[int, bytes]:
    """Returns (hash, compressed_bytes) ready for bronze.payload_blobs.

    Hash is an unsigned 64-bit int (xxh3_64_intdigest with seed=0) compatible with UBIGINT.
    """
    canonical = canonical_bytes(payload)
    digest = xxhash.xxh3_64_intdigest(canonical, seed=0)
    compressed = zstd.ZstdCompressor(level=3).compress(canonical)
    return digest, compressed


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
    """Garbage-collects an old preview blob if it is no longer referenced anywhere."""
    if old_hash is None:
        return False

    row = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT hash FROM bronze.snapshots WHERE hash = $1
            UNION ALL
            SELECT hash FROM bronze.snapshot_previews WHERE hash = $1
        ) AS refs
        """,
        [old_hash],
    ).fetchone()
    referenced = row[0] if row else 0

    if referenced == 0:
        conn.execute("DELETE FROM bronze.payload_blobs WHERE hash = $1", [old_hash])
        return True
    return False


@dataclass(frozen=True)
class SeriesSpec:
    """Table/column layout for one bronze timeseries + its cold_storage archive."""

    bronze_table: str
    cold_table: str
    columns: tuple[str, ...]
    value_columns: tuple[str, ...]


PRICES_SPEC = SeriesSpec(
    bronze_table="bronze.prices",
    cold_table="cold_storage.prices",
    columns=("open", "high", "low", "close", "volume", "average", "bar_count"),
    value_columns=("open", "high", "low", "close", "volume", "average"),
)

SENTIMENT_SPEC = SeriesSpec(
    bronze_table="bronze.sentiment",
    cold_table="cold_storage.sentiment",
    columns=("svolatility", "sdispersion", "svscore", "sbuzz", "svolume", "sdelta", "sscore", "smean"),
    value_columns=("svolatility", "sdispersion", "svscore", "sbuzz", "svolume", "sdelta", "sscore", "smean"),
)


def overlap_start_for(last_date: datetime) -> datetime:
    """Closed-closed window W starts at last_date minus OVERLAP_CALENDAR_DAYS."""
    return last_date - timedelta(days=OVERLAP_CALENDAR_DAYS)


def validate_overlap(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    product_id: int,
    new_points: dict[datetime, dict[str, Any]],
    last_date: datetime,
) -> tuple[bool, str | None]:
    """Set-equality checksum on W = [last_date - 7d, last_date] (closed-closed).

    Dates outside W are ignored (fetch margin before W; new tail after last_date).
    Value columns are compared with math.isclose; missing/None on either side is skipped.
    Returns (is_valid, mismatch_type) where mismatch_type is None, 'date_mismatch',
    or 'value_mismatch'.
    """
    start = overlap_start_for(last_date)
    col_sql = ", ".join(("date", *spec.columns))
    existing_rows = conn.execute(
        f"""
        SELECT {col_sql}
        FROM {spec.bronze_table}
        WHERE product_id = $1 AND date >= $2 AND date <= $3
        """,
        [product_id, start, last_date],
    ).fetchall()

    existing_dates: set[datetime] = set()
    existing_vals: dict[datetime, dict[str, Any]] = {}
    for row in existing_rows:
        d = row[0]
        existing_dates.add(d)
        existing_vals[d] = {col: row[i + 1] for i, col in enumerate(spec.columns)}

    new_in_w = {d: vals for d, vals in new_points.items() if start <= d <= last_date}
    new_dates = set(new_in_w)

    if existing_dates != new_dates:
        return False, "date_mismatch"

    for d, new_vals in new_in_w.items():
        old_vals = existing_vals[d]
        for key in spec.value_columns:
            v1, v2 = old_vals.get(key), new_vals.get(key)
            if v1 is not None and v2 is not None and not math.isclose(float(v1), float(v2), rel_tol=1e-4, abs_tol=1e-4):
                return False, "value_mismatch"

    return True, None


def _insert_points(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    product_id: int,
    points: dict[datetime, dict[str, Any]],
    now: datetime,
) -> None:
    col_sql = ", ".join(("product_id", "date", *spec.columns, "updated_at"))
    placeholders = ", ".join(f"${i + 1}" for i in range(len(spec.columns) + 3))
    sql = f"INSERT INTO {spec.bronze_table} ({col_sql}) VALUES ({placeholders})"
    for bar_date, point in points.items():
        params: list[Any] = [product_id, bar_date]
        params.extend(point.get(col) for col in spec.columns)
        params.append(now)
        conn.execute(sql, params)


def replace_series(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    product_id: int,
    points: dict[datetime, dict[str, Any]],
    *,
    archive: bool = False,
    reason: str | None = None,
) -> None:
    """Replace all bronze rows for a product. Optionally archive first (same txn).

    `archive=True` is only for mismatch-triggered replace. `--force` and first
    fill pass archive=False.
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    conn.execute("BEGIN TRANSACTION")
    try:
        if archive:
            if not reason:
                raise ValueError("archive=True requires a mismatch reason")
            col_sql = ", ".join(("product_id", "run_id", "date", *spec.columns, "reason"))
            select_cols = ", ".join(("product_id", "$2", "date", *spec.columns, "$3"))
            conn.execute(
                f"""
                INSERT INTO {spec.cold_table} ({col_sql})
                SELECT {select_cols}
                FROM {spec.bronze_table}
                WHERE product_id = $1
                """,
                [product_id, now, reason],
            )
        conn.execute(
            f"DELETE FROM {spec.bronze_table} WHERE product_id = $1",
            [product_id],
        )
        _insert_points(conn, spec, product_id, points, now)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def upsert_series(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    product_id: int,
    points: dict[datetime, dict[str, Any]],
) -> None:
    """Upsert points (the incremental tail, date > last_date). Overlap is not written."""
    now = datetime.now(UTC).replace(tzinfo=None)
    col_sql = ", ".join(("product_id", "date", *spec.columns, "updated_at"))
    placeholders = ", ".join(f"${i + 1}" for i in range(len(spec.columns) + 3))
    assignments = ", ".join(f"{col} = EXCLUDED.{col}" for col in (*spec.columns, "updated_at"))
    sql = f"""
    INSERT INTO {spec.bronze_table} ({col_sql})
    VALUES ({placeholders})
    ON CONFLICT (product_id, date) DO UPDATE SET
        {assignments}
    """
    conn.execute("BEGIN TRANSACTION")
    try:
        for bar_date, point in points.items():
            params: list[Any] = [product_id, bar_date]
            params.extend(point.get(col) for col in spec.columns)
            params.append(now)
            conn.execute(sql, params)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def is_fresh(last_seen: datetime | None, hours: float) -> bool:
    """True iff `last_seen` is within `hours` of now.

    Naive datetimes are treated as UTC. `None` is never fresh.
    """
    if last_seen is None:
        return False
    now = datetime.now(UTC)
    if last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=UTC)
    delta = max(0.0, (now - last_seen).total_seconds())
    return delta <= (hours * 3600.0)
