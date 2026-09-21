"""Shared synchronous timeseries engine for bronze price and FX series."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from etfportfolio.ingest.utils import is_fresh

logger = logging.getLogger(__name__)

OVERLAP_CALENDAR_DAYS = 14
FETCH_MARGIN_DAYS = 2
MIN_REFETCH_RETENTION_RATIO = 0.90


@dataclass(frozen=True)
class SeriesSpec:
    bronze_table: str
    cold_table: str
    status_table: str
    entity_columns: tuple[str, ...]
    data_columns: tuple[str, ...]
    tolerance_columns: tuple[str, ...]
    rel_tol: float = 1e-4
    abs_tol: float = 0.01
    settlement_rel_tol: float = 0.01
    settlement_abs_tol: float = 0.10
    settlement_trading_days: int = 5
    detect_splits: bool = True


@dataclass(frozen=True)
class SeriesStatus:
    last_date: datetime | None
    last_updated: datetime | None
    last_checked_at: datetime | None
    status: str | None  # 'ok', 'no_data', 'error', or None


def coerce_entity_key(entity_id: Any) -> tuple[Any, ...]:
    """Normalize a scalar entity id to a 1-tuple; leave tuples unchanged."""
    if isinstance(entity_id, tuple):
        return entity_id
    return (entity_id,)


def build_entity_where(
    entity_columns: tuple[str, ...],
    prefix: str = "",
    start_param: int = 1,
) -> tuple[str, int]:
    """Build `col = $N` conjunctions for entity columns.

    Returns (clause, next_parameter_index).
    """
    clauses = [f"{prefix}{col} = ${start_param + i}" for i, col in enumerate(entity_columns)]
    return " AND ".join(clauses), start_param + len(entity_columns)


def overlap_start_for(last_date: datetime, calendar_days: int = OVERLAP_CALENDAR_DAYS) -> datetime:
    """Closed-closed window W starts at last_date minus calendar_days."""
    return last_date - timedelta(days=calendar_days)


def is_series_fresh(
    status: SeriesStatus | None,
    target_date: datetime,
    hours: float,
) -> bool:
    """Evaluate series freshness with differentiated status dampening.

    1. Fresh if bars reach target_date.
    2. Fresh if checked/updated within hours and status was 'ok' or 'no_data'.
    3. An 'error' or None status is NEVER fresh (unless bars already reach target).
    """
    if not status:
        return False
    if status.last_date is not None and status.last_date >= target_date:
        return True
    if status.status in ("ok", "no_data"):
        return is_fresh(status.last_checked_at, hours) or is_fresh(status.last_updated, hours)
    return False


def get_last_date(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    entity_id: Any,
) -> datetime | None:
    """Return MAX(date) for the entity in spec.bronze_table."""
    entity_vals = coerce_entity_key(entity_id)
    where, _ = build_entity_where(spec.entity_columns)
    row = conn.execute(
        f"SELECT MAX(date) FROM {spec.bronze_table} WHERE {where}",
        list(entity_vals),
    ).fetchone()
    return row[0] if row and row[0] else None


def get_series_count(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    entity_id: Any,
) -> int:
    """Return COUNT(*) for the entity in spec.bronze_table."""
    entity_vals = coerce_entity_key(entity_id)
    where, _ = build_entity_where(spec.entity_columns)
    row = conn.execute(
        f"SELECT COUNT(*) FROM {spec.bronze_table} WHERE {where}",
        list(entity_vals),
    ).fetchone()
    return row[0] if row else 0


def _is_uniform_ratio_shift(
    dates: list[datetime],
    existing_vals: dict[datetime, dict[str, Any]],
    new_in_w: dict[datetime, dict[str, Any]],
) -> bool:
    """Detect a multiplicative restatement (split/dividend) on close prices."""
    if len(dates) < 2:
        return False
    ratios: list[float] = []
    for d in dates:
        c_old = existing_vals[d].get("close")
        c_new = new_in_w[d].get("close")
        if c_old is None or c_new is None:
            continue
        c_old_f = float(c_old)
        c_new_f = float(c_new)
        if c_old_f > 0:
            ratios.append(c_new_f / c_old_f)
    if len(ratios) < 2:
        return False
    mean_r = sum(ratios) / len(ratios)
    variance = sum((r - mean_r) ** 2 for r in ratios) / len(ratios)
    std_r = math.sqrt(variance)
    return std_r <= 1e-3 and abs(mean_r - 1.0) > 1e-3


def validate_overlap(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    entity_id: Any,
    new_points: dict[datetime, dict[str, Any]],
    last_date: datetime,
) -> tuple[bool, str | None]:
    """Set-equality checksum on W = [last_date - 14d, last_date] (closed-closed).

    Dates outside W are ignored (fetch margin before W; new tail after last_date).
    Tolerance columns are compared with calibrated tolerances; missing/None on
    either side is skipped. Returns (is_valid, mismatch_type) where mismatch_type
    is None, 'date_mismatch', 'value_mismatch', 'corporate_action', or 'restatement'.
    """
    entity_vals = coerce_entity_key(entity_id)
    start = overlap_start_for(last_date)
    col_sql = ", ".join(("date", *spec.data_columns))
    where, nxt = build_entity_where(spec.entity_columns)
    existing_rows = conn.execute(
        f"""
        SELECT {col_sql}
        FROM {spec.bronze_table}
        WHERE {where} AND date >= ${nxt} AND date <= ${nxt + 1}
        ORDER BY date ASC
        """,
        [*entity_vals, start, last_date],
    ).fetchall()

    existing_dates = [row[0] for row in existing_rows]
    existing_set = set(existing_dates)
    existing_vals: dict[datetime, dict[str, Any]] = {
        row[0]: {col: row[i + 1] for i, col in enumerate(spec.data_columns)} for row in existing_rows
    }

    new_in_w = {d: vals for d, vals in new_points.items() if start <= d <= last_date}
    new_dates = set(new_in_w)

    if existing_set != new_dates:
        return False, "date_mismatch"

    mismatched_dates: list[datetime] = []
    for d in sorted(new_in_w):
        old_vals = existing_vals[d]
        new_vals = new_in_w[d]
        diff_found = False
        for key in spec.tolerance_columns:
            v1, v2 = old_vals.get(key), new_vals.get(key)
            if (
                v1 is not None
                and v2 is not None
                and not math.isclose(float(v1), float(v2), rel_tol=spec.rel_tol, abs_tol=spec.abs_tol)
            ):
                diff_found = True
                break
        if diff_found:
            mismatched_dates.append(d)

    if not mismatched_dates:
        return True, None

    recent_dates = (
        set(existing_dates[-spec.settlement_trading_days :])
        if len(existing_dates) >= spec.settlement_trading_days
        else set(existing_dates)
    )
    core_dates = [d for d in existing_dates if d not in recent_dates]
    core_mismatches = [d for d in mismatched_dates if d in core_dates]

    if core_mismatches:
        return False, "corporate_action" if spec.detect_splits else "restatement"

    recent_mismatches = [d for d in mismatched_dates if d in recent_dates]
    if spec.detect_splits and _is_uniform_ratio_shift(recent_mismatches, existing_vals, new_in_w):
        return False, "corporate_action"

    for d in recent_mismatches:
        old_bar = existing_vals[d]
        new_bar = new_in_w[d]
        for key in spec.tolerance_columns:
            v1, v2 = old_bar.get(key), new_bar.get(key)
            if v1 is not None and v2 is not None:
                f1, f2 = float(v1), float(v2)
                if not math.isclose(f1, f2, rel_tol=spec.rel_tol, abs_tol=spec.abs_tol):
                    abs_diff = abs(f2 - f1)
                    rel_diff = abs_diff / abs(f1) if f1 != 0 else float("inf")
                    if abs_diff > spec.settlement_abs_tol and rel_diff > spec.settlement_rel_tol:
                        return False, "value_mismatch"

    entity_label = entity_vals if len(entity_vals) > 1 else entity_vals[0]
    logger.info(
        "Entity %s: accepted bounded settlement revision on %s; overwriting with official settlement.",
        entity_label,
        [d.strftime("%Y-%m-%d") for d in mismatched_dates],
    )
    return True, None


def _insert_points(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    entity_vals: tuple[Any, ...],
    points: dict[datetime, dict[str, Any]],
    now: datetime,
) -> None:
    cols = (*spec.entity_columns, "date", *spec.data_columns, "updated_at")
    col_sql = ", ".join(cols)
    placeholders = ", ".join(f"${i + 1}" for i in range(len(cols)))
    sql = f"INSERT INTO {spec.bronze_table} ({col_sql}) VALUES ({placeholders})"
    for bar_date, point in points.items():
        params: list[Any] = [*entity_vals, bar_date]
        params.extend(point.get(col) for col in spec.data_columns)
        params.append(now)
        conn.execute(sql, params)


def replace_series(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    entity_id: Any,
    points: dict[datetime, dict[str, Any]],
    *,
    archive: bool = False,
    reason: str | None = None,
) -> None:
    """Replace all bronze rows for an entity. Optionally archive first (same txn).

    `archive=True` is only for mismatch-triggered replace. `--force` and first
    fill pass archive=False.
    """
    entity_vals = coerce_entity_key(entity_id)
    now = datetime.now(UTC).replace(tzinfo=None)
    where, _ = build_entity_where(spec.entity_columns)
    conn.execute("BEGIN TRANSACTION")
    try:
        if archive:
            if not reason:
                raise ValueError("archive=True requires a mismatch reason")
            insert_cols = (*spec.entity_columns, "run_id", "date", *spec.data_columns, "reason")
            run_id_param = len(entity_vals) + 1
            reason_param = run_id_param + 1
            select_parts = [
                *spec.entity_columns,
                f"${run_id_param}",
                "date",
                *spec.data_columns,
                f"${reason_param}",
            ]
            conn.execute(
                f"""
                INSERT INTO {spec.cold_table} ({", ".join(insert_cols)})
                SELECT {", ".join(select_parts)}
                FROM {spec.bronze_table}
                WHERE {where}
                """,
                [*entity_vals, now, reason],
            )
        conn.execute(
            f"DELETE FROM {spec.bronze_table} WHERE {where}",
            list(entity_vals),
        )
        _insert_points(conn, spec, entity_vals, points, now)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def upsert_series(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    entity_id: Any,
    points: dict[datetime, dict[str, Any]],
) -> None:
    """Upsert points (overlap window bars and incremental tail).

    Overlapping bars are updated in place, refreshing updated_at.
    """
    entity_vals = coerce_entity_key(entity_id)
    now = datetime.now(UTC).replace(tzinfo=None)
    cols = (*spec.entity_columns, "date", *spec.data_columns, "updated_at")
    col_sql = ", ".join(cols)
    placeholders = ", ".join(f"${i + 1}" for i in range(len(cols)))
    conflict_cols = ", ".join((*spec.entity_columns, "date"))
    assignments = ", ".join(f"{col} = EXCLUDED.{col}" for col in (*spec.data_columns, "updated_at"))
    sql = f"""
    INSERT INTO {spec.bronze_table} ({col_sql})
    VALUES ({placeholders})
    ON CONFLICT ({conflict_cols}) DO UPDATE SET
        {assignments}
    """
    conn.execute("BEGIN TRANSACTION")
    try:
        for bar_date, point in points.items():
            params: list[Any] = [*entity_vals, bar_date]
            params.extend(point.get(col) for col in spec.data_columns)
            params.append(now)
            conn.execute(sql, params)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def record_series_status(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    entity_id: Any,
    status: str,
    error_message: str | None = None,
) -> None:
    """Upsert status for an entity, truncating error_message to 500 chars."""
    entity_vals = coerce_entity_key(entity_id)
    now = datetime.now(UTC).replace(tzinfo=None)
    truncated_msg = error_message[:500] if error_message else None
    cols = (*spec.entity_columns, "last_checked_at", "status", "error_message")
    placeholders = ", ".join(f"${i + 1}" for i in range(len(cols)))
    conflict_cols = ", ".join(spec.entity_columns)
    conn.execute(
        f"""
        INSERT INTO {spec.status_table} ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols}) DO UPDATE SET
            last_checked_at = EXCLUDED.last_checked_at,
            status = EXCLUDED.status,
            error_message = EXCLUDED.error_message
        """,
        [*entity_vals, now, status, truncated_msg],
    )


def load_series_status(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
) -> dict[Any, SeriesStatus]:
    """Load entity -> SeriesStatus via a full outer join of bronze and status tables.

    Single-column entity keys are returned as scalars; composite keys as tuples.
    """
    coalesced = ", ".join(f"COALESCE(b.{col}, s.{col}) AS {col}" for col in spec.entity_columns)
    join_on = " AND ".join(f"b.{col} = s.{col}" for col in spec.entity_columns)
    group_by = ", ".join(str(i + 1) for i in range(len(spec.entity_columns)))
    n_ent = len(spec.entity_columns)
    rows = conn.execute(
        f"""
        SELECT
            {coalesced},
            MAX(b.date) AS last_date,
            MAX(b.updated_at) AS last_updated,
            MAX(s.last_checked_at) AS last_checked_at,
            MAX(s.status) AS status
        FROM {spec.bronze_table} b
        FULL OUTER JOIN {spec.status_table} s
          ON {join_on}
        GROUP BY {group_by}
        """
    ).fetchall()

    result: dict[Any, SeriesStatus] = {}
    for row in rows:
        if n_ent == 1:
            key: Any = row[0]
        else:
            key = tuple(row[i] for i in range(n_ent))
        result[key] = SeriesStatus(
            last_date=row[n_ent],
            last_updated=row[n_ent + 1],
            last_checked_at=row[n_ent + 2],
            status=row[n_ent + 3],
        )
    return result


def has_historical_series_change(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    entity_id: Any,
    new_bars: dict[datetime, dict[str, Any]],
    cutoff_date: datetime,
) -> bool:
    """True if any historical bar (date < cutoff_date) is missing from new_bars or differs."""
    entity_vals = coerce_entity_key(entity_id)
    col_sql = ", ".join(("date", *spec.tolerance_columns))
    where, nxt = build_entity_where(spec.entity_columns)
    rows = conn.execute(
        f"""
        SELECT {col_sql}
        FROM {spec.bronze_table}
        WHERE {where} AND date < ${nxt}
        """,
        [*entity_vals, cutoff_date],
    ).fetchall()

    for row in rows:
        d = row[0]
        new_val = new_bars.get(d)
        if new_val is None:
            return True
        for i, col in enumerate(spec.tolerance_columns):
            v_old = row[i + 1]
            v_new = new_val.get(col)
            if (
                v_old is not None
                and v_new is not None
                and not math.isclose(float(v_old), float(v_new), rel_tol=spec.rel_tol, abs_tol=spec.abs_tol)
            ):
                return True
    return False


def clean_series_cold_storage(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
) -> int:
    """Purge redundant cold-storage runs whose historical bars match bronze.

    A run is redundant if all bars older than settlement_trading_days (+ 2 calendar
    days of buffer) match bronze within spec tolerances. Returns deleted row count.
    """
    entity_cols_sql = ", ".join(spec.entity_columns)
    runs = conn.execute(f"SELECT DISTINCT {entity_cols_sql}, run_id FROM {spec.cold_table}").fetchall()
    deleted_rows = 0
    n_ent = len(spec.entity_columns)

    join_on = " AND ".join(f"c.{col} = b.{col}" for col in spec.entity_columns)
    join_on += " AND c.date = b.date"

    col_conditions = []
    for i, col in enumerate(spec.tolerance_columns):
        abs_p = 1 + n_ent + 2 + (2 * i)  # after entity, run_id, cutoff
        rel_p = abs_p + 1
        col_conditions.append(
            f"(c.{col} IS NULL AND b.{col} IS NOT NULL) OR "
            f"(c.{col} IS NOT NULL AND b.{col} IS NULL) OR "
            f"(c.{col} IS NOT NULL AND b.{col} IS NOT NULL AND "
            f"(abs(c.{col} - b.{col}) > ${abs_p} OR "
            f"abs(c.{col} - b.{col}) / nullif(abs(c.{col}), 0) > ${rel_p}))"
        )
    value_cols_predicate = " OR\n        ".join(col_conditions)

    where_entity, nxt = build_entity_where(spec.entity_columns, prefix="c.", start_param=1)
    mismatch_sql = f"""
        SELECT count(*)
        FROM {spec.cold_table} c
        LEFT JOIN {spec.bronze_table} b ON {join_on}
        WHERE {where_entity} AND c.run_id = ${nxt} AND c.date <= ${nxt + 1}
        AND (
            b.date IS NULL OR
            {value_cols_predicate}
        )
    """

    max_where, max_nxt = build_entity_where(spec.entity_columns, start_param=1)
    max_sql = f"SELECT MAX(date) FROM {spec.cold_table} WHERE {max_where} AND run_id = ${max_nxt}"
    del_where, del_nxt = build_entity_where(spec.entity_columns, start_param=1)
    del_sql = f"DELETE FROM {spec.cold_table} WHERE {del_where} AND run_id = ${del_nxt}"

    for row in runs:
        entity_vals = tuple(row[:n_ent])
        run_id = row[n_ent]

        max_date_row = conn.execute(max_sql, [*entity_vals, run_id]).fetchone()
        if not max_date_row or max_date_row[0] is None:
            continue
        max_date = max_date_row[0]
        cutoff_date = max_date - timedelta(days=spec.settlement_trading_days + 2)

        params: list[Any] = [*entity_vals, run_id, cutoff_date]
        for _ in spec.tolerance_columns:
            params.extend([spec.abs_tol, spec.rel_tol])

        mismatch_row = conn.execute(mismatch_sql, params).fetchone()
        mismatches = mismatch_row[0] if mismatch_row is not None else 0

        if mismatches == 0:
            del_row = conn.execute(del_sql, [*entity_vals, run_id]).fetchone()
            count = del_row[0] if del_row is not None else 0
            deleted_rows += count
            logger.info(
                "Entity %s (run %s): deleted %d redundant cold storage rows from %s.",
                entity_vals if n_ent > 1 else entity_vals[0],
                run_id,
                count,
                spec.cold_table,
            )

    return deleted_rows
