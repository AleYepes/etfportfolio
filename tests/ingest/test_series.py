from datetime import UTC, datetime, timedelta

from etfportfolio.ingest.prices import PRICE_ABS_TOL, PRICE_REL_TOL, PRICES_SPEC
from etfportfolio.ingest.series import (
    OVERLAP_CALENDAR_DAYS,
    SeriesSpec,
    SeriesStatus,
    build_entity_where,
    clean_series_cold_storage,
    coerce_entity_key,
    has_historical_series_change,
    is_series_fresh,
    load_series_status,
    overlap_start_for,
    record_series_status,
    replace_series,
    upsert_series,
    validate_overlap,
)

FX_LIKE_SPEC = SeriesSpec(
    bronze_table="bronze.fx",
    cold_table="cold_storage.fx",
    status_table="bronze.fx_status",
    entity_columns=("source_currency", "target_currency"),
    data_columns=("open", "high", "low", "close"),
    tolerance_columns=("open", "high", "low", "close"),
    rel_tol=1e-4,
    abs_tol=1e-8,
    settlement_rel_tol=0.01,
    settlement_abs_tol=1e-6,
    settlement_trading_days=5,
    detect_splits=False,
)


def _ohlc(close: float, open_: float | None = None) -> dict:
    o = open_ if open_ is not None else close
    return {
        "open": o,
        "high": max(o, close) + 1.0,
        "low": min(o, close) - 1.0,
        "close": close,
        "volume": 1000.0,
        "average": close,
        "bar_count": 50,
    }


def _fx_ohlc(close: float) -> dict:
    return {"open": close, "high": close * 1.001, "low": close * 0.999, "close": close}


def test_coerce_entity_key_and_build_where():
    assert coerce_entity_key(101) == (101,)
    assert coerce_entity_key("EUR") == ("EUR",)
    assert coerce_entity_key(("EUR", "USD")) == ("EUR", "USD")

    clause, nxt = build_entity_where(("product_id",))
    assert clause == "product_id = $1"
    assert nxt == 2

    clause, nxt = build_entity_where(("source_currency", "target_currency"), prefix="c.", start_param=1)
    assert clause == "c.source_currency = $1 AND c.target_currency = $2"
    assert nxt == 3


def test_overlap_start_for_default_window():
    last_date = datetime(2026, 8, 20, 0, 0)
    assert overlap_start_for(last_date) == datetime(2026, 8, 6, 0, 0)
    assert overlap_start_for(last_date, calendar_days=7) == datetime(2026, 8, 13, 0, 0)


def test_is_series_fresh_differentiated():
    now = datetime.now(UTC)
    yesterday = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    old_date = yesterday - timedelta(days=10)
    recent_check = now - timedelta(hours=5)

    assert is_series_fresh(None, yesterday, 24.0) is False
    assert (
        is_series_fresh(
            SeriesStatus(last_date=old_date, last_updated=None, last_checked_at=recent_check, status="no_data"),
            yesterday,
            24.0,
        )
        is True
    )
    assert (
        is_series_fresh(
            SeriesStatus(last_date=old_date, last_updated=None, last_checked_at=recent_check, status="ok"),
            yesterday,
            24.0,
        )
        is True
    )
    assert (
        is_series_fresh(
            SeriesStatus(
                last_date=old_date,
                last_updated=None,
                last_checked_at=now - timedelta(seconds=1),
                status="error",
            ),
            yesterday,
            24.0,
        )
        is False
    )
    assert (
        is_series_fresh(
            SeriesStatus(last_date=yesterday, last_updated=None, last_checked_at=None, status=None),
            yesterday,
            24.0,
        )
        is True
    )


def test_validate_overlap_exact_match(db_conn):
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing = {last_date - timedelta(days=i): _ohlc(100.0 + i) for i in range(OVERLAP_CALENDAR_DAYS + 1)}
    replace_series(db_conn, PRICES_SPEC, 1001, existing, archive=False)

    new_points = dict(existing)
    new_points[last_date - timedelta(days=16)] = _ohlc(999.0)
    new_points[last_date + timedelta(days=1)] = _ohlc(200.0)

    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 1001, new_points, last_date)
    assert valid is True
    assert reason is None


def test_validate_overlap_date_mismatch(db_conn):
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing = {last_date - timedelta(days=i): _ohlc(100.0 + i) for i in range(OVERLAP_CALENDAR_DAYS + 1)}
    replace_series(db_conn, PRICES_SPEC, 1001, existing, archive=False)

    missing = dict(existing)
    del missing[last_date - timedelta(days=2)]
    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 1001, missing, last_date)
    assert valid is False
    assert reason == "date_mismatch"


def test_validate_overlap_core_drift_corporate_action(db_conn):
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing = {last_date - timedelta(days=i): _ohlc(50.0) for i in range(12)}
    replace_series(db_conn, PRICES_SPEC, 3001, existing, archive=False)

    new_points = {d: dict(v) for d, v in existing.items()}
    core_d = last_date - timedelta(days=10)
    new_points[core_d]["close"] = 55.0

    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 3001, new_points, last_date)
    assert valid is False
    assert reason == "corporate_action"


def test_validate_overlap_core_drift_restatement_when_splits_disabled(db_conn):
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing = {last_date - timedelta(days=i): _fx_ohlc(1.08) for i in range(12)}
    replace_series(db_conn, FX_LIKE_SPEC, ("EUR", "USD"), existing, archive=False)

    new_points = {d: dict(v) for d, v in existing.items()}
    core_d = last_date - timedelta(days=10)
    new_points[core_d]["close"] = 1.20

    valid, reason = validate_overlap(db_conn, FX_LIKE_SPEC, ("EUR", "USD"), new_points, last_date)
    assert valid is False
    assert reason == "restatement"


def test_validate_overlap_uniform_ratio_corporate_action(db_conn):
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing = {last_date - timedelta(days=i): _ohlc(50.0) for i in range(5)}
    replace_series(db_conn, PRICES_SPEC, 3002, existing, archive=False)

    new_points = {d: dict(v) for d, v in existing.items()}
    for d in [last_date, last_date - timedelta(days=1)]:
        new_points[d]["close"] = 25.0
        new_points[d]["open"] = 25.0

    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 3002, new_points, last_date)
    assert valid is False
    assert reason == "corporate_action"


def test_validate_overlap_bounded_settlement_accepted(db_conn):
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing = {
        last_date - timedelta(days=1): _ohlc(50.5),
        last_date: _ohlc(50.0),
    }
    replace_series(db_conn, PRICES_SPEC, 2002, existing, archive=False)

    new_points = {
        last_date - timedelta(days=1): dict(existing[last_date - timedelta(days=1)]),
        last_date: dict(existing[last_date]),
    }
    new_points[last_date]["close"] = 50.0 + PRICE_ABS_TOL

    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 2002, new_points, last_date)
    assert valid is True
    assert reason is None


def test_validate_overlap_excessive_settlement_value_mismatch(db_conn):
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing = {
        last_date - timedelta(days=1): _ohlc(50.5),
        last_date: _ohlc(50.0),
    }
    replace_series(db_conn, PRICES_SPEC, 2004, existing, archive=False)

    new_points = {
        last_date - timedelta(days=1): dict(existing[last_date - timedelta(days=1)]),
        last_date: dict(existing[last_date]),
    }
    new_points[last_date]["close"] = 75.0

    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 2004, new_points, last_date)
    assert valid is False
    assert reason == "value_mismatch"


def test_validate_overlap_relative_tolerance_passes(db_conn):
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing = {last_date - timedelta(days=i): _ohlc(100.0 + i) for i in range(OVERLAP_CALENDAR_DAYS + 1)}
    replace_series(db_conn, PRICES_SPEC, 1001, existing, archive=False)

    new_points = {d: dict(v) for d, v in existing.items()}
    new_points[last_date]["close"] = new_points[last_date]["close"] * (1 + 0.5 * PRICE_REL_TOL)

    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 1001, new_points, last_date)
    assert valid is True
    assert reason is None


def test_replace_and_upsert_series(db_conn):
    old_points = {
        datetime(2026, 8, 1, 0, 0): _ohlc(10.5, 10.0),
        datetime(2026, 8, 2, 0, 0): _ohlc(11.5, 11.0),
    }
    replace_series(db_conn, PRICES_SPEC, 1001, old_points, archive=False)
    assert db_conn.execute("SELECT COUNT(*) FROM bronze.prices WHERE product_id = 1001").fetchone()[0] == 2

    new_points = {datetime(2026, 8, 1, 0, 0): _ohlc(10.8, 10.0)}
    replace_series(db_conn, PRICES_SPEC, 1001, new_points, archive=True, reason="value_mismatch")

    bronze_rows = db_conn.execute("SELECT close FROM bronze.prices WHERE product_id = 1001").fetchall()
    assert len(bronze_rows) == 1
    assert bronze_rows[0][0] == 10.8

    cold_rows = db_conn.execute(
        "SELECT close, reason FROM cold_storage.prices WHERE product_id = 1001 ORDER BY date"
    ).fetchall()
    assert len(cold_rows) == 2
    assert cold_rows[0][0] == 10.5
    assert cold_rows[0][1] == "value_mismatch"

    incremental = {
        datetime(2026, 8, 1, 0, 0): _ohlc(12.0, 10.0),
        datetime(2026, 8, 2, 0, 0): _ohlc(12.5, 12.0),
    }
    upsert_series(db_conn, PRICES_SPEC, 1001, incremental)
    rows = db_conn.execute("SELECT date, close FROM bronze.prices WHERE product_id = 1001 ORDER BY date").fetchall()
    assert len(rows) == 2
    assert rows[0][1] == 12.0
    assert rows[1][1] == 12.5


def test_composite_entity_keys(db_conn):
    entity = ("EUR", "USD")
    last_date = datetime(2026, 8, 30, 0, 0)
    points = {last_date - timedelta(days=i): _fx_ohlc(1.08 + i * 0.001) for i in range(10)}
    replace_series(db_conn, FX_LIKE_SPEC, entity, points, archive=False)

    count = db_conn.execute(
        "SELECT COUNT(*) FROM bronze.fx WHERE source_currency = 'EUR' AND target_currency = 'USD'"
    ).fetchone()[0]
    assert count == 10

    valid, reason = validate_overlap(db_conn, FX_LIKE_SPEC, entity, points, last_date)
    assert valid is True
    assert reason is None

    tail = {last_date + timedelta(days=1): _fx_ohlc(1.10)}
    upsert_series(db_conn, FX_LIKE_SPEC, entity, tail)
    count = db_conn.execute(
        "SELECT COUNT(*) FROM bronze.fx WHERE source_currency = 'EUR' AND target_currency = 'USD'"
    ).fetchone()[0]
    assert count == 11

    restated = {d: _fx_ohlc(1.50) for d in points}
    replace_series(db_conn, FX_LIKE_SPEC, entity, restated, archive=True, reason="restatement")
    cold = db_conn.execute(
        "SELECT COUNT(*) FROM cold_storage.fx WHERE source_currency = 'EUR' AND target_currency = 'USD'"
    ).fetchone()[0]
    assert cold == 11


def test_record_and_load_series_status(db_conn):
    record_series_status(db_conn, PRICES_SPEC, 101, "no_data", None)
    status_map = load_series_status(db_conn, PRICES_SPEC)
    assert 101 in status_map
    assert status_map[101].status == "no_data"
    assert status_map[101].last_checked_at is not None

    long_error = "E" * 600
    record_series_status(db_conn, PRICES_SPEC, 101, "error", long_error)
    row = db_conn.execute("SELECT status, error_message FROM bronze.price_status WHERE product_id = 101").fetchone()
    assert row[0] == "error"
    assert len(row[1]) == 500

    record_series_status(db_conn, FX_LIKE_SPEC, ("EUR", "USD"), "ok", None)
    fx_status = load_series_status(db_conn, FX_LIKE_SPEC)
    assert ("EUR", "USD") in fx_status
    assert fx_status[("EUR", "USD")].status == "ok"


def test_has_historical_series_change(db_conn):
    pid = 4001
    cutoff = datetime(2026, 8, 15, 0, 0)
    points = {
        datetime(2026, 8, 1, 0, 0): _ohlc(10.0),
        datetime(2026, 8, 20, 0, 0): _ohlc(12.0),
    }
    replace_series(db_conn, PRICES_SPEC, pid, points, archive=False)

    refetch_same = {
        datetime(2026, 8, 1, 0, 0): {**_ohlc(10.0), "volume": 150.0},
        datetime(2026, 8, 20, 0, 0): _ohlc(99.0),
    }
    assert has_historical_series_change(db_conn, PRICES_SPEC, pid, refetch_same, cutoff) is False

    refetch_diff = {
        datetime(2026, 8, 1, 0, 0): _ohlc(5.0),
        datetime(2026, 8, 20, 0, 0): _ohlc(12.0),
    }
    assert has_historical_series_change(db_conn, PRICES_SPEC, pid, refetch_diff, cutoff) is True


def test_clean_series_cold_storage_prices(db_conn):
    now = datetime(2026, 9, 10, 0, 0)
    bronze_points = {now - timedelta(days=i): _ohlc(102.0, 100.0) for i in range(20)}
    replace_series(db_conn, PRICES_SPEC, 101, bronze_points, archive=False)

    run_time_1 = datetime(2026, 9, 9, 12, 0)
    for d, b in bronze_points.items():
        c_close = b["close"] + 0.05 if d >= now - timedelta(days=2) else b["close"]
        db_conn.execute(
            """
            INSERT INTO cold_storage.prices
            (product_id, run_id, date, open, high, low, close, volume, average, bar_count, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                101,
                run_time_1,
                d,
                b["open"],
                b["high"],
                b["low"],
                c_close,
                b["volume"],
                b["average"],
                b["bar_count"],
                "value_mismatch",
            ],
        )

    run_time_2 = datetime(2026, 9, 1, 12, 0)
    for d, b in bronze_points.items():
        db_conn.execute(
            """
            INSERT INTO cold_storage.prices
            (product_id, run_id, date, open, high, low, close, volume, average, bar_count, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                101,
                run_time_2,
                d,
                b["open"] * 0.5,
                b["high"] * 0.5,
                b["low"] * 0.5,
                b["close"] * 0.5,
                b["volume"],
                b["average"],
                b["bar_count"],
                "corporate_action",
            ],
        )

    deleted = clean_series_cold_storage(db_conn, PRICES_SPEC)
    assert deleted == 20
    remaining = db_conn.execute("SELECT DISTINCT run_id FROM cold_storage.prices WHERE product_id = 101").fetchall()
    assert len(remaining) == 1
    assert remaining[0][0] == run_time_2


def test_clean_series_cold_storage_composite_fx(db_conn):
    now = datetime(2026, 9, 10, 0, 0)
    entity = ("EUR", "USD")
    bronze_points = {now - timedelta(days=i): _fx_ohlc(1.08) for i in range(20)}
    replace_series(db_conn, FX_LIKE_SPEC, entity, bronze_points, archive=False)

    redundant_run = datetime(2026, 9, 9, 12, 0)
    for d, b in bronze_points.items():
        close = b["close"] + 1e-7 if d >= now - timedelta(days=2) else b["close"]
        db_conn.execute(
            """
            INSERT INTO cold_storage.fx
            (source_currency, target_currency, run_id, date, open, high, low, close, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ["EUR", "USD", redundant_run, d, b["open"], b["high"], b["low"], close, "value_mismatch"],
        )

    genuine_run = datetime(2026, 9, 1, 12, 0)
    for d, b in bronze_points.items():
        db_conn.execute(
            """
            INSERT INTO cold_storage.fx
            (source_currency, target_currency, run_id, date, open, high, low, close, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "EUR",
                "USD",
                genuine_run,
                d,
                b["open"] * 1.1,
                b["high"] * 1.1,
                b["low"] * 1.1,
                b["close"] * 1.1,
                "restatement",
            ],
        )

    deleted = clean_series_cold_storage(db_conn, FX_LIKE_SPEC)
    assert deleted == 20
    remaining = db_conn.execute("SELECT DISTINCT run_id FROM cold_storage.fx WHERE source_currency = 'EUR'").fetchall()
    assert len(remaining) == 1
    assert remaining[0][0] == genuine_run
