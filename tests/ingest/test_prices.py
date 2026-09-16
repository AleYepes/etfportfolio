import math
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest
from ib_async import BarData

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker, apply_schema
from etfportfolio.ingest.gateway import IBConnectionError
from etfportfolio.ingest.pipeline import Ingest
from etfportfolio.ingest.prices import (
    ABS_TOL,
    MIN_REFETCH_RETENTION_RATIO,
    OVERLAP_CALENDAR_DAYS,
    PRICES_SPEC,
    REL_TOL,
    PriceSeriesStatus,
    _fetch_and_store,
    _load_price_series_status,
    _record_price_status,
    _run_price_ingestion,
    format_duration,
    is_series_fresh,
    overlap_start_for,
    replace_series,
    upsert_series,
    validate_overlap,
)
from etfportfolio.ingest.utils import ProductContract


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    apply_schema(conn)
    return conn


# --- Scenario: IB Duration Format ---
def test_format_duration():
    # Minimum 10 D for small values
    assert format_duration(1) == "10 D"
    assert format_duration(9) == "10 D"
    assert format_duration(10) == "10 D"

    # Days between 11 and 365 use 'D'
    assert format_duration(15 + 9) == "24 D"
    assert format_duration(100) == "100 D"
    assert format_duration(365) == "365 D"

    # Days > 365 use 'Y'
    assert format_duration(366) == "2 Y"  # ceil(366/365.25) == 2
    assert format_duration(400) == "2 Y"
    assert format_duration(730) == "2 Y"
    assert format_duration(731) == "3 Y"
    assert format_duration(1000) == "3 Y"

    # Capped at 30 Y
    assert format_duration(36525) == "30 Y"
    assert format_duration(100000) == "30 Y"


# --- Scenario: Status Tracking & Python Freshness Evaluation ---
def test_record_and_load_price_status(db_conn):
    db_conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, created_at, updated_at)
        VALUES (101, 'XYZ', now(), now())
        """
    )

    # Initial record: 'no_data'
    _record_price_status(db_conn, 101, "no_data", None)
    status_map = _load_price_series_status(db_conn)
    assert 101 in status_map
    assert status_map[101].status == "no_data"
    assert status_map[101].last_checked_at is not None

    # Update record: 'error' with truncation > 500 chars
    long_error = "E" * 600
    _record_price_status(db_conn, 101, "error", long_error)
    row = db_conn.execute("SELECT status, error_message FROM bronze.price_status WHERE product_id = 101").fetchone()
    assert row[0] == "error"
    assert len(row[1]) == 500
    assert row[1] == "E" * 500

    # Update record: 'ok'
    _record_price_status(db_conn, 101, "ok", None)
    row = db_conn.execute("SELECT status, error_message FROM bronze.price_status WHERE product_id = 101").fetchone()
    assert row[0] == "ok"
    assert row[1] is None


def test_is_series_fresh_differentiated():
    now = datetime.now(UTC)
    yesterday = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    # 1. 'ok' or 'no_data' within freshness window is fresh
    recent_check = now - timedelta(hours=5)
    old_date = yesterday - timedelta(days=10)
    assert (
        is_series_fresh(
            PriceSeriesStatus(last_date=old_date, last_updated=None, last_checked_at=recent_check, status="no_data"),
            yesterday,
            24.0,
        )
        is True
    )
    assert (
        is_series_fresh(
            PriceSeriesStatus(last_date=old_date, last_updated=None, last_checked_at=recent_check, status="ok"),
            yesterday,
            24.0,
        )
        is True
    )

    # 2. 'error' is NEVER fresh even if checked 1 second ago
    one_sec_ago = now - timedelta(seconds=1)
    assert (
        is_series_fresh(
            PriceSeriesStatus(last_date=old_date, last_updated=None, last_checked_at=one_sec_ago, status="error"),
            yesterday,
            24.0,
        )
        is False
    )

    # 3. Bar reaches target_date is always fresh regardless of status
    assert (
        is_series_fresh(
            PriceSeriesStatus(last_date=yesterday, last_updated=None, last_checked_at=None, status=None),
            yesterday,
            24.0,
        )
        is True
    )


# --- Scenario: Overlap Calculations & Database Persistence ---
def test_overlap_start_for_margin_trimming():
    last_date = datetime(2026, 8, 20, 0, 0)
    start = overlap_start_for(last_date)
    assert start == datetime(2026, 8, 6, 0, 0)  # exactly 14 days prior

    incoming_dates = [
        datetime(2026, 8, 4, 0, 0),  # 16 days prior (margin, trimmed)
        datetime(2026, 8, 5, 0, 0),  # 15 days prior (margin, trimmed)
        datetime(2026, 8, 6, 0, 0),  # 14 days prior (start of W, kept)
        datetime(2026, 8, 20, 0, 0),  # last_date (end of W, kept)
        datetime(2026, 8, 21, 0, 0),  # tail (kept)
    ]
    trimmed = [d for d in incoming_dates if d >= start]
    assert trimmed == [
        datetime(2026, 8, 6, 0, 0),
        datetime(2026, 8, 20, 0, 0),
        datetime(2026, 8, 21, 0, 0),
    ]


def test_replace_series_with_archive(db_conn):
    old_points = {
        datetime(2026, 8, 1, 0, 0): {
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 100.0,
            "average": 10.2,
            "bar_count": 10,
        },
        datetime(2026, 8, 2, 0, 0): {
            "open": 11.0,
            "high": 12.0,
            "low": 10.0,
            "close": 11.5,
            "volume": 100.0,
            "average": 11.2,
            "bar_count": 10,
        },
    }
    replace_series(db_conn, PRICES_SPEC, 1001, old_points, archive=False)

    bronze_count = db_conn.execute("SELECT COUNT(*) FROM bronze.prices WHERE product_id = 1001").fetchone()[0]
    assert bronze_count == 2

    # Mismatch-triggered replace with archive=True
    new_points = {
        datetime(2026, 8, 1, 0, 0): {
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.8,
            "volume": 100.0,
            "average": 10.5,
            "bar_count": 10,
        },
    }
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
    assert cold_rows[1][0] == 11.5
    assert cold_rows[1][1] == "value_mismatch"


def test_upsert_series(db_conn):
    initial_points = {
        datetime(2026, 8, 1, 0, 0): {
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 100.0,
            "average": 10.2,
            "bar_count": 10,
        },
    }
    upsert_series(db_conn, PRICES_SPEC, 1001, initial_points)
    assert db_conn.execute("SELECT COUNT(*) FROM bronze.prices WHERE product_id = 1001").fetchone()[0] == 1

    # Incremental update with 1 updated point and 1 new point
    incremental_points = {
        datetime(2026, 8, 1, 0, 0): {
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 12.0,
            "volume": 100.0,
            "average": 10.2,
            "bar_count": 10,
        },
        datetime(2026, 8, 2, 0, 0): {
            "open": 12.0,
            "high": 13.0,
            "low": 11.0,
            "close": 12.5,
            "volume": 200.0,
            "average": 12.2,
            "bar_count": 20,
        },
    }
    upsert_series(db_conn, PRICES_SPEC, 1001, incremental_points)

    rows = db_conn.execute("SELECT date, close FROM bronze.prices WHERE product_id = 1001 ORDER BY date").fetchall()
    assert len(rows) == 2
    assert rows[0][1] == 12.0
    assert rows[1][1] == 12.5


def test_upsert_series_updates_updated_at_on_overlap(db_conn):
    old_time = datetime(2026, 8, 1, 10, 0, 0)
    bar_date = datetime(2026, 8, 1, 0, 0, 0)

    db_conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1001, $1, 100.0, $2)
        """,
        [bar_date, old_time],
    )

    row = db_conn.execute("SELECT updated_at FROM bronze.prices WHERE product_id = 1001").fetchone()
    assert row[0] == old_time

    overlap_points = {
        bar_date: {
            "open": 99.0,
            "high": 101.0,
            "low": 98.0,
            "close": 100.0,
            "volume": 1000.0,
            "average": 100.0,
            "bar_count": 50,
        }
    }
    upsert_series(db_conn, PRICES_SPEC, 1001, overlap_points)

    row = db_conn.execute("SELECT updated_at, close FROM bronze.prices WHERE product_id = 1001").fetchone()
    assert row[0] > old_time
    assert row[1] == 100.0

    count = db_conn.execute("SELECT COUNT(*) FROM bronze.prices WHERE product_id = 1001").fetchone()[0]
    assert count == 1


# --- Scenario: Overlap Validation Scenarios ---
def test_validate_overlap_prices(db_conn):
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing_points = {}
    for i in range(OVERLAP_CALENDAR_DAYS + 1):
        d = last_date - timedelta(days=i)
        existing_points[d] = {
            "open": 100.0 + i,
            "high": 105.0 + i,
            "low": 95.0 + i,
            "close": 102.0 + i,
            "volume": 1000.0,
            "average": 101.0 + i,
            "bar_count": 50,
        }

    replace_series(db_conn, PRICES_SPEC, 1001, existing_points, archive=False)

    # 1. Exact match in window W + tail
    new_points = {}
    # Margin before W (date < last_date - 14d)
    new_points[last_date - timedelta(days=16)] = {
        "open": 999.0,
        "high": 999.0,
        "low": 999.0,
        "close": 999.0,
        "volume": 1.0,
        "average": 999.0,
        "bar_count": 1,
    }
    for i in range(OVERLAP_CALENDAR_DAYS + 1):
        d = last_date - timedelta(days=i)
        new_points[d] = dict(existing_points[d])
    new_points[last_date + timedelta(days=1)] = {
        "open": 200.0,
        "high": 205.0,
        "low": 195.0,
        "close": 202.0,
        "volume": 1500.0,
        "average": 201.0,
        "bar_count": 60,
    }

    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 1001, new_points, last_date)
    assert valid is True
    assert reason is None

    # 2. Date mismatch in window W
    missing_points = dict(new_points)
    del missing_points[last_date - timedelta(days=2)]
    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 1001, missing_points, last_date)
    assert valid is False
    assert reason == "date_mismatch"

    # 3. Value mismatch in interior of window W
    interior_mismatched = dict(new_points)
    interior_d = last_date - timedelta(days=2)
    interior_mismatched[interior_d] = dict(new_points[interior_d])
    interior_mismatched[interior_d]["close"] = 999.99
    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 1001, interior_mismatched, last_date)
    assert valid is False
    assert reason == "value_mismatch"

    # 4. Floating-point tolerance check (within REL_TOL passes)
    tolerant_points = dict(new_points)
    tolerant_points[last_date] = dict(new_points[last_date])
    tolerant_points[last_date]["close"] = tolerant_points[last_date]["close"] * (1 + 0.5 * REL_TOL)
    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 1001, tolerant_points, last_date)
    assert valid is True
    assert reason is None


def test_validate_overlap_volume_vwap_drift_accepted(db_conn):
    """Spec 1: Volume & average revisions on last_date must NOT trigger refetch."""
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing_points = {
        last_date - timedelta(days=1): {
            "open": 50.0,
            "high": 51.0,
            "low": 49.0,
            "close": 50.5,
            "volume": 10000.0,
            "average": 50.2,
            "bar_count": 100,
        },
        last_date: {
            "open": 50.5,
            "high": 52.0,
            "low": 50.0,
            "close": 51.5,
            "volume": 20000.0,
            "average": 51.1,
            "bar_count": 200,
        },
    }
    replace_series(db_conn, PRICES_SPEC, 2001, existing_points, archive=False)

    # Incoming points have identical OHLC, but drastically different volume and average
    new_points = {
        last_date - timedelta(days=1): dict(existing_points[last_date - timedelta(days=1)]),
        last_date: {
            "open": 50.5,
            "high": 52.0,
            "low": 50.0,
            "close": 51.5,
            "volume": 25000.0,  # 25% volume change
            "average": 51.9,  # VWAP revision
            "bar_count": 250,
        },
    }
    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 2001, new_points, last_date)
    assert valid is True
    assert reason is None


def test_validate_overlap_seam_penny_shift_accepted(db_conn):
    """Spec 3: A $0.01 closing cross revision on last_date is accepted under bounded seam drift."""
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing_points = {
        last_date - timedelta(days=1): {
            "open": 50.0,
            "high": 51.0,
            "low": 49.0,
            "close": 50.5,
            "volume": 1000.0,
            "average": 50.2,
            "bar_count": 50,
        },
        last_date: {
            "open": 50.0,
            "high": 51.0,
            "low": 49.5,
            "close": 50.00,  # Continuous close was $50.00
            "volume": 1000.0,
            "average": 50.1,
            "bar_count": 50,
        },
    }
    replace_series(db_conn, PRICES_SPEC, 2002, existing_points, archive=False)

    # Official auction close shifts close within ABS_TOL
    new_points = {
        last_date - timedelta(days=1): dict(existing_points[last_date - timedelta(days=1)]),
        last_date: {
            "open": 50.0,
            "high": 51.0,
            "low": 49.5,
            "close": 50.00 + ABS_TOL,  # shift within ABS_TOL
            "volume": 1000.0,
            "average": 50.1,
            "bar_count": 50,
        },
    }
    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 2002, new_points, last_date)
    assert valid is True
    assert reason is None


def test_validate_overlap_interior_shift_triggers_mismatch(db_conn):
    """Spec 3: Any price difference prior to last_date is treated as a corporate action."""
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    interior_d = last_date - timedelta(days=1)
    existing_points = {
        interior_d: {
            "open": 50.0,
            "high": 51.0,
            "low": 49.0,
            "close": 50.5,
            "volume": 1000.0,
            "average": 50.2,
            "bar_count": 50,
        },
        last_date: {
            "open": 50.0,
            "high": 51.0,
            "low": 49.5,
            "close": 50.0,
            "volume": 1000.0,
            "average": 50.1,
            "bar_count": 50,
        },
    }
    replace_series(db_conn, PRICES_SPEC, 2003, existing_points, archive=False)

    new_points = {
        interior_d: {
            "open": 50.0,
            "high": 51.0,
            "low": 49.0,
            "close": 52.0,  # $1.50 shift on d < last_date (well beyond tolerances)
            "volume": 1000.0,
            "average": 50.2,
            "bar_count": 50,
        },
        last_date: dict(existing_points[last_date]),
    }
    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 2003, new_points, last_date)
    assert valid is False
    assert reason == "value_mismatch"


def test_validate_overlap_extreme_seam_shift_triggers_mismatch(db_conn):
    """Spec 3: Extreme price shift on last_date exceeding ABS_TOL and REL_TOL triggers mismatch."""
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing_points = {
        last_date - timedelta(days=1): {
            "open": 50.0,
            "high": 51.0,
            "low": 49.0,
            "close": 50.5,
            "volume": 1000.0,
            "average": 50.2,
            "bar_count": 50,
        },
        last_date: {
            "open": 50.0,
            "high": 51.0,
            "low": 49.5,
            "close": 50.0,
            "volume": 1000.0,
            "average": 50.1,
            "bar_count": 50,
        },
    }
    replace_series(db_conn, PRICES_SPEC, 2004, existing_points, archive=False)

    # 50% shift on last_date close ($50 -> $75)
    new_points = {
        last_date - timedelta(days=1): dict(existing_points[last_date - timedelta(days=1)]),
        last_date: {
            "open": 50.0,
            "high": 51.0,
            "low": 49.5,
            "close": 75.0,  # +$25.00 (> ABS_TOL and > REL_TOL)
            "volume": 1000.0,
            "average": 50.1,
            "bar_count": 50,
        },
    }
    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 2004, new_points, last_date)
    assert valid is False
    assert reason == "value_mismatch"


# --- Scenario: Anti-Truncation Safety Guard ---
@pytest.mark.anyio
async def test_fetch_and_store_anti_truncation_guard_preserves_bronze(tmp_path):
    """Spec 4: If refetch returns fewer than MIN_REFETCH_RETENTION_RATIO of existing bars, abort replacement and preserve bronze."""
    db_file = str(tmp_path / "test_anti_truncation.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    pid = 888
    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, created_at, updated_at)
        VALUES (888, 'GUARD', now(), now())
        """
    )

    now = datetime.now(UTC)
    yesterday = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    last_stored_date = yesterday - timedelta(days=2)
    base_date = last_stored_date - timedelta(days=2499)

    # Insert 2,500 historical rows
    bulk_points = [
        (
            pid,
            base_date + timedelta(days=i),
            100.0,
            105.0,
            95.0,
            100.0,
            1000.0,
            100.0,
            10,
            now.replace(tzinfo=None),
        )
        for i in range(2500)
    ]
    conn.executemany(
        """
        INSERT INTO bronze.prices (product_id, date, open, high, low, close, volume, average, bar_count, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        bulk_points,
    )
    conn.close()

    # 1. Incremental fetch returns a mismatched bar (e.g. 50% price mismatch)
    mismatched_incremental_bar = BarData(
        date=last_stored_date,
        open=100.0,
        high=105.0,
        low=95.0,
        close=999.0,  # huge mismatch
        volume=100.0,
        average=100.0,
        barCount=10,
    )

    # 2. 30Y refetch returns ONLY 1 bar (transient IBKR gateway truncation)
    single_refetch_bar = BarData(
        date=last_stored_date,
        open=100.0,
        high=100.0,
        low=100.0,
        close=100.0,
        volume=0.0,
        average=100.0,
        barCount=0,
    )

    mock_ib = MagicMock()
    mock_ib.reqHistoricalDataAsync = AsyncMock(
        side_effect=[
            [mismatched_incremental_bar],  # incremental call
            [single_refetch_bar],  # 30Y refetch call
        ]
    )

    product = ProductContract(product_id=pid, symbol="GUARD")

    async with AsyncDbWorker(db_file) as worker:
        await _fetch_and_store(worker, mock_ib, product)

    verify_conn = duckdb.connect(db_file)

    # Existing 2,500 rows must remain 100% intact!
    row_count_row = verify_conn.execute("SELECT COUNT(*) FROM bronze.prices WHERE product_id = ?", [pid]).fetchone()
    assert row_count_row is not None
    assert row_count_row[0] == 2500

    # Cold storage should NOT have been written
    cold_count_row = verify_conn.execute(
        "SELECT COUNT(*) FROM cold_storage.prices WHERE product_id = ?", [pid]
    ).fetchone()
    assert cold_count_row is not None
    assert cold_count_row[0] == 0

    # Status must be 'error' with truncation message
    status_row = verify_conn.execute(
        "SELECT status, error_message FROM bronze.price_status WHERE product_id = ?", [pid]
    ).fetchone()
    assert status_row is not None
    assert status_row[0] == "error"
    assert "Truncated refetch" in status_row[1]
    expected_min = math.floor(2500 * MIN_REFETCH_RETENTION_RATIO)
    assert f"received 1 bars, expected >= {expected_min}" in status_row[1]

    verify_conn.close()


# --- Scenario: Preservation on Zero Bars ---
@pytest.mark.anyio
async def test_fetch_and_store_preserves_prices_on_zero_bars(tmp_path):
    db_file = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    last_date = datetime(2026, 8, 20, 0, 0)
    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, created_at, updated_at)
        VALUES (555, 'PRESERVE', now(), now())
        """
    )
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (555, $1, 123.45, now())
        """,
        [last_date],
    )
    conn.close()

    mock_ib = MagicMock()
    mock_ib.reqHistoricalDataAsync = AsyncMock(return_value=[])

    product = ProductContract(product_id=555, symbol="PRESERVE")

    async with AsyncDbWorker(db_file) as worker:
        await _fetch_and_store(worker, mock_ib, product)

    conn = duckdb.connect(db_file)
    prices = conn.execute("SELECT close FROM bronze.prices WHERE product_id = 555").fetchall()
    assert len(prices) == 1
    assert prices[0][0] == 123.45

    status_row = conn.execute("SELECT status, error_message FROM bronze.price_status WHERE product_id = 555").fetchone()
    assert status_row is not None
    assert status_row[0] == "no_data"
    assert status_row[1] is None
    conn.close()


@pytest.mark.anyio
async def test_fetch_and_store_mismatch_zero_bars_preserves(tmp_path):
    db_file = str(tmp_path / "test_mismatch.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    d1 = datetime(2026, 8, 20, 0, 0)
    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, created_at, updated_at)
        VALUES (777, 'MISMATCH', now(), now())
        """
    )
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (777, $1, 100.0, now())
        """,
        [d1],
    )
    conn.close()

    mismatched_bar = BarData(
        date=datetime(2026, 8, 20, 0, 0),
        open=100.0,
        high=105.0,
        low=95.0,
        close=999.0,
        volume=100.0,
        average=100.0,
        barCount=10,
    )

    mock_ib = MagicMock()
    mock_ib.reqHistoricalDataAsync = AsyncMock(side_effect=[[mismatched_bar], []])

    product = ProductContract(product_id=777, symbol="MISMATCH")

    async with AsyncDbWorker(db_file) as worker:
        await _fetch_and_store(worker, mock_ib, product)

    conn = duckdb.connect(db_file)
    prices = conn.execute("SELECT close FROM bronze.prices WHERE product_id = 777").fetchall()
    assert len(prices) == 1
    assert prices[0][0] == 100.0

    cold_row = conn.execute("SELECT COUNT(*) FROM cold_storage.prices WHERE product_id = 777").fetchone()
    assert cold_row is not None
    assert cold_row[0] == 0

    status_row = conn.execute("SELECT status, error_message FROM bronze.price_status WHERE product_id = 777").fetchone()
    assert status_row is not None
    assert status_row[0] == "error"
    assert "Truncated refetch" in status_row[1]
    conn.close()


# --- Scenario: Error Capture in _run_price_ingestion ---
@pytest.mark.anyio
async def test_run_price_ingestion_error_capture(tmp_path, monkeypatch):
    db_file = str(tmp_path / "test_error.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, created_at, updated_at)
        VALUES (999, 'ERR', now(), now())
        """
    )
    conn.close()

    monkeypatch.setattr(settings, "db_path", db_file)

    mock_ib = MagicMock()
    mock_ib.isConnected.return_value = True
    mock_ib.reqHistoricalDataAsync = AsyncMock(side_effect=RuntimeError("Gateway Timeout"))

    with patch("etfportfolio.ingest.prices.ib_connection") as mock_conn:
        mock_conn.return_value.__aenter__.return_value = mock_ib
        mock_conn.return_value.__aexit__.return_value = False
        count = await _run_price_ingestion(force=True)
        assert count == 1

    conn = duckdb.connect(db_file)
    status_row = conn.execute("SELECT status, error_message FROM bronze.price_status WHERE product_id = 999").fetchone()
    assert status_row is not None
    assert status_row[0] == "error"
    assert status_row[1] is not None
    assert "Gateway Timeout" in status_row[1]
    conn.close()


@pytest.mark.anyio
async def test_run_price_ingestion_ib_connection_error_aborts(tmp_path, monkeypatch):
    db_file = str(tmp_path / "test_abort.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, created_at, updated_at)
        VALUES (111, 'ONE', now(), now()), (222, 'TWO', now(), now())
        """
    )
    conn.close()

    monkeypatch.setattr(settings, "db_path", db_file)

    mock_ib = MagicMock()
    mock_ib.isConnected.return_value = True
    mock_ib.reqHistoricalDataAsync = AsyncMock(side_effect=IBConnectionError("Connection lost"))

    with patch("etfportfolio.ingest.prices.ib_connection") as mock_conn:
        mock_conn.return_value.__aenter__.return_value = mock_ib
        mock_conn.return_value.__aexit__.return_value = False
        with pytest.raises(IBConnectionError, match="Connection lost"):
            await _run_price_ingestion(force=True)


# --- Scenario: CLI Signatures reject --limit and --product_ids ---
def test_cli_signatures_reject_unused_flags():
    ingest = Ingest()
    with pytest.raises(TypeError):
        ingest(limit=10)  # type: ignore

    with pytest.raises(TypeError):
        ingest.contracts(product_ids="1001")  # type: ignore

    with pytest.raises(TypeError):
        ingest.prices(limit=5)  # type: ignore

    with pytest.raises(TypeError):
        ingest.details(product_ids="1001,1002")  # type: ignore


def test_validate_overlap_historical_core_triggers_corporate_action(db_conn):
    """Any discrepancy in historical core (d < last_date - 5 trading days) is classified as corporate_action."""
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing_points = {
        last_date - timedelta(days=i): {
            "open": 50.0,
            "high": 51.0,
            "low": 49.0,
            "close": 50.0,
            "volume": 1000.0,
            "average": 50.0,
            "bar_count": 50,
        }
        for i in range(12)  # 12 bars: last 5 are settlement, older 7 are core
    }
    replace_series(db_conn, PRICES_SPEC, 3001, existing_points, archive=False)

    new_points = {d: dict(v) for d, v in existing_points.items()}
    # Modify a bar in historical core (10 days prior)
    core_d = last_date - timedelta(days=10)
    new_points[core_d]["close"] = 55.0

    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 3001, new_points, last_date)
    assert valid is False
    assert reason == "corporate_action"


def test_validate_overlap_uniform_ratio_triggers_corporate_action(db_conn):
    """Multi-bar uniform ratio shift (stock split) triggers corporate_action."""
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    existing_points = {
        last_date - timedelta(days=i): {
            "open": 50.0,
            "high": 51.0,
            "low": 49.0,
            "close": 50.0,
            "volume": 1000.0,
            "average": 50.0,
            "bar_count": 50,
        }
        for i in range(5)
    }
    replace_series(db_conn, PRICES_SPEC, 3002, existing_points, archive=False)

    new_points = {d: dict(v) for d, v in existing_points.items()}
    # 2:1 stock split on recent bars (prices halved uniformly)
    for d in [last_date, last_date - timedelta(days=1)]:
        new_points[d]["close"] = 25.0
        new_points[d]["open"] = 25.0

    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 3002, new_points, last_date)
    assert valid is False
    assert reason == "corporate_action"


def test_has_historical_price_change(db_conn):
    from etfportfolio.ingest.prices import _has_historical_price_change

    pid = 4001
    cutoff = datetime(2026, 8, 15, 0, 0)
    points = {
        datetime(2026, 8, 1, 0, 0): {
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.0,
            "volume": 100.0,
            "average": 10.0,
            "bar_count": 10,
        },
        datetime(2026, 8, 20, 0, 0): {
            "open": 12.0,
            "high": 13.0,
            "low": 11.0,
            "close": 12.0,
            "volume": 100.0,
            "average": 12.0,
            "bar_count": 10,
        },
    }
    replace_series(db_conn, PRICES_SPEC, pid, points, archive=False)

    # Identical historical bars (date < cutoff)
    refetch_same = {
        datetime(2026, 8, 1, 0, 0): {
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.0,
            "volume": 150.0,
            "average": 10.1,
            "bar_count": 15,
        },
        datetime(2026, 8, 20, 0, 0): {"open": 12.0, "high": 13.0, "low": 11.0, "close": 99.0},  # Changed after cutoff
    }
    assert _has_historical_price_change(db_conn, PRICES_SPEC, pid, refetch_same, cutoff) is False

    # Differing historical bar (date < cutoff)
    refetch_diff = {
        datetime(2026, 8, 1, 0, 0): {"open": 5.0, "high": 5.5, "low": 4.5, "close": 5.0},  # Split before cutoff
        datetime(2026, 8, 20, 0, 0): {"open": 12.0, "high": 13.0, "low": 11.0, "close": 12.0},
    }
    assert _has_historical_price_change(db_conn, PRICES_SPEC, pid, refetch_diff, cutoff) is True
