from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from etfportfolio.core.db import apply_schema
from etfportfolio.core.utils import decompress_payload
from etfportfolio.ingestion.utils import (
    OVERLAP_CALENDAR_DAYS,
    PRICES_SPEC,
    SENTIMENT_SPEC,
    canonical_bytes,
    content_address,
    gc_preview_blob,
    is_fresh,
    is_series_fresh,
    overlap_start_for,
    replace_series,
    store_blob,
    upsert_series,
    validate_overlap,
)


def test_is_fresh():
    now = datetime.now(UTC)
    assert not is_fresh(None, 24.0)

    # 1 hour ago is fresh within 24h
    one_hour_ago = now - timedelta(hours=1)
    assert is_fresh(one_hour_ago, 24.0)
    assert not is_fresh(one_hour_ago, 0.5)

    # 48 hours ago is stale
    stale = now - timedelta(hours=48)
    assert not is_fresh(stale, 24.0)

    # Naive datetime treated as UTC
    naive_recent = (now - timedelta(hours=2)).replace(tzinfo=None)
    assert is_fresh(naive_recent, 24.0)
    naive_stale = (now - timedelta(hours=25)).replace(tzinfo=None)
    assert not is_fresh(naive_stale, 24.0)

    # Future-skewed timestamp clamped to 0.0 delta
    future = now + timedelta(minutes=10)
    assert is_fresh(future, 24.0)


def test_content_addressing_determinism():
    payload_1 = {"b": 2, "a": 1, "nested": {"z": 9, "y": 8}, "list": [3, 1, 2]}
    payload_2 = {"a": 1, "nested": {"y": 8, "z": 9}, "b": 2, "list": [2, 1, 3]}

    canon_1 = canonical_bytes(payload_1)
    canon_2 = canonical_bytes(payload_2)
    assert canon_1 == canon_2

    hash_1, comp_1 = content_address(payload_1)
    hash_2, comp_2 = content_address(payload_2)
    assert hash_1 == hash_2
    assert comp_1 == comp_2
    assert isinstance(hash_1, int)
    assert hash_1 >= 0  # unsigned 64-bit

    decompressed = decompress_payload(comp_1)
    # Lists are canonicalized to sorted order
    assert decompressed == {"a": 1, "b": 2, "list": [1, 2, 3], "nested": {"y": 8, "z": 9}}


def test_landing_stamp_rule():
    # Rule: stamp last_checked_at <=> NOT (fetch_gated AND NOT gated_success)
    def should_stamp(fetch_gated: bool, gated_success: bool) -> bool:
        return not (fetch_gated and not gated_success)

    # No gated fetch attempted -> stamp
    assert should_stamp(fetch_gated=False, gated_success=True) is True
    assert should_stamp(fetch_gated=False, gated_success=False) is True

    # Gated fetch attempted and succeeded -> stamp
    assert should_stamp(fetch_gated=True, gated_success=True) is True

    # Gated fetch attempted and failed -> do NOT stamp
    assert should_stamp(fetch_gated=True, gated_success=False) is False


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    apply_schema(conn)
    # Insert test product into bronze.products
    conn.execute(
        """
        INSERT INTO bronze.products (product_id, symbol, created_at, updated_at)
        VALUES (1001, 'TEST', now(), now())
        """
    )
    return conn


def test_store_blob_and_gc(db_conn):
    digest, comp = content_address({"key": "value"})
    store_blob(db_conn, digest, comp)
    # Idempotent insert
    store_blob(db_conn, digest, comp)

    # Initially not referenced in snapshots or snapshot_previews -> GC removes it
    assert gc_preview_blob(db_conn, digest) is True
    row = db_conn.execute("SELECT COUNT(*) FROM bronze.payload_blobs WHERE hash = $1", [digest]).fetchone()
    assert row[0] == 0

    # Re-store and reference in snapshot_previews
    store_blob(db_conn, digest, comp)
    db_conn.execute(
        """
        INSERT INTO bronze.snapshot_previews (product_id, hash, updated_at, last_checked_at)
        VALUES (1001, $1, now(), now())
        """,
        [digest],
    )
    # GC should NOT delete it because it's referenced
    assert gc_preview_blob(db_conn, digest) is False
    row = db_conn.execute("SELECT COUNT(*) FROM bronze.payload_blobs WHERE hash = $1", [digest]).fetchone()
    assert row[0] == 1


def test_validate_overlap_prices(db_conn):
    last_date = datetime(2026, 8, 30, 0, 0, 0)
    # Window W = [2026-08-23, 2026-08-30] (7 days before last_date)
    existing_points = {}
    for i in range(10):
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

    # Populate bronze.prices
    replace_series(db_conn, PRICES_SPEC, 1001, existing_points, archive=False)

    # 1. Exact match in window W + new points after last_date + margin points before W
    new_points = {}
    # Margin before W (date < 2026-08-23) - even if different or missing, should be ignored
    new_points[last_date - timedelta(days=12)] = {
        "open": 999.0,
        "high": 999.0,
        "low": 999.0,
        "close": 999.0,
        "volume": 1.0,
        "average": 999.0,
        "bar_count": 1,
    }
    # Within W: identical
    for i in range(OVERLAP_CALENDAR_DAYS + 1):
        d = last_date - timedelta(days=i)
        new_points[d] = dict(existing_points[d])
    # Beyond last_date (incremental tail)
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

    # 2. Date mismatch in window W (missing one date in new_points)
    missing_points = dict(new_points)
    del missing_points[last_date - timedelta(days=2)]
    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 1001, missing_points, last_date)
    assert valid is False
    assert reason == "date_mismatch"

    # 3. Value mismatch in window W
    mismatched_points = dict(new_points)
    mismatched_points[last_date] = dict(new_points[last_date])
    mismatched_points[last_date]["close"] = 999.99
    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 1001, mismatched_points, last_date)
    assert valid is False
    assert reason == "value_mismatch"

    # 4. Floating-point tolerance check (within 1e-4 tolerance passes)
    tolerant_points = dict(new_points)
    tolerant_points[last_date] = dict(new_points[last_date])
    tolerant_points[last_date]["close"] = tolerant_points[last_date]["close"] * (1 + 1e-5)
    valid, reason = validate_overlap(db_conn, PRICES_SPEC, 1001, tolerant_points, last_date)
    assert valid is True
    assert reason is None


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

    # Bronze table now has 1 row with new value
    bronze_rows = db_conn.execute("SELECT close FROM bronze.prices WHERE product_id = 1001").fetchall()
    assert len(bronze_rows) == 1
    assert bronze_rows[0][0] == 10.8

    # Cold storage table has the archived 2 rows with reason
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


def test_sentiment_fixture_overlap_and_archive(db_conn):
    import json
    from pathlib import Path

    from etfportfolio.ingestion.sentiment import _extract_sentiment_points

    fixture_path = Path("tests/fixtures/sentiment_incremental.json")
    raw = json.loads(fixture_path.read_text())
    points = _extract_sentiment_points(raw)
    assert len(points) > 0

    sorted_dates = sorted(points.keys())
    last_date = sorted_dates[-1]

    # Populate bronze.sentiment with all points
    replace_series(db_conn, SENTIMENT_SPEC, 1001, points, archive=False)
    assert db_conn.execute("SELECT COUNT(*) FROM bronze.sentiment WHERE product_id = 1001").fetchone()[0] == len(points)

    # Validate overlap passes on exact match
    valid, reason = validate_overlap(db_conn, SENTIMENT_SPEC, 1001, points, last_date)
    assert valid is True
    assert reason is None

    # Test archive to cold_storage.sentiment on mismatch
    mismatched = dict(points)
    mismatched[last_date] = dict(mismatched[last_date])
    mismatched[last_date]["sscore"] = 999.99
    valid, reason = validate_overlap(db_conn, SENTIMENT_SPEC, 1001, mismatched, last_date)
    assert valid is False
    assert reason == "value_mismatch"

    # Replace with archive
    replace_series(db_conn, SENTIMENT_SPEC, 1001, mismatched, archive=True, reason=reason)
    cold_rows = db_conn.execute(
        "SELECT COUNT(*) FROM cold_storage.sentiment WHERE product_id = 1001 AND reason = 'value_mismatch'"
    ).fetchone()[0]
    assert cold_rows == len(points)


def test_is_series_fresh():
    now = datetime.now(UTC)
    yesterday = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    # 1. None or empty status is never fresh
    assert is_series_fresh(None, yesterday, 24.0) is False
    assert is_series_fresh((None, None), yesterday, 24.0) is False

    # 2. Fresh by date (date >= yesterday), even if updated_at is stale or None
    assert is_series_fresh((yesterday, None), yesterday, 24.0) is True
    assert is_series_fresh((yesterday + timedelta(days=1), None), yesterday, 24.0) is True
    stale_updated = (now - timedelta(hours=48)).replace(tzinfo=None)
    assert is_series_fresh((yesterday, stale_updated), yesterday, 24.0) is True

    # 3. Fresh by updated_at (date < yesterday, but updated_at within 24h)
    old_date = yesterday - timedelta(days=3)
    recent_updated = (now - timedelta(hours=2)).replace(tzinfo=None)
    assert is_series_fresh((old_date, recent_updated), yesterday, 24.0) is True

    # 4. Stale: date < yesterday AND updated_at > 24h ago
    assert is_series_fresh((old_date, stale_updated), yesterday, 24.0) is False


def test_upsert_series_updates_updated_at_on_overlap(db_conn):
    old_time = datetime(2026, 8, 1, 10, 0, 0)
    bar_date = datetime(2026, 8, 1, 0, 0, 0)

    # Insert an initial bar directly with old_time
    db_conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1001, $1, 100.0, $2)
        """,
        [bar_date, old_time],
    )

    row = db_conn.execute("SELECT updated_at FROM bronze.prices WHERE product_id = 1001").fetchone()
    assert row[0] == old_time

    # Upsert with identical bar data on overlapping date
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

    # Ensure no duplicate row was created
    count = db_conn.execute("SELECT COUNT(*) FROM bronze.prices WHERE product_id = 1001").fetchone()[0]
    assert count == 1


def test_overlap_start_for_margin_trimming():
    last_date = datetime(2026, 8, 10, 0, 0)
    start = overlap_start_for(last_date)
    assert start == datetime(2026, 8, 3, 0, 0)  # exactly 7 days prior

    # Simulating margin: dates 9 days prior (fetch margin) vs 7 days prior (W) vs tail
    incoming_dates = [
        datetime(2026, 8, 1, 0, 0),  # 9 days prior (margin, should be trimmed)
        datetime(2026, 8, 2, 0, 0),  # 8 days prior (margin, should be trimmed)
        datetime(2026, 8, 3, 0, 0),  # 7 days prior (start of W, kept)
        datetime(2026, 8, 10, 0, 0),  # last_date (end of W, kept)
        datetime(2026, 8, 11, 0, 0),  # tail (kept)
    ]
    trimmed = [d for d in incoming_dates if d >= start]
    assert trimmed == [
        datetime(2026, 8, 3, 0, 0),
        datetime(2026, 8, 10, 0, 0),
        datetime(2026, 8, 11, 0, 0),
    ]
