from datetime import datetime, timedelta

import duckdb
import pytest

from etfportfolio.core.db import apply_schema
from etfportfolio.ingest.clean import clean_cold_storage, clean_payload_blobs, run_clean
from etfportfolio.ingest.fx import FX_SPEC
from etfportfolio.ingest.prices import PRICES_SPEC, replace_series


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    apply_schema(conn)
    return conn


def test_clean_cold_storage_purges_redundant_runs(db_conn):
    # Setup bronze prices for product 101
    now = datetime(2026, 9, 10, 0, 0)
    bronze_points = {
        now - timedelta(days=i): {
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 102.0,
            "volume": 1000.0,
            "average": 101.0,
            "bar_count": 50,
        }
        for i in range(20)
    }
    replace_series(db_conn, PRICES_SPEC, 101, bronze_points, archive=False)

    # Insert a redundant run into cold storage (historical bars older than 5 trading days are identical)
    # archived run has same bars, differing only on the most recent 1-2 days by 5 cents
    run_time_1 = datetime(2026, 9, 9, 12, 0)
    for d, b in bronze_points.items():
        c_close = b["close"] + 0.05 if d >= now - timedelta(days=2) else b["close"]
        db_conn.execute(
            """
            INSERT INTO cold_storage.prices (product_id, run_id, date, open, high, low, close, volume, average, bar_count, reason)
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

    # Insert a genuine corporate action run (historical bars older than 5 days differ by 50% split)
    run_time_2 = datetime(2026, 9, 1, 12, 0)
    for d, b in bronze_points.items():
        db_conn.execute(
            """
            INSERT INTO cold_storage.prices (product_id, run_id, date, open, high, low, close, volume, average, bar_count, reason)
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

    total_cold_before = db_conn.execute("SELECT COUNT(*) FROM cold_storage.prices WHERE product_id = 101").fetchone()[0]
    assert total_cold_before == 40

    deleted = clean_cold_storage(db_conn)
    assert deleted == 20  # Only the redundant run (run_time_1) was deleted

    remaining_runs = db_conn.execute(
        "SELECT DISTINCT run_id FROM cold_storage.prices WHERE product_id = 101"
    ).fetchall()
    assert len(remaining_runs) == 1
    assert remaining_runs[0][0] == run_time_2


def test_clean_payload_blobs(db_conn):
    # Insert snapshots-referenced blob
    db_conn.execute("INSERT INTO bronze.payload_blobs (hash, payload) VALUES (111, blob 'abc')")
    db_conn.execute(
        "INSERT INTO bronze.snapshots (product_id, url_prefix, hash, created_at, last_checked_at) VALUES (1, '/tws.proxy/fundamentals/mf_holdings/', 111, now(), now())"
    )

    # Insert unreferenced orphan blob
    db_conn.execute("INSERT INTO bronze.payload_blobs (hash, payload) VALUES (333, blob 'xyz')")

    assert db_conn.execute("SELECT COUNT(*) FROM bronze.payload_blobs").fetchone()[0] == 2

    deleted = clean_payload_blobs(db_conn)
    assert deleted == 1

    remaining_hashes = [r[0] for r in db_conn.execute("SELECT hash FROM bronze.payload_blobs").fetchall()]
    assert remaining_hashes == [111]

    # Verify no query accesses bronze.snapshot_previews (it does not exist in schema)
    with pytest.raises(duckdb.CatalogException):
        db_conn.execute("SELECT * FROM bronze.snapshot_previews")


def test_run_clean_end_to_end(monkeypatch, tmp_path):
    db_file = str(tmp_path / "test.duckdb")
    with duckdb.connect(db_file) as conn:
        apply_schema(conn)

    from etfportfolio.core.config import settings

    monkeypatch.setattr(settings, "db_path", db_file)

    run_clean()


def test_clean_cold_storage_purges_redundant_fx_runs(db_conn):
    now = datetime(2026, 9, 10, 0, 0)
    bronze_points = {
        now - timedelta(days=i): {
            "open": 1.08,
            "high": 1.085,
            "low": 1.075,
            "close": 1.08,
        }
        for i in range(20)
    }
    replace_series(db_conn, FX_SPEC, ("EUR", "USD"), bronze_points, archive=False)

    run_time_1 = datetime(2026, 9, 9, 12, 0)
    for d, b in bronze_points.items():
        c_close = b["close"] + 1e-7 if d >= now - timedelta(days=2) else b["close"]
        db_conn.execute(
            """
            INSERT INTO cold_storage.fx
            (source_currency, target_currency, run_id, date, open, high, low, close, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ["EUR", "USD", run_time_1, d, b["open"], b["high"], b["low"], c_close, "value_mismatch"],
        )

    run_time_2 = datetime(2026, 9, 1, 12, 0)
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
                run_time_2,
                d,
                b["open"] * 1.1,
                b["high"] * 1.1,
                b["low"] * 1.1,
                b["close"] * 1.1,
                "restatement",
            ],
        )

    deleted = clean_cold_storage(db_conn)
    assert deleted == 20

    remaining_runs = db_conn.execute(
        "SELECT DISTINCT run_id FROM cold_storage.fx WHERE source_currency = 'EUR'"
    ).fetchall()
    assert len(remaining_runs) == 1
    assert remaining_runs[0][0] == run_time_2
