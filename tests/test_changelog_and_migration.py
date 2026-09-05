from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from etfportfolio.core.db import apply_schema
from etfportfolio.ingestion.details import load_endpoint_freshness_cache
from etfportfolio.ingestion.snapshots import store_snapshot
from scripts.migrate_snapshots_changelog import migrate


@pytest.fixture
def test_db(tmp_path: Path):
    db_file = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_file))
    apply_schema(conn)
    conn.execute(
        """
        INSERT INTO bronze.products (product_id, symbol, created_at, updated_at)
        VALUES (1001, 'TEST', now(), now())
        """
    )
    conn.close()
    return str(db_file)


def test_store_snapshot_changelog(test_db):
    conn = duckdb.connect(test_db)
    t1 = datetime(2026, 9, 1, 10, 0, 0)
    payload_a = {"val": 1}

    # 1. Initial store: inserts new row
    store_snapshot(conn, 1001, "/test/ep/", "slug", payload_a, fetched_at=t1)

    rows = conn.execute(
        "SELECT snapshot_id, product_id, url_prefix, created_at, last_checked_at FROM bronze.snapshots"
    ).fetchall()
    assert len(rows) == 1
    snap_id_1, pid, prefix, created_at_1, last_checked_1 = rows[0]
    assert pid == 1001
    assert prefix == "/test/ep/"
    assert created_at_1 == t1
    assert last_checked_1 == t1

    # 2. Store identical payload at later time t2: updates last_checked_at in-place
    t2 = datetime(2026, 9, 2, 10, 0, 0)
    store_snapshot(conn, 1001, "/test/ep/", "slug", payload_a, fetched_at=t2)

    rows = conn.execute(
        "SELECT snapshot_id, product_id, url_prefix, created_at, last_checked_at FROM bronze.snapshots"
    ).fetchall()
    assert len(rows) == 1
    snap_id_2, pid, prefix, created_at_2, last_checked_2 = rows[0]
    assert snap_id_2 == snap_id_1
    assert created_at_2 == t1
    assert last_checked_2 == t2

    # 3. Freshness cache queries MAX(last_checked_at)
    cache = load_endpoint_freshness_cache(conn)
    assert cache[(1001, "/test/ep/")] == t2

    # 4. Store changed payload at t3: inserts new row
    t3 = datetime(2026, 9, 3, 10, 0, 0)
    payload_b = {"val": 2}
    store_snapshot(conn, 1001, "/test/ep/", "slug", payload_b, fetched_at=t3)

    rows = conn.execute(
        "SELECT snapshot_id, product_id, url_prefix, created_at, last_checked_at FROM bronze.snapshots ORDER BY snapshot_id"
    ).fetchall()
    assert len(rows) == 2
    assert rows[1][0] > snap_id_1
    assert rows[1][3] == t3
    assert rows[1][4] == t3

    conn.close()


def test_migrate_snapshots_changelog(tmp_path: Path):
    db_file = str(tmp_path / "legacy.duckdb")
    conn = duckdb.connect(db_file)
    conn.execute("CREATE SCHEMA bronze;")
    conn.execute("CREATE SEQUENCE bronze.snapshots_id_seq;")
    conn.execute(
        """
        CREATE TABLE bronze.payload_blobs (
            hash UBIGINT PRIMARY KEY,
            payload BLOB NOT NULL
        );
        CREATE TABLE bronze.snapshots (
            snapshot_id  INTEGER PRIMARY KEY DEFAULT nextval('bronze.snapshots_id_seq'),
            hash         UBIGINT NOT NULL,
            product_id   INTEGER NOT NULL,
            url_prefix   VARCHAR NOT NULL,
            url_slug     VARCHAR,
            fetched_at   TIMESTAMP NOT NULL
        );
        """
    )

    t1 = datetime(2026, 8, 1, 10, 0, 0)
    t2 = datetime(2026, 8, 2, 10, 0, 0)
    t3 = datetime(2026, 8, 3, 10, 0, 0)
    t4 = datetime(2026, 8, 4, 10, 0, 0)

    # Product 10: hash 100 at t1, t2; hash 200 at t3; hash 100 at t4
    conn.execute(
        """
        INSERT INTO bronze.snapshots (snapshot_id, hash, product_id, url_prefix, url_slug, fetched_at)
        VALUES
        (1, 100, 10, '/ep/', 'slug', $1),
        (2, 100, 10, '/ep/', 'slug', $2),
        (3, 200, 10, '/ep/', 'slug', $3),
        (4, 100, 10, '/ep/', 'slug', $4)
        """,
        [t1, t2, t3, t4],
    )
    conn.close()

    # Run migration
    migrate(db_file)

    conn = duckdb.connect(db_file)
    cols = [col[1] for col in conn.execute("PRAGMA table_info('bronze.snapshots')").fetchall()]
    assert "last_checked_at" in cols
    assert "created_at" in cols

    rows = conn.execute(
        "SELECT snapshot_id, hash, product_id, url_prefix, created_at, last_checked_at FROM bronze.snapshots ORDER BY snapshot_id"
    ).fetchall()
    assert len(rows) == 3

    # Row 1: collapsed (1, 2) -> created t1, last_checked t2
    assert rows[0][0] == 1
    assert rows[0][1] == 100
    assert rows[0][4] == t1
    assert rows[0][5] == t2

    # Row 2: (3) -> created t3, last_checked t3
    assert rows[1][0] == 3
    assert rows[1][1] == 200
    assert rows[1][4] == t3
    assert rows[1][5] == t3

    # Row 3: (4) -> created t4, last_checked t4
    assert rows[2][0] == 4
    assert rows[2][1] == 100
    assert rows[2][4] == t4
    assert rows[2][5] == t4

    # Test sequence is aligned
    next_row = conn.execute("SELECT nextval('bronze.snapshots_id_seq')").fetchone()
    assert next_row is not None
    assert next_row[0] >= 5

    conn.close()

    # Calling migrate again is a no-op
    migrate(db_file)
