from datetime import datetime
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingest.details import load_endpoint_freshness_cache
from etfportfolio.ingest.endpoints import ENDPOINTS_BY_NAME
from etfportfolio.ingest.snapshots import fetch_snapshot, store_snapshot


def test_store_snapshot_changelog(db_conn):
    db_conn.execute(
        """
        INSERT INTO bronze.products (product_id, symbol, created_at, updated_at)
        VALUES (1001, 'TEST', now(), now())
        """
    )

    t1 = datetime(2026, 9, 1, 10, 0, 0)
    payload_a = {"val": 1}

    # 1. Initial store: inserts new row
    store_snapshot(db_conn, 1001, "/test/ep/", "slug", payload_a, fetched_at=t1)

    rows = db_conn.execute(
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
    store_snapshot(db_conn, 1001, "/test/ep/", "slug", payload_a, fetched_at=t2)

    rows = db_conn.execute(
        "SELECT snapshot_id, product_id, url_prefix, created_at, last_checked_at FROM bronze.snapshots"
    ).fetchall()
    assert len(rows) == 1
    snap_id_2, pid, prefix, created_at_2, last_checked_2 = rows[0]
    assert snap_id_2 == snap_id_1
    assert created_at_2 == t1
    assert last_checked_2 == t2

    # 3. Freshness cache queries MAX(last_checked_at)
    cache = load_endpoint_freshness_cache(db_conn)
    assert cache[(1001, "/test/ep/")] == t2

    # 4. Store changed payload at t3: inserts new row
    t3 = datetime(2026, 9, 3, 10, 0, 0)
    payload_b = {"val": 2}
    store_snapshot(db_conn, 1001, "/test/ep/", "slug", payload_b, fetched_at=t3)

    rows = db_conn.execute(
        "SELECT snapshot_id, product_id, url_prefix, created_at, last_checked_at FROM bronze.snapshots ORDER BY snapshot_id"
    ).fetchall()
    assert len(rows) == 2
    assert rows[1][0] > snap_id_1
    assert rows[1][3] == t3
    assert rows[1][4] == t3


def test_store_snapshot_transactional_rollback(db_conn):
    # If store_blob or insert fails, rollback is triggered
    with (
        patch("etfportfolio.ingest.snapshots.store_blob", side_effect=RuntimeError("Disk write error")),
        pytest.raises(RuntimeError, match="Disk write error"),
    ):
        store_snapshot(db_conn, 1001, "/test/ep/", "slug", {"data": "abc"})

    # Ensure no rows were added to snapshots
    count = db_conn.execute("SELECT COUNT(*) FROM bronze.snapshots").fetchone()[0]
    assert count == 0


@pytest.mark.anyio
async def test_fetch_snapshot_stores_empty_dict_on_404(tmp_path):
    db_file = str(tmp_path / "test_404.duckdb")

    ep = ENDPOINTS_BY_NAME["ratios"]
    async with AsyncDbWorker(db_file) as worker:
        # Mock fetch_with_retry returning (404, None)
        with patch("etfportfolio.ingest.snapshots.session.fetch_with_retry", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (404, None)

            async with httpx.AsyncClient() as client:
                await fetch_snapshot(client, worker, ep, 1001, "U123456")

    # Verify DuckDB has stored an entry with {}
    import duckdb

    conn = duckdb.connect(db_file)
    rows = conn.execute("SELECT product_id, url_prefix, hash FROM bronze.snapshots").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 1001
    assert rows[0][1] == ep.url_prefix
    conn.close()
