import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingest.details import (
    load_endpoint_freshness_cache,
    load_landing_freshness_cache,
    process_product,
)


def test_load_freshness_caches(db_conn):
    t = datetime(2026, 9, 1, 12, 0, 0)
    db_conn.execute(
        """
        INSERT INTO bronze.snapshot_previews (product_id, hash, updated_at, last_checked_at)
        VALUES (101, 111, $1, $1)
        """,
        [t],
    )
    db_conn.execute(
        """
        INSERT INTO bronze.snapshots (product_id, url_prefix, hash, created_at, last_checked_at)
        VALUES (101, '/prefix/test/', 111, $1, $1)
        """,
        [t],
    )

    landing_cache = load_landing_freshness_cache(db_conn)
    assert landing_cache[101] == t

    endpoint_cache = load_endpoint_freshness_cache(db_conn)
    assert endpoint_cache[(101, "/prefix/test/")] == t


@pytest.mark.anyio
async def test_process_product_skips_when_fresh(tmp_path):
    db_file = str(tmp_path / "test_details_fresh.duckdb")
    now = datetime.now(UTC)

    # Prepare caches indicating everything is fresh
    from etfportfolio.core import endpoints

    landing_cache = {1001: now}
    endpoint_cache = {(1001, ep.url_prefix): now for ep in endpoints.UNGATED_ENDPOINTS}

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(5)
        res = await process_product(
            client,
            worker,
            1001,
            "U123456",
            semaphore,
            landing_cache,
            endpoint_cache,
            force=False,
        )

    assert res.ok is True
    assert res.product_skipped_fresh is True
    assert res.endpoints_skipped_fresh == len(endpoints.UNGATED_ENDPOINTS)


@pytest.mark.anyio
async def test_process_product_landing_changed_fetches_gated(tmp_path):
    db_file = str(tmp_path / "test_details_changed.duckdb")
    now = datetime.now(UTC)
    stale = now - timedelta(days=10)

    landing_cache = {1001: stale}
    endpoint_cache = {}

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(5)
        # Mock landing changed = True
        with (
            patch("etfportfolio.ingest.details.landing.fetch_and_gate", new_callable=AsyncMock) as mock_gate,
            patch("etfportfolio.ingest.details.landing.commit_preview", new_callable=AsyncMock) as mock_commit,
            patch("etfportfolio.ingest.details.snapshots.fetch_snapshot", new_callable=AsyncMock) as mock_snap,
        ):
            mock_gate.return_value = (True, 999999, b"compressed")

            res = await process_product(
                client,
                worker,
                1001,
                "U123456",
                semaphore,
                landing_cache,
                endpoint_cache,
                force=False,
            )

    assert res.ok is True
    assert res.product_skipped_fresh is False
    assert mock_gate.called
    assert mock_commit.called
    # Both ungated and gated endpoints should be fetched
    from etfportfolio.core import endpoints

    total_expected_eps = len(endpoints.DETAILS_ENDPOINTS)
    assert mock_snap.call_count == total_expected_eps
