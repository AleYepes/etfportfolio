import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from etfportfolio.core import endpoints
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingest import session
from etfportfolio.ingest.details import (
    load_endpoint_freshness_cache,
    load_landing_freshness_cache,
    process_product,
)


def test_load_freshness_caches(db_conn):
    t1 = datetime(2026, 9, 1, 10, 0, 0)
    t2 = datetime(2026, 9, 1, 12, 0, 0)
    db_conn.execute(
        """
        INSERT INTO bronze.snapshots (product_id, url_prefix, hash, created_at, last_checked_at)
        VALUES (101, '/prefix/test/', 111, $1, $1),
               (101, '/prefix/test/', 111, $2, $2)
        """,
        [t1, t2],
    )
    db_conn.execute(
        """
        INSERT INTO bronze.snapshot_previews (product_id, hash, updated_at, last_checked_at)
        VALUES (101, 999, $1, $2)
        """,
        [t1, t2],
    )

    endpoint_cache = load_endpoint_freshness_cache(db_conn)
    assert endpoint_cache[(101, "/prefix/test/")] == t2

    landing_cache = load_landing_freshness_cache(db_conn)
    assert landing_cache[101] == t2


@pytest.mark.anyio
async def test_process_product_all_fresh_skipped(tmp_path):
    db_file = str(tmp_path / "test_details_all_fresh.duckdb")
    now = datetime.now(UTC)

    landing_cache = {1001: now}
    endpoint_cache = {(1001, ep.url_prefix): now for ep in endpoints.DETAILS_ENDPOINTS}

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(3)
        with patch("etfportfolio.ingest.details.snapshots.fetch_snapshot", new_callable=AsyncMock) as mock_snap:
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
    # 2 ungated + 5 gated = 7
    assert res.endpoints_skipped_fresh == 7
    mock_snap.assert_not_called()


@pytest.mark.anyio
async def test_process_product_landing_fresh_stale_ungated_fetches_only_ungated(tmp_path):
    db_file = str(tmp_path / "test_details_landing_fresh.duckdb")
    now = datetime.now(UTC)

    landing_cache = {1001: now}
    # Only ungated are stale
    endpoint_cache = {(1001, ep.url_prefix): now for ep in endpoints.GATED_ENDPOINTS}

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(3)
        with (
            patch("etfportfolio.ingest.details.landing.fetch_and_gate", new_callable=AsyncMock) as mock_gate,
            patch("etfportfolio.ingest.details.snapshots.fetch_snapshot", new_callable=AsyncMock) as mock_snap,
        ):
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
    # Landing not fetched because fresh
    mock_gate.assert_not_called()
    # 2 ungated endpoints fetched
    assert mock_snap.call_count == len(endpoints.UNGATED_ENDPOINTS)
    # 5 gated endpoints skipped because landing was fresh
    assert res.endpoints_skipped_fresh == len(endpoints.GATED_ENDPOINTS)


@pytest.mark.anyio
async def test_process_product_landing_changed_fetches_gated_and_commits(tmp_path):
    db_file = str(tmp_path / "test_details_landing_changed.duckdb")
    stale = datetime.now(UTC) - timedelta(days=10)

    landing_cache = {1001: stale}
    endpoint_cache = {}  # All stale

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(3)
        with (
            patch("etfportfolio.ingest.details.landing.fetch_and_gate", new_callable=AsyncMock) as mock_gate,
            patch("etfportfolio.ingest.details.landing.commit_preview", new_callable=AsyncMock) as mock_commit,
            patch("etfportfolio.ingest.details.snapshots.fetch_snapshot", new_callable=AsyncMock) as mock_snap,
        ):
            mock_gate.return_value = (True, 999888777)

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
    # 2 ungated in Wave 1 + 5 gated in Wave 2 = 7 endpoint fetches
    assert mock_snap.call_count == len(endpoints.DETAILS_ENDPOINTS)
    mock_commit.assert_awaited_once_with(worker, 1001, 999888777)


@pytest.mark.anyio
async def test_process_product_landing_unchanged_skips_gated_and_stamps(tmp_path):
    db_file = str(tmp_path / "test_details_landing_unchanged.duckdb")
    stale = datetime.now(UTC) - timedelta(days=10)

    landing_cache = {1001: stale}
    endpoint_cache = {}

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(3)
        with (
            patch("etfportfolio.ingest.details.landing.fetch_and_gate", new_callable=AsyncMock) as mock_gate,
            patch("etfportfolio.ingest.details.landing.stamp_last_checked", new_callable=AsyncMock) as mock_stamp,
            patch("etfportfolio.ingest.details.landing.commit_preview", new_callable=AsyncMock) as mock_commit,
            patch("etfportfolio.ingest.details.snapshots.fetch_snapshot", new_callable=AsyncMock) as mock_snap,
        ):
            # Landing fetched, but unchanged
            mock_gate.return_value = (False, 111222333)

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
    # Wave 1 fetched 2 ungated endpoints; Wave 2 was skipped because landing is unchanged
    assert mock_snap.call_count == len(endpoints.UNGATED_ENDPOINTS)
    assert res.endpoints_skipped_fresh == len(endpoints.GATED_ENDPOINTS)
    mock_stamp.assert_awaited_once_with(worker, 1001)
    mock_commit.assert_not_called()


@pytest.mark.anyio
async def test_process_product_gated_failure_does_not_commit_landing(tmp_path):
    db_file = str(tmp_path / "test_details_gated_fail.duckdb")
    stale = datetime.now(UTC) - timedelta(days=10)

    landing_cache = {1001: stale}
    endpoint_cache = {}

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(3)
        with (
            patch("etfportfolio.ingest.details.landing.fetch_and_gate", new_callable=AsyncMock) as mock_gate,
            patch("etfportfolio.ingest.details.landing.commit_preview", new_callable=AsyncMock) as mock_commit,
            patch("etfportfolio.ingest.details.landing.stamp_last_checked", new_callable=AsyncMock) as mock_stamp,
            patch("etfportfolio.ingest.details.snapshots.fetch_snapshot", new_callable=AsyncMock) as mock_snap,
        ):
            mock_gate.return_value = (True, 999888777)

            async def side_effect(_client, _worker, ep, _product_id, _account_id):
                if ep.name == "holdings":
                    raise RuntimeError("HTTP 429 retries exhausted")
                return None

            mock_snap.side_effect = side_effect

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

    assert res.ok is False
    # Landing preview must NOT be committed or stamped when a gated endpoint fails
    mock_commit.assert_not_called()
    mock_stamp.assert_not_called()


@pytest.mark.anyio
async def test_process_product_session_invalid_reraises(tmp_path):
    db_file = str(tmp_path / "test_details_invalid_session.duckdb")
    stale = datetime.now(UTC) - timedelta(days=10)

    landing_cache = {1001: stale}
    endpoint_cache = {}

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(3)
        with patch("etfportfolio.ingest.details.landing.fetch_and_gate", new_callable=AsyncMock) as mock_gate:
            mock_gate.side_effect = session.SessionInvalidError("Invalid headers")
            with pytest.raises(session.SessionInvalidError):
                await process_product(
                    client,
                    worker,
                    1001,
                    "U123456",
                    semaphore,
                    landing_cache,
                    endpoint_cache,
                    force=False,
                )
