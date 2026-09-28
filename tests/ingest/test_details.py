import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import duckdb
import httpx
import pytest

from etfportfolio.core import endpoints
from etfportfolio.core.db import AsyncDbWorker, apply_schema
from etfportfolio.ingest import session
from etfportfolio.ingest.details import (
    load_endpoint_freshness_cache,
    process_product,
)


def test_load_endpoint_freshness_cache(db_conn):
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

    endpoint_cache = load_endpoint_freshness_cache(db_conn)
    assert endpoint_cache[(101, "/prefix/test/")] == t2


@pytest.mark.anyio
async def test_process_product_all_fresh_skipped(tmp_path):
    db_file = str(tmp_path / "test_details_all_fresh.duckdb")
    now = datetime.now(UTC)

    endpoint_cache = {(1001, ep.url_prefix): now for ep in endpoints.ENDPOINTS}

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(5)
        with patch("etfportfolio.ingest.details.snapshots.fetch_snapshot", new_callable=AsyncMock) as mock_snap:
            res = await process_product(
                client,
                worker,
                1001,
                "U123456",
                semaphore,
                endpoint_cache,
                force=False,
            )

    assert res.ok is True
    assert res.product_skipped_fresh is True
    assert res.endpoints_skipped_fresh == 7
    mock_snap.assert_not_called()


@pytest.mark.anyio
async def test_process_product_selective_stale_fetch(tmp_path):
    db_file = str(tmp_path / "test_details_selective.duckdb")
    now = datetime.now(UTC)

    fresh_eps = endpoints.ENDPOINTS[:5]
    stale_eps = endpoints.ENDPOINTS[5:]

    endpoint_cache = {(1001, ep.url_prefix): now for ep in fresh_eps}

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(5)
        with patch("etfportfolio.ingest.details.snapshots.fetch_snapshot", new_callable=AsyncMock) as mock_snap:
            res = await process_product(
                client,
                worker,
                1001,
                "U123456",
                semaphore,
                endpoint_cache,
                force=False,
            )

    assert res.ok is True
    assert res.product_skipped_fresh is False
    assert res.endpoints_skipped_fresh == 5
    assert mock_snap.call_count == 2
    requested_prefixes = {call.args[2].url_prefix for call in mock_snap.call_args_list}
    assert requested_prefixes == {ep.url_prefix for ep in stale_eps}


@pytest.mark.anyio
async def test_process_product_partial_failure_continuation(tmp_path):
    db_file = str(tmp_path / "test_details_partial.duckdb")
    with duckdb.connect(db_file) as conn:
        apply_schema(conn)

    endpoint_cache = {}  # All 7 stale

    failed_ep = endpoints.ENDPOINTS[0]

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(5)

        with patch("etfportfolio.ingest.snapshots.session.fetch_with_retry", new_callable=AsyncMock) as mock_fetch:

            async def side_effect(_client, url, **_kwargs):
                if failed_ep.url_prefix in url:
                    raise RuntimeError("Persistent network failure after 5 attempts")
                return 200, {"key": "val"}

            mock_fetch.side_effect = side_effect

            res = await process_product(
                client,
                worker,
                1001,
                "U123456",
                semaphore,
                endpoint_cache,
                force=False,
            )

    assert res.ok is False
    assert res.product_skipped_fresh is False
    assert res.endpoints_skipped_fresh == 0

    with duckdb.connect(db_file) as conn:
        rows = conn.execute("SELECT url_prefix FROM bronze.snapshots WHERE product_id = 1001").fetchall()
        persisted_prefixes = {r[0] for r in rows}

    assert len(persisted_prefixes) == 6
    assert failed_ep.url_prefix not in persisted_prefixes


@pytest.mark.anyio
async def test_process_product_session_invalid_reraises(tmp_path):
    db_file = str(tmp_path / "test_details_invalid_session.duckdb")
    endpoint_cache = {}

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(5)
        with patch("etfportfolio.ingest.details.snapshots.fetch_snapshot", new_callable=AsyncMock) as mock_snap:
            mock_snap.side_effect = session.SessionInvalidError("Invalid headers")
            with pytest.raises(session.SessionInvalidError):
                await process_product(
                    client,
                    worker,
                    1001,
                    "U123456",
                    semaphore,
                    endpoint_cache,
                    force=False,
                )
