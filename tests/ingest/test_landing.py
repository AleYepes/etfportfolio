from unittest.mock import AsyncMock, patch

import duckdb
import httpx
import pytest

from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingest import landing
from etfportfolio.ingest.utils import content_address


@pytest.mark.anyio
async def test_fetch_and_gate_new_product(tmp_path):
    db_file = str(tmp_path / "test_landing_new.duckdb")
    payload = {"widgets": {"holdings": {"items": [1, 2, 3]}}}
    digest, _ = content_address(payload)

    async with AsyncDbWorker(db_file) as worker:
        client = httpx.AsyncClient()
        with patch("etfportfolio.ingest.landing.session.fetch_with_retry", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (200, payload)

            changed, returned_digest = await landing.fetch_and_gate(client, 1001, worker)

            assert changed is True
            assert returned_digest == digest
        await client.aclose()


@pytest.mark.anyio
async def test_commit_and_stamp_preview(tmp_path):
    db_file = str(tmp_path / "test_landing_commit.duckdb")
    digest = 123456789

    async with AsyncDbWorker(db_file) as worker:
        # Commit new preview
        await landing.commit_preview(worker, 1001, digest)

        def _check(conn: duckdb.DuckDBPyConnection):
            row = conn.execute(
                "SELECT hash, updated_at, last_checked_at FROM bronze.snapshot_previews WHERE product_id = 1001"
            ).fetchone()
            assert row is not None
            assert row[0] == digest
            return row[1], row[2]

        updated_at_1, last_checked_1 = await worker.submit(_check)

        # Re-fetch with same payload -> changed is False
        client = httpx.AsyncClient()
        with patch("etfportfolio.ingest.landing.session.fetch_with_retry", new_callable=AsyncMock) as mock_fetch:
            with patch("etfportfolio.ingest.landing.content_address") as mock_ca:
                mock_fetch.return_value = (200, {"dummy": "data"})
                mock_ca.return_value = (digest, b"compressed")

                changed, returned_digest = await landing.fetch_and_gate(client, 1001, worker)
                assert changed is False
                assert returned_digest == digest
        await client.aclose()

        # Stamp last checked (hash unchanged)
        await landing.stamp_last_checked(worker, 1001)

        def _check_after_stamp(conn: duckdb.DuckDBPyConnection):
            row = conn.execute(
                "SELECT hash, updated_at, last_checked_at FROM bronze.snapshot_previews WHERE product_id = 1001"
            ).fetchone()
            assert row[0] == digest
            # updated_at must remain identical, last_checked_at must advance or match
            assert row[1] == updated_at_1
            assert row[2] >= last_checked_1

        await worker.submit(_check_after_stamp)


@pytest.mark.anyio
async def test_fetch_and_gate_404_handled_as_empty_dict(tmp_path):
    db_file = str(tmp_path / "test_landing_404.duckdb")
    expected_digest, _ = content_address({})

    async with AsyncDbWorker(db_file) as worker:
        client = httpx.AsyncClient()
        with patch("etfportfolio.ingest.landing.session.fetch_with_retry", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (404, None)

            changed, returned_digest = await landing.fetch_and_gate(client, 9999, worker)

            assert changed is True
            assert returned_digest == expected_digest
        await client.aclose()
