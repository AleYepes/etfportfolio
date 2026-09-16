from unittest.mock import AsyncMock, patch

import httpx
import pytest

from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingest.landing import (
    commit_preview,
    fetch_and_gate,
    stamp_last_checked,
)


@pytest.mark.anyio
async def test_fetch_and_gate_changed_and_unchanged(tmp_path):
    db_file = str(tmp_path / "test_landing.duckdb")

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        with patch("etfportfolio.ingest.landing.session.fetch_with_retry", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (200, {"title": "Landing Widget", "data": [1, 2, 3]})

            # First run: product has no preview in bronze.snapshot_previews -> changed = True
            changed, digest, comp = await fetch_and_gate(client, 1001, worker)
            assert changed is True
            assert isinstance(digest, int)
            assert isinstance(comp, bytes)

            # Commit preview
            await commit_preview(worker, 1001, digest, comp)

            # Second run with same payload -> changed = False
            changed_again, digest_again, _ = await fetch_and_gate(client, 1001, worker)
            assert changed_again is False
            assert digest_again == digest

            # Third run with modified payload -> changed = True
            mock_fetch.return_value = (200, {"title": "Landing Widget", "data": [1, 2, 3, 4]})
            changed_new, digest_new, comp_new = await fetch_and_gate(client, 1001, worker)
            assert changed_new is True
            assert digest_new != digest

            # Commit new preview -> should update preview and trigger old blob GC
            await commit_preview(worker, 1001, digest_new, comp_new)


@pytest.mark.anyio
async def test_stamp_last_checked(tmp_path):
    db_file = str(tmp_path / "test_stamp.duckdb")

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        with patch("etfportfolio.ingest.landing.session.fetch_with_retry", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (200, {"initial": 1})
            _, digest, comp = await fetch_and_gate(client, 1002, worker)
            await commit_preview(worker, 1002, digest, comp)

        # Stamp last checked
        await stamp_last_checked(worker, 1002)

    import duckdb

    conn = duckdb.connect(db_file)
    row = conn.execute(
        "SELECT hash, updated_at, last_checked_at FROM bronze.snapshot_previews WHERE product_id = 1002"
    ).fetchone()
    assert row is not None
    assert row[0] == digest
    assert row[2] is not None
    conn.close()
