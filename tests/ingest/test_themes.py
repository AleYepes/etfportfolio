from unittest.mock import AsyncMock, patch

import httpx
import pytest

from etfportfolio.core.config import settings
from etfportfolio.ingest.themes import sync, upsert_themes


def test_upsert_themes_hierarchy(db_conn):
    payload = {
        "parents": [
            {"key": "parent-1", "numId": 10, "name": "Technology"},
            {"key": "parent-2", "numId": 20, "name": "Healthcare"},
        ],
        "nodes": [
            {"key": "node-1", "numId": 101, "name": "Artificial Intelligence", "parentKey": "parent-1"},
            {"key": "node-2", "numId": 201, "name": "Genomics", "parentKey": "parent-2"},
        ],
    }

    p_count, n_count = upsert_themes(db_conn, payload)
    assert p_count == 2
    assert n_count == 2

    # Verify DuckDB records
    parents = db_conn.execute(
        "SELECT theme_id, name, parent_id FROM bronze.themes WHERE parent_id IS NULL ORDER BY theme_id"
    ).fetchall()
    assert len(parents) == 2
    assert parents[0][0] == "parent-1"
    assert parents[0][1] == "Technology"

    nodes = db_conn.execute(
        "SELECT theme_id, name, parent_id FROM bronze.themes WHERE parent_id IS NOT NULL ORDER BY theme_id"
    ).fetchall()
    assert len(nodes) == 2
    assert nodes[0][0] == "node-1"
    assert nodes[0][2] == "parent-1"


@pytest.mark.anyio
async def test_themes_sync_freshness_and_fetch(tmp_path, monkeypatch):
    db_file = str(tmp_path / "test_themes.duckdb")
    monkeypatch.setattr(settings, "db_path", db_file)

    sample_payload = {
        "parents": [{"key": "p1", "numId": 1, "name": "Parent"}],
        "nodes": [{"key": "n1", "numId": 2, "name": "Child", "parentKey": "p1"}],
    }

    async with httpx.AsyncClient() as client:
        with patch("etfportfolio.ingest.themes.session.fetch_with_retry", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = (200, sample_payload)

            # First sync: fetches from network
            p_cnt, n_cnt = await sync(client=client, force=False)
            assert p_cnt == 1
            assert n_cnt == 1
            assert mock_fetch.call_count == 1

            # Second sync immediately: fresh, skips network fetch
            p_cnt2, n_cnt2 = await sync(client=client, force=False)
            assert p_cnt2 == 1
            assert n_cnt2 == 1
            assert mock_fetch.call_count == 1  # Not incremented

            # Third sync with force=True: refetches from network
            p_cnt3, n_cnt3 = await sync(client=client, force=True)
            assert p_cnt3 == 1
            assert n_cnt3 == 1
            assert mock_fetch.call_count == 2
