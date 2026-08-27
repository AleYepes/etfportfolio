import json
from pathlib import Path

import httpx
import pytest
import respx

from etfportfolio.core.db import connect
from etfportfolio.ingestion.themes import sync, upsert_themes


def test_upsert_themes_hierarchy(tmp_path):
    db_file = str(tmp_path / "test_themes.duckdb")

    payload = {
        "parents": [
            {"key": "parent-1", "numId": 10, "name": "Technology"},
            {"key": "parent-2", "numId": 20, "name": "Healthcare"},
        ],
        "nodes": [
            {"key": "child-1", "numId": 101, "name": "AI", "parentKey": "parent-1"},
            {"key": "child-2", "numId": 102, "name": "Biotech", "parentKey": "parent-2"},
        ],
    }

    with connect(db_file) as conn:
        p_count, n_count = upsert_themes(conn, payload)
        assert p_count == 2
        assert n_count == 2

        # Check that parents have NULL parent_id
        parent_rows = conn.execute(
            "SELECT theme_id, num_id, name, parent_id FROM bronze.themes WHERE parent_id IS NULL ORDER BY num_id"
        ).fetchall()
        assert len(parent_rows) == 2
        assert parent_rows[0] == ("parent-1", 10, "Technology", None)

        # Check child nodes have parent_id
        child_rows = conn.execute(
            "SELECT theme_id, num_id, name, parent_id FROM bronze.themes WHERE parent_id IS NOT NULL ORDER BY num_id"
        ).fetchall()
        assert len(child_rows) == 2
        assert child_rows[0] == ("child-1", 101, "AI", "parent-1")


@pytest.mark.anyio
@respx.mock
async def test_themes_sync_with_fixture(tmp_path):
    db_file = str(tmp_path / "test_themes_sync.duckdb")
    fixture = json.loads(Path("tests/fixtures/all_themes.json").read_text(encoding="utf-8"))

    respx.get("https://www.interactivebrokers.ie/tws.proxy/knowledge-graph/meta/themes").mock(
        return_value=httpx.Response(200, json=fixture)
    )

    async with httpx.AsyncClient(base_url="https://www.interactivebrokers.ie") as client:
        with connect(db_file) as conn:
            p_cnt, n_cnt = await sync(client=client, conn=conn)
            assert p_cnt == len(fixture.get("parents", []))
            assert n_cnt == len(fixture.get("nodes", []))

            total = conn.execute("SELECT COUNT(*) FROM bronze.themes").fetchone()[0]
            assert total == p_cnt + n_cnt

            # Second sync against the same populated database to verify FK constraint idempotency
            p_cnt_2, n_cnt_2 = await sync(client=client, conn=conn)
            assert p_cnt_2 == p_cnt
            assert n_cnt_2 == n_cnt
            total_2 = conn.execute("SELECT COUNT(*) FROM bronze.themes").fetchone()[0]
            assert total_2 == total
