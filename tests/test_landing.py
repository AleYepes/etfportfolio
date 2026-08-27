import json
from pathlib import Path

import httpx
import pytest
import respx

from etfportfolio.core.db import connect
from etfportfolio.ingestion.landing import commit_preview, fetch_and_gate


@pytest.mark.anyio
@respx.mock
async def test_landing_fetch_and_gate_lifecycle(tmp_path):
    db_file = str(tmp_path / "test_landing.duckdb")
    fixture = json.loads(Path("tests/fixtures/landing_equity.json").read_text(encoding="utf-8"))

    pid = 756733
    landing_url = (
        f"https://www.interactivebrokers.ie/tws.proxy/fundamentals/landing/{pid}"
        "?widgets=objective,keyProfile,lipper_ratings,holdings,mf_key_ratios,ownership,mstar&lang=en"
    )

    respx.get(landing_url).mock(return_value=httpx.Response(200, json=fixture))

    async with httpx.AsyncClient(base_url="https://www.interactivebrokers.ie") as client:
        with connect(db_file) as conn:
            conn.execute(
                "INSERT INTO bronze.products (product_id, type, symbol, created_at, updated_at) VALUES (756733, 'ETF', 'SPY', now(), now())"
            )

            # 1. First fetch: product has no snapshot_preview -> changed = True
            changed, digest, compressed, payload = await fetch_and_gate(client, pid, conn)
            assert changed is True
            assert isinstance(digest, int)
            assert len(compressed) > 0

            # 2. Commit preview
            commit_preview(conn, pid, digest, compressed)
            row = conn.execute("SELECT hash FROM bronze.snapshot_previews WHERE product_id = ?", [pid]).fetchone()
            assert row[0] == digest

            # 3. Second fetch with same response -> changed = False
            changed2, digest2, _, _ = await fetch_and_gate(client, pid, conn)
            assert changed2 is False
            assert digest2 == digest

            # 4. Modify fixture -> changed = True
            mod_fixture = dict(fixture)
            mod_fixture["objective"] = "Updated Objective Text"
            respx.get(landing_url).mock(return_value=httpx.Response(200, json=mod_fixture))

            changed3, digest3, compressed3, _ = await fetch_and_gate(client, pid, conn)
            assert changed3 is True
            assert digest3 != digest

            # Commit new preview -> old digest should be GC'd
            commit_preview(conn, pid, digest3, compressed3)
            old_blob = conn.execute("SELECT hash FROM bronze.payload_blobs WHERE hash = ?", [digest]).fetchone()
            assert old_blob is None  # Old blob was GC'd

            new_blob = conn.execute("SELECT hash FROM bronze.payload_blobs WHERE hash = ?", [digest3]).fetchone()
            assert new_blob is not None
