import json
from pathlib import Path

from etfportfolio.core.db import connect
from etfportfolio.core.utils import content_address
from etfportfolio.ingestion.snapshots import store_snapshot


def test_store_snapshot(tmp_path):
    db_file = str(tmp_path / "test_snapshots.duckdb")
    fixture_path = Path("tests/fixtures/holdings_complete.json")
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))

    with connect(db_file) as conn:
        # Create product first to satisfy FK
        conn.execute(
            "INSERT INTO bronze.products (product_id, type, symbol, created_at, updated_at) VALUES (1001, 'ETF', 'SPY', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        )

        digest = store_snapshot(
            conn=conn,
            product_id=1001,
            url_prefix="fundamentals/mf_holdings/",
            url_slug="1001?lang=en",
            payload=payload,
        )

        expected_digest, _ = content_address(payload)
        assert digest == expected_digest

        # Check payload_blobs
        blob_count = conn.execute("SELECT COUNT(*) FROM bronze.payload_blobs WHERE hash = ?", [digest]).fetchone()[0]
        assert blob_count == 1

        # Check snapshots lineage
        snap = conn.execute(
            "SELECT product_id, url_prefix, url_slug, hash FROM bronze.snapshots WHERE product_id = 1001"
        ).fetchone()
        assert snap == (1001, "fundamentals/mf_holdings/", "1001?lang=en", digest)

        # Store again with exact same payload -> should insert another lineage row but payload_blobs remains 1
        store_snapshot(
            conn=conn,
            product_id=1001,
            url_prefix="fundamentals/mf_holdings/",
            url_slug="1001?lang=en",
            payload=payload,
        )

        assert conn.execute("SELECT COUNT(*) FROM bronze.payload_blobs WHERE hash = ?", [digest]).fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM bronze.snapshots WHERE product_id = 1001").fetchone()[0] == 2
