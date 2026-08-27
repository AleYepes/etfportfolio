import pytest

from etfportfolio.core.db import connect, current, gc_preview_blob
from etfportfolio.core.utils import content_address


def test_db_connection_and_schema(tmp_path):
    db_file = str(tmp_path / "test.duckdb")

    with connect(db_file) as conn:
        assert current() is conn
        # Check that schemas exist
        schemas = [row[0] for row in conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()]
        assert "bronze" in schemas
        assert "silver" in schemas
        assert "gold" in schemas

        # Check bronze tables
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'"
            ).fetchall()
        ]
        assert "payload_blobs" in tables
        assert "products" in tables
        assert "snapshot_previews" in tables
        assert "snapshots" in tables
        assert "series" in tables
        assert "themes" in tables

    with pytest.raises(RuntimeError):
        current()


def test_blob_gc_orphaned_blob(tmp_path):
    db_file = str(tmp_path / "test_gc.duckdb")

    with connect(db_file) as conn:
        data1 = {"sample": "data1"}
        h1, b1 = content_address(data1)
        data2 = {"sample": "data2"}
        h2, b2 = content_address(data2)

        # Insert product first
        conn.execute(
            """
            INSERT INTO bronze.products (product_id, type, symbol, created_at, updated_at)
            VALUES (1, 'ETF', 'TEST', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
        )

        # Insert payload blobs
        conn.execute("INSERT INTO bronze.payload_blobs VALUES (?, ?)", [h1, b1])
        conn.execute("INSERT INTO bronze.payload_blobs VALUES (?, ?)", [h2, b2])

        # Point snapshot_preview to h1
        conn.execute("INSERT INTO bronze.snapshot_previews VALUES (1, ?, CURRENT_TIMESTAMP)", [h1])

        # Point snapshot_preview to h2 (update)
        conn.execute(
            "UPDATE bronze.snapshot_previews SET hash = ?, updated_at = CURRENT_TIMESTAMP WHERE product_id = 1", [h2]
        )

        # Run GC on h1 (should be deleted)
        deleted = gc_preview_blob(conn, h1)
        assert deleted is True

        res = conn.execute("SELECT hash FROM bronze.payload_blobs WHERE hash = ?", [h1]).fetchone()
        assert res is None

        # Run GC on h2 (should NOT be deleted because it is referenced in snapshot_previews)
        deleted2 = gc_preview_blob(conn, h2)
        assert deleted2 is False
        res2 = conn.execute("SELECT hash FROM bronze.payload_blobs WHERE hash = ?", [h2]).fetchone()
        assert res2 is not None
