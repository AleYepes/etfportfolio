from pathlib import Path

import duckdb
import pytest

from etfportfolio.core.db import AsyncDbWorker, apply_schema, db_connection


def test_apply_schema_creates_schemas_and_tables(db_conn):
    schemas = [r[0] for r in db_conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()]
    assert "bronze" in schemas
    assert "silver" in schemas
    assert "gold" in schemas
    assert "cold_storage" in schemas

    # Check key tables exist
    tables = [r[0] for r in db_conn.execute("SELECT table_name FROM information_schema.tables").fetchall()]
    assert "products" in tables
    assert "contracts" in tables
    assert "prices" in tables
    assert "snapshots" in tables
    assert "snapshot_previews" not in tables
    assert "observations" in tables
    assert "monthly_panel" in tables
    assert "product_metrics" not in tables
    assert "product_dimensions" not in tables


def test_monthly_panel_has_no_asset_class_column(db_conn):
    cols = [
        r[0]
        for r in db_conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'silver' AND table_name = 'monthly_panel'
            """
        ).fetchall()
    ]
    assert "asset_class" not in cols
    assert set(cols) == {"product_id", "as_of_date", "feature_id", "value"}


def test_observations_schema(db_conn):
    cols = [
        r[0]
        for r in db_conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'silver' AND table_name = 'observations'
            """
        ).fetchall()
    ]
    expected = {
        "product_id",
        "family",
        "metric",
        "code",
        "effective_date",
        "date_source_depth",
        "fetched_at",
        "value",
        "raw_value",
    }
    assert set(cols) == expected


def test_apply_schema_is_idempotent(db_conn):
    # Applying schema multiple times consecutively should not fail or duplicate sequences
    apply_schema(db_conn)
    apply_schema(db_conn)
    apply_schema(db_conn)

    # Sequences should still exist and work
    row = db_conn.execute("SELECT nextval('bronze.snapshots_id_seq')").fetchone()
    assert row is not None
    seq_val = row[0]
    assert seq_val >= 1


def test_db_connection_context_manager(tmp_path: Path):
    db_file = str(tmp_path / "test_ctx.duckdb")
    with db_connection(db_file) as conn:
        conn.execute(
            "INSERT INTO bronze.products (product_id, symbol, created_at, updated_at) VALUES (1, 'AAA', now(), now())"
        )
        row = conn.execute("SELECT COUNT(*) FROM bronze.products").fetchone()
        assert row is not None
        assert row[0] == 1

    # Verify closure and persistence
    verify_conn = duckdb.connect(db_file)
    row = verify_conn.execute("SELECT COUNT(*) FROM bronze.products").fetchone()
    assert row is not None
    assert row[0] == 1
    verify_conn.close()


@pytest.mark.anyio
async def test_async_db_worker_fifo_and_exceptions(tmp_path: Path):
    db_file = str(tmp_path / "test_worker.duckdb")

    def insert_record(conn, pid: int, symbol: str):
        conn.execute(
            "INSERT INTO bronze.products (product_id, symbol, created_at, updated_at) VALUES (?, ?, now(), now())",
            [pid, symbol],
        )
        return pid

    def failing_task(conn):
        raise ValueError("Simulated DB task failure")

    def fetch_symbols(conn):
        return [r[0] for r in conn.execute("SELECT symbol FROM bronze.products ORDER BY product_id").fetchall()]

    async with AsyncDbWorker(db_file) as worker:
        res1 = await worker.submit(insert_record, 10, "SYM10")
        res2 = await worker.submit(insert_record, 20, "SYM20")
        assert res1 == 10
        assert res2 == 20

        # Verify exception propagation across worker boundary
        with pytest.raises(ValueError, match="Simulated DB task failure"):
            await worker.submit(failing_task)

        # Ensure worker continues processing after an exception
        symbols = await worker.submit(fetch_symbols)
        assert symbols == ["SYM10", "SYM20"]


@pytest.mark.anyio
async def test_async_db_worker_startup_failure(tmp_path: Path):
    invalid_db_path = str(tmp_path / "non_existent_dir" / "\0_invalid.duckdb")
    with pytest.raises((duckdb.Error, ValueError, OSError)):
        async with AsyncDbWorker(invalid_db_path):
            pass
