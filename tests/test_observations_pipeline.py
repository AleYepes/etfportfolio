from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from etfportfolio.core.db import apply_schema
from etfportfolio.ingestion.snapshots import store_snapshot
from etfportfolio.observations.pipeline import run_observations


@pytest.fixture
def obs_test_db(tmp_path: Path):
    db_file = str(tmp_path / "obs_test.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    conn.execute(
        """
        INSERT INTO bronze.products (product_id, symbol, created_at, updated_at)
        VALUES (8335, 'ICF', now(), now())
        """
    )

    t = datetime(2026, 9, 1, 12, 0, 0, tzinfo=UTC)

    # 1. ratios
    store_snapshot(
        conn,
        8335,
        "/tws.proxy/fundamentals/mf_ratios_fundamentals/",
        "slug",
        {
            "as_of_date": 1785470400000,
            "ratios": [{"name_tag": "price_earnings", "value": 25.5}],
        },
        fetched_at=t,
    )

    # 2. profile
    store_snapshot(
        conn,
        8335,
        "/tws.proxy/fundamentals/mf_profile_and_fees/",
        "slug",
        {
            "fund_and_profile": [
                {"name_tag": "Total_Expense_Ratio", "value": "0.32%"},
                {"name_tag": "Management_Approach", "value": "Passive"},
            ]
        },
        fetched_at=t,
    )

    # 3. esg
    store_snapshot(
        conn,
        8335,
        "/tws.proxy/impact/esg/",
        "slug",
        {
            "asOfDate": "20260822",
            "coverage": 0.99,
            "content": [{"name": "TRESGS", "value": 7}],
        },
        fetched_at=t,
    )

    # 4. holdings
    store_snapshot(
        conn,
        8335,
        "/tws.proxy/fundamentals/mf_holdings/",
        "slug",
        {
            "as_of_date": 1785470400000,
            "allocation_self": [{"name": "Equity", "weight": 99.8}],
        },
        fetched_at=t,
    )

    # 5. theme_weights
    store_snapshot(
        conn,
        8335,
        "/tws.proxy/knowledge-graph/ui/fund?conid=",
        "slug",
        {
            "themes": [
                {
                    "key": "006a0c27-4a9a-4766-8988-0d8acc6ede8b",
                    "name": "Discount Retail",
                    "weight": 0.084,
                    "rank_adjusted_weight": 0.009,
                }
            ]
        },
        fetched_at=t,
    )

    # 6. empty stub (valid no-op)
    store_snapshot(
        conn,
        8335,
        "/tws.proxy/fundamentals/mf_lip_ratings/",
        "slug",
        {},
        fetched_at=t,
    )

    conn.close()
    return db_file


def test_pipeline_execution_and_idempotency(obs_test_db):
    # 1. Initial run: should process all 6 snapshots
    processed = run_observations(force=False, db_path=obs_test_db)
    assert processed == 6

    conn = duckdb.connect(obs_test_db)

    # Verify watermark table has 6 records
    watermark_row = conn.execute("SELECT COUNT(*) FROM silver.processed_snapshots").fetchone()
    assert watermark_row is not None and watermark_row[0] == 6

    # Verify silver.product_metrics
    metrics = conn.execute(
        """
        SELECT source, metric_id, effective_date, effective_date_source, value
        FROM silver.product_metrics
        ORDER BY source, metric_id
        """
    ).fetchall()
    assert len(metrics) == 5

    metrics_dict = {(r[0], r[1]): (r[2], r[3], r[4]) for r in metrics}
    assert metrics_dict[("ratios", "price_earnings")] == (date(2026, 7, 31), "payload", 25.5)
    assert metrics_dict[("profile", "is_passive")] == (date(2026, 9, 1), "snapshot", 1.0)
    assert pytest.approx(metrics_dict[("profile", "total_expense_ratio")][2]) == 0.0032
    assert metrics_dict[("esg", "esg_coverage")] == (date(2026, 8, 22), "payload", 0.99)
    assert metrics_dict[("esg", "tresgs")] == (date(2026, 8, 22), "payload", 7.0)

    # Verify silver.product_dimensions
    dims = conn.execute(
        """
        SELECT dimension_type, dimension_name, dimension_code, effective_date, value
        FROM silver.product_dimensions
        ORDER BY dimension_type, dimension_name
        """
    ).fetchall()
    assert len(dims) == 2

    dims_dict = {(r[0], r[1]): (r[2], r[3], r[4]) for r in dims}
    assert dims_dict[("asset_class", "Equity")][0] is None
    assert dims_dict[("asset_class", "Equity")][1] == date(2026, 7, 31)
    assert pytest.approx(dims_dict[("asset_class", "Equity")][2]) == 0.998

    assert dims_dict[("theme", "Discount Retail")][0] == "006a0c27-4a9a-4766-8988-0d8acc6ede8b"
    assert dims_dict[("theme", "Discount Retail")][1] == date(2026, 9, 1)
    assert pytest.approx(dims_dict[("theme", "Discount Retail")][2]) == 0.009

    conn.close()

    # 2. Immediate second run: all up to date, 0 processed
    processed_again = run_observations(force=False, db_path=obs_test_db)
    assert processed_again == 0

    # 3. Run with force=True: wipes and repopulates
    processed_force = run_observations(force=True, db_path=obs_test_db)
    assert processed_force == 6

    conn = duckdb.connect(obs_test_db)
    proc_cnt = conn.execute("SELECT COUNT(*) FROM silver.processed_snapshots").fetchone()[0]
    assert proc_cnt == 6
    met_cnt = conn.execute("SELECT COUNT(*) FROM silver.product_metrics").fetchone()[0]
    assert met_cnt == 5
    dim_cnt = conn.execute("SELECT COUNT(*) FROM silver.product_dimensions").fetchone()[0]
    assert dim_cnt == 2
    conn.close()


def test_pipeline_in_memory_deduplication(tmp_path: Path):
    """Verifies that duplicate primary keys within the same batch do not throw DuckDB Constraint Errors

    and that the observation with the newer fetched_at wins.
    """
    db_file = str(tmp_path / "dedup_test.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    conn.execute(
        """
        INSERT INTO bronze.products (product_id, symbol, created_at, updated_at)
        VALUES (9999, 'TEST', now(), now())
        """
    )

    t1 = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 9, 1, 14, 0, 0, tzinfo=UTC)

    # Older snapshot: P/E = 20.0
    store_snapshot(
        conn,
        9999,
        "/tws.proxy/fundamentals/mf_ratios_fundamentals/",
        "slug",
        {
            "as_of_date": 1785470400000,  # 2026-07-31
            "ratios": [{"name_tag": "price_earnings", "value": 20.0}],
        },
        fetched_at=t1,
    )

    # Newer snapshot with identical PK (product_id, source, metric_id, effective_date): P/E = 25.0
    store_snapshot(
        conn,
        9999,
        "/tws.proxy/fundamentals/mf_ratios_fundamentals/",
        "slug",
        {
            "as_of_date": 1785470400000,  # 2026-07-31
            "ratios": [{"name_tag": "price_earnings", "value": 25.0}],
        },
        fetched_at=t2,
    )
    conn.close()

    # Processing should complete without duplicate key Constraint Error
    processed = run_observations(force=False, db_path=db_file)
    assert processed == 2

    conn = duckdb.connect(db_file)
    rows = conn.execute(
        "SELECT value, raw_value, fetched_at FROM silver.product_metrics WHERE metric_id = 'price_earnings'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 25.0
    assert rows[0][1] == "25.0"
    conn.close()
