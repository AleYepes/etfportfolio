from datetime import datetime
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

    t = datetime(2026, 9, 1, 12, 0, 0)

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

    # Verify silver.processed_snapshots has 6 rows
    watermark_row = conn.execute("SELECT COUNT(*) FROM silver.processed_snapshots").fetchone()
    assert watermark_row is not None
    assert watermark_row[0] == 6

    # Verify metrics observations
    metrics = conn.execute(
        "SELECT source, metric_id, value FROM silver.metric_observations ORDER BY source, metric_id"
    ).fetchall()
    assert len(metrics) == 5
    metrics_dict = {(r[0], r[1]): r[2] for r in metrics}
    assert metrics_dict[("ratios", "price_earnings")] == 25.5
    assert pytest.approx(metrics_dict[("profile", "total_expense_ratio")]) == 0.0032
    assert metrics_dict[("profile", "is_passive")] == 1.0
    assert metrics_dict[("esg", "esg_coverage")] == 0.99
    assert metrics_dict[("esg", "tresgs")] == 7.0

    # Verify portfolio allocations
    allocations = conn.execute("SELECT breakdown_type, item_name, weight FROM silver.portfolio_allocations").fetchall()
    assert len(allocations) == 1
    assert allocations[0][0] == "asset_class"
    assert allocations[0][1] == "Equity"
    assert pytest.approx(allocations[0][2]) == 0.998

    # Verify theme exposures
    themes = conn.execute("SELECT theme_id, weight, rank_adjusted_weight FROM silver.theme_exposures").fetchall()
    assert len(themes) == 1
    assert themes[0][0] == "006a0c27-4a9a-4766-8988-0d8acc6ede8b"
    assert themes[0][1] == 0.084
    assert themes[0][2] == 0.009

    conn.close()

    # 2. Immediate second run: all up to date, 0 processed
    processed_again = run_observations(force=False, db_path=obs_test_db)
    assert processed_again == 0

    # 3. Run with force=True: repopulates all 6 snapshots
    processed_force = run_observations(force=True, db_path=obs_test_db)
    assert processed_force == 6

    conn = duckdb.connect(obs_test_db)
    proc_row = conn.execute("SELECT COUNT(*) FROM silver.processed_snapshots").fetchone()
    assert proc_row is not None and proc_row[0] == 6
    met_row = conn.execute("SELECT COUNT(*) FROM silver.metric_observations").fetchone()
    assert met_row is not None and met_row[0] == 5
    alloc_row = conn.execute("SELECT COUNT(*) FROM silver.portfolio_allocations").fetchone()
    assert alloc_row is not None and alloc_row[0] == 1
    theme_row = conn.execute("SELECT COUNT(*) FROM silver.theme_exposures").fetchone()
    assert theme_row is not None and theme_row[0] == 1
    conn.close()
