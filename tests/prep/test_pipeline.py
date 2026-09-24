from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from etfportfolio.core.db import apply_schema
from etfportfolio.ingest.snapshots import store_snapshot
from etfportfolio.prep.pipeline import run_observations


@pytest.fixture
def obs_test_db(tmp_path: Path):
    db_file = str(tmp_path / "obs_test.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, currency, created_at, updated_at)
        VALUES (8335, 'ICF', 'USD', now(), now())
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
                {"name": "Total Net Assets (Month End)", "value": "$78.63B (2026/07/31)"},
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
            "content": [{"name": "TRESGS", "value": 70}],
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
            "coverage": 0.91,
            "themes": [
                {
                    "key": "006a0c27-4a9a-4766-8988-0d8acc6ede8b",
                    "name": "Discount Retail",
                    "weight": 0.084,
                    "rank_adjusted_weight": 0.009,
                }
            ],
        },
        fetched_at=t,
    )

    # 6. empty stub (valid no-op, watermarked)
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

    # Verify silver.observations
    obs_rows = conn.execute(
        """
        SELECT family, metric, code, effective_date, date_source_depth, value, raw_value
        FROM silver.observations
        ORDER BY family, metric
        """
    ).fetchall()
    by_fm = {(r[0], r[1]): r for r in obs_rows}

    # Ratios
    assert ("ratios", "price_earnings") in by_fm
    r_pe = by_fm[("ratios", "price_earnings")]
    assert r_pe[3] == date(2026, 7, 31)
    assert r_pe[4] == 1  # Depth
    assert r_pe[5] == 25.5

    # Profile
    assert ("profile", "is_passive") in by_fm
    assert by_fm[("profile", "is_passive")][5] == 1.0

    assert ("profile", "total_expense_ratio") in by_fm
    assert pytest.approx(by_fm[("profile", "total_expense_ratio")][5]) == 0.0032

    assert ("profile", "total_net_assets_local") in by_fm
    aum = by_fm[("profile", "total_net_assets_local")]
    assert aum[2] == "USD"
    assert aum[3] == date(2026, 7, 31)
    assert aum[4] == 3  # Depth 3 leaf

    # ESG
    assert ("profile", "esg_coverage") in by_fm
    assert pytest.approx(by_fm[("profile", "esg_coverage")][5]) == 0.99

    assert ("esg", "tresgs") in by_fm
    assert by_fm[("esg", "tresgs")][5] == 70.0

    # Asset class
    assert ("asset_class", "equity") in by_fm
    assert pytest.approx(by_fm[("asset_class", "equity")][5]) == 1.0

    # Themes
    assert ("profile", "theme_coverage") in by_fm
    assert ("theme", "discount_retail") in by_fm
    assert ("rank_adj_theme", "discount_retail") in by_fm
    assert by_fm[("theme", "discount_retail")][2] == "006a0c27-4a9a-4766-8988-0d8acc6ede8b"

    conn.close()

    # 2. Second run: all up to date -> 0 processed
    assert run_observations(force=False, db_path=obs_test_db) == 0

    # 3. Force rebuild: deletes and re-extracts
    assert run_observations(force=True, db_path=obs_test_db) == 6
    conn = duckdb.connect(obs_test_db)
    proc_cnt = conn.execute("SELECT COUNT(*) FROM silver.processed_snapshots").fetchone()[0]
    assert proc_cnt == 6
    obs_cnt = conn.execute("SELECT COUNT(*) FROM silver.observations").fetchone()[0]
    assert obs_cnt == len(obs_rows)
    conn.close()


def test_pipeline_isolation(tmp_path: Path):
    """Test that a bad snapshot does not roll back siblings or watermark itself."""
    db_file = str(tmp_path / "isolation.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, currency, created_at, updated_at)
        VALUES (1, 'AAA', 'USD', now(), now()), (2, 'BBB', 'USD', now(), now())
        """
    )
    t = datetime(2026, 9, 1, 12, 0, 0, tzinfo=UTC)

    # Good snapshot 1
    store_snapshot(
        conn,
        1,
        "/tws.proxy/fundamentals/mf_ratios_fundamentals/",
        "slug",
        {"as_of_date": "2026-07-31", "ratios": [{"name_tag": "price_earnings", "value": 20.0}]},
        fetched_at=t,
    )

    # Bad snapshot 2 (corrupt payload blob)
    store_snapshot(
        conn,
        1,
        "/tws.proxy/fundamentals/mf_holdings/",
        "slug",
        {"raw": "corrupt"},
        fetched_at=t,
    )
    # Corrupt its blob in bronze.payload_blobs
    bad_hash = conn.execute(
        "SELECT hash FROM bronze.snapshots WHERE url_prefix = '/tws.proxy/fundamentals/mf_holdings/'"
    ).fetchone()[0]
    conn.execute("UPDATE bronze.payload_blobs SET payload = 'NOT_ZSTD' WHERE hash = ?", [bad_hash])

    # Bad snapshot 3 (causes extract failure: unknown closed-vocab tag in ratios)
    store_snapshot(
        conn,
        2,
        "/tws.proxy/fundamentals/mf_ratios_fundamentals/",
        "slug",
        {"as_of_date": "2026-07-31", "ratios": [{"name_tag": "totally_invalid_ratio_key", "value": 1.0}]},
        fetched_at=t,
    )

    # Good snapshot 4
    store_snapshot(
        conn,
        2,
        "/tws.proxy/impact/esg/",
        "slug",
        {"asOfDate": "2026-07-31", "content": [{"name": "TRESGS", "value": 85}]},
        fetched_at=t,
    )

    conn.close()

    # 4 snapshots total: 2 good, 2 bad.
    watermarked = run_observations(force=False, db_path=db_file)
    assert watermarked == 2

    conn = duckdb.connect(db_file)
    watermarked_ids = [r[0] for r in conn.execute("SELECT snapshot_id FROM silver.processed_snapshots").fetchall()]
    assert len(watermarked_ids) == 2

    # Check observations table contains rows from good snapshots 1 and 4
    obs = conn.execute(
        "SELECT product_id, family, metric, value FROM silver.observations ORDER BY product_id"
    ).fetchall()
    assert len(obs) == 2
    assert obs[0] == (1, "ratios", "price_earnings", 20.0)
    assert obs[1] == (2, "esg", "tresgs", 85.0)
    conn.close()


def test_pipeline_collision_depth_wins(tmp_path: Path):
    db_file = str(tmp_path / "depth_test.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, currency, created_at, updated_at)
        VALUES (10, 'CCC', 'USD', now(), now())
        """
    )
    t = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)

    # Snapshot with lower depth (e.g. depth 0 or depth 1)
    store_snapshot(
        conn,
        10,
        "/tws.proxy/fundamentals/mf_profile_and_fees/",
        "slug1",
        {
            "fund_and_profile": [
                {"name": "Total Net Assets (Month End)", "value": "$50M"}  # No embedded date -> depth 0
            ]
        },
        fetched_at=t,
    )

    # Snapshot with higher depth for same effective_date (depth 3 leaf)
    store_snapshot(
        conn,
        10,
        "/tws.proxy/fundamentals/mf_profile_and_fees/",
        "slug2",
        {
            "fund_and_profile": [
                {"name": "Total Net Assets (Month End)", "value": "$80M (2026/09/01)"}  # embedded date matches t.date()
            ]
        },
        fetched_at=t,
    )
    conn.close()

    run_observations(force=False, db_path=db_file)

    conn = duckdb.connect(db_file)
    row = conn.execute(
        "SELECT value, date_source_depth FROM silver.observations WHERE metric = 'total_net_assets_local'"
    ).fetchone()
    assert row is not None
    assert row[0] == 80000000.0
    assert row[1] == 3
    conn.close()


def test_pipeline_collision_later_fetched_at_wins(tmp_path: Path):
    db_file = str(tmp_path / "fetched_test.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, currency, created_at, updated_at)
        VALUES (20, 'DDD', 'USD', now(), now())
        """
    )
    t1 = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 9, 1, 14, 0, 0, tzinfo=UTC)

    # Earlier snapshot
    store_snapshot(
        conn,
        20,
        "/tws.proxy/fundamentals/mf_ratios_fundamentals/",
        "slug1",
        {"as_of_date": "2026-07-31", "ratios": [{"name_tag": "price_earnings", "value": 20.0}]},
        fetched_at=t1,
    )

    # Later snapshot (same depth 1, same PK)
    store_snapshot(
        conn,
        20,
        "/tws.proxy/fundamentals/mf_ratios_fundamentals/",
        "slug2",
        {"as_of_date": "2026-07-31", "ratios": [{"name_tag": "price_earnings", "value": 25.0}]},
        fetched_at=t2,
    )
    conn.close()

    run_observations(force=False, db_path=db_file)

    conn = duckdb.connect(db_file)
    row = conn.execute("SELECT value, fetched_at FROM silver.observations WHERE metric = 'price_earnings'").fetchone()
    assert row is not None
    assert row[0] == 25.0
    conn.close()


def test_pipeline_unregistered_extractor_raises(tmp_path: Path):
    db_file = str(tmp_path / "unknown_extractor.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    t = datetime(2026, 9, 1, 12, 0, 0, tzinfo=UTC)
    store_snapshot(
        conn,
        1234,
        "/unregistered/unknown/prefix/",
        "slug",
        {"data": 123},
        fetched_at=t,
    )
    conn.close()

    with pytest.raises(ValueError, match="Unregistered extractor for url_prefix: /unregistered/unknown/prefix/"):
        run_observations(force=False, db_path=db_file)
