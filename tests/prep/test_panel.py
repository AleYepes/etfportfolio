from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from etfportfolio.core.db import apply_schema
from etfportfolio.prep.panel import month_end_spine, run_panel


@pytest.fixture
def panel_db(tmp_path: Path):
    db_file = str(tmp_path / "panel_test.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)
    conn.close()
    return db_file


def test_month_end_spine_inclusive_and_leap():
    spine = month_end_spine(date(2015, 1, 15), date(2015, 3, 2))
    assert spine == [date(2015, 1, 31), date(2015, 2, 28), date(2015, 3, 31)]
    leap = month_end_spine(date(2016, 2, 1), date(2016, 2, 29))
    assert leap == [date(2016, 2, 29)]
    assert month_end_spine(date(2026, 8, 31), date(2026, 8, 1)) == []


def test_panel_four_column_schema_and_feature_id_shape(panel_db):
    conn = duckdb.connect(panel_db)
    # Add prices for product 1
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2024-01-15 00:00:00', 100.0, now()),
               (1, '2024-03-15 00:00:00', 105.0, now())
        """
    )
    # Add scalar observation
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES (1, 'ratios', 'price_earnings', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 25.0, '25.0')
        """
    )
    conn.close()

    rows_written = run_panel(panel_db)
    assert rows_written > 0

    conn = duckdb.connect(panel_db)
    cols = [
        r[0]
        for r in conn.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'silver' AND table_name = 'monthly_panel'
            """
        ).fetchall()
    ]
    assert "asset_class" not in cols
    assert set(cols) == {"product_id", "as_of_date", "feature_id", "value"}

    features = [r[0] for r in conn.execute("SELECT DISTINCT feature_id FROM silver.monthly_panel").fetchall()]
    assert features == ["ratios_price_earnings"]
    conn.close()


def test_singleton_spans_full_per_product_price_range_including_before_effective_date(panel_db):
    conn = duckdb.connect(panel_db)
    # Prices span 2020 through 2024
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2020-01-15', 50.0, now()),
               (1, '2024-04-10', 100.0, now())
        """
    )
    # Single observation at 2022-06-30
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES (1, 'ratios', 'price_earnings', NULL, '2022-06-30', 1, '2022-06-30 10:00:00+00', 18.5, '18.5')
        """
    )
    conn.close()

    run_panel(panel_db)

    conn = duckdb.connect(panel_db)
    panel_dates = [
        r[0]
        for r in conn.execute(
            "SELECT as_of_date FROM silver.monthly_panel WHERE feature_id = 'ratios_price_earnings' ORDER BY as_of_date"
        ).fetchall()
    ]
    expected_months = month_end_spine(date(2020, 1, 15), date(2024, 4, 10))
    assert panel_dates == expected_months

    # Value is 18.5 across all dates
    values = [r[0] for r in conn.execute("SELECT DISTINCT value FROM silver.monthly_panel").fetchall()]
    assert values == [18.5]
    conn.close()


def test_multi_obs_no_pre_first_fill_and_p99_cap_drops_stale_tail(panel_db):
    conn = duckdb.connect(panel_db)
    # Prices from 2023-12-01 to 2024-06-30
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2023-12-01', 50.0, now()),
               (1, '2024-06-30', 60.0, now())
        """
    )
    # Two ratio points: 2024-01-31 and 2024-03-31 (gap = 60 days, p99 = 60)
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'ratios', 'price_earnings', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 15.0, '15.0'),
            (1, 'ratios', 'price_earnings', NULL, '2024-03-31', 1, '2024-03-31 10:00:00+00', 20.0, '20.0')
        """
    )
    conn.close()

    run_panel(panel_db)

    conn = duckdb.connect(panel_db)
    rows = conn.execute(
        "SELECT as_of_date, value FROM silver.monthly_panel WHERE feature_id = 'ratios_price_earnings' ORDER BY as_of_date"
    ).fetchall()
    by_date = {r[0]: r[1] for r in rows}

    # Month 2023-12-31 is before first obs -> omitted!
    assert date(2023, 12, 31) not in by_date

    # Month 2024-01-31: 15.0
    assert by_date[date(2024, 1, 31)] == 15.0

    # Month 2024-02-29: 15.0 (carried forward within cap)
    assert by_date[date(2024, 2, 29)] == 15.0

    # Month 2024-03-31: 20.0
    assert by_date[date(2024, 3, 31)] == 20.0

    # Month 2024-04-30: 20.0 (30 days <= 60 cap)
    assert by_date[date(2024, 4, 30)] == 20.0

    # Month 2024-05-31: 61 days > 60 cap -> dropped!
    assert date(2024, 5, 31) not in by_date
    assert date(2024, 6, 30) not in by_date
    conn.close()


def test_omitted_cash_becomes_zero_not_leaked(panel_db):
    conn = duckdb.connect(panel_db)
    # Prices cover Jan and Feb 2024
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2024-01-01', 10.0, now()),
               (1, '2024-02-29', 10.0, now())
        """
    )
    # Jan snapshot: equity 0.8, cash 0.2
    # Feb snapshot: equity 1.0 (cash omitted)
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'asset_class', 'equity', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 0.8, '80%'),
            (1, 'asset_class', 'cash', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 0.2, '20%'),
            (1, 'asset_class', 'equity', NULL, '2024-02-29', 1, '2024-02-29 10:00:00+00', 1.0, '100%')
        """
    )
    conn.close()

    run_panel(panel_db)

    conn = duckdb.connect(panel_db)
    feb_rows = conn.execute(
        """
        SELECT feature_id, value FROM silver.monthly_panel
        WHERE as_of_date = '2024-02-29' AND feature_id LIKE 'asset_class_%'
        """
    ).fetchall()
    feb_map = dict(feb_rows)

    assert feb_map["asset_class_equity"] == 1.0
    assert feb_map["asset_class_cash"] == 0.0  # Must be 0.0, NOT 0.2 leaked from Jan!
    assert feb_map["asset_class_fixed_income"] == 0.0  # Canonical zero
    assert "asset_class_other" not in feb_map
    conn.close()


def test_stale_sleeve_emits_no_rows_after_cap(panel_db):
    conn = duckdb.connect(panel_db)
    # Prices span Dec 2023 through June 2024
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2023-12-01', 10.0, now()),
               (1, '2024-06-30', 10.0, now())
        """
    )
    # Snapshots: 2023-12-31 and 2024-01-31 (gap = 31 days -> cap = 31)
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'asset_class', 'equity', NULL, '2023-12-31', 1, '2023-12-31 10:00:00+00', 1.0, '100%'),
            (1, 'asset_class', 'equity', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 1.0, '100%')
        """
    )
    conn.close()

    run_panel(panel_db)

    conn = duckdb.connect(panel_db)
    dates_with_ac = [
        r[0]
        for r in conn.execute(
            """
            SELECT DISTINCT as_of_date FROM silver.monthly_panel
            WHERE feature_id LIKE 'asset_class_%'
            ORDER BY as_of_date
            """
        ).fetchall()
    ]
    # Jan 31 is live (gap 0 from Jan snapshot)
    # Feb 29 is live (29 days <= 31 cap)
    # March 31 is 60 days after Jan 31 -> beyond 31 cap -> NO ROWS (not a 0/0/0 sleeve)
    assert date(2023, 12, 31) in dates_with_ac
    assert date(2024, 1, 31) in dates_with_ac
    assert date(2024, 2, 29) in dates_with_ac
    assert date(2024, 3, 31) not in dates_with_ac
    assert date(2024, 4, 30) not in dates_with_ac
    conn.close()


def test_style_box_12_cells_and_hist_merge(panel_db):
    conn = duckdb.connect(panel_db)
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2024-01-01', 10.0, now()),
               (1, '2024-02-29', 10.0, now())
        """
    )
    # Date 1 (2024-01-31): selected has mid_core, hist has large_value. Selected beats hist!
    # Date 2 (2024-02-29): only hist has large_value.
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'style_box', 'mid_core', NULL, '2024-01-31', 0, '2024-01-31 10:00:00+00', 1.0, '[1, 2]'),
            (1, 'style_box_hist', 'large_value', NULL, '2024-01-31', 0, '2024-01-31 10:00:00+00', 1.0, '[0, 0]'),
            (1, 'style_box_hist', 'large_value', NULL, '2024-02-29', 0, '2024-02-29 10:00:00+00', 1.0, '[0, 0]')
        """
    )
    conn.close()

    run_panel(panel_db)

    conn = duckdb.connect(panel_db)
    # January month: mid_core is 1.0, all other 11 cells are 0.0 (including large_value)
    jan_cells = dict(
        conn.execute(
            """
            SELECT feature_id, value FROM silver.monthly_panel
            WHERE as_of_date = '2024-01-31' AND feature_id LIKE 'style_box_%'
            """
        ).fetchall()
    )
    assert len(jan_cells) == 12
    assert jan_cells["style_box_mid_core"] == 1.0
    assert jan_cells["style_box_large_value"] == 0.0
    assert sum(jan_cells.values()) == 1.0

    # February month: hist used -> large_value is 1.0, all other 11 cells are 0.0
    feb_cells = dict(
        conn.execute(
            """
            SELECT feature_id, value FROM silver.monthly_panel
            WHERE as_of_date = '2024-02-29' AND feature_id LIKE 'style_box_%'
            """
        ).fetchall()
    )
    assert len(feb_cells) == 12
    assert feb_cells["style_box_large_value"] == 1.0
    assert feb_cells["style_box_mid_core"] == 0.0
    assert sum(feb_cells.values()) == 1.0
    conn.close()


def test_aum_monthly_usd_conversion_and_interpolation(panel_db):
    conn = duckdb.connect(panel_db)
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2024-07-01', 10.0, now()),
               (1, '2024-08-31', 10.0, now())
        """
    )
    # Bronze FX for CAD to USD in July
    conn.execute(
        """
        INSERT INTO bronze.fx (source_currency, target_currency, date, close, updated_at)
        VALUES ('CAD', 'USD', '2024-07-15 00:00:00', 0.75, now()),
               ('CAD', 'USD', '2024-07-31 00:00:00', 0.75, now())
        """
    )
    # July AUM: 15 Jul CAD 10_000_000 and 31 Jul USD 8_000_000
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'profile', 'total_net_assets_local', 'CAD', '2024-07-15', 3, '2024-07-15 10:00:00+00', 10000000.0, 'CAD10M'),
            (1, 'profile', 'total_net_assets_local', 'USD', '2024-07-31', 3, '2024-07-31 10:00:00+00', 8000000.0, '$8M')
        """
    )
    conn.close()

    run_panel(panel_db)

    conn = duckdb.connect(panel_db)
    rows = conn.execute(
        """
        SELECT as_of_date, feature_id, value FROM silver.monthly_panel
        WHERE feature_id LIKE '%total_net_assets%'
        """
    ).fetchall()
    by_date = {r[0]: (r[1], r[2]) for r in rows}

    # No local AUM feature exists in panel
    assert not any("total_net_assets_local" in r[1] for r in rows)

    # July USD average = (10e6*0.75 + 8e6*1.0)/2 = 7.75e6
    assert by_date[date(2024, 7, 31)][0] == "profile_total_net_assets_usd"
    assert pytest.approx(by_date[date(2024, 7, 31)][1]) == 7_750_000.0

    # Singleton AUM series carries forward to August as USD 7.75e6
    assert by_date[date(2024, 8, 31)][0] == "profile_total_net_assets_usd"
    assert pytest.approx(by_date[date(2024, 8, 31)][1]) == 7_750_000.0
    conn.close()


def test_no_prices_no_panel_rows(panel_db):
    conn = duckdb.connect(panel_db)
    # Observations exist, but NO prices exist in bronze.prices
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES (999, 'ratios', 'price_earnings', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 20.0, '20.0')
        """
    )
    conn.close()

    rows = run_panel(panel_db)
    assert rows == 0


def test_open_vocab_densify_per_product_only(panel_db):
    conn = duckdb.connect(panel_db)
    # Product 1 and Product 2 both have prices
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2024-01-01', 10.0, now()), (1, '2024-02-29', 10.0, now()),
               (2, '2024-01-01', 20.0, now()), (2, '2024-02-29', 20.0, now())
        """
    )
    # Product 1 has themes A and B
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'theme', 'theme_a', 'code_a', '2024-01-31', 0, '2024-01-31 10:00:00+00', 0.5, '0.5'),
            (1, 'theme', 'theme_b', 'code_b', '2024-01-31', 0, '2024-01-31 10:00:00+00', 0.5, '0.5'),
            (1, 'theme', 'theme_a', 'code_a', '2024-02-29', 0, '2024-02-29 10:00:00+00', 1.0, '1.0')
        """
    )
    # Product 2 has theme C only
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (2, 'theme', 'theme_c', 'code_c', '2024-01-31', 0, '2024-01-31 10:00:00+00', 1.0, '1.0')
        """
    )
    conn.close()

    run_panel(panel_db)

    conn = duckdb.connect(panel_db)
    # Product 1 in Feb: theme_a is 1.0, theme_b is 0.0 (densified against product 1's universe)
    p1_feb = dict(
        conn.execute(
            """
            SELECT feature_id, value FROM silver.monthly_panel
            WHERE product_id = 1 AND as_of_date = '2024-02-29'
            """
        ).fetchall()
    )
    assert p1_feb["theme_theme_a"] == 1.0
    assert p1_feb["theme_theme_b"] == 0.0
    assert "theme_theme_c" not in p1_feb  # Not in product 1's universe!

    # Product 2: has theme_c only, not theme_a or theme_b
    p2_features = [
        r[0]
        for r in conn.execute("SELECT DISTINCT feature_id FROM silver.monthly_panel WHERE product_id = 2").fetchall()
    ]
    assert p2_features == ["theme_theme_c"]
    conn.close()
