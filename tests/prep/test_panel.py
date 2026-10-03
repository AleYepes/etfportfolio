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
               (1, '2024-05-15 00:00:00', 105.0, now())
        """
    )
    # Add 5 scalar observations across dates to satisfy global low-count pruning threshold (>= 5)
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'ratios', 'price_earnings', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 25.0, '25.0'),
            (1, 'ratios', 'price_earnings', NULL, '2024-02-29', 1, '2024-02-29 10:00:00+00', 25.0, '25.0'),
            (1, 'ratios', 'price_earnings', NULL, '2024-03-31', 1, '2024-03-31 10:00:00+00', 25.0, '25.0'),
            (1, 'ratios', 'price_earnings', NULL, '2024-04-30', 1, '2024-04-30 10:00:00+00', 25.0, '25.0'),
            (1, 'ratios', 'price_earnings', NULL, '2024-05-31', 1, '2024-05-31 10:00:00+00', 25.0, '25.0')
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
    # Single observation for product 1 at 2022-06-30.
    # Add dummy observations for other products (without prices) so global count is >= 5.
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'ratios', 'price_earnings', NULL, '2022-06-30', 1, '2022-06-30 10:00:00+00', 18.5, '18.5'),
            (2, 'ratios', 'price_earnings', NULL, '2022-06-30', 1, '2022-06-30 10:00:00+00', 18.5, '18.5'),
            (3, 'ratios', 'price_earnings', NULL, '2022-06-30', 1, '2022-06-30 10:00:00+00', 18.5, '18.5'),
            (4, 'ratios', 'price_earnings', NULL, '2022-06-30', 1, '2022-06-30 10:00:00+00', 18.5, '18.5'),
            (5, 'ratios', 'price_earnings', NULL, '2022-06-30', 1, '2022-06-30 10:00:00+00', 18.5, '18.5')
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
    # Two ratio points for product 1: 2024-01-31 and 2024-03-31 (gap = 60 days, p99 = 60)
    # Plus 3 observations on other products so metric count is >= 5.
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'ratios', 'price_earnings', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 15.0, '15.0'),
            (1, 'ratios', 'price_earnings', NULL, '2024-03-31', 1, '2024-03-31 10:00:00+00', 20.0, '20.0'),
            (2, 'ratios', 'price_earnings', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 10.0, '10.0'),
            (2, 'ratios', 'price_earnings', NULL, '2024-02-29', 1, '2024-02-29 10:00:00+00', 10.0, '10.0'),
            (2, 'ratios', 'price_earnings', NULL, '2024-03-31', 1, '2024-03-31 10:00:00+00', 10.0, '10.0')
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
    # Dummy observations for product 2 to bring global count >= 5 for both equity and cash
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'asset_class', 'equity', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 0.8, '80%'),
            (1, 'asset_class', 'cash', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 0.2, '20%'),
            (1, 'asset_class', 'equity', NULL, '2024-02-29', 1, '2024-02-29 10:00:00+00', 1.0, '100%'),
            (2, 'asset_class', 'equity', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 1.0, '100%'),
            (2, 'asset_class', 'equity', NULL, '2024-02-29', 1, '2024-02-29 10:00:00+00', 1.0, '100%'),
            (2, 'asset_class', 'equity', NULL, '2024-03-31', 1, '2024-03-31 10:00:00+00', 1.0, '100%'),
            (2, 'asset_class', 'cash', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 0.1, '10%'),
            (2, 'asset_class', 'cash', NULL, '2024-02-29', 1, '2024-02-29 10:00:00+00', 0.1, '10%'),
            (2, 'asset_class', 'cash', NULL, '2024-03-31', 1, '2024-03-31 10:00:00+00', 0.1, '10%'),
            (2, 'asset_class', 'cash', NULL, '2024-04-30', 1, '2024-04-30 10:00:00+00', 0.1, '10%')
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
    # Snapshots for product 1: 2023-12-31 and 2024-01-31 (gap = 31 days -> cap = 31)
    # Plus dummy obs on product 2 to satisfy count >= 5
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'asset_class', 'equity', NULL, '2023-12-31', 1, '2023-12-31 10:00:00+00', 1.0, '100%'),
            (1, 'asset_class', 'equity', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 1.0, '100%'),
            (2, 'asset_class', 'equity', NULL, '2023-12-31', 1, '2023-12-31 10:00:00+00', 1.0, '100%'),
            (2, 'asset_class', 'equity', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 1.0, '100%'),
            (2, 'asset_class', 'equity', NULL, '2024-02-29', 1, '2024-02-29 10:00:00+00', 1.0, '100%')
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
    # Provide dummy observations to reach >= 5 global observations for mid_core and large_value
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'style_box', 'mid_core', NULL, '2024-01-31', 0, '2024-01-31 10:00:00+00', 1.0, '[1, 2]'),
            (1, 'style_box_hist', 'large_value', NULL, '2024-01-31', 0, '2024-01-31 10:00:00+00', 1.0, '[0, 0]'),
            (1, 'style_box_hist', 'large_value', NULL, '2024-02-29', 0, '2024-02-29 10:00:00+00', 1.0, '[0, 0]'),
            (2, 'style_box', 'mid_core', NULL, '2024-01-31', 0, '2024-01-31 10:00:00+00', 1.0, '[1, 2]'),
            (2, 'style_box', 'mid_core', NULL, '2024-02-29', 0, '2024-02-29 10:00:00+00', 1.0, '[1, 2]'),
            (2, 'style_box', 'mid_core', NULL, '2024-03-31', 0, '2024-03-31 10:00:00+00', 1.0, '[1, 2]'),
            (2, 'style_box', 'mid_core', NULL, '2024-04-30', 0, '2024-04-30 10:00:00+00', 1.0, '[1, 2]'),
            (2, 'style_box_hist', 'large_value', NULL, '2024-01-31', 0, '2024-01-31 10:00:00+00', 1.0, '[0, 0]'),
            (2, 'style_box_hist', 'large_value', NULL, '2024-02-29', 0, '2024-02-29 10:00:00+00', 1.0, '[0, 0]'),
            (2, 'style_box_hist', 'large_value', NULL, '2024-03-31', 0, '2024-03-31 10:00:00+00', 1.0, '[0, 0]')
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
    # Plus dummy observations on other products to reach >= 5 observations globally
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'profile', 'total_net_assets_local', 'CAD', '2024-07-15', 3, '2024-07-15 10:00:00+00', 10000000.0, 'CAD10M'),
            (1, 'profile', 'total_net_assets_local', 'USD', '2024-07-31', 3, '2024-07-31 10:00:00+00', 8000000.0, '$8M'),
            (2, 'profile', 'total_net_assets_local', 'USD', '2024-07-31', 3, '2024-07-31 10:00:00+00', 5000000.0, '$5M'),
            (2, 'profile', 'total_net_assets_local', 'USD', '2024-08-31', 3, '2024-08-31 10:00:00+00', 5000000.0, '$5M'),
            (2, 'profile', 'total_net_assets_local', 'USD', '2024-09-30', 3, '2024-09-30 10:00:00+00', 5000000.0, '$5M')
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
    # Observations exist (>= 5), but NO prices exist in bronze.prices
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (999, 'ratios', 'price_earnings', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 20.0, '20.0'),
            (999, 'ratios', 'price_earnings', NULL, '2024-02-29', 1, '2024-02-29 10:00:00+00', 20.0, '20.0'),
            (999, 'ratios', 'price_earnings', NULL, '2024-03-31', 1, '2024-03-31 10:00:00+00', 20.0, '20.0'),
            (999, 'ratios', 'price_earnings', NULL, '2024-04-30', 1, '2024-04-30 10:00:00+00', 20.0, '20.0'),
            (999, 'ratios', 'price_earnings', NULL, '2024-05-31', 1, '2024-05-31 10:00:00+00', 20.0, '20.0')
        """
    )
    conn.close()

    rows = run_panel(panel_db)
    assert rows == 0


def test_open_vocab_densify_per_product_only(panel_db):
    """Updated according to FRD 6.1: uses country as open per-product universe."""
    conn = duckdb.connect(panel_db)
    # Product 1 and Product 2 both have prices
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2024-01-01', 10.0, now()), (1, '2024-02-29', 10.0, now()),
               (2, '2024-01-01', 20.0, now()), (2, '2024-02-29', 20.0, now())
        """
    )
    # Product 1 has country US and CA. Product 2 has country DE.
    # Provide >= 5 observations per country metric so they survive pruning.
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'country', 'us', 'US', '2024-01-31', 0, '2024-01-31 10:00:00+00', 0.5, '0.5'),
            (1, 'country', 'ca', 'CA', '2024-01-31', 0, '2024-01-31 10:00:00+00', 0.5, '0.5'),
            (1, 'country', 'us', 'US', '2024-02-29', 0, '2024-02-29 10:00:00+00', 1.0, '1.0'),
            (3, 'country', 'us', 'US', '2024-01-31', 0, '2024-01-31 10:00:00+00', 1.0, '1.0'),
            (3, 'country', 'us', 'US', '2024-02-29', 0, '2024-02-29 10:00:00+00', 1.0, '1.0'),
            (3, 'country', 'us', 'US', '2024-03-31', 0, '2024-03-31 10:00:00+00', 1.0, '1.0'),
            (3, 'country', 'ca', 'CA', '2024-01-31', 0, '2024-01-31 10:00:00+00', 1.0, '1.0'),
            (3, 'country', 'ca', 'CA', '2024-02-29', 0, '2024-02-29 10:00:00+00', 1.0, '1.0'),
            (3, 'country', 'ca', 'CA', '2024-03-31', 0, '2024-03-31 10:00:00+00', 1.0, '1.0'),
            (3, 'country', 'ca', 'CA', '2024-04-30', 0, '2024-04-30 10:00:00+00', 1.0, '1.0'),
            (2, 'country', 'de', 'DE', '2024-01-31', 0, '2024-01-31 10:00:00+00', 1.0, '1.0'),
            (3, 'country', 'de', 'DE', '2024-01-31', 0, '2024-01-31 10:00:00+00', 1.0, '1.0'),
            (3, 'country', 'de', 'DE', '2024-02-29', 0, '2024-02-29 10:00:00+00', 1.0, '1.0'),
            (3, 'country', 'de', 'DE', '2024-03-31', 0, '2024-03-31 10:00:00+00', 1.0, '1.0'),
            (3, 'country', 'de', 'DE', '2024-04-30', 0, '2024-04-30 10:00:00+00', 1.0, '1.0')
        """
    )
    conn.close()

    run_panel(panel_db)

    conn = duckdb.connect(panel_db)
    # Product 1 in Feb: country_us is 1.0, country_ca is 0.0 (densified against product 1's universe)
    p1_feb = dict(
        conn.execute(
            """
            SELECT feature_id, value FROM silver.monthly_panel
            WHERE product_id = 1 AND as_of_date = '2024-02-29'
            """
        ).fetchall()
    )
    assert p1_feb["country_us"] == 1.0
    assert p1_feb["country_ca"] == 0.0
    assert "country_de" not in p1_feb  # Not in product 1's universe!

    # Product 2: has country_de only, not country_us or country_ca
    p2_features = [
        r[0]
        for r in conn.execute("SELECT DISTINCT feature_id FROM silver.monthly_panel WHERE product_id = 2").fetchall()
    ]
    assert p2_features == ["country_de"]
    conn.close()


def test_panel_interpolates_new_profile_scalars(panel_db):
    conn = duckdb.connect(panel_db)
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2024-01-15', 100.0, now()),
               (1, '2024-03-31', 110.0, now())
        """
    )
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'profile', 'audited_gross_expense_ratio', NULL, '2024-01-31', 2, '2024-01-31 10:00:00+00', 0.0018, '0.18%'),
            (2, 'profile', 'audited_gross_expense_ratio', NULL, '2024-01-31', 2, '2024-01-31 10:00:00+00', 0.0018, '0.18%'),
            (3, 'profile', 'audited_gross_expense_ratio', NULL, '2024-01-31', 2, '2024-01-31 10:00:00+00', 0.0018, '0.18%'),
            (4, 'profile', 'audited_gross_expense_ratio', NULL, '2024-01-31', 2, '2024-01-31 10:00:00+00', 0.0018, '0.18%'),
            (5, 'profile', 'audited_gross_expense_ratio', NULL, '2024-01-31', 2, '2024-01-31 10:00:00+00', 0.0018, '0.18%'),
            (1, 'profile', 'redemption_charge_max', NULL, '2024-02-29', 0, '2024-02-29 10:00:00+00', 0.05, '5%'),
            (2, 'profile', 'redemption_charge_max', NULL, '2024-02-29', 0, '2024-02-29 10:00:00+00', 0.05, '5%'),
            (3, 'profile', 'redemption_charge_max', NULL, '2024-02-29', 0, '2024-02-29 10:00:00+00', 0.05, '5%'),
            (4, 'profile', 'redemption_charge_max', NULL, '2024-02-29', 0, '2024-02-29 10:00:00+00', 0.05, '5%'),
            (5, 'profile', 'redemption_charge_max', NULL, '2024-02-29', 0, '2024-02-29 10:00:00+00', 0.05, '5%')
        """
    )
    conn.close()

    rows_written = run_panel(panel_db)
    assert rows_written > 0

    conn = duckdb.connect(panel_db)
    features = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT feature_id FROM silver.monthly_panel WHERE product_id = 1 ORDER BY feature_id"
        ).fetchall()
    ]
    assert features == [
        "profile_audited_gross_expense_ratio",
        "profile_redemption_charge_max",
    ]

    # Verify singleton scalar fills full spine
    audited_rows = conn.execute(
        "SELECT as_of_date, value FROM silver.monthly_panel WHERE feature_id = 'profile_audited_gross_expense_ratio' ORDER BY as_of_date"
    ).fetchall()
    expected_dates = [date(2024, 1, 31), date(2024, 2, 29), date(2024, 3, 31)]
    assert [r[0] for r in audited_rows] == expected_dates
    for _, val in audited_rows:
        assert val == pytest.approx(0.0018)

    redemption_rows = conn.execute(
        "SELECT as_of_date, value FROM silver.monthly_panel WHERE feature_id = 'profile_redemption_charge_max' ORDER BY as_of_date"
    ).fetchall()
    assert [r[0] for r in redemption_rows] == expected_dates
    for _, val in redemption_rows:
        assert val == pytest.approx(0.05)
    conn.close()


# ==============================================================================
# NEW TEST SPECIFICATIONS FROM FRD 6.2
# ==============================================================================


def test_direct_parent_theme_observation_raises_error(panel_db):
    """FRD 6.2 #1: Direct parent theme observation must raise ValueError immediately."""
    conn = duckdb.connect(panel_db)
    conn.execute(
        """
        INSERT INTO bronze.themes (theme_id, name, parent_id, created_at, updated_at)
        VALUES ('P1', 'Artificial Intelligence', NULL, now(), now())
        """
    )
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2024-01-15', 100.0, now())
        """
    )
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES (1, 'theme', 'artificial_intelligence', 'P1', '2024-01-31', 0, now(), 0.5, '0.5')
        """
    )
    conn.close()

    with pytest.raises(ValueError, match="direct parent theme observation"):
        run_panel(panel_db)


def test_unmapped_child_theme_strictly_dropped(panel_db):
    """FRD 6.2 #2: Unmapped child theme produced zero rows in silver.monthly_panel."""
    conn = duckdb.connect(panel_db)
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2024-01-15', 100.0, now())
        """
    )
    # Insert 5 observations so it would otherwise pass low-count pruning
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'theme', 'unmapped_theme', 'UNKNOWN', '2024-01-31', 0, now(), 0.5, '0.5'),
            (1, 'theme', 'unmapped_theme', 'UNKNOWN', '2024-02-29', 0, now(), 0.5, '0.5'),
            (1, 'theme', 'unmapped_theme', 'UNKNOWN', '2024-03-31', 0, now(), 0.5, '0.5'),
            (1, 'theme', 'unmapped_theme', 'UNKNOWN', '2024-04-30', 0, now(), 0.5, '0.5'),
            (1, 'theme', 'unmapped_theme', 'UNKNOWN', '2024-05-31', 0, now(), 0.5, '0.5')
        """
    )
    conn.close()

    rows = run_panel(panel_db)
    assert rows == 0


def test_child_themes_aggregated_to_parent_theme(panel_db):
    """FRD 6.2 #3: Technology parent with child Hardware (0.3) + Software (0.5) aggregates to 0.8."""
    conn = duckdb.connect(panel_db)
    conn.execute(
        """
        INSERT INTO bronze.themes (theme_id, name, parent_id, created_at, updated_at)
        VALUES
            ('P1', 'Technology', NULL, now(), now()),
            ('C1', 'Hardware', 'P1', now(), now()),
            ('C2', 'Software', 'P1', now(), now())
        """
    )
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2024-01-15', 100.0, now())
        """
    )
    # Product 1 has Hardware (0.3) and Software (0.5) on 2024-01-31
    # Plus dummy obs across products to ensure parent theme reaches count >= 5
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'theme', 'hardware', 'C1', '2024-01-31', 0, now(), 0.3, '0.3'),
            (1, 'theme', 'software', 'C2', '2024-01-31', 0, now(), 0.5, '0.5'),
            (2, 'theme', 'hardware', 'C1', '2024-01-31', 0, now(), 0.4, '0.4'),
            (3, 'theme', 'hardware', 'C1', '2024-01-31', 0, now(), 0.4, '0.4'),
            (4, 'theme', 'hardware', 'C1', '2024-01-31', 0, now(), 0.4, '0.4'),
            (5, 'theme', 'hardware', 'C1', '2024-01-31', 0, now(), 0.4, '0.4')
        """
    )
    conn.close()

    run_panel(panel_db)

    conn = duckdb.connect(panel_db)
    rows = conn.execute(
        """
        SELECT feature_id, value FROM silver.monthly_panel
        WHERE product_id = 1 AND as_of_date = '2024-01-31' AND feature_id = 'theme_technology'
        """
    ).fetchall()
    assert len(rows) == 1
    assert pytest.approx(rows[0][1]) == 0.8
    conn.close()


def test_all_19_parent_themes_densified_in_canonical_universe(panel_db):
    """FRD 6.2 #4: All 19 parent themes emitted in monthly_panel (unreported themes evaluate to 0.0)."""
    parent_names = [
        "Consumer Goods and Retail",
        "Food and Beverage",
        "Financial Services and FinTech",
        "Transportation and Logistics",
        "Technology and Innovation",
        "Technology Hardware and Semiconductors",
        "Environmental and Sustainability Solutions",
        "Energy and Utilities",
        "Healthcare and Biotechnology",
        "Aerospace and Defense",
        "Automotive and Mobility",
        "Construction and Infrastructure",
        "Industrial Products and Services",
        "Entertainment and Media",
        "Real Estate and Property Management",
        "Mining and Metals",
        "Agriculture and Food Production",
        "Telecommunications and Connectivity",
        "Hospitality and Leisure",
    ]
    assert len(parent_names) == 19

    conn = duckdb.connect(panel_db)
    for i, name in enumerate(parent_names, 1):
        conn.execute(
            "INSERT INTO bronze.themes (theme_id, name, parent_id, created_at, updated_at) VALUES (?, ?, NULL, now(), now())",
            [f"P{i}", name],
        )

    # Add 1 child under P1
    conn.execute(
        """
        INSERT INTO bronze.themes (theme_id, name, parent_id, created_at, updated_at)
        VALUES ('C1', 'Specialty Retail', 'P1', now(), now())
        """
    )

    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2024-01-15', 100.0, now())
        """
    )

    # 5 observations of C1 so the rolled-up parent passes low-count pruning
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'theme', 'specialty_retail', 'C1', '2024-01-31', 0, now(), 0.75, '0.75'),
            (2, 'theme', 'specialty_retail', 'C1', '2024-01-31', 0, now(), 0.50, '0.50'),
            (3, 'theme', 'specialty_retail', 'C1', '2024-01-31', 0, now(), 0.50, '0.50'),
            (4, 'theme', 'specialty_retail', 'C1', '2024-01-31', 0, now(), 0.50, '0.50'),
            (5, 'theme', 'specialty_retail', 'C1', '2024-01-31', 0, now(), 0.50, '0.50')
        """
    )
    conn.close()

    run_panel(panel_db)

    conn = duckdb.connect(panel_db)
    theme_features = dict(
        conn.execute(
            """
            SELECT feature_id, value FROM silver.monthly_panel
            WHERE product_id = 1 AND as_of_date = '2024-01-31' AND feature_id LIKE 'theme_%'
            """
        ).fetchall()
    )
    # All 19 parent themes emitted
    assert len(theme_features) == 19
    assert theme_features["theme_consumer_goods_and_retail"] == 0.75
    # Unreported parents evaluate to 0.0
    for fid, val in theme_features.items():
        if fid != "theme_consumer_goods_and_retail":
            assert val == 0.0
    conn.close()


def test_rolling_mad_outlier_detection_and_iqr_zero_guardrail(panel_db):
    """FRD 6.2 #5: 15-point series trims 1500.0 outlier; 15-point flat series with IQR=0 retains all points."""
    conn = duckdb.connect(panel_db)
    # Prices spanning 15 months: Jan 2024 to Mar 2025
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES
            (1, '2024-01-01', 10.0, now()), (1, '2025-03-31', 10.0, now()),
            (2, '2024-01-01', 10.0, now()), (2, '2025-03-31', 10.0, now())
        """
    )

    # 1. Product 1: 15-point series with 14 points around 20.0 and 1 point at 1500.0 (at month 8)
    spine = month_end_spine(date(2024, 1, 1), date(2025, 3, 31))
    assert len(spine) == 15

    p1_rows = []
    for idx, d in enumerate(spine):
        val = 1500.0 if idx == 7 else round(20.0 + (idx * 0.1), 2)
        p1_rows.append((1, "ratios", "price_earnings", None, d, 1, f"{d} 10:00:00+00", val, str(val)))

    # 2. Product 2: 15-point flat series where all points are 0.0020
    # To qualify total_expense_ratio into candidate_metrics (> 10 distinct values across DB),
    # add Product 3 with 12 distinct values.
    p2_rows = [(2, "profile", "total_expense_ratio", None, d, 1, f"{d} 10:00:00+00", 0.0020, "0.0020") for d in spine]
    p3_rows = [
        (3, "profile", "total_expense_ratio", None, d, 1, f"{d} 10:00:00+00", round(0.0020 + (i * 0.0001), 5), "val")
        for i, d in enumerate(spine[:12])
    ]

    conn.executemany(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        p1_rows + p2_rows + p3_rows,
    )
    conn.close()

    run_panel(panel_db)

    conn = duckdb.connect(panel_db)
    # Check Product 1: 1500.0 must be trimmed as an outlier!
    p1_vals = [
        r[0]
        for r in conn.execute(
            """
            SELECT value FROM silver.monthly_panel
            WHERE product_id = 1 AND feature_id = 'ratios_price_earnings'
            """
        ).fetchall()
    ]
    assert 1500.0 not in p1_vals

    # Check Product 2: flat series must retain all 15 points (IQR == 0 guardrail)
    p2_vals = [
        r[0]
        for r in conn.execute(
            """
            SELECT value FROM silver.monthly_panel
            WHERE product_id = 2 AND feature_id = 'profile_total_expense_ratio'
            """
        ).fetchall()
    ]
    assert len(p2_vals) == 15
    assert all(v == pytest.approx(0.0020) for v in p2_vals)
    conn.close()


def test_metric_low_count_pruning(panel_db):
    """FRD 6.2 #6: Metric with only 3 observations is pruned by the global cutoff."""
    conn = duckdb.connect(panel_db)
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (1, '2024-01-01', 10.0, now()), (1, '2024-06-30', 10.0, now())
        """
    )
    # 1. Degenerate metric with only 3 observations total across all products -> must be pruned (< 5)
    # 2. Surviving metric with 6 observations total -> must be retained (>= 5)
    conn.execute(
        """
        INSERT INTO silver.observations (
            product_id, family, metric, code, effective_date, date_source_depth,
            fetched_at, value, raw_value
        ) VALUES
            (1, 'ratios', 'price_sales', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 2.0, '2.0'),
            (1, 'ratios', 'price_sales', NULL, '2024-02-29', 1, '2024-02-29 10:00:00+00', 2.1, '2.1'),
            (1, 'ratios', 'price_sales', NULL, '2024-03-31', 1, '2024-03-31 10:00:00+00', 2.2, '2.2'),

            (1, 'ratios', 'price_book', NULL, '2024-01-31', 1, '2024-01-31 10:00:00+00', 3.0, '3.0'),
            (1, 'ratios', 'price_book', NULL, '2024-02-29', 1, '2024-02-29 10:00:00+00', 3.1, '3.1'),
            (1, 'ratios', 'price_book', NULL, '2024-03-31', 1, '2024-03-31 10:00:00+00', 3.2, '3.2'),
            (1, 'ratios', 'price_book', NULL, '2024-04-30', 1, '2024-04-30 10:00:00+00', 3.3, '3.3'),
            (1, 'ratios', 'price_book', NULL, '2024-05-31', 1, '2024-05-31 10:00:00+00', 3.4, '3.4'),
            (1, 'ratios', 'price_book', NULL, '2024-06-30', 1, '2024-06-30 10:00:00+00', 3.5, '3.5')
        """
    )
    conn.close()

    run_panel(panel_db)

    conn = duckdb.connect(panel_db)
    ps_rows = conn.execute(
        "SELECT COUNT(*) FROM silver.monthly_panel WHERE feature_id = 'ratios_price_sales'"
    ).fetchone()[0]
    pb_rows = conn.execute(
        "SELECT COUNT(*) FROM silver.monthly_panel WHERE feature_id = 'ratios_price_book'"
    ).fetchone()[0]

    assert ps_rows == 0  # Pruned
    assert pb_rows > 0  # Retained
    conn.close()
