from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from etfportfolio.core.db import apply_schema
from etfportfolio.prep.panel import (
    CROSS_SECTION_WINSOR_FEATURES,
    DEBT_CLUSTERS,
    DEBT_TYPE_TO_CLUSTER,
    ESG_CAP_DAYS,
    ESG_FEATURE_MAP,
    MEDALIST_FEE_CAP_DAYS,
    MONTHLY_CAP_DAYS,
    STYLE_FEATURE_IDS,
    TENURE_STYLE_CAP_DAYS,
    country_feature_id,
    credit_feature_id,
    debt_cluster,
    industry_feature_id,
    month_end_spine,
    run_panel,
)


def _fetched(day: date, hour: int = 12) -> datetime:
    return datetime(day.year, day.month, day.day, hour, 0, 0, tzinfo=UTC)


def _insert_metric(
    conn,
    product_id: int,
    source: str,
    metric_id: str,
    effective_date: date,
    value: float,
    *,
    raw_value: str | None = None,
    fetched_at: datetime | None = None,
    effective_date_source: str = "payload",
    currency: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO silver.product_metrics (
            product_id, source, metric_id, effective_date, effective_date_source,
            fetched_at, value, raw_value, currency
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            product_id,
            source,
            metric_id,
            effective_date,
            effective_date_source,
            fetched_at or _fetched(effective_date),
            value,
            raw_value if raw_value is not None else str(value),
            currency,
        ],
    )


def _insert_dim(
    conn,
    product_id: int,
    dimension_type: str,
    dimension_name: str,
    effective_date: date,
    value: float,
    *,
    dimension_code: str | None = None,
    raw_value: str | None = None,
    fetched_at: datetime | None = None,
    effective_date_source: str = "payload",
) -> None:
    conn.execute(
        """
        INSERT INTO silver.product_dimensions (
            product_id, dimension_type, dimension_name, dimension_code,
            effective_date, effective_date_source, fetched_at, value, raw_value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            product_id,
            dimension_type,
            dimension_name,
            dimension_code,
            effective_date,
            effective_date_source,
            fetched_at or _fetched(effective_date),
            value,
            raw_value if raw_value is not None else str(value),
        ],
    )


def _insert_complete_asset_class(
    conn,
    product_id: int,
    effective_date: date,
    equity: float = 0.90,
    fixed_income: float = 0.05,
    cash: float = 0.03,
    other: float = 0.02,
) -> None:
    _insert_dim(conn, product_id, "asset_class", "Equity", effective_date, equity)
    _insert_dim(conn, product_id, "asset_class", "Fixed Income", effective_date, fixed_income)
    _insert_dim(conn, product_id, "asset_class", "Cash", effective_date, cash)
    _insert_dim(conn, product_id, "asset_class", "Other", effective_date, other)


@pytest.fixture
def panel_db(tmp_path: Path) -> str:
    db_file = str(tmp_path / "panel.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)
    conn.close()
    return db_file


def _panel_map(db_file: str, product_id: int, as_of: date) -> dict[str, float]:
    conn = duckdb.connect(db_file)
    rows = conn.execute(
        """
        SELECT feature_id, value, asset_class
        FROM silver.monthly_panel
        WHERE product_id = ? AND as_of_date = ?
        """,
        [product_id, as_of],
    ).fetchall()
    conn.close()
    return {r[0]: (r[1], r[2]) for r in rows}


def _values(db_file: str, product_id: int, as_of: date) -> dict[str, float]:
    return {k: v[0] for k, v in _panel_map(db_file, product_id, as_of).items()}


def test_month_end_spine_inclusive_and_leap():
    spine = month_end_spine(date(2015, 1, 15), date(2015, 3, 2))
    assert spine == [date(2015, 1, 31), date(2015, 2, 28), date(2015, 3, 31)]
    leap = month_end_spine(date(2016, 2, 1), date(2016, 2, 29))
    assert leap == [date(2016, 2, 29)]
    assert month_end_spine(date(2026, 8, 31), date(2026, 8, 1)) == []


def test_debt_cluster_map_is_exhaustive_and_unique():
    assert sum(len(v) for v in DEBT_CLUSTERS.values()) == 110
    assert len(DEBT_TYPE_TO_CLUSTER) == 110
    assert debt_cluster("CORP") == "debt_corporate_senior"
    assert debt_cluster("OTHER") == "debt_specialty_derivatives"
    assert debt_cluster("Bond") == "debt_unsecured_general"
    with pytest.raises(ValueError, match="Unmapped debt_type"):
        debt_cluster("Unknown Widget Bond")


def test_feature_id_helpers():
    assert industry_feature_id("Technology") == "sector_technology"
    assert industry_feature_id("Communication Services") == "sector_communication_services"
    assert industry_feature_id("Not Classified - Non Equity") == "sector_unclassified_non_equity"
    assert industry_feature_id("Non Classified Equity") == "sector_unclassified_equity"
    assert country_feature_id("US") == "country_us"
    assert country_feature_id(None) == "country_unidentified"
    assert country_feature_id("  ") == "country_unidentified"
    assert credit_feature_id("AAA") == "credit_aaa"
    assert credit_feature_id("Not Rated") == "credit_not_rated"
    assert "style_large_growth" in STYLE_FEATURE_IDS
    assert len(STYLE_FEATURE_IDS) == 12
    assert ESG_FEATURE_MAP["tresgs"] == "esg_score"
    assert "ebit_to_interest" in CROSS_SECTION_WINSOR_FEATURES
    assert ESG_CAP_DAYS == 90
    assert MONTHLY_CAP_DAYS == 180
    assert MEDALIST_FEE_CAP_DAYS == 540
    assert TENURE_STYLE_CAP_DAYS == 365


def test_run_panel_empty_clears_table(panel_db):
    assert run_panel(db_path=panel_db) == 0
    conn = duckdb.connect(panel_db)
    n = conn.execute("SELECT COUNT(*) FROM silver.monthly_panel").fetchone()[0]
    conn.close()
    assert n == 0


def test_asset_class_partition_and_dominant(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 7, 31)
    _insert_complete_asset_class(conn, 1, as_of, equity=0.90, fixed_income=0.05, cash=0.03, other=0.02)
    conn.close()

    n = run_panel(db_path=panel_db)
    assert n > 0
    vals = _values(panel_db, 1, date(2026, 7, 31))
    assert vals["asset_class_equity"] == pytest.approx(0.90)
    assert vals["asset_class_fixed_income"] == pytest.approx(0.05)
    assert vals["asset_class_cash"] == pytest.approx(0.03)
    assert vals["asset_class_other"] == pytest.approx(0.02)
    ac = _panel_map(panel_db, 1, date(2026, 7, 31))["asset_class_equity"][1]
    assert ac == "Equity"


def test_asset_class_negatives_retained_and_still_partition(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 7, 31)
    _insert_complete_asset_class(conn, 1, as_of, equity=1.12, fixed_income=0.0, cash=0.0, other=-0.12)
    conn.close()

    run_panel(db_path=panel_db)
    vals = _values(panel_db, 1, date(2026, 7, 31))
    assert vals["asset_class_equity"] == pytest.approx(1.12)
    assert vals["asset_class_other"] == pytest.approx(-0.12)
    assert _panel_map(panel_db, 1, date(2026, 7, 31))["asset_class_equity"][1] == "Equity"


def test_asset_class_partition_failure_raises(panel_db):
    conn = duckdb.connect(panel_db)
    _insert_dim(conn, 1, "asset_class", "Equity", date(2026, 7, 31), 0.50)
    conn.close()
    with pytest.raises(ValueError, match="asset_class weights do not sum"):
        run_panel(db_path=panel_db)


def test_unknown_asset_class_raises(panel_db):
    conn = duckdb.connect(panel_db)
    _insert_dim(conn, 1, "asset_class", "Commodities", date(2026, 7, 31), 1.0)
    conn.close()
    with pytest.raises(ValueError, match="Unknown asset_class"):
        run_panel(db_path=panel_db)


def test_locf_monthly_cap_inclusive(panel_db):
    conn = duckdb.connect(panel_db)
    obs = date(2026, 1, 31)
    _insert_metric(conn, 1, "ratios", "price_earnings", obs, 20.0)
    conn.close()

    run_panel(db_path=panel_db)
    # Jan 31 → Jun 30 is 150 days (<= 180): filled
    assert _values(panel_db, 1, date(2026, 6, 30))["price_earnings"] == 20.0
    # Jan 31 → Jul 31 is 181 days (> 180): stale
    assert "price_earnings" not in _values(panel_db, 1, date(2026, 7, 31))


def test_esg_cap_90_days(panel_db):
    conn = duckdb.connect(panel_db)
    obs = date(2026, 1, 31)
    _insert_metric(conn, 1, "esg", "tresgs", obs, 7.0)
    _insert_metric(conn, 1, "esg", "esg_coverage", obs, 0.99)
    conn.close()

    run_panel(db_path=panel_db)
    apr = _values(panel_db, 1, date(2026, 4, 30))
    assert apr["esg_score"] == 7.0
    assert apr["esg_coverage"] == pytest.approx(0.99)
    assert "esg_score" not in _values(panel_db, 1, date(2026, 5, 31))


def test_is_passive_is_perpetual(panel_db):
    conn = duckdb.connect(panel_db)
    _insert_metric(conn, 1, "profile", "is_passive", date(2020, 1, 31), 1.0, raw_value="Passive")
    # A later observation on another product extends the global spine.
    _insert_metric(conn, 2, "ratios", "price_book", date(2026, 7, 31), 3.0)
    conn.close()

    run_panel(db_path=panel_db)
    assert _values(panel_db, 1, date(2026, 7, 31))["is_passive"] == 1.0


def test_dynamic_manager_tenure(panel_db):
    conn = duckdb.connect(panel_db)
    start = date(2013, 1, 1)
    obs = date(2025, 12, 31)
    frozen = round((obs - start).days / 365.25, 4)
    _insert_metric(
        conn,
        1,
        "profile",
        "manager_tenure_years",
        obs,
        frozen,
        raw_value="2013/01/01",
        effective_date_source="snapshot",
    )
    conn.close()

    run_panel(db_path=panel_db)
    as_of = date(2026, 8, 31)
    expected = round((as_of - start).days / 365.25, 4)
    assert _values(panel_db, 1, as_of)["manager_tenure_years"] == pytest.approx(expected)
    assert _values(panel_db, 1, as_of)["manager_tenure_years"] != pytest.approx(frozen)


def test_tenure_fallback_when_raw_is_not_a_date(panel_db):
    conn = duckdb.connect(panel_db)
    obs = date(2025, 8, 31)
    _insert_metric(
        conn,
        1,
        "profile",
        "manager_tenure_years",
        obs,
        12.5,
        raw_value="12.5 years",
    )
    conn.close()

    run_panel(db_path=panel_db)
    as_of = date(2026, 8, 31)
    expected = 12.5 + (as_of - obs).days / 365.25
    assert _values(panel_db, 1, as_of)["manager_tenure_years"] == pytest.approx(expected)


def test_style_box_emits_all_twelve_and_precedence(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 6, 30)
    _insert_dim(conn, 1, "style_box", "Mid Core", as_of, 1.0, dimension_code="mid_core", raw_value="[1, 2]")
    _insert_dim(
        conn,
        1,
        "style_box_hist",
        "Large Value",
        as_of,
        1.0,
        dimension_code="large_value",
        raw_value="[0, 0]",
    )
    conn.close()

    run_panel(db_path=panel_db)
    vals = _values(panel_db, 1, date(2026, 6, 30))
    for fid in STYLE_FEATURE_IDS:
        assert fid in vals
    assert vals["style_mid_core"] == 1.0
    assert vals["style_large_value"] == 0.0  # hist loses to selected
    assert vals["style_large_growth"] == 0.0
    assert vals["style_mid_value"] == 0.0
    assert sum(vals[f] for f in STYLE_FEATURE_IDS) == pytest.approx(1.0)


def test_style_box_hist_used_when_selected_absent(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 6, 30)
    _insert_dim(
        conn,
        1,
        "style_box_hist",
        "Large Value",
        as_of,
        1.0,
        dimension_code="large_value",
    )
    conn.close()

    run_panel(db_path=panel_db)
    vals = _values(panel_db, 1, date(2026, 6, 30))
    assert vals["style_large_value"] == 1.0
    assert vals["style_mid_core"] == 0.0


def test_debt_type_clusters_sum_weights(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 7, 31)
    _insert_dim(conn, 1, "debt_type", "CORP", as_of, 0.40, dimension_code="corp")
    _insert_dim(conn, 1, "debt_type", "Senior Note", as_of, 0.10, dimension_code="sn")
    _insert_dim(conn, 1, "debt_type", "Bond", as_of, 0.25, dimension_code="bond")
    _insert_dim(conn, 1, "debt_type", "OTHER", as_of, 0.05, dimension_code="oth")
    conn.close()

    run_panel(db_path=panel_db)
    vals = _values(panel_db, 1, date(2026, 7, 31))
    assert vals["debt_corporate_senior"] == pytest.approx(0.50)
    assert vals["debt_unsecured_general"] == pytest.approx(0.25)
    assert vals["debt_specialty_derivatives"] == pytest.approx(0.05)
    assert "CORP" not in vals


def test_unmapped_debt_type_raises(panel_db):
    conn = duckdb.connect(panel_db)
    _insert_dim(conn, 1, "debt_type", "Unknown Widget Bond", date(2026, 7, 31), 0.1)
    conn.close()
    with pytest.raises(ValueError, match="Unmapped debt_type"):
        run_panel(db_path=panel_db)


def test_theme_children_and_parent_rollups(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 9, 1)
    parent_id = "parent-tech"
    child_a = "child-a"
    child_b = "child-b"
    now = datetime(2026, 9, 1, 12, 0, 0)
    conn.execute(
        """
        INSERT INTO bronze.themes (theme_id, name, parent_id, created_at, updated_at)
        VALUES (?, 'Technology and Innovation', NULL, ?, ?),
               (?, 'Discount Retail', ?, ?, ?),
               (?, 'Cloud Computing', ?, ?, ?)
        """,
        [
            parent_id,
            now,
            now,
            child_a,
            parent_id,
            now,
            now,
            child_b,
            parent_id,
            now,
            now,
        ],
    )
    _insert_metric(conn, 1, "theme_weights", "theme_coverage", as_of, 0.91, effective_date_source="snapshot")
    _insert_dim(conn, 1, "theme", "Discount Retail", as_of, 0.10, dimension_code=child_a)
    _insert_dim(conn, 1, "theme", "Cloud Computing", as_of, 0.20, dimension_code=child_b)
    conn.close()

    run_panel(db_path=panel_db)
    vals = _values(panel_db, 1, date(2026, 9, 30))
    assert vals["theme_coverage"] == pytest.approx(0.91)
    assert vals[f"theme_{child_a}"] == pytest.approx(0.10)
    assert vals[f"theme_{child_b}"] == pytest.approx(0.20)
    assert vals[f"theme_parent_{parent_id}"] == pytest.approx(0.30)


def test_theme_coverage_gate_drops_weights(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 9, 1)
    child = "child-a"
    now = datetime(2026, 9, 1, 12, 0, 0)
    conn.execute(
        """
        INSERT INTO bronze.themes (theme_id, name, parent_id, created_at, updated_at)
        VALUES (?, 'Child', 'parent-x', ?, ?)
        """,
        [child, now, now],
    )
    _insert_metric(conn, 1, "theme_weights", "theme_coverage", as_of, 0.50)
    _insert_dim(conn, 1, "theme", "Discount Retail", as_of, 0.10, dimension_code=child)
    conn.close()

    run_panel(db_path=panel_db)
    vals = _values(panel_db, 1, date(2026, 9, 30))
    assert vals["theme_coverage"] == pytest.approx(0.50)
    assert f"theme_{child}" not in vals
    assert "theme_parent_parent-x" not in vals


def test_esg_aliases(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 8, 22)
    _insert_metric(conn, 1, "esg", "tresgs", as_of, 6.0)
    _insert_metric(conn, 1, "esg", "tresgccs", as_of, 0.0)
    _insert_metric(conn, 1, "esg", "esg_coverage", as_of, 0.8)
    conn.close()

    run_panel(db_path=panel_db)
    vals = _values(panel_db, 1, date(2026, 8, 31))
    assert "tresgs" not in vals
    assert vals["esg_score"] == 6.0
    assert vals["esg_controversies"] == 0.0
    assert vals["esg_coverage"] == pytest.approx(0.8)


def test_average_quality_dash_is_omitted(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 7, 31)
    _insert_metric(conn, 1, "ratios", "average_quality", as_of, 10.0, raw_value="-")
    _insert_metric(conn, 1, "ratios", "price_book", as_of, 4.0, raw_value="4.0")
    conn.close()

    run_panel(db_path=panel_db)
    vals = _values(panel_db, 1, date(2026, 7, 31))
    assert "average_quality" not in vals
    assert vals["price_book"] == 4.0


def test_expense_ratio_winsorization_and_fee_rate(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 8, 15)
    _insert_metric(conn, 1, "profile", "total_expense_ratio", as_of, 0.006, raw_value="0.60%")
    _insert_metric(conn, 1, "profile", "management_expense_ratio", as_of, 96.05, raw_value="96.05")
    _insert_metric(conn, 1, "profile", "non_management_expense_ratio", as_of, -95.05, raw_value="-95.05")
    conn.close()

    run_panel(db_path=panel_db)
    vals = _values(panel_db, 1, date(2026, 8, 31))
    assert vals["management_expense_ratio"] == pytest.approx(2.0)
    assert vals["non_management_expense_ratio"] == pytest.approx(-1.0)
    # Fee rate uses the unclipped allocation ratio.
    assert vals["management_fee_rate"] == pytest.approx(0.006 * 96.05)


def test_is_leveraged_flag(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 7, 31)
    _insert_metric(conn, 1, "holdings", "portfolio_top_10_concentration", as_of, 1.5, raw_value="150%")
    _insert_metric(conn, 2, "holdings", "portfolio_top_10_concentration", as_of, 0.30, raw_value="30%")
    conn.close()

    run_panel(db_path=panel_db)
    assert _values(panel_db, 1, date(2026, 7, 31))["portfolio_top_10_concentration"] == pytest.approx(1.5)
    assert _values(panel_db, 1, date(2026, 7, 31))["is_leveraged"] == 1.0
    assert _values(panel_db, 2, date(2026, 7, 31))["is_leveraged"] == 0.0
    assert _values(panel_db, 2, date(2026, 7, 31))["portfolio_top_10_concentration"] == pytest.approx(0.30)


def test_morningstar_pillar_analyst_priority_then_quant_fallback(panel_db):
    conn = duckdb.connect(panel_db)
    analyst_date = date(2025, 1, 31)
    quant_date = date(2025, 6, 30)
    _insert_metric(conn, 1, "mstar", "mstar_people_analyst", analyst_date, 5.0, raw_value="High")
    _insert_metric(conn, 1, "mstar", "mstar_people_quant", quant_date, 3.0, raw_value="Average")
    conn.close()

    run_panel(db_path=panel_db)
    # While analyst is still fresh, it wins even after the quant observation.
    june = _values(panel_db, 1, date(2025, 6, 30))
    assert june["pillar_people"] == 5.0
    assert june["pillar_people_is_quant"] == 0.0

    # Analyst 540d cap: 2025-01-31 + 540d = 2026-07-25. 2026-07-31 is stale for analyst.
    # Quant 2025-06-30 + 540d = 2026-12-22, still live at 2026-07-31.
    late = _values(panel_db, 1, date(2026, 7, 31))
    assert late["pillar_people"] == 3.0
    assert late["pillar_people_is_quant"] == 1.0
    assert "mstar_people_analyst" not in late
    assert "mstar_people_quant" not in late


def test_country_industry_credit_maturity_feature_ids(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 7, 31)
    _insert_dim(conn, 1, "country", "United States", as_of, 0.71, dimension_code="US")
    _insert_dim(conn, 1, "country", "Unidentified", as_of, -0.015, dimension_code=None)
    _insert_dim(conn, 1, "industry", "Technology", as_of, 0.22)
    _insert_dim(
        conn,
        1,
        "industry",
        "Not Classified - Non Equity",
        as_of,
        0.01,
    )
    _insert_dim(conn, 1, "credit_rating", "AAA", as_of, 0.15, dimension_code="AAA")
    _insert_dim(conn, 1, "credit_rating", "Not Rated", as_of, 0.05, dimension_code="Not Rated")
    _insert_dim(
        conn,
        1,
        "maturity",
        "% Maturity Less than 1 Year",
        as_of,
        0.10,
        dimension_code="mat_lt_1y",
    )
    conn.close()

    run_panel(db_path=panel_db)
    vals = _values(panel_db, 1, date(2026, 7, 31))
    assert vals["country_us"] == pytest.approx(0.71)
    assert vals["country_unidentified"] == pytest.approx(-0.015)
    assert vals["sector_technology"] == pytest.approx(0.22)
    assert vals["sector_unclassified_non_equity"] == pytest.approx(0.01)
    assert vals["credit_aaa"] == pytest.approx(0.15)
    assert vals["credit_not_rated"] == pytest.approx(0.05)
    assert vals["mat_lt_1y"] == pytest.approx(0.10)


def test_family_locf_does_not_mix_asset_class_snapshots(panel_db):
    """A later holdings snapshot that drops Cash must not keep the old Cash weight."""
    conn = duckdb.connect(panel_db)
    first = date(2026, 1, 31)
    second = date(2026, 2, 28)
    _insert_complete_asset_class(conn, 1, first, equity=0.80, fixed_income=0.0, cash=0.20, other=0.0)
    _insert_complete_asset_class(conn, 1, second, equity=1.0, fixed_income=0.0, cash=0.0, other=0.0)
    conn.close()

    run_panel(db_path=panel_db)
    jan = _values(panel_db, 1, date(2026, 1, 31))
    assert jan["asset_class_equity"] == pytest.approx(0.80)
    assert jan["asset_class_cash"] == pytest.approx(0.20)
    feb = _values(panel_db, 1, date(2026, 2, 28))
    assert feb["asset_class_equity"] == pytest.approx(1.0)
    assert feb["asset_class_cash"] == pytest.approx(0.0)


def test_rebuild_is_idempotent(panel_db):
    conn = duckdb.connect(panel_db)
    _insert_metric(conn, 1, "ratios", "price_book", date(2026, 7, 31), 4.2)
    _insert_complete_asset_class(conn, 1, date(2026, 7, 31))
    conn.close()

    n1 = run_panel(db_path=panel_db)
    n2 = run_panel(db_path=panel_db)
    assert n1 == n2
    conn = duckdb.connect(panel_db)
    count = conn.execute("SELECT COUNT(*) FROM silver.monthly_panel").fetchone()[0]
    conn.close()
    assert count == n1


def test_unknown_asset_class_on_metrics_only_product(panel_db):
    conn = duckdb.connect(panel_db)
    _insert_metric(conn, 1, "ratios", "price_earnings", date(2026, 7, 31), 18.0)
    conn.close()

    run_panel(db_path=panel_db)
    row = _panel_map(panel_db, 1, date(2026, 7, 31))["price_earnings"]
    assert row[0] == 18.0
    assert row[1] == "Unknown"


def test_medalist_cap_540(panel_db):
    conn = duckdb.connect(panel_db)
    obs = date(2025, 1, 31)
    _insert_metric(conn, 1, "mstar", "mstar_medalist_rating", obs, 5.0, raw_value="Gold")
    conn.close()

    run_panel(db_path=panel_db)
    # 2025-01-31 + 540d = 2026-07-25; 2026-06-30 is still inside the cap.
    assert _values(panel_db, 1, date(2026, 6, 30))["mstar_medalist_rating"] == 5.0
    assert "mstar_medalist_rating" not in _values(panel_db, 1, date(2026, 7, 31))


def test_cross_section_winsor_clips_outliers(panel_db):
    conn = duckdb.connect(panel_db)
    as_of = date(2026, 7, 31)
    # 100 products at 0.15, one extreme ROE so 1st/99th actually move.
    for pid in range(1, 101):
        _insert_metric(conn, pid, "ratios", "return_on_equity_1yr", as_of, 0.15)
    _insert_metric(conn, 101, "ratios", "return_on_equity_1yr", as_of, 29711.73)
    conn.close()

    run_panel(db_path=panel_db)
    extreme = _values(panel_db, 101, date(2026, 7, 31))["return_on_equity_1yr"]
    typical = _values(panel_db, 1, date(2026, 7, 31))["return_on_equity_1yr"]
    assert extreme < 29711.73
    assert typical == pytest.approx(0.15)
