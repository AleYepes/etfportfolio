from datetime import UTC, date, datetime

import pytest

from etfportfolio.observations.extractors import (
    extract_esg,
    extract_holdings,
    extract_lipper,
    extract_mstar,
    extract_profile,
    extract_ratios,
    extract_theme_weights,
)
from etfportfolio.observations.utils import (
    clean_credit_rating,
    parse_effective_date,
    parse_manager_tenure,
    parse_net_assets,
    parse_percentage,
    sanitize_metric_id,
)


def test_utils():
    fallback = date(2026, 9, 1)

    # parse_effective_date fallbacks
    assert parse_effective_date(None, fallback) == (fallback, "snapshot")
    assert parse_effective_date(0, fallback) == (fallback, "snapshot")
    assert parse_effective_date(-100, fallback) == (fallback, "snapshot")
    assert parse_effective_date("", fallback) == (fallback, "snapshot")
    assert parse_effective_date("invalid", fallback) == (fallback, "snapshot")

    # parse_effective_date successes
    assert parse_effective_date(1785470400000, fallback) == (date(2026, 7, 31), "payload")
    assert parse_effective_date(1785470400000, fallback, default_source="item") == (date(2026, 7, 31), "item")
    assert parse_effective_date("20260831", fallback) == (date(2026, 8, 31), "payload")
    assert parse_effective_date("2026-08-15", fallback) == (date(2026, 8, 15), "payload")
    assert parse_effective_date("2026/08/20", fallback) == (date(2026, 8, 20), "payload")

    # clean_credit_rating
    assert clean_credit_rating("% Quality/AAA") == "AAA"
    assert clean_credit_rating("% Quality/BBB") == "BBB"
    assert clean_credit_rating("% Quality/Below B") == "Below B"
    assert clean_credit_rating("% Quality Not Rated") == "Not Rated"
    assert clean_credit_rating("% Quality Not Available") == "Not Available"
    assert clean_credit_rating("A") == "A"

    # sanitize_metric_id
    assert sanitize_metric_id("Price/Earnings") == "price_earnings"
    assert sanitize_metric_id("Dividend_Yield_Weighted_Average") == "dividend_yield_weighted_average"
    assert sanitize_metric_id("  Sales Growth 5 Yr  ") == "sales_growth_5_yr"
    assert sanitize_metric_id("TRESGS") == "tresgs"

    # parse_net_assets
    aum_us = parse_net_assets("$78.63B (2026/07/31)", fallback_date=fallback)
    assert aum_us is not None
    assert aum_us[0] == 78630000000.0
    assert aum_us[1] == "$78.63B (2026/07/31)"
    assert aum_us[2] == date(2026, 7, 31)
    assert aum_us[3] == "item"

    aum_eu = parse_net_assets("2,5B", fallback_date=fallback)
    assert aum_eu is not None
    assert aum_eu[0] == 2500000000.0
    assert aum_eu[2] == fallback
    assert aum_eu[3] == "snapshot"

    aum_thousands = parse_net_assets("1,250.5M", fallback_date=fallback)
    assert aum_thousands is not None
    assert aum_thousands[0] == 1250500000.0

    assert parse_net_assets(None, fallback_date=fallback) is None
    assert parse_net_assets("", fallback_date=fallback) is None
    assert parse_net_assets("N/A", fallback_date=fallback) is None
    assert parse_net_assets("abc", fallback_date=fallback) is None

    # parse_manager_tenure
    ref_date = date(2026, 8, 15)
    tenure = parse_manager_tenure("2013/01/01", ref_date=ref_date)
    assert tenure is not None
    expected_years = round((ref_date - date(2013, 1, 1)).days / 365.25, 4)
    assert tenure[0] == expected_years
    assert tenure[1] == "2013/01/01"

    assert parse_manager_tenure(None, ref_date=ref_date) is None
    assert parse_manager_tenure("", ref_date=ref_date) is None
    with pytest.raises(ValueError, match="Invalid Manager Tenure date string"):
        parse_manager_tenure("not-a-date", ref_date=ref_date)

    # parse_percentage
    assert parse_percentage("0.32%") == (0.0032, "0.32%")
    assert parse_percentage("<0.01%", allow_bound=True) == (0.0001, "<0.01%")
    assert parse_percentage(None) is None
    assert parse_percentage("-") is None
    with pytest.raises(ValueError, match="Cannot parse percentage"):
        parse_percentage("invalid_pct")


def test_extract_ratios():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "as_of_date": 1785470400000,  # 2026-07-31
        "dividend": [
            {"name_tag": "Dividend_Yield_Weighted_Average", "value": 3.3433, "avg": 3.41},
            {"name_tag": "Price_to_Dividend", "value": None},
        ],
        "financials": [
            {"name_tag": "Sales_Growth_5_Yr", "value": 11.352},
        ],
        "fixed_income": [
            {"name_tag": "Yield_to_Maturity", "value": 4.5},
        ],
        "ratios": [
            {"name_tag": "Price/Earnings", "value": 38.15},
        ],
        "zscore": [
            {"name_tag": "Latest_Composite_Z_Score", "value": -0.18},
        ],
    }

    res = extract_ratios(1001, payload, created_at)
    assert len(res.metrics) == 5
    assert len(res.dimensions) == 0

    eff_date = date(2026, 7, 31)
    expected_metrics = {
        "dividend_yield_weighted_average": 3.3433,
        "sales_growth_5_yr": 11.352,
        "yield_to_maturity": 4.5,
        "price_earnings": 38.15,
        "latest_composite_z_score": -0.18,
    }

    for m in res.metrics:
        pid, source, metric_id, eff, eff_src, snap_time, val, raw_val = m
        assert pid == 1001
        assert source == "ratios"
        assert eff == eff_date
        assert eff_src == "payload"
        assert snap_time == created_at
        assert val == expected_metrics[metric_id]

    empty_res = extract_ratios(1001, {}, created_at)
    assert len(empty_res.metrics) == 0
    assert len(empty_res.dimensions) == 0


def test_extract_profile():
    created_at = datetime(2026, 8, 15, 10, 0, 0, tzinfo=UTC)
    payload = {
        "expenses_allocation": [
            {"name": "Management Expenses", "ratio": 0.85},
            {"name": "Non-Management Expenses", "ratio": 0.15},
        ],
        "fund_and_profile": [
            {"name_tag": "Total_Expense_Ratio", "value": "0.06%"},
            {"name_tag": "Management_Approach", "value": "Passive"},
            {"name": "Total Net Assets (Month End)", "value": "$78.63B (2026/07/31)"},
            {"name": "Manager Tenure", "value": "2013/01/01"},
            {"name_tag": "Inception_Date", "value": "2001/01/29"},  # ignored
        ],
        "reports": [
            {
                "name": "Annual Report",
                "as_of_date": 1761883200000,  # 2025-10-31
                "fields": [{"name": "Total Net Expense", "value": "0.0564%"}],
            }
        ],
        "mstar": {
            "x_axis_tag": ["value", "core", "growth"],
            "y_axis_tag": ["large", "multi", "mid", "small"],
            "selected": [[1, 1], [1, 2]],
            "hist": [[0, 0]],
        },
    }

    res = extract_profile(1001, payload, created_at)
    # 2 expenses + 4 fund_and_profile + 1 annual report = 7 metrics
    assert len(res.metrics) == 7
    # 2 selected style_box + 1 hist style_box_hist = 3 dimensions
    assert len(res.dimensions) == 3

    metrics = {m[2]: (m[3], m[4], m[6], m[7]) for m in res.metrics}
    snap_date = date(2026, 8, 15)

    assert metrics["management_expense_ratio"] == (snap_date, "snapshot", 0.85, "0.85")
    assert metrics["non_management_expense_ratio"] == (snap_date, "snapshot", 0.15, "0.15")
    assert pytest.approx(metrics["total_expense_ratio"][2]) == 0.0006
    assert metrics["total_expense_ratio"][:2] == (snap_date, "snapshot")
    assert metrics["total_expense_ratio"][3] == "0.06%"
    assert metrics["is_passive"] == (snap_date, "snapshot", 1.0, "Passive")
    assert metrics["total_net_assets_local"] == (date(2026, 7, 31), "item", 78630000000.0, "$78.63B (2026/07/31)")
    assert metrics["manager_tenure_years"][1] == "snapshot"
    assert pytest.approx(metrics["manager_tenure_years"][2]) == round((snap_date - date(2013, 1, 1)).days / 365.25, 4)

    audited = metrics["audited_net_expense_ratio"]
    assert audited[0] == date(2025, 10, 31)
    assert audited[1] == "item"
    assert pytest.approx(audited[2]) == 0.000564
    assert audited[3] == "0.0564%"

    # Style Box dimensions check
    dims = {(d[1], d[2], d[3]): (d[4], d[5], d[7], d[8]) for d in res.dimensions}
    assert dims[("style_box", "Multi Core", "multi_core")] == (snap_date, "snapshot", 1.0, "[1, 1]")
    assert dims[("style_box", "Mid Core", "mid_core")] == (snap_date, "snapshot", 1.0, "[1, 2]")
    assert dims[("style_box_hist", "Large Value", "large_value")] == (snap_date, "snapshot", 1.0, "[0, 0]")

    # Active fund test
    active_payload = {"fund_and_profile": [{"name_tag": "Management_Approach", "value": "Active"}]}
    active_res = extract_profile(1001, active_payload, created_at)
    assert len(active_res.metrics) == 1
    assert active_res.metrics[0][2] == "is_passive"
    assert active_res.metrics[0][6] == 0.0

    # Validation errors
    bad_approach = {"fund_and_profile": [{"name_tag": "Management_Approach", "value": "Robotic"}]}
    with pytest.raises(ValueError, match="Unknown Management Approach"):
        extract_profile(1001, bad_approach, created_at)

    bad_style_tag = {
        "mstar": {
            "x_axis_tag": ["unknown"],
            "y_axis_tag": ["large"],
            "selected": [[0, 0]],
        }
    }
    with pytest.raises(ValueError, match="Unrecognized style box axis configuration"):
        extract_profile(1001, bad_style_tag, created_at)

    bad_style_coord = {
        "mstar": {
            "x_axis_tag": ["value", "core", "growth"],
            "y_axis_tag": ["large", "mid", "small"],
            "selected": [[5, 5]],
        }
    }
    with pytest.raises(ValueError, match="Style box coordinate out of bounds"):
        extract_profile(1001, bad_style_coord, created_at)

    empty_res = extract_profile(1001, {}, created_at)
    assert len(empty_res.metrics) == 0
    assert len(empty_res.dimensions) == 0


def test_extract_esg():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "asOfDate": "20260822",
        "coverage": 0.99762,
        "content": [
            {
                "name": "TRESGS",
                "value": 6,
            },
            {
                "name": "TRESGENS",
                "value": 7,
                "children": [
                    {"name": "TRESGENERS", "value": 8},
                    {"name": "TRESGENPIS", "value": 5},
                ],
            },
        ],
    }

    res = extract_esg(1001, payload, created_at)
    assert len(res.metrics) == 5
    assert len(res.dimensions) == 0

    eff_date = date(2026, 8, 22)
    metrics = {m[2]: (m[4], m[6]) for m in res.metrics}
    assert metrics["esg_coverage"] == ("payload", 0.99762)
    assert metrics["tresgs"] == ("payload", 6.0)
    assert metrics["tresgens"] == ("payload", 7.0)
    assert metrics["tresgeners"] == ("payload", 8.0)
    assert metrics["tresgenpis"] == ("payload", 5.0)

    for m in res.metrics:
        assert m[0] == 1001
        assert m[1] == "esg"
        assert m[3] == eff_date

    empty_res = extract_esg(1001, {}, created_at)
    assert len(empty_res.metrics) == 0
    assert len(empty_res.dimensions) == 0


def test_extract_mstar():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "as_of_date": "20260731",
        "summary": [
            {"id": "category", "value": "Real Estate"},  # skipped
            {"id": "medalist_rating", "value": "Gold", "q": False, "publish_date": "20260427"},
            {"id": "process", "value": "High", "q": False, "publish_date": "20260427"},
            {"id": "q_process", "value": "Below_Average", "q": True, "publish_date": "20260731"},
            {"id": "morningstar_rating", "value": "4", "q": False, "publish_date": "20260731"},
            {"id": "parent", "value": "Above_Average", "q": False},  # fallback to payload as_of_date
            {"id": "people", "value": "Under_Review"},  # skipped
            {"id": "sustainability_rating", "value": "High", "publish_date": "20260630"},
        ],
    }

    res = extract_mstar(1001, payload, created_at)
    # 5 active ratings: medalist_rating_analyst, process_analyst, process_quant, morningstar_rating, parent_analyst, sustainability_rating = 6
    assert len(res.metrics) == 6
    assert len(res.dimensions) == 0

    metrics = {m[2]: (m[3], m[4], m[6], m[7]) for m in res.metrics}
    assert metrics["mstar_medalist_rating_analyst"] == (date(2026, 4, 27), "item", 5.0, "Gold")
    assert metrics["mstar_process_analyst"] == (date(2026, 4, 27), "item", 5.0, "High")
    assert metrics["mstar_process_quant"] == (date(2026, 7, 31), "item", 2.0, "Below_Average")
    assert metrics["mstar_morningstar_rating"] == (date(2026, 7, 31), "item", 4.0, "4")
    assert metrics["mstar_parent_analyst"] == (date(2026, 7, 31), "payload", 4.0, "Above_Average")
    assert metrics["mstar_sustainability_rating"] == (date(2026, 6, 30), "item", 5.0, "High")

    # Unrecognized rating string raises ValueError
    bad_payload = {"summary": [{"id": "people", "value": "Nonexistent_Rating"}]}
    with pytest.raises(ValueError, match="Unrecognized rating string"):
        extract_mstar(1001, bad_payload, created_at)

    # Missing id raises ValueError
    missing_id_payload = {"summary": [{"value": "High"}]}
    with pytest.raises(ValueError, match="Missing 'id'"):
        extract_mstar(1001, missing_id_payload, created_at)

    empty_res = extract_mstar(1001, {}, created_at)
    assert len(empty_res.metrics) == 0
    assert len(empty_res.dimensions) == 0


def test_extract_lipper():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "universes": [
            {
                "name": "United States",
                "as_of_date": 1785470400000,  # 2026-07-31
                "3_year": [{"name_tag": "consistent_return", "rating": {"value": 4}}],
            },
            {
                "name": "Chile",
                "as_of_date": 1785470400000,  # 2026-07-31
                "3_year": [{"name_tag": "consistent_return", "rating": {"value": 5}}],
            },
        ]
    }

    res = extract_lipper(1001, payload, created_at)
    assert len(res.metrics) == 2
    assert len(res.dimensions) == 0

    metrics = {m[2]: (m[3], m[4], m[6], m[7]) for m in res.metrics}
    eff_date = date(2026, 7, 31)
    assert metrics["lipper_consistent_return_3yr_united_states"] == (eff_date, "item", 4.0, "4")
    assert metrics["lipper_consistent_return_3yr_chile"] == (eff_date, "item", 5.0, "5")

    empty_res = extract_lipper(1001, {}, created_at)
    assert len(empty_res.metrics) == 0
    assert len(empty_res.dimensions) == 0


def test_extract_holdings():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "as_of_date": 1785470400000,  # 2026-07-31
        "top_10_weight": "30.47%",
        "allocation_self": [
            {"name": "Equity", "weight": 99.76},
        ],
        "currency": [  # explicitly discarded per FRD
            {"name": "US Dollar", "code": "USD", "weight": 99.8},
        ],
        "geographic": [  # explicitly discarded per FRD
            {"name": "North America", "weight": 99.8},
        ],
        "investor_country": [
            {"name": "United States", "country_code": "US", "weight": 71.2451},
        ],
        "debtor": [
            {"name": "% Quality/AAA", "weight": 1.5},
        ],
        "top_10": [
            {
                "name": "GUGGENHEIM STRATEGIC OPPORTUNITIES FUND",
                "conids": [86174372],
                "assets_pct": "3.49%",
            },
            {
                "name": "MICROSOFT CORP",
                "ticker": "MSFT",
                "conids": [272093],
                "assets_pct": "<0.01%",
            },
        ],
    }

    res = extract_holdings(1001, payload, created_at)
    assert len(res.metrics) == 1
    assert len(res.dimensions) == 5

    # Scalar metric check
    m = res.metrics[0]
    assert m[1] == "holdings"
    assert m[2] == "portfolio_top_10_concentration"
    assert m[3] == date(2026, 7, 31)
    assert m[4] == "payload"
    assert pytest.approx(m[6]) == 0.3047
    assert m[7] == "30.47%"

    # Dimensions check
    dims = {(d[1], d[2]): (d[3], d[7], d[8]) for d in res.dimensions}
    assert pytest.approx(dims[("asset_class", "Equity")][1]) == 0.9976
    assert dims[("country", "United States")][0] == "US"
    assert pytest.approx(dims[("country", "United States")][1]) == 0.712451
    assert dims[("credit_rating", "AAA")][0] == "AAA"
    assert pytest.approx(dims[("credit_rating", "AAA")][1]) == 0.015

    # Top-10 holdings
    assert dims[("top_holding", "GUGGENHEIM STRATEGIC OPPORTUNITIES FUND")][0] == "86174372"
    assert pytest.approx(dims[("top_holding", "GUGGENHEIM STRATEGIC OPPORTUNITIES FUND")][1]) == 0.0349
    assert dims[("top_holding", "MSFT - MICROSOFT CORP")][0] == "272093"
    assert pytest.approx(dims[("top_holding", "MSFT - MICROSOFT CORP")][1]) == 0.0001

    empty_res = extract_holdings(1001, {}, created_at)
    assert len(empty_res.metrics) == 0
    assert len(empty_res.dimensions) == 0


def test_extract_theme_weights():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "themes": [
            {
                "key": "006a0c27-4a9a-4766-8988-0d8acc6ede8b",
                "name": "Discount Retail",
                "weight": 0.084085,  # discarded
                "rank_adjusted_weight": 0.0094658,  # preserved
            }
        ]
    }

    res = extract_theme_weights(1001, payload, created_at)
    assert len(res.metrics) == 0
    assert len(res.dimensions) == 1

    d = res.dimensions[0]
    assert d[0] == 1001
    assert d[1] == "theme"
    assert d[2] == "Discount Retail"
    assert d[3] == "006a0c27-4a9a-4766-8988-0d8acc6ede8b"
    assert d[4] == date(2026, 9, 1)
    assert d[5] == "snapshot"
    assert d[6] == created_at
    assert pytest.approx(d[7]) == 0.0094658
    assert d[8] == "0.0094658"

    empty_res = extract_theme_weights(1001, {}, created_at)
    assert len(empty_res.metrics) == 0
    assert len(empty_res.dimensions) == 0
