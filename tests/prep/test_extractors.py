from __future__ import annotations

import math
from datetime import UTC, date, datetime

import pytest

from etfportfolio.prep.extractors import (
    extract_esg,
    extract_holdings,
    extract_lipper,
    extract_mstar,
    extract_profile,
    extract_ratios,
    extract_theme_weights,
)
from tests.conftest import load_fixture


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
            {"name_tag": "average_quality", "value": 8.0, "value_fmt": "-"},  # Should be skipped
        ],
        "ratios": [
            {"name_tag": "Price/Earnings", "value": 38.15},
        ],
        "zscore": [
            {"name_tag": "Latest_Composite_Z_Score", "value": -0.18},
        ],
    }

    obs_list = extract_ratios(1001, payload, created_at)
    assert len(obs_list) == 5

    eff_date = date(2026, 7, 31)
    expected_metrics = {
        "dividend_yield_weighted_average": 0.033433,
        "sales_growth_5_yr": 0.11352,
        "yield_to_maturity": 0.045,
        "price_earnings": 38.15,
        "latest_composite_z_score": -0.18,
    }

    for obs in obs_list:
        assert obs.product_id == 1001
        assert obs.family == "ratios"
        assert obs.effective_date == eff_date
        assert obs.date_source_depth == 1
        assert obs.fetched_at == created_at
        assert obs.value == pytest.approx(expected_metrics[obs.metric])
        assert obs.code is None

    # Unknown ratio tag raises
    bad_payload = {
        "ratios": [{"name_tag": "Unknown_Ratio_Tag_XYZ", "value": 1.0}],
    }
    with pytest.raises(ValueError, match="Unrecognized ratio metric"):
        extract_ratios(1001, bad_payload, created_at)

    # Empty payload returns empty
    assert extract_ratios(1001, {}, created_at) == []


def test_extract_profile():
    created_at = datetime(2026, 8, 15, 10, 0, 0, tzinfo=UTC)
    payload = {
        "expenses_allocation": [
            {"name": "Management Expenses", "ratio": 0.002, "value": "0.20%"},
            {"name": "Non-Management Expenses", "ratio": 0.0005, "value": "0.05%"},
        ],
        "fund_and_profile": [
            {"name_tag": "Total_Expense_Ratio", "value": "0.25%"},
            {"name_tag": "Management_Approach", "value": "Passive"},
            {"name_tag": "Total_Net_Assets_Month_End", "value": "$100.5M (2026/07/31)"},
            {"name_tag": "Manager_Tenure", "value": "2020/01/01"},
        ],
        "reports": [
            {
                "name": "Annual Report",
                "as_of_date": "2025-12-31",
                "fields": [{"name": "Total Net Expense", "value": "0.24%"}],
            }
        ],
        "mstar": {
            "x_axis_tag": ["Value", "Core", "Growth"],
            "y_axis_tag": ["Large", "Multi", "Mid", "Small"],
            "selected": [[1, 2]],  # Core, Mid -> mid_core
            "hist": [[0, 0]],  # Value, Large -> large_value
        },
    }

    known = {"USD", "CAD", "EUR"}
    obs_list = extract_profile(
        1001,
        payload,
        created_at,
        contract_currency="USD",
        known_currencies=known,
    )

    by_key = {(obs.family, obs.metric): obs for obs in obs_list}

    # Management expenses
    m_exp = by_key[("profile", "management_expense_ratio")]
    assert m_exp.value == 0.002
    assert m_exp.effective_date == date(2026, 8, 15)
    assert m_exp.date_source_depth == 0

    # Total expense ratio
    ter = by_key[("profile", "total_expense_ratio")]
    assert ter.value == 0.0025

    # Passive approach
    pas = by_key[("profile", "is_passive")]
    assert pas.value == 1.0

    # AUM with resolved currency
    aum = by_key[("profile", "total_net_assets_local")]
    assert aum.value == 100500000.0
    assert aum.code == "USD"
    assert aum.effective_date == date(2026, 7, 31)
    assert aum.date_source_depth == 3

    # Manager tenure
    ten = by_key[("profile", "manager_tenure_years")]
    assert ten.value > 0

    # Audited net expense ratio
    aud = by_key[("profile", "audited_net_expense_ratio")]
    assert aud.value == 0.0024
    assert aud.effective_date == date(2025, 12, 31)
    assert aud.date_source_depth == 2

    # Style boxes
    sb = by_key[("style_box", "mid_core")]
    assert sb.value == 1.0
    assert sb.code is None

    sb_hist = by_key[("style_box_hist", "large_value")]
    assert sb_hist.value == 1.0
    assert sb_hist.code is None

    # Unresolved currency omits AUM row
    obs_unresolved = extract_profile(
        1001,
        payload,
        created_at,
        contract_currency="EUR",  # $ with EUR -> unresolved -> omitted
        known_currencies=known,
    )
    unresolved_keys = {(obs.family, obs.metric) for obs in obs_unresolved}
    assert ("profile", "total_net_assets_local") not in unresolved_keys


def test_extract_esg():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "asOfDate": "2026-06-30",
        "coverage": 0.95,
        "content": [
            {
                "name": "TRESGS",
                "value": 72.5,
                "children": [
                    {"name": "TRESGENS", "value": 68.0},
                    {"name": "TRESGSOS", "value": 0.0},  # 0 must be kept
                ],
            }
        ],
    }

    obs_list = extract_esg(1001, payload, created_at)
    assert len(obs_list) == 4

    by_key = {(obs.family, obs.metric): obs for obs in obs_list}
    cov = by_key[("profile", "esg_coverage")]
    assert cov.value == 0.95
    assert cov.effective_date == date(2026, 6, 30)

    score = by_key[("esg", "tresgs")]
    assert score.value == 72.5

    env = by_key[("esg", "tresgens")]
    assert env.value == 68.0

    soc = by_key[("esg", "tresgsos")]
    assert soc.value == 0.0

    # Unknown pillar raises
    bad_payload = {
        "asOfDate": "2026-06-30",
        "content": [{"name": "UNKNOWN_PILLAR", "value": 50.0}],
    }
    with pytest.raises(ValueError, match="Unrecognized ESG metric"):
        extract_esg(1001, bad_payload, created_at)


def test_extract_mstar():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "as_of_date": "2026-07-31",
        "summary": [
            {"id": "category", "value": "US Fund Large Blend"},  # Ignored
            {"id": "quantitative_rating", "value": "Silver", "publish_date": "2026-07-15"},
            {"id": "q_people", "value": "Above Average", "publish_date": "2026-07-10"},
            {"id": "process", "value": "High", "publish_date": "2026-07-12"},
            {"id": "parent", "value": "Average"},
            {"id": "morningstar_rating", "value": "4"},
            {"id": "sustainability_rating", "value": "5"},
        ],
    }

    obs_list = extract_mstar(1001, payload, created_at)
    by_key = {(obs.family, obs.metric): obs for obs in obs_list}

    # Coverage: 3 pillars present (people, process, parent) -> 3/3 = 1.0
    cov = by_key[("profile", "mstar_coverage")]
    assert cov.value == 1.0

    # Medalist rating (quantitative_rating mapped to medalist_rating)
    med = by_key[("mstar", "medalist_rating")]
    assert med.value == 4.0
    assert med.effective_date == date(2026, 7, 15)
    assert med.date_source_depth == 3

    # People (q_ stripped, no quant suffix)
    peop = by_key[("mstar", "people")]
    assert peop.value == 4.0
    assert peop.effective_date == date(2026, 7, 10)

    # Process
    proc = by_key[("mstar", "process")]
    assert proc.value == 5.0

    # Parent (inherits payload date)
    parent = by_key[("mstar", "parent")]
    assert parent.value == 3.0
    assert parent.effective_date == date(2026, 7, 31)

    # Stars
    stars = by_key[("mstar", "morningstar_rating")]
    assert stars.value == 4.0

    sust = by_key[("mstar", "sustainability_rating")]
    assert sust.value == 5.0

    # Unknown rating raises
    bad_payload = {
        "as_of_date": "2026-07-31",
        "summary": [{"id": "process", "value": "Super Duper"}],
    }
    with pytest.raises(ValueError, match="Unrecognized rating string"):
        extract_mstar(1001, bad_payload, created_at)


def test_extract_lipper():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "universes": [
            {
                "name": "United States",
                "as_of_date": "2026-07-31",
                "overall": [
                    {
                        "name_tag": "Total_Return",
                        "rating": {"value": 5, "name": "5 (1500 funds)"},
                    }
                ],
                "3_year": [
                    {
                        "name_tag": "Consistent_Return",
                        "rating": {"value": 4, "name": "4 (1200 funds)"},
                    }
                ],
            }
        ]
    }

    obs_list = extract_lipper(1001, payload, created_at)
    assert len(obs_list) == 2

    by_metric = {obs.metric: obs for obs in obs_list}
    tot = by_metric["total_return_overall"]
    assert tot.family == "lipper"
    assert tot.value == 5.0
    assert tot.effective_date == date(2026, 7, 31)
    assert tot.date_source_depth == 2

    cons = by_metric["consistent_return_3yr"]
    assert cons.family == "lipper"
    assert cons.value == 4.0


def test_extract_holdings():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "as_of_date": "2026-07-31",
        "top_10_weight": "32.5%",
        "allocation_self": [
            {"name": "Equity", "weight": 80.0},
            {"name": "Cash", "weight": 10.0},
            {"name": "Other", "weight": 10.0},  # Residual -> dropped
        ],
        "investor_country": [
            {"name": "United States", "country_code": "US", "weight": 70.0},
            {"name": "Croatia", "weight": 5.0},  # Remapped to HR
            {"name": "Unidentified", "weight": 5.0},  # Residual -> dropped
        ],
        "industry": [
            {"name": "Technology", "weight": 40.0},
            {
                "name": "Telecommunication Services-Discontinued eff 09/19/2020",
                "weight": 10.0,
            },  # Remapped to communication_services
            {"name": "Non Classified Equity", "weight": 10.0},  # Residual -> dropped
        ],
        "debtor": [
            {"name": "% Quality/AAA", "weight": 20.0},
            {"name": "% Quality Not Rated", "weight": 5.0},  # Residual -> dropped
        ],
        "maturity": [
            {"name": "% Maturity Less than 1 Year", "weight": 15.0},
            {"name": "% Maturity Other", "weight": 5.0},  # Residual -> dropped
        ],
        # debt_type must NOT be extracted
        "debt_type": [
            {"name": "Corporate Bond", "code": "corp", "weight": 40.0},
        ],
    }

    obs_list = extract_holdings(1001, payload, created_at)
    by_key = {(obs.family, obs.metric): obs for obs in obs_list}

    # Top 10 weight in profile
    top10 = by_key[("profile", "top_10_weight")]
    assert top10.value == pytest.approx(0.325)

    # Asset class: 80 equity + 10 cash + 10 other = 100. Survivors: equity 0.80, cash 0.10. Other dropped.
    assert pytest.approx(by_key[("asset_class", "equity")].value) == 0.80
    assert pytest.approx(by_key[("asset_class", "cash")].value) == 0.10
    assert ("asset_class", "other") not in by_key

    # Country: 70 US + 5 Croatia + 5 Unidentified = 80 total.
    # US = 70/80 = 0.875, Croatia = 5/80 = 0.0625. Unidentified dropped.
    us = by_key[("country", "united_states")]
    assert us.code == "US"
    assert pytest.approx(us.value) == 70.0 / 80.0
    hr = by_key[("country", "croatia")]
    assert hr.code == "HR"
    assert pytest.approx(hr.value) == 5.0 / 80.0
    assert ("country", "unidentified") not in by_key

    # Industry: 40 tech + 10 telecom + 10 non-classified = 60 total.
    # Tech = 40/60, Comm = 10/60. Non-classified dropped.
    assert pytest.approx(by_key[("industry", "technology")].value) == 40.0 / 60.0
    assert pytest.approx(by_key[("industry", "communication_services")].value) == 10.0 / 60.0
    assert ("industry", "non_classified_equity") not in by_key

    # Credit: 20 AAA + 5 Not Rated = 25 total. AAA = 20/25 = 0.8.
    assert pytest.approx(by_key[("credit_rating", "aaa")].value) == 20.0 / 25.0
    assert ("credit_rating", "not_rated") not in by_key

    # Maturity: 15 less than 1 year + 5 other = 20 total.
    assert pytest.approx(by_key[("maturity", "maturity_less_than_1_year")].value) == 15.0 / 20.0
    assert ("maturity", "maturity_other") not in by_key

    # debt_type is strictly NOT extracted
    assert not any(obs.family == "debt_type" for obs in obs_list)


def test_extract_theme_weights():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "coverage": 0.84,
        "themes": [
            {
                "key": "006a0c27-4a9a-4766-8988-0d8acc6ede8b",
                "name": "Discount Retail",
                "weight": 0.084085,
                "rank_adjusted_weight": 0.0094658,
            }
        ],
    }

    obs_list = extract_theme_weights(1001, payload, created_at)
    by_key = {(obs.family, obs.metric): obs for obs in obs_list}

    # Coverage in profile
    cov = by_key[("profile", "theme_coverage")]
    assert cov.value == 0.84

    # Dual theme families unscaled
    th = by_key[("theme", "discount_retail")]
    assert th.code == "006a0c27-4a9a-4766-8988-0d8acc6ede8b"
    assert th.value == 0.084085

    rank_th = by_key[("rank_adj_theme", "discount_retail")]
    assert rank_th.code == "006a0c27-4a9a-4766-8988-0d8acc6ede8b"
    assert rank_th.value == 0.0094658


@pytest.mark.parametrize(
    "fixture_name, extractor, expect_observations",
    [
        ("ratios_complete", extract_ratios, True),
        ("ratios_equity", extract_ratios, True),
        ("ratios_bond", extract_ratios, True),
        ("ratios_empty", extract_ratios, False),
        ("profile_complete", extract_profile, True),
        ("profile_equity", extract_profile, True),
        ("profile_bond", extract_profile, True),
        ("profile_empty", extract_profile, False),
        ("esg", extract_esg, True),
        ("mstar_equity", extract_mstar, True),
        ("mstar_bond", extract_mstar, True),
        ("mstar_empty", extract_mstar, False),
        ("lipper_equity", extract_lipper, True),
        ("lipper_bond", extract_lipper, True),
        ("lipper_empty", extract_lipper, False),
        ("holdings_complete", extract_holdings, True),
        ("holdings_equity", extract_holdings, True),
        ("holdings_bond", extract_holdings, True),
        ("holdings_empty", extract_holdings, False),
        ("theme_weights", extract_theme_weights, True),
    ],
)
def test_extractors_against_payload_fixtures(fixture_name, extractor, expect_observations):
    payload = load_fixture(fixture_name)
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    obs_list = extractor(
        8335,
        payload,
        created_at,
        contract_currency="USD",
        known_currencies={"USD", "CAD", "EUR", "GBP"},
    )

    if expect_observations:
        assert len(obs_list) > 0, f"Expected observations for {fixture_name}"
        for obs in obs_list:
            assert obs.product_id == 8335
            assert obs.family
            assert obs.metric
            assert obs.effective_date <= obs.fetched_at.date()
            assert not (math.isnan(obs.value) or math.isinf(obs.value))
            assert obs.family != "debt_type"
    else:
        assert len(obs_list) == 0, f"Expected no observations for {fixture_name}"
