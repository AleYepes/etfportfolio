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
    sanitize_metric_id,
)


def test_utils():
    fallback = date(2026, 9, 1)
    assert parse_effective_date(None, fallback) == fallback
    assert parse_effective_date(0, fallback) == fallback
    assert parse_effective_date(-100, fallback) == fallback
    assert parse_effective_date("", fallback) == fallback
    assert parse_effective_date("invalid", fallback) == fallback
    # Epoch ms: 1785470400000 -> 2026-07-31
    assert parse_effective_date(1785470400000, fallback) == date(2026, 7, 31)
    # YYYYMMDD
    assert parse_effective_date("20260831", fallback) == date(2026, 8, 31)
    # YYYY-MM-DD
    assert parse_effective_date("2026-08-15", fallback) == date(2026, 8, 15)
    # YYYY/MM/DD
    assert parse_effective_date("2026/08/20", fallback) == date(2026, 8, 20)

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

    rows = extract_ratios(1001, payload, created_at)
    assert len(rows) == 5

    eff_date = date(2026, 7, 31)
    expected_metrics = {
        "dividend_yield_weighted_average": 3.3433,
        "sales_growth_5_yr": 11.352,
        "yield_to_maturity": 4.5,
        "price_earnings": 38.15,
        "latest_composite_z_score": -0.18,
    }

    for r in rows:
        pid, source, metric_id, eff, snap_time, val = r
        assert pid == 1001
        assert source == "ratios"
        assert eff == eff_date
        assert snap_time == created_at
        assert val == expected_metrics[metric_id]

    # Empty payload returns []
    assert extract_ratios(1001, {}, created_at) == []


def test_extract_profile():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "expenses_allocation": [
            {"name": "Management Expenses", "ratio": 0.85},
            {"name": "Non-Management Expenses", "ratio": 0.15},
        ],
        "fund_and_profile": [
            {"name": "Total Expense Ratio", "name_tag": "Total_Expense_Ratio", "value": "0.32%"},
            {"name": "Management Approach", "name_tag": "Management_Approach", "value": "Passive"},
            {"name": "Inception Date", "name_tag": "Inception_Date", "value": "2001/01/29"},
        ],
        "mstar": {
            "name": "Large-Cap Growth Funds",
            "selected": [[0, 2]],
            "x_axis": ["Core", "Growth", "Value"],
            "y_axis": ["Large", "Mid", "Multi", "Small"],
        },
    }

    rows = extract_profile(1001, payload, created_at)
    # 2 expenses + 2 fund_and_profile + 2 mstar style box = 6 metrics
    assert len(rows) == 6

    eff_date = created_at.date()
    for r in rows:
        assert r[3] == eff_date

    metrics = {r[2]: r[5] for r in rows}
    assert metrics["management_expense_ratio"] == 0.85
    assert metrics["non_management_expense_ratio"] == 0.15
    assert pytest.approx(metrics["total_expense_ratio"]) == 0.0032
    assert metrics["is_passive"] == 1.0
    assert metrics["mstar_style_size"] == 3.0  # Large
    assert metrics["mstar_style_value"] == 3.0  # Growth

    # Active fund test
    active_payload = {
        "fund_and_profile": [
            {"name_tag": "Management_Approach", "value": "Active"},
        ]
    }
    active_rows = extract_profile(1001, active_payload, created_at)
    assert len(active_rows) == 1
    assert active_rows[0][2] == "is_passive"
    assert active_rows[0][5] == 0.0

    # Empty payload returns []
    assert extract_profile(1001, {}, created_at) == []


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

    rows = extract_esg(1001, payload, created_at)
    assert len(rows) == 5

    eff_date = date(2026, 8, 22)
    metrics = {r[2]: r[5] for r in rows}
    assert metrics["esg_coverage"] == 0.99762
    assert metrics["tresgs"] == 6.0
    assert metrics["tresgens"] == 7.0
    assert metrics["tresgeners"] == 8.0
    assert metrics["tresgenpis"] == 5.0

    for r in rows:
        assert r[0] == 1001
        assert r[1] == "esg"
        assert r[3] == eff_date

    assert extract_esg(1001, {}, created_at) == []


def test_extract_mstar():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "as_of_date": "20260831",
        "summary": [
            {"id": "category", "value": "Real Estate"},  # non-rating, should be skipped
            {"id": "medalist_rating", "publish_date": "20260731", "value": "Neutral"},
            {"id": "morningstar_rating", "publish_date": "20260831", "value": "3"},
            {"id": "parent", "publish_date": "20260731", "value": "Above_Average"},
            {"id": "people", "publish_date": "20260731", "value": "High"},
            {"id": "q_process", "publish_date": "20260731", "value": "Below_Average"},
            {"id": "process", "value": "Not_Applicable"},  # should be skipped gracefully
            {"id": "sustainability_rating", "publish_date": "20260630", "value": "Average"},
            {"id": "quantitative_rating", "publish_date": "20221130", "value": "Gold"},
        ],
    }

    rows = extract_mstar(1001, payload, created_at)
    assert len(rows) == 7

    metrics = {r[2]: (r[3], r[5]) for r in rows}
    assert metrics["medalist_rating"] == (date(2026, 7, 31), 2.0)  # Neutral = 2.0
    assert metrics["morningstar_rating"] == (date(2026, 8, 31), 3.0)  # 3 = 3.0
    assert metrics["parent"] == (date(2026, 7, 31), 4.0)  # Above_Average = 4.0
    assert metrics["people"] == (date(2026, 7, 31), 5.0)  # High = 5.0
    assert metrics["q_process"] == (date(2026, 7, 31), 2.0)  # Below_Average = 2.0
    assert metrics["sustainability_rating"] == (date(2026, 6, 30), 3.0)  # Average = 3.0
    assert metrics["quantitative_rating"] == (date(2022, 11, 30), 5.0)  # Gold = 5.0

    # Test unrecognized rating raises ValueError
    bad_payload = {"summary": [{"id": "people", "value": "SuperDuper"}]}
    with pytest.raises(ValueError, match="Unrecognized rating string"):
        extract_mstar(1001, bad_payload, created_at)

    # Test missing id raises ValueError
    missing_id_payload = {"summary": [{"value": "High"}]}
    with pytest.raises(ValueError, match="Missing 'id'"):
        extract_mstar(1001, missing_id_payload, created_at)

    assert extract_mstar(1001, {}, created_at) == []


def test_extract_lipper():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    # Two universes with same date (should be averaged) and one historical date
    payload = {
        "universes": [
            {
                "name": "United States",
                "as_of_date": 1785470400000,  # 2026-07-31
                "overall": [
                    {"name_tag": "consistent_return", "rating": {"value": 4}},
                    {"name_tag": "preservation", "rating": {"value": 2}},
                ],
                "3_year": [
                    {"name_tag": "total_return", "rating": {"value": 5}},
                ],
            },
            {
                "name": "Peru",
                "as_of_date": 1785470400000,  # same date
                "overall": [
                    {"name_tag": "consistent_return", "rating": {"value": 2}},  # avg with 4 -> 3.0
                    {"name_tag": "preservation", "rating": {"value": 4}},  # avg with 2 -> 3.0
                ],
                "3_year": [
                    {"name_tag": "total_return", "rating": {"value": 3}},  # avg with 5 -> 4.0
                ],
            },
            {
                "name": "Chile",
                "as_of_date": 1600000000000,  # 2020-09-13
                "overall": [
                    {"name_tag": "preservation", "rating": {"value": 1}},
                ],
            },
        ]
    }

    rows = extract_lipper(1001, payload, created_at)
    # Date 1 (2026-07-31): consistent_return_overall, preservation_overall, total_return_3yr
    # Date 2 (2020-09-13): preservation_overall
    # Total: 4 rows
    assert len(rows) == 4

    d1 = date(2026, 7, 31)
    d2 = date(2020, 9, 13)

    metrics_d1 = {r[2]: r[5] for r in rows if r[3] == d1}
    assert metrics_d1["consistent_return_overall"] == 3.0
    assert metrics_d1["preservation_overall"] == 3.0
    assert metrics_d1["total_return_3yr"] == 4.0

    metrics_d2 = {r[2]: r[5] for r in rows if r[3] == d2}
    assert metrics_d2["preservation_overall"] == 1.0

    assert extract_lipper(1001, {}, created_at) == []


def test_extract_holdings():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "as_of_date": 1785470400000,  # 2026-07-31
        "allocation_self": [
            {"name": "Equity", "weight": 99.76},
            {"name": "Cash", "weight": 0.20},
            {"name": "Other", "weight": -0.02},
            {"name": "Other", "weight": 0.06},  # duplicate "Other", should sum to 0.04% -> 0.0004
        ],
        "currency": [
            {"name": "US Dollar", "code": "USD", "weight": 99.8},
            {"name": "<No Currency>", "weight": 0.2},  # should map to 'Unassigned'
        ],
        "investor_country": [
            {"name": "United States", "country_code": "US", "weight": 100.0},
        ],
        "debtor": [
            {"name": "% Quality/AAA", "weight": 1.5},
            {"name": "% Quality Not Rated", "weight": 0.5},
        ],
    }

    rows = extract_holdings(1001, payload, created_at)
    eff_date = date(2026, 7, 31)

    # 3 unique asset_class + 2 currency + 1 country + 2 debtor = 8 rows
    assert len(rows) == 8

    by_key = {(r[1], r[2]): (r[3], r[6]) for r in rows}

    # Asset class
    assert pytest.approx(by_key[("asset_class", "Equity")][1]) == 0.9976
    assert pytest.approx(by_key[("asset_class", "Cash")][1]) == 0.0020
    assert pytest.approx(by_key[("asset_class", "Other")][1]) == 0.0004  # -0.0002 + 0.0006

    # Currency
    assert by_key[("currency", "US Dollar")][0] == "USD"
    assert pytest.approx(by_key[("currency", "US Dollar")][1]) == 0.998
    assert by_key[("currency", "Unassigned")][0] is None
    assert pytest.approx(by_key[("currency", "Unassigned")][1]) == 0.002

    # Country
    assert by_key[("country", "United States")][0] == "US"
    assert pytest.approx(by_key[("country", "United States")][1]) == 1.0

    # Credit rating
    assert by_key[("credit_rating", "AAA")][0] == "AAA"
    assert pytest.approx(by_key[("credit_rating", "AAA")][1]) == 0.015
    assert by_key[("credit_rating", "Not Rated")][0] == "Not Rated"
    assert pytest.approx(by_key[("credit_rating", "Not Rated")][1]) == 0.005

    for r in rows:
        assert r[0] == 1001
        assert r[4] == eff_date

    assert extract_holdings(1001, {}, created_at) == []


def test_extract_theme_weights():
    created_at = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    payload = {
        "themes": [
            {
                "key": "006a0c27-4a9a-4766-8988-0d8acc6ede8b",
                "name": "Discount Retail",
                "weight": 0.084085,
                "rank_adjusted_weight": 0.0094658,
            },
            {
                "key": "00c23a97-6299-462b-bec4-4dfec9639ed9",
                "name": "Last-mile Logistics",
                "weight": 0.094797,
                "rank_adjusted_weight": 0.080694,
            },
        ]
    }

    rows = extract_theme_weights(1001, payload, created_at)
    assert len(rows) == 2

    r1 = rows[0]
    assert r1[0] == 1001
    assert r1[1] == "006a0c27-4a9a-4766-8988-0d8acc6ede8b"
    assert r1[2] == created_at.date()
    assert r1[3] == created_at
    assert r1[4] == 0.084085
    assert r1[5] == 0.0094658

    assert extract_theme_weights(1001, {}, created_at) == []
