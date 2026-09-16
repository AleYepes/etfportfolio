from datetime import date

import pytest

from etfportfolio.ingest.utils import content_address
from etfportfolio.prep.utils import (
    clean_credit_rating,
    decompress_payload,
    parse_effective_date,
    parse_manager_tenure,
    parse_net_assets,
    parse_percentage,
    sanitize_metric_id,
)


def test_parse_effective_date():
    fallback = date(2026, 9, 1)

    # Fallbacks
    assert parse_effective_date(None, fallback) == (fallback, "snapshot")
    assert parse_effective_date(0, fallback) == (fallback, "snapshot")
    assert parse_effective_date(-100, fallback) == (fallback, "snapshot")
    assert parse_effective_date("", fallback) == (fallback, "snapshot")
    assert parse_effective_date("invalid", fallback) == (fallback, "snapshot")

    # Successes
    assert parse_effective_date(1785470400000, fallback) == (date(2026, 7, 31), "payload")
    assert parse_effective_date(1785470400000, fallback, default_source="item") == (date(2026, 7, 31), "item")
    assert parse_effective_date("20260831", fallback) == (date(2026, 8, 31), "payload")
    assert parse_effective_date("2026-08-15", fallback) == (date(2026, 8, 15), "payload")
    assert parse_effective_date("2026/08/20", fallback) == (date(2026, 8, 20), "payload")


def test_clean_credit_rating():
    assert clean_credit_rating("% Quality/AAA") == "AAA"
    assert clean_credit_rating("% Quality/BBB") == "BBB"
    assert clean_credit_rating("% Quality/Below B") == "Below B"
    assert clean_credit_rating("% Quality Not Rated") == "Not Rated"
    assert clean_credit_rating("% Quality Not Available") == "Not Available"
    assert clean_credit_rating("A") == "A"


def test_sanitize_metric_id():
    assert sanitize_metric_id("Price/Earnings") == "price_earnings"
    assert sanitize_metric_id("Dividend_Yield_Weighted_Average") == "dividend_yield_weighted_average"
    assert sanitize_metric_id("  Sales Growth 5 Yr  ") == "sales_growth_5_yr"
    assert sanitize_metric_id("TRESGS") == "tresgs"


def test_parse_net_assets():
    fallback = date(2026, 9, 1)

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


def test_parse_manager_tenure():
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


def test_parse_percentage():
    assert parse_percentage("0.32%") == (0.0032, "0.32%")
    assert parse_percentage("<0.01%", allow_bound=True) == (0.0001, "<0.01%")
    assert parse_percentage(None) is None
    assert parse_percentage("-") is None
    with pytest.raises(ValueError, match="Cannot parse percentage"):
        parse_percentage("invalid_pct")


def test_decompress_payload():
    data = {"hello": "world", "num": 123, "arr": [1, 2, 3]}
    _, comp = content_address(data)
    decomp = decompress_payload(comp)
    assert decomp == data
