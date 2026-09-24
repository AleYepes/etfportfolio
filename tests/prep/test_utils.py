from __future__ import annotations

from datetime import date

import pytest

from etfportfolio.ingest.utils import content_address
from etfportfolio.prep.utils import (
    DateContext,
    Observation,
    clean_credit_rating,
    clean_simplex,
    decompress_payload,
    disambiguate_aum_currency,
    parse_manager_tenure,
    parse_net_assets,
    parse_percentage,
    to_lower_snake_case,
)


def test_to_lower_snake_case():
    assert to_lower_snake_case("Price/Earnings") == "price_earnings"
    assert to_lower_snake_case("Research & Development") == "research_and_development"
    assert to_lower_snake_case("  Sales Growth 5 Yr  ") == "sales_growth_5_yr"
    assert to_lower_snake_case("% Quality/AAA") == "quality_aaa"
    assert to_lower_snake_case("TRESGS") == "tresgs"


def test_date_context():
    snap_date = date(2024, 5, 1)
    ctx = DateContext(snap_date)

    assert ctx.current.effective_date == snap_date
    assert ctx.current.depth == 0

    # Payload scope
    with ctx.scope("2024-04-30", 1):
        assert ctx.current.effective_date == date(2024, 4, 30)
        assert ctx.current.depth == 1

        # Container scope with epoch millis
        with ctx.scope(1713139200000, 2):  # 2024-04-15
            assert ctx.current.effective_date == date(2024, 4, 15)
            assert ctx.current.depth == 2

            # Future date must be ignored (causality bound against snapshot_date)
            with ctx.scope("2024-06-01", 3):
                assert ctx.current.effective_date == date(2024, 4, 15)
                assert ctx.current.depth == 2

            # 8-digit date
            with ctx.scope("20240401", 3):
                assert ctx.current.effective_date == date(2024, 4, 1)
                assert ctx.current.depth == 3

        # Reverts to depth 1
        assert ctx.current.effective_date == date(2024, 4, 30)
        assert ctx.current.depth == 1

    # Reverts to depth 0
    assert ctx.current.effective_date == snap_date
    assert ctx.current.depth == 0


def test_clean_simplex():
    # Example from Worked Example 1: Equity 10, Other 90
    raw_items = [
        {"name": "Equity", "weight": 10.0},
        {"name": "Other", "weight": 90.0},
    ]
    metric_map = {"equity": False, "other": True}
    res = clean_simplex(
        raw_items,
        name_extractor=lambda x: x["name"],
        weight_extractor=lambda x: x["weight"],
        metric_map=metric_map,
    )
    assert len(res) == 1
    metric, code, val, raw = res[0]
    assert metric == "equity"
    assert code is None
    assert pytest.approx(val) == 0.10

    # Negative clipping
    raw_items_neg = [
        {"name": "Equity", "weight": 100.0},
        {"name": "Cash", "weight": -10.0},
    ]
    metric_map_ac = {"equity": False, "cash": False, "other": True}
    res_neg = clean_simplex(
        raw_items_neg,
        name_extractor=lambda x: x["name"],
        weight_extractor=lambda x: x["weight"],
        metric_map=metric_map_ac,
    )
    assert len(res_neg) == 2
    assert pytest.approx(res_neg[0][2]) == 1.0
    assert pytest.approx(res_neg[1][2]) == 0.0

    # Zero total -> empty
    raw_zero = [{"name": "Equity", "weight": 0.0}]
    assert clean_simplex(raw_zero, lambda x: x["name"], lambda x: x["weight"], metric_map) == []

    # Closed vocab error
    raw_unknown = [{"name": "Cryptocurrency", "weight": 50.0}]
    with pytest.raises(ValueError, match="Unrecognized metric 'cryptocurrency' in simplex"):
        clean_simplex(raw_unknown, lambda x: x["name"], lambda x: x["weight"], metric_map, open_vocab=False)

    # Open vocab with residual
    raw_country = [
        {"country": "United States", "code": "US", "w": 80.0},
        {"country": "Unidentified", "code": None, "w": 20.0},
    ]
    res_country = clean_simplex(
        raw_country,
        name_extractor=lambda x: x["country"],
        weight_extractor=lambda x: x["w"],
        metric_map=None,
        code_extractor=lambda x: x["code"],
        open_vocab=True,
        residual_names={"unidentified"},
    )
    assert len(res_country) == 1
    assert res_country[0][0] == "united_states"
    assert res_country[0][1] == "US"
    assert pytest.approx(res_country[0][2]) == 0.80


def test_disambiguate_aum_currency():
    known = {"USD", "CAD", "EUR", "GBP", "JPY", "AUD", "CHF"}

    # Leading ISO-3 token
    assert disambiguate_aum_currency("CAD 500M", "USD", known) == "CAD"
    assert disambiguate_aum_currency("AUD1.5B", "USD", known) == "AUD"
    assert disambiguate_aum_currency("XYZ 100M", "USD", known) is None

    # Symbol match against contract currency
    assert disambiguate_aum_currency("$78.63B (2026/07/31)", "USD", known) == "USD"
    assert disambiguate_aum_currency("$10.0M", "CAD", known) == "CAD"
    assert disambiguate_aum_currency("€2.5B", "EUR", known) == "EUR"
    assert disambiguate_aum_currency("£1.2B", "GBP", known) == "GBP"

    # Worked Example 8: $10M with contract currency EUR -> None (EUR not in $ currencies)
    assert disambiguate_aum_currency("$10.0M", "EUR", known) is None

    # Contract currency direct match if no symbol
    assert disambiguate_aum_currency("1000000", "USD", known) == "USD"
    assert disambiguate_aum_currency("1000000", "XYZ", known) is None


def test_parse_net_assets():
    aum_us = parse_net_assets("$78.63B (2026/07/31)")
    assert aum_us is not None
    assert aum_us[0] == 78630000000.0
    assert aum_us[1] == "$78.63B (2026/07/31)"
    assert aum_us[2] == "2026/07/31"

    aum_eu = parse_net_assets("2,5B")
    assert aum_eu is not None
    assert aum_eu[0] == 2500000000.0
    assert aum_eu[2] is None

    aum_thousands = parse_net_assets("1,250.5M")
    assert aum_thousands is not None
    assert aum_thousands[0] == 1250500000.0

    aum_zero = parse_net_assets("CAD0 (2020/08/31)")
    assert aum_zero is not None
    assert aum_zero[0] == 0.0
    assert aum_zero[2] == "2020/08/31"

    aum_micro = parse_net_assets("$19.08")
    assert aum_micro is not None
    assert aum_micro[0] == 19.08

    assert parse_net_assets(None) is None
    assert parse_net_assets("") is None
    assert parse_net_assets("N/A") is None
    assert parse_net_assets("abc") is None


def test_parse_manager_tenure():
    ref_date = date(2026, 8, 15)
    tenure = parse_manager_tenure("2013/01/01", ref_date=ref_date)
    assert tenure is not None
    expected_years = round((ref_date - date(2013, 1, 1)).days / 365.25, 4)
    assert tenure[0] == expected_years
    assert tenure[1] == "2013/01/01"

    tenure_year = parse_manager_tenure("2013", ref_date=ref_date)
    assert tenure_year is not None
    assert tenure_year[0] == round((ref_date - date(2013, 1, 1)).days / 365.25, 4)

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


def test_clean_credit_rating():
    assert clean_credit_rating("% Quality/AAA") == "AAA"
    assert clean_credit_rating("% Quality/BBB") == "BBB"
    assert clean_credit_rating("% Quality Not Rated") == "Not Rated"


def test_decompress_payload():
    data = {"hello": "world", "num": 123, "arr": [1, 2, 3]}
    _, comp = content_address(data)
    decomp = decompress_payload(comp)
    assert decomp == data


def test_observation_to_row():
    obs = Observation(
        product_id=1,
        family="asset_class",
        metric="equity",
        code=None,
        effective_date=date(2024, 1, 1),
        date_source_depth=1,
        fetched_at=date(2024, 1, 2),
        value=0.8,
        raw_value="80%",
    )
    row = obs.to_row()
    assert row == (1, "asset_class", "equity", None, date(2024, 1, 1), 1, date(2024, 1, 2), 0.8, "80%")
