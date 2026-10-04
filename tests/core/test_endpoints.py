from __future__ import annotations

from etfportfolio.core.endpoints import (
    ALL_RATIOS_METRICS,
    ANNUAL_REPORT_METRICS,
    ASSET_CLASS_METRICS,
    CREDIT_RATING_METRICS,
    DEFAULT_ZERO_FAMILIES,
    DETAILS_ENDPOINTS,
    ENDPOINTS,
    ENDPOINTS_BY_NAME,
    ESG_METRICS,
    FUND_PROFILE_REDEMPTION_METRICS,
    GATED_ENDPOINTS,
    INDUSTRY_METRICS,
    MATURITY_METRICS,
    MSTAR_METRICS,
    PROFILE_METRICS,
    PROSPECTUS_REPORT_METRICS,
    RATIOS_PERCENTAGE_METRICS,
    RATIOS_STANDARD_METRICS,
    SCALAR_FAMILIES,
    STYLE_CELLS,
    UNGATED_ENDPOINTS,
)


def test_endpoints_registry():
    expected_names = {"landing", "profile", "ratios", "holdings", "mstar", "esg", "lipper", "theme_weights"}
    assert set(ENDPOINTS_BY_NAME.keys()) == expected_names
    assert len(ENDPOINTS) == 8
    assert "landing" in ENDPOINTS_BY_NAME
    assert len(DETAILS_ENDPOINTS) == 7
    assert len(GATED_ENDPOINTS) == 5
    assert len(UNGATED_ENDPOINTS) == 2
    assert {ep.name for ep in GATED_ENDPOINTS} == {"holdings", "ratios", "profile", "lipper", "mstar"}
    assert {ep.name for ep in UNGATED_ENDPOINTS} == {"esg", "theme_weights"}
    assert ENDPOINTS_BY_NAME["landing"].gated is False


def test_endpoint_resolve():
    ep = ENDPOINTS_BY_NAME["ratios"]
    prefix, slug, full_url = ep.resolve(product_id=8335)
    assert prefix == "/tws.proxy/fundamentals/mf_ratios_fundamentals/"
    assert slug == "8335?lang=en"
    assert full_url == "/tws.proxy/fundamentals/mf_ratios_fundamentals/8335?lang=en"

    # With account_id
    esg_ep = ENDPOINTS_BY_NAME["esg"]
    prefix, slug, full_url = esg_ep.resolve(product_id=8335, account_id="U123456")
    assert "accounts=U123456" in slug


def test_family_classifications():
    assert "debt_type" not in DEFAULT_ZERO_FAMILIES
    assert "debt_type" not in SCALAR_FAMILIES
    assert len(DEFAULT_ZERO_FAMILIES & SCALAR_FAMILIES) == 0
    assert "asset_class" in DEFAULT_ZERO_FAMILIES
    assert "ratios" in SCALAR_FAMILIES
    assert len(STYLE_CELLS) == 12


def test_simplex_maps_residuals_flagged():
    # Asset class residuals
    assert ASSET_CLASS_METRICS["other"] is True
    assert ASSET_CLASS_METRICS["equity"] is False
    assert ASSET_CLASS_METRICS["fixed_income"] is False
    assert ASSET_CLASS_METRICS["cash"] is False

    # Industry residuals
    assert INDUSTRY_METRICS["non_classified_equity"] is True
    assert INDUSTRY_METRICS["not_classified_non_equity"] is True
    assert INDUSTRY_METRICS["technology"] is False

    # Credit rating residuals
    assert CREDIT_RATING_METRICS["not_rated"] is True
    assert CREDIT_RATING_METRICS["not_available"] is True
    assert CREDIT_RATING_METRICS["aaa"] is False

    # Maturity residuals
    assert MATURITY_METRICS["maturity_other"] is True
    assert MATURITY_METRICS["maturity_less_than_1_year"] is False


def test_scalar_metric_sets():
    assert ALL_RATIOS_METRICS == (RATIOS_PERCENTAGE_METRICS | RATIOS_STANDARD_METRICS)
    assert len(RATIOS_PERCENTAGE_METRICS & RATIOS_STANDARD_METRICS) == 0
    assert "eps_growth_1yr" in RATIOS_PERCENTAGE_METRICS
    assert "price_earnings" in RATIOS_STANDARD_METRICS
    assert "tresgs" in ESG_METRICS
    assert "esg_coverage" not in ESG_METRICS
    assert "people" in MSTAR_METRICS
    assert "mstar_coverage" in PROFILE_METRICS


def test_profile_metrics_membership():
    new_metrics = {
        "audited_gross_expense_ratio",
        "audited_management_fee_ratio",
        "audited_non_management_fee_ratio",
        "prospectus_net_expense_ratio",
        "prospectus_gross_expense_ratio",
        "prospectus_net_management_fee_ratio",
        "prospectus_gross_management_fee_ratio",
        "prospectus_fee_waiver_ratio",
        "prospectus_net_12b1_fee_ratio",
        "prospectus_gross_12b1_fee_ratio",
        "redemption_charge_max",
        "redemption_charge_actual",
    }
    assert len(PROFILE_METRICS) == 23
    assert new_metrics.issubset(PROFILE_METRICS)

    for metric in ANNUAL_REPORT_METRICS.values():
        assert metric in PROFILE_METRICS

    for metric in PROSPECTUS_REPORT_METRICS.values():
        assert metric in PROFILE_METRICS

    for metric in FUND_PROFILE_REDEMPTION_METRICS.values():
        assert metric in PROFILE_METRICS
