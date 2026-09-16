from etfportfolio.ingest.endpoints import (
    DETAILS_ENDPOINTS,
    ENDPOINTS,
    ENDPOINTS_BY_NAME,
    GATED_ENDPOINTS,
    UNGATED_ENDPOINTS,
)


def test_endpoints_registry():
    expected_names = {"landing", "profile", "ratios", "holdings", "mstar", "esg", "lipper", "theme_weights"}
    assert set(ENDPOINTS_BY_NAME.keys()) == expected_names
    assert len(ENDPOINTS) == len(expected_names)


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


def test_gated_and_ungated_partition():
    assert "landing" not in [ep.name for ep in DETAILS_ENDPOINTS]
    for ep in GATED_ENDPOINTS:
        assert ep.gated is True
    for ep in UNGATED_ENDPOINTS:
        assert ep.gated is False
    assert set(GATED_ENDPOINTS) | set(UNGATED_ENDPOINTS) == set(DETAILS_ENDPOINTS)
