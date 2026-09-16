from etfportfolio.core.config import Settings


def test_settings_defaults():
    s = Settings(
        _env_file=None,
        db_path="data/etf.duckdb",
        ib_gateway_port=4001,
        details_concurrency=10,
    )
    assert s.db_path == "data/etf.duckdb"
    assert s.ib_gateway_port == 4001
    assert s.ib_gateway_host == "127.0.0.1"
    assert s.ibkr_base_url == "https://www.interactivebrokers.ie"
    assert s.details_concurrency == 10


def test_settings_env_override(monkeypatch):
    monkeypatch.setenv("ETF_DB_PATH", "custom/path.duckdb")
    monkeypatch.setenv("ACCOUNT_ID", "U999999")
    monkeypatch.setenv("IB_GATEWAY_PORT", "4002")

    s = Settings(_env_file=None)
    assert s.account_id == "U999999"
    assert s.ib_gateway_port == 4002


def test_settings_pyproject_toml_loading():
    # pyproject.toml has [tool.etfportfolio] with freshness_window_hours = 72.0, ib_gateway_timeout = 90.0
    s = Settings(_env_file=None)
    # PyprojectTomlConfigSettingsSource should read from tool.etfportfolio
    assert s.freshness_window_hours == 72.0
    assert s.ib_gateway_timeout == 90.0
    assert "SFB" in s.blocked_exchanges


def test_settings_explicit_init_overrides_all():
    s = Settings(
        _env_file=None,
        freshness_window_hours=12.0,
        blocked_exchanges=["NYSE"],
    )
    assert s.freshness_window_hours == 12.0
    assert s.blocked_exchanges == ["NYSE"]
