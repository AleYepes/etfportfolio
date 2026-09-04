from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    PyprojectTomlConfigSettingsSource,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        pyproject_toml_table_header=("tool", "etfportfolio"),
        env_file=".env",
        extra="ignore",
    )

    db_path: str = "data/etf.duckdb"
    data_dir: str = "data"
    session_state_path: str = "data/session_state.json"
    log_dir: str = "data/logs"
    ibkr_base_url: str = "https://www.interactivebrokers.ie"
    details_concurrency: int = 10
    freshness_window_hours: float = 24.0

    # IB Gateway
    ib_gateway_host: str = "127.0.0.1"
    ib_gateway_port: int = 4001
    ib_gateway_timeout: float = 60.0
    # client IDs are hardcoded per phase (1 for contracts, 2 for prices)

    ibkr_username: str | None = None
    ibkr_password: str | None = None
    account_id: str | None = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            PyprojectTomlConfigSettingsSource(settings_cls),
        )


settings = Settings()
