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
    endpoint_concurrency: int = 5

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
