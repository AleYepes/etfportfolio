import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import duckdb
import pytest

from etfportfolio.core.config import settings
from etfportfolio.core.db import apply_schema

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict | list:
    """Load and parse a JSON fixture from tests/fixtures/<name>.json."""
    fixture_path = FIXTURES_DIR / f"{name}.json"
    if not fixture_path.exists():
        raise FileNotFoundError(f"Fixture not found: {fixture_path}")
    with fixture_path.open("rb") as f:
        return json.load(f)


@pytest.fixture(autouse=True)
def isolate_test_environment(tmp_path, monkeypatch):
    """Guarantees tests do not touch live user sessions, network, or .env files."""
    test_session_file = tmp_path / "test_session_state.json"
    monkeypatch.setattr(settings, "session_state_path", str(test_session_file))
    monkeypatch.setattr(settings, "account_id", "U123456")

    # Prevent Playwright browser login from accidentally executing in tests
    with patch("etfportfolio.ingest.session.login", new_callable=AsyncMock):
        yield


@pytest.fixture
def db_conn():
    """Provides a clean in-memory DuckDB connection with schema applied."""
    conn = duckdb.connect(":memory:")
    apply_schema(conn)
    return conn
