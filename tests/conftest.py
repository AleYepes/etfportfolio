from unittest.mock import AsyncMock

import pytest

from etfportfolio.core.config import settings
from etfportfolio.ingestion import session


@pytest.fixture(autouse=True)
def isolate_test_environment(monkeypatch, tmp_path):
    """Isolates mutable settings and prevents real Playwright browser launches during tests."""
    monkeypatch.setattr(settings, "account_id", None)
    monkeypatch.setattr(settings, "session_state_path", str(tmp_path / "test_session_state.json"))
    monkeypatch.setattr(session, "login", AsyncMock(return_value=None))
