import json
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from etfportfolio.core.config import settings
from etfportfolio.ingestion.session import (
    _choose_account_id,
    discover_account_id,
    get_credentials,
    is_session_invalid,
    probe,
    validate_client_session,
)


def test_choose_account_id_ranking():
    assert _choose_account_id([]) is None
    assert _choose_account_id(["invalid", "abc"]) is None
    # Prefers U-prefixed account
    assert _choose_account_id(["DU9999", "U1234567"]) == "U1234567"
    assert _choose_account_id(["F123", "U555"]) == "U555"


def test_is_session_invalid_with_fixture():
    login_err_payload = json.loads(Path("tests/fixtures/login_error.json").read_text(encoding="utf-8"))
    resp = httpx.Response(400, json=login_err_payload)
    assert is_session_invalid(resp) is True

    ok_resp = httpx.Response(200, json={"status": "ok"})
    assert is_session_invalid(ok_resp) is False

    unauth_resp = httpx.Response(401, json={})
    assert is_session_invalid(unauth_resp) is True


@pytest.mark.anyio
async def test_get_credentials_from_settings(monkeypatch):
    monkeypatch.setattr(settings, "ibkr_username", "user_from_env")
    monkeypatch.setattr(settings, "ibkr_password", "pass_from_env")

    u, p = await get_credentials()
    assert u == "user_from_env"
    assert p == "pass_from_env"


@pytest.mark.anyio
async def test_get_credentials_prompts_when_missing(monkeypatch):
    monkeypatch.setattr(settings, "ibkr_username", None)
    monkeypatch.setattr(settings, "ibkr_password", None)

    import questionary

    mock_text = AsyncMock(return_value="prompted_user")
    mock_pass = AsyncMock(return_value="prompted_pass")

    monkeypatch.setattr(questionary, "text", lambda _: type("M", (), {"ask_async": mock_text}))
    monkeypatch.setattr(questionary, "password", lambda _: type("M", (), {"ask_async": mock_pass}))

    u, p = await get_credentials()
    assert u == "prompted_user"
    assert p == "prompted_pass"


@pytest.mark.anyio
@respx.mock
async def test_discover_account_id_sources(monkeypatch):
    monkeypatch.setattr(settings, "account_id", None)
    base = "https://www.interactivebrokers.ie"

    # 1. From cookie paths in state dict
    state_dict = {
        "cookies": [
            {"name": "test", "value": "1", "path": "/portal.proxy/v1/portal/portfolio2/U9876543"},
        ]
    }
    async with httpx.AsyncClient(base_url=base) as client:
        acct = await discover_account_id(client, state_dict=state_dict)
        assert acct == "U9876543"

    # 2. From OneBarAuthentication
    respx.get(f"{base}/AccountManagement/OneBarAuthentication?json=1").mock(
        return_value=httpx.Response(200, json={"mostRelevantAccount": "U1122334"})
    )
    async with httpx.AsyncClient(base_url=base) as client:
        acct = await discover_account_id(client, state_dict=None)
        assert acct == "U1122334"


@pytest.mark.anyio
@respx.mock
async def test_validate_client_session_and_probe(monkeypatch):
    monkeypatch.setattr(settings, "account_id", None)
    base = "https://www.interactivebrokers.ie"

    respx.get(f"{base}/tws.proxy/fundamentals/landing/756733?widgets=objective&lang=en").mock(
        return_value=httpx.Response(200, json={"objective": "Test"})
    )
    respx.get(f"{base}/tws.proxy/acesws/accountList").mock(
        return_value=httpx.Response(200, json={"accessibleAccounts": ["U7788990"]})
    )

    async with httpx.AsyncClient(base_url=base) as client:
        is_valid = await validate_client_session(client)
        assert is_valid is True

        acct = await probe(client)
        assert acct == "U7788990"
