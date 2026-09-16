import httpx
import pytest
import respx

from etfportfolio.core.config import settings
from etfportfolio.ingest.session import fetch_with_retry, reconcile_account_id


@pytest.mark.anyio
async def test_fetch_with_retry_404_no_retry():
    with respx.mock(base_url="https://test.ibkr.com") as respx_mock:
        route = respx_mock.get("/endpoint-404").respond(404, json={"error": "not found"})

        async with httpx.AsyncClient() as client:
            status, payload = await fetch_with_retry(
                client,
                "https://test.ibkr.com/endpoint-404",
                max_retries=3,
            )

        assert status == 404
        assert payload is None
        # Must only call ONCE, without retries
        assert route.call_count == 1


@pytest.mark.anyio
async def test_fetch_with_retry_200_success():
    with respx.mock(base_url="https://test.ibkr.com") as respx_mock:
        respx_mock.get("/endpoint-200").respond(200, json={"result": "ok"})

        async with httpx.AsyncClient() as client:
            status, payload = await fetch_with_retry(
                client,
                "https://test.ibkr.com/endpoint-200",
            )

        assert status == 200
        assert payload == {"result": "ok"}


def test_reconcile_account_id(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("ACCOUNT_ID=U123456\n", encoding="utf-8")
    settings.account_id = "U123456"

    # Same account ID: returns True, does not rewrite
    assert reconcile_account_id("U123456", env_path=env_file) is True
    assert env_file.read_text(encoding="utf-8") == "ACCOUNT_ID=U123456\n"
