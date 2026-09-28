import asyncio
import time
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx

from etfportfolio.core.config import settings
from etfportfolio.ingest.session import (
    RateLimiter,
    SessionInvalidError,
    fetch_with_retry,
    is_session_invalid,
    reconcile_account_id,
)


@pytest.mark.anyio
async def test_rate_limiter_wave_deduplication():
    limiter = RateLimiter(initial_delay=1.0, max_delay=60.0, cooldown_window=15.0)

    # 5 concurrent coroutines calling report_429() simultaneously
    tasks = [limiter.report_429() for _ in range(5)]
    await asyncio.gather(*tasks)

    # Current delay should only double once: 1.0 -> 2.0
    assert limiter._current_delay == 2.0
    now = time.monotonic()
    # Pause until should be approx now + 1.0 (between now and now + 1.5)
    assert limiter._pause_until > now
    assert limiter._pause_until <= now + 1.5


@pytest.mark.anyio
async def test_rate_limiter_cooldown_reset():
    limiter = RateLimiter(initial_delay=1.0, max_delay=60.0, cooldown_window=15.0)
    await limiter.report_429()
    assert limiter._current_delay == 2.0

    # Simulate passing of cooldown window without any 429
    with patch("time.monotonic") as mock_time:
        base_time = limiter._last_429_time
        # Advance time by 20s (past 15s cooldown_window)
        mock_time.return_value = base_time + 20.0

        await limiter.report_429()
        # Should reset to initial_delay (1.0), then double to 2.0 for next wave
        assert limiter._current_delay == 2.0
        assert limiter._pause_until == (base_time + 20.0) + 1.0


@pytest.mark.anyio
async def test_rate_limiter_retry_after_header():
    limiter = RateLimiter(initial_delay=1.0, max_delay=60.0)
    now = time.monotonic()
    await limiter.report_429(retry_after=45.0)

    assert limiter._pause_until >= now + 45.0


@pytest.mark.anyio
async def test_rate_limiter_jittered_wait():
    limiter = RateLimiter(initial_delay=2.0)
    now = time.monotonic()
    limiter._pause_until = now + 1.0

    slept_durations = []

    async def fake_sleep(duration):
        slept_durations.append(duration)
        limiter._pause_until = 0.0

    with patch("asyncio.sleep", side_effect=fake_sleep):
        await limiter.wait_if_paused()

    assert len(slept_durations) == 1
    sleep_duration = slept_durations[0]
    # base remaining is ~1.0, jitter is between 0.1 and 0.5 -> total sleep between 1.0 and 1.6
    assert 1.0 <= sleep_duration <= 1.6


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


@pytest.mark.anyio
async def test_fetch_with_retry_sentinel_empty_and_non_json_200():
    with respx.mock(base_url="https://test.ibkr.com") as respx_mock:
        respx_mock.get("/empty-200").respond(200, text="")
        respx_mock.get("/text-200").respond(200, text="Not a JSON document")

        async with httpx.AsyncClient() as client:
            status1, payload1 = await fetch_with_retry(client, "https://test.ibkr.com/empty-200")
            status2, payload2 = await fetch_with_retry(client, "https://test.ibkr.com/text-200")

        assert status1 == 200
        assert payload1 == {}
        assert status2 == 200
        assert payload2 == {}


@pytest.mark.anyio
async def test_fetch_with_retry_session_invalid():
    with respx.mock(base_url="https://test.ibkr.com") as respx_mock:
        r401 = respx_mock.get("/unauthorized").respond(401, text="Unauthorized")
        r403 = respx_mock.get("/forbidden").respond(403, text="Forbidden")
        r400 = respx_mock.get("/bad-request-invalid-headers").respond(
            400, json={"statusCode": 400, "error": "Invalid headers"}
        )

        async with httpx.AsyncClient() as client:
            with pytest.raises(SessionInvalidError):
                await fetch_with_retry(client, "https://test.ibkr.com/unauthorized")
            assert r401.call_count == 1

            with pytest.raises(SessionInvalidError):
                await fetch_with_retry(client, "https://test.ibkr.com/forbidden")
            assert r403.call_count == 1

            with pytest.raises(SessionInvalidError):
                await fetch_with_retry(client, "https://test.ibkr.com/bad-request-invalid-headers")
            assert r400.call_count == 1


@pytest.mark.anyio
async def test_fetch_with_retry_5xx_exponential_backoff_no_limiter_pause():
    limiter = RateLimiter(initial_delay=1.0)
    with respx.mock(base_url="https://test.ibkr.com") as respx_mock:
        route = respx_mock.get("/server-error").respond(500, text="Internal Server Error")

        slept_intervals = []

        async def fake_sleep(dur):
            slept_intervals.append(dur)

        with patch("asyncio.sleep", side_effect=fake_sleep):
            async with httpx.AsyncClient() as client:
                with pytest.raises(RuntimeError, match="failed after 5 attempts"):
                    await fetch_with_retry(
                        client,
                        "https://test.ibkr.com/server-error",
                        max_retries=5,
                        initial_backoff=0.5,
                        rate_limiter=limiter,
                    )

        assert route.call_count == 5
        # Backoffs: 0.5, 1.0, 2.0, 4.0 (4 sleeps for 5 attempts)
        assert slept_intervals == [0.5, 1.0, 2.0, 4.0]
        # RateLimiter global pause must NOT be triggered for 5XX
        assert limiter._pause_until == 0.0


@pytest.mark.anyio
async def test_fetch_with_retry_429_triggers_rate_limiter():
    limiter = RateLimiter(initial_delay=2.0)
    with respx.mock(base_url="https://test.ibkr.com") as respx_mock:
        route = respx_mock.get("/throttled")
        route.side_effect = [
            httpx.Response(429, headers={"Retry-After": "10"}),
            httpx.Response(200, json={"ok": True}),
        ]

        async with httpx.AsyncClient() as client:
            with patch.object(limiter, "wait_if_paused", new_callable=AsyncMock) as mock_wait:
                status, data = await fetch_with_retry(
                    client,
                    "https://test.ibkr.com/throttled",
                    rate_limiter=limiter,
                )
                assert mock_wait.call_count >= 1

        assert status == 200
        assert data == {"ok": True}
        assert route.call_count == 2
        assert limiter._pause_until > 0.0


def test_is_session_invalid_detection():
    # 200 success -> False
    r200 = httpx.Response(200, json={"ok": True})
    assert is_session_invalid(r200) is False

    # 401 / 403 -> True
    r401 = httpx.Response(401, text="Unauthorized")
    assert is_session_invalid(r401) is True
    r403 = httpx.Response(403, text="Forbidden")
    assert is_session_invalid(r403) is True

    # 400 with Invalid headers -> True
    r400_json = httpx.Response(400, json={"statusCode": 400, "error": "Invalid headers"})
    assert is_session_invalid(r400_json) is True
    r400_text = httpx.Response(400, text="Some text with Invalid headers inside")
    assert is_session_invalid(r400_text) is True

    # 400 without invalid headers -> False
    r400_other = httpx.Response(400, json={"error": "Malformed query"})
    assert is_session_invalid(r400_other) is False

    # 500 server error -> False
    r500 = httpx.Response(500, text="Internal Server Error")
    assert is_session_invalid(r500) is False


def test_reconcile_account_id(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("ACCOUNT_ID=U123456\n", encoding="utf-8")
    settings.account_id = "U123456"

    # Same account ID: returns True, does not rewrite
    assert reconcile_account_id("U123456", env_path=env_file) is True
    assert env_file.read_text(encoding="utf-8") == "ACCOUNT_ID=U123456\n"
