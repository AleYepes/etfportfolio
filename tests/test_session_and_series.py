import httpx
import pytest
import respx

from etfportfolio.ingestion.session import fetch_with_retry


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
    from etfportfolio.core.config import settings
    from etfportfolio.ingestion.session import reconcile_account_id

    env_file = tmp_path / ".env"
    env_file.write_text("ACCOUNT_ID=U123456\n", encoding="utf-8")
    settings.account_id = "U123456"

    # Same account ID: returns True, does not rewrite
    assert reconcile_account_id("U123456", env_path=env_file) is True
    assert env_file.read_text(encoding="utf-8") == "ACCOUNT_ID=U123456\n"


def test_load_series_status():
    from datetime import datetime

    import duckdb

    from etfportfolio.core.db import apply_schema
    from etfportfolio.ingestion.prices import _load_price_series_status

    conn = duckdb.connect(":memory:")
    apply_schema(conn)

    # Empty tables return empty dicts
    assert _load_price_series_status(conn) == {}

    # Insert test product into bronze.products
    conn.execute(
        """
        INSERT INTO bronze.products (product_id, symbol, created_at, updated_at)
        VALUES (1001, 'TEST1', now(), now()), (1002, 'TEST2', now(), now())
        """
    )

    d1 = datetime(2026, 8, 1, 0, 0)
    d2 = datetime(2026, 8, 2, 0, 0)
    u1 = datetime(2026, 8, 2, 10, 0)
    u2 = datetime(2026, 8, 2, 12, 0)

    # Populate bronze.prices
    conn.execute(
        "INSERT INTO bronze.prices (product_id, date, close, updated_at) VALUES (1001, $1, 10.0, $2)",
        [d1, u1],
    )
    conn.execute(
        "INSERT INTO bronze.prices (product_id, date, close, updated_at) VALUES (1001, $1, 11.0, $2)",
        [d2, u2],
    )
    conn.execute(
        "INSERT INTO bronze.prices (product_id, date, close, updated_at) VALUES (1002, $1, 20.0, $2)",
        [d1, u1],
    )

    price_status = _load_price_series_status(conn)
    assert price_status[1001] == (d2, u2)
    assert price_status[1002] == (d1, u1)
