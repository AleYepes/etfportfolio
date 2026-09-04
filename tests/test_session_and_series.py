import httpx
import pytest
import respx

from etfportfolio.ingestion.sentiment import _extract_sentiment_points
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


def test_extract_sentiment_points_empty():
    assert _extract_sentiment_points(None) == {}
    assert _extract_sentiment_points({}) == {}
    assert _extract_sentiment_points({"sentiment": []}) == {}


def test_extract_sentiment_points_valid():
    payload = {
        "sentiment": [
            {
                "datetime": 1725000000000,
                "svolatility": 0.12,
                "sdispersion": 0.34,
                "svscore": 0.56,
                "sbuzz": 10.0,
                "svolume": 20.0,
                "sdelta": 0.05,
                "sscore": 0.8,
                "smean": 0.75,
                "price": 999.0,  # Should be stripped
            }
        ]
    }
    extracted = _extract_sentiment_points(payload)
    assert len(extracted) == 1
    point = list(extracted.values())[0]
    assert "price" not in point
    assert point["svolatility"] == 0.12
    assert point["sscore"] == 0.8


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
    from etfportfolio.ingestion.sentiment import _load_sentiment_series_status

    conn = duckdb.connect(":memory:")
    apply_schema(conn)

    # Empty tables return empty dicts
    assert _load_price_series_status(conn) == {}
    assert _load_sentiment_series_status(conn) == {}

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

    # Populate bronze.sentiment
    conn.execute(
        "INSERT INTO bronze.sentiment (product_id, date, sscore, updated_at) VALUES (1001, $1, 0.5, $2)",
        [d1, u1],
    )
    conn.execute(
        "INSERT INTO bronze.sentiment (product_id, date, sscore, updated_at) VALUES (1001, $1, 0.8, $2)",
        [d2, u2],
    )

    sentiment_status = _load_sentiment_series_status(conn)
    assert sentiment_status[1001] == (d2, u2)
    assert 1002 not in sentiment_status
