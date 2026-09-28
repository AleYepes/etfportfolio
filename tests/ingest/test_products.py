from unittest.mock import AsyncMock, patch

import duckdb
import httpx
import pytest

from etfportfolio.core.config import settings
from etfportfolio.core.db import apply_schema
from etfportfolio.ingest.products import (
    _build_unauthenticated_client,
    resolve_target_products,
    sync,
)
from etfportfolio.ingest.session import RateLimiter
from etfportfolio.ingest.utils import ProductContract


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    apply_schema(conn)
    return conn


def test_resolve_target_products_empty_contracts(db_conn):
    with pytest.raises(RuntimeError, match="bronze.contracts is empty"):
        resolve_target_products(db_conn)


def test_resolve_target_products_filtering(db_conn, monkeypatch):
    monkeypatch.setattr(settings, "blocked_exchanges", ["SFB", "EBS", "TSEJ"])

    # Insert test contracts
    # 1. Product on allowed primary exchange
    # 2. Product on blocked primary exchange
    # 3. Product with null primary exchange but blocked exchange_id
    # 4. Product with null primary exchange and allowed exchange_id
    # 5. Product with both null
    db_conn.execute(
        """
        INSERT INTO bronze.contracts (
            product_id, symbol, sec_type, exchange_id, primary_exchange_id,
            currency, local_symbol, trading_class, created_at, updated_at
        ) VALUES
        (1, 'AAA', 'STK', 'SMART', 'NASDAQ', 'USD', 'AAA', 'AAA', now(), now()),
        (2, 'BBB', 'STK', 'SMART', 'SFB', 'CHF', 'BBB', 'BBB', now(), now()),
        (3, 'CCC', 'STK', 'EBS', NULL, 'CHF', 'CCC', 'CCC', now(), now()),
        (4, 'DDD', 'STK', 'LSE', NULL, 'GBP', 'DDD', 'DDD', now(), now()),
        (5, 'EEE', 'STK', NULL, NULL, 'USD', 'EEE', 'EEE', now(), now())
        """
    )

    targets = resolve_target_products(db_conn)
    target_ids = [p.product_id for p in targets]

    assert target_ids == [1, 4, 5]
    assert all(isinstance(p, ProductContract) for p in targets)
    assert targets[0].symbol == "AAA"
    assert targets[0].primary_exchange_id == "NASDAQ"


def test_resolve_target_products_all_blocked(db_conn, monkeypatch, caplog):
    monkeypatch.setattr(settings, "blocked_exchanges", ["SFB", "EBS"])
    db_conn.execute(
        """
        INSERT INTO bronze.contracts (
            product_id, symbol, sec_type, exchange_id, primary_exchange_id, created_at, updated_at
        ) VALUES
        (1, 'AAA', 'STK', 'SMART', 'SFB', now(), now()),
        (2, 'BBB', 'STK', 'EBS', NULL, now(), now())
        """
    )

    with caplog.at_level("INFO"):
        targets = resolve_target_products(db_conn)

    assert targets == []
    assert "All products were excluded by blocked_exchanges." in caplog.text


@pytest.mark.anyio
async def test_build_unauthenticated_client():
    limiter = RateLimiter()
    client = _build_unauthenticated_client(rate_limiter=limiter)
    try:
        assert isinstance(client, httpx.AsyncClient)
        assert client.headers["X-Requested-With"] == "XMLHttpRequest"
        assert "Mozilla" in client.headers["User-Agent"]
        # Unauthenticated: no cookies configured
        assert len(client.cookies) == 0
        assert getattr(client, "rate_limiter", None) is limiter
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_sync_products_unauthenticated_and_routes_fetch_with_retry(tmp_path, monkeypatch):
    db_file = str(tmp_path / "test_products.duckdb")
    with duckdb.connect(db_file) as conn:
        apply_schema(conn)

    monkeypatch.setattr(settings, "db_path", db_file)
    limiter = RateLimiter()

    sample_products = [
        {
            "conid": 1001,
            "type": "ETF",
            "symbol": "SPY",
            "exchangeId": "ARCA",
            "localSymbol": "SPY",
            "description": "SPDR S&P 500 ETF Trust",
            "isin": "US78462F1030",
            "currency": "USD",
            "country": "US",
        }
    ]

    with patch("etfportfolio.ingest.products.fetch_with_retry", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = (200, {"products": sample_products})

        synced_count = await sync(rate_limiter=limiter, force=True)

        assert synced_count == 1
        assert mock_fetch.called
        call_args = mock_fetch.call_args
        # client passed should be unauthenticated with limiter
        passed_client = call_args[0][0]
        assert getattr(passed_client, "rate_limiter", None) is limiter
        assert len(passed_client.cookies) == 0
        assert call_args[0][1] == "/webrest/search/products-by-filters"
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["rate_limiter"] is limiter

    with duckdb.connect(db_file) as conn:
        row = conn.execute("SELECT COUNT(*) FROM bronze.products WHERE product_id = 1001").fetchone()
        assert row is not None and row[0] == 1
