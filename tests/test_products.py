import duckdb
import pytest

from etfportfolio.core.config import settings
from etfportfolio.core.db import apply_schema
from etfportfolio.ingest.products import resolve_target_products
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
