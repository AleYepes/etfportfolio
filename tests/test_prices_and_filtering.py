from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest
from ib_async import BarData

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker, apply_schema
from etfportfolio.ingestion.gateway import IBConnectionError
from etfportfolio.ingestion.pipeline import Ingest
from etfportfolio.ingestion.prices import (
    PriceSeriesStatus,
    _fetch_and_store,
    _load_price_series_status,
    _record_price_status,
    _run_price_ingestion,
    format_duration,
    is_series_fresh,
)
from etfportfolio.ingestion.products import resolve_target_products
from etfportfolio.ingestion.utils import ProductContract


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    apply_schema(conn)
    return conn


# --- Scenario: IB Duration Format ---
def test_format_duration():
    # Minimum 10 D for small values
    assert format_duration(1) == "10 D"
    assert format_duration(9) == "10 D"
    assert format_duration(10) == "10 D"

    # Days between 11 and 365 use 'D'
    assert format_duration(15 + 9) == "24 D"
    assert format_duration(100) == "100 D"
    assert format_duration(365) == "365 D"

    # Days > 365 use 'Y'
    assert format_duration(366) == "2 Y"  # ceil(366/365.25) == 2
    assert format_duration(400) == "2 Y"  # ceil(400/365.25) == 2
    assert format_duration(730) == "2 Y"  # ceil(730/365.25) == 2
    assert format_duration(731) == "3 Y"  # ceil(731/365.25) == 3
    assert format_duration(1000) == "3 Y"

    # Capped at 30 Y
    assert format_duration(36525) == "30 Y"
    assert format_duration(100000) == "30 Y"


# --- Scenario: Exchange Blacklisting & Target Product Resolution ---
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


# --- Scenario: Status Tracking & Python Freshness Evaluation ---
def test_record_and_load_price_status(db_conn):
    db_conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, created_at, updated_at)
        VALUES (101, 'XYZ', now(), now())
        """
    )

    # Initial record: 'no_data'
    _record_price_status(db_conn, 101, "no_data", None)
    status_map = _load_price_series_status(db_conn)
    assert 101 in status_map
    assert status_map[101].status == "no_data"
    assert status_map[101].last_checked_at is not None

    # Update record: 'error' with truncation > 500 chars
    long_error = "E" * 600
    _record_price_status(db_conn, 101, "error", long_error)
    row = db_conn.execute("SELECT status, error_message FROM bronze.price_status WHERE product_id = 101").fetchone()
    assert row[0] == "error"
    assert len(row[1]) == 500
    assert row[1] == "E" * 500

    # Update record: 'ok'
    _record_price_status(db_conn, 101, "ok", None)
    row = db_conn.execute("SELECT status, error_message FROM bronze.price_status WHERE product_id = 101").fetchone()
    assert row[0] == "ok"
    assert row[1] is None


def test_is_series_fresh_differentiated():
    now = datetime.now(UTC)
    yesterday = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    # 1. 'ok' or 'no_data' within freshness window is fresh
    recent_check = now - timedelta(hours=5)
    old_date = yesterday - timedelta(days=10)
    assert (
        is_series_fresh(
            PriceSeriesStatus(last_date=old_date, last_updated=None, last_checked_at=recent_check, status="no_data"),
            yesterday,
            24.0,
        )
        is True
    )
    assert (
        is_series_fresh(
            PriceSeriesStatus(last_date=old_date, last_updated=None, last_checked_at=recent_check, status="ok"),
            yesterday,
            24.0,
        )
        is True
    )

    # 2. 'error' is NEVER fresh even if checked 1 second ago
    one_sec_ago = now - timedelta(seconds=1)
    assert (
        is_series_fresh(
            PriceSeriesStatus(last_date=old_date, last_updated=None, last_checked_at=one_sec_ago, status="error"),
            yesterday,
            24.0,
        )
        is False
    )

    # 3. Bar reaches target_date is always fresh regardless of status
    assert (
        is_series_fresh(
            PriceSeriesStatus(last_date=yesterday, last_updated=None, last_checked_at=None, status=None),
            yesterday,
            24.0,
        )
        is True
    )


# --- Scenario: Preservation of Prices on 0 Bars ---
@pytest.mark.anyio
async def test_fetch_and_store_preserves_prices_on_zero_bars(tmp_path):
    db_file = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    last_date = datetime(2026, 8, 20, 0, 0)
    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, created_at, updated_at)
        VALUES (555, 'PRESERVE', now(), now())
        """
    )
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (555, $1, 123.45, now())
        """,
        [last_date],
    )
    conn.close()

    mock_ib = MagicMock()
    mock_ib.reqHistoricalDataAsync = AsyncMock(return_value=[])

    product = ProductContract(product_id=555, symbol="PRESERVE")

    async with AsyncDbWorker(db_file) as worker:
        await _fetch_and_store(worker, mock_ib, product)

    conn = duckdb.connect(db_file)
    # Existing price row must remain intact
    prices = conn.execute("SELECT close FROM bronze.prices WHERE product_id = 555").fetchall()
    assert len(prices) == 1
    assert prices[0][0] == 123.45

    # Status must be 'no_data'
    status_row = conn.execute("SELECT status, error_message FROM bronze.price_status WHERE product_id = 555").fetchone()
    assert status_row is not None
    assert status_row[0] == "no_data"
    assert status_row[1] is None
    conn.close()


# --- Scenario: Overlap Mismatch Refetch Returning 0 Bars Preserves Existing Rows ---
@pytest.mark.anyio
async def test_fetch_and_store_mismatch_zero_bars_preserves(tmp_path):
    db_file = str(tmp_path / "test_mismatch.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    d1 = datetime(2026, 8, 20, 0, 0)
    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, created_at, updated_at)
        VALUES (777, 'MISMATCH', now(), now())
        """
    )
    conn.execute(
        """
        INSERT INTO bronze.prices (product_id, date, close, updated_at)
        VALUES (777, $1, 100.0, now())
        """,
        [d1],
    )
    conn.close()

    # First call (incremental): returns mismatched value on 2026-08-20
    # Second call (30 Y full refetch): returns empty list
    mismatched_bar = BarData(
        date=datetime(2026, 8, 20, 0, 0),
        open=100.0,
        high=105.0,
        low=95.0,
        close=999.0,  # mismatch!
        volume=100.0,
        average=100.0,
        barCount=10,
    )

    mock_ib = MagicMock()
    mock_ib.reqHistoricalDataAsync = AsyncMock(side_effect=[[mismatched_bar], []])

    product = ProductContract(product_id=777, symbol="MISMATCH")

    async with AsyncDbWorker(db_file) as worker:
        await _fetch_and_store(worker, mock_ib, product)

    conn = duckdb.connect(db_file)
    # Existing price row must be preserved
    prices = conn.execute("SELECT close FROM bronze.prices WHERE product_id = 777").fetchall()
    assert len(prices) == 1
    assert prices[0][0] == 100.0

    # Cold storage should NOT have been written since full refetch returned no bars
    cold_row = conn.execute("SELECT COUNT(*) FROM cold_storage.prices WHERE product_id = 777").fetchone()
    assert cold_row is not None
    assert cold_row[0] == 0

    # Status must be 'no_data'
    status_row = conn.execute("SELECT status, error_message FROM bronze.price_status WHERE product_id = 777").fetchone()
    assert status_row is not None
    assert status_row[0] == "no_data"
    conn.close()


# --- Scenario: Error Capture in _run_price_ingestion ---
@pytest.mark.anyio
async def test_run_price_ingestion_error_capture(tmp_path, monkeypatch):
    db_file = str(tmp_path / "test_error.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, created_at, updated_at)
        VALUES (999, 'ERR', now(), now())
        """
    )
    conn.close()

    monkeypatch.setattr(settings, "db_path", db_file)

    mock_ib = MagicMock()
    mock_ib.isConnected.return_value = True
    mock_ib.reqHistoricalDataAsync = AsyncMock(side_effect=RuntimeError("Gateway Timeout"))

    with patch("etfportfolio.ingestion.prices.ib_connection") as mock_conn:
        mock_conn.return_value.__aenter__.return_value = mock_ib
        mock_conn.return_value.__aexit__.return_value = False
        count = await _run_price_ingestion(force=True)
        assert count == 1

    conn = duckdb.connect(db_file)
    status_row = conn.execute("SELECT status, error_message FROM bronze.price_status WHERE product_id = 999").fetchone()
    assert status_row is not None
    assert status_row[0] == "error"
    assert status_row[1] is not None
    assert "Gateway Timeout" in status_row[1]
    conn.close()


# --- Scenario: IBConnectionError Immediately Aborts ---
@pytest.mark.anyio
async def test_run_price_ingestion_ib_connection_error_aborts(tmp_path, monkeypatch):
    db_file = str(tmp_path / "test_abort.duckdb")
    conn = duckdb.connect(db_file)
    apply_schema(conn)

    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, created_at, updated_at)
        VALUES (111, 'ONE', now(), now()), (222, 'TWO', now(), now())
        """
    )
    conn.close()

    monkeypatch.setattr(settings, "db_path", db_file)

    mock_ib = MagicMock()
    mock_ib.isConnected.return_value = True
    mock_ib.reqHistoricalDataAsync = AsyncMock(side_effect=IBConnectionError("Connection lost"))

    with patch("etfportfolio.ingestion.prices.ib_connection") as mock_conn:
        mock_conn.return_value.__aenter__.return_value = mock_ib
        mock_conn.return_value.__aexit__.return_value = False
        with pytest.raises(IBConnectionError, match="Connection lost"):
            await _run_price_ingestion(force=True)


# --- Scenario: CLI Signatures reject --limit and --product_ids ---
def test_cli_signatures_reject_unused_flags():
    ingest = Ingest()
    # Ensure contracts, prices, details, and __call__ only accept force
    with pytest.raises(TypeError):
        ingest(limit=10)  # type: ignore

    with pytest.raises(TypeError):
        ingest.contracts(product_ids="1001")  # type: ignore

    with pytest.raises(TypeError):
        ingest.prices(limit=5)  # type: ignore

    with pytest.raises(TypeError):
        ingest.details(product_ids="1001,1002")  # type: ignore
