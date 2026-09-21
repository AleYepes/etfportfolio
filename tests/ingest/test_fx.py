from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from etfportfolio.core.config import settings
from etfportfolio.ingest.fx import (
    DEFAULT_TARGET_CURRENCY,
    FX_SPEC,
    extract_fx_bars,
    resolve_target_currencies,
    sync,
    ticker_symbol_for,
)
from etfportfolio.ingest.series import load_series_status


def _insert_contract(conn, product_id: int, currency: str, sec_type: str = "STK") -> None:
    conn.execute(
        """
        INSERT INTO bronze.contracts (product_id, symbol, sec_type, currency, created_at, updated_at)
        VALUES (?, ?, ?, ?, now(), now())
        """,
        [product_id, f"SYM{product_id}", sec_type, currency],
    )


def _fx_df(rows: list[tuple[datetime, float]]) -> pd.DataFrame:
    index = pd.DatetimeIndex([r[0] for r in rows])
    closes = [r[1] for r in rows]
    return pd.DataFrame(
        {
            "Open": closes,
            "High": [c * 1.001 for c in closes],
            "Low": [c * 0.999 for c in closes],
            "Close": closes,
            "Volume": [0.0] * len(closes),
        },
        index=index,
    )


def test_resolve_target_currencies_sorted_non_usd(db_conn):
    _insert_contract(db_conn, 1, "EUR")
    _insert_contract(db_conn, 2, "USD")
    _insert_contract(db_conn, 3, "JPY")
    _insert_contract(db_conn, 4, "EUR")
    _insert_contract(db_conn, 5, "GBP", sec_type="CASH")
    _insert_contract(db_conn, 6, None)

    currencies = resolve_target_currencies(db_conn)
    assert currencies == ["EUR", "JPY"]


def test_resolve_target_currencies_empty_contracts_raises(db_conn):
    with pytest.raises(RuntimeError, match="bronze.contracts is empty"):
        resolve_target_currencies(db_conn)


def test_ticker_symbol_cnh_override():
    assert ticker_symbol_for("EUR") == "EURUSD=X"
    assert ticker_symbol_for("CNH") == "CNYUSD=X"
    assert ticker_symbol_for("CNY") == "CNYUSD=X"
    assert ticker_symbol_for("JPY", "USD") == "JPYUSD=X"


def test_extract_fx_bars_normalizes_tz_and_drops_nan():
    yesterday = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None) - timedelta(days=1)
    tz_ny = ZoneInfo("America/New_York")
    future_utc = datetime.now(UTC) + timedelta(days=2)
    rows = [
        (datetime(2026, 8, 1, 16, 0, tzinfo=tz_ny), 1.10),
        (datetime(2026, 8, 2, 16, 0, tzinfo=tz_ny), 1.11),
        (future_utc.astimezone(tz_ny), 1.99),
    ]
    df = _fx_df(rows)
    df.loc[df.index[1], "Close"] = float("nan")

    bars = extract_fx_bars(df, max_date=yesterday)
    future_key = future_utc.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    assert future_key not in bars
    # 2026-08-01 16:00 NY -> 20:00 UTC -> naive midnight 2026-08-01
    assert datetime(2026, 8, 1, 0, 0) in bars
    assert bars[datetime(2026, 8, 1, 0, 0)]["close"] == pytest.approx(1.10)
    # NaN close dropped
    assert datetime(2026, 8, 2, 0, 0) not in bars


def test_extract_fx_bars_empty():
    assert extract_fx_bars(None) == {}
    assert extract_fx_bars(pd.DataFrame()) == {}


def test_cnh_maps_to_cny_ticker_but_stores_as_cnh(tmp_path, monkeypatch):
    db_file = str(tmp_path / "fx_cnh.duckdb")
    monkeypatch.setattr(settings, "db_path", db_file)

    import duckdb

    from etfportfolio.core.db import apply_schema

    conn = duckdb.connect(db_file)
    apply_schema(conn)
    _insert_contract(conn, 1, "CNH")
    _insert_contract(conn, 2, "CNY")
    conn.close()

    yesterday = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None) - timedelta(days=1)
    cnh_df = _fx_df([(yesterday - timedelta(days=i), 0.14) for i in range(5)])
    cny_df = _fx_df([(yesterday - timedelta(days=i), 0.15) for i in range(5)])

    called: list[str] = []

    def ticker_factory(symbol):
        called.append(symbol)
        mock = MagicMock()
        mock.history.return_value = cnh_df if len(called) == 1 else cny_df
        return mock

    with patch("etfportfolio.ingest.fx.yf.Ticker", side_effect=ticker_factory):
        count = sync(force=True)

    assert count == 2
    assert called == ["CNYUSD=X", "CNYUSD=X"]

    conn = duckdb.connect(db_file)
    sources = {
        r[0]: r[1] for r in conn.execute("SELECT source_currency, COUNT(*) FROM bronze.fx GROUP BY 1").fetchall()
    }
    assert sources["CNH"] == 5
    assert sources["CNY"] == 5
    cnh_close = conn.execute("SELECT DISTINCT close FROM bronze.fx WHERE source_currency = 'CNH'").fetchone()[0]
    cny_close = conn.execute("SELECT DISTINCT close FROM bronze.fx WHERE source_currency = 'CNY'").fetchone()[0]
    assert cnh_close == pytest.approx(0.14)
    assert cny_close == pytest.approx(0.15)
    conn.close()


def test_sync_initial_fill_and_incremental(tmp_path, monkeypatch):
    db_file = str(tmp_path / "fx_sync.duckdb")
    monkeypatch.setattr(settings, "db_path", db_file)

    import duckdb

    from etfportfolio.core.db import apply_schema

    conn = duckdb.connect(db_file)
    apply_schema(conn)
    _insert_contract(conn, 1, "EUR")
    conn.close()

    yesterday = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None) - timedelta(days=1)
    initial = _fx_df([(yesterday - timedelta(days=i), 1.08) for i in range(20)])

    with patch("etfportfolio.ingest.fx.yf.Ticker") as mock_cls:
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = initial
        mock_cls.return_value = mock_ticker
        assert sync(force=True) == 1
        mock_cls.assert_called_with("EURUSD=X")
        assert mock_ticker.history.call_args.kwargs.get("period") == "max"
        assert mock_ticker.history.call_args.kwargs.get("auto_adjust") is False

    conn = duckdb.connect(db_file)
    assert conn.execute("SELECT COUNT(*) FROM bronze.fx").fetchone()[0] == 20
    status = load_series_status(conn, FX_SPEC)
    assert status[("EUR", DEFAULT_TARGET_CURRENCY)].status == "ok"
    conn.close()

    incremental = _fx_df([(yesterday - timedelta(days=i), 1.08) for i in range(16)])

    with patch("etfportfolio.ingest.fx.yf.Ticker") as mock_cls:
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = incremental
        mock_cls.return_value = mock_ticker
        assert sync(force=True) == 1
        assert "start" in mock_ticker.history.call_args.kwargs

    conn = duckdb.connect(db_file)
    assert conn.execute("SELECT COUNT(*) FROM bronze.fx").fetchone()[0] == 20
    conn.close()


def test_sync_restatement_archives(tmp_path, monkeypatch):
    db_file = str(tmp_path / "fx_restate.duckdb")
    monkeypatch.setattr(settings, "db_path", db_file)

    import duckdb

    from etfportfolio.core.db import apply_schema

    conn = duckdb.connect(db_file)
    apply_schema(conn)
    _insert_contract(conn, 1, "EUR")
    conn.close()

    yesterday = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None) - timedelta(days=1)
    initial = _fx_df([(yesterday - timedelta(days=i), 1.08) for i in range(20)])
    restated = _fx_df([(yesterday - timedelta(days=i), 1.20) for i in range(20)])

    with patch("etfportfolio.ingest.fx.yf.Ticker") as mock_cls:
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = initial
        mock_cls.return_value = mock_ticker
        sync(force=True)

    with patch("etfportfolio.ingest.fx.yf.Ticker") as mock_cls:
        mock_ticker = MagicMock()
        mock_ticker.history.side_effect = [restated, restated]
        mock_cls.return_value = mock_ticker
        sync(force=True)

    conn = duckdb.connect(db_file)
    closes = conn.execute("SELECT DISTINCT close FROM bronze.fx").fetchall()
    assert all(abs(r[0] - 1.20) < 1e-9 for r in closes)
    cold = conn.execute("SELECT COUNT(*) FROM cold_storage.fx").fetchone()[0]
    assert cold == 20
    reason = conn.execute("SELECT DISTINCT reason FROM cold_storage.fx").fetchone()[0]
    assert reason == "restatement"
    conn.close()


def test_sync_network_failure_records_error_without_crash(tmp_path, monkeypatch):
    db_file = str(tmp_path / "fx_err.duckdb")
    monkeypatch.setattr(settings, "db_path", db_file)

    import duckdb

    from etfportfolio.core.db import apply_schema

    conn = duckdb.connect(db_file)
    apply_schema(conn)
    _insert_contract(conn, 1, "EUR")
    _insert_contract(conn, 2, "JPY")
    conn.close()

    yesterday = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None) - timedelta(days=1)
    jpy_df = _fx_df([(yesterday - timedelta(days=i), 0.0067) for i in range(5)])

    def factory(symbol):
        mock = MagicMock()
        if symbol == "EURUSD=X":
            mock.history.side_effect = RuntimeError("yahoo timeout")
        else:
            mock.history.return_value = jpy_df
        return mock

    with patch("etfportfolio.ingest.fx.yf.Ticker", side_effect=factory):
        count = sync(force=True)
        assert count == 2

    conn = duckdb.connect(db_file)
    statuses = {r[0]: r[1] for r in conn.execute("SELECT source_currency, status FROM bronze.fx_status").fetchall()}
    assert statuses["EUR"] == "error"
    assert statuses["JPY"] == "ok"
    err = conn.execute("SELECT error_message FROM bronze.fx_status WHERE source_currency = 'EUR'").fetchone()[0]
    assert "yahoo timeout" in err
    conn.close()
