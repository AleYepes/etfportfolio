"""
Historical daily FX rate fetching via yfinance.
Quotes foreign currencies directly in USD ({CUR}USD=X).
"""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
import pandas as pd
import yfinance as yf

from etfportfolio.core.config import settings
from etfportfolio.core.db import db_connection
from etfportfolio.core.logging import console
from etfportfolio.core.progress import progress_bar
from etfportfolio.ingest.series import (
    FETCH_MARGIN_DAYS,
    MIN_REFETCH_RETENTION_RATIO,
    SeriesSpec,
    get_last_date,
    get_series_count,
    has_historical_series_change,
    is_series_fresh,
    load_series_status,
    overlap_start_for,
    record_series_status,
    replace_series,
    upsert_series,
    validate_overlap,
)

logger = logging.getLogger(__name__)

FX_SPEC = SeriesSpec(
    bronze_table="bronze.fx",
    cold_table="cold_storage.fx",
    status_table="bronze.fx_status",
    entity_columns=("source_currency", "target_currency"),
    data_columns=("open", "high", "low", "close"),
    tolerance_columns=("open", "high", "low", "close"),
    rel_tol=1e-4,  # 1 basis point
    abs_tol=1e-8,  # Machine epsilon for FX rates
    settlement_rel_tol=0.01,  # 1.00%
    settlement_abs_tol=1e-6,
    settlement_trading_days=5,
    detect_splits=False,  # Currencies do not undergo stock splits
)

DEFAULT_TARGET_CURRENCY = "USD"
FX_TICKER_OVERRIDES = {
    "CNH": "CNY",  # yfinance lacks CNH history; substitute with CNY
}


def resolve_target_currencies(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Select unique non-USD contract currencies."""
    total_row = conn.execute("SELECT COUNT(*) FROM bronze.contracts").fetchone()
    total_contracts = total_row[0] if total_row else 0
    if total_contracts == 0:
        raise RuntimeError("bronze.contracts is empty. Run 'ingest contracts' first to qualify products.")

    query = """
    SELECT DISTINCT currency
    FROM bronze.contracts
    WHERE currency IS NOT NULL
      AND currency != 'USD'
      AND sec_type != 'CASH'
    ORDER BY currency
    """
    rows = conn.execute(query).fetchall()
    return [row[0] for row in rows]


def ticker_symbol_for(source_currency: str, target_currency: str = DEFAULT_TARGET_CURRENCY) -> str:
    """Map a source currency to the yfinance FX ticker, applying overrides."""
    fetch_symbol = FX_TICKER_OVERRIDES.get(source_currency, source_currency)
    return f"{fetch_symbol}{target_currency}=X"


def _to_naive_utc_midnight(value: Any) -> datetime:
    """Normalize a pandas/py datetime to naive UTC midnight."""
    dt = value if isinstance(value, datetime) else pd.Timestamp(value).to_pydatetime()
    if getattr(dt, "tzinfo", None) is not None:
        dt = dt.astimezone(UTC)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)


def extract_fx_bars(
    df: pd.DataFrame | None,
    max_date: datetime | None = None,
) -> dict[datetime, dict[str, float]]:
    """Convert a yfinance history DataFrame into naive-UTC midnight OHLC bars."""
    if df is None or df.empty:
        return {}

    frame = df.copy()
    frame.columns = [str(c).lower() for c in frame.columns]

    result: dict[datetime, dict[str, float]] = {}
    for idx, row in frame.iterrows():
        bar_date = _to_naive_utc_midnight(idx)
        if max_date is not None and bar_date > max_date:
            continue

        close = row["close"] if "close" in row.index else None
        if close is None or pd.isna(close):
            continue

        point: dict[str, float] = {}
        for col in ("open", "high", "low", "close"):
            if col not in row.index:
                continue
            val = row[col]
            if val is None or pd.isna(val):
                continue
            point[col] = float(val)
        if "close" not in point:
            continue
        result[bar_date] = point
    return result


def _download_history(
    ticker_symbol: str,
    *,
    period: str | None = None,
    start: str | None = None,
) -> pd.DataFrame:
    ticker = yf.Ticker(ticker_symbol)
    kwargs: dict[str, Any] = {"auto_adjust": False}
    if period is not None:
        kwargs["period"] = period
    if start is not None:
        kwargs["start"] = start
    return ticker.history(**kwargs)


def _fetch_and_store_fx(
    conn: duckdb.DuckDBPyConnection,
    source_currency: str,
    target_currency: str,
) -> None:
    """Fetch and store FX rates for one currency pair."""
    entity_id = (source_currency, target_currency)
    symbol = ticker_symbol_for(source_currency, target_currency)

    last_date = get_last_date(conn, FX_SPEC, entity_id)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    yesterday = today - timedelta(days=1)

    if last_date is None:
        df = _download_history(symbol, period="max")
        new_bars = extract_fx_bars(df, max_date=yesterday)
        if new_bars:
            replace_series(conn, FX_SPEC, entity_id, new_bars, archive=False)
            record_series_status(conn, FX_SPEC, entity_id, "ok")
            logger.info(
                "FX %s/%s: full refetch complete (%d bars)",
                source_currency,
                target_currency,
                len(new_bars),
            )
        else:
            record_series_status(conn, FX_SPEC, entity_id, "no_data")
            logger.warning("FX %s/%s: no bars returned", source_currency, target_currency)
        return

    start_date = overlap_start_for(last_date) - timedelta(days=FETCH_MARGIN_DAYS)
    df = _download_history(symbol, start=start_date.strftime("%Y-%m-%d"))
    new_bars = extract_fx_bars(df, max_date=yesterday)

    if not new_bars:
        record_series_status(conn, FX_SPEC, entity_id, "no_data")
        logger.info("FX %s/%s: no bars returned for incremental update", source_currency, target_currency)
        return

    valid, mismatch_type = validate_overlap(conn, FX_SPEC, entity_id, new_bars, last_date)

    if not valid:
        logger.warning(
            "FX %s/%s: %s detected. Refetching full history...",
            source_currency,
            target_currency,
            mismatch_type,
        )
        existing_count = get_series_count(conn, FX_SPEC, entity_id)

        df_full = _download_history(symbol, period="max")
        full_bars = extract_fx_bars(df_full, max_date=yesterday)

        min_expected_bars = (
            max(1, math.floor(existing_count * MIN_REFETCH_RETENTION_RATIO)) if existing_count > 5 else 1
        )

        if len(full_bars) < min_expected_bars:
            err_msg = (
                f"Truncated refetch: received {len(full_bars)} bars, "
                f"expected >= {min_expected_bars} (existing: {existing_count})"
            )
            logger.error(
                "FX %s/%s: %s. Aborting replace to prevent data loss.",
                source_currency,
                target_currency,
                err_msg,
            )
            record_series_status(conn, FX_SPEC, entity_id, "error", err_msg)
            return

        if full_bars:
            overlap_start = overlap_start_for(last_date)
            hist_changed = has_historical_series_change(conn, FX_SPEC, entity_id, full_bars, overlap_start)
            if hist_changed:
                replace_series(conn, FX_SPEC, entity_id, full_bars, archive=True, reason=mismatch_type)
                logger.info(
                    "FX %s/%s: restatement confirmed; archived old series and replaced (%d bars)",
                    source_currency,
                    target_currency,
                    len(full_bars),
                )
            else:
                replace_series(conn, FX_SPEC, entity_id, full_bars, archive=False)
                logger.info(
                    "FX %s/%s: historical bars identical; replaced bronze WITHOUT archival (%d bars)",
                    source_currency,
                    target_currency,
                    len(full_bars),
                )
            record_series_status(conn, FX_SPEC, entity_id, "ok")
        else:
            record_series_status(conn, FX_SPEC, entity_id, "no_data")
            logger.warning(
                "FX %s/%s: full refetch returned no bars after mismatch. Preserving existing rows.",
                source_currency,
                target_currency,
            )
        return

    overlap_start = overlap_start_for(last_date)
    points_to_store = {d: pt for d, pt in new_bars.items() if d >= overlap_start}
    upsert_series(conn, FX_SPEC, entity_id, points_to_store)
    record_series_status(conn, FX_SPEC, entity_id, "ok")
    logger.info(
        "FX %s/%s: incremental update complete (%d bars)",
        source_currency,
        target_currency,
        len(points_to_store),
    )


def sync(force: bool = False, target_currency: str = DEFAULT_TARGET_CURRENCY) -> int:
    """Synchronous entry point for FX rate ingestion."""
    console.info("Starting FX rate ingestion...")
    with db_connection(settings.db_path) as conn:
        currencies = resolve_target_currencies(conn)
        if not currencies:
            console.info("No non-USD currencies to process.")
            return 0

        today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        yesterday = today - timedelta(days=1)

        if force:
            to_process = currencies
        else:
            status_cache = load_series_status(conn, FX_SPEC)
            to_process = [
                cur
                for cur in currencies
                if not is_series_fresh(
                    status_cache.get((cur, target_currency)),
                    yesterday,
                    settings.freshness_window_hours,
                )
            ]

        skipped = len(currencies) - len(to_process)
        if skipped:
            console.info(f"{skipped}/{len(currencies)} currencies up to date, {len(to_process)} to process.")
        else:
            console.info(f"Processing {len(currencies)} currency/currencies...")

        if not to_process:
            console.info("Done. All FX series are up to date.")
            return 0

        with progress_bar(len(to_process), desc="FX Rates", unit="currency") as bar:
            for currency in to_process:
                bar.set_postfix_str(currency)
                try:
                    _fetch_and_store_fx(conn, currency, target_currency)
                except Exception as e:
                    logger.error("Failed to fetch FX for %s/%s: %s", currency, target_currency, e)
                    record_series_status(conn, FX_SPEC, (currency, target_currency), "error", str(e))
                finally:
                    bar.update(1)

        return len(to_process)
