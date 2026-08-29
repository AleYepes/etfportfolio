"""
Historical daily price fetching via IB Gateway (clientId=2).
Handles incremental updates, overlap validation, and cold storage archiving.
"""

import asyncio
import logging
import math
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
from ib_async import BarData, Contract

from etfportfolio.ingestion.gateway import ib_connection

logger = logging.getLogger(__name__)

# WhatToShow for adjusted last prices
WHAT_TO_SHOW = "ADJUSTED_LAST"
BAR_SIZE = "1 day"


def _get_last_date(conn: duckdb.DuckDBPyConnection, product_id: int) -> datetime | None:
    """Get the most recent date for a product in bronze.prices."""
    row = conn.execute(
        "SELECT MAX(date) FROM bronze.prices WHERE product_id = $1",
        [product_id],
    ).fetchone()
    return row[0] if row and row[0] else None


def _archive_prices(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    rows: list[dict[str, Any]],
    run_id: datetime,
) -> None:
    """Archive existing price rows to cold_storage.prices."""
    if not rows:
        return

    for row in rows:
        conn.execute(
            """
            INSERT INTO cold_storage.prices
            (product_id, run_id, date, open, high, low, close, volume, average, bar_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (product_id, run_id, date) DO NOTHING
            """,
            [
                product_id,
                run_id,
                row["date"],
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
                row.get("average"),
                row.get("bar_count"),
            ],
        )


def _delete_prices(conn: duckdb.DuckDBPyConnection, product_id: int) -> None:
    """Delete all price rows for a product."""
    conn.execute("DELETE FROM bronze.prices WHERE product_id = $1", [product_id])


def _upsert_prices(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    bars: list[BarData],
) -> None:
    """Upsert price bars into bronze.prices."""
    now = datetime.now(UTC)
    for bar in bars:
        bar_date = datetime.combine(bar.date, datetime.min.time(), tzinfo=UTC)
        conn.execute(
            """
            INSERT INTO bronze.prices
            (product_id, date, open, high, low, close, volume, average, bar_count, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (product_id, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                average = EXCLUDED.average,
                bar_count = EXCLUDED.bar_count,
                updated_at = EXCLUDED.updated_at
            """,
            [
                product_id,
                bar_date,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.average,
                bar.barCount,
                now,
            ],
        )


def _extract_bars(bars: list[BarData]) -> dict[datetime, dict[str, Any]]:
    """Extract bar data into a dict keyed by date."""
    result = {}
    for bar in bars:
        bar_date = datetime.combine(bar.date, datetime.min.time(), tzinfo=UTC)
        result[bar_date] = {
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "average": bar.average,
            "bar_count": bar.barCount,
        }
    return result


def _validate_overlap(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    new_bars: dict[datetime, dict[str, Any]],
    overlap_start: datetime,
) -> tuple[bool, str | None]:
    """Validate that overlapping dates/values match existing data.

    Returns (is_valid, mismatch_type).
    mismatch_type: None if valid, 'date_mismatch' or 'value_mismatch'.
    """
    # Get existing rows within overlap window
    existing_rows = conn.execute(
        """
        SELECT date, open, high, low, close, volume, average, bar_count
        FROM bronze.prices
        WHERE product_id = $1 AND date >= $2
        """,
        [product_id, overlap_start],
    ).fetchall()

    existing_dict = {}
    for row in existing_rows:
        existing_dict[row[0]] = {
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "volume": row[5],
            "average": row[6],
            "bar_count": row[7],
        }

    # Check date mismatch: all existing dates must be in new data
    for date in existing_dict:
        if date not in new_bars:
            return False, "date_mismatch"

    # Check value mismatch for overlapping dates
    for date, new_vals in new_bars.items():
        if date in existing_dict:
            old_vals = existing_dict[date]
            for key in ["open", "high", "low", "close", "volume", "average"]:
                v1, v2 = old_vals.get(key), new_vals.get(key)
                if (
                    v1 is not None
                    and v2 is not None
                    and not math.isclose(float(v1), float(v2), rel_tol=1e-4, abs_tol=1e-4)
                ):
                    return False, "value_mismatch"

    return True, None


async def _fetch_historical(
    ib: Any,
    product_id: int,
    symbol: str,
    exchange: str,
    currency: str,
    duration: str,
    end_datetime: str,
) -> list[BarData]:
    """Fetch historical bars for a single product."""
    contract = Contract()
    contract.conId = product_id
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = exchange
    contract.currency = currency

    bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime=end_datetime,
        durationStr=duration,
        barSizeSetting=BAR_SIZE,
        whatToShow=WHAT_TO_SHOW,
        useRTH=True,
        formatDate=1,
    )
    return bars or []


def _is_identical(
    existing: dict[datetime, dict[str, Any]],
    new: dict[datetime, dict[str, Any]],
) -> bool:
    """Check if two series are identical."""
    if set(existing.keys()) != set(new.keys()):
        return False
    for date, new_vals in new.items():
        old_vals = existing.get(date)
        if old_vals is None:
            return False
        for key in ["open", "high", "low", "close", "volume", "average"]:
            v1, v2 = old_vals.get(key), new_vals.get(key)
            if v1 is not None and v2 is not None and not math.isclose(float(v1), float(v2), rel_tol=1e-4, abs_tol=1e-4):
                return False
    return True


async def _fetch_and_store(
    conn: duckdb.DuckDBPyConnection,
    ib: Any,
    product_id: int,
    symbol: str,
    exchange: str,
    currency: str,
    force: bool = False,
) -> bool:
    """Fetch and store prices for one product."""
    last_date = _get_last_date(conn, product_id)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    end_datetime = yesterday.strftime("%Y%m%d") + " 23:59:59 UTC"

    if force or last_date is None:
        # Full refetch
        duration = "30Y"
        bars = await _fetch_historical(ib, product_id, symbol, exchange, currency, duration, end_datetime)
        new_bars = _extract_bars(bars)

        if last_date is not None and not force:
            # Check if full refetch is identical to existing
            existing_rows = conn.execute(
                "SELECT date, open, high, low, close, volume, average, bar_count FROM bronze.prices WHERE product_id = $1",
                [product_id],
            ).fetchall()
            existing = {}
            for row in existing_rows:
                existing[row[0]] = {
                    "open": row[1],
                    "high": row[2],
                    "low": row[3],
                    "close": row[4],
                    "volume": row[5],
                    "average": row[6],
                    "bar_count": row[7],
                }
            if _is_identical(existing, new_bars):
                logger.info("Product %d: full refetch is identical to existing, skipping", product_id)
                return True

        # Archive existing before deletion
        if last_date is not None:
            run_id = (
                conn.execute(
                    "SELECT MAX(updated_at) FROM bronze.prices WHERE product_id = $1",
                    [product_id],
                ).fetchone()[0]
                or today
            )
            existing_rows = conn.execute(
                "SELECT date, open, high, low, close, volume, average, bar_count FROM bronze.prices WHERE product_id = $1",
                [product_id],
            ).fetchall()
            archive_rows = []
            for row in existing_rows:
                archive_rows.append(
                    {
                        "date": row[0],
                        "open": row[1],
                        "high": row[2],
                        "low": row[3],
                        "close": row[4],
                        "volume": row[5],
                        "average": row[6],
                        "bar_count": row[7],
                    }
                )
            _archive_prices(conn, product_id, archive_rows, run_id)

        _delete_prices(conn, product_id)
        _upsert_prices(conn, product_id, bars)
        logger.info("Product %d: full price refetch complete (%d bars)", product_id, len(bars))
        return True

    # Incremental fetch
    gap_days = (today - last_date).days
    duration = f"{gap_days + 7} D"
    bars = await _fetch_historical(ib, product_id, symbol, exchange, currency, duration, end_datetime)
    new_bars = _extract_bars(bars)

    # Validate overlap
    overlap_start = last_date - timedelta(days=7)
    valid, mismatch_type = _validate_overlap(conn, product_id, new_bars, overlap_start)

    if not valid:
        logger.warning("Product %d: %s detected. Archiving and refetching full series...", product_id, mismatch_type)
        # Force full refetch
        return await _fetch_and_store(conn, ib, product_id, symbol, exchange, currency, force=True)

    # Store incremental
    _upsert_prices(conn, product_id, bars)
    logger.info("Product %d: incremental price update complete (%d bars)", product_id, len(bars))
    return True


async def _run_price_ingestion(conn: duckdb.DuckDBPyConnection, force: bool = False) -> int:
    """Run the price ingestion phase."""
    # Get all products from silver.products (which requires contracts)
    products = conn.execute(
        """
        SELECT p.product_id, p.symbol, p.exchange_id, p.currency
        FROM silver.products p
        ORDER BY p.product_id
        """
    ).fetchall()

    if not products:
        logger.error("No products in silver.products. Run contracts phase first.")
        raise RuntimeError("silver.products is empty. Run contracts phase first.")

    logger.info("Price ingestion: %d products to process", len(products))

    semaphore = asyncio.Semaphore(1)

    async with ib_connection(client_id=2) as ib:

        async def process_one(product_id: int, symbol: str, exchange: str, currency: str):
            async with semaphore:
                try:
                    await _fetch_and_store(conn, ib, product_id, symbol, exchange, currency, force)
                except Exception as e:
                    logger.error("Failed to fetch prices for product %d: %s", product_id, e)

        tasks = [process_one(pid, sym, exch, cur) for pid, sym, exch, cur in products]
        await asyncio.gather(*tasks)

    return len(products)


async def sync(conn: duckdb.DuckDBPyConnection, force: bool = False) -> int:
    """Public entry point for the price phase."""
    return await _run_price_ingestion(conn, force=force)
