"""
Historical daily price fetching via IB Gateway (clientId=2).
Handles incremental updates and overlap validation.
"""

import asyncio
import logging
import math
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
from ib_async import BarData, Contract

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingestion.gateway import IBConnectionError, ib_connection

logger = logging.getLogger(__name__)

WHAT_TO_SHOW = "ADJUSTED_LAST"
BAR_SIZE = "1 day"


def _select_product_ids(conn: duckdb.DuckDBPyConnection) -> list[int]:
    rows = conn.execute(
        """
        SELECT product_id
        FROM silver.products
        ORDER BY product_id
        """
    ).fetchall()
    return [row[0] for row in rows]


def _get_last_date(conn: duckdb.DuckDBPyConnection, product_id: int) -> datetime | None:
    row = conn.execute(
        "SELECT MAX(date) FROM bronze.prices WHERE product_id = $1",
        [product_id],
    ).fetchone()
    return row[0] if row and row[0] else None


def _extract_bars(bars: list[BarData]) -> dict[datetime, dict[str, Any]]:
    """Convert IB BarData list into a dict keyed by UTC midnight date."""
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
    mismatch_type is None if valid, otherwise 'date_mismatch' or 'value_mismatch'.
    """
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

    # Every existing date in overlap must appear in new data
    for date in existing_dict:
        if date not in new_bars:
            return False, "date_mismatch"

    # Matching dates must have matching values
    for date, new_vals in new_bars.items():
        if date in existing_dict:
            old_vals = existing_dict[date]
            for key in ("open", "high", "low", "close", "volume", "average"):
                v1, v2 = old_vals.get(key), new_vals.get(key)
                if (
                    v1 is not None
                    and v2 is not None
                    and not math.isclose(float(v1), float(v2), rel_tol=1e-4, abs_tol=1e-4)
                ):
                    return False, "value_mismatch"

    return True, None


def _replace_prices(conn: duckdb.DuckDBPyConnection, product_id: int, bars: list[BarData]) -> None:
    """Replace all price rows for a product inside a single transaction."""
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM bronze.prices WHERE product_id = $1", [product_id])
        now = datetime.now(UTC)
        for bar in bars:
            bar_date = datetime.combine(bar.date, datetime.min.time(), tzinfo=UTC)
            conn.execute(
                """
                INSERT INTO bronze.prices
                    (product_id, date, open, high, low, close, volume, average, bar_count, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
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
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _upsert_prices(conn: duckdb.DuckDBPyConnection, product_id: int, bars: list[BarData]) -> None:
    """Upsert price bars into bronze.prices inside a single transaction."""
    conn.execute("BEGIN TRANSACTION")
    try:
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
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


async def _fetch_historical(
    ib: Any,
    product_id: int,
    duration: str,
    end_datetime: str,
) -> list[BarData]:
    """Fetch historical bars for a single product using conId only."""
    contract = Contract(conId=product_id)
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


async def _fetch_and_store(
    worker: AsyncDbWorker,
    ib: Any,
    product_id: int,
    force: bool = False,
) -> None:
    """Fetch and store prices for one product."""
    last_date = await worker.submit(_get_last_date, product_id)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    end_datetime = yesterday.strftime("%Y%m%d") + " 23:59:59 UTC"

    if force or last_date is None:
        # Full refetch, replacing existing data
        duration = "30Y"
        bars = await _fetch_historical(ib, product_id, duration, end_datetime)
        await worker.submit(_replace_prices, product_id, bars)
        logger.info("Product %d: full price refetch complete (%d bars)", product_id, len(bars))
        return

    # Incremental fetch
    gap_days = (today - last_date).days
    duration = f"{gap_days + 7} D"
    bars = await _fetch_historical(ib, product_id, duration, end_datetime)
    new_bars = _extract_bars(bars)

    overlap_start = last_date - timedelta(days=7)
    valid, mismatch_type = await worker.submit(_validate_overlap, product_id, new_bars, overlap_start)

    if not valid:
        logger.warning(
            "Product %d: %s detected. Replacing with full refetch...",
            product_id,
            mismatch_type,
        )
        await _fetch_and_store(worker, ib, product_id, force=True)
        return

    await worker.submit(_upsert_prices, product_id, bars)
    logger.info("Product %d: incremental price update complete (%d bars)", product_id, len(bars))


async def _run_price_ingestion(force: bool = False) -> int:
    """Run the price ingestion phase."""
    async with AsyncDbWorker(settings.db_path) as worker:
        product_ids = await worker.submit(_select_product_ids)

        if not product_ids:
            raise RuntimeError("silver.products is empty. Run contracts phase first.")

        logger.info("Price ingestion: %d products to process", len(product_ids))

        semaphore = asyncio.Semaphore(1)

        async with ib_connection(client_id=2) as ib:

            async def process_one(product_id: int):
                async with semaphore:
                    try:
                        await _fetch_and_store(worker, ib, product_id, force)
                    except IBConnectionError:
                        raise
                    except Exception as e:
                        logger.error("Failed to fetch prices for product %d: %s", product_id, e)

            tasks = [process_one(pid) for pid in product_ids]
            await asyncio.gather(*tasks)

    return len(product_ids)


async def sync(force: bool = False) -> int:
    """Public entry point for the price phase."""
    return await _run_price_ingestion(force=force)
