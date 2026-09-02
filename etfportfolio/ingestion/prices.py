"""
Historical daily price fetching via IB Gateway (clientId=2).
Handles incremental updates and overlap validation.
"""

import asyncio
import logging
import math
from dataclasses import dataclass
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


@dataclass(frozen=True)
class ProductContract:
    product_id: int
    symbol: str | None = None
    sec_type: str | None = None
    exchange_id: str | None = None
    primary_exchange_id: str | None = None
    currency: str | None = None
    local_symbol: str | None = None
    trading_class: str | None = None


def _select_target_products(
    conn: duckdb.DuckDBPyConnection,
    product_ids: str | None = None,
    limit: int | None = None,
) -> list[ProductContract]:
    if product_ids is not None and limit is not None:
        raise ValueError("product_ids and limit are mutually exclusive.")

    target_ids = None
    if product_ids is not None:
        from etfportfolio.ingestion.products import _parse_product_ids_arg

        target_ids = _parse_product_ids_arg(product_ids)

    query = """
    SELECT
        product_id,
        symbol,
        sec_type,
        exchange_id,
        primary_exchange_id,
        currency,
        local_symbol,
        trading_class
    FROM silver.products
    """
    if target_ids is not None:
        if not target_ids:
            return []
        ids_str = ", ".join(str(int(x)) for x in target_ids)
        query += f" WHERE product_id IN ({ids_str}) ORDER BY product_id"
    else:
        query += " ORDER BY product_id"
        if limit is not None and limit > 0:
            query += f" LIMIT {int(limit)}"

    rows = conn.execute(query).fetchall()
    return [
        ProductContract(
            product_id=row[0],
            symbol=row[1],
            sec_type=row[2],
            exchange_id=row[3],
            primary_exchange_id=row[4],
            currency=row[5],
            local_symbol=row[6],
            trading_class=row[7],
        )
        for row in rows
    ]


def _get_last_date(conn: duckdb.DuckDBPyConnection, product_id: int) -> datetime | None:
    row = conn.execute(
        "SELECT MAX(date) FROM bronze.prices WHERE product_id = $1",
        [product_id],
    ).fetchone()
    return row[0] if row and row[0] else None


def _extract_bars(
    bars: list[BarData],
    max_date: datetime | None = None,
) -> dict[datetime, dict[str, Any]]:
    """Convert IB BarData list into a dict keyed by naive UTC midnight datetime.

    If max_date is specified, bars with date > max_date are excluded to prevent
    storing incomplete/intraday bars for the current trading day.
    """
    result = {}
    for bar in bars:
        b_date = bar.date
        if isinstance(b_date, str):
            if len(b_date) == 8 and b_date.isdigit():
                bar_date = datetime.strptime(b_date, "%Y%m%d")
            else:
                bar_date = datetime.fromisoformat(b_date[:10])
        elif isinstance(b_date, datetime):
            bar_date = b_date.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        else:  # date
            bar_date = datetime.combine(b_date, datetime.min.time())

        if max_date is not None and bar_date > max_date:
            continue

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


def _replace_prices(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    points: dict[datetime, dict[str, Any]],
) -> None:
    """Replace all price rows for a product inside a single transaction."""
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM bronze.prices WHERE product_id = $1", [product_id])
        now = datetime.now(UTC).replace(tzinfo=None)
        for bar_date, point in points.items():
            conn.execute(
                """
                INSERT INTO bronze.prices
                    (product_id, date, open, high, low, close, volume, average, bar_count, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                [
                    product_id,
                    bar_date,
                    point.get("open"),
                    point.get("high"),
                    point.get("low"),
                    point.get("close"),
                    point.get("volume"),
                    point.get("average"),
                    point.get("bar_count"),
                    now,
                ],
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _upsert_prices(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    points: dict[datetime, dict[str, Any]],
) -> None:
    """Upsert price bars into bronze.prices inside a single transaction."""
    conn.execute("BEGIN TRANSACTION")
    try:
        now = datetime.now(UTC).replace(tzinfo=None)
        for bar_date, point in points.items():
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
                    point.get("open"),
                    point.get("high"),
                    point.get("low"),
                    point.get("close"),
                    point.get("volume"),
                    point.get("average"),
                    point.get("bar_count"),
                    now,
                ],
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


async def _fetch_historical(
    ib: Any,
    product: ProductContract,
    duration: str,
    end_datetime: str = "",
) -> list[BarData]:
    """Fetch historical bars for a single product using complete contract declaration."""
    sec_type = "STK" if product.sec_type in (None, "ETF", "FUND", "STK") else product.sec_type
    contract = Contract(
        conId=product.product_id,
        symbol=product.symbol or "",
        secType=sec_type,
        exchange="SMART",
        primaryExchange=product.primary_exchange_id or "",
        currency=product.currency or "",
        localSymbol=product.local_symbol or "",
        tradingClass=product.trading_class or "",
    )
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
    product: ProductContract,
    force: bool = False,
) -> None:
    """Fetch and store prices for one product."""
    last_date = await worker.submit(_get_last_date, product.product_id)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    yesterday = today - timedelta(days=1)

    if force or last_date is None:
        # Full refetch, replacing existing data
        duration = "30 Y"
        bars = await _fetch_historical(ib, product, duration, end_datetime="")
        new_bars = _extract_bars(bars, max_date=yesterday)
        if new_bars:
            await worker.submit(_replace_prices, product.product_id, new_bars)
            logger.info("Product %d: full price refetch complete (%d bars)", product.product_id, len(new_bars))
        else:
            logger.warning("Product %d: no price bars returned", product.product_id)
        return

    # Incremental fetch
    gap_days = (today - last_date).days
    duration = f"{max(gap_days + 7, 8)} D"
    bars = await _fetch_historical(ib, product, duration, end_datetime="")
    new_bars = _extract_bars(bars, max_date=yesterday)

    if not new_bars:
        logger.info("Product %d: no price bars returned for incremental update", product.product_id)
        return

    overlap_start = last_date - timedelta(days=7)
    valid, mismatch_type = await worker.submit(_validate_overlap, product.product_id, new_bars, overlap_start)

    if not valid:
        logger.warning(
            "Product %d: %s detected. Replacing with full refetch...",
            product.product_id,
            mismatch_type,
        )
        await _fetch_and_store(worker, ib, product, force=True)
        return

    # Only persist points strictly newer than the existing latest date
    points_to_store = {date: point for date, point in new_bars.items() if date > last_date}

    if points_to_store:
        await worker.submit(_upsert_prices, product.product_id, points_to_store)
        logger.info(
            "Product %d: incremental price update complete (%d new bars)",
            product.product_id,
            len(points_to_store),
        )
    else:
        logger.info("Product %d: no new price bars to store", product.product_id)


async def _run_price_ingestion(
    product_ids: str | None = None,
    limit: int | None = None,
    force: bool = False,
) -> int:
    """Run the price ingestion phase."""
    async with AsyncDbWorker(settings.db_path) as worker:
        products = await worker.submit(_select_target_products, product_ids, limit)

        if not products:
            if product_ids or limit:
                logger.warning("No matching products found in silver.products.")
                return 0
            raise RuntimeError("silver.products is empty. Run contracts phase first.")

        logger.info("Price ingestion: %d products to process", len(products))

        semaphore = asyncio.Semaphore(1)

        async with ib_connection(client_id=2) as ib:

            async def process_one(product: ProductContract):
                async with semaphore:
                    if not ib.isConnected():
                        raise IBConnectionError("IB Gateway connection was lost during price ingestion.")
                    try:
                        await _fetch_and_store(worker, ib, product, force)
                    except IBConnectionError:
                        raise
                    except Exception as e:
                        logger.error("Failed to fetch prices for product %d: %s", product.product_id, e)

            tasks = [process_one(p) for p in products]
            await asyncio.gather(*tasks)

    return len(products)


async def sync(
    product_ids: str | None = None,
    limit: int | None = None,
    force: bool = False,
) -> int:
    """Public entry point for the price phase."""
    return await _run_price_ingestion(product_ids=product_ids, limit=limit, force=force)

