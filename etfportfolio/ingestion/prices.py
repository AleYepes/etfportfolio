"""
Historical daily price fetching via IB Gateway (clientId=2).
Handles incremental updates and overlap validation.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
from ib_async import BarData, Contract

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.core.progress import progress_bar
from etfportfolio.ingestion.gateway import IBConnectionError, ib_connection
from etfportfolio.ingestion.utils import (
    PRICES_SPEC,
    replace_series,
    upsert_series,
    validate_overlap,
)

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
        duration = "30 Y"
        bars = await _fetch_historical(ib, product, duration, end_datetime="")
        new_bars = _extract_bars(bars, max_date=yesterday)
        if new_bars:
            await worker.submit(replace_series, PRICES_SPEC, product.product_id, new_bars, archive=False)
            logger.info("Product %d: full price refetch complete (%d bars)", product.product_id, len(new_bars))
        else:
            logger.warning("Product %d: no price bars returned", product.product_id)
        return

    # Incremental fetch
    gap_days = (today - last_date).days
    duration = f"{max(gap_days + 7 + 2, 10)} D"
    bars = await _fetch_historical(ib, product, duration, end_datetime="")
    new_bars = _extract_bars(bars, max_date=yesterday)

    if not new_bars:
        logger.info("Product %d: no price bars returned for incremental update", product.product_id)
        return

    valid, mismatch_type = await worker.submit(
        validate_overlap, PRICES_SPEC, product.product_id, new_bars, last_date
    )

    if not valid:
        logger.warning(
            "Product %d: %s detected. Replacing with full refetch and archiving...",
            product.product_id,
            mismatch_type,
        )
        full_bars_raw = await _fetch_historical(ib, product, "30 Y", end_datetime="")
        full_bars = _extract_bars(full_bars_raw, max_date=yesterday)
        if full_bars:
            await worker.submit(
                replace_series,
                PRICES_SPEC,
                product.product_id,
                full_bars,
                archive=True,
                reason=mismatch_type,
            )
            logger.info(
                "Product %d: mismatch full refetch archived and replaced (%d bars)",
                product.product_id,
                len(full_bars),
            )
        else:
            logger.warning(
                "Product %d: full refetch returned no bars after mismatch. Preserving existing rows.",
                product.product_id,
            )
        return

    # Only persist points strictly newer than the existing latest date
    points_to_store = {date: point for date, point in new_bars.items() if date > last_date}

    if points_to_store:
        await worker.submit(upsert_series, PRICES_SPEC, product.product_id, points_to_store)
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

        async with ib_connection(client_id=2) as ib:
            with progress_bar(len(products), desc="Prices") as bar:
                for product in products:
                    bar.set_postfix_str(str(product.product_id))
                    if not ib.isConnected():
                        raise IBConnectionError("IB Gateway connection was lost during price ingestion.")
                    try:
                        await _fetch_and_store(worker, ib, product, force)
                    except IBConnectionError:
                        raise
                    except Exception as e:
                        logger.error("Failed to fetch prices for product %d: %s", product.product_id, e)
                    finally:
                        bar.update(1)

        return len(products)


async def sync(
    product_ids: str | None = None,
    limit: int | None = None,
    force: bool = False,
) -> int:
    """Public entry point for the price phase."""
    return await _run_price_ingestion(product_ids=product_ids, limit=limit, force=force)

