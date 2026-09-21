"""
Historical daily price fetching via IB Gateway (clientId=2).
Handles incremental updates, overlap validation, and status tracking.
"""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime, timedelta
from typing import Any

from ib_async import BarData, Contract

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.core.logging import console
from etfportfolio.core.progress import progress_bar
from etfportfolio.ingest.gateway import IBConnectionError, ib_connection
from etfportfolio.ingest.series import (
    FETCH_MARGIN_DAYS,
    MIN_REFETCH_RETENTION_RATIO,
    OVERLAP_CALENDAR_DAYS,
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
from etfportfolio.ingest.utils import ProductContract

logger = logging.getLogger(__name__)

WHAT_TO_SHOW = "ADJUSTED_LAST"
BAR_SIZE = "1 day"

PRICE_REL_TOL = 1e-4  # 1 basis point
PRICE_ABS_TOL = 0.01  # 1 cent

SETTLEMENT_REL_TOL = 0.01  # 1.00% max tolerance for recent settlement drift
SETTLEMENT_ABS_TOL = 0.10  # $0.10 max absolute drift for recent settlement prints
SETTLEMENT_TRADING_DAYS = 5  # Last 4-5 trading days horizon for settlement drift

PRICES_SPEC = SeriesSpec(
    bronze_table="bronze.prices",
    cold_table="cold_storage.prices",
    status_table="bronze.price_status",
    entity_columns=("product_id",),
    data_columns=("open", "high", "low", "close", "volume", "average", "bar_count"),
    tolerance_columns=("open", "high", "low", "close"),
    rel_tol=PRICE_REL_TOL,
    abs_tol=PRICE_ABS_TOL,
    settlement_rel_tol=SETTLEMENT_REL_TOL,
    settlement_abs_tol=SETTLEMENT_ABS_TOL,
    settlement_trading_days=SETTLEMENT_TRADING_DAYS,
    detect_splits=True,
)


def format_duration(days: int) -> str:
    """Format duration string for IB reqHistoricalDataAsync.

    Days <= 365 use 'D' (minimum 10 D for margin).
    Days > 365 must use 'Y' (up to 30 Y).
    """
    if days <= 365:
        return f"{max(days, 10)} D"
    years = math.ceil(days / 365.25)
    return f"{min(years, 30)} Y"


def _extract_bars(
    bars: list[BarData],
    max_date: datetime | None = None,
) -> dict[datetime, dict[str, Any]]:
    """Convert IB BarData list into a dict keyed by naive UTC midnight datetime."""
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
        else:
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
    exchange = product.exchange_id or "SMART"
    contract = Contract(
        conId=product.product_id,
        symbol=product.symbol or "",
        secType=sec_type,
        exchange=exchange,
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
) -> None:
    """Fetch and store prices for one product.

    Initial fetch pulls 30 Y. Incremental fetch calculates duration with
    14-day overlap and 2-day margin, replacing and archiving on mismatch.
    """
    last_date = await worker.submit(get_last_date, PRICES_SPEC, product.product_id)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    yesterday = today - timedelta(days=1)

    if last_date is None:
        duration = "30 Y"
        bars = await _fetch_historical(ib, product, duration, end_datetime="")
        new_bars = _extract_bars(bars, max_date=yesterday)
        if new_bars:
            await worker.submit(replace_series, PRICES_SPEC, product.product_id, new_bars, archive=False)
            await worker.submit(record_series_status, PRICES_SPEC, product.product_id, "ok", None)
            logger.info("Product %d: full price refetch complete (%d bars)", product.product_id, len(new_bars))
        else:
            await worker.submit(record_series_status, PRICES_SPEC, product.product_id, "no_data", None)
            logger.warning("Product %d: no price bars returned", product.product_id)
        return

    gap_days = (today - last_date).days
    duration = format_duration(gap_days + OVERLAP_CALENDAR_DAYS + FETCH_MARGIN_DAYS)
    bars = await _fetch_historical(ib, product, duration, end_datetime="")
    new_bars = _extract_bars(bars, max_date=yesterday)

    if not new_bars:
        await worker.submit(record_series_status, PRICES_SPEC, product.product_id, "no_data", None)
        logger.info("Product %d: no price bars returned for incremental update", product.product_id)
        return

    valid, mismatch_type = await worker.submit(validate_overlap, PRICES_SPEC, product.product_id, new_bars, last_date)

    if not valid:
        logger.warning(
            "Product %d: %s detected. Refetching full 30Y history...",
            product.product_id,
            mismatch_type,
        )
        existing_count = await worker.submit(get_series_count, PRICES_SPEC, product.product_id)

        full_bars_raw = await _fetch_historical(ib, product, "30 Y", end_datetime="")
        full_bars = _extract_bars(full_bars_raw, max_date=yesterday)

        min_expected_bars = (
            max(1, math.floor(existing_count * MIN_REFETCH_RETENTION_RATIO)) if existing_count > 5 else 1
        )

        if len(full_bars) < min_expected_bars:
            err_msg = (
                f"Truncated refetch: received {len(full_bars)} bars, "
                f"expected >= {min_expected_bars} (existing: {existing_count})"
            )
            logger.error(
                "Product %d: %s. Aborting replace to prevent data loss. Preserving existing bronze rows.",
                product.product_id,
                err_msg,
            )
            await worker.submit(record_series_status, PRICES_SPEC, product.product_id, "error", err_msg)
            return

        if full_bars:
            overlap_start = overlap_start_for(last_date)
            hist_changed = await worker.submit(
                has_historical_series_change,
                PRICES_SPEC,
                product.product_id,
                full_bars,
                overlap_start,
            )
            if hist_changed:
                await worker.submit(
                    replace_series,
                    PRICES_SPEC,
                    product.product_id,
                    full_bars,
                    archive=True,
                    reason=mismatch_type,
                )
                logger.info(
                    "Product %d: corporate action confirmed; archived old series and replaced (%d bars)",
                    product.product_id,
                    len(full_bars),
                )
            else:
                await worker.submit(
                    replace_series,
                    PRICES_SPEC,
                    product.product_id,
                    full_bars,
                    archive=False,
                )
                logger.info(
                    "Product %d: historical bars identical; replaced bronze WITHOUT cold storage archival (%d bars)",
                    product.product_id,
                    len(full_bars),
                )
            await worker.submit(record_series_status, PRICES_SPEC, product.product_id, "ok", None)
        else:
            await worker.submit(record_series_status, PRICES_SPEC, product.product_id, "no_data", None)
            logger.warning(
                "Product %d: full refetch returned no bars after mismatch. Preserving existing rows.",
                product.product_id,
            )
        return

    overlap_start = overlap_start_for(last_date)
    points_to_store = {d: pt for d, pt in new_bars.items() if d >= overlap_start}
    await worker.submit(upsert_series, PRICES_SPEC, product.product_id, points_to_store)
    await worker.submit(record_series_status, PRICES_SPEC, product.product_id, "ok", None)
    logger.info("Product %d: incremental price update complete (%d bars)", product.product_id, len(points_to_store))


async def _run_price_ingestion(force: bool = False) -> int:
    async with AsyncDbWorker(settings.db_path) as worker:
        from etfportfolio.ingest.products import resolve_target_products

        products = await worker.submit(resolve_target_products)
        if not products:
            return 0

        if force:
            to_process = products
        else:
            status_cache = await worker.submit(load_series_status, PRICES_SPEC)
            today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
            yesterday = today - timedelta(days=1)
            to_process = [
                p
                for p in products
                if not is_series_fresh(status_cache.get(p.product_id), yesterday, settings.freshness_window_hours)
            ]

        skipped = len(products) - len(to_process)
        if skipped:
            console.info(f"{skipped}/{len(products)} products up to date, {len(to_process)} to process.")
        else:
            console.info(f"Processing {len(products)} product(s)...")

        if not to_process:
            console.info("Done. All products are up to date.")
            return 0

        logger.info("Price ingestion: %d products to process", len(to_process))

        async with ib_connection(client_id=2) as ib:
            with progress_bar(len(to_process), desc="Prices") as bar:
                for product in to_process:
                    bar.set_postfix_str(str(product.product_id))
                    if not ib.isConnected():
                        raise IBConnectionError("IB Gateway connection was lost during price ingestion.")
                    try:
                        await _fetch_and_store(worker, ib, product)
                    except IBConnectionError:
                        raise
                    except Exception as e:
                        logger.error("Failed to fetch prices for product %d: %s", product.product_id, e)
                        await worker.submit(record_series_status, PRICES_SPEC, product.product_id, "error", str(e))
                    finally:
                        bar.update(1)

        return len(to_process)


async def sync(force: bool = False) -> int:
    """Public entry point for the price phase."""
    return await _run_price_ingestion(force=force)
