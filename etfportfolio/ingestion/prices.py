"""
Historical daily price fetching via IB Gateway (clientId=2).
Handles incremental updates, overlap validation, and status tracking.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
from ib_async import BarData, Contract

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.core.logging import console
from etfportfolio.core.progress import progress_bar
from etfportfolio.ingestion.gateway import IBConnectionError, ib_connection
from etfportfolio.ingestion.utils import ProductContract, is_fresh

logger = logging.getLogger(__name__)

WHAT_TO_SHOW = "ADJUSTED_LAST"
BAR_SIZE = "1 day"
OVERLAP_CALENDAR_DAYS = 7
FETCH_MARGIN_DAYS = 2


@dataclass(frozen=True)
class SeriesSpec:
    """Table/column layout for one bronze timeseries + its cold_storage archive."""

    bronze_table: str
    cold_table: str
    columns: tuple[str, ...]
    value_columns: tuple[str, ...]


PRICES_SPEC = SeriesSpec(
    bronze_table="bronze.prices",
    cold_table="cold_storage.prices",
    columns=("open", "high", "low", "close", "volume", "average", "bar_count"),
    value_columns=("open", "high", "low", "close", "volume", "average"),
)


@dataclass(frozen=True)
class PriceSeriesStatus:
    last_date: datetime | None
    last_updated: datetime | None  # MAX(bronze.prices.updated_at)
    last_checked_at: datetime | None  # MAX(bronze.price_status.last_checked_at)
    status: str | None  # 'ok', 'no_data', 'error', or None


def format_duration(days: int) -> str:
    """Format duration string for IB reqHistoricalDataAsync.

    Days <= 365 use 'D' (minimum 10 D for margin).
    Days > 365 must use 'Y' (up to 30 Y).
    """
    if days <= 365:
        return f"{max(days, 10)} D"
    years = math.ceil(days / 365.25)
    return f"{min(years, 30)} Y"


def overlap_start_for(last_date: datetime) -> datetime:
    """Closed-closed window W starts at last_date minus OVERLAP_CALENDAR_DAYS."""
    return last_date - timedelta(days=OVERLAP_CALENDAR_DAYS)


def validate_overlap(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    product_id: int,
    new_points: dict[datetime, dict[str, Any]],
    last_date: datetime,
) -> tuple[bool, str | None]:
    """Set-equality checksum on W = [last_date - 7d, last_date] (closed-closed).

    Dates outside W are ignored (fetch margin before W; new tail after last_date).
    Value columns are compared with math.isclose; missing/None on either side is skipped.
    Returns (is_valid, mismatch_type) where mismatch_type is None, 'date_mismatch',
    or 'value_mismatch'.
    """
    start = overlap_start_for(last_date)
    col_sql = ", ".join(("date", *spec.columns))
    existing_rows = conn.execute(
        f"""
        SELECT {col_sql}
        FROM {spec.bronze_table}
        WHERE product_id = $1 AND date >= $2 AND date <= $3
        """,
        [product_id, start, last_date],
    ).fetchall()

    existing_dates: set[datetime] = set()
    existing_vals: dict[datetime, dict[str, Any]] = {}
    for row in existing_rows:
        d = row[0]
        existing_dates.add(d)
        existing_vals[d] = {col: row[i + 1] for i, col in enumerate(spec.columns)}

    new_in_w = {d: vals for d, vals in new_points.items() if start <= d <= last_date}
    new_dates = set(new_in_w)

    if existing_dates != new_dates:
        return False, "date_mismatch"

    for d, new_vals in new_in_w.items():
        old_vals = existing_vals[d]
        for key in spec.value_columns:
            v1, v2 = old_vals.get(key), new_vals.get(key)
            if v1 is not None and v2 is not None and not math.isclose(float(v1), float(v2), rel_tol=1e-4, abs_tol=1e-4):
                return False, "value_mismatch"

    return True, None


def _insert_points(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    product_id: int,
    points: dict[datetime, dict[str, Any]],
    now: datetime,
) -> None:
    col_sql = ", ".join(("product_id", "date", *spec.columns, "updated_at"))
    placeholders = ", ".join(f"${i + 1}" for i in range(len(spec.columns) + 3))
    sql = f"INSERT INTO {spec.bronze_table} ({col_sql}) VALUES ({placeholders})"
    for bar_date, point in points.items():
        params: list[Any] = [product_id, bar_date]
        params.extend(point.get(col) for col in spec.columns)
        params.append(now)
        conn.execute(sql, params)


def replace_series(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    product_id: int,
    points: dict[datetime, dict[str, Any]],
    *,
    archive: bool = False,
    reason: str | None = None,
) -> None:
    """Replace all bronze rows for a product. Optionally archive first (same txn).

    `archive=True` is only for mismatch-triggered replace. `--force` and first
    fill pass archive=False.
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    conn.execute("BEGIN TRANSACTION")
    try:
        if archive:
            if not reason:
                raise ValueError("archive=True requires a mismatch reason")
            col_sql = ", ".join(("product_id", "run_id", "date", *spec.columns, "reason"))
            select_cols = ", ".join(("product_id", "$2", "date", *spec.columns, "$3"))
            conn.execute(
                f"""
                INSERT INTO {spec.cold_table} ({col_sql})
                SELECT {select_cols}
                FROM {spec.bronze_table}
                WHERE product_id = $1
                """,
                [product_id, now, reason],
            )
        conn.execute(
            f"DELETE FROM {spec.bronze_table} WHERE product_id = $1",
            [product_id],
        )
        _insert_points(conn, spec, product_id, points, now)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def upsert_series(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    product_id: int,
    points: dict[datetime, dict[str, Any]],
) -> None:
    """Upsert points (the incremental tail, date > last_date). Overlap is not written."""
    now = datetime.now(UTC).replace(tzinfo=None)
    col_sql = ", ".join(("product_id", "date", *spec.columns, "updated_at"))
    placeholders = ", ".join(f"${i + 1}" for i in range(len(spec.columns) + 3))
    assignments = ", ".join(f"{col} = EXCLUDED.{col}" for col in (*spec.columns, "updated_at"))
    sql = f"""
    INSERT INTO {spec.bronze_table} ({col_sql})
    VALUES ({placeholders})
    ON CONFLICT (product_id, date) DO UPDATE SET
        {assignments}
    """
    conn.execute("BEGIN TRANSACTION")
    try:
        for bar_date, point in points.items():
            params: list[Any] = [product_id, bar_date]
            params.extend(point.get(col) for col in spec.columns)
            params.append(now)
            conn.execute(sql, params)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _record_price_status(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    status: str,
    error_message: str | None = None,
) -> None:
    now = datetime.now(UTC).replace(tzinfo=None)
    truncated_msg = error_message[:500] if error_message else None
    conn.execute(
        """
        INSERT INTO bronze.price_status (product_id, last_checked_at, status, error_message)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (product_id) DO UPDATE SET
            last_checked_at = EXCLUDED.last_checked_at,
            status = EXCLUDED.status,
            error_message = EXCLUDED.error_message
        """,
        [product_id, now, status, truncated_msg],
    )


def _load_price_series_status(conn: duckdb.DuckDBPyConnection) -> dict[int, PriceSeriesStatus]:
    """Load product_id -> PriceSeriesStatus from bronze tables."""
    rows = conn.execute(
        """
        SELECT
            c.product_id,
            MAX(pr.date) AS last_date,
            MAX(pr.updated_at) AS last_updated,
            MAX(ps.last_checked_at) AS last_checked_at,
            MAX(ps.status) AS status
        FROM bronze.contracts c
        LEFT JOIN bronze.prices pr ON c.product_id = pr.product_id
        LEFT JOIN bronze.price_status ps ON c.product_id = ps.product_id
        GROUP BY c.product_id
        """
    ).fetchall()
    return {
        row[0]: PriceSeriesStatus(
            last_date=row[1],
            last_updated=row[2],
            last_checked_at=row[3],
            status=row[4],
        )
        for row in rows
    }


def is_series_fresh(
    status: PriceSeriesStatus | None,
    target_date: datetime,
    hours: float,
) -> bool:
    """Evaluate price freshness with differentiated status dampening.

    1. Fresh if price bars reach target_date (yesterday).
    2. Fresh if checked within hours and status was 'ok' or 'no_data'.
    3. Fresh if last prices were updated within hours and status was 'ok' or 'no_data' (fallback).
    4. An 'error' or None status is NEVER fresh; retried on subsequent runs.
    """
    if not status:
        return False
    if status.last_date is not None and status.last_date >= target_date:
        return True
    if status.status in ("ok", "no_data"):
        return is_fresh(status.last_checked_at, hours) or is_fresh(status.last_updated, hours)
    return False


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
    7-day overlap and 2-day margin, replacing and archiving on mismatch.
    """
    last_date = await worker.submit(_get_last_date, product.product_id)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    yesterday = today - timedelta(days=1)

    if last_date is None:
        duration = "30 Y"
        bars = await _fetch_historical(ib, product, duration, end_datetime="")
        new_bars = _extract_bars(bars, max_date=yesterday)
        if new_bars:
            await worker.submit(replace_series, PRICES_SPEC, product.product_id, new_bars, archive=False)
            await worker.submit(_record_price_status, product.product_id, "ok", None)
            logger.info("Product %d: full price refetch complete (%d bars)", product.product_id, len(new_bars))
        else:
            await worker.submit(_record_price_status, product.product_id, "no_data", None)
            logger.warning("Product %d: no price bars returned", product.product_id)
        return

    gap_days = (today - last_date).days
    duration = format_duration(gap_days + 9)  # 7 days overlap + 2 days fetch margin
    bars = await _fetch_historical(ib, product, duration, end_datetime="")
    new_bars = _extract_bars(bars, max_date=yesterday)

    if not new_bars:
        await worker.submit(_record_price_status, product.product_id, "no_data", None)
        logger.info("Product %d: no price bars returned for incremental update", product.product_id)
        return

    valid, mismatch_type = await worker.submit(validate_overlap, PRICES_SPEC, product.product_id, new_bars, last_date)

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
            await worker.submit(_record_price_status, product.product_id, "ok", None)
            logger.info(
                "Product %d: mismatch refetch archived and replaced (%d bars)",
                product.product_id,
                len(full_bars),
            )
        else:
            await worker.submit(_record_price_status, product.product_id, "no_data", None)
            logger.warning(
                "Product %d: full refetch returned no bars after mismatch. Preserving existing rows.",
                product.product_id,
            )
        return

    overlap_start = overlap_start_for(last_date)
    points_to_store = {d: pt for d, pt in new_bars.items() if d >= overlap_start}
    await worker.submit(upsert_series, PRICES_SPEC, product.product_id, points_to_store)
    await worker.submit(_record_price_status, product.product_id, "ok", None)
    logger.info("Product %d: incremental price update complete (%d bars)", product.product_id, len(points_to_store))


async def _run_price_ingestion(force: bool = False) -> int:
    async with AsyncDbWorker(settings.db_path) as worker:
        from etfportfolio.ingestion.products import resolve_target_products

        products = await worker.submit(resolve_target_products)
        if not products:
            return 0

        if force:
            to_process = products
        else:
            status_cache = await worker.submit(_load_price_series_status)
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
                        await worker.submit(_record_price_status, product.product_id, "error", str(e))
                    finally:
                        bar.update(1)

        return len(to_process)


async def sync(force: bool = False) -> int:
    """Public entry point for the price phase."""
    return await _run_price_ingestion(force=force)
