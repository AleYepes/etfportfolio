"""
Sentiment series fetching via web portal endpoint.
Handles incremental updates, overlap validation, and cold storage archiving.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
import httpx

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.core.progress import progress_bar
from etfportfolio.ingestion import endpoints, session
from etfportfolio.ingestion.utils import (
    SENTIMENT_SPEC,
    replace_series,
    upsert_series,
    validate_overlap,
)

logger = logging.getLogger(__name__)

SENTIMENT_METRICS = ["svolatility", "sdispersion", "svscore", "sbuzz", "svolume", "sdelta", "sscore", "smean"]


def _get_last_date(conn: duckdb.DuckDBPyConnection, product_id: int) -> datetime | None:
    """Get the most recent date for a product in bronze.sentiment."""
    row = conn.execute(
        "SELECT MAX(date) FROM bronze.sentiment WHERE product_id = $1",
        [product_id],
    ).fetchone()
    return row[0] if row and row[0] else None


def _extract_sentiment_points(payload: dict[str, Any] | None) -> dict[datetime, dict[str, Any]]:
    """Extract sentiment points from payload, stripping price keys."""
    if not payload:
        return {}
    result = {}
    sentiment_list = payload.get("sentiment", [])
    if not isinstance(sentiment_list, list):
        return {}
    for entry in sentiment_list:
        if not isinstance(entry, dict) or "datetime" not in entry:
            continue
        ts = entry["datetime"]
        date = datetime.fromtimestamp(ts / 1000.0, tz=UTC).replace(tzinfo=None)
        point = {}
        for metric in SENTIMENT_METRICS:
            if metric in entry:
                point[metric] = entry[metric]
        result[date] = point
    return result


async def _fetch_and_store(
    client: httpx.AsyncClient,
    worker: AsyncDbWorker,
    ep: endpoints.Endpoint,
    product_id: int,
    force: bool = False,
) -> None:
    """Fetch and store sentiment for one product."""
    last_date = await worker.submit(_get_last_date, product_id)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    yesterday = today - timedelta(days=1)

    if force or last_date is None:
        from_date = "1990-01-01"
        to_date = yesterday.strftime("%Y-%m-%d")
        _, _, full_url = ep.resolve(
            product_id=product_id,
            from_date=from_date,
            to_date=to_date,
        )
        status, payload = await session.fetch_with_retry(client, full_url)
        if status == 404 or not payload:
            logger.info("Product %d: sentiment 404 or empty payload", product_id)
            return

        new_points = _extract_sentiment_points(payload)
        if not new_points:
            logger.info("Product %d: no sentiment points in payload", product_id)
            return

        await worker.submit(replace_series, SENTIMENT_SPEC, product_id, new_points, archive=False)
        logger.info("Product %d: full sentiment refetch complete (%d points)", product_id, len(new_points))
        return

    # Incremental fetch
    overlap_start = last_date - timedelta(days=7)
    from_date = (overlap_start - timedelta(days=2)).strftime("%Y-%m-%d")
    to_date = yesterday.strftime("%Y-%m-%d")
    _, _, full_url = ep.resolve(
        product_id=product_id,
        from_date=from_date,
        to_date=to_date,
    )
    status, payload = await session.fetch_with_retry(client, full_url)
    if status == 404 or not payload:
        logger.info("Product %d: sentiment 404 or empty on incremental fetch", product_id)
        return

    new_points = _extract_sentiment_points(payload)
    if not new_points:
        logger.info("Product %d: no sentiment points returned for incremental update", product_id)
        return

    valid, mismatch_type = await worker.submit(
        validate_overlap, SENTIMENT_SPEC, product_id, new_points, last_date
    )

    if not valid:
        logger.warning(
            "Product %d: %s detected in sentiment. Replacing with full refetch and archiving...",
            product_id,
            mismatch_type,
        )
        from_date = "1990-01-01"
        to_date = yesterday.strftime("%Y-%m-%d")
        _, _, full_url = ep.resolve(
            product_id=product_id,
            from_date=from_date,
            to_date=to_date,
        )
        status, full_payload = await session.fetch_with_retry(client, full_url)
        if status != 404 and full_payload:
            full_points = _extract_sentiment_points(full_payload)
            if full_points:
                await worker.submit(
                    replace_series,
                    SENTIMENT_SPEC,
                    product_id,
                    full_points,
                    archive=True,
                    reason=mismatch_type,
                )
                logger.info(
                    "Product %d: sentiment mismatch full refetch archived and replaced (%d points)",
                    product_id,
                    len(full_points),
                )
                return
        logger.warning(
            "Product %d: full refetch returned no sentiment points after mismatch. Preserving existing rows.",
            product_id,
        )
        return

    # Only persist points strictly newer than the existing latest date.
    points_to_store = {date: point for date, point in new_points.items() if date > last_date}

    if points_to_store:
        await worker.submit(upsert_series, SENTIMENT_SPEC, product_id, points_to_store)
        logger.info(
            "Product %d: incremental sentiment update complete (%d new points)",
            product_id,
            len(points_to_store),
        )
    else:
        logger.info("Product %d: no new sentiment points to store", product_id)


async def _run_sentiment_ingestion(
    client: httpx.AsyncClient,
    account_id: str,
    product_ids: str | None = None,
    limit: int | None = None,
    force: bool = False,
) -> int:
    """Run the sentiment ingestion phase."""
    async with AsyncDbWorker(settings.db_path) as worker:
        from etfportfolio.ingestion.products import resolve_target_ids

        target_ids = await worker.submit(resolve_target_ids, product_ids, limit)
        if not target_ids:
            return 0

        logger.info("Sentiment ingestion: %d products to process", len(target_ids))
        ep = endpoints.ENDPOINTS_BY_NAME["sentiment"]
        semaphore = asyncio.Semaphore(settings.sentiment_concurrency)

        with progress_bar(len(target_ids), desc="Sentiment") as bar:

            async def process_one(product_id: int):
                async with semaphore:
                    bar.set_postfix_str(str(product_id))
                    try:
                        await _fetch_and_store(client, worker, ep, product_id, force=force)
                    except session.SessionInvalidError:
                        logger.error("Session became invalid during sentiment ingestion for product %d", product_id)
                        raise
                    except Exception as e:
                        logger.error("Failed to fetch sentiment for product %d: %s", product_id, e)
                    finally:
                        bar.update(1)

            tasks = [process_one(pid) for pid in target_ids]
            await asyncio.gather(*tasks)

    return len(target_ids)


async def sync(
    client: httpx.AsyncClient | None = None,
    account_id: str | None = None,
    product_ids: str | None = None,
    limit: int | None = None,
    force: bool = False,
) -> int:
    """Public entry point for the sentiment phase."""
    close_client = False
    if client is None or account_id is None:
        client, account_id = await session.ensure_session()
        close_client = True

    try:
        return await _run_sentiment_ingestion(
            client=client,
            account_id=account_id,
            product_ids=product_ids,
            limit=limit,
            force=force,
        )
    finally:
        if close_client and client:
            await client.aclose()
