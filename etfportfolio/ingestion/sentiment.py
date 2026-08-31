"""
Sentiment series fetching via web portal endpoint.
Handles incremental updates and overlap validation.
"""

import logging
import math
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
import httpx

from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingestion import endpoints, session

logger = logging.getLogger(__name__)

SENTIMENT_METRICS = ["svolatility", "sdispersion", "svscore", "sbuzz", "svolume", "sdelta", "sscore", "smean"]


def _get_last_date(conn: duckdb.DuckDBPyConnection, product_id: int) -> datetime | None:
    """Get the most recent date for a product in bronze.sentiment."""
    row = conn.execute(
        "SELECT MAX(date) FROM bronze.sentiment WHERE product_id = $1",
        [product_id],
    ).fetchone()
    return row[0] if row and row[0] else None


def _extract_sentiment_points(payload: dict[str, Any]) -> dict[datetime, dict[str, Any]]:
    """Extract sentiment points from payload, stripping price keys."""
    result = {}
    sentiment_list = payload.get("sentiment", [])
    for entry in sentiment_list:
        if "datetime" not in entry:
            continue
        ts = entry["datetime"]
        date = datetime.fromtimestamp(ts / 1000.0, tz=UTC).replace(tzinfo=None)
        point = {}
        for metric in SENTIMENT_METRICS:
            if metric in entry:
                point[metric] = entry[metric]
        result[date] = point
    return result


def _validate_overlap(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    new_points: dict[datetime, dict[str, Any]],
    overlap_start: datetime,
) -> tuple[bool, str | None]:
    """Validate overlapping dates/values against existing data."""
    existing_rows = conn.execute(
        """
        SELECT date, svolatility, sdispersion, svscore, sbuzz, svolume, sdelta, sscore, smean
        FROM bronze.sentiment
        WHERE product_id = $1 AND date >= $2
        """,
        [product_id, overlap_start],
    ).fetchall()

    existing_dict = {}
    for row in existing_rows:
        existing_dict[row[0]] = {
            "svolatility": row[1],
            "sdispersion": row[2],
            "svscore": row[3],
            "sbuzz": row[4],
            "svolume": row[5],
            "sdelta": row[6],
            "sscore": row[7],
            "smean": row[8],
        }

    for date in existing_dict:
        if date not in new_points:
            return False, "date_mismatch"

    for date, new_vals in new_points.items():
        if date in existing_dict:
            old_vals = existing_dict[date]
            for metric in SENTIMENT_METRICS:
                v1, v2 = old_vals.get(metric), new_vals.get(metric)
                if (
                    v1 is not None
                    and v2 is not None
                    and not math.isclose(float(v1), float(v2), rel_tol=1e-4, abs_tol=1e-4)
                ):
                    return False, "value_mismatch"

    return True, None


def _replace_sentiment(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    points: dict[datetime, dict[str, Any]],
) -> None:
    """Replace all sentiment rows for a product inside a single transaction."""
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM bronze.sentiment WHERE product_id = $1", [product_id])
        now = datetime.now(UTC)
        for date, point in points.items():
            conn.execute(
                """
                INSERT INTO bronze.sentiment
                    (product_id, date, svolatility, sdispersion, svscore, sbuzz, svolume, sdelta, sscore, smean, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                [
                    product_id,
                    date,
                    point.get("svolatility"),
                    point.get("sdispersion"),
                    point.get("svscore"),
                    point.get("sbuzz"),
                    point.get("svolume"),
                    point.get("sdelta"),
                    point.get("sscore"),
                    point.get("smean"),
                    now,
                ],
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _upsert_sentiment(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    points: dict[datetime, dict[str, Any]],
) -> None:
    """Upsert sentiment points into bronze.sentiment inside a single transaction."""
    conn.execute("BEGIN TRANSACTION")
    try:
        now = datetime.now(UTC)
        for date, point in points.items():
            conn.execute(
                """
                INSERT INTO bronze.sentiment
                    (product_id, date, svolatility, sdispersion, svscore, sbuzz, svolume, sdelta, sscore, smean, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (product_id, date) DO UPDATE SET
                    svolatility = EXCLUDED.svolatility,
                    sdispersion = EXCLUDED.sdispersion,
                    svscore = EXCLUDED.svscore,
                    sbuzz = EXCLUDED.sbuzz,
                    svolume = EXCLUDED.svolume,
                    sdelta = EXCLUDED.sdelta,
                    sscore = EXCLUDED.sscore,
                    smean = EXCLUDED.smean,
                    updated_at = EXCLUDED.updated_at
                """,
                [
                    product_id,
                    date,
                    point.get("svolatility"),
                    point.get("sdispersion"),
                    point.get("svscore"),
                    point.get("sbuzz"),
                    point.get("svolume"),
                    point.get("sdelta"),
                    point.get("sscore"),
                    point.get("smean"),
                    now,
                ],
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


async def _fetch_and_store(
    client: httpx.AsyncClient,
    worker: AsyncDbWorker,
    ep: endpoints.Endpoint,
    product_id: int,
    force: bool = False,
) -> None:
    """Fetch and store sentiment for one product."""
    last_date = await worker.submit(_get_last_date, product_id)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)

    if force or last_date is None:
        from_date = "1990-01-01"
        to_date = yesterday.strftime("%Y-%m-%d")
        url_prefix, url_slug, full_url = ep.resolve(
            product_id=product_id,
            from_date=from_date,
            to_date=to_date,
        )
        _, payload = await session.fetch_with_retry(client, full_url)
        new_points = _extract_sentiment_points(payload)
        await worker.submit(_replace_sentiment, product_id, new_points)
        logger.info("Product %d: full sentiment refetch complete (%d points)", product_id, len(new_points))
        return

    # Incremental fetch
    from_date = (last_date - timedelta(days=7)).strftime("%Y-%m-%d")
    to_date = yesterday.strftime("%Y-%m-%d")
    url_prefix, url_slug, full_url = ep.resolve(
        product_id=product_id,
        from_date=from_date,
        to_date=to_date,
    )
    _, payload = await session.fetch_with_retry(client, full_url)
    new_points = _extract_sentiment_points(payload)

    overlap_start = last_date - timedelta(days=7)
    valid, mismatch_type = await worker.submit(_validate_overlap, product_id, new_points, overlap_start)

    if not valid:
        logger.warning(
            "Product %d: %s detected. Replacing with full refetch...",
            product_id,
            mismatch_type,
        )
        await _fetch_and_store(client, worker, ep, product_id, force=True)
        return

    # Only persist points strictly newer than the existing latest date.
    points_to_store = {date: point for date, point in new_points.items() if date > last_date}

    if points_to_store:
        await worker.submit(_upsert_sentiment, product_id, points_to_store)
        logger.info(
            "Product %d: incremental sentiment update complete (%d new points)",
            product_id,
            len(points_to_store),
        )
    else:
        logger.info("Product %d: no new sentiment points to store", product_id)


async def fetch_incremental(
    client: httpx.AsyncClient,
    worker: AsyncDbWorker,
    ep: endpoints.Endpoint,
    product_id: int,
) -> None:
    """Public entry point for fetching sentiment for one product."""
    await _fetch_and_store(client, worker, ep, product_id)
