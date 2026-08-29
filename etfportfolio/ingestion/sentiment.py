"""
Sentiment series fetching via web portal endpoint.
Handles incremental updates, overlap validation, and cold storage archiving.
"""

import logging
import math
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
import httpx

from etfportfolio.ingestion import endpoints, session

logger = logging.getLogger(__name__)

# Sentiment metrics to extract
SENTIMENT_METRICS = ["svolatility", "sdispersion", "svscore", "sbuzz", "svolume", "sdelta", "sscore", "smean"]


def _get_last_date(conn: duckdb.DuckDBPyConnection, product_id: int) -> datetime | None:
    """Get the most recent date for a product in bronze.sentiment."""
    row = conn.execute(
        "SELECT MAX(date) FROM bronze.sentiment WHERE product_id = $1",
        [product_id],
    ).fetchone()
    return row[0] if row and row[0] else None


def _archive_sentiment(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    rows: list[dict[str, Any]],
    run_id: datetime,
) -> None:
    """Archive existing sentiment rows to cold_storage.sentiment."""
    if not rows:
        return

    for row in rows:
        conn.execute(
            """
            INSERT INTO cold_storage.sentiment
            (product_id, run_id, date, svolatility, sdispersion, svscore, sbuzz, svolume, sdelta, sscore, smean)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (product_id, run_id, date) DO NOTHING
            """,
            [
                product_id,
                run_id,
                row["date"],
                row.get("svolatility"),
                row.get("sdispersion"),
                row.get("svscore"),
                row.get("sbuzz"),
                row.get("svolume"),
                row.get("sdelta"),
                row.get("sscore"),
                row.get("smean"),
            ],
        )


def _delete_sentiment(conn: duckdb.DuckDBPyConnection, product_id: int) -> None:
    """Delete all sentiment rows for a product."""
    conn.execute("DELETE FROM bronze.sentiment WHERE product_id = $1", [product_id])


def _extract_sentiment_points(payload: dict[str, Any]) -> dict[datetime, dict[str, Any]]:
    """Extract sentiment points from payload, stripping price keys."""
    result = {}
    sentiment_list = payload.get("sentiment", [])
    for entry in sentiment_list:
        if "datetime" not in entry:
            continue
        ts = entry["datetime"]
        # Convert milliseconds to datetime (UTC midnight)
        date = datetime.fromtimestamp(ts / 1000.0, tz=UTC).replace(tzinfo=None)
        # Only keep sentiment metrics
        point = {}
        for metric in SENTIMENT_METRICS:
            if metric in entry:
                point[metric] = entry[metric]
        result[date] = point
    return result


def _upsert_sentiment(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    points: dict[datetime, dict[str, Any]],
) -> None:
    """Upsert sentiment points into bronze.sentiment."""
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

    # Date mismatch check
    for date in existing_dict:
        if date not in new_points:
            return False, "date_mismatch"

    # Value mismatch check
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


def _is_identical(
    existing: dict[datetime, dict[str, Any]],
    new: dict[datetime, dict[str, Any]],
) -> bool:
    """Check if two sentiment series are identical."""
    if set(existing.keys()) != set(new.keys()):
        return False
    for date, new_vals in new.items():
        old_vals = existing.get(date)
        if old_vals is None:
            return False
        for metric in SENTIMENT_METRICS:
            v1, v2 = old_vals.get(metric), new_vals.get(metric)
            if v1 is not None and v2 is not None and not math.isclose(float(v1), float(v2), rel_tol=1e-4, abs_tol=1e-4):
                return False
    return True


async def _fetch_and_store(
    client: httpx.AsyncClient,
    conn: duckdb.DuckDBPyConnection,
    ep: endpoints.Endpoint,
    product_id: int,
    force: bool = False,
) -> bool:
    """Fetch and store sentiment for one product."""
    last_date = _get_last_date(conn, product_id)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)

    if force or last_date is None:
        # Full refetch
        from_date = "1990-01-01"
        to_date = yesterday.strftime("%Y-%m-%d")
        url_prefix, url_slug, full_url = ep.resolve(
            product_id=product_id,
            from_date=from_date,
            to_date=to_date,
        )
        _, payload = await session.fetch_with_retry(client, full_url)
        new_points = _extract_sentiment_points(payload)

        if last_date is not None and not force:
            # Check if full refetch is identical to existing
            existing_rows = conn.execute(
                "SELECT date, svolatility, sdispersion, svscore, sbuzz, svolume, sdelta, sscore, smean FROM bronze.sentiment WHERE product_id = $1",
                [product_id],
            ).fetchall()
            existing = {}
            for row in existing_rows:
                existing[row[0]] = {
                    "svolatility": row[1],
                    "sdispersion": row[2],
                    "svscore": row[3],
                    "sbuzz": row[4],
                    "svolume": row[5],
                    "sdelta": row[6],
                    "sscore": row[7],
                    "smean": row[8],
                }
            if _is_identical(existing, new_points):
                logger.info("Product %d: full sentiment refetch is identical to existing, skipping", product_id)
                return True

        # Archive existing before deletion
        if last_date is not None:
            run_id = (
                conn.execute(
                    "SELECT MAX(updated_at) FROM bronze.sentiment WHERE product_id = $1",
                    [product_id],
                ).fetchone()[0]
                or today
            )
            existing_rows = conn.execute(
                "SELECT date, svolatility, sdispersion, svscore, sbuzz, svolume, sdelta, sscore, smean FROM bronze.sentiment WHERE product_id = $1",
                [product_id],
            ).fetchall()
            archive_rows = []
            for row in existing_rows:
                archive_rows.append(
                    {
                        "date": row[0],
                        "svolatility": row[1],
                        "sdispersion": row[2],
                        "svscore": row[3],
                        "sbuzz": row[4],
                        "svolume": row[5],
                        "sdelta": row[6],
                        "sscore": row[7],
                        "smean": row[8],
                    }
                )
            _archive_sentiment(conn, product_id, archive_rows, run_id)

        _delete_sentiment(conn, product_id)
        _upsert_sentiment(conn, product_id, new_points)
        logger.info("Product %d: full sentiment refetch complete (%d points)", product_id, len(new_points))
        return True

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

    # Validate overlap
    overlap_start = last_date - timedelta(days=7)
    valid, mismatch_type = _validate_overlap(conn, product_id, new_points, overlap_start)

    if not valid:
        logger.warning("Product %d: %s detected. Archiving and refetching full series...", product_id, mismatch_type)
        return await _fetch_and_store(client, conn, ep, product_id, force=True)

    # Store incremental
    _upsert_sentiment(conn, product_id, new_points)
    logger.info("Product %d: incremental sentiment update complete (%d points)", product_id, len(new_points))
    return True


async def fetch_incremental(
    client: httpx.AsyncClient,
    conn: duckdb.DuckDBPyConnection,
    ep: endpoints.Endpoint,
    product_id: int,
) -> None:
    """Public entry point for fetching sentiment for one product."""
    await _fetch_and_store(client, conn, ep, product_id)
