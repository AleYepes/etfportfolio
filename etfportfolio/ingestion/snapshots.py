from datetime import UTC, datetime
from typing import Any

import duckdb
import httpx

from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingestion import endpoints, session
from etfportfolio.ingestion.utils import content_address, store_blob


def store_snapshot(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    url_prefix: str,
    url_slug: str,
    payload: Any,
    fetched_at: datetime | None = None,
) -> int:
    """Content-addresses a snapshot payload, stores the blob, and updates or appends to the changelog."""
    digest, compressed = content_address(payload)
    timestamp = fetched_at or datetime.now(UTC).replace(tzinfo=None)

    conn.execute("BEGIN TRANSACTION")
    try:
        store_blob(conn, digest, compressed)

        # Check latest snapshot for this product and endpoint
        row = conn.execute(
            """
            SELECT snapshot_id, hash
            FROM bronze.snapshots
            WHERE product_id = $1 AND url_prefix = $2
            ORDER BY snapshot_id DESC
            LIMIT 1
            """,
            [product_id, url_prefix],
        ).fetchone()

        if row and row[1] == digest:
            # Hash unchanged: bump last_checked_at
            conn.execute(
                """
                UPDATE bronze.snapshots
                SET last_checked_at = $1
                WHERE snapshot_id = $2
                """,
                [timestamp, row[0]],
            )
        else:
            # New or changed payload: insert new changelog record
            conn.execute(
                """
                INSERT INTO bronze.snapshots (hash, product_id, url_prefix, url_slug, created_at, last_checked_at)
                VALUES ($1, $2, $3, $4, $5, $5)
                """,
                [digest, product_id, url_prefix, url_slug, timestamp],
            )

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return digest


async def fetch_snapshot(
    client: httpx.AsyncClient,
    worker: AsyncDbWorker,
    ep: endpoints.Endpoint,
    product_id: int,
    account_id: str,
) -> None:
    """Fetches a single snapshot endpoint for a product and stores it.

    404 is persisted as ``{}`` so ``fetched_at`` is written and the freshness
    cache can skip the endpoint on the next run.
    """
    url_prefix, url_slug, full_url = ep.resolve(product_id=product_id, account_id=account_id)
    _, payload = await session.fetch_with_retry(client, full_url)
    await worker.submit(store_snapshot, product_id, url_prefix, url_slug, payload or {})
