from datetime import datetime
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
    """Content-addresses a snapshot payload, stores the blob, and writes a lineage row."""
    digest, compressed = content_address(payload)

    conn.execute("BEGIN TRANSACTION")
    try:
        store_blob(conn, digest, compressed)

        conn.execute(
            """
            INSERT INTO bronze.snapshots (hash, product_id, url_prefix, url_slug, fetched_at)
            VALUES ($1, $2, $3, $4, COALESCE($5, CURRENT_TIMESTAMP))
            """,
            [digest, product_id, url_prefix, url_slug, fetched_at],
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
