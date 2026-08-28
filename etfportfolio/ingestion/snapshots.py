from datetime import datetime
from typing import Any

import duckdb
import httpx

from etfportfolio.core import db
from etfportfolio.core.utils import content_address
from etfportfolio.ingestion import endpoints, session


def store_snapshot(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    url_prefix: str,
    url_slug: str,
    payload: Any,
    fetched_at: datetime | None = None,
) -> int:
    """Content-addresses a snapshot payload, ensures the blob is stored,

    and writes a lineage row to bronze.snapshots. Returns the payload hash digest.
    """
    digest, compressed = content_address(payload)

    db.store_blob(conn, digest, compressed)

    conn.execute(
        """
        INSERT INTO bronze.snapshots (hash, product_id, url_prefix, url_slug, fetched_at)
        VALUES ($1, $2, $3, $4, COALESCE($5, CURRENT_TIMESTAMP))
        """,
        [digest, product_id, url_prefix, url_slug, fetched_at],
    )

    return digest


async def fetch_snapshot(
    client: httpx.AsyncClient,
    conn: duckdb.DuckDBPyConnection,
    ep: endpoints.Endpoint,
    product_id: int,
    account_id: str,
) -> None:
    """Fetches a single snapshot-shaped endpoint for a product and stores it.

    Every snapshot endpoint (gated or not) resolves and stores identically —
    there's no per-endpoint special-casing at this shape, unlike series.
    """
    url_prefix, url_slug, full_url = ep.resolve(product_id=product_id, account_id=account_id)
    _, payload = await session.fetch_with_retry(client, full_url)
    store_snapshot(conn, product_id, url_prefix, url_slug, payload)
