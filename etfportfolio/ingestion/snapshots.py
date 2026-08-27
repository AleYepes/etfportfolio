from datetime import datetime
from typing import Any

import duckdb

from etfportfolio.core.utils import content_address


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

    # Insert into payload_blobs (idempotent on duplicate hash)
    conn.execute(
        """
        INSERT INTO bronze.payload_blobs (hash, payload)
        VALUES ($1, $2)
        ON CONFLICT (hash) DO NOTHING
        """,
        [digest, compressed],
    )

    # Insert lineage row into bronze.snapshots
    conn.execute(
        """
        INSERT INTO bronze.snapshots (hash, product_id, url_prefix, url_slug, fetched_at)
        VALUES ($1, $2, $3, $4, COALESCE($5, CURRENT_TIMESTAMP))
        """,
        [digest, product_id, url_prefix, url_slug, fetched_at],
    )

    return digest
