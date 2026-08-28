import logging

import duckdb
import httpx

from etfportfolio.core import db
from etfportfolio.core.utils import content_address
from etfportfolio.ingestion import session

logger = logging.getLogger(__name__)

LANDING_URL_TEMPLATE = "/tws.proxy/fundamentals/landing/{product_id}?widgets=objective,keyProfile,lipper_ratings,holdings,mf_key_ratios,ownership,mstar&lang=en"


async def fetch_and_gate(
    client: httpx.AsyncClient,
    product_id: int,
    conn: duckdb.DuckDBPyConnection,
) -> tuple[bool, int, bytes]:
    """Fetches minimal landing widget payload, computes content-address digest,

    and compares against bronze.snapshot_previews.
    Returns: (changed, digest, compressed_bytes)
    """
    url = LANDING_URL_TEMPLATE.format(product_id=product_id)
    _, payload = await session.fetch_with_retry(client, url)

    digest, compressed = content_address(payload)

    row = conn.execute(
        "SELECT hash FROM bronze.snapshot_previews WHERE product_id = $1",
        [product_id],
    ).fetchone()

    changed = row is None or row[0] != digest
    logger.debug("Landing for product %d: changed=%s", product_id, changed)

    return changed, digest, compressed


def commit_preview(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    digest: int,
    compressed: bytes,
) -> None:
    """Commits pending preview hash to bronze.snapshot_previews and runs blob GC on old hash."""
    row = conn.execute(
        "SELECT hash FROM bronze.snapshot_previews WHERE product_id = $1",
        [product_id],
    ).fetchone()
    old_hash = row[0] if row else None

    # 1. Ensure payload blob is stored
    db.store_blob(conn, digest, compressed)

    # 2. Upsert snapshot_previews
    conn.execute(
        """
        INSERT INTO bronze.snapshot_previews (product_id, hash, updated_at)
        VALUES ($1, $2, now())
        ON CONFLICT (product_id) DO UPDATE SET
            hash = EXCLUDED.hash,
            updated_at = now()
        """,
        [product_id, digest],
    )

    # 3. Clean up orphaned old hash blob if replaced
    if old_hash is not None and old_hash != digest:
        db.gc_preview_blob(conn, old_hash)
