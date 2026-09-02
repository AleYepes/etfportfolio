import logging

import duckdb
import httpx

from etfportfolio.core import db
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.core.utils import content_address
from etfportfolio.ingestion import session

logger = logging.getLogger(__name__)

LANDING_URL_TEMPLATE = "/tws.proxy/fundamentals/landing/{product_id}?widgets=objective,keyProfile,lipper_ratings,holdings,mf_key_ratios,mstar&lang=en"


def _content_address(conn: duckdb.DuckDBPyConnection, payload: dict) -> tuple[int, bytes]:
    return content_address(payload)


def _select_preview_hash(conn: duckdb.DuckDBPyConnection, product_id: int) -> int | None:
    row = conn.execute(
        "SELECT hash FROM bronze.snapshot_previews WHERE product_id = $1",
        [product_id],
    ).fetchone()
    return row[0] if row else None


def _commit_preview(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    digest: int,
    compressed: bytes,
) -> None:
    """Commits the preview upsert transaction, then attempts garbage collection."""
    old_hash = _select_preview_hash(conn, product_id)

    # Store blob and upsert preview atomically
    conn.execute("BEGIN TRANSACTION")
    try:
        db.store_blob(conn, digest, compressed)

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
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    # Run GC after commit; failure here must not roll back the preview update
    if old_hash is not None and old_hash != digest:
        db.gc_preview_blob(conn, old_hash)


async def fetch_and_gate(
    client: httpx.AsyncClient,
    product_id: int,
    worker: AsyncDbWorker,
) -> tuple[bool, int, bytes]:
    """Fetches minimal landing widget payload, computes content-address digest,
    and compares against bronze.snapshot_previews.
    Returns: (changed, digest, compressed_bytes)
    """
    url = LANDING_URL_TEMPLATE.format(product_id=product_id)
    _, payload = await session.fetch_with_retry(client, url)

    digest, compressed = await worker.submit(_content_address, payload)

    old_hash = await worker.submit(_select_preview_hash, product_id)
    changed = old_hash is None or old_hash != digest
    logger.debug("Landing for product %d: changed=%s", product_id, changed)

    return changed, digest, compressed


async def commit_preview(
    worker: AsyncDbWorker,
    product_id: int,
    digest: int,
    compressed: bytes,
) -> None:
    """Commits pending preview hash to bronze.snapshot_previews and runs blob GC on old hash."""
    await worker.submit(_commit_preview, product_id, digest, compressed)
