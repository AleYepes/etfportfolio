import logging

import duckdb
import httpx

from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingestion import endpoints, session
from etfportfolio.ingestion.utils import content_address, gc_preview_blob, store_blob

logger = logging.getLogger(__name__)


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
    """Upserts preview hash, stamps last_checked_at, then GCs the previous blob."""
    old_hash = _select_preview_hash(conn, product_id)

    conn.execute("BEGIN TRANSACTION")
    try:
        store_blob(conn, digest, compressed)

        conn.execute(
            """
            INSERT INTO bronze.snapshot_previews (product_id, hash, updated_at, last_checked_at)
            VALUES ($1, $2, (now() AT TIME ZONE 'UTC'), (now() AT TIME ZONE 'UTC'))
            ON CONFLICT (product_id) DO UPDATE SET
                hash = EXCLUDED.hash,
                updated_at = CASE
                    WHEN bronze.snapshot_previews.hash IS DISTINCT FROM EXCLUDED.hash
                    THEN (now() AT TIME ZONE 'UTC')
                    ELSE bronze.snapshot_previews.updated_at
                END,
                last_checked_at = (now() AT TIME ZONE 'UTC')
            """,
            [product_id, digest],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    if old_hash is not None and old_hash != digest:
        gc_preview_blob(conn, old_hash)


def _stamp_last_checked(conn: duckdb.DuckDBPyConnection, product_id: int) -> None:
    """Bump last_checked_at without touching hash / updated_at."""
    conn.execute(
        """
        UPDATE bronze.snapshot_previews
        SET last_checked_at = (now() AT TIME ZONE 'UTC')
        WHERE product_id = $1
        """,
        [product_id],
    )


async def fetch_and_gate(
    client: httpx.AsyncClient,
    product_id: int,
    worker: AsyncDbWorker,
) -> tuple[bool, int, bytes]:
    """Fetches minimal landing widget payload, computes content-address digest,
    and compares against bronze.snapshot_previews.
    Returns: (changed, digest, compressed_bytes)

    A 404 is content-addressed as ``{}`` so repeated absences hash identically.
    """
    ep = endpoints.ENDPOINTS_BY_NAME["landing"]
    _, _, full_url = ep.resolve(product_id=product_id)
    _, payload = await session.fetch_with_retry(client, full_url)

    digest, compressed = await worker.submit(_content_address, payload or {})

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


async def stamp_last_checked(worker: AsyncDbWorker, product_id: int) -> None:
    """Stamp last_checked_at after a stable landing check that did not change the hash."""
    await worker.submit(_stamp_last_checked, product_id)
