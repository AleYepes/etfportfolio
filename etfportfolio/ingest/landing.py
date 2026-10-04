import logging

import duckdb
import httpx

from etfportfolio.core import endpoints
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingest import session
from etfportfolio.ingest.utils import content_address

logger = logging.getLogger(__name__)


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
) -> None:
    """Upserts preview hash and stamps last_checked_at in bronze.snapshot_previews."""
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


def _stamp_last_checked(conn: duckdb.DuckDBPyConnection, product_id: int) -> None:
    """Bump last_checked_at without modifying hash or updated_at."""
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
) -> tuple[bool, int]:
    """Fetches landing widget payload, computes content-address digest,

    and compares against bronze.snapshot_previews.
    Returns: (changed, digest)

    HTTP 404 and empty responses are content-addressed as `{}` so repeated
    absences hash identically.
    """
    ep = endpoints.ENDPOINTS_BY_NAME["landing"]
    _, _, full_url = ep.resolve(product_id=product_id)
    _, payload = await session.fetch_with_retry(client, full_url)

    digest, _ = content_address(payload or {})

    old_hash = await worker.submit(_select_preview_hash, product_id)
    changed = old_hash is None or old_hash != digest
    logger.debug("Landing for product %d: changed=%s", product_id, changed)

    return changed, digest


async def commit_preview(
    worker: AsyncDbWorker,
    product_id: int,
    digest: int,
) -> None:
    """Commits pending preview hash to bronze.snapshot_previews."""
    await worker.submit(_commit_preview, product_id, digest)


async def stamp_last_checked(worker: AsyncDbWorker, product_id: int) -> None:
    """Stamp last_checked_at after a stable landing check that did not change the hash."""
    await worker.submit(_stamp_last_checked, product_id)
