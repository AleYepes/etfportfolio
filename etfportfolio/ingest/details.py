import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime

import duckdb
import httpx

from etfportfolio.core import endpoints
from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingest import session, snapshots
from etfportfolio.ingest.utils import is_fresh

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProductDetailsResult:
    ok: bool
    product_skipped_fresh: bool
    endpoints_skipped_fresh: int


def load_endpoint_freshness_cache(conn: duckdb.DuckDBPyConnection) -> dict[tuple[int, str], datetime]:
    """Load (product_id, url_prefix) -> MAX(last_checked_at) from bronze.snapshots."""
    rows = conn.execute(
        """
        SELECT product_id, url_prefix, MAX(last_checked_at)
        FROM bronze.snapshots
        GROUP BY product_id, url_prefix
        """
    ).fetchall()
    return {(row[0], row[1]): row[2] for row in rows}


async def _fetch_one(
    client: httpx.AsyncClient,
    worker: AsyncDbWorker,
    ep: endpoints.Endpoint,
    product_id: int,
    account_id: str,
    semaphore: asyncio.Semaphore,
) -> bool:
    """Fetches and stores a single snapshot endpoint for a product. Returns True on success."""
    async with semaphore:
        try:
            await snapshots.fetch_snapshot(client, worker, ep, product_id, account_id)
            return True
        except session.SessionInvalidError:
            raise
        except Exception as e:
            logger.error("Failed to fetch %s for product %d: %s", ep.name, product_id, e)
            return False


async def process_product(
    client: httpx.AsyncClient,
    worker: AsyncDbWorker,
    product_id: int,
    account_id: str,
    semaphore: asyncio.Semaphore,
    endpoint_cache: dict[tuple[int, str], datetime],
    force: bool = False,
) -> ProductDetailsResult:
    """Runs snapshot ingestion for one product across all endpoints.

    Evaluates freshness per endpoint. Fetches only stale endpoints concurrently.
    Employs partial continuation on individual endpoint errors.
    """
    to_fetch: list[endpoints.Endpoint] = []
    skipped_fresh_eps = 0

    for ep in endpoints.ENDPOINTS:
        last_checked = endpoint_cache.get((product_id, ep.url_prefix))
        if not force and is_fresh(last_checked, settings.freshness_window_hours):
            skipped_fresh_eps += 1
        else:
            to_fetch.append(ep)

    if not to_fetch:
        return ProductDetailsResult(
            ok=True,
            product_skipped_fresh=True,
            endpoints_skipped_fresh=skipped_fresh_eps,
        )

    tasks = [_fetch_one(client, worker, ep, product_id, account_id, semaphore) for ep in to_fetch]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, session.SessionInvalidError):
            raise r

    fetch_success = {ep: (res is True) for ep, res in zip(to_fetch, results, strict=False)}
    all_ok = all(fetch_success.values()) if fetch_success else True

    return ProductDetailsResult(
        ok=all_ok,
        product_skipped_fresh=False,
        endpoints_skipped_fresh=skipped_fresh_eps,
    )
