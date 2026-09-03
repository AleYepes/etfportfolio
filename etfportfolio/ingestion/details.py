import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime

import duckdb
import httpx

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingestion import endpoints, landing, session, snapshots
from etfportfolio.ingestion.utils import is_fresh

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProductDetailsResult:
    ok: bool
    product_skipped_fresh: bool
    endpoints_skipped_fresh: int


def load_landing_freshness_cache(conn: duckdb.DuckDBPyConnection) -> dict[int, datetime]:
    """Load product_id -> last_checked_at from bronze.snapshot_previews."""
    rows = conn.execute(
        "SELECT product_id, last_checked_at FROM bronze.snapshot_previews WHERE last_checked_at IS NOT NULL"
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def load_endpoint_freshness_cache(conn: duckdb.DuckDBPyConnection) -> dict[tuple[int, str], datetime]:
    """Load (product_id, url_prefix) -> MAX(fetched_at) from bronze.snapshots."""
    rows = conn.execute(
        """
        SELECT product_id, url_prefix, MAX(fetched_at)
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
    landing_cache: dict[int, datetime],
    endpoint_cache: dict[tuple[int, str], datetime],
    force: bool = False,
) -> ProductDetailsResult:
    """Runs the full 'details' snapshot phase for one product.

    Checks landing freshness and endpoint freshness caches. Gated endpoints are
    only fetched if landing changed (or force=True). Skipped-fresh endpoints count
    as satisfied. Commits preview or stamps last_checked_at per the §3.5.2 rule.
    """
    landing_fresh = (not force) and is_fresh(landing_cache.get(product_id), settings.freshness_window_hours)

    changed = False
    digest = None
    compressed = None
    landing_fetched = False

    if not landing_fresh:
        try:
            changed, digest, compressed = await landing.fetch_and_gate(client, product_id, worker)
            landing_fetched = True
        except session.SessionInvalidError:
            raise
        except Exception as e:
            logger.error("Landing fetch failed for product %d: %s. Skipping product.", product_id, e)
            return ProductDetailsResult(ok=False, product_skipped_fresh=False, endpoints_skipped_fresh=0)

    fetch_gated = force or changed
    base_plan = list(endpoints.UNGATED_ENDPOINTS)
    if fetch_gated:
        base_plan += endpoints.GATED_ENDPOINTS

    to_fetch: list[endpoints.Endpoint] = []
    skipped_fresh_eps = 0

    for ep in base_plan:
        if not force and is_fresh(endpoint_cache.get((product_id, ep.url_prefix)), settings.freshness_window_hours):
            skipped_fresh_eps += 1
        else:
            to_fetch.append(ep)

    if landing_fresh and len(to_fetch) == 0:
        return ProductDetailsResult(
            ok=True,
            product_skipped_fresh=True,
            endpoints_skipped_fresh=skipped_fresh_eps,
        )

    tasks = [_fetch_one(client, worker, ep, product_id, account_id, semaphore) for ep in to_fetch]
    results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []

    for r in results:
        if isinstance(r, session.SessionInvalidError):
            raise r

    fetch_success = {ep: (res is True) for ep, res in zip(to_fetch, results, strict=False)}

    gated_success = True
    if fetch_gated:
        for ep in endpoints.GATED_ENDPOINTS:
            if ep in fetch_success and not fetch_success[ep]:
                gated_success = False
                break

    should_stamp = not (fetch_gated and not gated_success)

    if should_stamp:
        if landing_fetched:
            if changed and digest is not None and compressed is not None:
                await landing.commit_preview(worker, product_id, digest, compressed)
            else:
                await landing.stamp_last_checked(worker, product_id)
    else:
        logger.warning("Product %d: partial gated endpoint failure. Preview not updated.", product_id)

    all_ok = all(fetch_success.values()) if fetch_success else True
    if fetch_gated and not gated_success:
        all_ok = False

    return ProductDetailsResult(
        ok=all_ok,
        product_skipped_fresh=False,
        endpoints_skipped_fresh=skipped_fresh_eps,
    )
