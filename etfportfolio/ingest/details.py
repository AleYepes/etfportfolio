import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime

import duckdb
import httpx

from etfportfolio.core import endpoints
from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingest import landing, session, snapshots
from etfportfolio.ingest.utils import is_fresh

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
    landing_cache: dict[int, datetime],
    endpoint_cache: dict[tuple[int, str], datetime],
    force: bool = False,
) -> ProductDetailsResult:
    """Runs snapshot ingestion for one product across landing, ungated, and gated endpoints.

    Wave 1: Stale ungated endpoints and landing (if stale) are fetched concurrently (bounded by semaphore).
    Wave 2: If landing changed (or force=True), stale gated endpoints are fetched (bounded by semaphore).
    Landing preview commits/stamps only if all gated endpoints are satisfied (fresh or successfully fetched).
    """
    landing_fresh = (not force) and is_fresh(landing_cache.get(product_id), settings.freshness_window_hours)

    stale_ungated: list[endpoints.Endpoint] = []
    skipped_fresh_eps = 0

    for ep in endpoints.UNGATED_ENDPOINTS:
        last_checked = endpoint_cache.get((product_id, ep.url_prefix))
        if not force and is_fresh(last_checked, settings.freshness_window_hours):
            skipped_fresh_eps += 1
        else:
            stale_ungated.append(ep)

    # Fast path: landing is fresh and all ungated endpoints are fresh
    if landing_fresh and not stale_ungated:
        # Gated endpoints are all skipped because landing is fresh
        skipped_fresh_eps += len(endpoints.GATED_ENDPOINTS)
        return ProductDetailsResult(
            ok=True,
            product_skipped_fresh=True,
            endpoints_skipped_fresh=skipped_fresh_eps,
        )

    # --- Wave 1: Fetch stale ungated endpoints + landing (if not fresh) ---
    landing_fetched = False
    changed = False
    digest = None

    async def _fetch_landing() -> None:
        nonlocal changed, digest, landing_fetched
        async with semaphore:
            changed, digest = await landing.fetch_and_gate(client, product_id, worker)
            landing_fetched = True

    wave1_tasks = [_fetch_one(client, worker, ep, product_id, account_id, semaphore) for ep in stale_ungated]
    if not landing_fresh:
        wave1_tasks.append(_fetch_landing())

    wave1_results = await asyncio.gather(*wave1_tasks, return_exceptions=True)

    for r in wave1_results:
        if isinstance(r, session.SessionInvalidError):
            raise r

    if not landing_fresh and not landing_fetched:
        # Landing fetch raised an exception (e.g. network/5xx error)
        landing_err = wave1_results[-1] if wave1_results else "Unknown error"
        logger.error("Landing fetch failed for product %d: %s. Skipping product.", product_id, landing_err)
        return ProductDetailsResult(
            ok=False,
            product_skipped_fresh=False,
            endpoints_skipped_fresh=skipped_fresh_eps,
        )

    ungated_success = {
        ep: (res is True) for ep, res in zip(stale_ungated, wave1_results[: len(stale_ungated)], strict=False)
    }

    # --- Wave 2: Fetch stale gated endpoints if landing changed or forced ---
    fetch_gated = force or changed
    stale_gated: list[endpoints.Endpoint] = []

    if fetch_gated:
        for ep in endpoints.GATED_ENDPOINTS:
            last_checked = endpoint_cache.get((product_id, ep.url_prefix))
            if not force and is_fresh(last_checked, settings.freshness_window_hours):
                skipped_fresh_eps += 1
            else:
                stale_gated.append(ep)

        wave2_tasks = [_fetch_one(client, worker, ep, product_id, account_id, semaphore) for ep in stale_gated]
        wave2_results = await asyncio.gather(*wave2_tasks, return_exceptions=True) if wave2_tasks else []

        for r in wave2_results:
            if isinstance(r, session.SessionInvalidError):
                raise r

        gated_fetch_success = {ep: (res is True) for ep, res in zip(stale_gated, wave2_results, strict=False)}
    else:
        # Gated endpoints not fetched because landing is unchanged
        skipped_fresh_eps += len(endpoints.GATED_ENDPOINTS)
        gated_fetch_success = {}

    # --- Gated Satisfaction & Landing Commit Boundary ---
    # When gated endpoints are fetched (landing changed or force), all gated endpoints must be
    # satisfied (fresh in pre-run cache, or successfully fetched / 404 in this run).
    # When landing is unchanged (fetch_gated is False), the landing gate treats gated endpoints
    # as fresh/satisfied.
    if fetch_gated:
        gated_all_satisfied = all(
            (not force and is_fresh(endpoint_cache.get((product_id, ep.url_prefix)), settings.freshness_window_hours))
            or gated_fetch_success.get(ep, False)
            for ep in endpoints.GATED_ENDPOINTS
        )
    else:
        gated_all_satisfied = True

    if gated_all_satisfied:
        if landing_fetched:
            if changed and digest is not None:
                await landing.commit_preview(worker, product_id, digest)
            else:
                await landing.stamp_last_checked(worker, product_id)
    else:
        logger.warning("Product %d: gated endpoint(s) not satisfied. Preview not updated.", product_id)

    all_ungated_ok = all(ungated_success.values()) if ungated_success else True
    all_gated_ok = all(gated_fetch_success.values()) if gated_fetch_success else True
    all_ok = all_ungated_ok and all_gated_ok and gated_all_satisfied

    return ProductDetailsResult(
        ok=all_ok,
        product_skipped_fresh=False,
        endpoints_skipped_fresh=skipped_fresh_eps,
    )
