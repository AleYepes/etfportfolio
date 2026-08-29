import asyncio
import logging

import duckdb
import httpx

from etfportfolio.ingestion import endpoints, landing, sentiment, session, snapshots

logger = logging.getLogger(__name__)

SNAPSHOT_ENDPOINTS = [ep for ep in endpoints.ENDPOINTS if ep.shape == "snapshot"]
SERIES_ENDPOINTS = [ep for ep in endpoints.ENDPOINTS if ep.shape == "series"]
GATED_ENDPOINTS = [ep for ep in SNAPSHOT_ENDPOINTS if ep.gated]
UNGATED_SNAPSHOT_ENDPOINTS = [ep for ep in SNAPSHOT_ENDPOINTS if not ep.gated]


async def _fetch_one(
    client: httpx.AsyncClient,
    conn: duckdb.DuckDBPyConnection,
    ep: endpoints.Endpoint,
    product_id: int,
    account_id: str,
    semaphore: asyncio.Semaphore,
) -> bool:
    """Fetches and stores a single endpoint for a product. Returns True on success."""
    async with semaphore:
        try:
            if ep.shape == "snapshot":
                await snapshots.fetch_snapshot(client, conn, ep, product_id, account_id)
            else:
                await sentiment.fetch_incremental(client, conn, ep, product_id)
            return True
        except session.SessionInvalidError:
            raise
        except Exception as e:
            logger.error("Failed to fetch %s for product %d: %s", ep.name, product_id, e)
            return False


async def process_product(
    client: httpx.AsyncClient,
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    account_id: str,
    semaphore: asyncio.Semaphore,
    force: bool = False,
) -> bool:
    """Runs the full 'details' phase for one product.

    Always fetches the landing payload and gate-checks it against the stored
    preview. Fetches the gated snapshot endpoints only if landing changed (or
    `force` is set, bypassing the gate check). Always fetches the ungated
    snapshot endpoints and both series endpoints, regardless of gating — they
    were never gated to begin with. Commits the new landing preview only if
    every gated endpoint that was attempted succeeded; a preview must never
    advance past gated data that wasn't actually confirmed fresh.

    Returns True if every fetched endpoint succeeded.
    """
    try:
        changed, digest, compressed = await landing.fetch_and_gate(client, product_id, conn)
    except session.SessionInvalidError:
        raise
    except Exception as e:
        logger.error("Landing fetch failed for product %d: %s. Skipping product.", product_id, e)
        return False

    fetch_gated = force or changed
    plan = list(UNGATED_SNAPSHOT_ENDPOINTS) + list(SERIES_ENDPOINTS)
    if fetch_gated:
        plan += GATED_ENDPOINTS

    tasks = [_fetch_one(client, conn, ep, product_id, account_id, semaphore) for ep in plan]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, session.SessionInvalidError):
            raise r

    gated_success = True
    if fetch_gated:
        for ep, res in zip(plan, results, strict=False):
            if ep.gated and (isinstance(res, Exception) or res is not True):
                gated_success = False
                break

    if fetch_gated and gated_success:
        landing.commit_preview(conn, product_id, digest, compressed)
    elif fetch_gated:
        logger.warning("Product %d: partial gated endpoint failure. Preview not updated.", product_id)

    return all(r is True for r in results)
