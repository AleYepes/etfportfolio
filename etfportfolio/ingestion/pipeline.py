import asyncio
import contextlib
import logging
from pathlib import Path
from typing import Any

import duckdb
import httpx

from etfportfolio.core.config import settings
from etfportfolio.core.db import connect, current
from etfportfolio.ingestion import endpoints, landing, products, series, session, snapshots, themes

logger = logging.getLogger(__name__)


class SessionInvalidError(Exception):
    """Raised when IBKR returns the session-invalid signature."""


async def _fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    max_retries: int = 3,
    initial_backoff: float = 1.0,
) -> tuple[int, Any]:
    """Fetches URL with retry on non-2xx responses.

    Bypasses retry immediately if session-invalid signature is encountered.
    Returns: (status_code, json_payload)
    """
    attempt = 0
    backoff = initial_backoff

    while attempt < max_retries:
        attempt += 1
        try:
            resp = await client.get(url)
            if resp.is_success:
                return resp.status_code, resp.json()

            if session.is_session_invalid(resp):
                logger.error("Session invalid signature hit on %s", url)
                raise SessionInvalidError("Session is invalid ('Invalid headers').")

            logger.warning(
                "Request to %s failed (status %d), attempt %d/%d",
                url,
                resp.status_code,
                attempt,
                max_retries,
            )
        except (httpx.RequestError, httpx.TimeoutException) as e:
            logger.warning("Network error on %s: %s (attempt %d/%d)", url, e, attempt, max_retries)

        if attempt < max_retries:
            await asyncio.sleep(backoff)
            backoff *= 2.0

    raise RuntimeError(f"Request to {url} failed after {max_retries} attempts.")


async def _fetch_and_store_endpoint(
    client: httpx.AsyncClient,
    conn: duckdb.DuckDBPyConnection,
    ep: endpoints.Endpoint,
    product_id: int,
    account_id: str,
    semaphore: asyncio.Semaphore,
) -> bool:
    """Executes a single endpoint fetch according to shape and stores the result in DuckDB."""
    async with semaphore:
        try:
            if ep.shape == "snapshot":
                url_prefix, url_slug, full_url = ep.resolve(product_id=product_id, account_id=account_id)
                _, payload = await _fetch_with_retry(client, full_url)
                snapshots.store_snapshot(conn, product_id, url_prefix, url_slug, payload)
                return True

            elif ep.shape == "series":
                url_prefix = ep.url_prefix
                last_dt, prior_payload = series.get_last_series_info(conn, product_id, url_prefix)

                if ep.name == "price":
                    period = series.determine_price_period(last_dt)
                    url_prefix, url_slug, full_url = ep.resolve(product_id=product_id, period=period)
                    _, payload = await _fetch_with_retry(client, full_url)

                    date_range = series.extract_series_date_range(ep.name, payload)
                    if not date_range:
                        logger.warning("Empty price series returned for product %d", product_id)
                        return True

                    first_dt, new_last_dt = date_range

                    # Check overlap if prior payload exists
                    if last_dt is not None and prior_payload is not None:
                        is_valid = series.validate_overlap(ep.name, payload, prior_payload)
                        if not is_valid:
                            logger.warning(
                                "Price series overlap mismatch for product %d. Discarding incremental and refetching MAX...",
                                product_id,
                            )
                            # Refetch MAX
                            url_prefix, max_slug, max_url = ep.resolve(product_id=product_id, period="MAX")
                            _, max_payload = await _fetch_with_retry(client, max_url)
                            max_range = series.extract_series_date_range(ep.name, max_payload)
                            if max_range:
                                m_first, m_last = max_range
                                series.store_series(
                                    conn, product_id, url_prefix, max_slug, m_first, m_last, max_payload
                                )
                            return True

                    series.store_series(conn, product_id, url_prefix, url_slug, first_dt, new_last_dt, payload)
                    return True

                elif ep.name == "sentiment":
                    from_d, to_d = series.determine_sentiment_dates(last_dt)
                    url_prefix, url_slug, full_url = ep.resolve(product_id=product_id, from_date=from_d, to_date=to_d)
                    _, payload = await _fetch_with_retry(client, full_url)

                    date_range = series.extract_series_date_range(ep.name, payload)
                    if not date_range:
                        logger.warning("Empty sentiment series returned for product %d", product_id)
                        return True

                    first_dt, new_last_dt = date_range

                    # Check overlap if prior payload exists
                    if last_dt is not None and prior_payload is not None:
                        is_valid = series.validate_overlap(ep.name, payload, prior_payload)
                        if not is_valid:
                            logger.warning(
                                "Sentiment series overlap mismatch for product %d. Discarding incremental and refetching full...",
                                product_id,
                            )
                            # Refetch full
                            url_prefix, full_slug, full_range_url = ep.resolve(
                                product_id=product_id, from_date="2000-01-01", to_date=to_d
                            )
                            _, full_payload = await _fetch_with_retry(client, full_range_url)
                            full_range = series.extract_series_date_range(ep.name, full_payload)
                            if full_range:
                                f_first, f_last = full_range
                                series.store_series(
                                    conn, product_id, url_prefix, full_slug, f_first, f_last, full_payload
                                )
                            return True

                    series.store_series(conn, product_id, url_prefix, url_slug, first_dt, new_last_dt, payload)
                    return True

            return False
        except SessionInvalidError:
            raise
        except Exception as e:
            logger.error("Failed to fetch/store endpoint %s for product %d: %s", ep.name, product_id, e)
            return False


async def run_pipeline(
    product_ids: list[int] | None = None,
    limit: int | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
    client: httpx.AsyncClient | None = None,
) -> None:
    """Executes full DB & Ingestion pipeline across Phases 1, 2, 2.5, and 3."""
    db_conn = conn if conn is not None else current()
    close_client = False
    if client is None:
        client = session.build_async_client()
        close_client = True

    try:
        # Phase 1: Product discovery crawl
        logger.info("=== Starting Phase 1: Product Discovery ===")
        try:
            await products.sync(client=client, conn=db_conn)
        except Exception as e:
            logger.error("Phase 1 product sync failed (continuing with existing products in DB): %s", e)

        # Phase 2: Session validation probe
        logger.info("=== Starting Phase 2: Session Validation ===")
        account_id = None
        try:
            account_id = await session.probe(client)
            logger.info("Session validated successfully. Active account: %s", account_id)
        except Exception as e:
            logger.warning("Initial session probe failed: %s. Attempting login and re-probe...", e)
            try:
                await session.login()
                # Rebuild client with updated session cookies
                if close_client:
                    await client.aclose()
                client = session.build_async_client()
                account_id = await session.probe(client)
                logger.info("Session validated after re-login. Active account: %s", account_id)
            except Exception as login_err:
                logger.fatal("Session re-probe failed after login: %s. Aborting pipeline.", login_err)
                raise RuntimeError(f"Session authentication failed: {login_err}") from login_err

        # Phase 2.5: Theme taxonomy sync
        logger.info("=== Starting Phase 2.5: Theme Taxonomy Sync ===")
        try:
            await themes.sync(client=client, conn=db_conn)
        except Exception as e:
            logger.error("Phase 2.5 theme taxonomy sync failed: %s", e)

        # Phase 3: Per-product fetch loop
        logger.info("=== Starting Phase 3: Per-Product Ingestion ===")

        # Determine target product IDs
        if product_ids is not None:
            target_ids = product_ids
        else:
            query = "SELECT product_id FROM bronze.products ORDER BY product_id"
            if limit is not None and limit > 0:
                query += f" LIMIT {int(limit)}"
            rows = db_conn.execute(query).fetchall()
            target_ids = [row[0] for row in rows]

        logger.info("Processing %d products...", len(target_ids))
        semaphore = asyncio.Semaphore(settings.endpoint_concurrency)

        for idx, pid in enumerate(target_ids, 1):
            logger.info("[%d/%d] Processing product %d...", idx, len(target_ids), pid)

            # Step 1: Landing fetch and gating check
            try:
                changed, digest, compressed, _ = await landing.fetch_and_gate(client, pid, db_conn)
            except SessionInvalidError:
                logger.fatal("Session invalid signature detected during landing fetch. Halting pipeline.")
                raise
            except Exception as e:
                logger.error("Landing fetch failed for product %d: %s. Skipping product.", pid, e)
                continue

            # Step 2: Build fetch plan
            plan: list[endpoints.Endpoint] = []
            for ep in endpoints.ENDPOINTS:
                if not ep.gated or changed:
                    plan.append(ep)

            logger.info("Product %d landing changed=%s. Fetching %d endpoints...", pid, changed, len(plan))

            # Step 3: Fetch plan concurrently
            tasks = [_fetch_and_store_endpoint(client, db_conn, ep, pid, account_id, semaphore) for ep in plan]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check if any task raised SessionInvalidError
            for r in results:
                if isinstance(r, SessionInvalidError):
                    logger.fatal("Session invalid signature detected in batch worker. Halting pipeline.")
                    raise r

            # Step 4 & 5: Check if all gated endpoints succeeded before committing preview
            gated_success = True
            for ep, res in zip(plan, results, strict=False):
                if ep.gated and (isinstance(res, Exception) or res is False):
                    gated_success = False

            if gated_success:
                landing.commit_preview(db_conn, pid, digest, compressed)
                logger.info("Product %d: successfully committed snapshot preview.", pid)
            else:
                logger.warning("Product %d: partial gated endpoint failure. Snapshot preview not updated.", pid)

        logger.info("Pipeline execution completed successfully.")
    finally:
        if close_client:
            await client.aclose()


def _parse_product_ids_arg(product_ids_arg: str | None) -> list[int] | None:
    if not product_ids_arg:
        return None

    path = Path(product_ids_arg)
    if path.is_file():
        content = path.read_text(encoding="utf-8")
        ids: list[int] = []
        for line in content.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                with contextlib.suppress(ValueError):
                    ids.append(int(line))
        return ids

    # Comma-separated list
    return [int(x.strip()) for x in product_ids_arg.split(",") if x.strip()]


class PipelineCLI:
    def run(
        self,
        product_ids: str | None = None,
        limit: int | None = None,
    ) -> None:
        """Run full ingestion pipeline."""
        if product_ids is not None and limit is not None:
            raise ValueError("--product-ids and --limit are mutually exclusive.")

        parsed_ids = _parse_product_ids_arg(product_ids)
        with connect() as conn:
            asyncio.run(run_pipeline(product_ids=parsed_ids, limit=limit, conn=conn))


cli = PipelineCLI()
