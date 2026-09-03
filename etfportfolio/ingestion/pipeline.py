import asyncio
import logging

import httpx

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.core.logging import console
from etfportfolio.core.progress import progress_bar
from etfportfolio.ingestion import contracts, details, prices, products, sentiment, session, themes

logger = logging.getLogger(__name__)


async def _run_details_phase(
    worker: AsyncDbWorker,
    client: httpx.AsyncClient,
    account_id: str,
    target_ids: list[int],
    force: bool,
) -> None:
    """Runs the details phase across a resolved list of target products."""
    console.info(f"Processing {len(target_ids)} product(s)...")
    semaphore = asyncio.Semaphore(settings.details_concurrency)
    failures = 0
    skipped_prods = 0
    skipped_eps = 0

    landing_cache = await worker.submit(details.load_landing_freshness_cache)
    endpoint_cache = await worker.submit(details.load_endpoint_freshness_cache)

    with progress_bar(len(target_ids), desc="Details") as bar:
        for product_id in target_ids:
            bar.set_postfix_str(str(product_id))
            try:
                res = await details.process_product(
                    client,
                    worker,
                    product_id,
                    account_id,
                    semaphore,
                    landing_cache,
                    endpoint_cache,
                    force=force,
                )
            except session.SessionInvalidError:
                logger.error("Session became invalid while processing product %d. Aborting.", product_id)
                raise
            finally:
                bar.update(1)

            if not res.ok:
                failures += 1
            if res.product_skipped_fresh:
                skipped_prods += 1
            else:
                skipped_eps += res.endpoints_skipped_fresh

    processed_prods = len(target_ids) - skipped_prods
    console.info(
        f"details: {skipped_prods}/{len(target_ids)} products skipped (fresh), "
        f"{processed_prods} processed; {skipped_eps} endpoint requests skipped (fresh) among processed products."
    )
    if failures:
        console.info(
            f"Done. {len(target_ids) - failures}/{len(target_ids)} products fully succeeded, "
            f"{failures} had failures — see log for detail."
        )
    else:
        console.info(f"Done. All {len(target_ids)} products fully succeeded.")


async def _run_session() -> str:
    client, account_id = await session.ensure_session()
    await client.aclose()
    return account_id


async def _run_themes(force: bool = False) -> tuple[int, int]:
    client, account_id = await session.ensure_session()
    try:
        return await themes.sync(client=client, force=force)
    finally:
        await client.aclose()


async def _run_details_only(product_ids: str | None, limit: int | None, force: bool) -> None:
    async with AsyncDbWorker(settings.db_path) as worker:
        client, account_id = await session.ensure_session()
        console.info(f"Session OK. Active account: {account_id}")
        try:
            target_ids = await worker.submit(products.resolve_target_ids, product_ids, limit)
            await _run_details_phase(worker, client, account_id, target_ids, force)
        finally:
            await client.aclose()


async def _run_full(product_ids: str | None, limit: int | None, force: bool) -> None:
    console.info("=== Phase 1: Product discovery ===")
    try:
        count = await products.sync(force=force)
        console.info(f"Product sync complete. Total products synced: {count}")
    except Exception as e:
        logger.error("Product sync failed (continuing with existing products in DB): %s", e)

    console.info("=== Phase 2: Contract qualification ===")
    try:
        count = await contracts.sync(product_ids=product_ids, limit=limit, force=force)
        console.info(f"Contract qualification complete. {count} products processed.")
    except Exception as e:
        logger.error("Contract qualification failed: %s", e)
        raise

    console.info("=== Phase 3: Price series ===")
    try:
        count = await prices.sync(product_ids=product_ids, limit=limit, force=force)
        console.info(f"Price series complete. {count} products processed.")
    except Exception as e:
        logger.error("Price series failed: %s", e)
        raise

    console.info("=== Phase 4: Session validation ===")
    client, account_id = await session.ensure_session()
    console.info(f"Session OK. Active account: {account_id}")

    try:
        console.info("=== Phase 5: Theme taxonomy sync ===")
        try:
            p_count, n_count = await themes.sync(client=client, force=force)
            console.info(f"Theme taxonomy synced: {p_count} parents, {n_count} nodes.")
        except Exception as e:
            logger.error("Theme taxonomy sync failed: %s", e)

        console.info("=== Phase 6: Product details ===")
        async with AsyncDbWorker(settings.db_path) as worker:
            target_ids = await worker.submit(products.resolve_target_ids, product_ids, limit)
            await _run_details_phase(worker, client, account_id, target_ids, force)

        console.info("=== Phase 7: Sentiment series ===")
        try:
            s_count = await sentiment.sync(
                client=client,
                account_id=account_id,
                product_ids=product_ids,
                limit=limit,
                force=force,
            )
            console.info(f"Sentiment series complete. {s_count} products processed.")
        except Exception as e:
            logger.error("Sentiment series failed: %s", e)
            raise
    finally:
        await client.aclose()

    console.info("=== Ingestion complete ===")


class Ingest:
    """CLI surface for the ingestion pipeline: `main.py ingest <phase>`."""

    def __call__(self, product_ids: str | None = None, limit: int | None = None, force: bool = False) -> None:
        asyncio.run(_run_full(product_ids, limit, force))

    def session(self) -> None:
        account_id = asyncio.run(_run_session())
        console.info(f"Session OK. Active account: {account_id}")

    def products(self, force: bool = False) -> None:
        count = asyncio.run(products.sync(force=force))
        console.info(f"Product sync complete. Total products synced: {count}")

    def contracts(self, product_ids: str | None = None, limit: int | None = None, force: bool = False) -> None:
        count = asyncio.run(contracts.sync(product_ids=product_ids, limit=limit, force=force))
        console.info(f"Contract qualification complete. {count} products processed.")

    def prices(self, product_ids: str | None = None, limit: int | None = None, force: bool = False) -> None:
        count = asyncio.run(prices.sync(product_ids=product_ids, limit=limit, force=force))
        console.info(f"Price series complete. {count} products processed.")

    def themes(self, force: bool = False) -> None:
        p_count, n_count = asyncio.run(_run_themes(force=force))
        console.info(f"Theme taxonomy synced: {p_count} parents, {n_count} nodes.")

    def details(self, product_ids: str | None = None, limit: int | None = None, force: bool = False) -> None:
        asyncio.run(_run_details_only(product_ids, limit, force))

    def sentiment(self, product_ids: str | None = None, limit: int | None = None, force: bool = False) -> None:
        count = asyncio.run(sentiment.sync(product_ids=product_ids, limit=limit, force=force))
        console.info(f"Sentiment series complete. {count} products processed.")


cli = Ingest()

