import asyncio
import logging

import duckdb
import httpx

from etfportfolio.core.config import settings
from etfportfolio.core.db import connect
from etfportfolio.core.logging import console
from etfportfolio.ingestion import contracts, details, prices, products, session, themes

logger = logging.getLogger(__name__)


async def _run_details_phase(
    conn: duckdb.DuckDBPyConnection,
    client: httpx.AsyncClient,
    account_id: str,
    target_ids: list[int],
    force: bool,
) -> None:
    """Runs the details phase across a resolved list of target products,
    reporting progress and a final tally on the console channel.
    """
    console.info(f"Processing {len(target_ids)} product(s)...")
    semaphore = asyncio.Semaphore(settings.endpoint_concurrency)
    failures = 0

    for idx, product_id in enumerate(target_ids, 1):
        console.info(f"[{idx}/{len(target_ids)}] Processing product {product_id}...")
        try:
            ok = await details.process_product(client, conn, product_id, account_id, semaphore, force=force)
        except session.SessionInvalidError:
            logger.error("Session became invalid while processing product %d. Aborting.", product_id)
            raise
        if not ok:
            failures += 1

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


async def _run_themes(conn: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    client, account_id = await session.ensure_session()
    try:
        return await themes.sync(client=client, conn=conn)
    finally:
        await client.aclose()


async def _run_details_only(product_ids: str | None, limit: int | None, force: bool) -> None:
    with connect() as conn:
        client, account_id = await session.ensure_session()
        console.info(f"Session OK. Active account: {account_id}")
        try:
            target_ids = products.resolve_target_ids(conn, product_ids, limit)
            await _run_details_phase(conn, client, account_id, target_ids, force)
        finally:
            await client.aclose()


async def _run_full(product_ids: str | None, limit: int | None, force: bool) -> None:
    with connect() as conn:
        console.info("=== Phase 1: Product discovery ===")
        try:
            count = await products.sync(conn=conn)
            console.info(f"Product sync complete. Total products synced: {count}")
        except Exception as e:
            logger.error("Product sync failed (continuing with existing products in DB): %s", e)

        console.info("=== Phase 2: Contract qualification ===")
        try:
            count = await contracts.sync(conn=conn, force=force)
            console.info(f"Contract qualification complete. {count} products processed.")
        except Exception as e:
            logger.error("Contract qualification failed: %s", e)
            raise

        console.info("=== Phase 3: Price series ===")
        try:
            count = await prices.sync(conn=conn, force=force)
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
                p_count, n_count = await themes.sync(client=client, conn=conn)
                console.info(f"Theme taxonomy synced: {p_count} parents, {n_count} nodes.")
            except Exception as e:
                logger.error("Theme taxonomy sync failed: %s", e)

            console.info("=== Phase 6: Product details ===")
            target_ids = products.resolve_target_ids(conn, product_ids, limit)
            await _run_details_phase(conn, client, account_id, target_ids, force)
        finally:
            await client.aclose()

    console.info("=== Ingestion complete ===")


class Ingest:
    """CLI surface for the ingestion pipeline: `main.py ingest <phase>`.

    All logic lives in the domain modules (session, products, contracts,
    prices, themes, details) — this class only wires phases together and
    reports progress.
    """

    def __call__(self, product_ids: str | None = None, limit: int | None = None, force: bool = False) -> None:
        """Runs the full ingestion pipeline: products -> contracts -> prices -> session -> themes -> details.

        `product_ids` and `limit` scope Phase 6 (details) to a subset of
        products; both are mutually exclusive and default to the full catalog.
        `force` bypasses the landing gate during Phase 6, fetching every
        gated endpoint for every target product regardless of whether landing
        changed.
        """
        asyncio.run(_run_full(product_ids, limit, force))

    def session(self) -> None:
        """Ensures a valid IBKR session, launching interactive login only if needed."""
        account_id = asyncio.run(_run_session())
        console.info(f"Session OK. Active account: {account_id}")

    def products(self) -> None:
        """Crawls the IBKR product catalog into bronze.products."""
        with connect() as conn:
            count = asyncio.run(products.sync(conn=conn))
        console.info(f"Product sync complete. Total products synced: {count}")

    def contracts(self, force: bool = False) -> None:
        """Qualifies contracts via IB Gateway and populates bronze.contracts."""
        with connect() as conn:
            count = asyncio.run(contracts.sync(conn=conn, force=force))
        console.info(f"Contract qualification complete. {count} products processed.")

    def prices(self, force: bool = False) -> None:
        """Fetches historical daily prices via IB Gateway and populates bronze.prices."""
        with connect() as conn:
            count = asyncio.run(prices.sync(conn=conn, force=force))
        console.info(f"Price series complete. {count} products processed.")

    def themes(self) -> None:
        """Syncs the global theme taxonomy into bronze.themes."""
        with connect() as conn:
            p_count, n_count = asyncio.run(_run_themes(conn))
        console.info(f"Theme taxonomy synced: {p_count} parents, {n_count} nodes.")

    def details(self, product_ids: str | None = None, limit: int | None = None, force: bool = False) -> None:
        """Runs the details phase (landing + gated + non-gated) for target products.

        `product_ids` and `limit` are mutually exclusive; with neither given,
        targets the full catalog. `force` bypasses the landing gate, fetching
        every gated endpoint for every target product regardless of whether
        landing changed.
        """
        asyncio.run(_run_details_only(product_ids, limit, force))


cli = Ingest()
