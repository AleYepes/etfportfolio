import logging
from datetime import UTC, datetime
from typing import Any

import duckdb
import httpx

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.core.logging import console
from etfportfolio.core.progress import progress_bar
from etfportfolio.ingestion.session import build_async_client
from etfportfolio.ingestion.utils import ProductContract, is_fresh

logger = logging.getLogger(__name__)

PAGE_SIZE = 500


def _parse_bool(val: Any) -> bool | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        if val.upper() in ("T", "TRUE", "1", "Y", "YES"):
            return True
        if val.upper() in ("F", "FALSE", "0", "N", "NO"):
            return False
    return bool(val)


def _check_products_freshness(conn: duckdb.DuckDBPyConnection) -> datetime | None:
    row = conn.execute("SELECT MAX(last_checked_at) FROM bronze.products").fetchone()
    return row[0] if row else None


def _count_products(conn: duckdb.DuckDBPyConnection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM bronze.products").fetchone()
    return row[0] if row else 0


def _stamp_products_last_checked(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("UPDATE bronze.products SET last_checked_at = (now() AT TIME ZONE 'UTC')")


def upsert_products(conn: duckdb.DuckDBPyConnection, products: list[dict[str, Any]]) -> int:
    """Upserts a list of raw product dicts into bronze.products."""
    if not products:
        return 0

    query = """
    INSERT INTO bronze.products (
        product_id, product_type, symbol, exchange_id, local_symbol, name, under_conid,
        isin, cusip, currency, country, is_primary_exchange_id, is_new_product,
        assoc_entity_id, fc_conid, created_at, updated_at
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
        (now() AT TIME ZONE 'UTC'), (now() AT TIME ZONE 'UTC')
    )
    ON CONFLICT (product_id) DO UPDATE SET
        product_type = EXCLUDED.product_type,
        symbol = EXCLUDED.symbol,
        exchange_id = EXCLUDED.exchange_id,
        local_symbol = EXCLUDED.local_symbol,
        name = EXCLUDED.name,
        under_conid = EXCLUDED.under_conid,
        isin = EXCLUDED.isin,
        cusip = EXCLUDED.cusip,
        currency = EXCLUDED.currency,
        country = EXCLUDED.country,
        is_primary_exchange_id = EXCLUDED.is_primary_exchange_id,
        is_new_product = EXCLUDED.is_new_product,
        assoc_entity_id = EXCLUDED.assoc_entity_id,
        fc_conid = EXCLUDED.fc_conid,
        updated_at = (now() AT TIME ZONE 'UTC')
    """

    count = 0
    for p in products:
        conid = p.get("conid")
        if conid is None:
            continue

        params = [
            int(conid),
            p.get("type"),
            p.get("symbol"),
            p.get("exchangeId"),
            p.get("localSymbol"),
            p.get("description"),
            str(p["underConid"]) if p.get("underConid") is not None else None,
            p.get("isin"),
            p.get("cusip"),
            p.get("currency"),
            p.get("country"),
            _parse_bool(p.get("isPrimeExchId")),
            _parse_bool(p.get("isNewPdt")),
            str(p["assocEntityId"]) if p.get("assocEntityId") is not None else None,
            str(p["fcConid"]) if p.get("fcConid") is not None else None,
        ]
        conn.execute(query, params)
        count += 1

    return count


async def sync(client: httpx.AsyncClient | None = None, force: bool = False) -> int:
    """Crawls webrest/search/products-by-filters endpoint and populates bronze.products."""
    page_number = 1
    total_synced = 0
    close_client = False
    clean_complete = False

    if client is None:
        client = build_async_client()
        close_client = True
    assert client is not None

    try:
        async with AsyncDbWorker(settings.db_path) as worker:
            if not force:
                last_checked = await worker.submit(_check_products_freshness)
                if last_checked and is_fresh(last_checked, settings.freshness_window_hours):
                    now = datetime.now(UTC)
                    if last_checked.tzinfo is None:
                        last_checked = last_checked.replace(tzinfo=UTC)
                    seconds = max(0.0, (now - last_checked).total_seconds())
                    hours = round(seconds / 3600.0, 1)
                    console.info(f"Products sync skipped (checked {hours}h ago; use --force to refresh).")
                    return await worker.submit(_count_products)

            existing_count = await worker.submit(_count_products)
            initial_total = existing_count if existing_count > 0 else None

            with progress_bar(initial_total, desc="Products", unit="product") as bar:
                while True:
                    bar.set_postfix_str(f"page {page_number}")
                    logger.debug("Fetching products page %d (pageSize=%d)...", page_number, PAGE_SIZE)
                    url = "/webrest/search/products-by-filters"
                    payload = {
                        "domain": "ie",
                        "newProduct": "all",
                        "pageNumber": page_number,
                        "pageSize": PAGE_SIZE,
                        "productCountry": [],
                        "productSymbol": "",
                        "productType": ["ETF", "FUND"],
                        "sortDirection": "asc",
                        "sortField": "conid",
                    }
                    resp = await client.post(url, json=payload)
                    if not resp.is_success:
                        logger.error(
                            "Products crawl failed at page %d with status %d: %s",
                            page_number,
                            resp.status_code,
                            resp.text,
                        )
                        break

                    data = resp.json()
                    products_list = data.get("products", [])
                    logger.debug("Received %d products on page %d", len(products_list), page_number)

                    if products_list:
                        await worker.submit(upsert_products, products_list)
                        total_synced += len(products_list)
                        bar.update(len(products_list))

                    if len(products_list) < PAGE_SIZE:
                        logger.debug(
                            "Pagination complete after %d pages. Total products: %d",
                            page_number,
                            total_synced,
                        )
                        clean_complete = True
                        if bar.total and total_synced != bar.total:
                            bar.total = total_synced
                            bar.refresh()
                        break

                    page_number += 1

            if clean_complete:
                await worker.submit(_stamp_products_last_checked)
    finally:
        if close_client:
            await client.aclose()

    return total_synced


def resolve_target_products(conn: duckdb.DuckDBPyConnection) -> list[ProductContract]:
    """Select active qualified products from bronze.contracts, excluding blocked exchanges."""
    blocked = settings.blocked_exchanges
    query = """
    SELECT
        product_id,
        symbol,
        sec_type,
        exchange_id,
        primary_exchange_id,
        currency,
        local_symbol,
        trading_class
    FROM bronze.contracts
    """
    params: list[Any] = []
    if blocked:
        placeholders = ", ".join(f"${i + 1}" for i in range(len(blocked)))
        query += f" WHERE (COALESCE(primary_exchange_id, exchange_id) IS NULL OR COALESCE(primary_exchange_id, exchange_id) NOT IN ({placeholders}))"
        params.extend(blocked)

    query += " ORDER BY product_id"
    rows = conn.execute(query, params).fetchall()

    if not rows:
        total_row = conn.execute("SELECT COUNT(*) FROM bronze.contracts").fetchone()
        total_contracts = total_row[0] if total_row else 0
        if total_contracts == 0:
            raise RuntimeError("bronze.contracts is empty. Run 'ingest contracts' first to qualify products.")
        logger.info("All products were excluded by blocked_exchanges.")
        return []

    return [
        ProductContract(
            product_id=row[0],
            symbol=row[1],
            sec_type=row[2],
            exchange_id=row[3],
            primary_exchange_id=row[4],
            currency=row[5],
            local_symbol=row[6],
            trading_class=row[7],
        )
        for row in rows
    ]
