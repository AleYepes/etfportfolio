import asyncio
import logging
from typing import Any

import duckdb
import httpx

from etfportfolio.core.db import connect, current
from etfportfolio.ingestion.session import build_async_client

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


def upsert_products(conn: duckdb.DuckDBPyConnection, products: list[dict[str, Any]]) -> int:
    """Upserts a list of raw product dicts into bronze.products."""
    if not products:
        return 0

    query = """
    INSERT INTO bronze.products (
        product_id, type, symbol, exchange_id, local_symbol, name, under_conid,
        isin, cusip, currency, country, is_primary_exchange_id, is_new_product,
        assoc_entity_id, fc_conid, created_at, updated_at
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
        now(), now()
    )
    ON CONFLICT (product_id) DO UPDATE SET
        type = EXCLUDED.type,
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
        updated_at = now()
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
            p.get("description"),  # description maps to name
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


async def sync(
    client: httpx.AsyncClient | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Crawls webrest/search/products-by-filters endpoint and populates bronze.products."""
    page_number = 1
    total_synced = 0
    close_client = False

    if client is None:
        client = build_async_client()
        close_client = True

    try:
        while True:
            logger.info("Fetching products page %d (pageSize=%d)...", page_number, PAGE_SIZE)
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
                    "Products crawl failed at page %d with status %d: %s", page_number, resp.status_code, resp.text
                )
                break

            data = resp.json()
            products_list = data.get("products", [])
            logger.info("Received %d products on page %d", len(products_list), page_number)

            if conn is not None:
                upsert_products(conn, products_list)
            else:
                upsert_products(current(), products_list)

            total_synced += len(products_list)

            # Terminate pagination when fewer than pageSize items are returned
            if len(products_list) < PAGE_SIZE:
                logger.info("Pagination complete after %d pages. Total products: %d", page_number, total_synced)
                break

            page_number += 1
    finally:
        if close_client:
            await client.aclose()

    return total_synced


class ProductsCLI:
    def sync(self) -> None:
        """Standalone product discovery crawl."""
        with connect() as conn:
            count = asyncio.run(sync(conn=conn))
            print(f"Product sync complete. Total products synced: {count}")


cli = ProductsCLI()
