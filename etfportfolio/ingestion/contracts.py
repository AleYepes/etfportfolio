"""
Contract qualification via IB Gateway (clientId=1).
Fetches ContractDetails for all bronze.products and upserts into bronze.contracts.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
from ib_async import Contract, ContractDetails

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingestion.gateway import IBConnectionError, ib_connection

logger = logging.getLogger(__name__)

CONTRACT_FIELDS = [
    "sec_type",
    "symbol",
    "exchange_id",
    "primary_exchange_id",
    "currency",
    "local_symbol",
    "trading_class",
    "market_name",
    "min_tick",
    "order_types",
    "valid_exchanges",
    "price_magnifier",
    "under_conid",
    "name",
    "contract_month",
    "industry",
    "category",
    "subcategory",
    "time_zone_id",
    "trading_hours",
    "liquid_hours",
    "ev_rule",
    "ev_multiplier",
    "md_size_multiplier",
    "agg_group",
    "under_symbol",
    "under_sec_type",
    "market_rule_ids",
    "real_expiration_date",
    "last_trade_time",
    "stock_type",
    "min_size",
    "size_increment",
    "suggested_size_increment",
    "cusip",
    "ratings",
    "desc_append",
    "bond_type",
    "coupon_type",
    "callable",
    "putable",
    "coupon",
    "convertible",
    "maturity",
    "issue_date",
    "next_option_date",
    "next_option_type",
    "next_option_partial",
    "notes",
    "isin",
]

ATTR_MAP = {
    "secType": "sec_type",
    "symbol": "symbol",
    "exchange": "exchange_id",
    "primaryExchange": "primary_exchange_id",
    "currency": "currency",
    "localSymbol": "local_symbol",
    "tradingClass": "trading_class",
    "marketName": "market_name",
    "minTick": "min_tick",
    "orderTypes": "order_types",
    "validExchanges": "valid_exchanges",
    "priceMagnifier": "price_magnifier",
    "underConId": "under_conid",
    "longName": "name",
    "contractMonth": "contract_month",
    "industry": "industry",
    "category": "category",
    "subcategory": "subcategory",
    "timeZoneId": "time_zone_id",
    "tradingHours": "trading_hours",
    "liquidHours": "liquid_hours",
    "evRule": "ev_rule",
    "evMultiplier": "ev_multiplier",
    "mdSizeMultiplier": "md_size_multiplier",
    "aggGroup": "agg_group",
    "underSymbol": "under_symbol",
    "underSecType": "under_sec_type",
    "marketRuleIds": "market_rule_ids",
    "realExpirationDate": "real_expiration_date",
    "lastTradeTime": "last_trade_time",
    "stockType": "stock_type",
    "minSize": "min_size",
    "sizeIncrement": "size_increment",
    "suggestedSizeIncrement": "suggested_size_increment",
    "cusip": "cusip",
    "ratings": "ratings",
    "descAppend": "desc_append",
    "bondType": "bond_type",
    "couponType": "coupon_type",
    "callable": "callable",
    "putable": "putable",
    "coupon": "coupon",
    "convertible": "convertible",
    "maturity": "maturity",
    "issueDate": "issue_date",
    "nextOptionDate": "next_option_date",
    "nextOptionType": "next_option_type",
    "nextOptionPartial": "next_option_partial",
    "notes": "notes",
    "isin": "isin",
}


def _get_contract_details_attr(cd: ContractDetails, attr: str) -> Any:
    """Safely get an attribute from ContractDetails, handling missing keys."""
    try:
        return getattr(cd, attr)
    except AttributeError, KeyError:
        return None


def _flatten_contract_details(cd: ContractDetails) -> dict[str, Any]:
    """Flatten ContractDetails into a dict with our column names."""
    result = {}
    for attr, col in ATTR_MAP.items():
        val = _get_contract_details_attr(cd, attr)
        result[col] = val
    return result


def upsert_contract(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    cd: ContractDetails,
) -> None:
    """Upsert a single ContractDetails into bronze.contracts."""
    data = _flatten_contract_details(cd)
    data["product_id"] = product_id

    columns = list(data.keys()) + ["created_at", "updated_at"]
    placeholders = ", ".join([f"${i + 1}" for i in range(len(columns))])
    values = list(data.values()) + [datetime.now(UTC), datetime.now(UTC)]

    insert_sql = f"""
    INSERT INTO bronze.contracts ({", ".join(columns)})
    VALUES ({placeholders})
    ON CONFLICT (product_id) DO UPDATE SET
        {", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in ("product_id", "created_at")])}
    """

    conn.execute(insert_sql, values)


def _select_product_ids(conn: duckdb.DuckDBPyConnection) -> list[int]:
    """Return all product IDs from bronze.products."""
    rows = conn.execute("SELECT product_id FROM bronze.products ORDER BY product_id").fetchall()
    return [row[0] for row in rows]


def _contract_is_fresh(conn: duckdb.DuckDBPyConnection, product_id: int, cutoff: datetime) -> bool:
    """Check if a contract's updated_at is newer than cutoff."""
    row = conn.execute(
        "SELECT updated_at FROM bronze.contracts WHERE product_id = $1",
        [product_id],
    ).fetchone()
    return row is not None and row[0] >= cutoff


async def _qualify_one(ib: Any, product_id: int) -> ContractDetails | None:
    """Qualify a single contract using conId only."""
    contract = Contract(conId=product_id)
    details = await ib.reqContractDetailsAsync(contract)
    if details:
        return details[0]
    logger.warning("No contract details found for product %d", product_id)
    return None


async def _run_contract_qualification(force: bool = False) -> int:
    """Run the contract qualification phase."""
    async with AsyncDbWorker(settings.db_path) as worker:
        product_ids = await worker.submit(_select_product_ids)

        now = datetime.now(UTC)
        cutoff = now - timedelta(hours=24)

        to_process = []
        for pid in product_ids:
            if force:
                to_process.append(pid)
            else:
                fresh = await worker.submit(_contract_is_fresh, pid, cutoff)
                if not fresh:
                    to_process.append(pid)

        logger.info("Contract qualification: %d products to process", len(to_process))

        semaphore = asyncio.Semaphore(1)

        async with ib_connection(client_id=1) as ib:

            async def process_one(product_id: int):
                async with semaphore:
                    try:
                        cd = await _qualify_one(ib, product_id)
                        if cd:
                            await worker.submit(upsert_contract, product_id, cd)
                    except IBConnectionError:
                        raise
                    except Exception as e:
                        logger.error("Failed to qualify product %d: %s", product_id, e)

            tasks = [process_one(pid) for pid in to_process]
            await asyncio.gather(*tasks)

    return len(to_process)


async def sync(force: bool = False) -> int:
    """Public entry point for the contracts phase."""
    return await _run_contract_qualification(force=force)
