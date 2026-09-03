"""
Contract qualification via IB Gateway (clientId=1).
Fetches ContractDetails for all bronze.products and upserts into bronze.contracts.
"""

import logging
from datetime import UTC, datetime
from typing import Any

import duckdb
from ib_async import Contract, ContractDetails

from etfportfolio.core.config import settings
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.core.progress import progress_bar
from etfportfolio.ingestion.gateway import IBConnectionError, ib_connection
from etfportfolio.ingestion.utils import is_fresh

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
    """Safely get an attribute from ContractDetails or nested Contract, handling missing keys."""
    try:
        val = getattr(cd, attr, None)
        if val is not None:
            return val
    except (AttributeError, KeyError):
        pass

    if getattr(cd, "contract", None) is not None:
        try:
            val = getattr(cd.contract, attr, None)
            if val is not None:
                return val
        except (AttributeError, KeyError):
            pass

    return None


def _flatten_contract_details(cd: ContractDetails) -> dict[str, Any]:
    """Flatten ContractDetails into a dict with our column names."""
    result = {}
    for attr, col in ATTR_MAP.items():
        val = _get_contract_details_attr(cd, attr)
        result[col] = val

    if not result.get("isin") and getattr(cd, "secIdList", None):
        for tv in cd.secIdList:
            if getattr(tv, "tag", None) == "ISIN":
                result["isin"] = getattr(tv, "value", None)
                break

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


def _select_target_product_ids(
    conn: duckdb.DuckDBPyConnection,
    product_ids: str | None = None,
    limit: int | None = None,
) -> list[int]:
    """Return target product IDs from bronze.products."""
    if product_ids is not None and limit is not None:
        raise ValueError("product_ids and limit are mutually exclusive.")

    if product_ids is not None:
        from etfportfolio.ingestion.products import _parse_product_ids_arg
        return _parse_product_ids_arg(product_ids)

    query = "SELECT product_id FROM bronze.products ORDER BY product_id"
    if limit is not None and limit > 0:
        query += f" LIMIT {int(limit)}"
    rows = conn.execute(query).fetchall()
    return [row[0] for row in rows]


def _contract_is_fresh(conn: duckdb.DuckDBPyConnection, product_id: int) -> bool:
    """Check if a contract's updated_at is within freshness_window_hours."""
    row = conn.execute(
        "SELECT updated_at FROM bronze.contracts WHERE product_id = $1",
        [product_id],
    ).fetchone()
    if not row or not row[0]:
        return False
    return is_fresh(row[0], settings.freshness_window_hours)


async def _qualify_one(ib: Any, product_id: int) -> ContractDetails | None:
    """Qualify a single contract using conId only."""
    contract = Contract(conId=product_id)
    details = await ib.reqContractDetailsAsync(contract)
    if details:
        return details[0]
    logger.warning("No contract details found for product %d", product_id)
    return None


async def _run_contract_qualification(
    product_ids: str | None = None,
    limit: int | None = None,
    force: bool = False,
) -> int:
    """Run the contract qualification phase."""
    async with AsyncDbWorker(settings.db_path) as worker:
        target_ids = await worker.submit(_select_target_product_ids, product_ids, limit)

        to_process = []
        for pid in target_ids:
            if force:
                to_process.append(pid)
            else:
                fresh = await worker.submit(_contract_is_fresh, pid)
                if not fresh:
                    to_process.append(pid)

        logger.info("Contract qualification: %d products to process", len(to_process))

        if not to_process:
            return 0

        async with ib_connection(client_id=1) as ib:
            with progress_bar(len(to_process), desc="Contracts") as bar:
                for pid in to_process:
                    bar.set_postfix_str(str(pid))
                    if not ib.isConnected():
                        raise IBConnectionError("IB Gateway connection was lost during contract qualification.")
                    try:
                        cd = await _qualify_one(ib, pid)
                        if cd:
                            await worker.submit(upsert_contract, pid, cd)
                    except IBConnectionError:
                        raise
                    except Exception as e:
                        logger.error("Failed to qualify product %d: %s", pid, e)
                    finally:
                        bar.update(1)

    return len(to_process)


async def sync(
    product_ids: str | None = None,
    limit: int | None = None,
    force: bool = False,
) -> int:
    """Public entry point for the contracts phase."""
    return await _run_contract_qualification(product_ids=product_ids, limit=limit, force=force)
