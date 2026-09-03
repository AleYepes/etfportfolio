import duckdb
import pytest
from ib_async import Contract, ContractDetails, TagValue

from etfportfolio.core.db import apply_schema
from etfportfolio.ingestion.contracts import (
    _clean_val,
    _flatten_contract_details,
    upsert_contract,
)


def test_clean_val():
    assert _clean_val("") is None
    assert _clean_val("   ") is None
    assert _clean_val("\t\n") is None
    assert _clean_val("ETF") == "ETF"
    assert _clean_val("  US/Eastern  ") == "US/Eastern"
    assert _clean_val(0) == 0
    assert _clean_val(0.0) == 0.0
    assert _clean_val(False) is False
    assert _clean_val(True) is True
    assert _clean_val(None) is None


@pytest.fixture
def sample_contract_details():
    return ContractDetails(
        contract=Contract(
            secType="STK",
            conId=8335,
            symbol="ICF",
            exchange="SMART",
            primaryExchange="BATS",
            currency="USD",
            localSymbol="ICF",
            tradingClass="ICF",
        ),
        marketName="ICF",
        minTick=0.01,
        orderTypes="ACTIVETIM,AD,ADDONT",
        validExchanges="SMART,AMEX,NYSE",
        priceMagnifier=1,
        underConId=0,
        longName="ISHARES SELECT U.S. REIT ETF",
        contractMonth="",
        industry="",
        category="",
        subcategory="",
        timeZoneId="US/Eastern",
        tradingHours="20260903:0400-20260903:2000",
        liquidHours="20260903:0930-20260903:1600",
        evRule="",
        evMultiplier=0,
        mdSizeMultiplier=1,
        aggGroup=1,
        underSymbol="",
        underSecType="",
        marketRuleIds="26,26,26",
        secIdList=[TagValue(tag="ISIN", value="US4642875649")],
        realExpirationDate="",
        lastTradeTime="",
        stockType="ETF",
        minSize=0.0001,
        sizeIncrement=0.0001,
        suggestedSizeIncrement=100.0,
        cusip="",
        ratings="",
        descAppend="",
        bondType="",
        couponType="",
        callable=False,
        putable=False,
        coupon=0,
        convertible=False,
        maturity="",
        issueDate="",
        nextOptionDate="",
        nextOptionType="",
        nextOptionPartial=False,
        notes="",
    )


def test_flatten_contract_details_normalizes_empty_strings(sample_contract_details):
    data = _flatten_contract_details(sample_contract_details)

    # Empty string attributes should all be None
    empty_expected = [
        "contract_month",
        "industry",
        "category",
        "subcategory",
        "ev_rule",
        "under_symbol",
        "under_sec_type",
        "real_expiration_date",
        "last_trade_time",
        "cusip",
        "ratings",
        "desc_append",
        "bond_type",
        "coupon_type",
        "maturity",
        "issue_date",
        "next_option_date",
        "next_option_type",
        "notes",
    ]
    for key in empty_expected:
        assert data[key] is None, f"Expected data[{key}] to be None, got {data[key]!r}"

    # Non-empty string attributes must be preserved
    assert data["sec_type"] == "STK"
    assert data["symbol"] == "ICF"
    assert data["name"] == "ISHARES SELECT U.S. REIT ETF"
    assert data["isin"] == "US4642875649"
    assert data["stock_type"] == "ETF"
    assert data["time_zone_id"] == "US/Eastern"

    # Numeric and boolean attributes must be preserved
    assert data["min_tick"] == 0.01
    assert data["price_magnifier"] == 1
    assert data["under_conid"] == 0
    assert data["callable"] is False
    assert data["putable"] is False
    assert data["coupon"] == 0


def test_upsert_contract_stores_nulls_in_db(sample_contract_details):
    conn = duckdb.connect(":memory:")
    apply_schema(conn)

    # Insert prerequisite product into bronze.products
    conn.execute(
        """
        INSERT INTO bronze.products (product_id, symbol, created_at, updated_at)
        VALUES (8335, 'ICF', now(), now())
        """
    )

    upsert_contract(conn, 8335, sample_contract_details)

    row = conn.execute(
        """
        SELECT
            sec_type, symbol, name, isin, contract_month, industry, category,
            subcategory, cusip, notes, callable, coupon
        FROM bronze.contracts
        WHERE product_id = 8335
        """
    ).fetchone()

    assert row is not None
    sec_type, symbol, name, isin, contract_month, industry, category, subcat, cusip, notes, callable_, coupon = row

    assert sec_type == "STK"
    assert symbol == "ICF"
    assert name == "ISHARES SELECT U.S. REIT ETF"
    assert isin == "US4642875649"

    # Empty string values stored as NULL in DuckDB
    assert contract_month is None
    assert industry is None
    assert category is None
    assert subcat is None
    assert cusip is None
    assert notes is None

    # Booleans and numbers preserved
    assert callable_ is False
    assert coupon == 0.0

    # Ensure zero empty strings exist in bronze.contracts
    empty_strings_row = conn.execute(
        """
        SELECT COUNT(*)
        FROM bronze.contracts
        WHERE contract_month = '' OR industry = '' OR category = '' OR cusip = '' OR notes = ''
        """
    ).fetchone()
    assert empty_strings_row is not None
    assert empty_strings_row[0] == 0
