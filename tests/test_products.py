import httpx
import pytest
import respx

from etfportfolio.core.db import connect
from etfportfolio.ingestion.products import sync, upsert_products


def test_upsert_products_preserves_created_at(tmp_path):
    db_file = str(tmp_path / "test_products.duckdb")

    with connect(db_file) as conn:
        p1 = [
            {
                "conid": 100,
                "type": "ETF",
                "symbol": "SPY",
                "exchangeId": "NYSE",
                "localSymbol": "SPY",
                "description": "SPDR S&P 500",
                "isin": "US123",
                "isPrimeExchId": "T",
                "isNewPdt": "F",
            }
        ]

        upsert_products(conn, p1)
        row1 = conn.execute(
            "SELECT product_id, symbol, is_primary_exchange_id, created_at, updated_at FROM bronze.products WHERE product_id = 100"
        ).fetchone()
        assert row1[0] == 100
        assert row1[1] == "SPY"
        assert row1[2] is True
        created_at_1 = row1[3]

        # Update product with new symbol and attributes
        p1_updated = [
            {
                "conid": 100,
                "type": "ETF",
                "symbol": "SPY_UPDATED",
                "exchangeId": "NYSE",
                "localSymbol": "SPY",
                "description": "SPDR S&P 500",
                "isin": "US123",
                "isPrimeExchId": "F",
                "isNewPdt": "T",
            }
        ]
        upsert_products(conn, p1_updated)
        row2 = conn.execute(
            "SELECT product_id, symbol, is_primary_exchange_id, created_at, updated_at FROM bronze.products WHERE product_id = 100"
        ).fetchone()
        assert row2[1] == "SPY_UPDATED"
        assert row2[2] is False
        # created_at should remain identical
        assert row2[3] == created_at_1


@pytest.mark.anyio
@respx.mock
async def test_products_sync_pagination(tmp_path):
    db_file = str(tmp_path / "test_products_sync.duckdb")

    # Mock page 1 (500 items)
    page1_items = [{"conid": i, "type": "ETF", "symbol": f"SYM{i}"} for i in range(1, 501)]
    # Mock page 2 (5 items -> causes termination)
    page2_items = [{"conid": i, "type": "FUND", "symbol": f"FUND{i}"} for i in range(501, 506)]

    route = respx.post("https://www.interactivebrokers.ie/webrest/search/products-by-filters")
    route.side_effect = [
        httpx.Response(200, json={"products": page1_items}),
        httpx.Response(200, json={"products": page2_items}),
    ]

    async with httpx.AsyncClient(base_url="https://www.interactivebrokers.ie") as client:
        with connect(db_file) as conn:
            total = await sync(client=client, conn=conn)
            assert total == 505

            db_count = conn.execute("SELECT COUNT(*) FROM bronze.products").fetchone()[0]
            assert db_count == 505
