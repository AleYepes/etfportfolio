import json
from pathlib import Path

import httpx
import pytest
import respx

from etfportfolio.core.db import connect
from etfportfolio.ingestion.pipeline import _parse_product_ids_arg, run_pipeline


def test_parse_product_ids_arg(tmp_path):
    assert _parse_product_ids_arg(None) is None
    assert _parse_product_ids_arg("100, 200,300") == [100, 200, 300]

    sample_file = tmp_path / "ids.txt"
    sample_file.write_text("101\n# comment\n102\n\n103\n", encoding="utf-8")
    assert _parse_product_ids_arg(str(sample_file)) == [101, 102, 103]


@pytest.mark.anyio
@respx.mock
async def test_full_pipeline_mocked_run(tmp_path):
    db_file = str(tmp_path / "test_pipeline.duckdb")

    base = "https://www.interactivebrokers.ie"

    # Phase 1: Products
    products_payload = {"products": [{"conid": 756733, "type": "ETF", "symbol": "SPY"}]}
    respx.post(f"{base}/webrest/search/products-by-filters").mock(
        return_value=httpx.Response(200, json=products_payload)
    )

    # Phase 2: Probe
    respx.get(f"{base}/tws.proxy/acesws/accountList").mock(
        return_value=httpx.Response(200, json={"accessibleAccounts": ["U1234567"]})
    )

    # Phase 2.5: Themes taxonomy
    themes_tax_payload = {
        "parents": [{"key": "p1", "numId": 1, "name": "Tech"}],
        "nodes": [{"key": "n1", "numId": 2, "name": "AI", "parentKey": "p1"}],
    }
    respx.get(f"{base}/tws.proxy/knowledge-graph/meta/themes").mock(
        return_value=httpx.Response(200, json=themes_tax_payload)
    )

    # Phase 3: Landing for 756733
    landing_fixture = json.loads(Path("tests/fixtures/landing_equity.json").read_text(encoding="utf-8"))
    respx.get(
        f"{base}/fundamentals/landing/756733?widgets=objective,keyProfile,lipper_ratings,holdings,mf_key_ratios,ownership,mstar&lang=en"
    ).mock(return_value=httpx.Response(200, json=landing_fixture))

    # Snapshot endpoints
    holdings_fx = json.loads(Path("tests/fixtures/holdings_equity.json").read_text(encoding="utf-8"))
    ratios_fx = json.loads(Path("tests/fixtures/ratios_equity.json").read_text(encoding="utf-8"))
    ownership_fx = json.loads(Path("tests/fixtures/ownership_complete.json").read_text(encoding="utf-8"))
    profile_fx = json.loads(Path("tests/fixtures/profile_equity.json").read_text(encoding="utf-8"))
    lipper_fx = json.loads(Path("tests/fixtures/lipper_equity.json").read_text(encoding="utf-8"))
    mstar_fx = json.loads(Path("tests/fixtures/mstar_equity.json").read_text(encoding="utf-8"))
    esg_fx = json.loads(Path("tests/fixtures/esg.json").read_text(encoding="utf-8"))
    themes_prod_fx = json.loads(Path("tests/fixtures/themes.json").read_text(encoding="utf-8"))

    respx.get(f"{base}/fundamentals/mf_holdings/756733?lang=en").mock(
        return_value=httpx.Response(200, json=holdings_fx)
    )
    respx.get(f"{base}/fundamentals/mf_ratios_fundamentals/756733?lang=en").mock(
        return_value=httpx.Response(200, json=ratios_fx)
    )
    respx.get(
        f"{base}/fundamentals/ownership/756733?fields=owners_types,institutional_owners,insider_owners,institutional_total,insider_total,institutional_summary,insider_summary,others_summary&lang=en"
    ).mock(return_value=httpx.Response(200, json=ownership_fx))
    respx.get(f"{base}/fundamentals/mf_profile_and_fees/756733?lang=en").mock(
        return_value=httpx.Response(200, json=profile_fx)
    )
    respx.get(f"{base}/fundamentals/mf_lip_ratings/756733?lang=en").mock(
        return_value=httpx.Response(200, json=lipper_fx)
    )
    respx.get(f"{base}/mstar/fund/detail?conid=756733&lang=en").mock(return_value=httpx.Response(200, json=mstar_fx))
    respx.get(f"{base}/impact/esg/756733?accounts=U1234567&lang=en").mock(return_value=httpx.Response(200, json=esg_fx))
    respx.get(f"{base}/knowledge-graph/ui/fund?conid=756733&max=999999999&lang=en").mock(
        return_value=httpx.Response(200, json=themes_prod_fx)
    )

    # Series endpoints
    price_fx = json.loads(Path("tests/fixtures/price_incremental.json").read_text(encoding="utf-8"))
    sentiment_fx = json.loads(Path("tests/fixtures/sentiment_incremental.json").read_text(encoding="utf-8"))

    respx.get(f"{base}/fundamentals/mf_performance_chart/756733?chart_period=MAX&lang=en").mock(
        return_value=httpx.Response(200, json=price_fx)
    )
    respx.get(url__regex=r".*/sma/request\?type=search&conid=756733&from=.*").mock(
        return_value=httpx.Response(200, json=sentiment_fx)
    )

    async with httpx.AsyncClient(base_url=base) as client:
        with connect(db_file) as conn:
            await run_pipeline(conn=conn, client=client)

            # Check products
            assert conn.execute("SELECT COUNT(*) FROM bronze.products").fetchone()[0] == 1
            # Check themes
            assert conn.execute("SELECT COUNT(*) FROM bronze.themes").fetchone()[0] == 2
            # Check preview
            assert (
                conn.execute("SELECT COUNT(*) FROM bronze.snapshot_previews WHERE product_id = 756733").fetchone()[0]
                == 1
            )
            # Check snapshots (8 endpoints)
            assert conn.execute("SELECT COUNT(*) FROM bronze.snapshots WHERE product_id = 756733").fetchone()[0] == 8
            # Check series (2 endpoints: price and sentiment)
            assert conn.execute("SELECT COUNT(*) FROM bronze.series WHERE product_id = 756733").fetchone()[0] == 2


@pytest.mark.anyio
@respx.mock
async def test_pipeline_session_invalid_halting(tmp_path):
    db_file = str(tmp_path / "test_pipeline_invalid.duckdb")
    base = "https://www.interactivebrokers.ie"

    respx.post(f"{base}/webrest/search/products-by-filters").mock(
        return_value=httpx.Response(200, json={"products": [{"conid": 999, "type": "ETF", "symbol": "XYZ"}]})
    )
    respx.get(f"{base}/tws.proxy/acesws/accountList").mock(
        return_value=httpx.Response(400, json={"error": "Invalid headers", "statusCode": 400})
    )

    async with httpx.AsyncClient(base_url=base) as client:
        with connect(db_file) as conn:
            with pytest.raises(RuntimeError, match="Session authentication failed"):
                await run_pipeline(conn=conn, client=client)
