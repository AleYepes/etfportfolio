from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from etfportfolio.core import endpoints
from etfportfolio.core.db import AsyncDbWorker
from etfportfolio.ingest.pipeline import (
    Ingest,
    _is_product_fully_fresh,
    _run_details_phase,
)


def test_ingest_cli_dispatch():
    cli = Ingest()

    with patch("etfportfolio.ingest.pipeline.products.sync", new_callable=AsyncMock) as mock_sync:
        mock_sync.return_value = 10
        cli.products(force=False)
        mock_sync.assert_called_once_with(force=False)

    with patch("etfportfolio.ingest.pipeline.contracts.sync", new_callable=AsyncMock) as mock_sync:
        mock_sync.return_value = 15
        cli.contracts(force=True)
        mock_sync.assert_called_once_with(force=True)

    with patch("etfportfolio.ingest.pipeline.prices.sync", new_callable=AsyncMock) as mock_sync:
        mock_sync.return_value = 20
        cli.prices(force=False)
        mock_sync.assert_called_once_with(force=False)

    with patch("etfportfolio.ingest.pipeline.fx.sync") as mock_sync:
        mock_sync.return_value = 8
        cli.fx(force=True)
        mock_sync.assert_called_once_with(force=True)

    with patch("etfportfolio.ingest.pipeline._run_themes", new_callable=AsyncMock) as mock_themes:
        mock_themes.return_value = (5, 10)
        cli.themes(force=True)
        mock_themes.assert_called_once_with(force=True)

    with patch("etfportfolio.ingest.pipeline._run_details_only", new_callable=AsyncMock) as mock_details:
        cli.details(force=False)
        mock_details.assert_called_once_with(force=False)

    with patch("etfportfolio.ingest.clean.run_clean") as mock_clean:
        cli.clean()
        mock_clean.assert_called_once()


def test_cli_signatures_reject_unused_flags():
    ingest = Ingest()
    with pytest.raises(TypeError):
        ingest(limit=10)  # type: ignore

    with pytest.raises(TypeError):
        ingest.contracts(product_ids="1001")  # type: ignore

    with pytest.raises(TypeError):
        ingest.prices(limit=5)  # type: ignore

    with pytest.raises(TypeError):
        ingest.details(product_ids="1001,1002")  # type: ignore

    with pytest.raises(TypeError):
        ingest.fx(limit=3)  # type: ignore


def test_is_product_fully_fresh():
    now = datetime.now(UTC)
    stale = now - timedelta(days=10)

    # 1. Landing and all ungated endpoints fresh -> True
    landing_fresh = {1001: now}
    endpoint_all_fresh = {(1001, ep.url_prefix): now for ep in endpoints.UNGATED_ENDPOINTS}
    assert _is_product_fully_fresh(1001, landing_fresh, endpoint_all_fresh) is True

    # 2. Landing stale -> False
    landing_stale = {1001: stale}
    assert _is_product_fully_fresh(1001, landing_stale, endpoint_all_fresh) is False

    # 3. Landing missing -> False
    assert _is_product_fully_fresh(1001, {}, endpoint_all_fresh) is False

    # 4. One ungated endpoint stale -> False
    endpoint_one_stale = dict(endpoint_all_fresh)
    endpoint_one_stale[(1001, endpoints.UNGATED_ENDPOINTS[0].url_prefix)] = stale
    assert _is_product_fully_fresh(1001, landing_fresh, endpoint_one_stale) is False


@pytest.mark.anyio
async def test_run_details_phase_landing_and_ungated_freshness(tmp_path):
    db_file = str(tmp_path / "test_pipeline_details.duckdb")
    now = datetime.now(UTC)

    # product 1: landing and ungated fresh
    # product 2: landing missing/stale
    landing_cache = {1: now}
    endpoint_cache = {(1, ep.url_prefix): now for ep in endpoints.UNGATED_ENDPOINTS}

    async with AsyncDbWorker(db_file) as worker, httpx.AsyncClient() as client:
        with (
            patch("etfportfolio.ingest.details.load_landing_freshness_cache", return_value=landing_cache),
            patch("etfportfolio.ingest.details.load_endpoint_freshness_cache", return_value=endpoint_cache),
            patch("etfportfolio.ingest.details.process_product", new_callable=AsyncMock) as mock_process,
        ):
            from etfportfolio.ingest.details import ProductDetailsResult

            mock_process.return_value = ProductDetailsResult(
                ok=True,
                product_skipped_fresh=False,
                endpoints_skipped_fresh=0,
            )

            await _run_details_phase(
                worker,
                client,
                "U123456",
                target_ids=[1, 2],
                force=False,
            )

            # Product 1 was fully fresh, so only product 2 should have been processed
            assert mock_process.call_count == 1
            call_args, _ = mock_process.call_args
            # Positional arguments: client, worker, product_id, account_id, semaphore, landing_cache, endpoint_cache
            assert call_args[2] == 2  # product_id
            assert call_args[5] == landing_cache
            assert call_args[6] == endpoint_cache
