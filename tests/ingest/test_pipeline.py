from unittest.mock import AsyncMock, patch

import pytest

from etfportfolio.ingest.pipeline import Ingest


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
