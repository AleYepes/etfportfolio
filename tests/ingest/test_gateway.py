from unittest.mock import AsyncMock, patch

import pytest

from etfportfolio.ingest.gateway import IBConnectionError, ib_connection


@pytest.mark.anyio
async def test_ib_connection_lifecycle():
    with (
        patch("etfportfolio.ingest.gateway.IB") as mock_ib_cls,
        patch("etfportfolio.ingest.session.reconcile_account_id") as mock_reconcile,
    ):
        mock_ib_instance = mock_ib_cls.return_value
        mock_ib_instance.connectAsync = AsyncMock()
        mock_ib_instance.managedAccounts.return_value = ["U123456"]

        async with ib_connection(client_id=1) as ib:
            assert ib == mock_ib_instance
            mock_ib_instance.connectAsync.assert_called_once()
            mock_reconcile.assert_called_once_with("U123456")

        mock_ib_instance.disconnect.assert_called_once()


@pytest.mark.anyio
async def test_ib_connection_refused_raises_ib_connection_error():
    with patch("etfportfolio.ingest.gateway.IB") as mock_ib_cls:
        mock_ib_instance = mock_ib_cls.return_value
        mock_ib_instance.connectAsync = AsyncMock(side_effect=ConnectionRefusedError("Connection refused"))

        with pytest.raises(IBConnectionError, match="IB Gateway is not reachable"):
            async with ib_connection(client_id=1):
                pass


@pytest.mark.anyio
async def test_ib_connection_timeout_raises_ib_connection_error():
    with patch("etfportfolio.ingest.gateway.IB") as mock_ib_cls:
        mock_ib_instance = mock_ib_cls.return_value
        mock_ib_instance.connectAsync = AsyncMock(side_effect=TimeoutError("Timed out"))

        with pytest.raises(IBConnectionError, match="Connection to IB Gateway timed out"):
            async with ib_connection(client_id=1):
                pass
