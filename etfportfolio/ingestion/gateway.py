"""
Async context manager for IB Gateway connections.
Used by contracts.py (clientId=1) and prices.py (clientId=2).
"""

import logging
from contextlib import asynccontextmanager

from ib_async import IB

from etfportfolio.core.config import settings

logger = logging.getLogger(__name__)


class IBConnectionError(RuntimeError):
    """Raised when the connection to IB Gateway cannot be established."""


@asynccontextmanager
async def ib_connection(client_id: int):
    """Connect to IB Gateway, yield the IB instance, then disconnect.

    Raises IBConnectionError with clear instructions if the connection fails.
    """
    ib = IB()
    try:
        await ib.connectAsync(
            host=settings.ib_gateway_host,
            port=settings.ib_gateway_port,
            clientId=client_id,
            timeout=settings.ib_gateway_timeout,
        )
        logger.info("Connected to IB Gateway with clientId=%d", client_id)
        yield ib
    except ConnectionRefusedError as e:
        raise IBConnectionError(
            "IB Gateway is not reachable at "
            f"{settings.ib_gateway_host}:{settings.ib_gateway_port}. "
            "Please launch IB Gateway and enable API connections."
        ) from e
    except TimeoutError as e:
        raise IBConnectionError(f"Connection to IB Gateway timed out after {settings.ib_gateway_timeout}s.") from e
    finally:
        ib.disconnect()
        logger.info("Disconnected from IB Gateway (clientId=%d)", client_id)
