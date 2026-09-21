from __future__ import annotations

import logging

import duckdb

from etfportfolio.core.config import settings
from etfportfolio.core.db import db_connection
from etfportfolio.core.logging import console

logger = logging.getLogger(__name__)


def clean_cold_storage(conn: duckdb.DuckDBPyConnection) -> int:
    """Purges redundant runs in cold_storage.prices and cold_storage.fx.

    Returns the number of deleted rows.
    """
    from etfportfolio.ingest.fx import FX_SPEC
    from etfportfolio.ingest.prices import PRICES_SPEC
    from etfportfolio.ingest.series import clean_series_cold_storage

    deleted_prices = clean_series_cold_storage(conn, PRICES_SPEC)
    deleted_fx = clean_series_cold_storage(conn, FX_SPEC)

    logger.info("Cold storage purged: %d prices rows, %d fx rows.", deleted_prices, deleted_fx)
    return deleted_prices + deleted_fx


def clean_payload_blobs(conn: duckdb.DuckDBPyConnection) -> int:
    """Deletes unreferenced payload blobs from bronze.payload_blobs.

    Returns the number of deleted blobs.
    """
    res = conn.execute(
        """
        DELETE FROM bronze.payload_blobs
        WHERE hash NOT IN (
            SELECT hash FROM bronze.snapshots
            UNION
            SELECT hash FROM bronze.snapshot_previews
        )
        """
    ).fetchone()
    return res[0] if res else 0


def checkpoint(conn: duckdb.DuckDBPyConnection) -> None:
    """Compact DuckDB storage and write WAL to disk."""
    conn.execute("CHECKPOINT")


def run_clean() -> None:
    """Execute complete ingestion cleanup suite."""
    console.info("Starting ingestion cleanup...")
    with db_connection(settings.db_path) as conn:
        deleted_cold = clean_cold_storage(conn)
        console.info(f"Cold storage cleanup: purged {deleted_cold:,} redundant rows.")

        deleted_blobs = clean_payload_blobs(conn)
        console.info(f"Payload blob cleanup: purged {deleted_blobs:,} orphaned blobs.")

        checkpoint(conn)
        console.info("DuckDB storage checkpointed and compacted.")
    console.info("Ingestion cleanup complete.")
