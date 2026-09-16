from __future__ import annotations

import logging
from datetime import timedelta

import duckdb

from etfportfolio.core.config import settings
from etfportfolio.core.db import db_connection
from etfportfolio.core.logging import console
from etfportfolio.ingest.prices import PRICE_ABS_TOL, PRICE_REL_TOL, SETTLEMENT_TRADING_DAYS

logger = logging.getLogger(__name__)


def clean_cold_storage(conn: duckdb.DuckDBPyConnection) -> int:
    """Purges redundant runs in cold_storage.prices.

    A cold storage run is redundant if all historical bars older than
    SETTLEMENT_TRADING_DAYS match bronze.prices within tolerance.
    Corporate actions (splits/dividends) alter prices across years of history,
    whereas false-positive archives differ only on recent settlement days or not at all.

    Returns the number of deleted rows.
    """
    runs = conn.execute("SELECT DISTINCT product_id, run_id FROM cold_storage.prices").fetchall()
    deleted_rows = 0

    for pid, run_id in runs:
        # Find maximum date in this archived run
        max_date_row = conn.execute(
            "SELECT MAX(date) FROM cold_storage.prices WHERE product_id = ? AND run_id = ?",
            [pid, run_id],
        ).fetchone()
        if not max_date_row or max_date_row[0] is None:
            continue
        max_date = max_date_row[0]
        # 5 trading days horizon approximated by calendar buffer
        cutoff_date = max_date - timedelta(days=SETTLEMENT_TRADING_DAYS + 2)

        # Check if any historical bar (date <= cutoff_date) differs between cold_storage and bronze
        # If count of mismatches is 0, the run is not a corporate action
        mismatches = conn.execute(
            """
            SELECT count(*)
            FROM cold_storage.prices c
            LEFT JOIN bronze.prices b ON c.product_id = b.product_id AND c.date = b.date
            WHERE c.product_id = ? AND c.run_id = ? AND c.date <= ?
            AND (
                b.date IS NULL OR
                abs(c.open - b.open) > ? OR abs(c.open - b.open) / nullif(abs(c.open), 0) > ? OR
                abs(c.high - b.high) > ? OR abs(c.high - b.high) / nullif(abs(c.high), 0) > ? OR
                abs(c.low - b.low) > ? OR abs(c.low - b.low) / nullif(abs(c.low), 0) > ? OR
                abs(c.close - b.close) > ? OR abs(c.close - b.close) / nullif(abs(c.close), 0) > ?
            )
            """,
            [
                pid,
                run_id,
                cutoff_date,
                PRICE_ABS_TOL,
                PRICE_REL_TOL,
                PRICE_ABS_TOL,
                PRICE_REL_TOL,
                PRICE_ABS_TOL,
                PRICE_REL_TOL,
                PRICE_ABS_TOL,
                PRICE_REL_TOL,
            ],
        ).fetchone()[0]

        if mismatches == 0:
            count = conn.execute(
                "DELETE FROM cold_storage.prices WHERE product_id = ? AND run_id = ?",
                [pid, run_id],
            ).fetchone()[0]
            deleted_rows += count
            logger.info("Product %d (run %s): deleted %d redundant cold storage rows.", pid, run_id, count)

    return deleted_rows


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
