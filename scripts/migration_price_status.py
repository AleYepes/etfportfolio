"""Temporary migration script: backfill bronze.price_status from bronze.prices."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import duckdb

# Ensure repository root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etfportfolio.core.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("migration")


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bronze.price_status (
    product_id      INTEGER PRIMARY KEY,
    last_checked_at TIMESTAMP NOT NULL,
    status          VARCHAR NOT NULL,  -- 'ok', 'no_data', 'error'
    error_message   VARCHAR            -- NULL for 'ok' and 'no_data'
);
"""

BACKFILL_SQL = """
INSERT INTO bronze.price_status (product_id, last_checked_at, status, error_message)
SELECT
    product_id,
    MAX(updated_at) AS last_checked_at,
    'ok' AS status,
    NULL AS error_message
FROM bronze.prices
GROUP BY product_id
ON CONFLICT (product_id) DO UPDATE SET
    last_checked_at = GREATEST(bronze.price_status.last_checked_at, EXCLUDED.last_checked_at),
    status = CASE
        WHEN bronze.price_status.status = 'error' THEN 'ok'
        ELSE bronze.price_status.status
    END;
"""


def run_migration(db_path: str | None = None) -> None:
    target_db = db_path or settings.db_path
    logger.info("Starting migration on database: %s", target_db)

    conn = duckdb.connect(target_db)
    try:
        # 1. Ensure table exists (DDL outside transaction)
        conn.execute(CREATE_TABLE_SQL)

        # Check initial state
        initial_status_row = conn.execute("SELECT COUNT(*) FROM bronze.price_status").fetchone()
        initial_status_count = initial_status_row[0] if initial_status_row else 0

        prices_product_row = conn.execute("SELECT COUNT(DISTINCT product_id) FROM bronze.prices").fetchone()
        prices_product_count = prices_product_row[0] if prices_product_row else 0

        logger.info(
            "Found %d unique product(s) in bronze.prices (current bronze.price_status rows: %d).",
            prices_product_count,
            initial_status_count,
        )

        # 2. Execute backfill in transaction
        conn.execute("BEGIN TRANSACTION")
        conn.execute(BACKFILL_SQL)
        conn.execute("COMMIT")

        final_status_row = conn.execute("SELECT COUNT(*) FROM bronze.price_status").fetchone()
        final_status_count = final_status_row[0] if final_status_row else 0
        logger.info(
            "Migration successful. bronze.price_status now contains %d product records (+%d new).",
            final_status_count,
            final_status_count - initial_status_count,
        )
    except Exception:
        conn.execute("ROLLBACK")
        logger.exception("Migration failed. Transaction rolled back.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    db_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_migration(db_arg)
