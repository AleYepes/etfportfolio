"""
One-off migration script to restore truncated price series for products 229325937, 236798101, 332383610.
Idempotent and safe to run multiple times.
"""

import logging
import sys
from pathlib import Path

# Ensure repository root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etfportfolio.core.config import settings
from etfportfolio.core.db import db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TARGET_PRODUCTS = [229325937, 236798101, 332383610]


def restore_products() -> None:
    with db_connection(settings.db_path) as conn:
        for pid in TARGET_PRODUCTS:
            cold_count_row = conn.execute(
                "SELECT COUNT(*) FROM cold_storage.prices WHERE product_id = $1 AND reason = 'date_mismatch'",
                [pid],
            ).fetchone()
            cold_count = cold_count_row[0] if cold_count_row else 0

            if cold_count == 0:
                logger.info("Product %d: No archived date_mismatch rows in cold_storage. Skipping.", pid)
                continue

            bronze_count_row = conn.execute(
                "SELECT COUNT(*) FROM bronze.prices WHERE product_id = $1",
                [pid],
            ).fetchone()
            bronze_count = bronze_count_row[0] if bronze_count_row else 0

            if bronze_count > 1:
                logger.warning(
                    "Product %d: bronze.prices already contains %d rows (> 1). Aborting restore to avoid data loss.",
                    pid,
                    bronze_count,
                )
                continue

            logger.info("Product %d: Restoring %d historical bars from cold_storage...", pid, cold_count)
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("DELETE FROM bronze.prices WHERE product_id = $1", [pid])
                conn.execute(
                    """
                    INSERT INTO bronze.prices (product_id, date, open, high, low, close, volume, average, bar_count, updated_at)
                    SELECT product_id, date, open, high, low, close, volume, average, bar_count, now()
                    FROM cold_storage.prices
                    WHERE product_id = $1 AND reason = 'date_mismatch'
                    """,
                    [pid],
                )
                conn.execute(
                    "DELETE FROM cold_storage.prices WHERE product_id = $1 AND reason = 'date_mismatch'",
                    [pid],
                )
                conn.execute(
                    """
                    INSERT INTO bronze.price_status (product_id, last_checked_at, status, error_message)
                    VALUES ($1, now(), 'error', 'Restored from cold_storage truncation; awaiting incremental extension')
                    ON CONFLICT (product_id) DO UPDATE SET
                        last_checked_at = EXCLUDED.last_checked_at,
                        status = EXCLUDED.status,
                        error_message = EXCLUDED.error_message
                    """,
                    [pid],
                )
                conn.execute("COMMIT")
                logger.info("Product %d: Successfully restored.", pid)
            except Exception:
                conn.execute("ROLLBACK")
                logger.exception("Product %d: Failed to restore. Transaction rolled back.", pid)
                raise


if __name__ == "__main__":
    restore_products()
