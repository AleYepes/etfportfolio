import logging
from typing import Any

from etfportfolio.core.db import db_connection
from etfportfolio.core.logging import console
from etfportfolio.core.progress import progress_bar
from etfportfolio.core.utils import decompress_payload
from etfportfolio.observations.extractors import EXTRACTOR_REGISTRY

logger = logging.getLogger(__name__)

BATCH_SIZE = 500

INSERT_METRICS_SQL = """
INSERT INTO silver.metric_observations (product_id, source, metric_id, effective_date, fetched_at, value)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (product_id, source, metric_id, effective_date) DO UPDATE SET
    value = EXCLUDED.value,
    fetched_at = EXCLUDED.fetched_at;
"""

INSERT_ALLOCATIONS_SQL = """
INSERT INTO silver.portfolio_allocations (product_id, breakdown_type, item_name, item_code, effective_date, fetched_at, weight)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (product_id, breakdown_type, item_name, effective_date) DO UPDATE SET
    weight = EXCLUDED.weight,
    item_code = EXCLUDED.item_code,
    fetched_at = EXCLUDED.fetched_at;
"""

INSERT_THEMES_SQL = """
INSERT INTO silver.theme_exposures (product_id, theme_id, effective_date, fetched_at, weight, rank_adjusted_weight)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (product_id, theme_id, effective_date) DO UPDATE SET
    weight = EXCLUDED.weight,
    rank_adjusted_weight = EXCLUDED.rank_adjusted_weight,
    fetched_at = EXCLUDED.fetched_at;
"""

INSERT_WATERMARK_SQL = """
INSERT INTO silver.processed_snapshots (snapshot_id, processed_at)
VALUES (?, CURRENT_TIMESTAMP);
"""


def run_observations(force: bool = False, db_path: str | None = None) -> int:
    """Extracts and normalizes bronze snapshots into canonical silver observations tables."""
    with db_connection(db_path) as conn:
        if force:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("DELETE FROM silver.processed_snapshots")
                conn.execute("DELETE FROM silver.metric_observations")
                conn.execute("DELETE FROM silver.portfolio_allocations")
                conn.execute("DELETE FROM silver.theme_exposures")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        pending_snapshots = conn.execute(
            """
            SELECT
                s.snapshot_id,
                s.product_id,
                s.url_prefix,
                s.created_at,
                b.payload
            FROM bronze.snapshots s
            JOIN bronze.payload_blobs b ON s.hash = b.hash
            LEFT JOIN silver.processed_snapshots p ON s.snapshot_id = p.snapshot_id
            WHERE p.snapshot_id IS NULL
            ORDER BY s.snapshot_id ASC;
            """
        ).fetchall()

        if not pending_snapshots:
            console.info("All snapshots up to date.")
            return 0

        total_pending = len(pending_snapshots)
        processed_total = 0

        with progress_bar(total_pending, desc="Observations", unit="snapshot") as bar:
            for i in range(0, total_pending, BATCH_SIZE):
                chunk = pending_snapshots[i : i + BATCH_SIZE]

                metrics_rows: list[tuple[Any, ...]] = []
                allocations_rows: list[tuple[Any, ...]] = []
                themes_rows: list[tuple[Any, ...]] = []
                processed_ids: list[tuple[int]] = []

                for row in chunk:
                    snapshot_id, product_id, url_prefix, created_at, raw_blob = row

                    try:
                        data = decompress_payload(raw_blob)
                    except Exception as e:
                        logger.error("Decompression failed for snapshot %d: %s", snapshot_id, e)
                        raise

                    if not data:
                        # Empty stub payload: mark processed as valid no-op
                        processed_ids.append((snapshot_id,))
                        continue

                    extractor = EXTRACTOR_REGISTRY.get(url_prefix)
                    if extractor is not None:
                        extracted = extractor(product_id, data, created_at)
                        if url_prefix == "/tws.proxy/fundamentals/mf_holdings/":
                            allocations_rows.extend(extracted)
                        elif url_prefix == "/tws.proxy/knowledge-graph/ui/fund?conid=":
                            themes_rows.extend(extracted)
                        else:
                            metrics_rows.extend(extracted)

                    processed_ids.append((snapshot_id,))

                conn.execute("BEGIN TRANSACTION")
                try:
                    if metrics_rows:
                        conn.executemany(INSERT_METRICS_SQL, metrics_rows)
                    if allocations_rows:
                        conn.executemany(INSERT_ALLOCATIONS_SQL, allocations_rows)
                    if themes_rows:
                        conn.executemany(INSERT_THEMES_SQL, themes_rows)
                    if processed_ids:
                        conn.executemany(INSERT_WATERMARK_SQL, processed_ids)
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

                bar.update(len(chunk))
                bar.set_postfix_str(f"snap {chunk[-1][0]}")
                processed_total += len(chunk)

        return processed_total
