from __future__ import annotations

import logging
from datetime import date

from etfportfolio.core.db import db_connection
from etfportfolio.core.logging import console
from etfportfolio.core.progress import progress_bar
from etfportfolio.observations.extractors import EXTRACTOR_REGISTRY
from etfportfolio.observations.utils import DimensionTuple, MetricTuple, decompress_payload

logger = logging.getLogger(__name__)

BATCH_SIZE = 500

INSERT_METRICS_SQL = """
INSERT INTO silver.product_metrics (
    product_id, source, metric_id, effective_date, effective_date_source, fetched_at, value, raw_value
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (product_id, source, metric_id, effective_date) DO UPDATE SET
    value = EXCLUDED.value,
    raw_value = EXCLUDED.raw_value,
    effective_date_source = EXCLUDED.effective_date_source,
    fetched_at = EXCLUDED.fetched_at;
"""

INSERT_DIMENSIONS_SQL = """
INSERT INTO silver.product_dimensions (
    product_id, dimension_type, dimension_name, dimension_code, effective_date, effective_date_source, fetched_at, value, raw_value
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (product_id, dimension_type, dimension_name, effective_date) DO UPDATE SET
    value = EXCLUDED.value,
    dimension_code = EXCLUDED.dimension_code,
    raw_value = EXCLUDED.raw_value,
    effective_date_source = EXCLUDED.effective_date_source,
    fetched_at = EXCLUDED.fetched_at;
"""

INSERT_WATERMARK_SQL = """
INSERT INTO silver.processed_snapshots (snapshot_id, processed_at)
VALUES (?, CURRENT_TIMESTAMP);
"""


def run_observations(force: bool = False, db_path: str | None = None) -> int:
    with db_connection(db_path) as conn:
        if force:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("DELETE FROM silver.processed_snapshots")
                conn.execute("DELETE FROM silver.product_metrics")
                conn.execute("DELETE FROM silver.product_dimensions")
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
                s.hash
            FROM bronze.snapshots s
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
                unique_hashes = list({row[4] for row in chunk})
                placeholders = ", ".join("?" for _ in unique_hashes)
                blob_rows = conn.execute(
                    f"SELECT hash, payload FROM bronze.payload_blobs WHERE hash IN ({placeholders})",
                    unique_hashes,
                ).fetchall()
                payload_map = dict(blob_rows)

                metrics_staged: dict[tuple[int, str, str, date], MetricTuple] = {}
                dimensions_staged: dict[tuple[int, str, str, date], DimensionTuple] = {}
                processed_ids: list[tuple[int]] = []

                for row in chunk:
                    snapshot_id, product_id, url_prefix, created_at, blob_hash = row
                    raw_blob = payload_map[blob_hash]

                    try:
                        data = decompress_payload(raw_blob)
                    except Exception as e:
                        logger.error("Decompression failed for snapshot %d: %s", snapshot_id, e)
                        raise

                    if not data:
                        processed_ids.append((snapshot_id,))
                        continue

                    extractor = EXTRACTOR_REGISTRY.get(url_prefix)
                    if extractor is not None:
                        result = extractor(product_id, data, created_at)
                        for m in result.metrics:
                            metric_pk = (m[0], m[1], m[2], m[3])
                            # Inter-snapshot collision within batch: newer fetched_at overwrites
                            if metric_pk in metrics_staged:
                                if m[5] >= metrics_staged[metric_pk][5]:
                                    metrics_staged[metric_pk] = m
                            else:
                                metrics_staged[metric_pk] = m

                        for d in result.dimensions:
                            dim_pk = (d[0], d[1], d[2], d[4])
                            # Inter-snapshot collision within batch: newer fetched_at overwrites
                            if dim_pk in dimensions_staged:
                                if d[6] >= dimensions_staged[dim_pk][6]:
                                    dimensions_staged[dim_pk] = d
                            else:
                                dimensions_staged[dim_pk] = d

                    processed_ids.append((snapshot_id,))

                conn.execute("BEGIN TRANSACTION")
                try:
                    if metrics_staged:
                        conn.executemany(INSERT_METRICS_SQL, list(metrics_staged.values()))
                    if dimensions_staged:
                        conn.executemany(INSERT_DIMENSIONS_SQL, list(dimensions_staged.values()))
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
