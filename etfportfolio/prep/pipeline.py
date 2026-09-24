from __future__ import annotations

import logging
from datetime import date

from etfportfolio.core.db import db_connection
from etfportfolio.core.endpoints import ENDPOINTS
from etfportfolio.core.logging import console
from etfportfolio.core.progress import progress_bar
from etfportfolio.prep.extractors import EXTRACTOR_REGISTRY
from etfportfolio.prep.utils import DateContext, Observation, decompress_payload

logger = logging.getLogger(__name__)

URL_PREFIX_TO_NAME: dict[str, str] = {ep.url_prefix: ep.name for ep in ENDPOINTS}
EXCLUDED_ENDPOINTS: frozenset[str] = frozenset({"landing"})

BATCH_SIZE = 100

UPSERT_OBSERVATIONS_SQL = """
INSERT INTO silver.observations (
    product_id, family, metric, code, effective_date, date_source_depth,
    fetched_at, value, raw_value
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (product_id, family, metric, effective_date) DO UPDATE SET
    code = EXCLUDED.code,
    value = EXCLUDED.value,
    raw_value = EXCLUDED.raw_value,
    date_source_depth = EXCLUDED.date_source_depth,
    fetched_at = EXCLUDED.fetched_at
WHERE EXCLUDED.date_source_depth > date_source_depth
   OR (EXCLUDED.date_source_depth = date_source_depth
       AND EXCLUDED.fetched_at > fetched_at);
"""

INSERT_WATERMARK_SQL = """
INSERT INTO silver.processed_snapshots (snapshot_id, processed_at)
VALUES (?, CURRENT_TIMESTAMP);
"""


def _load_contract_currencies(conn, product_ids: list[int]) -> dict[int, str]:
    if not product_ids:
        return {}
    placeholders = ", ".join("?" for _ in product_ids)
    rows = conn.execute(
        f"SELECT product_id, currency FROM bronze.contracts WHERE product_id IN ({placeholders})",
        product_ids,
    ).fetchall()
    return {pid: curr for pid, curr in rows if curr is not None}


def run_observations(force: bool = False, db_path: str | None = None) -> int:
    """Extract point-in-time observations from bronze snapshots into silver.observations.

    Returns the number of snapshots watermarked this run (successes + empty payloads).
    Failed snapshots remain pending.
    """
    with db_connection(db_path) as conn:
        if force:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("DELETE FROM silver.monthly_panel")
                conn.execute("DELETE FROM silver.processed_snapshots")
                conn.execute("DELETE FROM silver.observations")
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

        known_currencies = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT currency FROM bronze.contracts WHERE currency IS NOT NULL"
            ).fetchall()
        }

        total_pending = len(pending_snapshots)
        watermarked_total = 0
        failed_count = 0
        empty_count = 0

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

                product_ids = list({row[1] for row in chunk})
                contract_currencies = _load_contract_currencies(conn, product_ids)

                staged_observations: dict[tuple[int, str, str, date], Observation] = {}
                succeeded_snapshot_ids: list[tuple[int]] = []

                for row in chunk:
                    snapshot_id, product_id, url_prefix, created_at, blob_hash = row

                    ep_name = URL_PREFIX_TO_NAME.get(url_prefix)
                    if ep_name is None:
                        raise ValueError(f"Unregistered extractor for url_prefix: {url_prefix}")

                    raw_blob = payload_map.get(blob_hash)
                    if raw_blob is None:
                        logger.error("Missing blob payload for hash %s, snapshot %d", blob_hash, snapshot_id)
                        failed_count += 1
                        continue

                    try:
                        data = decompress_payload(raw_blob)
                    except Exception as e:
                        logger.error("Decompression failed for snapshot %d: %s", snapshot_id, e)
                        failed_count += 1
                        continue

                    if not data or not isinstance(data, dict):
                        succeeded_snapshot_ids.append((snapshot_id,))
                        empty_count += 1
                        continue

                    if ep_name in EXCLUDED_ENDPOINTS:
                        succeeded_snapshot_ids.append((snapshot_id,))
                        continue

                    extractor = EXTRACTOR_REGISTRY.get(ep_name)
                    if extractor is None:
                        raise ValueError(f"Unregistered extractor for url_prefix: {url_prefix}")

                    date_ctx = DateContext(created_at.date())
                    contract_curr = contract_currencies.get(product_id)

                    try:
                        observations = extractor(
                            product_id,
                            data,
                            created_at,
                            date_ctx=date_ctx,
                            contract_currency=contract_curr,
                            known_currencies=known_currencies,
                        )
                    except Exception as e:
                        logger.error(
                            "Extract failed for product %d, snapshot %d, endpoint %s: %s",
                            product_id,
                            snapshot_id,
                            ep_name,
                            e,
                        )
                        failed_count += 1
                        continue

                    for obs in observations:
                        pk = (obs.product_id, obs.family, obs.metric, obs.effective_date)
                        if pk not in staged_observations:
                            staged_observations[pk] = obs
                        else:
                            existing = staged_observations[pk]
                            if obs.date_source_depth > existing.date_source_depth or (
                                obs.date_source_depth == existing.date_source_depth
                                and obs.fetched_at > existing.fetched_at
                            ):
                                staged_observations[pk] = obs

                    succeeded_snapshot_ids.append((snapshot_id,))

                conn.execute("BEGIN TRANSACTION")
                try:
                    if staged_observations:
                        rows = [obs.to_row() for obs in staged_observations.values()]
                        conn.executemany(UPSERT_OBSERVATIONS_SQL, rows)
                    if succeeded_snapshot_ids:
                        conn.executemany(INSERT_WATERMARK_SQL, succeeded_snapshot_ids)
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

                bar.update(len(chunk))
                bar.set_postfix_str(f"snap {chunk[-1][0]}")
                watermarked_total += len(succeeded_snapshot_ids)

        console.info(
            f"Observations finished: {watermarked_total} watermarked, {failed_count} failed, {empty_count} empty."
        )
        return watermarked_total
