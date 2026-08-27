import logging
from typing import Any

import duckdb
import httpx

from etfportfolio.core.db import gc_preview_blob
from etfportfolio.core.utils import content_address

logger = logging.getLogger(__name__)

LANDING_URL_TEMPLATE = "/tws.proxy/fundamentals/landing/{product_id}?widgets=objective,keyProfile,lipper_ratings,holdings,mf_key_ratios,ownership,mstar&lang=en"


async def fetch_and_gate(
    client: httpx.AsyncClient,
    product_id: int,
    conn: duckdb.DuckDBPyConnection,
) -> tuple[bool, int, bytes, dict[str, Any]]:
    """Fetches minimal landing widget payload, computes content-address digest,

    and compares against bronze.snapshot_previews.
    Returns: (changed, digest, compressed_bytes, payload)
    """
    url = LANDING_URL_TEMPLATE.format(product_id=product_id)
    resp = await client.get(url)

    if not resp.is_success:
        raise RuntimeError(f"Landing fetch for product {product_id} failed with status {resp.status_code}: {resp.text}")

    payload = resp.json()
    digest, compressed = content_address(payload)

    row = conn.execute(
        "SELECT hash FROM bronze.snapshot_previews WHERE product_id = $1",
        [product_id],
    ).fetchone()

    if row is None:
        changed = True
    else:
        existing_hash = row[0]
        changed = existing_hash != digest

    return changed, digest, compressed, payload


def commit_preview(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    digest: int,
    compressed: bytes,
) -> None:
    """Commits pending preview hash to bronze.snapshot_previews and runs blob GC on old hash."""
    row = conn.execute(
        "SELECT hash FROM bronze.snapshot_previews WHERE product_id = $1",
        [product_id],
    ).fetchone()
    old_hash = row[0] if row else None

    # 1. Ensure payload blob is stored
    conn.execute(
        """
        INSERT INTO bronze.payload_blobs (hash, payload)
        VALUES ($1, $2)
        ON CONFLICT (hash) DO NOTHING
        """,
        [digest, compressed],
    )

    # 2. Upsert snapshot_previews
    conn.execute(
        """
        INSERT INTO bronze.snapshot_previews (product_id, hash, updated_at)
        VALUES ($1, $2, now())
        ON CONFLICT (product_id) DO UPDATE SET
            hash = EXCLUDED.hash,
            updated_at = now()
        """,
        [product_id, digest],
    )

    # 3. Clean up orphaned old hash blob if replaced
    if old_hash is not None and old_hash != digest:
        gc_preview_blob(conn, old_hash)
