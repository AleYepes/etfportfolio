from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from etfportfolio.core.db import apply_schema
from etfportfolio.ingest.utils import (
    canonical_bytes,
    content_address,
    gc_preview_blob,
    is_fresh,
    store_blob,
)
from etfportfolio.prep.utils import decompress_payload


def test_is_fresh():
    now = datetime.now(UTC)
    assert not is_fresh(None, 24.0)

    # 1 hour ago is fresh within 24h
    one_hour_ago = now - timedelta(hours=1)
    assert is_fresh(one_hour_ago, 24.0)
    assert not is_fresh(one_hour_ago, 0.5)

    # 48 hours ago is stale
    stale = now - timedelta(hours=48)
    assert not is_fresh(stale, 24.0)

    # Naive datetime treated as UTC
    naive_recent = (now - timedelta(hours=2)).replace(tzinfo=None)
    assert is_fresh(naive_recent, 24.0)
    naive_stale = (now - timedelta(hours=25)).replace(tzinfo=None)
    assert not is_fresh(naive_stale, 24.0)

    # Future-skewed timestamp clamped to 0.0 delta
    future = now + timedelta(minutes=10)
    assert is_fresh(future, 24.0)


def test_content_addressing_determinism():
    payload_1 = {"b": 2, "a": 1, "nested": {"z": 9, "y": 8}, "list": [3, 1, 2]}
    payload_2 = {"a": 1, "nested": {"y": 8, "z": 9}, "b": 2, "list": [2, 1, 3]}

    canon_1 = canonical_bytes(payload_1)
    canon_2 = canonical_bytes(payload_2)
    assert canon_1 == canon_2

    hash_1, comp_1 = content_address(payload_1)
    hash_2, comp_2 = content_address(payload_2)
    assert hash_1 == hash_2
    assert comp_1 == comp_2
    assert isinstance(hash_1, int)
    assert hash_1 >= 0  # unsigned 64-bit

    decompressed = decompress_payload(comp_1)
    # Lists are canonicalized to sorted order
    assert decompressed == {"a": 1, "b": 2, "list": [1, 2, 3], "nested": {"y": 8, "z": 9}}


def test_landing_stamp_rule():
    # Rule: stamp last_checked_at <=> NOT (fetch_gated AND NOT gated_success)
    def should_stamp(fetch_gated: bool, gated_success: bool) -> bool:
        return not (fetch_gated and not gated_success)

    # No gated fetch attempted -> stamp
    assert should_stamp(fetch_gated=False, gated_success=True) is True
    assert should_stamp(fetch_gated=False, gated_success=False) is True

    # Gated fetch attempted and succeeded -> stamp
    assert should_stamp(fetch_gated=True, gated_success=True) is True

    # Gated fetch attempted and failed -> do NOT stamp
    assert should_stamp(fetch_gated=True, gated_success=False) is False


@pytest.fixture
def db_conn():
    conn = duckdb.connect(":memory:")
    apply_schema(conn)
    # Insert test product into bronze.products
    conn.execute(
        """
        INSERT INTO bronze.products (product_id, symbol, created_at, updated_at)
        VALUES (1001, 'TEST', now(), now())
        """
    )
    return conn


def test_store_blob_and_gc(db_conn):
    digest, comp = content_address({"key": "value"})
    store_blob(db_conn, digest, comp)
    # Idempotent insert
    store_blob(db_conn, digest, comp)

    # Initially not referenced in snapshots or snapshot_previews -> GC removes it
    assert gc_preview_blob(db_conn, digest) is True
    row = db_conn.execute("SELECT COUNT(*) FROM bronze.payload_blobs WHERE hash = $1", [digest]).fetchone()
    assert row[0] == 0

    # Re-store and reference in snapshot_previews
    store_blob(db_conn, digest, comp)
    db_conn.execute(
        """
        INSERT INTO bronze.snapshot_previews (product_id, hash, updated_at, last_checked_at)
        VALUES (1001, $1, now(), now())
        """,
        [digest],
    )
    # GC should NOT delete it because it's referenced
    assert gc_preview_blob(db_conn, digest) is False
    row = db_conn.execute("SELECT COUNT(*) FROM bronze.payload_blobs WHERE hash = $1", [digest]).fetchone()
    assert row[0] == 1
