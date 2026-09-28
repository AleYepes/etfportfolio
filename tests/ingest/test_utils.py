from datetime import UTC, datetime, timedelta

from etfportfolio.ingest.utils import (
    canonical_bytes,
    content_address,
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


def test_store_blob_idempotent(db_conn):
    digest, comp = content_address({"key": "value"})
    store_blob(db_conn, digest, comp)
    # Idempotent insert
    store_blob(db_conn, digest, comp)

    row = db_conn.execute("SELECT COUNT(*) FROM bronze.payload_blobs WHERE hash = $1", [digest]).fetchone()
    assert row[0] == 1
