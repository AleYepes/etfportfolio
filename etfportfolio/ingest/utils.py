"""Ingestion-shared helpers

Content-addressing, snapshot blob store, freshness checks, and ProductContract DTO.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import duckdb
import orjson
import xxhash
import zstandard as zstd

_TYPE_RANK = {type(None): 0, bool: 1, int: 2, float: 2, str: 3, list: 4, dict: 5}


@dataclass(frozen=True)
class ProductContract:
    product_id: int
    symbol: str | None = None
    sec_type: str | None = None
    exchange_id: str | None = None
    primary_exchange_id: str | None = None
    currency: str | None = None
    local_symbol: str | None = None
    trading_class: str | None = None


def _sort_key(value: Any) -> tuple[int, str]:
    return (
        _TYPE_RANK.get(type(value), 6),
        orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode(),
    )


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _canonicalize(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return sorted((_canonicalize(v) for v in value), key=_sort_key)
    return value


def canonical_bytes(payload: Any) -> bytes:
    """Return deterministically sorted canonical JSON bytes."""
    return orjson.dumps(_canonicalize(payload), option=orjson.OPT_SORT_KEYS)


_COMPRESSOR = zstd.ZstdCompressor(level=3)


def content_address(payload: Any) -> tuple[int, bytes]:
    """Returns (hash, compressed_bytes) ready for bronze.payload_blobs.

    Hash is an unsigned 64-bit int (xxh3_64_intdigest with seed=0) compatible with UBIGINT.
    """
    canonical = canonical_bytes(payload)
    digest = xxhash.xxh3_64_intdigest(canonical, seed=0)
    compressed = _COMPRESSOR.compress(canonical)
    return digest, compressed


def store_blob(conn: duckdb.DuckDBPyConnection, digest: int, compressed: bytes) -> None:
    """Ensures a content-addressed payload blob is stored (idempotent on duplicate hash)."""
    conn.execute(
        """
        INSERT INTO bronze.payload_blobs (hash, payload)
        VALUES ($1, $2)
        ON CONFLICT (hash) DO NOTHING
        """,
        [digest, compressed],
    )


def is_fresh(last_seen: datetime | None, hours: float) -> bool:
    """True iff `last_seen` is within `hours` of now.

    Naive datetimes are treated as UTC. `None` is never fresh.
    """
    if last_seen is None:
        return False
    now = datetime.now(UTC)
    if last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=UTC)
    delta = max(0.0, (now - last_seen).total_seconds())
    return delta <= (hours * 3600.0)
