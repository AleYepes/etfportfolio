from typing import Any

import orjson
import xxhash
import zstandard as zstd

_TYPE_RANK = {type(None): 0, bool: 1, int: 2, float: 2, str: 3, list: 4, dict: 5}


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


def content_address(payload: Any) -> tuple[int, bytes]:
    """Returns (hash, compressed_bytes) ready for bronze.payload_blobs.

    Hash is an unsigned 64-bit int (xxh3_64_intdigest with seed=0) compatible with UBIGINT.
    """
    canonical = canonical_bytes(payload)
    digest = xxhash.xxh3_64_intdigest(canonical, seed=0)
    compressed = zstd.ZstdCompressor(level=3).compress(canonical)
    return digest, compressed


def decompress_payload(compressed: bytes) -> Any:
    """Decompresses zstd-compressed bytes and parses JSON."""
    canonical = zstd.ZstdDecompressor().decompress(compressed)
    return orjson.loads(canonical)
