import orjson, xxhash, zstandard as zstd

_TYPE_RANK = {type(None): 0, bool: 1, int: 2, float: 2, str: 3, list: 4, dict: 5}

def _sort_key(value):
    return (_TYPE_RANK.get(type(value), 6),
            orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode())

def _canonicalize(value):
    if isinstance(value, dict):
        return {k: _canonicalize(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return sorted((_canonicalize(v) for v in value), key=_sort_key)
    return value

def canonical_bytes(payload) -> bytes:
    return orjson.dumps(_canonicalize(payload), option=orjson.OPT_SORT_KEYS)

def content_address(payload) -> tuple[str, bytes]:
    """Returns (hash, compressed_bytes) ready for bronze.payload_blobs."""
    canonical = canonical_bytes(payload)
    digest = xxhash.xxh128_hexdigest(canonical)
    compressed = zstd.ZstdCompressor(level=3).compress(canonical)
    return digest, compressed