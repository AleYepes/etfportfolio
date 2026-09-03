from typing import Any

import orjson
import zstandard as zstd


def decompress_payload(compressed: bytes) -> Any:
    """Decompresses zstd-compressed bytes and parses JSON."""
    canonical = zstd.ZstdDecompressor().decompress(compressed)
    return orjson.loads(canonical)
