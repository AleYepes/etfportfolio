# Performance Specification: Silver Observations Pipeline & Zstd Decompressor

**Scope:** `etfportfolio/observations/pipeline.py`, `etfportfolio/core/utils.py`, `etfportfolio/ingestion/utils.py`

---

## 1. Problem Statement

The Silver Observations pipeline (`uv run python main.py prep`) processes ~193K Bronze snapshots joined with their zstd-compressed BLOB payloads. At `BATCH_SIZE = 500`, the process crashes with a DuckDB `OutOfMemoryException` after consuming >6.3 GB. At `BATCH_SIZE = 100`, it survives but consumes ~3 GB — far more than it should.

### 1.1 Root Cause

The memory problem is **not** caused by the batch processing loop or the extractors. It is caused by a single line in `etfportfolio/observations/pipeline.py` (line 72 at time of writing):

```python
pending_snapshots = conn.execute("""
    SELECT
        s.snapshot_id, s.product_id, s.url_prefix, s.created_at,
        b.payload
    FROM bronze.snapshots s
    JOIN bronze.payload_blobs b ON s.hash = b.hash
    LEFT JOIN silver.processed_snapshots p ON s.snapshot_id = p.snapshot_id
    WHERE p.snapshot_id IS NULL
    ORDER BY s.snapshot_id ASC;
""").fetchall()  # ← THIS LINE
```

`.fetchall()` forces DuckDB to:
1. Materialize the full JOIN result set (including all BLOB payloads) in DuckDB's internal buffers.
2. Copy **every row** into Python as a list of tuples.

With ~158K pending rows carrying compressed payloads (~287 MB compressed total), DuckDB's internal materialization of the JOIN + ORDER BY + anti-join pattern consumes ~6 GB of DuckDB-managed memory. The Python list adds another ~300–500 MB on top.

The subsequent `pending_snapshots[i : i + BATCH_SIZE]` slicing creates the illusion of batched processing, but all data was already loaded before the first batch starts. Processed batches' data remains pinned in the master list for the entire process lifetime.

### 1.2 Secondary Issue: Redundant `ZstdDecompressor` Allocation

`etfportfolio/core/utils.py` currently creates a new `zstd.ZstdDecompressor()` instance on every call to `decompress_payload()`. Over ~158K calls, this is wasteful. `ZstdDecompressor` is stateless and thread-safe for `.decompress()` — it should be a module-level singleton.

The same pattern exists in `etfportfolio/ingestion/utils.py` where `content_address()` creates a new `zstd.ZstdCompressor(level=3)` on every call.

---

## 2. Specification of Changes

### 2.1 Replace `.fetchall()` with `cursor.fetchmany()` in `pipeline.py`

> [!IMPORTANT]
> This is the critical fix. All other changes are minor optimizations.

**Current code** (lines ~58–83 of `etfportfolio/observations/pipeline.py`):
```python
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
        # ... batch processing ...
```

**Required behavior after the change:**

1. **Obtain the pending count cheaply** via a lightweight `COUNT(*)` query (no JOIN to `payload_blobs`, no BLOB transfer):
   ```sql
   SELECT count(*)
   FROM bronze.snapshots s
   LEFT JOIN silver.processed_snapshots p ON s.snapshot_id = p.snapshot_id
   WHERE p.snapshot_id IS NULL
   ```
   This runs in ~20ms and provides the total for the progress bar. If the count is 0, print `"All snapshots up to date."` and return 0 (same early-exit behavior as before).

2. **Execute the main query once** (same SQL as before), but **do not call `.fetchall()`**. Instead, retain the cursor object returned by `conn.execute(...)`.

3. **Loop with `cursor.fetchmany(BATCH_SIZE)`** to pull one batch of rows at a time:
   ```python
   cursor = conn.execute("""...""")  # same query, no .fetchall()

   while True:
       chunk = cursor.fetchmany(BATCH_SIZE)
       if not chunk:
           break
       # ... process chunk (same staging, extraction, upsert logic as before) ...
   ```
   This way, only `BATCH_SIZE` rows (with their payloads) are held in Python memory at any given time. DuckDB streams the result set incrementally.

4. **Progress bar**: Use the count from step 1 as the `total` parameter to `progress_bar()`.

5. **Everything else stays the same**: The per-batch staging dicts, extraction, deduplication, transaction boundaries, upsert SQL, watermark writes, and progress bar updates remain unchanged.

> [!NOTE]
> Do **not** change the `BATCH_SIZE` value; it should remain at its current value of `100`.

### 2.2 Module-Level `ZstdDecompressor` Singleton in `core/utils.py`

**Current code** (`etfportfolio/core/utils.py`):
```python
import orjson
import zstandard as zstd

def decompress_payload(compressed: bytes) -> Any:
    """Decompresses zstd-compressed bytes and parses JSON."""
    canonical = zstd.ZstdDecompressor().decompress(compressed)
    return orjson.loads(canonical)
```

**Required change**: Create a module-level singleton decompressor and reuse it:

```python
import orjson
import zstandard as zstd

_DECOMPRESSOR = zstd.ZstdDecompressor()

def decompress_payload(compressed: bytes) -> Any:
    """Decompresses zstd-compressed bytes and parses JSON."""
    canonical = _DECOMPRESSOR.decompress(compressed)
    return orjson.loads(canonical)
```

> [!NOTE]
> `ZstdDecompressor` does **not** accept a `level` argument. Compression level is a compression-time concept embedded in the stream header; the decompressor auto-detects it.

### 2.3 Module-Level `ZstdCompressor` Singleton in `ingestion/utils.py`

**Current code** (`etfportfolio/ingestion/utils.py`, inside `content_address()`):
```python
def content_address(payload: Any) -> tuple[int, bytes]:
    canonical = canonical_bytes(payload)
    digest = xxhash.xxh3_64_intdigest(canonical, seed=0)
    compressed = zstd.ZstdCompressor(level=3).compress(canonical)
    return digest, compressed
```

**Required change**: Create a module-level singleton compressor and reuse it:

```python
_COMPRESSOR = zstd.ZstdCompressor(level=3)

def content_address(payload: Any) -> tuple[int, bytes]:
    canonical = canonical_bytes(payload)
    digest = xxhash.xxh3_64_intdigest(canonical, seed=0)
    compressed = _COMPRESSOR.compress(canonical)
    return digest, compressed
```

> [!NOTE]
> The compression level `3` stays as a literal in `ingestion/utils.py`. It does **not** need to be extracted into `core/` — compression is only used in the `ingestion` module. Per the project's module organization rules, functionality used in only one directory stays in that directory.

---

## 3. Files Modified (Summary)

| File | Change |
|------|--------|
| `etfportfolio/observations/pipeline.py` | Replace `.fetchall()` with a `COUNT(*)` query + `cursor.fetchmany(BATCH_SIZE)` loop |
| `etfportfolio/core/utils.py` | Add module-level `_DECOMPRESSOR` singleton; use in `decompress_payload()` |
| `etfportfolio/ingestion/utils.py` | Add module-level `_COMPRESSOR` singleton; use in `content_address()` |

No other files are modified. No new files are created. No schema changes. No FRD updates.

---

## 4. Verification

1. **Functional correctness**: Run `uv run python main.py prep --force` and confirm it processes all snapshots to completion without errors. Verify the final row counts in `silver.product_metrics`, `silver.product_dimensions`, and `silver.processed_snapshots` match expectations (i.e., same as before the change if you have a baseline).

2. **Memory**: Monitor Python process memory (e.g., Activity Monitor) during `main.py prep`. Peak RSS should stay well under 1 GB (previously ~3 GB at `BATCH_SIZE=100`, crashed at `BATCH_SIZE=500`).

3. **Idempotency**: Run `uv run python main.py prep` a second time (without `--force`) and confirm it prints `"All snapshots up to date."` and processes 0 rows.

4. **Existing tests**: Run `uv run pytest tests/` and confirm no regressions. Note: the pipeline and extractor test files may have pre-existing sync issues with the current schema (they may reference legacy table names). These are pre-existing issues unrelated to this change and are out of scope.
