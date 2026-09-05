"""Inspect bronze snapshots and generate representative Frankenstein JSON payloads.

This script scans unique payload blobs in data/etf.duckdb across all 7 snapshot
endpoints (excluding landing), generates property-complete sample JSON payloads,
and writes them to docs/samples/.
"""

from __future__ import annotations

import copy
import shutil
from pathlib import Path
from typing import Any

import duckdb
import orjson
import zstandard as zstd

DB_PATH = "data/etf.duckdb"
OUTPUT_DIR = Path("docs/samples")
OLD_OUTPUT_DIR = Path("data/samples")

# The 7 detail endpoints in bronze.snapshots
ENDPOINTS: dict[str, str] = {
    "ratios": "/tws.proxy/fundamentals/mf_ratios_fundamentals/",
    "holdings": "/tws.proxy/fundamentals/mf_holdings/",
    "profile": "/tws.proxy/fundamentals/mf_profile_and_fees/",
    "lipper": "/tws.proxy/fundamentals/mf_lip_ratings/",
    "mstar": "/tws.proxy/mstar/fund/detail?conid=",
    "esg": "/tws.proxy/impact/esg/",
    "theme_weights": "/tws.proxy/knowledge-graph/ui/fund?conid=",
}

# Candidate identifier fields for semantic metric/factor items
# 'subsection_id' is placed before 'id' to prevent collisions in Morningstar commentaries
ITEM_ID_CANDIDATES = ("name_tag", "subsection_id", "id", "name", "key", "country_code", "code")

# Semantic metric/factor lists where we maintain the full union of distinct metric items
METRIC_FACTOR_LISTS = frozenset(
    {
        "ratios",
        "financials",
        "fixed_income",
        "dividend",
        "zscore",
        "fund_and_profile",
        "summary",
        "commentary",
        "content",
        "children",
        "reports",
        "fields",
        "expenses_allocation",
        "10_year",
        "3_year",
        "5_year",
        "overall",
    }
)


def _get_item_key(item: dict[str, Any], list_name: str) -> str | None:
    """Return a unique semantic identifier for a list item."""
    # Composite key for commentary items that share section IDs
    if "id" in item and "subsection_id" in item and item["subsection_id"]:
        return f"commentary:{item['id']}#{item['subsection_id']}"

    for id_field in ITEM_ID_CANDIDATES:
        if id_field in item and item[id_field] is not None:
            return f"{id_field}:{item[id_field]}"
    return None


def merge_dicts(
    target: dict[str, Any],
    source: dict[str, Any],
    path: str = "",
    seen_signatures_by_list: dict[str, set[frozenset[str]]] | None = None,
) -> dict[str, Any]:
    """Recursively deep-merge source dict into target dict."""
    if seen_signatures_by_list is None:
        seen_signatures_by_list = {}
    for k, v in source.items():
        sub_path = f"{path}.{k}" if path else k
        if k not in target or target[k] is None:
            if isinstance(v, list):
                target[k] = merge_lists([], v, sub_path, seen_signatures_by_list)
            elif isinstance(v, dict):
                target[k] = merge_dicts({}, v, sub_path, seen_signatures_by_list)
            else:
                target[k] = copy.deepcopy(v)
        elif isinstance(target[k], dict) and isinstance(v, dict):
            target[k] = merge_dicts(target[k], v, sub_path, seen_signatures_by_list)
        elif isinstance(target[k], list) and isinstance(v, list):
            target[k] = merge_lists(target[k], v, sub_path, seen_signatures_by_list)
        elif target[k] == "" and v:
            target[k] = v
    return target


def merge_lists(
    target: list[Any],
    source: list[Any],
    list_name: str,
    seen_signatures_by_list: dict[str, set[frozenset[str]]] | None = None,
) -> list[Any]:
    """Merge two lists respecting the boundary between invariant schema and domain variety.

    - For METRIC_FACTOR_LISTS: Preserves the full union of distinct metrics/fields,
      merging attributes on match so every observed property variant is present.
    - For high-cardinality entity lists (top_10 holdings, currency, countries, debt types, theme weights):
      Guarantees every distinct property signature (e.g. presence vs absence of optional
      discriminators like 'code' or 'country_code') is retained, while capping samples
      at a compact 3-5 items to avoid payload bloating.
    - For primitives (e.g. themes list of strings): keeps up to 10 distinct values.
    """
    if not source:
        return target

    if seen_signatures_by_list is None:
        seen_signatures_by_list = {}

    list_basename = list_name.split(".")[-1]
    is_metric_list = list_basename in METRIC_FACTOR_LISTS

    if (target and isinstance(target[0], dict)) or (source and isinstance(source[0], dict)):
        target_by_id: dict[str, dict[str, Any]] = {}
        for idx, item in enumerate(target):
            if isinstance(item, dict):
                item_id = _get_item_key(item, list_name) or f"idx_{idx}"
                target_by_id[item_id] = item

        seen_sigs = seen_signatures_by_list.setdefault(list_name, set())
        for item in target:
            if isinstance(item, dict):
                seen_sigs.add(frozenset(item.keys()))

        max_sample_len = 5

        for item in source:
            if not isinstance(item, dict):
                continue

            item_id = _get_item_key(item, list_name)
            item_signature = frozenset(item.keys())

            if is_metric_list:
                if item_id and item_id in target_by_id:
                    # Merge properties into existing item
                    target_by_id[item_id] = merge_dicts(target_by_id[item_id], item, list_name, seen_signatures_by_list)
                    seen_sigs.add(frozenset(target_by_id[item_id].keys()))
                elif item_id:
                    # Metric item not yet seen: preserve full union
                    new_item = copy.deepcopy(item)
                    target_by_id[item_id] = new_item
                    target.append(new_item)
                    seen_sigs.add(item_signature)
                else:
                    # Nameless metric item (e.g. {'as_of_date': 0} placeholder in reports)
                    if item in target:
                        continue
                    if item_signature not in seen_sigs or len(target) < max_sample_len:
                        target.append(copy.deepcopy(item))
                        seen_sigs.add(item_signature)
            else:
                # Entity list (e.g. universes, top_10, currency, themes):
                # Keep up to max_sample_len standard items, PLUS any new structural shape
                is_new_structural_shape = item_signature not in seen_sigs
                if is_new_structural_shape or len(target) < max_sample_len:
                    if item in target:
                        continue
                    new_item = copy.deepcopy(item)
                    if item_id:
                        target_by_id[item_id] = new_item
                    target.append(new_item)
                    seen_sigs.add(item_signature)

        return target

    if (target and isinstance(target[0], (str, int, float, bool))) or (
        source and isinstance(source[0], (str, int, float, bool))
    ):
        seen = set(target)
        for val in source:
            if val not in seen and len(target) < 10:
                seen.add(val)
                target.append(val)
        return target

    return target


def profile_endpoint(
    conn: duckdb.DuckDBPyConnection,
    dctx: zstd.ZstdDecompressor,
    url_prefix: str,
) -> tuple[dict[str, Any], int]:
    """Scan all blobs for an endpoint using cursor streaming and construct Frankenstein payload."""
    cursor = conn.execute(
        """
        SELECT b.payload
        FROM (SELECT DISTINCT hash FROM bronze.snapshots WHERE url_prefix = $1) AS s
        JOIN bronze.payload_blobs b ON s.hash = b.hash
        """,
        [url_prefix],
    )

    frankenstein: dict[str, Any] = {}
    total_blobs = 0
    sigs_by_list: dict[str, set[frozenset[str]]] = {}

    while True:
        batch = cursor.fetchmany(1000)
        if not batch:
            break

        for (blob,) in batch:
            total_blobs += 1
            raw = dctx.decompress(blob)
            payload = orjson.loads(raw)

            if not isinstance(payload, dict) or not payload:
                continue

            frankenstein = merge_dicts(frankenstein, payload, "", sigs_by_list)

    return frankenstein, total_blobs


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DB_PATH, read_only=True)
    dctx = zstd.ZstdDecompressor()

    for name, prefix in ENDPOINTS.items():
        print(f"Profiling {name} ({prefix})...")
        frankenstein, total_blobs = profile_endpoint(conn, dctx, prefix)

        # Write Frankenstein JSON to docs/samples/
        json_path = OUTPUT_DIR / f"{name}.json"
        with open(json_path, "wb") as f:
            f.write(orjson.dumps(frankenstein, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))

        print(f"  Wrote {json_path} (from {total_blobs} blobs)")

    # Clean up legacy data/samples if present
    if OLD_OUTPUT_DIR.exists():
        shutil.rmtree(OLD_OUTPUT_DIR)
        print(f"Removed legacy directory {OLD_OUTPUT_DIR}")


if __name__ == "__main__":
    main()
