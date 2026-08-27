import math
from datetime import UTC, datetime
from typing import Any

import duckdb

from etfportfolio.core.utils import content_address, decompress_payload

PRICE_PRESETS_DAYS = [
    ("1M", 30),
    ("3M", 90),
    ("6M", 180),
    ("1Y", 365),
    ("3Y", 1095),
    ("5Y", 1825),
    ("10Y", 3650),
]


def extract_series_date_range(endpoint_name: str, payload: dict[str, Any]) -> tuple[datetime, datetime] | None:
    """Extracts (first_date, last_date) as UTC datetimes from price or sentiment payloads."""
    timestamps: list[int] = []

    if endpoint_name == "price":
        series_list = payload.get("plot", {}).get("series", [])
        for s in series_list:
            for pt in s.get("plotData", []):
                if "x" in pt:
                    timestamps.append(pt["x"])
    elif endpoint_name == "sentiment":
        for entry in payload.get("sentiment", []):
            if "datetime" in entry:
                timestamps.append(entry["datetime"])

    if not timestamps:
        return None

    min_ts = min(timestamps)
    max_ts = max(timestamps)

    # Convert milliseconds to datetime
    first_dt = datetime.fromtimestamp(min_ts / 1000.0, tz=UTC).replace(tzinfo=None)
    last_dt = datetime.fromtimestamp(max_ts / 1000.0, tz=UTC).replace(tzinfo=None)
    return first_dt, last_dt


def extract_series_points(endpoint_name: str, payload: dict[str, Any]) -> dict[int, dict[str, Any]]:
    """Extracts timestamp -> values mapping for overlap validation."""
    points: dict[int, dict[str, Any]] = {}

    if endpoint_name == "price":
        series_list = payload.get("plot", {}).get("series", [])
        for s in series_list:
            for pt in s.get("plotData", []):
                if "x" in pt:
                    points[pt["x"]] = {
                        "close": pt.get("close"),
                        "open": pt.get("open"),
                        "high": pt.get("high"),
                        "low": pt.get("low"),
                        "y": pt.get("y"),
                    }
    elif endpoint_name == "sentiment":
        for entry in payload.get("sentiment", []):
            if "datetime" in entry:
                points[entry["datetime"]] = {
                    "sscore": entry.get("sscore"),
                    "svolatility": entry.get("svolatility"),
                    "sbuzz": entry.get("sbuzz"),
                }

    return points


def validate_overlap(endpoint_name: str, new_payload: dict[str, Any], prior_payload: dict[str, Any]) -> bool:
    """Validates that overlapping points between new and prior series payloads match."""
    new_pts = extract_series_points(endpoint_name, new_payload)
    prior_pts = extract_series_points(endpoint_name, prior_payload)

    overlap_keys = set(new_pts.keys()) & set(prior_pts.keys())
    if not overlap_keys:
        return True

    for ts in overlap_keys:
        p1 = new_pts[ts]
        p2 = prior_pts[ts]
        for k in p1:
            if k in p2:
                v1, v2 = p1[k], p2[k]
                if v1 is None and v2 is None:
                    continue
                if v1 is None or v2 is None:
                    return False
                if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                    if not math.isclose(float(v1), float(v2), rel_tol=1e-4, abs_tol=1e-4):
                        return False
                elif v1 != v2:
                    return False
    return True


def get_last_series_info(
    conn: duckdb.DuckDBPyConnection, product_id: int, url_prefix: str
) -> tuple[datetime | None, dict[str, Any] | None]:
    """Returns the most recent last_date and payload for a given (product_id, url_prefix)."""
    row = conn.execute(
        """
        SELECT s.last_date, pb.payload
        FROM bronze.series s
        JOIN bronze.payload_blobs pb ON s.hash = pb.hash
        WHERE s.product_id = $1 AND s.url_prefix = $2
        ORDER BY s.last_date DESC, s.fetched_at DESC
        LIMIT 1
        """,
        [product_id, url_prefix],
    ).fetchone()

    if not row:
        return None, None

    last_date, raw_blob = row[0], row[1]
    payload = decompress_payload(raw_blob)
    return last_date, payload


def determine_price_period(last_date: datetime | None, now: datetime | None = None) -> str:
    """Selects the smallest preset period covering the gap since last_date."""
    if last_date is None:
        return "MAX"

    ref_now = now or datetime.now(UTC).replace(tzinfo=None)
    gap_days = (ref_now - last_date).days

    for period, max_days in PRICE_PRESETS_DAYS:
        if gap_days <= max_days:
            return period
    return "MAX"


def determine_sentiment_dates(last_date: datetime | None, now: datetime | None = None) -> tuple[str, str]:
    """Returns (from_date, to_date) strings for incremental or full sentiment fetch."""
    ref_now = now or datetime.now(UTC).replace(tzinfo=None)
    to_date_str = ref_now.strftime("%Y-%m-%d")

    if last_date is None:
        return "2000-01-01", to_date_str

    from_date_str = last_date.strftime("%Y-%m-%d")
    return from_date_str, to_date_str


def store_series(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    url_prefix: str,
    url_slug: str,
    first_date: datetime,
    last_date: datetime,
    payload: Any,
    fetched_at: datetime | None = None,
) -> int:
    """Stores a series payload blob and writes a lineage row to bronze.series."""
    digest, compressed = content_address(payload)

    conn.execute(
        """
        INSERT INTO bronze.payload_blobs (hash, payload)
        VALUES ($1, $2)
        ON CONFLICT (hash) DO NOTHING
        """,
        [digest, compressed],
    )

    conn.execute(
        """
        INSERT INTO bronze.series (hash, product_id, url_prefix, url_slug, first_date, last_date, fetched_at)
        VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, CURRENT_TIMESTAMP))
        """,
        [digest, product_id, url_prefix, url_slug, first_date, last_date, fetched_at],
    )

    return digest
