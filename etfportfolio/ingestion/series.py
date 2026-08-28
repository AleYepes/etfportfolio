import logging
import math
from datetime import UTC, datetime
from typing import Any

import duckdb
import httpx

from etfportfolio.core import db
from etfportfolio.core.utils import content_address, decompress_payload
from etfportfolio.ingestion import endpoints, session

logger = logging.getLogger(__name__)

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

    db.store_blob(conn, digest, compressed)

    conn.execute(
        """
        INSERT INTO bronze.series (hash, product_id, url_prefix, url_slug, first_date, last_date, fetched_at)
        VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, CURRENT_TIMESTAMP))
        """,
        [digest, product_id, url_prefix, url_slug, first_date, last_date, fetched_at],
    )

    return digest


# --- Per-endpoint request-parameter builders --------------------------------
# price and sentiment share identical fetch/overlap-check/refetch/store
# orchestration (fetch_incremental, below) but take fundamentally different
# query parameters — price wants a relative period ("1M".."MAX"), sentiment
# wants an explicit date range. Isolating just that difference here keeps the
# orchestration itself unified in one function instead of two parallel copies.


def _incremental_params(endpoint_name: str, last_date: datetime | None) -> dict[str, Any]:
    if endpoint_name == "price":
        return {"period": determine_price_period(last_date)}
    if endpoint_name == "sentiment":
        from_date, to_date = determine_sentiment_dates(last_date)
        return {"from_date": from_date, "to_date": to_date}
    raise ValueError(f"No series parameter builder for endpoint '{endpoint_name}'.")


def _full_refetch_params(endpoint_name: str) -> dict[str, Any]:
    if endpoint_name == "price":
        return {"period": "MAX"}
    if endpoint_name == "sentiment":
        _, to_date = determine_sentiment_dates(None)
        return {"from_date": "2000-01-01", "to_date": to_date}
    raise ValueError(f"No series parameter builder for endpoint '{endpoint_name}'.")


async def fetch_incremental(
    client: httpx.AsyncClient,
    conn: duckdb.DuckDBPyConnection,
    ep: endpoints.Endpoint,
    product_id: int,
) -> None:
    """Fetches a series-shaped endpoint incrementally and stores the result.

    Shared by both 'price' and 'sentiment': fetches just the gap since the
    last stored point, validates that any overlapping points still agree with
    what's already stored, and — if they don't — discards the incremental
    fetch and refetches the full range instead.
    """
    last_date, prior_payload = get_last_series_info(conn, product_id, ep.url_prefix)

    params = _incremental_params(ep.name, last_date)
    url_prefix, url_slug, full_url = ep.resolve(product_id=product_id, **params)
    _, payload = await session.fetch_with_retry(client, full_url)

    date_range = extract_series_date_range(ep.name, payload)
    if not date_range:
        logger.warning("Empty %s series returned for product %d", ep.name, product_id)
        return

    first_date, last_fetched_date = date_range

    if last_date is not None and prior_payload is not None and not validate_overlap(ep.name, payload, prior_payload):
        logger.warning(
            "%s series overlap mismatch for product %d. Discarding incremental fetch, refetching full range...",
            ep.name,
            product_id,
        )
        refetch_params = _full_refetch_params(ep.name)
        url_prefix, url_slug, refetch_url = ep.resolve(product_id=product_id, **refetch_params)
        _, refetch_payload = await session.fetch_with_retry(client, refetch_url)

        refetch_range = extract_series_date_range(ep.name, refetch_payload)
        if refetch_range:
            r_first, r_last = refetch_range
            store_series(conn, product_id, url_prefix, url_slug, r_first, r_last, refetch_payload)
        return

    store_series(conn, product_id, url_prefix, url_slug, first_date, last_fetched_date, payload)
