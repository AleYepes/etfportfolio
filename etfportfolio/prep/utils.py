from __future__ import annotations

import contextlib
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Literal

import orjson
import zstandard as zstd

_DECOMPRESSOR = zstd.ZstdDecompressor()


def decompress_payload(compressed: bytes) -> Any:
    canonical = _DECOMPRESSOR.decompress(compressed)
    return orjson.loads(canonical)


EffectiveDateSource = Literal["payload", "item", "snapshot"]

# Field order matches silver.product_metrics (post-currency migration).
MetricRow = tuple[int, str, str, date, str, datetime, float, str, str | None]
MetricTuple = MetricRow

# Field order matches silver.product_dimensions in schema.sql
DimensionRow = tuple[int, str, str, str | None, date, str, datetime, float, str]
DimensionTuple = DimensionRow


@dataclass(slots=True)
class ExtractionResult:
    metrics: list[MetricRow] = field(default_factory=list)
    dimensions: list[DimensionRow] = field(default_factory=list)


_DATE_REGEX = re.compile(r"(\d{4}[-/]?\d{2}[-/]?\d{2}|\d{8})")
_AUM_REGEX = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*([kKmMbBtT]?)")
_AUM_MULTIPLIERS = {
    "k": 1e3,
    "m": 1e6,
    "b": 1e9,
    "t": 1e12,
}

_AUM_SYMBOL_TO_ISO = {
    "€": "EUR",
    "£": "GBP",
    "¥": "JPY",
    "₹": "INR",
}

_AUM_ISO_PREFIXES = frozenset(
    {
        "CAD",
        "AUD",
        "CNY",
        "TWD",
        "HKD",
        "CHF",
        "BRL",
        "SGD",
        "MXN",
        "KRW",
        "MYR",
        "CNH",
        "AED",
        "SEK",
        "ZAR",
        "ILS",
        "SAR",
        "NOK",
        "HUF",
        "DKK",
        "VND",
    }
)

_CAD_EXCHANGES = frozenset({"TSE", "TSX"})


def parse_effective_date(
    val: Any,
    fallback_date: date,
    default_source: str = "payload",
) -> tuple[date, str]:
    if val is None:
        return fallback_date, "snapshot"

    if isinstance(val, (int, float)):
        if val <= 0:
            return fallback_date, "snapshot"
        try:
            return datetime.fromtimestamp(val / 1000.0, tz=UTC).date(), default_source
        except ValueError, OverflowError, OSError:
            return fallback_date, "snapshot"

    if isinstance(val, str):
        val_str = val.strip()
        if not val_str:
            return fallback_date, "snapshot"

        if val_str.isdigit() and len(val_str) > 8:
            try:
                return datetime.fromtimestamp(int(val_str) / 1000.0, tz=UTC).date(), default_source
            except ValueError, OverflowError, OSError:
                return fallback_date, "snapshot"

        if len(val_str) == 8 and val_str.isdigit():
            with contextlib.suppress(ValueError):
                return datetime.strptime(val_str, "%Y%m%d").date(), default_source

        if "-" in val_str:
            with contextlib.suppress(ValueError):
                return datetime.strptime(val_str[:10], "%Y-%m-%d").date(), default_source

        if "/" in val_str:
            with contextlib.suppress(ValueError):
                return datetime.strptime(val_str[:10], "%Y/%m/%d").date(), default_source

    return fallback_date, "snapshot"


def disambiguate_aum_currency(
    raw_value: str,
    product_currency: str | None = None,
    listing_exchange: str | None = None,
    country: str | None = None,
) -> str:
    """Map an AUM display string to an ISO-4217 code (§2.5)."""
    s = str(raw_value).lstrip()
    if s:
        first = s[0]
        if first in _AUM_SYMBOL_TO_ISO:
            return _AUM_SYMBOL_TO_ISO[first]

        iso_match = re.match(r"^([A-Za-z]{3})", s)
        if iso_match:
            token = iso_match.group(1).upper()
            if token in _AUM_ISO_PREFIXES:
                return token

        if s.startswith("$"):
            prod = (product_currency or "").strip().upper()
            if prod == "CAD":
                return "CAD"
            exch = (listing_exchange or "").strip().upper()
            if exch in _CAD_EXCHANGES or "CANADIAN" in exch:
                return "CAD"
            ctry = (country or "").strip().lower()
            if ctry in {"canada", "ca"}:
                return "CAD"
            return "USD"

    prod = (product_currency or "").strip().upper()
    if len(prod) == 3:
        return prod
    return "USD"


def parse_net_assets(
    raw_val: Any,
    fallback_date: date,
) -> tuple[float, str, date, str] | None:
    if raw_val is None:
        return None

    raw_str = str(raw_val).strip()
    if not raw_str or raw_str.lower() in ("-", "n/a", "none"):
        return None

    date_match = _DATE_REGEX.search(raw_str)
    if date_match:
        eff_date, _ = parse_effective_date(
            date_match.group(1),
            fallback_date=fallback_date,
            default_source="item",
        )
        eff_source = "item"
        amt_str = raw_str[: date_match.start()] + raw_str[date_match.end() :]
    else:
        eff_date = fallback_date
        eff_source = "snapshot"
        amt_str = raw_str

    amt_clean = re.sub(r"[^\d.,kKmMbBtT]", "", amt_str)
    if not amt_clean:
        return None

    if "," in amt_clean and "." in amt_clean:
        first_comma = amt_clean.find(",")
        first_dot = amt_clean.find(".")
        if first_comma < first_dot:
            amt_clean = amt_clean.replace(",", "")
        else:
            amt_clean = amt_clean.replace(".", "").replace(",", ".")
    elif "," in amt_clean:
        parts = amt_clean.split(",")
        last_digits = re.sub(r"[^\d]", "", parts[-1])
        if len(last_digits) == 3 and len(parts) > 1 and len(parts[0]) <= 3:
            amt_clean = amt_clean.replace(",", "")
        else:
            amt_clean = amt_clean.replace(",", ".")

    m = _AUM_REGEX.search(amt_clean)
    if not m or not m.group(1):
        return None

    try:
        base_val = float(m.group(1))
    except ValueError:
        return None

    suffix = m.group(2).lower()
    multiplier = _AUM_MULTIPLIERS.get(suffix, 1.0)
    final_val = base_val * multiplier

    return final_val, raw_str, eff_date, eff_source


def parse_manager_tenure(
    raw_val: Any,
    ref_date: date,
) -> tuple[float, str] | None:
    if raw_val is None:
        return None

    raw_str = str(raw_val).strip()
    if not raw_str or raw_str.lower() in ("-", "n/a", "none"):
        return None

    start_date: date | None = None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y"):
        with contextlib.suppress(ValueError):
            if fmt == "%Y":
                if len(raw_str) == 4 and raw_str.isdigit():
                    start_date = date(int(raw_str), 1, 1)
                    break
                continue
            start_date = datetime.strptime(raw_str[:10], fmt).date()
            break

    if start_date is None:
        raise ValueError(f"Invalid Manager Tenure date string: '{raw_val}'")

    delta_days = (ref_date - start_date).days
    years = round(max(0.0, delta_days / 365.25), 4)
    return years, raw_str


def parse_percentage(val: Any, allow_bound: bool = False) -> tuple[float, str] | None:
    if val is None:
        return None

    raw_str = str(val).strip()
    if not raw_str or raw_str.lower() in ("-", "n/a", "none"):
        return None

    clean_str = raw_str.replace("%", "").strip()
    if allow_bound:
        clean_str = clean_str.lstrip("<>~ ").strip()

    try:
        pct_float = float(clean_str) / 100.0
        return pct_float, raw_str
    except ValueError as e:
        raise ValueError(f"Cannot parse percentage from '{val}': {e}") from e


def clean_credit_rating(raw_name: str) -> str:
    s = raw_name.strip()
    if s.startswith("% Quality/"):
        return s[len("% Quality/") :].strip()
    if s.startswith("% Quality "):
        return s[len("% Quality ") :].strip()
    if s.startswith("% Quality-"):
        return s[len("% Quality-") :].strip()
    return s


def sanitize_metric_id(tag: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", tag.strip().lower()).strip("_")
