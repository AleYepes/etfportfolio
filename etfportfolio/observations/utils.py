from __future__ import annotations

import contextlib
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

# Field order matches silver.product_metrics in schema.sql
MetricTuple = tuple[int, str, str, date, str, datetime, float, str]

# Field order matches silver.product_dimensions in schema.sql
DimensionTuple = tuple[int, str, str, str | None, date, str, datetime, float, str]


@dataclass(slots=True)
class ExtractionResult:
    metrics: list[MetricTuple] = field(default_factory=list)
    dimensions: list[DimensionTuple] = field(default_factory=list)


_DATE_REGEX = re.compile(r"(\d{4}[-/]\d{2}[-/]\d{2}|\d{8})")
_AUM_REGEX = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*([kKmMbBtT]?)")
_AUM_MULTIPLIERS = {
    "k": 1e3,
    "m": 1e6,
    "b": 1e9,
    "t": 1e12,
}


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


def parse_net_assets(
    raw_val: Any,
    fallback_date: date,
) -> tuple[float, str, date, str]:
    raw_str = str(raw_val).strip()
    if not raw_str or raw_str.lower() in ("-", "n/a", "none"):
        raise ValueError(f"Invalid net assets value: '{raw_val}'")

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
        raise ValueError(f"Cannot parse net assets amount from '{raw_val}'")

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
        raise ValueError(f"Cannot parse numeric AUM from '{raw_val}'")

    base_val = float(m.group(1))
    suffix = m.group(2).lower()
    multiplier = _AUM_MULTIPLIERS.get(suffix, 1.0)
    final_val = base_val * multiplier

    return final_val, raw_str, eff_date, eff_source


def parse_manager_tenure(
    raw_val: Any,
    ref_date: date,
) -> tuple[float, str]:
    raw_str = str(raw_val).strip()
    if not raw_str or raw_str.lower() in ("-", "n/a", "none"):
        raise ValueError(f"Empty manager tenure value: '{raw_val}'")

    start_date: date | None = None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"):
        with contextlib.suppress(ValueError):
            start_date = datetime.strptime(raw_str[:10], fmt).date()
            break

    if start_date is None and len(raw_str) == 4 and raw_str.isdigit():
        with contextlib.suppress(ValueError):
            start_date = date(int(raw_str), 1, 1)

    if start_date is None:
        raise ValueError(f"Unparseable manager tenure start date: '{raw_val}'")

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
