import re
from datetime import UTC, date, datetime
from typing import Any


def parse_effective_date(val: Any, fallback_date: date) -> date:
    """Parses date from epoch ms (int/float), YYYYMMDD, YYYY-MM-DD, or YYYY/MM/DD string.

    Returns fallback_date if value is None, <= 0, unparseable, or invalid.
    """
    if val is None:
        return fallback_date

    if isinstance(val, (int, float)):
        if val <= 0:
            return fallback_date
        try:
            return datetime.fromtimestamp(val / 1000.0, tz=UTC).date()
        except ValueError, OverflowError, OSError:
            return fallback_date

    if isinstance(val, str):
        val_str = val.strip()
        if not val_str:
            return fallback_date

        # Try YYYYMMDD
        if len(val_str) == 8 and val_str.isdigit():
            try:
                return datetime.strptime(val_str, "%Y%m%d").date()
            except ValueError:
                return fallback_date

        # Try YYYY-MM-DD
        if "-" in val_str:
            try:
                return datetime.strptime(val_str[:10], "%Y-%m-%d").date()
            except ValueError:
                pass

        # Try YYYY/MM/DD
        if "/" in val_str:
            try:
                return datetime.strptime(val_str[:10], "%Y/%m/%d").date()
            except ValueError:
                pass

    return fallback_date


def clean_credit_rating(raw_name: str) -> str:
    """Strips leading % Quality prefixes and maps credit rating strings."""
    s = raw_name.strip()
    if s.startswith("% Quality/"):
        s = s[len("% Quality/") :].strip()
    elif s.startswith("% Quality "):
        s = s[len("% Quality ") :].strip()
    return s


def sanitize_metric_id(tag: str) -> str:
    """Converts a raw metric tag to lower_snake_case."""
    return re.sub(r"[^a-z0-9]+", "_", tag.strip().lower()).strip("_")
