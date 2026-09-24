from __future__ import annotations

import contextlib
import math
import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import orjson
import zstandard as zstd

_DECOMPRESSOR = zstd.ZstdDecompressor()


def decompress_payload(compressed: bytes) -> Any:
    canonical = _DECOMPRESSOR.decompress(compressed)
    return orjson.loads(canonical)


@dataclass(frozen=True, slots=True)
class Observation:
    product_id: int
    family: str
    metric: str
    code: str | None
    effective_date: date
    date_source_depth: int
    fetched_at: datetime
    value: float
    raw_value: str

    def to_row(self) -> tuple[int, str, str, str | None, date, int, datetime, float, str]:
        return (
            self.product_id,
            self.family,
            self.metric,
            self.code,
            self.effective_date,
            self.date_source_depth,
            self.fetched_at,
            self.value,
            self.raw_value,
        )


def to_lower_snake_case(s: str) -> str:
    cleaned = s.replace("&", "and")
    return re.sub(r"[^a-z0-9]+", "_", cleaned.lower()).strip("_")


@dataclass(frozen=True)
class DateScope:
    effective_date: date
    depth: int


class DateContext:
    def __init__(self, snapshot_date: date):
        self._stack: list[DateScope] = [DateScope(snapshot_date, 0)]

    @property
    def current(self) -> DateScope:
        return self._stack[-1]

    @contextmanager
    def scope(self, raw_date: Any, depth: int) -> Iterator[DateScope]:
        parsed = self.parse_date(raw_date)
        if parsed is not None and parsed <= self._stack[0].effective_date:
            self._stack.append(DateScope(parsed, depth))
        else:
            self._stack.append(self.current)
        try:
            yield self.current
        finally:
            self._stack.pop()

    @staticmethod
    def parse_date(val: Any) -> date | None:
        if val is None:
            return None

        if isinstance(val, (int, float)):
            if val <= 0:
                return None
            try:
                return datetime.fromtimestamp(val / 1000.0, tz=UTC).date()
            except ValueError, OverflowError, OSError:
                return None

        if isinstance(val, str):
            val_str = val.strip()
            if not val_str:
                return None

            if val_str.isdigit() and len(val_str) > 8:
                try:
                    return datetime.fromtimestamp(int(val_str) / 1000.0, tz=UTC).date()
                except ValueError, OverflowError, OSError:
                    return None

            if len(val_str) == 8 and val_str.isdigit():
                with contextlib.suppress(ValueError):
                    return datetime.strptime(val_str, "%Y%m%d").date()

            if "-" in val_str:
                with contextlib.suppress(ValueError):
                    return datetime.strptime(val_str[:10], "%Y-%m-%d").date()

            if "/" in val_str:
                with contextlib.suppress(ValueError):
                    return datetime.strptime(val_str[:10], "%Y/%m/%d").date()

        return None


def clean_simplex(
    raw_items: list[dict[str, Any]],
    name_extractor: Callable[[dict[str, Any]], str | None],
    weight_extractor: Callable[[dict[str, Any]], float | None],
    metric_map: dict[str, bool] | None,
    code_extractor: Callable[[dict[str, Any]], str | None] | None = None,
    open_vocab: bool = False,
    residual_names: set[str] | None = None,
) -> list[tuple[str, str | None, float, str]]:
    parsed: list[tuple[str, str | None, float, str]] = []
    for item in raw_items:
        raw_name = name_extractor(item)
        raw_w = weight_extractor(item)
        if raw_name is None or raw_w is None:
            continue

        metric = to_lower_snake_case(raw_name)
        code = code_extractor(item) if code_extractor else None
        raw = str(item.get("formatted_weight", raw_w))

        if not open_vocab and metric_map is not None and metric not in metric_map:
            raise ValueError(f"Unrecognized metric '{metric}' in simplex")

        clipped_w = max(0.0, float(raw_w))
        parsed.append((metric, code, clipped_w, raw))

    total = sum(p[2] for p in parsed)
    if total <= 0:
        return []

    survivors: list[tuple[str, str | None, float, str]] = []
    for metric, code, clipped_w, raw in parsed:
        norm_val = clipped_w / total
        is_residual = (metric_map is not None and metric_map.get(metric) is True) or (
            residual_names is not None and metric in residual_names
        )
        if not is_residual:
            survivors.append((metric, code, norm_val, raw))

    return survivors


SYMBOL_TO_CURRENCIES: dict[str, set[str]] = {
    "$": {"USD", "CAD", "AUD", "MXN", "SGD", "HKD", "NZD", "TWD"},
    "€": {"EUR"},
    "£": {"GBP", "EGP", "LBP"},
    "¥": {"JPY", "CNY", "CNH"},
    "₩": {"KRW", "KPW"},
    "₹": {"INR"},
}


def disambiguate_aum_currency(
    raw_value: str,
    contract_currency: str | None,
    known_currencies: set[str],
) -> str | None:
    s = raw_value.strip()
    if not s:
        return None
    m = re.match(r"^([A-Za-z]{3})(?![A-Za-z])", s)
    if m:
        token = m.group(1).upper()
        return token if token in known_currencies else None
    if s[0] in SYMBOL_TO_CURRENCIES:
        prod = (contract_currency or "").strip().upper()
        return prod if prod in SYMBOL_TO_CURRENCIES[s[0]] else None
    prod = (contract_currency or "").strip().upper()
    if prod in known_currencies:
        return prod
    return None


_DATE_REGEX = re.compile(r"(\d{4}[-/]?\d{2}[-/]?\d{2}|\d{8})")
_AUM_REGEX = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*([kKmMbBtT]?)")
_AUM_MULTIPLIERS = {
    "k": 1e3,
    "m": 1e6,
    "b": 1e9,
    "t": 1e12,
}


def parse_net_assets(
    raw_val: Any,
) -> tuple[float, str, str | None] | None:
    if raw_val is None:
        return None

    raw_str = str(raw_val).strip()
    if not raw_str or raw_str.lower() in ("-", "n/a", "none"):
        return None

    date_match = _DATE_REGEX.search(raw_str)
    if date_match:
        embedded_date = date_match.group(1)
        amt_str = raw_str[: date_match.start()] + raw_str[date_match.end() :]
    else:
        embedded_date = None
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

    return final_val, raw_str, embedded_date


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
        if math.isnan(pct_float) or math.isinf(pct_float):
            return None
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
