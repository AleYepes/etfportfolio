from collections import defaultdict
from collections.abc import Callable
from datetime import datetime
from typing import Any

from etfportfolio.observations.utils import (
    clean_credit_rating,
    parse_effective_date,
    sanitize_metric_id,
)

# Ordinal mappings for Morningstar ratings and pillars
_MSTAR_MEDALIST_MAP = {
    "gold": 5.0,
    "silver": 4.0,
    "bronze": 3.0,
    "neutral": 2.0,
    "negative": 1.0,
}

_MSTAR_PILLAR_MAP = {
    "high": 5.0,
    "above_average": 4.0,
    "average": 3.0,
    "below_average": 2.0,
    "low": 1.0,
}

_MSTAR_STAR_MAP = {
    "1": 1.0,
    "2": 2.0,
    "3": 3.0,
    "4": 4.0,
    "5": 5.0,
}

_MSTAR_SUSTAINABILITY_MAP = {
    "1": 1.0,
    "2": 2.0,
    "3": 3.0,
    "4": 4.0,
    "5": 5.0,
    "high": 5.0,
    "above_average": 4.0,
    "average": 3.0,
    "below_average": 2.0,
    "low": 1.0,
}

_MSTAR_PILLAR_TARGETS = {
    "medalist_rating": _MSTAR_MEDALIST_MAP,
    "quantitative_rating": _MSTAR_MEDALIST_MAP,
    "people": _MSTAR_PILLAR_MAP,
    "process": _MSTAR_PILLAR_MAP,
    "parent": _MSTAR_PILLAR_MAP,
    "q_people": _MSTAR_PILLAR_MAP,
    "q_process": _MSTAR_PILLAR_MAP,
    "q_parent": _MSTAR_PILLAR_MAP,
    "morningstar_rating": _MSTAR_STAR_MAP,
    "sustainability_rating": _MSTAR_SUSTAINABILITY_MAP,
}

_LIPPER_HORIZON_SUFFIXES = {
    "overall": "_overall",
    "3_year": "_3yr",
    "5_year": "_5yr",
    "10_year": "_10yr",
}

_HOLDINGS_BREAKDOWN_MAPPING = {
    "allocation_self": "asset_class",
    "currency": "currency",
    "investor_country": "country",
    "industry": "industry",
    "maturity": "maturity",
    "debtor": "credit_rating",
}


def extract_ratios(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> list[tuple[int, str, str, Any, datetime, float]]:
    """Extracts valuation multiples, growth rates, profitability, fixed income, and factor Z-scores."""
    if not payload:
        return []

    effective_date = parse_effective_date(
        payload.get("as_of_date"),
        fallback_date=snapshot_created_at.date(),
    )

    rows: list[tuple[int, str, str, Any, datetime, float]] = []
    for section in ("dividend", "financials", "fixed_income", "ratios", "zscore"):
        items = payload.get(section, [])
        for item in items:
            value = item.get("value")
            if value is None:
                continue

            tag = item.get("name_tag")
            if not tag:
                continue

            metric_id = sanitize_metric_id(tag)
            val_float = float(value)
            rows.append((product_id, "ratios", metric_id, effective_date, snapshot_created_at, val_float))

    return rows


def _extract_mstar_style(mstar: dict[str, Any]) -> list[tuple[str, float]]:
    """Extracts normalized style coordinates for size and value."""
    selected = mstar.get("selected")
    if not selected or len(selected) == 0 or len(selected[0]) < 2:
        return []

    name = (mstar.get("name") or "").lower()
    name_tokens = name.replace("-", " ").replace("/", " ").split()

    # Determine size score
    # Large=3.0, Mid=2.0, Small=1.0, Multi=0.0
    size_val: float | None = None
    if "large" in name_tokens:
        size_val = 3.0
    elif "mid" in name_tokens:
        size_val = 2.0
    elif "small" in name_tokens:
        size_val = 1.0
    elif "multi" in name_tokens:
        size_val = 0.0

    # Determine value score
    # Value=1.0, Core=2.0, Growth=3.0
    val_score: float | None = None
    if "growth" in name_tokens:
        val_score = 3.0
    elif "core" in name_tokens or "blend" in name_tokens:
        val_score = 2.0
    elif "value" in name_tokens:
        val_score = 1.0

    y_coord, x_coord = selected[0][0], selected[0][1]

    # Fallback to coordinate mapping if not resolved from name
    if size_val is None:
        y_axis = [s.lower() for s in mstar.get("y_axis", [])]
        if 0 <= y_coord < len(y_axis):
            cat = y_axis[y_coord]
            if "large" in cat:
                size_val = 3.0
            elif "mid" in cat:
                size_val = 2.0
            elif "small" in cat:
                size_val = 1.0
            elif "multi" in cat:
                size_val = 0.0
        if size_val is None:
            size_val = float(y_coord)

    if val_score is None:
        x_axis = [s.lower() for s in mstar.get("x_axis", [])]
        if 0 <= x_coord < len(x_axis):
            cat = x_axis[x_coord]
            if "growth" in cat:
                val_score = 3.0
            elif "core" in cat:
                val_score = 2.0
            elif "value" in cat:
                val_score = 1.0
        if val_score is None:
            val_score = float(x_coord)

    return [
        ("mstar_style_size", size_val),
        ("mstar_style_value", val_score),
    ]


def extract_profile(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> list[tuple[int, str, str, Any, datetime, float]]:
    """Extracts management/non-management expense ratios, TER, passive/active, and style box."""
    if not payload:
        return []

    effective_date = snapshot_created_at.date()
    rows: list[tuple[int, str, str, Any, datetime, float]] = []

    # 1. expenses_allocation
    for item in payload.get("expenses_allocation", []):
        ratio = item.get("ratio")
        if ratio is None:
            continue
        name = item.get("name")
        if name == "Management Expenses":
            rows.append(
                (product_id, "profile", "management_expense_ratio", effective_date, snapshot_created_at, float(ratio))
            )
        elif name == "Non-Management Expenses":
            rows.append(
                (
                    product_id,
                    "profile",
                    "non_management_expense_ratio",
                    effective_date,
                    snapshot_created_at,
                    float(ratio),
                )
            )

    # 2. fund_and_profile
    for item in payload.get("fund_and_profile", []):
        name_tag = item.get("name_tag") or ""
        name = item.get("name") or ""
        val = item.get("value")
        if val is None:
            continue

        if name_tag == "Total_Expense_Ratio" or name == "Total Expense Ratio":
            clean_str = str(val).replace("%", "").strip()
            if clean_str:
                ter_val = float(clean_str) / 100.0
                rows.append(
                    (product_id, "profile", "total_expense_ratio", effective_date, snapshot_created_at, ter_val)
                )
        elif name_tag == "Management_Approach" or name == "Management Approach":
            approach = str(val).strip().lower()
            if approach == "passive":
                rows.append((product_id, "profile", "is_passive", effective_date, snapshot_created_at, 1.0))
            elif approach == "active":
                rows.append((product_id, "profile", "is_passive", effective_date, snapshot_created_at, 0.0))

    # 3. mstar (Style Box)
    mstar = payload.get("mstar")
    if isinstance(mstar, dict):
        for metric_id, style_val in _extract_mstar_style(mstar):
            rows.append((product_id, "profile", metric_id, effective_date, snapshot_created_at, style_val))

    return rows


def extract_esg(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> list[tuple[int, str, str, Any, datetime, float]]:
    """Extracts ESG portfolio coverage and hierarchical Refinitiv score tree."""
    if not payload:
        return []

    effective_date = parse_effective_date(
        payload.get("asOfDate"),
        fallback_date=snapshot_created_at.date(),
    )

    rows: list[tuple[int, str, str, Any, datetime, float]] = []

    coverage = payload.get("coverage")
    if coverage is not None:
        rows.append((product_id, "esg", "esg_coverage", effective_date, snapshot_created_at, float(coverage)))

    for node in payload.get("content", []):
        node_name = node.get("name")
        node_val = node.get("value")
        if node_name and node_val is not None:
            metric_id = sanitize_metric_id(node_name)
            rows.append((product_id, "esg", metric_id, effective_date, snapshot_created_at, float(node_val)))

        for child in node.get("children", []):
            child_name = child.get("name")
            child_val = child.get("value")
            if child_name and child_val is not None:
                metric_id = sanitize_metric_id(child_name)
                rows.append((product_id, "esg", metric_id, effective_date, snapshot_created_at, float(child_val)))

    return rows


def extract_mstar(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> list[tuple[int, str, str, Any, datetime, float]]:
    """Extracts Medalist ratings, pillar scores, star ratings, and sustainability globe ratings."""
    if not payload:
        return []

    top_level_date = parse_effective_date(
        payload.get("as_of_date"),
        fallback_date=snapshot_created_at.date(),
    )

    rows: list[tuple[int, str, str, Any, datetime, float]] = []

    for pillar in payload.get("summary", []):
        pillar_id = pillar.get("id")
        if not pillar_id:
            raise ValueError("Missing 'id' in mstar summary item")

        pillar_key = pillar_id.strip().lower()
        if pillar_key not in _MSTAR_PILLAR_TARGETS:
            # Skip non-rating summary items like 'category' and 'category_index'
            continue

        raw_val = pillar.get("value")
        if raw_val is None:
            continue

        val_str = str(raw_val).strip()
        if val_str in ("", "-"):
            continue

        # Non-numeric rating statuses representing unrated/suspended are skipped
        norm_val = val_str.lower()
        if norm_val in ("under_review", "not_applicable", "not applicable", "under review", "na", "n/a"):
            continue

        mapping = _MSTAR_PILLAR_TARGETS[pillar_key]
        if norm_val not in mapping:
            raise ValueError(f"Unrecognized rating string '{raw_val}' for mstar pillar '{pillar_id}'")

        mapped_score = mapping[norm_val]
        effective_date = parse_effective_date(
            pillar.get("publish_date"),
            fallback_date=top_level_date,
        )

        rows.append((product_id, "mstar", pillar_key, effective_date, snapshot_created_at, mapped_score))

    return rows


def extract_lipper(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> list[tuple[int, str, str, Any, datetime, float]]:
    """Groups universes by date, averages same-date scores, and extracts ratings across horizons."""
    universes = payload.get("universes", [])
    if not universes:
        return []

    # Group universes by raw as_of_date
    date_groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for u in universes:
        date_groups[u.get("as_of_date")].append(u)

    rows: list[tuple[int, str, str, Any, datetime, float]] = []

    for as_of_date, group_universes in date_groups.items():
        effective_date = parse_effective_date(
            as_of_date,
            fallback_date=snapshot_created_at.date(),
        )

        metrics_acc: dict[str, list[float]] = defaultdict(list)
        for horizon, suffix in _LIPPER_HORIZON_SUFFIXES.items():
            for u in group_universes:
                for item in u.get(horizon, []):
                    tag = item.get("name_tag")
                    if not tag:
                        continue
                    rating = item.get("rating")
                    if not isinstance(rating, dict):
                        continue
                    val = rating.get("value")
                    if val is None:
                        continue
                    metric_id = f"{tag.strip().lower()}{suffix}"
                    metrics_acc[metric_id].append(float(val))

        for metric_id, values in metrics_acc.items():
            consensus_val = sum(values) / len(values)
            rows.append((product_id, "lipper", metric_id, effective_date, snapshot_created_at, consensus_val))

    return rows


def extract_holdings(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> list[tuple[int, str, str, str | None, Any, datetime, float]]:
    """Extracts portfolio allocations across asset class, currency, country, industry, maturity, credit rating."""
    if not payload:
        return []

    effective_date = parse_effective_date(
        payload.get("as_of_date"),
        fallback_date=snapshot_created_at.date(),
    )

    rows: list[tuple[int, str, str, str | None, Any, datetime, float]] = []

    for field, breakdown_type in _HOLDINGS_BREAKDOWN_MAPPING.items():
        items = payload.get(field, [])
        if not items:
            continue

        agg: dict[tuple[str, str], dict[str, Any]] = {}
        for item in items:
            raw_name = item.get("name")
            if not raw_name:
                continue

            weight_val = item.get("weight")
            if weight_val is None:
                continue

            # Standardized decimal weight (e.g. 99.76% -> 0.9976)
            weight = float(weight_val) / 100.0

            if breakdown_type == "credit_rating":
                item_name = clean_credit_rating(raw_name)
                item_code = item_name
            elif breakdown_type == "currency":
                s = raw_name.strip()
                item_name = "Unassigned" if s in ("<No Currency>", "<NoCurrency>") else s
                item_code = item.get("code")
            elif breakdown_type == "country":
                item_name = raw_name.strip()
                item_code = item.get("country_code")
            else:
                item_name = raw_name.strip()
                item_code = None

            key = (breakdown_type, item_name)
            if key in agg:
                agg[key]["weight"] += weight
            else:
                agg[key] = {"item_code": item_code, "weight": weight}

        for (b_type, name), data in agg.items():
            rows.append(
                (
                    product_id,
                    b_type,
                    name,
                    data["item_code"],
                    effective_date,
                    snapshot_created_at,
                    data["weight"],
                )
            )

    return rows


def extract_theme_weights(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> list[tuple[int, str, Any, datetime, float, float]]:
    """Extracts thematic factor exposures and rank-adjusted weights."""
    if not payload:
        return []

    effective_date = snapshot_created_at.date()
    rows: list[tuple[int, str, Any, datetime, float, float]] = []

    for theme in payload.get("themes", []):
        theme_id = theme.get("key")
        if not theme_id:
            continue

        weight = float(theme["weight"])
        rank_adj_weight = float(theme["rank_adjusted_weight"])
        rows.append((product_id, str(theme_id), effective_date, snapshot_created_at, weight, rank_adj_weight))

    return rows


EXTRACTOR_REGISTRY: dict[str, Callable] = {
    "/tws.proxy/fundamentals/mf_ratios_fundamentals/": extract_ratios,
    "/tws.proxy/fundamentals/mf_profile_and_fees/": extract_profile,
    "/tws.proxy/impact/esg/": extract_esg,
    "/tws.proxy/mstar/fund/detail?conid=": extract_mstar,
    "/tws.proxy/fundamentals/mf_lip_ratings/": extract_lipper,
    "/tws.proxy/fundamentals/mf_holdings/": extract_holdings,
    "/tws.proxy/knowledge-graph/ui/fund?conid=": extract_theme_weights,
}
