from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from etfportfolio.observations.utils import (
    DimensionTuple,
    ExtractionResult,
    clean_credit_rating,
    parse_effective_date,
    parse_manager_tenure,
    parse_net_assets,
    parse_percentage,
    sanitize_metric_id,
)

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

_LIPPER_HORIZONS = {
    "overall": "overall",
    "3_year": "3yr",
    "5_year": "5yr",
    "10_year": "10yr",
}

_HOLDINGS_BREAKDOWNS = (
    ("allocation_self", "asset_class"),
    ("investor_country", "country"),
    ("industry", "industry"),
    ("debtor", "credit_rating"),
    ("debt_type", "debt_type"),
    ("maturity", "maturity"),
)

_VALID_STYLE_X_TAGS = {"value", "core", "growth"}
_VALID_STYLE_Y_TAGS = {"large", "multi", "mid", "small"}


def extract_ratios(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> ExtractionResult:
    result = ExtractionResult()
    if not payload:
        return result

    eff_date, eff_source = parse_effective_date(
        payload.get("as_of_date"),
        fallback_date=snapshot_created_at.date(),
        default_source="payload",
    )

    for section in ("dividend", "financials", "fixed_income", "ratios", "zscore"):
        for item in payload.get(section, []):
            value = item.get("value")
            if value is None:
                continue

            tag = item.get("name_tag")
            if not tag:
                continue

            metric_id = sanitize_metric_id(tag)
            val_float = float(value)
            raw_value = str(item.get("value_fmt") if item.get("value_fmt") is not None else value)
            result.metrics.append(
                (product_id, "ratios", metric_id, eff_date, eff_source, snapshot_created_at, val_float, raw_value)
            )

    return result


def _extract_style_box_dimensions(
    product_id: int,
    mstar: dict[str, Any],
    snapshot_created_at: datetime,
) -> list[DimensionTuple]:
    selected = mstar.get("selected") or []
    hist = mstar.get("hist") or []
    if not selected and not hist:
        return []

    x_tags = [str(t).strip().lower() for t in (mstar.get("x_axis_tag") or mstar.get("x_axis") or [])]
    y_tags = [str(t).strip().lower() for t in (mstar.get("y_axis_tag") or mstar.get("y_axis") or [])]

    if not x_tags or not y_tags:
        raise ValueError(f"Missing style box axis tags: {mstar}")

    if any(t not in _VALID_STYLE_X_TAGS for t in x_tags) or any(t not in _VALID_STYLE_Y_TAGS for t in y_tags):
        raise ValueError(f"Unrecognized style box axis configuration: {mstar}")

    snapshot_date = snapshot_created_at.date()
    dimensions: list[DimensionTuple] = []

    for dim_type, coords in (("style_box", selected), ("style_box_hist", hist)):
        for coord in coords:
            if not isinstance(coord, (list, tuple)) or len(coord) < 2:
                raise ValueError(f"Invalid coordinate format in style box: {coord}")
            x_idx, y_idx = coord[0], coord[1]
            if not (0 <= x_idx < len(x_tags)) or not (0 <= y_idx < len(y_tags)):
                raise ValueError(
                    f"Style box coordinate out of bounds: [{x_idx}, {y_idx}] for axes x={x_tags}, y={y_tags}"
                )

            x_tag = x_tags[x_idx]
            y_tag = y_tags[y_idx]
            dim_name = f"{y_tag.title()} {x_tag.title()}"
            dim_code = f"{y_tag}_{x_tag}"
            dimensions.append(
                (
                    product_id,
                    dim_type,
                    dim_name,
                    dim_code,
                    snapshot_date,
                    "snapshot",
                    snapshot_created_at,
                    1.0,
                    str(coord),
                )
            )

    return dimensions


def extract_profile(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> ExtractionResult:
    result = ExtractionResult()
    if not payload:
        return result

    snapshot_date = snapshot_created_at.date()

    for item in payload.get("expenses_allocation", []):
        ratio = item.get("ratio")
        if ratio is None:
            continue
        name = item.get("name")
        raw_val = str(item.get("value", ratio))
        if name == "Management Expenses":
            result.metrics.append(
                (
                    product_id,
                    "profile",
                    "management_expense_ratio",
                    snapshot_date,
                    "snapshot",
                    snapshot_created_at,
                    float(ratio),
                    raw_val,
                )
            )
        elif name == "Non-Management Expenses":
            result.metrics.append(
                (
                    product_id,
                    "profile",
                    "non_management_expense_ratio",
                    snapshot_date,
                    "snapshot",
                    snapshot_created_at,
                    float(ratio),
                    raw_val,
                )
            )

    for item in payload.get("fund_and_profile", []):
        name_tag = item.get("name_tag") or ""
        name = item.get("name") or ""
        val = item.get("value")
        if val is None:
            continue

        if name_tag == "Total_Expense_Ratio" or name == "Total Expense Ratio":
            parsed = parse_percentage(val)
            if parsed is not None:
                ter_val, raw_str = parsed
                result.metrics.append(
                    (
                        product_id,
                        "profile",
                        "total_expense_ratio",
                        snapshot_date,
                        "snapshot",
                        snapshot_created_at,
                        ter_val,
                        raw_str,
                    )
                )
        elif name_tag == "Management_Approach" or name == "Management Approach":
            raw_str = str(val).strip()
            approach = raw_str.lower()
            if approach == "passive":
                approach_val = 1.0
            elif approach == "active":
                approach_val = 0.0
            else:
                raise ValueError(f"Unrecognized management approach: '{val}'")
            result.metrics.append(
                (
                    product_id,
                    "profile",
                    "is_passive",
                    snapshot_date,
                    "snapshot",
                    snapshot_created_at,
                    approach_val,
                    raw_str,
                )
            )
        elif name_tag == "Total_Net_Assets_Month_End" or name.startswith("Total Net Assets"):
            aum_val, raw_str, aum_date, aum_source = parse_net_assets(val, fallback_date=snapshot_date)
            result.metrics.append(
                (
                    product_id,
                    "profile",
                    "total_net_assets_local",
                    aum_date,
                    aum_source,
                    snapshot_created_at,
                    aum_val,
                    raw_str,
                )
            )
        elif name_tag == "Manager_Tenure" or name == "Manager Tenure":
            raw_str = str(val).strip()
            if raw_str and raw_str.lower() not in ("-", "n/a", "none"):
                tenure_years, raw_str = parse_manager_tenure(val, ref_date=snapshot_date)
                result.metrics.append(
                    (
                        product_id,
                        "profile",
                        "manager_tenure_years",
                        snapshot_date,
                        "snapshot",
                        snapshot_created_at,
                        tenure_years,
                        raw_str,
                    )
                )

    for report in payload.get("reports", []):
        if report.get("name") == "Annual Report":
            report_date, report_source = parse_effective_date(
                report.get("as_of_date"),
                fallback_date=snapshot_date,
                default_source="item",
            )
            for report_field in report.get("fields", []):
                if report_field.get("name") == "Total Net Expense":
                    parsed = parse_percentage(report_field.get("value"))
                    if parsed is not None:
                        fee_val, raw_str = parsed
                        result.metrics.append(
                            (
                                product_id,
                                "profile",
                                "audited_net_expense_ratio",
                                report_date,
                                report_source,
                                snapshot_created_at,
                                fee_val,
                                raw_str,
                            )
                        )

    mstar = payload.get("mstar")
    if isinstance(mstar, dict):
        result.dimensions.extend(_extract_style_box_dimensions(product_id, mstar, snapshot_created_at))

    return result


def extract_esg(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> ExtractionResult:
    result = ExtractionResult()
    if not payload:
        return result

    eff_date, eff_source = parse_effective_date(
        payload.get("asOfDate"),
        fallback_date=snapshot_created_at.date(),
        default_source="payload",
    )

    coverage = payload.get("coverage")
    if coverage is not None:
        result.metrics.append(
            (
                product_id,
                "esg",
                "esg_coverage",
                eff_date,
                eff_source,
                snapshot_created_at,
                float(coverage),
                str(coverage),
            )
        )

    for node in payload.get("content", []):
        node_name = node.get("name")
        node_val = node.get("value")
        if node_name and node_val is not None:
            metric_id = sanitize_metric_id(node_name)
            result.metrics.append(
                (
                    product_id,
                    "esg",
                    metric_id,
                    eff_date,
                    eff_source,
                    snapshot_created_at,
                    float(node_val),
                    str(node_val),
                )
            )

        for child in node.get("children", []):
            child_name = child.get("name")
            child_val = child.get("value")
            if child_name and child_val is not None:
                metric_id = sanitize_metric_id(child_name)
                result.metrics.append(
                    (
                        product_id,
                        "esg",
                        metric_id,
                        eff_date,
                        eff_source,
                        snapshot_created_at,
                        float(child_val),
                        str(child_val),
                    )
                )

    return result


def extract_mstar(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> ExtractionResult:
    result = ExtractionResult()
    if not payload:
        return result

    top_level_date, top_level_source = parse_effective_date(
        payload.get("as_of_date"),
        fallback_date=snapshot_created_at.date(),
        default_source="payload",
    )

    for pillar in payload.get("summary", []):
        pillar_id = pillar.get("id")
        if not pillar_id:
            raise ValueError("Missing 'id' in mstar summary item")

        pillar_key = pillar_id.strip().lower()
        if pillar_key in ("category", "category_index"):
            continue

        raw_val = pillar.get("value")
        if raw_val is None:
            continue

        val_str = str(raw_val).strip()
        if val_str in ("", "-"):
            continue

        norm_val = val_str.lower()
        if norm_val in ("under_review", "not_applicable", "not applicable", "under review", "na", "n/a"):
            continue

        is_quant = bool(pillar.get("q") is True or pillar_key.startswith("q_"))
        base_metric = pillar_key[2:] if pillar_key.startswith("q_") else pillar_key
        if base_metric == "quantitative_rating":
            base_metric = "medalist_rating"

        if base_metric in ("medalist_rating", "people", "process", "parent"):
            metric_id = f"mstar_{base_metric}_{'quant' if is_quant else 'analyst'}"
            mapping = _MSTAR_MEDALIST_MAP if base_metric == "medalist_rating" else _MSTAR_PILLAR_MAP
        elif base_metric == "morningstar_rating":
            metric_id = f"mstar_{base_metric}"
            mapping = _MSTAR_STAR_MAP
        elif base_metric == "sustainability_rating":
            metric_id = f"mstar_{base_metric}"
            mapping = _MSTAR_SUSTAINABILITY_MAP
        else:
            raise ValueError(f"Unrecognized mstar pillar id: '{pillar_id}'")

        if norm_val not in mapping:
            raise ValueError(f"Unrecognized rating string '{raw_val}' for mstar pillar '{pillar_id}'")

        score = mapping[norm_val]
        if pillar.get("publish_date"):
            eff_date, eff_source = parse_effective_date(
                pillar.get("publish_date"),
                fallback_date=top_level_date,
                default_source="item",
            )
        else:
            eff_date, eff_source = top_level_date, top_level_source

        result.metrics.append(
            (product_id, "mstar", metric_id, eff_date, eff_source, snapshot_created_at, score, val_str)
        )

    return result


def extract_lipper(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> ExtractionResult:
    result = ExtractionResult()
    universes = payload.get("universes", [])
    if not universes:
        return result

    for u in universes:
        country_name = sanitize_metric_id(u.get("name") or "global")
        eff_date, eff_source = parse_effective_date(
            u.get("as_of_date"),
            fallback_date=snapshot_created_at.date(),
            default_source="item",
        )

        for horizon_key, horizon_suffix in _LIPPER_HORIZONS.items():
            for item in u.get(horizon_key, []):
                tag = item.get("name_tag")
                if not tag:
                    continue
                rating = item.get("rating")
                if not isinstance(rating, dict):
                    continue
                val = rating.get("value")
                if val is None:
                    continue

                metric_id = f"lipper_{sanitize_metric_id(tag)}_{horizon_suffix}_{country_name}"
                result.metrics.append(
                    (
                        product_id,
                        "lipper",
                        metric_id,
                        eff_date,
                        eff_source,
                        snapshot_created_at,
                        float(val),
                        str(val),
                    )
                )

    return result


def extract_holdings(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> ExtractionResult:
    result = ExtractionResult()
    if not payload:
        return result

    eff_date, eff_source = parse_effective_date(
        payload.get("as_of_date"),
        fallback_date=snapshot_created_at.date(),
        default_source="payload",
    )

    top_10_weight_raw = payload.get("top_10_weight")
    if top_10_weight_raw is not None:
        parsed = parse_percentage(top_10_weight_raw)
        if parsed is not None:
            conc_val, raw_str = parsed
            result.metrics.append(
                (
                    product_id,
                    "holdings",
                    "portfolio_top_10_concentration",
                    eff_date,
                    eff_source,
                    snapshot_created_at,
                    conc_val,
                    raw_str,
                )
            )

    # Exclude currency and geographic to eliminate collinearity with investor_country
    for field_name, dim_type in _HOLDINGS_BREAKDOWNS:
        for item in payload.get(field_name, []):
            raw_name = item.get("name")
            if not raw_name:
                continue

            weight_val = item.get("weight")
            if weight_val is None:
                continue

            weight = float(weight_val) / 100.0
            raw_str = str(item.get("formatted_weight", f"{weight_val}%"))

            if dim_type == "credit_rating":
                dim_name = clean_credit_rating(raw_name)
                dim_code = dim_name
            elif dim_type == "country":
                dim_name = raw_name.strip()
                dim_code = item.get("country_code")
            else:
                dim_name = raw_name.strip()
                dim_code = None

            result.dimensions.append(
                (
                    product_id,
                    dim_type,
                    dim_name,
                    dim_code,
                    eff_date,
                    eff_source,
                    snapshot_created_at,
                    weight,
                    raw_str,
                )
            )

    for item in payload.get("top_10", []):
        name = item.get("name")
        if not name:
            continue

        ticker = item.get("ticker")
        dim_name = f"{ticker.strip()} - {name.strip()}" if ticker and str(ticker).strip() else name.strip()
        conids = item.get("conids", [])
        dim_code = ",".join(str(c) for c in conids) if conids else None

        assets_pct = item.get("assets_pct")
        parsed = parse_percentage(assets_pct, allow_bound=True)
        if parsed is not None:
            pct_val, raw_str = parsed
            result.dimensions.append(
                (
                    product_id,
                    "top_holding",
                    dim_name,
                    dim_code,
                    eff_date,
                    eff_source,
                    snapshot_created_at,
                    pct_val,
                    raw_str,
                )
            )

    return result


def extract_theme_weights(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> ExtractionResult:
    result = ExtractionResult()
    if not payload:
        return result

    eff_date = snapshot_created_at.date()

    # Raw weight is discarded to prevent multicollinearity with rank_adjusted_weight
    for theme in payload.get("themes", []):
        theme_id = theme.get("key")
        name = theme.get("name")
        if not theme_id or not name:
            continue

        rank_adj_weight = theme.get("rank_adjusted_weight")
        if rank_adj_weight is None:
            continue

        val_float = float(rank_adj_weight)
        result.dimensions.append(
            (
                product_id,
                "theme",
                name.strip(),
                str(theme_id).strip(),
                eff_date,
                "snapshot",
                snapshot_created_at,
                val_float,
                str(rank_adj_weight),
            )
        )

    return result


EXTRACTOR_REGISTRY: dict[str, Callable[[int, dict[str, Any], datetime], ExtractionResult]] = {
    "/tws.proxy/fundamentals/mf_ratios_fundamentals/": extract_ratios,
    "/tws.proxy/fundamentals/mf_profile_and_fees/": extract_profile,
    "/tws.proxy/impact/esg/": extract_esg,
    "/tws.proxy/mstar/fund/detail?conid=": extract_mstar,
    "/tws.proxy/fundamentals/mf_lip_ratings/": extract_lipper,
    "/tws.proxy/fundamentals/mf_holdings/": extract_holdings,
    "/tws.proxy/knowledge-graph/ui/fund?conid=": extract_theme_weights,
}
