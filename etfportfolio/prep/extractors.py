from __future__ import annotations

import re
from collections.abc import Callable
from datetime import datetime
from typing import Any

from etfportfolio.prep.utils import (
    DimensionTuple,
    ExtractionResult,
    clean_credit_rating,
    disambiguate_aum_currency,
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

_SKIP_RATING_TOKENS = frozenset(
    {
        "under_review",
        "under review",
        "not_applicable",
        "not applicable",
        "na",
        "n/a",
        "-",
        "",
    }
)

_LIPPER_HORIZONS = {
    "overall": "overall",
    "3_year": "3yr",
    "5_year": "5yr",
    "10_year": "10yr",
}

_LIPPER_TIEBREAK = ("United States", "Germany", "UK", "Canada", "Japan", "Australia")

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

# Ratios delivered as percentage points; convert to decimal fractions.
_RATIOS_PERCENTAGE_METRICS = frozenset(
    {
        "eps_growth_1yr",
        "eps_growth_3yr",
        "eps_growth_5yr",
        "sales_growth_1_year",
        "sales_growth_3_year",
        "sales_growth_5_yr",
        "sales_per_share_growth_1_year",
        "sales_per_share_growth_3_year",
        "operating_cash_flow_growth_rate_3yr",
        "return_on_assets_1yr",
        "return_on_assets_3yr",
        "return_on_equity_1yr",
        "return_on_equity_3yr",
        "return_on_investment_1yr",
        "return_on_investment_3yr",
        "return_on_capital",
        "return_on_capital_3yr",
        "dividend_yield_weighted_average",
        "dividendpayoutratio5yr",
        "dividend_per_share_1yr",
        "dividend_per_share_3yr",
        "yield_to_maturity",
        "average_coupon",
        "relative_strength",
    }
)

_COUNTRY_CODE_REMAPS: dict[str, str | None] = {
    "Croatia": "HR",
    "Bulgaria": "BG",
    "Guam": "GU",
    "Uzbekistan": "UZ",
    "Unidentified": None,
}

_INDUSTRY_NAME_REMAPS = {
    "Telecommunication Services-Discontinued eff 09/19/2020": "Communication Services",
}

_MATURITY_CODE_MAP = {
    "% Maturity Less than 1 Year": "mat_lt_1y",
    "% Maturity 1 to 3 Years": "mat_1_to_3y",
    "% Maturity 3 to 5 Years": "mat_3_to_5y",
    "% Maturity 5 to 10 Years": "mat_5_to_10y",
    "% Maturity 10 to 20 Years": "mat_10_to_20y",
    "% Maturity 20 to 30 Years": "mat_20_to_30y",
    "% Maturity Greater than 30 Years": "mat_gt_30y",
    "% Maturity Other": "mat_other",
}

_INT_RE = re.compile(r"\d{1,3}(?:,\d{3})+|\d+")


def _regex_int(raw: str) -> int:
    matches = _INT_RE.findall(raw)
    if not matches:
        return 0
    return max(int(m.replace(",", "")) for m in matches)


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
            if metric_id in _RATIOS_PERCENTAGE_METRICS:
                val_float = val_float / 100.0
            raw_value = str(item.get("value_fmt") if item.get("value_fmt") is not None else value)
            result.metrics.append(
                (
                    product_id,
                    "ratios",
                    metric_id,
                    eff_date,
                    eff_source,
                    snapshot_created_at,
                    val_float,
                    raw_value,
                    None,
                )
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
    product_currency: str | None = None,
    listing_exchange: str | None = None,
    country: str | None = None,
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
                    None,
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
                    None,
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
                        None,
                    )
                )
        elif name_tag == "Management_Approach" or name == "Management Approach":
            raw_str = str(val).strip()
            approach = raw_str.lower()
            if approach in _SKIP_RATING_TOKENS:
                continue
            if approach == "passive":
                approach_val = 1.0
            elif approach == "active":
                approach_val = 0.0
            else:
                raise ValueError(f"Unknown Management Approach: '{val}'")
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
                    None,
                )
            )
        elif name_tag == "Total_Net_Assets_Month_End" or name.startswith("Total Net Assets"):
            parsed_aum = parse_net_assets(val, fallback_date=snapshot_date)
            if parsed_aum is not None:
                aum_val, raw_str, aum_date, aum_source = parsed_aum
                currency = disambiguate_aum_currency(
                    raw_str,
                    product_currency=product_currency,
                    listing_exchange=listing_exchange,
                    country=country,
                )
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
                        currency,
                    )
                )
        elif name_tag == "Manager_Tenure" or name == "Manager Tenure":
            parsed_tenure = parse_manager_tenure(val, ref_date=snapshot_date)
            if parsed_tenure is not None:
                tenure_years, raw_str = parsed_tenure
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
                        None,
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
                                None,
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
                None,
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
                    None,
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
                        None,
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

    analyst_count = 0
    foundational_seen = False

    for pillar in payload.get("summary", []):
        pillar_id = pillar.get("id")
        if not pillar_id:
            raise ValueError("Missing 'id' in mstar summary item")

        pillar_key = pillar_id.strip().lower()
        if pillar_key in ("category", "category_index"):
            continue

        is_quant = bool(pillar.get("q") is True or pillar_key.startswith("q_"))
        base_metric = pillar_key[2:] if pillar_key.startswith("q_") else pillar_key
        if base_metric == "quantitative_rating":
            base_metric = "medalist_rating"

        if base_metric in ("people", "process", "parent"):
            foundational_seen = True
            if not is_quant:
                analyst_count += 1

        raw_val = pillar.get("value")
        if raw_val is None:
            continue

        val_str = str(raw_val).strip()
        if val_str in ("", "-"):
            continue

        norm_val = val_str.lower()
        if norm_val in _SKIP_RATING_TOKENS:
            continue

        if base_metric == "medalist_rating":
            metric_id = "mstar_medalist_rating"
            mapping = _MSTAR_MEDALIST_MAP
        elif base_metric in ("people", "process", "parent"):
            metric_id = f"mstar_{base_metric}_{'quant' if is_quant else 'analyst'}"
            mapping = _MSTAR_PILLAR_MAP
        elif base_metric == "morningstar_rating":
            metric_id = f"mstar_{base_metric}"
            mapping = _MSTAR_STAR_MAP
        elif base_metric == "sustainability_rating":
            metric_id = f"mstar_{base_metric}"
            mapping = _MSTAR_SUSTAINABILITY_MAP
        else:
            raise ValueError(f"Unrecognized mstar pillar id: '{pillar_id}'")

        if norm_val not in mapping:
            raise ValueError(f"Unrecognized rating string '{raw_val}' for pillar '{pillar_id}'")

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
            (
                product_id,
                "mstar",
                metric_id,
                eff_date,
                eff_source,
                snapshot_created_at,
                score,
                val_str,
                None,
            )
        )

    if foundational_seen:
        result.metrics.append(
            (
                product_id,
                "mstar",
                "mstar_analyst_coverage_pct",
                top_level_date,
                top_level_source,
                snapshot_created_at,
                analyst_count / 3.0,
                f"{analyst_count}/3 analyst pillars",
                None,
            )
        )

    return result


def _lipper_universe_peer_count(universe: dict[str, Any]) -> int:
    max_n = 0
    for horizon_key in _LIPPER_HORIZONS:
        for item in universe.get(horizon_key, []):
            rating = item.get("rating")
            if not isinstance(rating, dict):
                continue
            n = _regex_int(str(rating.get("name") or ""))
            if n > max_n:
                max_n = n
    return max_n


def _select_lipper_universe(universes: list[dict[str, Any]]) -> dict[str, Any]:
    if len(universes) == 1:
        return universes[0]

    def sort_key(u: dict[str, Any]) -> tuple[int, int]:
        name = u.get("name") or ""
        try:
            tie = _LIPPER_TIEBREAK.index(name)
        except ValueError:
            tie = len(_LIPPER_TIEBREAK)
        return (_lipper_universe_peer_count(u), -tie)

    return max(universes, key=sort_key)


def extract_lipper(
    product_id: int,
    payload: dict[str, Any],
    snapshot_created_at: datetime,
) -> ExtractionResult:
    result = ExtractionResult()
    universes = payload.get("universes", [])
    if not universes:
        return result

    universe = _select_lipper_universe(universes)
    universe_name = universe.get("name") or "global"
    universe_n = _lipper_universe_peer_count(universe)
    eff_date, eff_source = parse_effective_date(
        universe.get("as_of_date"),
        fallback_date=snapshot_created_at.date(),
        default_source="item",
    )

    for horizon_key, horizon_suffix in _LIPPER_HORIZONS.items():
        for item in universe.get(horizon_key, []):
            tag = item.get("name_tag")
            if not tag:
                continue
            rating = item.get("rating")
            if not isinstance(rating, dict):
                continue
            val = rating.get("value")
            if val is None:
                continue

            fund_count = _regex_int(str(rating.get("name") or "")) or universe_n
            metric_id = f"lipper_{sanitize_metric_id(tag)}_{horizon_suffix}"
            result.metrics.append(
                (
                    product_id,
                    "lipper",
                    metric_id,
                    eff_date,
                    eff_source,
                    snapshot_created_at,
                    float(val),
                    f"{val} ({universe_name}: {fund_count} funds)",
                    None,
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
                    None,
                )
            )

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
            dim_name = raw_name.strip()

            if dim_type == "credit_rating":
                dim_name = clean_credit_rating(raw_name)
                dim_code: str | None = dim_name
            elif dim_type == "country":
                if dim_name in _COUNTRY_CODE_REMAPS:
                    dim_code = _COUNTRY_CODE_REMAPS[dim_name]
                else:
                    dim_code = item.get("country_code")
            elif dim_type == "industry":
                dim_name = _INDUSTRY_NAME_REMAPS.get(dim_name, dim_name)
                dim_code = None
            elif dim_type == "maturity":
                dim_code = _MATURITY_CODE_MAP.get(dim_name)
            elif dim_type == "debt_type":
                dim_code = item.get("code")
            else:
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

    if result.dimensions:
        coalesced: dict[tuple[int, str, str, object], DimensionTuple] = {}
        for dim in result.dimensions:
            coalesced[(dim[0], dim[1], dim[2], dim[4])] = dim
        result.dimensions = list(coalesced.values())

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

    coverage = payload.get("coverage")
    if coverage is not None:
        result.metrics.append(
            (
                product_id,
                "theme_weights",
                "theme_coverage",
                eff_date,
                "snapshot",
                snapshot_created_at,
                float(coverage),
                str(coverage),
                None,
            )
        )

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
    "ratios": extract_ratios,
    "profile": extract_profile,
    "esg": extract_esg,
    "mstar": extract_mstar,
    "lipper": extract_lipper,
    "holdings": extract_holdings,
    "theme_weights": extract_theme_weights,
}
