from __future__ import annotations

import logging
import math
import re
from collections.abc import Callable
from datetime import datetime
from typing import Any

from etfportfolio.core.endpoints import (
    ALL_RATIOS_METRICS,
    ANNUAL_REPORT_METRICS,
    ASSET_CLASS_METRICS,
    COUNTRY_CODE_REMAPS,
    CREDIT_RATING_METRICS,
    ESG_METRICS,
    FUND_PROFILE_REDEMPTION_METRICS,
    INDUSTRY_METRICS,
    INDUSTRY_NAME_REMAPS,
    LIPPER_HORIZONS,
    LIPPER_TIEBREAK,
    MATURITY_METRICS,
    MSTAR_MEDALIST_MAP,
    MSTAR_PILLAR_MAP,
    MSTAR_STAR_MAP,
    MSTAR_SUSTAINABILITY_MAP,
    PROSPECTUS_REPORT_METRICS,
    RATIOS_PERCENTAGE_METRICS,
    SKIP_RATING_TOKENS,
)
from etfportfolio.prep.utils import (
    DateContext,
    Observation,
    clean_credit_rating,
    clean_simplex,
    disambiguate_aum_currency,
    parse_manager_tenure,
    parse_net_assets,
    parse_percentage,
    to_lower_snake_case,
)

_INT_RE = re.compile(r"\d{1,3}(?:,\d{3})+|\d+")
_VALID_STYLE_X_TAGS = {"value", "core", "growth"}
_VALID_STYLE_Y_TAGS = {"large", "multi", "mid", "small"}

logger = logging.getLogger(__name__)


def _regex_int(raw: str) -> int:
    matches = _INT_RE.findall(raw)
    if not matches:
        return 0
    return max(int(m.replace(",", "")) for m in matches)


def _obs(
    product_id: int,
    family: str,
    metric: str,
    value: float,
    raw_value: str,
    date_ctx: DateContext,
    fetched_at: datetime,
    code: str | None = None,
) -> Observation | None:
    if math.isnan(value) or math.isinf(value):
        return None
    scope = date_ctx.current
    return Observation(
        product_id=product_id,
        family=family,
        metric=metric,
        code=code,
        effective_date=scope.effective_date,
        date_source_depth=scope.depth,
        fetched_at=fetched_at,
        value=value,
        raw_value=raw_value,
    )


def extract_holdings(
    product_id: int,
    payload: dict[str, Any],
    fetched_at: datetime,
    date_ctx: DateContext | None = None,
    contract_currency: str | None = None,
    known_currencies: set[str] | None = None,
) -> list[Observation]:
    if not payload or not isinstance(payload, dict):
        return []

    if date_ctx is None:
        date_ctx = DateContext(fetched_at.date())

    observations: list[Observation] = []

    with date_ctx.scope(payload.get("as_of_date"), 1):
        top_10_weight_raw = payload.get("top_10_weight")
        if top_10_weight_raw is not None:
            parsed = parse_percentage(top_10_weight_raw)
            if parsed is not None:
                conc_val, raw_str = parsed
                obs = _obs(product_id, "profile", "top_10_weight", conc_val, raw_str, date_ctx, fetched_at)
                if obs:
                    observations.append(obs)

        # Asset class sleeve
        ac_survivors = clean_simplex(
            payload.get("allocation_self", []),
            name_extractor=lambda x: x.get("name"),
            weight_extractor=lambda x: float(x["weight"]) / 100.0 if x.get("weight") is not None else None,
            metric_map=ASSET_CLASS_METRICS,
        )
        for metric, code, val, raw in ac_survivors:
            obs = _obs(product_id, "asset_class", metric, val, raw, date_ctx, fetched_at, code=code)
            if obs:
                observations.append(obs)

        # Country sleeve
        def _country_code(item: dict[str, Any]) -> str | None:
            raw_name = item.get("name") or ""
            name_strip = raw_name.strip()
            if name_strip in COUNTRY_CODE_REMAPS:
                return COUNTRY_CODE_REMAPS[name_strip]
            return item.get("country_code")

        country_survivors = clean_simplex(
            payload.get("investor_country", []),
            name_extractor=lambda x: x.get("name"),
            weight_extractor=lambda x: float(x["weight"]) / 100.0 if x.get("weight") is not None else None,
            metric_map=None,
            code_extractor=_country_code,
            open_vocab=True,
            residual_names={"unidentified"},
        )
        for metric, code, val, raw in country_survivors:
            obs = _obs(product_id, "country", metric, val, raw, date_ctx, fetched_at, code=code)
            if obs:
                observations.append(obs)

        # Industry sleeve
        def _industry_name(item: dict[str, Any]) -> str | None:
            raw_name = item.get("name")
            if not raw_name:
                return None
            return INDUSTRY_NAME_REMAPS.get(raw_name, raw_name)

        ind_survivors = clean_simplex(
            payload.get("industry", []),
            name_extractor=_industry_name,
            weight_extractor=lambda x: float(x["weight"]) / 100.0 if x.get("weight") is not None else None,
            metric_map=INDUSTRY_METRICS,
        )
        for metric, code, val, raw in ind_survivors:
            obs = _obs(product_id, "industry", metric, val, raw, date_ctx, fetched_at, code=code)
            if obs:
                observations.append(obs)

        # Credit rating sleeve
        def _credit_name(item: dict[str, Any]) -> str | None:
            raw_name = item.get("name")
            if not raw_name:
                return None
            return clean_credit_rating(raw_name)

        cr_survivors = clean_simplex(
            payload.get("debtor", []),
            name_extractor=_credit_name,
            weight_extractor=lambda x: float(x["weight"]) / 100.0 if x.get("weight") is not None else None,
            metric_map=CREDIT_RATING_METRICS,
        )
        for metric, code, val, raw in cr_survivors:
            obs = _obs(product_id, "credit_rating", metric, val, raw, date_ctx, fetched_at, code=code)
            if obs:
                observations.append(obs)

        # Maturity sleeve
        mat_survivors = clean_simplex(
            payload.get("maturity", []),
            name_extractor=lambda x: x.get("name"),
            weight_extractor=lambda x: float(x["weight"]) / 100.0 if x.get("weight") is not None else None,
            metric_map=MATURITY_METRICS,
        )
        for metric, code, val, raw in mat_survivors:
            obs = _obs(product_id, "maturity", metric, val, raw, date_ctx, fetched_at, code=code)
            if obs:
                observations.append(obs)

    return observations


def _extract_style_box_dimensions(
    product_id: int,
    mstar: dict[str, Any],
    fetched_at: datetime,
    date_ctx: DateContext,
) -> list[Observation]:
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

    dimensions: list[Observation] = []

    for fam, coords in (("style_box", selected), ("style_box_hist", hist)):
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
            metric = f"{y_tag}_{x_tag}"
            obs = _obs(
                product_id,
                fam,
                metric,
                1.0,
                str(coord),
                date_ctx,
                fetched_at,
                code=None,
            )
            if obs:
                dimensions.append(obs)

    return dimensions


def _extract_profile_expenses_allocation(
    observations: list[Observation],
    product_id: int,
    expenses_allocation: list[dict[str, Any]],
    fetched_at: datetime,
    date_ctx: DateContext,
) -> None:
    for item in expenses_allocation:
        ratio = item.get("ratio")
        if ratio is None:
            continue
        name = item.get("name")
        raw_val = str(item.get("value", ratio))
        if name == "Management Expenses":
            obs = _obs(
                product_id,
                "profile",
                "management_expense_ratio",
                float(ratio),
                raw_val,
                date_ctx,
                fetched_at,
            )
            if obs:
                observations.append(obs)
        elif name == "Non-Management Expenses":
            obs = _obs(
                product_id,
                "profile",
                "non_management_expense_ratio",
                float(ratio),
                raw_val,
                date_ctx,
                fetched_at,
            )
            if obs:
                observations.append(obs)


def _extract_profile_fund_tags(
    observations: list[Observation],
    product_id: int,
    fund_and_profile: list[dict[str, Any]],
    fetched_at: datetime,
    date_ctx: DateContext,
    contract_currency: str | None = None,
    known_currencies: set[str] | None = None,
) -> None:
    for item in fund_and_profile:
        name_tag = item.get("name_tag") or ""
        name = item.get("name") or ""
        val = item.get("value")
        if val is None:
            continue

        if name_tag in ("Inception_Date", "Maturity_Date") or name in ("Inception Date", "Maturity Date"):
            continue

        redemption_metric = FUND_PROFILE_REDEMPTION_METRICS.get(name_tag) or FUND_PROFILE_REDEMPTION_METRICS.get(name)
        if redemption_metric is not None:
            parsed = parse_percentage(val)
            if parsed is not None:
                fee_val, raw_str = parsed
                obs = _obs(
                    product_id,
                    "profile",
                    redemption_metric,
                    fee_val,
                    raw_str,
                    date_ctx,
                    fetched_at,
                )
                if obs:
                    observations.append(obs)
            continue

        if name_tag == "Total_Expense_Ratio" or name == "Total Expense Ratio":
            parsed = parse_percentage(val)
            if parsed is not None:
                ter_val, raw_str = parsed
                obs = _obs(product_id, "profile", "total_expense_ratio", ter_val, raw_str, date_ctx, fetched_at)
                if obs:
                    observations.append(obs)
        elif name_tag == "Management_Approach" or name == "Management Approach":
            raw_str = str(val).strip()
            approach = raw_str.lower()
            if approach in SKIP_RATING_TOKENS:
                continue
            if approach == "passive":
                approach_val = 1.0
            elif approach == "active":
                approach_val = 0.0
            else:
                raise ValueError(f"Unknown Management Approach: '{val}'")
            obs = _obs(product_id, "profile", "is_passive", approach_val, raw_str, date_ctx, fetched_at)
            if obs:
                observations.append(obs)
        elif name_tag == "Total_Net_Assets_Month_End" or name.startswith("Total Net Assets"):
            parsed_aum = parse_net_assets(val)
            if parsed_aum is not None:
                aum_val, raw_str, aum_date_str = parsed_aum
                code = disambiguate_aum_currency(raw_str, contract_currency, known_currencies or set())
                if code is None:
                    logger.debug(
                        "Product %d: Dropped total_net_assets_local due to unresolved currency (raw='%s', contract_currency='%s')",
                        product_id,
                        raw_str,
                        contract_currency,
                    )
                else:
                    with date_ctx.scope(aum_date_str, 3):
                        obs = _obs(
                            product_id,
                            "profile",
                            "total_net_assets_local",
                            aum_val,
                            raw_str,
                            date_ctx,
                            fetched_at,
                            code=code,
                        )
                        if obs:
                            observations.append(obs)
        elif name_tag == "Manager_Tenure" or name == "Manager Tenure":
            parsed_tenure = parse_manager_tenure(val, ref_date=date_ctx.current.effective_date)
            if parsed_tenure is not None:
                tenure_years, raw_str = parsed_tenure
                obs = _obs(
                    product_id,
                    "profile",
                    "manager_tenure_years",
                    tenure_years,
                    raw_str,
                    date_ctx,
                    fetched_at,
                )
                if obs:
                    observations.append(obs)


def _extract_profile_reports(
    observations: list[Observation],
    product_id: int,
    reports: list[dict[str, Any]],
    fetched_at: datetime,
    date_ctx: DateContext,
) -> None:
    for report in reports:
        report_name = report.get("name")
        if report_name == "Annual Report":
            active_map = ANNUAL_REPORT_METRICS
        elif report_name == "Prospectus Report":
            active_map = PROSPECTUS_REPORT_METRICS
        else:
            continue

        with date_ctx.scope(report.get("as_of_date"), 2):
            for field in report.get("fields", []):
                field_name = field.get("name")
                if field_name in active_map:
                    metric = active_map[field_name]
                    parsed = parse_percentage(field.get("value"))
                    if parsed is not None:
                        val_float, raw_str = parsed
                        obs = _obs(
                            product_id,
                            "profile",
                            metric,
                            val_float,
                            raw_str,
                            date_ctx,
                            fetched_at,
                        )
                        if obs:
                            observations.append(obs)


def extract_profile(
    product_id: int,
    payload: dict[str, Any],
    fetched_at: datetime,
    date_ctx: DateContext | None = None,
    contract_currency: str | None = None,
    known_currencies: set[str] | None = None,
) -> list[Observation]:
    if not payload or not isinstance(payload, dict):
        return []

    if date_ctx is None:
        date_ctx = DateContext(fetched_at.date())

    observations: list[Observation] = []

    # 1. Expenses Allocation (depth 0)
    _extract_profile_expenses_allocation(
        observations, product_id, payload.get("expenses_allocation", []), fetched_at, date_ctx
    )

    # 2. Fund and Profile Tags (depth 0, except AUM at depth 3)
    _extract_profile_fund_tags(
        observations,
        product_id,
        payload.get("fund_and_profile", []),
        fetched_at,
        date_ctx,
        contract_currency,
        known_currencies,
    )

    # 3. Reports (depth 2)
    _extract_profile_reports(observations, product_id, payload.get("reports", []), fetched_at, date_ctx)

    # 4. Morningstar Style Box Dimensions
    mstar = payload.get("mstar")
    if isinstance(mstar, dict):
        observations.extend(_extract_style_box_dimensions(product_id, mstar, fetched_at, date_ctx))

    return observations


def extract_ratios(
    product_id: int,
    payload: dict[str, Any],
    fetched_at: datetime,
    date_ctx: DateContext | None = None,
    contract_currency: str | None = None,
    known_currencies: set[str] | None = None,
) -> list[Observation]:
    if not payload or not isinstance(payload, dict):
        return []

    if date_ctx is None:
        date_ctx = DateContext(fetched_at.date())

    observations: list[Observation] = []

    with date_ctx.scope(payload.get("as_of_date"), 1):
        for section in ("dividend", "financials", "fixed_income", "ratios", "zscore"):
            for item in payload.get(section, []):
                val = item.get("value")
                if val is None:
                    continue

                tag = item.get("name_tag")
                if not tag:
                    continue

                metric = to_lower_snake_case(tag)
                if metric not in ALL_RATIOS_METRICS:
                    raise ValueError(f"Unrecognized ratio metric '{metric}' (tag '{tag}')")

                raw_val = str(item.get("value_fmt") if item.get("value_fmt") is not None else val)
                if metric == "average_quality" and raw_val == "-":
                    continue

                val_float = float(val)
                if metric in RATIOS_PERCENTAGE_METRICS:
                    val_float = val_float / 100.0

                obs = _obs(product_id, "ratios", metric, val_float, raw_val, date_ctx, fetched_at)
                if obs:
                    observations.append(obs)

    return observations


def extract_esg(
    product_id: int,
    payload: dict[str, Any],
    fetched_at: datetime,
    date_ctx: DateContext | None = None,
    contract_currency: str | None = None,
    known_currencies: set[str] | None = None,
) -> list[Observation]:
    if not payload or not isinstance(payload, dict):
        return []

    if date_ctx is None:
        date_ctx = DateContext(fetched_at.date())

    observations: list[Observation] = []

    with date_ctx.scope(payload.get("asOfDate"), 1):
        coverage = payload.get("coverage")
        if coverage is not None:
            obs = _obs(
                product_id,
                "profile",
                "esg_coverage",
                float(coverage),
                str(coverage),
                date_ctx,
                fetched_at,
            )
            if obs:
                observations.append(obs)

        def walk(nodes: list[dict[str, Any]]) -> None:
            for node in nodes:
                node_name = node.get("name")
                node_val = node.get("value")
                if node_name and node_val is not None:
                    metric = to_lower_snake_case(node_name)
                    if metric not in ESG_METRICS:
                        raise ValueError(f"Unrecognized ESG metric '{metric}' (name '{node_name}')")
                    obs = _obs(
                        product_id,
                        "esg",
                        metric,
                        float(node_val),
                        str(node_val),
                        date_ctx,
                        fetched_at,
                    )
                    if obs:
                        observations.append(obs)

                children = node.get("children")
                if children and isinstance(children, list):
                    walk(children)

        walk(payload.get("content", []))

    return observations


def extract_mstar(
    product_id: int,
    payload: dict[str, Any],
    fetched_at: datetime,
    date_ctx: DateContext | None = None,
    contract_currency: str | None = None,
    known_currencies: set[str] | None = None,
) -> list[Observation]:
    if not payload or not isinstance(payload, dict):
        return []

    if date_ctx is None:
        date_ctx = DateContext(fetched_at.date())

    observations: list[Observation] = []

    with date_ctx.scope(payload.get("as_of_date"), 1):
        summary = payload.get("summary", [])

        # Count pillar coverage
        k = 0
        for pillar in summary:
            pillar_id = pillar.get("id")
            if not pillar_id:
                raise ValueError("Missing 'id' in mstar summary item")
            pillar_key = pillar_id.strip().lower()
            if pillar_key in ("category", "category_index"):
                continue
            base_metric = pillar_key[2:] if pillar_key.startswith("q_") else pillar_key
            if base_metric == "quantitative_rating":
                base_metric = "medalist_rating"
            if base_metric in ("people", "process", "parent"):
                k += 1

        if k > 0:
            obs = _obs(
                product_id,
                "profile",
                "mstar_coverage",
                k / 3.0,
                f"{k}/3 pillars",
                date_ctx,
                fetched_at,
            )
            if obs:
                observations.append(obs)

        # Emit scores
        for pillar in summary:
            pillar_id = pillar.get("id")
            if not pillar_id:
                raise ValueError("Missing 'id' in mstar summary item")
            pillar_key = pillar_id.strip().lower()
            if pillar_key in ("category", "category_index"):
                continue

            base_metric = pillar_key[2:] if pillar_key.startswith("q_") else pillar_key
            if base_metric == "quantitative_rating":
                base_metric = "medalist_rating"

            raw_val = pillar.get("value")
            if raw_val is None:
                continue

            val_str = str(raw_val).strip()
            if val_str in ("", "-"):
                continue

            norm_val = val_str.lower()
            if norm_val in SKIP_RATING_TOKENS:
                continue

            if base_metric == "medalist_rating":
                metric = "medalist_rating"
                mapping = MSTAR_MEDALIST_MAP
            elif base_metric in ("people", "process", "parent"):
                metric = base_metric
                mapping = MSTAR_PILLAR_MAP
            elif base_metric == "morningstar_rating":
                metric = base_metric
                mapping = MSTAR_STAR_MAP
            elif base_metric == "sustainability_rating":
                metric = base_metric
                mapping = MSTAR_SUSTAINABILITY_MAP
            else:
                raise ValueError(f"Unrecognized mstar pillar id: '{pillar_id}'")

            if norm_val not in mapping:
                raise ValueError(f"Unrecognized rating string '{raw_val}' for pillar '{pillar_id}'")

            score = mapping[norm_val]
            with date_ctx.scope(pillar.get("publish_date"), 3):
                obs = _obs(product_id, "mstar", metric, score, val_str, date_ctx, fetched_at)
                if obs:
                    observations.append(obs)

    return observations


def _lipper_universe_peer_count(universe: dict[str, Any]) -> int:
    max_n = 0
    for horizon_key in LIPPER_HORIZONS:
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
            tie = LIPPER_TIEBREAK.index(name)
        except ValueError:
            tie = len(LIPPER_TIEBREAK)
        return (_lipper_universe_peer_count(u), -tie)

    return max(universes, key=sort_key)


def extract_lipper(
    product_id: int,
    payload: dict[str, Any],
    fetched_at: datetime,
    date_ctx: DateContext | None = None,
    contract_currency: str | None = None,
    known_currencies: set[str] | None = None,
) -> list[Observation]:
    if not payload or not isinstance(payload, dict):
        return []

    if date_ctx is None:
        date_ctx = DateContext(fetched_at.date())

    universes = payload.get("universes", [])
    if not universes:
        return []

    universe = _select_lipper_universe(universes)
    universe_name = universe.get("name") or "global"
    universe_n = _lipper_universe_peer_count(universe)

    observations: list[Observation] = []

    with date_ctx.scope(universe.get("as_of_date"), 2):
        for horizon_key, horizon_suffix in LIPPER_HORIZONS.items():
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
                metric = f"{to_lower_snake_case(tag)}_{horizon_suffix}"
                raw_str = f"{val} ({universe_name}: {fund_count} funds)"
                obs = _obs(
                    product_id,
                    "lipper",
                    metric,
                    float(val),
                    raw_str,
                    date_ctx,
                    fetched_at,
                )
                if obs:
                    observations.append(obs)

    return observations


def extract_theme_weights(
    product_id: int,
    payload: dict[str, Any],
    fetched_at: datetime,
    date_ctx: DateContext | None = None,
    contract_currency: str | None = None,
    known_currencies: set[str] | None = None,
) -> list[Observation]:
    if not payload or not isinstance(payload, dict):
        return []

    if date_ctx is None:
        date_ctx = DateContext(fetched_at.date())

    observations: list[Observation] = []

    coverage = payload.get("coverage")
    if coverage is not None:
        obs = _obs(
            product_id,
            "profile",
            "theme_coverage",
            float(coverage),
            str(coverage),
            date_ctx,
            fetched_at,
        )
        if obs:
            observations.append(obs)

    for theme in payload.get("themes", []):
        theme_id = theme.get("key")
        name = theme.get("name")
        if not theme_id or not name:
            continue

        metric = to_lower_snake_case(name)
        code = str(theme_id).strip()

        weight = theme.get("weight")
        rank_adj = theme.get("rank_adjusted_weight")

        if weight is not None:
            obs = _obs(
                product_id,
                "theme",
                metric,
                float(weight),
                str(weight),
                date_ctx,
                fetched_at,
                code=code,
            )
            if obs:
                observations.append(obs)

        if rank_adj is not None:
            obs = _obs(
                product_id,
                "rank_adj_theme",
                metric,
                float(rank_adj),
                str(rank_adj),
                date_ctx,
                fetched_at,
                code=code,
            )
            if obs:
                observations.append(obs)

    return observations


EXTRACTOR_REGISTRY: dict[str, Callable[..., list[Observation]]] = {
    "holdings": extract_holdings,
    "profile": extract_profile,
    "ratios": extract_ratios,
    "esg": extract_esg,
    "mstar": extract_mstar,
    "lipper": extract_lipper,
    "theme_weights": extract_theme_weights,
}
