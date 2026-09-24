from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_DETAILS_EXCLUDED = frozenset({"landing"})


@dataclass(frozen=True)
class Endpoint:
    name: str
    url_prefix: str
    slug_template: str
    gated: bool

    @property
    def url_template(self) -> str:
        return f"{self.url_prefix}{self.slug_template}"

    def resolve(self, **kwargs: Any) -> tuple[str, str, str]:
        slug = self.slug_template.format(**kwargs)
        full_url = f"{self.url_prefix}{slug}"
        return self.url_prefix, slug, full_url


ENDPOINTS: list[Endpoint] = [
    Endpoint(
        name="landing",
        url_prefix="/tws.proxy/fundamentals/landing/",
        slug_template="{product_id}?widgets=objective,keyProfile,lipper_ratings,holdings,mf_key_ratios,mstar&lang=en",
        gated=False,
    ),
    Endpoint(
        name="holdings",
        url_prefix="/tws.proxy/fundamentals/mf_holdings/",
        slug_template="{product_id}?lang=en",
        gated=True,
    ),
    Endpoint(
        name="ratios",
        url_prefix="/tws.proxy/fundamentals/mf_ratios_fundamentals/",
        slug_template="{product_id}?lang=en",
        gated=True,
    ),
    Endpoint(
        name="profile",
        url_prefix="/tws.proxy/fundamentals/mf_profile_and_fees/",
        slug_template="{product_id}?lang=en",
        gated=True,
    ),
    Endpoint(
        name="lipper",
        url_prefix="/tws.proxy/fundamentals/mf_lip_ratings/",
        slug_template="{product_id}?lang=en",
        gated=True,
    ),
    Endpoint(
        name="mstar",
        url_prefix="/tws.proxy/mstar/fund/detail?conid=",
        slug_template="{product_id}&lang=en",
        gated=True,
    ),
    Endpoint(
        name="esg",
        url_prefix="/tws.proxy/impact/esg/",
        slug_template="{product_id}?accounts={account_id}&lang=en",
        gated=False,
    ),
    Endpoint(
        name="theme_weights",
        url_prefix="/tws.proxy/knowledge-graph/ui/fund?conid=",
        slug_template="{product_id}&max=999999999&lang=en",
        gated=False,
    ),
]

ENDPOINTS_BY_NAME: dict[str, Endpoint] = {ep.name: ep for ep in ENDPOINTS}

# Snapshot endpoints the details phase actually fetches (excludes landing).
DETAILS_ENDPOINTS: list[Endpoint] = [ep for ep in ENDPOINTS if ep.name not in _DETAILS_EXCLUDED]
GATED_ENDPOINTS: list[Endpoint] = [ep for ep in DETAILS_ENDPOINTS if ep.gated]
UNGATED_ENDPOINTS: list[Endpoint] = [ep for ep in DETAILS_ENDPOINTS if not ep.gated]


# ==============================================================================
# FAMILY CLASSIFICATIONS & CONTROL LISTS
# ==============================================================================

DEFAULT_ZERO_FAMILIES = frozenset(
    {
        "asset_class",
        "country",
        "industry",
        "credit_rating",
        "maturity",
        "theme",
        "rank_adj_theme",
        "style_box",  # panel name after hist merge; observations also have style_box_hist
    }
)

SCALAR_FAMILIES = frozenset(
    {
        "ratios",
        "esg",
        "mstar",
        "lipper",
        "profile",
    }
)

STYLE_SIZES = ("large", "multi", "mid", "small")
STYLE_STYLES = ("value", "core", "growth")

STYLE_CELLS: tuple[str, ...] = tuple(f"{size}_{style}" for size in STYLE_SIZES for style in STYLE_STYLES)


# ==============================================================================
# DEFAULT-0 METRIC MAPS (True = residual, dropped after simplex normalize)
# ==============================================================================

ASSET_CLASS_METRICS: dict[str, bool] = {
    "equity": False,
    "fixed_income": False,
    "cash": False,
    "other": True,  # residual
}

COUNTRY_CODE_REMAPS: dict[str, str | None] = {
    "Croatia": "HR",
    "Bulgaria": "BG",
    "Guam": "GU",
    "Uzbekistan": "UZ",
    "Unidentified": None,
}

INDUSTRY_NAME_REMAPS: dict[str, str] = {
    "Telecommunication Services-Discontinued eff 09/19/2020": "Communication Services",
}

INDUSTRY_METRICS: dict[str, bool] = {
    "academic_and_educational_services": False,
    "basic_materials": False,
    "communication_services": False,
    "consumer_cyclicals": False,
    "consumer_non_cyclicals": False,
    "energy": False,
    "financials": False,
    "healthcare": False,
    "industrials": False,
    "real_estate": False,
    "technology": False,
    "utilities": False,
    "non_classified_equity": True,
    "not_classified_non_equity": True,
}

CREDIT_RATING_METRICS: dict[str, bool] = {
    "aaa": False,
    "aa": False,
    "a": False,
    "bbb": False,
    "bb": False,
    "b": False,
    "ccc": False,
    "cc": False,
    "c": False,
    "d": False,
    "not_rated": True,
    "not_available": True,
}

MATURITY_METRICS: dict[str, bool] = {
    "maturity_less_than_1_year": False,
    "maturity_1_to_3_years": False,
    "maturity_3_to_5_years": False,
    "maturity_5_to_10_years": False,
    "maturity_10_to_20_years": False,
    "maturity_20_to_30_years": False,
    "maturity_greater_than_30_years": False,
    "maturity_other": True,
}


# ==============================================================================
# SCALAR METRIC SETS & MAPS
# ==============================================================================

RATIOS_PERCENTAGE_METRICS = frozenset(
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

RATIOS_STANDARD_METRICS = frozenset(
    {
        "price_earnings",
        "price_book",
        "price_sales",
        "price_cash",
        "price_to_dividend",
        "average_final_composite_zscore",
        "latest_composite_z_score",
        "latest_dividend_yield_zscore",
        "latest_price_sales_zscore",
        "latest_price_to_book_zscore",
        "latest_price_to_earnings_zscore",
        "latest_return_on_equity_zscore",
        "latest_sps_growth_zscore",
        "weighted_final_composite_zscore",
        "average_quality",
        "effective_maturity",
        "nominal_maturity",
        "total_assets_total_equity",
        "total_debt_total_capital",
        "total_debt_total_equity",
        "lt_debt_shareholders_equity",
        "ebit_to_interest",
        "sales_to_total_assets",
    }
)

ALL_RATIOS_METRICS = RATIOS_PERCENTAGE_METRICS | RATIOS_STANDARD_METRICS

ESG_METRICS = frozenset(
    {
        "tresgs",
        "tresgcs",
        "tresgccs",
        "tresgens",
        "tresgenrrs",
        "tresgeners",
        "tresgenpis",
        "tresgsos",
        "tresgsowos",
        "tresgsohrs",
        "tresgsocos",
        "tresgsoprs",
        "tresgcgs",
        "tresgcgbds",
        "tresgcgsrs",
        "tresgcgvss",
    }
)

MSTAR_METRICS = frozenset(
    {
        "people",
        "process",
        "parent",
        "medalist_rating",
        "morningstar_rating",
        "sustainability_rating",
    }
)

MSTAR_MEDALIST_MAP = {
    "gold": 5.0,
    "silver": 4.0,
    "bronze": 3.0,
    "neutral": 2.0,
    "negative": 1.0,
}

MSTAR_PILLAR_MAP = {
    "high": 5.0,
    "above_average": 4.0,
    "above average": 4.0,
    "above-average": 4.0,
    "average": 3.0,
    "below_average": 2.0,
    "below average": 2.0,
    "below-average": 2.0,
    "low": 1.0,
}

MSTAR_STAR_MAP = {
    "1": 1.0,
    "2": 2.0,
    "3": 3.0,
    "4": 4.0,
    "5": 5.0,
}

MSTAR_SUSTAINABILITY_MAP = {
    "1": 1.0,
    "2": 2.0,
    "3": 3.0,
    "4": 4.0,
    "5": 5.0,
    "high": 5.0,
    "above_average": 4.0,
    "above average": 4.0,
    "above-average": 4.0,
    "average": 3.0,
    "below_average": 2.0,
    "below average": 2.0,
    "below-average": 2.0,
    "low": 1.0,
}

SKIP_RATING_TOKENS = frozenset(
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

LIPPER_HORIZONS: dict[str, str] = {
    "overall": "overall",
    "3_year": "3yr",
    "5_year": "5yr",
    "10_year": "10yr",
}

LIPPER_TIEBREAK: tuple[str, ...] = ("United States", "Germany", "UK", "Canada", "Japan", "Australia")

PROFILE_METRICS = frozenset(
    {
        "total_expense_ratio",
        "total_net_assets_local",
        "is_passive",
        "manager_tenure_years",
        "audited_net_expense_ratio",
        "management_expense_ratio",
        "non_management_expense_ratio",
        "top_10_weight",
        "theme_coverage",
        "esg_coverage",
        "mstar_coverage",
    }
)
