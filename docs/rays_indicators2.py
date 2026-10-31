# 1. MACRO PROJECTION MODEL: FUTURE REAL GDP GROWTH (Additive Equation)
# RGDP Growth = Pop_Growth + Growth_Per_Worker + Error_Adj
FUTURE_GROWTH_FORMULA = {
    "type": "additive_growth_identity",
    "formula": "working_age_pop_growth + growth_per_worker_estimate",
    "growth_per_worker_estimate": {
        "type": "weighted_growth_impact",
        "components": {
            # Implied growth rates derived from standardized scores (Report p. 143, 152)
            "productivity_implied_growth": 0.65,  # ~2/3 weight
            "indebtedness_implied_growth": 0.35,  # ~1/3 weight
        },
    },
}

# 2. STANDARDIZED GAUGE TREES (Hierarchical Weighted Composites)
RAY_DALIO_GAUGE_TREES = {
    # PRODUCTIVITY GAUGE (Z-score relative to countries with similar income; pp. 143, 155)
    "productivity_gauge": (
        {
            "what_you_pay_vs_what_you_get": (
                {
                    "education_cost_quality_adj": (None, 0.25),
                    "labor_productivity_cost_adj": (None, 0.25),
                    "working_hard": (
                        {
                            "avg_hours_worked_per_week": (None, 2 / 3),
                            "dependency_ratio_10y_fwd_change": (None, 1 / 3),
                        },
                        0.25,
                    ),
                    "investing": (
                        {
                            "investment_pct_gdp": (None, 3.1 / 17.5),
                            "household_savings_rate": (None, 3.1 / 17.5),
                            "growth_in_capital_stock": (None, 3.1 / 17.5),
                            "productivity_of_capital": (None, 3.1 / 17.5),
                            "income_volatility": (None, 2.6 / 17.5),
                            "inflation_volatility": (None, 2.6 / 17.5),
                        },
                        0.25,
                    ),
                },
                0.70,  # 70% of Productivity
            ),
            "culture": (
                {
                    "corruption": (
                        {
                            "transparency_index": (None, 0.50),
                            "surveys_of_corruption": (
                                {
                                    "diversion_of_public_funds": (None, 1 / 3),
                                    "irregular_payments_and_bribes": (None, 1 / 3),
                                    "favoritism_in_govt_decisions": (None, 1 / 3),
                                },
                                0.50,
                            ),
                        },
                        1 / 6,
                    ),
                    "bureaucracy": (
                        {
                            "starting_a_business": (None, 1 / 3),
                            "construction_permits": (None, 1 / 3),
                            "burden_of_govt_regulation": (None, 1 / 3),
                        },
                        1 / 6,
                    ),
                    "rule_of_law": (
                        {
                            "efficiency_of_legal_frameworks": (None, 0.25),
                            "property_rights": (None, 0.25),
                            "protecting_investors_value": (None, 0.25),
                            "enforcing_contracts": (None, 0.25),
                        },
                        1 / 6,
                    ),
                    "savoring_life_vs_achieving": (
                        {
                            "observed_outcomes_hours_worked": (None, 0.50),
                            "expressed_values_surveys": (None, 0.50),
                        },
                        1 / 6,
                    ),
                    "innovation_commercialism": (
                        {
                            "innovation_inputs": (None, 0.50),
                            "innovation_outputs": (None, 0.50),
                        },
                        1 / 6,
                    ),
                    "self_sufficiency": (
                        {
                            "hard_work": (None, 0.50),
                            "government_support": (None, 0.25),
                            "labor_market_rigidity": (None, 0.25),
                        },
                        1 / 6,
                    ),
                },
                0.30,  # 30% of Productivity
            ),
        },
        1.0,
    ),

    # INDEBTEDNESS GAUGE (Appendix p. 143)
    "indebtedness_gauge": (
        {
            "debt_levels_and_flow": (None, 0.50),
            "monetary_policy": (None, 0.50),
        },
        1.0,
    ),

    # WELLBEING GAUGES (Appendix p. 136 - Exact weights from report)
    "health_index": (
        {
            "life_expectancy_at_birth": (None, 0.300),
            "share_facing_food_insecurity": (None, 0.150),
            "access_to_essential_health_services": (None, 0.150),
            "child_mortality_rate": (None, 0.100),
            "homicide_rate": (None, 0.075),
            "displaced_by_conflict": (None, 0.075),
            "air_pollution_exposure_ppm": (None, 0.050),
            "safely_managed_drinking_water": (None, 0.050),
            "basic_sanitation_facilities": (None, 0.050),
        },
        1.0,
    ),
    "happiness_index": (
        {
            "share_reported_happy": (None, 0.225),
            "share_reported_daily_enjoyment": (None, 0.225),
            "reported_life_satisfaction": (None, 0.225),
            "good_social_support_network": (None, 0.225),
            "suicide_rate_per_100k": (None, 0.100),
        },
        1.0,
    ),

    # THREE BIG CYCLES (Tracked as separate archetype Z-scores; p. 8, 15)
    "internal_order_cycle": (
        {
            "wealth_values_gap": (None, 0.50),
            "internal_conflict": (None, 0.50),
        },
        1.0,
    ),
    "economic_financial_cycle": (
        {
            "debt_burden": (None, 0.50),
            "expected_growth": (None, 0.50),
        },
        1.0,
    ),
    "external_order_cycle": (None, 1.0),

    # OVERALL STRENGTH GAUGE (0 to 1; weights are estimated heuristics)
    "overall_strength_gauge_heuristic": (
        {
            "key_eight_measures_of_power": (
                {
                    "economic_output": ({"total": 0.5, "per_capita": 0.5}, 0.15),
                    "financial_center": ({"total": 0.5, "per_capita": 0.5}, 0.15),
                    "reserve_currency_status": ({"total": 1.0}, 0.12),
                    "military_strength": ({"total": 0.5, "per_capita": 0.5}, 0.12),
                    "innovation_and_technology": ({"total": 0.5, "per_capita": 0.5}, 0.12),
                    "trade": ({"total": 0.5, "per_capita": 0.5}, 0.10),
                    "education": ({"total": 0.5, "per_capita": 0.5}, 0.10),
                    "competitiveness": ({"total": 1.0}, 0.08),
                },
                0.80,  # Estimated heuristic
            ),
            "additional_measures_of_power": (
                {
                    "infrastructure_and_investment": (None, 0.20),
                    "resource_allocation_efficiency": (None, 0.20),
                    "geology": (None, 0.20),
                    "character_determination_civility": (None, 0.15),
                    "governance_rule_of_law": (None, 0.15),
                    "acts_of_nature": (None, 0.10),
                },
                0.20,  # Estimated heuristic
            ),
        },
        1.0,
    ),
}