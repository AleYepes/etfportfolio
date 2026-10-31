"""
Great Powers Index (2024) - Pure Vetted Specification
=====================================================
Built strictly on recency-passing series from `clean_indicators.csv`.
No external dependencies. No multi-year extrapolation. Zero stale series.
"""

from typing import Any, Dict

GAUGE_CONFIGS: Dict[str, Dict[str, Any]] = {
    # =========================================================================
    # 1. WELL-BEING: HEALTH INDEX (Report p. 16 / 136)
    # =========================================================================
    "health_index": {
        "name": "Health Index",
        "description": "Measures bodily longevity, infant survival, nutritional access, and environmental infrastructure.",
        "indicators": {
            "SP.DYN.LE00.IN": {
                "name": "Life expectancy at birth, total (years)",
                "weight": 0.350,
                "polarity": +1,
                "transform": "identity",
            },
            "SH.DYN.MORT": {
                "name": "Mortality rate, under-5 (per 1,000 live births)",
                "weight": 0.200,
                "polarity": -1,
                "transform": "identity",
            },
            "SN.ITK.DEFC.ZS": {
                "name": "Prevalence of undernourishment (% of population)",
                "weight": 0.200,
                "polarity": -1,
                "transform": "identity",
            },
            "SH.STA.BASS.ZS": {
                "name": "People using at least basic sanitation services (%)",
                "weight": 0.100,
                "polarity": +1,
                "transform": "identity",
            },
            "SH.H2O.BASW.ZS": {
                "name": "People using at least basic drinking water services (%)",
                "weight": 0.075,
                "polarity": +1,
                "transform": "identity",
            },
            "EG.CFT.ACCS.ZS": {
                "name": "Access to clean fuels/tech for cooking (% pop) [Clean Air / Indoor Environment]",
                "weight": 0.075,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    # =========================================================================
    # 2. EMPIRE PILLAR: ECONOMIC OUTPUT (Report pp. 12, 15, 132)
    # =========================================================================
    "economic_output": {
        "name": "Economic Output Pillar",
        "description": "Blends absolute global economic volume (PPP & Nominal) with living standards per capita.",
        "indicators": {
            "NY.GDP.MKTP.PP.KD": {
                "name": "GDP, PPP (constant 2021 int$) [World Share]",
                "weight": 0.450,
                "polarity": +1,
                "transform": "world_share",
            },
            "NY.GDP.MKTP.CD": {
                "name": "GDP (current US$) [World Share]",
                "weight": 0.250,
                "polarity": +1,
                "transform": "world_share",
            },
            "NY.GDP.PCAP.PP.KD": {
                "name": "GDP per capita, PPP (constant 2021 int$) [Log Scale]",
                "weight": 0.300,
                "polarity": +1,
                "transform": "log",
            },
        },
    },
    # =========================================================================
    # 3. EMPIRE PILLAR: TRADE STRENGTH (Report pp. 12, 15, 132)
    # =========================================================================
    "trade_strength": {
        "name": "Trade Pillar",
        "description": "Measures export dominance, value-add sophistication, and global commercial integration.",
        "indicators": {
            "BX.GSR.GNFS.CD": {
                "name": "Exports of goods and services (current US$) [World Share]",
                "weight": 0.450,
                "polarity": +1,
                "transform": "world_share",
            },
            "TX.VAL.MRCH.CD.WT": {
                "name": "Merchandise exports (current US$) [World Share]",
                "weight": 0.250,
                "polarity": +1,
                "transform": "world_share",
            },
            "TX.MNF.TECH.ZS.UN": {
                "name": "Medium and high-tech exports (% manufactured exports)",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
            "NE.TRD.GNFS.ZS": {
                "name": "Trade (% of GDP) [Openness & Market Integration]",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    # =========================================================================
    # 4. EMPIRE PILLAR: INNOVATION & TECHNOLOGY (Report pp. 10, 12, 15)
    # =========================================================================
    "innovation_technology": {
        "name": "Innovation & Technology Pillar",
        "description": "Measures resident intellectual property, royalty receipts, and high-tech manufacturing.",
        "indicators": {
            "IP.PAT.RESD": {
                "name": "Patent applications, residents [World Share]",
                "weight": 0.350,
                "polarity": +1,
                "transform": "world_share",
            },
            "BX.GSR.ROYL.CD": {
                "name": "Charges for the use of intellectual property, receipts [World Share]",
                "weight": 0.250,
                "polarity": +1,
                "transform": "world_share",
            },
            "IP.TMK.RSCT": {
                "name": "Trademark applications, resident [World Share]",
                "weight": 0.150,
                "polarity": +1,
                "transform": "world_share",
            },
            "NV.MNF.TECH.ZS.UN": {
                "name": "Medium and high-tech manufacturing value added (% of MVA)",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
            "IT.NET.SECR.P6": {
                "name": "Secure Internet servers (per 1 million people)",
                "weight": 0.100,
                "polarity": +1,
                "transform": "log",
            },
        },
    },
    # =========================================================================
    # 5. EMPIRE PILLAR: MILITARY STRENGTH (Report pp. 12, 15)
    # =========================================================================
    "military_strength": {
        "name": "Military Strength Pillar",
        "description": "Measures absolute defense expenditures, fiscal military allocation, and arms acquisitions.",
        "indicators": {
            "MS.MIL.XPND.CD": {
                "name": "Military expenditure (current USD) [World Share]",
                "weight": 0.550,
                "polarity": +1,
                "transform": "world_share",
            },
            "MS.MIL.XPND.GD.ZS": {
                "name": "Military expenditure (% of GDP)",
                "weight": 0.250,
                "polarity": +1,
                "transform": "identity",
            },
            "MS.MIL.XPND.ZS": {
                "name": "Military expenditure (% of general government expenditure)",
                "weight": 0.100,
                "polarity": +1,
                "transform": "identity",
            },
            "MS.MIL.MPRT.KD": {
                "name": "Arms imports (SIPRI trend indicator values) [World Share]",
                "weight": 0.100,
                "polarity": +1,
                "transform": "world_share",
            },
        },
    },
    # =========================================================================
    # 6. EMPIRE PILLAR: FINANCIAL CENTER & CAPITAL MARKETS (Report pp. 12, 15)
    # =========================================================================
    "financial_center": {
        "name": "Markets & Financial Center Pillar",
        "description": "Measures private credit depth, bank capital adequacy, asset health, and portfolio equity inflows.",
        "indicators": {
            "FS.AST.PRVT.GD.ZS": {
                "name": "Domestic credit to private sector (% of GDP)",
                "weight": 0.350,
                "polarity": +1,
                "transform": "identity",
            },
            "FB.BNK.CAPA.ZS": {
                "name": "Bank capital to assets ratio (%)",
                "weight": 0.250,
                "polarity": +1,
                "transform": "identity",
            },
            "FB.AST.NPER.ZS": {
                "name": "Bank nonperforming loans to total gross loans (%)",
                "weight": 0.150,
                "polarity": -1,
                "transform": "identity",
            },
            "BX.PEF.TOTL.CD.WD": {
                "name": "Portfolio equity, net inflows (BoP, current US$) [World Share]",
                "weight": 0.150,
                "polarity": +1,
                "transform": "world_share",
            },
            "FB.CBK.BRCH.P5": {
                "name": "Commercial bank branches (per 100,000 adults)",
                "weight": 0.100,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    # =========================================================================
    # 7. EMPIRE PILLAR: INFRASTRUCTURE & INVESTMENT (Report pp. 12, 15)
    # =========================================================================
    "infrastructure_investment": {
        "name": "Infrastructure & Investment Pillar",
        "description": "Measures fixed capital accumulation, national savings rate, electrification, and broadband depth.",
        "indicators": {
            "NE.GDI.FTOT.CD": {
                "name": "Gross fixed capital formation (current US$) [World Share]",
                "weight": 0.350,
                "polarity": +1,
                "transform": "world_share",
            },
            "NE.GDI.FTOT.ZS": {
                "name": "Gross fixed capital formation (% of GDP)",
                "weight": 0.250,
                "polarity": +1,
                "transform": "identity",
            },
            "NY.GNS.ICTR.ZS": {
                "name": "Gross savings (% of GDP)",
                "weight": 0.200,
                "polarity": +1,
                "transform": "identity",
            },
            "EG.ELC.ACCS.ZS": {
                "name": "Access to electricity (% of population)",
                "weight": 0.100,
                "polarity": +1,
                "transform": "identity",
            },
            "IT.NET.BBND": {
                "name": "Fixed broadband subscriptions [World Share]",
                "weight": 0.100,
                "polarity": +1,
                "transform": "world_share",
            },
        },
    },
    # =========================================================================
    # 8. FUTURE GROWTH: DEMOGRAPHIC ENGINE (Report pp. 28 / 148)
    # =========================================================================
    "working_age_population_growth": {
        "name": "Working Age Population Growth (10Y Forward)",
        "description": "Forward-looking demographic tailwind derived from incoming young cohorts vs retiring cohorts.",
        "indicators": {
            "SP.POP.0014.TO": {
                "name": "Population ages 0-14, total (inflow cohort entering working age)",
                "weight": 0.500,
                "polarity": +1,
                "transform": "ratio_to_working_pop",  # SP.POP.0014.TO / SP.POP.1564.TO
            },
            "SP.POP.DPND.OL": {
                "name": "Age dependency ratio, old (% of working-age population)",
                "weight": 0.300,
                "polarity": -1,
                "transform": "identity",
            },
            "SP.POP.GROW": {
                "name": "Population growth (annual %)",
                "weight": 0.200,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    # =========================================================================
    # 9. FUTURE GROWTH: WHAT YOU PAY VS. WHAT YOU GET (Report pp. 38 / 158)
    # =========================================================================
    "what_you_pay_vs_what_you_get": {
        "name": "Cost Competitiveness & Value Proposition",
        "description": "Labor productivity relative to wage cost hurdle, savings rates, and macroeconomic stability.",
        "indicators": {
            "NY.GDP.PCAP.CD": {
                "name": "Nominal GDP per capita (USD in log terms) [Inverse cost hurdle, p. 160]",
                "weight": 0.250,
                "polarity": -1,
                "transform": "log",
            },
            "SL.GDP.PCAP.EM.KD": {
                "name": "GDP per person employed (constant 2021 PPP $) [Productivity output]",
                "weight": 0.250,
                "polarity": +1,
                "transform": "log",
            },
            "NE.GDI.FTOT.ZS": {
                "name": "Gross fixed capital formation (% of GDP)",
                "weight": 0.125,
                "polarity": +1,
                "transform": "identity",
            },
            "NY.GNS.ICTR.ZS": {
                "name": "Gross savings (% of GDP)",
                "weight": 0.125,
                "polarity": +1,
                "transform": "identity",
            },
            "SL.TLF.CACT.ZS": {
                "name": "Labor force participation rate (15+) [Work ethic & effort proxy]",
                "weight": 0.125,
                "polarity": +1,
                "transform": "identity",
            },
            "SP.POP.DPND": {
                "name": "Age dependency ratio (% of working-age population)",
                "weight": 0.125,
                "polarity": -1,
                "transform": "identity",
            },
        },
    },
    # =========================================================================
    # 10. FUTURE GROWTH: CULTURE & GOVERNANCE (Report pp. 49 / 169)
    # =========================================================================
    "culture_and_governance": {
        "name": "Culture & Institutional Effectiveness",
        "description": "Rule of law, corruption control, regulatory burden, and market self-sufficiency.",
        "indicators": {
            "GOV_WGI_RL_EST": {
                "name": "Rule of Law - Governance estimate",
                "weight": 0.300,
                "polarity": +1,
                "transform": "identity",
            },
            "GOV_WGI_CC_EST": {
                "name": "Control of Corruption - Governance estimate",
                "weight": 0.300,
                "polarity": +1,
                "transform": "identity",
            },
            "GOV_WGI_RQ_EST": {
                "name": "Regulatory Quality - Governance estimate",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
            "GOV_WGI_GE_EST": {
                "name": "Government Effectiveness - Governance estimate",
                "weight": 0.100,
                "polarity": +1,
                "transform": "identity",
            },
            "NE.CON.GOVT.ZS": {
                "name": "General government consumption (% of GDP) [Inverse proxy for market self-reliance]",
                "weight": 0.150,
                "polarity": -1,
                "transform": "identity",
            },
        },
    },
    # =========================================================================
    # 11. FUTURE GROWTH: INDEBTEDNESS & DEBT CYCLES (Report pp. 31 / 151)
    # =========================================================================
    "indebtedness_debt_cycles": {
        "name": "Indebtedness & Macro Solvency",
        "description": "Private leverage overhang, fiscal claims, external account balance, and inflation stability.",
        "indicators": {
            "FS.AST.PRVT.GD.ZS": {
                "name": "Domestic credit to private sector (% of GDP) [Overhang drag]",
                "weight": 0.300,
                "polarity": -1,
                "transform": "identity",
            },
            "FS.AST.CGOV.GD.ZS": {
                "name": "Claims on central government (% of GDP) [Fiscal borrowing burden]",
                "weight": 0.250,
                "polarity": -1,
                "transform": "identity",
            },
            "BN.CAB.XOKA.GD.ZS": {
                "name": "Current account balance (% of GDP) [External solvency buffer]",
                "weight": 0.200,
                "polarity": +1,
                "transform": "identity",
            },
            "FI.RES.TOTL.MO": {
                "name": "Total reserves in months of imports [Liquidity buffer]",
                "weight": 0.100,
                "polarity": +1,
                "transform": "identity",
            },
            "FP.CPI.TOTL.ZG": {
                "name": "Inflation, consumer prices (annual %) [Monetary instability drag]",
                "weight": 0.150,
                "polarity": -1,
                "transform": "identity",
            },
        },
    },
}
