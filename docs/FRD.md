# Functional Requirements Document: Macroeconomic Country Indicator Ingestion and Factor Collapse

**Document Status:** Approved Architecture Specification (Empirically Grounded & Aligned with Ray Dalio / Bridgewater *Great Powers Index 2024*)  
**Primary Reference:** Ray Dalio / Bridgewater Associates, *The Great Powers Index: 2024 — How the Leading 24 Countries Are Doing and Their Prospects for the Next 10 Years* (July 31, 2024)  
**Target Scope:** `bronze.country_indicators` and `cold_storage.country_indicators` schemas, programmatic multi-source ingestion in `etfportfolio/ingest/country_indicators.py`, macroeconomic engine in `etfportfolio/prep/macro.py`, and portfolio factor collapse in `etfportfolio/prep/panel.py`.

---

## 1. Executive Summary & Problem Context

### 1.1 What the Country Family Currently Contains
In the existing architecture, `silver.observations` records per-ETF geographic asset allocations under `family = 'country'`. Each record contains the fractional portfolio weight (`value`) of a specific country for an ETF at a point in time extracted from IBKR holdings snapshots. The `metric` column holds the slugified country name (e.g. `united_states`) and the `code` column holds the ISO 3166-1 alpha-2 code (e.g. `US`). Approximately ~109 distinct country pairings exist across the universe.

### 1.2 The Problem: Raw Country Weights as Panel Features Are Unusable
Currently, `prep/panel.py` materializes all surviving country observations as individual default-0 columns in `silver.monthly_panel` (`country_united_states`, `country_germany`, etc.). This causes severe quantitative drawbacks:
1. **Curse of High Dimensionality**: The monthly factor panel expands by over 100 sparse columns, where most ETFs have zeroes for almost every country.
2. **Lack of Factor Premia / Economic Cohesion**: A 3% portfolio allocation to Japan or a 1% allocation to Brazil is not an economically interpretable factor. Cross-sectional asset returns and macro risks are driven by underlying structural drivers—economic scale, productivity arbitrage, institutional stability, debt solvency, monetary headroom, geopolitical alignment, and reserve currency hegemony—not by arbitrary political borders.
3. **Imbalance Across Universes**: Large single-country funds (e.g., SPY, INDA) and diversified global funds (e.g., ACWI, EEM) cannot be placed on a continuous factor ranking using isolated country dummy weights.

### 1.3 The Solution: Ray Dalio's Great Powers Factor Model (2024 Edition)
We replace the raw per-country dummy features in `silver.monthly_panel` with **18 economically robust, continuous portfolio-level macroeconomic factors** strictly aligned with Ray Dalio's *Great Powers Index (2024)*.

ETF country weights are collapsed into portfolio-weighted exposures across the core components of Dalio's framework:
1. **Empire Strength Scores (Total & Per Capita)**: Indexes from $0.0$ to $1.0$ measuring absolute geopolitical/economic dominance (**Total**, capturing mass and world share) and productivity/living-standard intensity (**Per Capita / Quality**, capturing efficiency and living standards) across Dalio’s 8 true headline pillars (Report p. 135):
   - *Economic Output*
   - *Trade*
   - *Innovation & Technology*
   - *Military Strength*
   - *Markets & Financial Center*
   - *Education*
   - *Competitiveness, Culture, & Investment*
   - *Reserve Currency Status* (the crowning superpower pillar)
2. **Top Near-Term Conflict Risks**:
   - *Internal Order / Conflict Gauge*: Measures domestic polarization, social discontent, and wealth/values gaps (Dalio's #1 near-term risk; Report p. 7).
   - *External Great Power Conflict Gauge*: Measures bilateral trade friction, sanctions, defense spending surges, and geopolitical bloc alignment (Dalio's #2 near-term risk; Report p. 8).
3. **Well-Being Health Index**: A standardized cross-sectional Z-score measuring bodily longevity, infant mortality, clean drinking water, sanitation facilities, nutritional sufficiency, air pollution, homicide rates, conflict displacement, and essential healthcare access (calibrated to Dalio's 9 indicators and published weights; Report p. 136).
   *(Note: The Happiness Index is deliberately omitted from production because underlying Gallup World Poll / World Happiness Report microdata feeds are paywalled or ended open updates around 2020. The objective Health Index serves as the authoritative well-being factor.)*
4. **10-Year Forward Real GDP Growth Engine**: An annualized growth projection combining demographic workforce expansion ($\Delta \text{Workers}$, derived from empirical working-age population projections) with output-per-worker potential (cost-competitiveness arbitrage, culture, and debt cycle solvency).

Missing or unclassified country weights are handled via **Bayesian shrinkage to the cross-sectional median**, preventing cliff dropouts and guaranteeing a continuous, well-conditioned factor panel.

---

## 2. Core Architectural Invariants & Package Separation

All implementation work must strictly comply with the project guidelines in [`AGENTS.md`](file:///Users/alex/Documents/etfportfolio/AGENTS.md):

### 2.1 Package Layering Rules
* **`etfportfolio/core/`**: Shared schemas, database connection utilities, endpoints, logging, progress bars, and configuration constants. Both `ingest/` and `prep/` import from `core/`.
* **`etfportfolio/ingest/`**: Programmatically acquires raw external data and persists it to the `bronze` and `cold_storage` database schemas. Must **never** import from `prep/`.
* **`etfportfolio/prep/`**: Pure offline data transformation and factor panel construction. Reads from `bronze` and `silver` layers; writes to `silver` and `gold`. Must **never** import from `ingest/` and must **never** perform network requests.

### 2.2 Ingestion Independence (No Prep Bootstrap Dependency)
`ingest/country_indicators.py` resolves its primary target universe dynamically from `country_converter.CountryConverter()` for all recognized sovereign economies (~217 countries).
As an additional validation check, it also inspects `silver.observations` if that table already exists and is non-empty. If an outlier country appears in `silver.observations` that is unrecognized by `country_converter`, ingestion raises an explicit error so a mapping can be added to `COUNTRY_CODE_REMAPS` in `core/endpoints.py`. On a fresh database where `silver.observations` is empty, ingestion proceeds normally across the sovereign universe.

### 2.3 Elimination of Obsolete Panel Constructs
In `prep/panel.py`, the temporary table `product_universe` existed exclusively to track sparse, per-product `country` observations. Because all raw `country_*` features are permanently eliminated from `silver.monthly_panel` and replaced by continuous `macro_*` features, `product_universe` and its join in Step 8 are **deleted outright**. No backward-compatibility shims or fallback flags are retained.

---

## 3. Database Schema Specifications

The following table definitions must be added to [`etfportfolio/core/schema.sql`](file:///Users/alex/Documents/etfportfolio/etfportfolio/core/schema.sql).

### 3.1 Raw Table: `bronze.country_indicators`
Stores raw, untransformed annual observations ingested from external macro providers. Both `country_code` (ISO-3) and `iso2_code` (ISO-2) are stored to enable direct, zero-overhead equi-joins in DuckDB against `silver.observations.code`:

```sql
CREATE TABLE IF NOT EXISTS bronze.country_indicators (
    country_code    VARCHAR NOT NULL,       -- ISO 3166-1 alpha-3 (e.g. 'USA', 'DEU')
    iso2_code       VARCHAR,                -- ISO 3166-1 alpha-2 (e.g. 'US', 'DE')
    indicator_code  VARCHAR NOT NULL,       -- Vendor indicator ID (e.g. 'IMF.GGXWDG_NGDP', 'SP.DYN.LE00.IN')
    year            INTEGER NOT NULL,       -- Calendar year (e.g. 2023)
    value           DOUBLE NOT NULL,        -- Raw observed numerical value
    source          VARCHAR NOT NULL,       -- Provider tag: 'WB', 'IMF', 'EIA', 'VDEM', 'NDGAIN', 'REF'
    fetched_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (country_code, indicator_code, year)
);
```

### 3.2 Ingestion Status Table: `bronze.country_indicators_status`
Tracks per-source and per-indicator freshness and vetting health:

```sql
CREATE TABLE IF NOT EXISTS bronze.country_indicators_status (
    indicator_code  VARCHAR PRIMARY KEY,
    source          VARCHAR NOT NULL,
    latest_year     INTEGER NOT NULL,
    country_count   INTEGER NOT NULL,
    status          VARCHAR NOT NULL DEFAULT 'OK', -- 'OK', 'FAILED_GATING'
    error_message   VARCHAR,
    last_fetched_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 3.3 Historical Revision Table: `cold_storage.country_indicators`
Preserves superseded data when an external statistical agency restates past historical figures:

```sql
CREATE TABLE IF NOT EXISTS cold_storage.country_indicators (
    country_code    VARCHAR NOT NULL,
    iso2_code       VARCHAR,
    indicator_code  VARCHAR NOT NULL,
    run_id          TIMESTAMP NOT NULL,     -- Timestamp of the refetch run
    year            INTEGER NOT NULL,
    value           DOUBLE NOT NULL,        -- Prior historical value before revision
    source          VARCHAR NOT NULL,
    reason          VARCHAR,                -- e.g. 'HISTORICAL_REVISION'
    PRIMARY KEY (country_code, indicator_code, run_id, year)
);
```

---

## 4. Multi-Source Ingestion (`etfportfolio/ingest/country_indicators.py`)

A modular ingestion pipeline acquires macroeconomic data across approved open public providers. Network operations use standard browser headers, retry logic, and programmatic parsing directly in memory.

### 4.1 Approved Providers & Access Patterns

#### 1. World Bank (WDI, WGI, Health & Demographics)
* **Provider**: World Bank Open Data API (`wbgapi`).
* **Scope**: Health index indicators, economic output, trade flows, tech manufacturing, patent counts, military spending, education enrollments, working-age demographic projections (`SP.POP.1564.TO.ZG`), and Worldwide Governance Indicators (`RL.EST`, `CC.EST`, `RQ.EST`, `GE.EST`).
* **Protocol**: Looping country-by-country over ISO-3 codes using `wbgapi.data.DataFrame(indicators, economy=iso3, time=range(2000, current_year+1), numericTimeKeys=True)`. Tested and validated in `new.ipynb`.

#### 2. IMF World Economic Outlook (WEO)
* **Provider**: International Monetary Fund SDMX 3.0 REST API.
* **Scope**:
  * `IMF.GGXWDG_NGDP`: General government gross debt (% of GDP).
  * `IMF.GGXCNL_NGDP`: General government net lending/borrowing deficit (% of GDP).
* **Endpoint Pattern**:
  `https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.RES/WEO/~/{iso3}.GGXWDG_NGDP+GGXCNL_NGDP.A?startPeriod=2000`
* **Protocol**: HTTP GET with `Accept: text/csv`. Parse into `(country_code, indicator, year, value)`.

#### 3. US Energy Information Administration (EIA)
* **Provider**: US EIA API v2 (`https://api.eia.gov/v2/international/data`).
* **API Key**: `kJpbTkcdzNctTcTMKHJF9bWdkwbwmqHWJFfRnKGL`.
* **Scope**: Primary energy production (activityId="1", QBTU, productId="44") and consumption (activityId="2", QBTU, productId="44").
* **Derived Metric**: Net Energy Self-Sufficiency ratio $(\text{Production} - \text{Consumption}) / \text{Consumption}$, stored as `EIA.ENERGY.SELF_SUFF`.

#### 4. V-Dem Institute (Varieties of Democracy)
* **Provider**: V-Dem Institute, University of Gothenburg (V-Dem v16 Core Dataset).
* **Indicators**:
  * `VDEM.v2excrptps`: Public sector corruption index.
  * `VDEM.v2cltrnslw`: Transparent laws with predictable enforcement.
  * `VDEM.v2clrspct`: Rigorous and impartial public administration.
  * `VDEM.v2x_rule`: Rule of law index.
  * `VDEM.v2cacamps`: Political polarization index (Dalio Internal Conflict).
  * `VDEM.v2caviol`: Political violence and discontent index (Dalio Internal Conflict).
* **Protocol**: Stream ZIP payload into memory (`io.BytesIO`), parse target columns with `pandas.read_csv`.

#### 5. ND-GAIN (Notre Dame Global Adaptation Initiative)
* **Provider**: University of Notre Dame (`ndgain_countryindex_2026.zip`).
* **Indicators**:
  * `NDGAIN.INDEX`: Overall Country Climate Resilience Index.
  * `NDGAIN.READINESS`: Climate Adaptation Readiness Score.
  * `NDGAIN.VULNERABILITY`: Climate Vulnerability Score.
* **Protocol**: Stream ZIP payload into memory, unpivot year columns into `(country_code, year, value)`.

#### 6. IMF COFER & BIS Currency Authority Reference Data
* **Provider**: IMF COFER (Currency Composition of Official Foreign Exchange Reserves) & BIS FX Triennial Turnover benchmark statistics.
* **Technical Ingestion Mechanism**:
  IMF COFER and BIS FX turnover report global currency aggregates across currency issuers, **not** country-by-country time series. To prevent phantom endpoint failures (querying 109 countries for non-existent endpoints), the published historical benchmark matrix is maintained as an authoritative reference table `RESERVE_CURRENCY_SHARES` in `etfportfolio/core/endpoints.py`:
  - USD (`USA`): 58.4% reserve share, 44.2% FX turnover share
  - EUR (`DEU`, `FRA`, `ITA`, `ESP`, `NLD`, etc.): 19.8% reserve share, 15.3% FX turnover share
  - JPY (`JPN`): 5.4% reserve share, 8.4% FX turnover share
  - GBP (`GBR`): 4.8% reserve share, 6.4% FX turnover share
  - CNY (`CHN`): 2.3% reserve share, 3.5% FX turnover share
  - CAD (`CAN`): 2.5% reserve share, 3.2% FX turnover share
  - AUD (`AUS`): 2.1% reserve share, 3.2% FX turnover share
  - CHF (`CHE`): 0.2% reserve share, 2.6% FX turnover share
  - All other economies: 0.0%
  During ingestion, `ingest/country_indicators.py` populates `bronze.country_indicators` under `source = 'REF'` for indicator codes `IMF.COFER_SHARE` and `BIS.FX_TURNOVER_SHARE` across all sovereign economies for all years, ensuring complete, robust coverage without fragile external scraping.

### 4.2 Freshness Caching & Revision Storage
1. **30-Day Freshness Window**: If an indicator has `last_fetched_at` within the last 30 days (720 hours) and `status = 'OK'`, ingestion skips it unless `force=True`.
2. **Historical Revision Archiving**: If incoming data differs from existing `bronze` records by $> 0.01\%$, the prior row is copied to `cold_storage.country_indicators` with `run_id = CURRENT_TIMESTAMP` and `reason = 'HISTORICAL_REVISION'`.

### 4.3 Automated Production Vetting Gates
Before committing incoming series:
1. **Sovereign Coverage**: Countries reporting / target sovereign countries $\ge 0.760$.
   *(Note: For conflict displacement `VC.IDP.TOCV`, countries with no reported displacement are imputed as 0.0 prior to gating, reflecting peacetime baseline).*
2. **Recent Coverage**: Countries reporting in the last 3 calendar years $\ge 0.700$.
   *(Note: For survey-based indicators with known global statistical release lags such as UHC index `SH.UHC.SRVS.CV.XD` or air pollution `EN.ATM.PM25.MC.M3`, recency is evaluated against the latest internationally published benchmark vintage $T \ge T_{\text{vendor\_max}} - 1$).*
3. **Non-Degeneracy**: $\sigma > 0$, distinct value count $\ge 2$, mode share $\le 0.95$.
4. **Dynamism**: Within-country variance $\sigma_{\text{within}} > 0$.

---

## 5. Master Macroeconomic Indicator Configurations (`MACRO_GAUGE_CONFIGS`)

The master configuration dictionary must be declared in [`etfportfolio/core/endpoints.py`](file:///Users/alex/Documents/etfportfolio/etfportfolio/core/endpoints.py) as `MACRO_GAUGE_CONFIGS`:

```python
MACRO_GAUGE_CONFIGS = {
    # =========================================================================
    # 1. WELL-BEING: HEALTH INDEX (Z-Score)
    # Calibrated to Ray Dalio GPI 2024 Appendix, p. 136 (exact weights & concepts)
    # =========================================================================
    "health_index": {
        "name": "Health Index",
        "type": "zscore",
        "indicators": {
            "SP.DYN.LE00.IN": {
                "name": "Life Expectancy At Birth",
                "weight": 0.300,
                "polarity": +1,
                "transform": "identity",
            },
            "SH.DYN.MORT": {
                "name": "Child Mortality Rate (per 1,000 live births)",
                "weight": 0.100,
                "polarity": -1,
                "transform": "identity",
            },
            "EN.ATM.PM25.MC.M3": {
                "name": "Air Pollution Exposure (PM2.5 mean annual)",
                "weight": 0.050,
                "polarity": -1,
                "transform": "identity",
            },
            "SH.H2O.BASW.ZS": {
                "name": "Pop Using Basic/Managed Drinking Water (%)",
                "weight": 0.050,
                "polarity": +1,
                "transform": "identity",
            },
            "SH.STA.BASS.ZS": {
                "name": "Pop With Basic Sanitation Facilities (%)",
                "weight": 0.050,
                "polarity": +1,
                "transform": "identity",
            },
            "SN.ITK.DEFC.ZS": {
                "name": "Pop Facing Food Insecurity / Undernourishment (%)",
                "weight": 0.150,
                "polarity": -1,
                "transform": "identity",
            },
            "VC.IHR.PSRC.P5": {
                "name": "Homicide Rate (per 100,000 people)",
                "weight": 0.075,
                "polarity": -1,
                "transform": "identity",
            },
            "VC.IDP.TOCV": {
                "name": "Pop Displaced By Conflict (% of total pop)",
                "weight": 0.075,
                "polarity": -1,
                "transform": "identity",
            },
            "SH.UHC.SRVS.CV.XD": {
                "name": "Access To Essential Health Services (UHC Index)",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    # =========================================================================
    # 2. DALIO'S 8 TRUE HEADLINE EMPIRE PILLARS (Report p. 135)
    # Evaluated across Total (Power/Scale) and Per Capita (Quality/Intensity)
    # =========================================================================
    "economic_output": {
        "name": "Economic Output Pillar",
        "pillar_weight": 0.200,
        "type": "percent_rank_dual",
        "indicators": {
            "NY.GDP.MKTP.PP.KD": {
                "name": "GDP, PPP constant int$ [Scale]",
                "weight": 0.450,
                "polarity": +1,
                "transform": "world_share",
            },
            "NY.GDP.MKTP.CD": {
                "name": "GDP, nominal US$ [Scale]",
                "weight": 0.250,
                "polarity": +1,
                "transform": "world_share",
            },
            "NY.GDP.PCAP.PP.KD": {
                "name": "GDP per capita, PPP [Quality]",
                "weight": 0.300,
                "polarity": +1,
                "transform": "log",
            },
        },
    },
    "trade_strength": {
        "name": "Trade Strength Pillar",
        "pillar_weight": 0.150,
        "type": "percent_rank_dual",
        "indicators": {
            "BX.GSR.GNFS.CD": {
                "name": "Exports of goods & services [Scale]",
                "weight": 0.450,
                "polarity": +1,
                "transform": "world_share",
            },
            "TX.VAL.MRCH.CD.WT": {
                "name": "Merchandise exports [Scale]",
                "weight": 0.250,
                "polarity": +1,
                "transform": "world_share",
            },
            "TX.MNF.TECH.ZS.UN": {
                "name": "Medium/high-tech exports (% mfg)",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
            "NE.TRD.GNFS.ZS": {
                "name": "Trade openness (% GDP)",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    "innovation_technology": {
        "name": "Innovation & Technology Pillar",
        "pillar_weight": 0.150,
        "type": "percent_rank_dual",
        "indicators": {
            "IP.PAT.RESD": {
                "name": "Patent applications, residents [Scale]",
                "weight": 0.300,
                "polarity": +1,
                "transform": "world_share",
            },
            "BX.GSR.ROYL.CD": {
                "name": "IP royalty receipts [Scale]",
                "weight": 0.200,
                "polarity": +1,
                "transform": "world_share",
            },
            "GB.XPD.RSDV.GD.ZS": {
                "name": "Research & development expenditure (% GDP)",
                "weight": 0.200,
                "polarity": +1,
                "transform": "identity",
            },
            "SP.POP.SCIE.RD.P6": {
                "name": "Researchers in R&D per million people",
                "weight": 0.150,
                "polarity": +1,
                "transform": "log",
            },
            "NV.MNF.TECH.ZS.UN": {
                "name": "High-tech manufacturing value added (% MVA)",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    "military_strength": {
        "name": "Military Strength Pillar",
        "pillar_weight": 0.125,
        "type": "percent_rank_dual",
        "indicators": {
            "MS.MIL.XPND.CD": {
                "name": "Military expenditure US$ [Scale]",
                "weight": 0.400,
                "polarity": +1,
                "transform": "world_share",
            },
            "MS.MIL.TOTL.P1": {
                "name": "Active armed forces personnel [Scale]",
                "weight": 0.250,
                "polarity": +1,
                "transform": "world_share",
            },
            "MS.MIL.XPND.GD.ZS": {
                "name": "Military expenditure (% of GDP)",
                "weight": 0.200,
                "polarity": +1,
                "transform": "identity",
            },
            "MS.MIL.MPRT.KD": {
                "name": "SIPRI conventional arms imports [Scale]",
                "weight": 0.150,
                "polarity": +1,
                "transform": "world_share",
            },
        },
    },
    "financial_center": {
        "name": "Markets & Financial Center Pillar",
        "pillar_weight": 0.100,
        "type": "percent_rank_dual",
        "indicators": {
            "CM.MKT.LCAP.CD": {
                "name": "Stock market capitalization [Scale]",
                "weight": 0.350,
                "polarity": +1,
                "transform": "world_share",
            },
            "CM.MKT.TRAD.CD": {
                "name": "Stock market trading volume [Scale]",
                "weight": 0.250,
                "polarity": +1,
                "transform": "world_share",
            },
            "BIS.FX_TURNOVER_SHARE": {
                "name": "Global OTC FX turnover share [Scale]",
                "weight": 0.200,
                "polarity": +1,
                "transform": "identity",
            },
            "FS.AST.PRVT.GD.ZS": {
                "name": "Domestic private credit (% GDP) [Depth]",
                "weight": 0.200,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    "education_strength": {
        "name": "Education Pillar",
        "pillar_weight": 0.100,
        "type": "percent_rank_dual",
        "indicators": {
            "SE.TER.ENRR": {
                "name": "Tertiary gross school enrollment (% pop)",
                "weight": 0.350,
                "polarity": +1,
                "transform": "identity",
            },
            "SE.SEC.ENRL": {
                "name": "Secondary school enrollment [Scale]",
                "weight": 0.250,
                "polarity": +1,
                "transform": "world_share",
            },
            "SE.PRM.ENRL": {
                "name": "Primary school enrollment [Scale]",
                "weight": 0.200,
                "polarity": +1,
                "transform": "world_share",
            },
            "HD.HCI.EYRS": {
                "name": "Expected years of schooling",
                "weight": 0.200,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    "competitiveness_culture_investment": {
        "name": "Competitiveness, Culture, & Investment Pillar",
        "pillar_weight": 0.100,
        "type": "percent_rank_dual",
        "indicators": {
            "NE.GDI.FTOT.ZS": {
                "name": "Gross capital formation (% GDP)",
                "weight": 0.300,
                "polarity": +1,
                "transform": "identity",
            },
            "NY.GNS.ICTR.ZS": {
                "name": "Gross national savings (% GDP)",
                "weight": 0.250,
                "polarity": +1,
                "transform": "identity",
            },
            "EIA.ENERGY.SELF_SUFF": {
                "name": "Primary energy net self-sufficiency ratio",
                "weight": 0.250,
                "polarity": +1,
                "transform": "identity",
            },
            "SL.TLF.CACT.ZS": {
                "name": "Labor force participation rate (%)",
                "weight": 0.200,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    "reserve_currency_status": {
        "name": "Reserve Currency Status Pillar",
        "pillar_weight": 0.075,
        "type": "percent_rank_dual",
        "indicators": {
            "IMF.COFER_SHARE": {
                "name": "Share of Allocated Global FX Reserves",
                "weight": 0.500,
                "polarity": +1,
                "transform": "identity",
            },
            "BIS.FX_TURNOVER_SHARE": {
                "name": "Share of Global FX Turnover",
                "weight": 0.300,
                "polarity": +1,
                "transform": "identity",
            },
            "BX.PEF.TOTL.CD.WD": {
                "name": "Portfolio equity and debt claims [Scale]",
                "weight": 0.200,
                "polarity": +1,
                "transform": "world_share",
            },
        },
    },
    # =========================================================================
    # 3. DALIO'S TOP TWO MACRO RISKS: INTERNAL & EXTERNAL CONFLICT
    # (Report Part 2, pp. 6-8, 133-134)
    # =========================================================================
    "internal_conflict": {
        "name": "Internal Order / Domestic Conflict Gauge",
        "type": "zscore",
        "indicators": {
            "VDEM.v2cacamps": {
                "name": "Political Polarization Index (V-Dem)",
                "weight": 0.300,
                "polarity": +1,
                "transform": "identity",
            },
            "VDEM.v2caviol": {
                "name": "Political Violence & Discontent (V-Dem)",
                "weight": 0.250,
                "polarity": +1,
                "transform": "identity",
            },
            "SI.POV.GINI": {
                "name": "Gini Index / Wealth & Income Gaps",
                "weight": 0.250,
                "polarity": +1,
                "transform": "identity",
            },
            "VDEM.v2clrspct": {
                "name": "Public Administration Impartiality Drag",
                "weight": 0.200,
                "polarity": -1,
                "transform": "identity",
            },
        },
    },
    "external_conflict": {
        "name": "External Great Power Conflict Gauge",
        "type": "zscore",
        "indicators": {
            "MS.MIL.XPND.GD.ZS": {
                "name": "Defense Spending as % of GDP",
                "weight": 0.400,
                "polarity": +1,
                "transform": "identity",
            },
            "TM.TAX.MRCH.WM.AR.ZS": {
                "name": "Tariff & Trade Barrier Intensity",
                "weight": 0.300,
                "polarity": +1,
                "transform": "identity",
            },
            "MS.MIL.MPRT.KD": {
                "name": "SIPRI Strategic Arms Inflows",
                "weight": 0.300,
                "polarity": +1,
                "transform": "world_share",
            },
        },
    },
    # =========================================================================
    # 4. ACTS OF NATURE / CLIMATE RESILIENCE (Report p. 133)
    # =========================================================================
    "acts_of_nature": {
        "name": "Acts of Nature & Climate Resilience Gauge",
        "type": "composite_score",
        "indicators": {
            "NDGAIN.INDEX": {"name": "ND-GAIN Country Index", "weight": 0.400, "polarity": +1, "transform": "identity"},
            "NDGAIN.READINESS": {
                "name": "Climate Adaptation Readiness",
                "weight": 0.300,
                "polarity": +1,
                "transform": "identity",
            },
            "NDGAIN.VULNERABILITY": {
                "name": "Climate Vulnerability Score",
                "weight": 0.300,
                "polarity": -1,
                "transform": "identity",
            },
        },
    },
    # =========================================================================
    # 5. 10-YEAR FORWARD REAL GDP GROWTH ENGINE SUB-COMPONENTS
    # Decomposition: 10Y Growth = Change in Workers + Growth Per Worker
    # Growth Per Worker = 65% Productivity (70% Arbitrage + 30% Culture) + 35% Indebtedness
    # (Report pp. 142-143, 148, 158, 169)
    # =========================================================================
    "growth_demographics": {
        "indicators": {
            "SP.POP.1564.TO.ZG": {
                "name": "Projected Working-Age Population Growth (15-64)",
                "weight": 1.000,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    "growth_productivity_arbitrage": {
        # "What You Pay vs. What You Get": Productivity relative to wages (Report pp. 158-165)
        "indicators": {
            "NY.GDP.PCAP.CD": {
                "name": "Nominal GDP per capita (Log Inverse Wages)",
                "weight": 0.350,
                "polarity": -1,
                "transform": "log",
            },
            "SL.GDP.PCAP.EM.KD": {
                "name": "Output per Person Employed (Productivity)",
                "weight": 0.350,
                "polarity": +1,
                "transform": "log",
            },
            "NE.GDI.FTOT.ZS": {
                "name": "Investment Rate (% GDP)",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
            "NY.GNS.ICTR.ZS": {
                "name": "Savings Rate (% GDP)",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    "growth_culture": {
        # Culture: Core dimensions (Corruption, Bureaucracy, Rule of Law, Achievement, Self-Sufficiency)
        "indicators": {
            "CC.EST": {"name": "Control of Corruption (WGI)", "weight": 0.200, "polarity": +1, "transform": "identity"},
            "VDEM.v2excrptps": {
                "name": "Public Sector Corruption Drag (V-Dem)",
                "weight": 0.150,
                "polarity": -1,
                "transform": "identity",
            },
            "RL.EST": {"name": "Rule of Law (WGI)", "weight": 0.200, "polarity": +1, "transform": "identity"},
            "VDEM.v2cltrnslw": {
                "name": "Transparent Law Enforcement (V-Dem)",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
            "RQ.EST": {
                "name": "Regulatory Burden / Bureaucracy (WGI)",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
            "SL.TLF.CACT.ZS": {
                "name": "Self-Sufficiency / Labor Ethic (%)",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    "growth_indebtedness_monetary": {
        # Indebtedness & Monetary Solvency (Report pp. 141-143)
        "indicators": {
            "IMF.GGXWDG_NGDP": {
                "name": "General Government Gross Debt (% GDP)",
                "weight": 0.300,
                "polarity": -1,
                "transform": "identity",
            },
            "IMF.GGXCNL_NGDP": {
                "name": "Government Net Lending/Deficit (% GDP)",
                "weight": 0.200,
                "polarity": +1,
                "transform": "identity",
            },
            "FS.AST.PRVT.GD.ZS": {
                "name": "Private Sector Debt Burden (% GDP)",
                "weight": 0.250,
                "polarity": -1,
                "transform": "identity",
            },
            "FS.AST.CGOV.GD.ZS": {
                "name": "Domestic Claims on Central Gov (% GDP)",
                "weight": 0.150,
                "polarity": -1,
                "transform": "identity",
            },
            "FP.CPI.TOTL.ZG": {
                "name": "Inflation / Monetary Instability (%)",
                "weight": 0.100,
                "polarity": -1,
                "transform": "identity",
            },
        },
    },
}
```

---

## 6. Macroeconomic Transformation Engine (`etfportfolio/prep/macro.py`)

A pure offline module `etfportfolio/prep/macro.py` implements all cross-sectional and time-series mathematical processing.

### 6.1 Mathematical Formulation

For indicator $i$, country $c$, and year $t$, raw observed value $x_{i,c,t}$ is transformed as follows:

#### 1. Indicator Transformations
* If `world_share`:
  $$s_{i,c,t} = \frac{x_{i,c,t}}{\sum_{k \in \text{All Countries}} x_{i,k,t}}$$
* If `log`:
  $$s_{i,c,t} = \ln(\max(x_{i,c,t}, 10^{-6}))$$
* If `identity`:
  $$s_{i,c,t} = x_{i,c,t}$$

#### 2. Cross-Sectional Winsorization & Z-Score Normalization
Outliers are clipped at the 2.5th and 97.5th percentiles across the active sovereign cross-section:
$$s'_{i,c,t} = \min(\max(s_{i,c,t}, Q_{0.025}(t)), Q_{0.975}(t))$$
$$z_{i,c,t} = \text{polarity}_i \times \frac{s'_{i,c,t} - \mu_{i,t}}{\sigma_{i,t}}$$

#### 3. Empirical Percentile Ranking (Empire Pillars)
In Dalio's master framework (Report p. 135), determinant scores are **empirical percentile ranks** from $0\%$ to $100\%$, not normal CDF probits:
$$R_{p,c,t} = \text{PercentileRank}\left( \sum_{i \in p} w_i \cdot z_{i,c,t} \right) \in [0.0, 1.0]$$
For every pillar $p$, both **Total** ($R^{\text{total}}_{p,c,t}$, based on absolute scale and world share) and **Per-Capita / Quality** ($R^{\text{per\_capita}}_{p,c,t}$, based on per-capita intensity and ratios) are computed.

#### 4. Headline Empire Strength Scores
Weighted sum across the 8 headline pillars:
$$\text{Strength}^{\text{total}}_{c,t} = \sum_{p=1}^{8} \text{PillarWeight}_p \times R^{\text{total}}_{p,c,t} \in [0.0, 1.0]$$
$$\text{Strength}^{\text{per\_capita}}_{c,t} = \sum_{p=1}^{8} \text{PillarWeight}_p \times R^{\text{per\_capita}}_{p,c,t} \in [0.0, 1.0]$$

#### 5. 10-Year Forward Real GDP Growth Engine
Following Dalio's exact two-component formula (Report pp. 142–143, 148):
$$\text{Expected 10Y Real Growth}_{c,t} = \Delta \text{Workers}_{c,t} + \text{Growth Per Worker}_{c,t}$$

1. **Expected Change in Workers ($\Delta \text{Workers}$)**:
   Directly equal to projected annual growth in the working-age population (15–64) from demographic data (`SP.POP.1564.TO.ZG`), winsorized to $[-1.5\%, +2.5\%]$.
2. **Growth Per Worker Potential**:
   $$\text{Productivity}_{c,t} = 0.70 \cdot Z_{\text{arbitrage}} + 0.30 \cdot Z_{\text{culture}}$$
   $$\text{Indebtedness}_{c,t} = Z_{\text{indebtedness\_monetary}}$$
   $$\text{Composite Potential}_{c,t} = \frac{2}{3} \text{Productivity}_{c,t} + \frac{1}{3} \text{Indebtedness}_{c,t}$$
   $$\text{Growth Per Worker (\%)} = \left[ 2.1\% + 1.8\% \times \text{Composite Potential}_{c,t} \right]_{[-1.0\%, +7.5\%]}$$

### 6.2 Benchmark Calibration Verification
To guarantee fidelity against Ray Dalio’s published 2024 index, `tests/prep/test_macro.py` must assert calibration within tolerance ($\pm 0.04$ on Strength, $\pm 0.3$ on Health Z-score, $\pm 0.4\%$ on 10Y Growth) against the published 2024 table (Report p. 3 / PDF p. 4):

| Country (ISO3) | Dalio Strength (Total) | Dalio Strength (Per Cap) | Dalio Health (Z) | Dalio 10Y Growth | Dalio $\Delta$Workers |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **USA** | **0.89** | **0.71** | **+1.33** | **1.4%** | +0.2% |
| **CHN** | **0.80** | **0.30** | **+1.27** | **4.0%** | -0.4% |
| **DEU** | **0.38** | **0.54** | **+2.06** | **-0.5%** | -1.1% |
| **JPN** | **0.33** | **0.40** | **+2.42** | **1.2%** | -0.8% |
| **IND** | **0.30** | **0.07** | **-1.41** | **6.3%** | +0.9% |
| **SGP** | **0.24** | **0.89** | **+2.45** | **2.6%** | -0.2% |

### 6.3 Module Interface with DuckDB
`etfportfolio/prep/macro.py` defines:
```python
def build_macro_gauges(conn: duckdb.DuckDBPyConnection) -> None:
    """Read bronze.country_indicators, compute all Dalio gauges, and materialize temp_macro_country_gauges."""
```
It materializes `temp_macro_country_gauges` in DuckDB with schema:
`(country_code VARCHAR, iso2_code VARCHAR, year INTEGER, gauge_name VARCHAR, value DOUBLE)`
and computes cross-sectional medians per `(year, gauge_name)` in `temp_macro_medians`.

---

## 7. Monthly Factor Panel Integration (`etfportfolio/prep/panel.py`)

### 7.1 Panel Alignment & Lookahead Elimination
For a monthly panel date `as_of_date` (e.g. `2024-03-31`), match against macro indicator year:
$$\text{indicator\_year} = \text{as\_of\_date.year} - 1$$
If a country has missing data for `year - 1`, fall back to the most recent observed year $\le \text{year} - 1$ (LOCF).

### 7.2 Bayesian Shrinkage to Neutral Median Prior
Let constituent country holdings for ETF $e$ at rebalance date $t$ be $w_{c,e,t}$ (where $\sum w_c \le 1.0$). For each macro gauge $M$:
$$M_{e,t} = \sum_{c \in C_e} w_{c,e,t} \times M_{c,t-1} + \left(1 - \sum_{c \in C_e} w_{c,e,t}\right) \times \text{Median}(M_{\cdot, t-1})$$

Neutral global benchmark medians:
* 8 Empire Pillars & Country Strength: $\text{Median} = 0.50$
* Health & Conflict Z-scores: $\text{Median} \approx 0.0$
* 10-Year Forward Real GDP Growth: $\text{Median} \approx 2.1\%$

### 7.3 Emitted Factor Features (18 Total)
The portfolio collapse emits 18 continuous scalar features under `family = 'macro'` directly into `silver.monthly_panel`:

| Feature ID | Type | Description |
| :--- | :--- | :--- |
| `macro_strength_total` | Float $[0, 1]$ | Portfolio Empire Score Total (Absolute Geopolitical & Economic Dominance) |
| `macro_strength_per_capita` | Float $[0, 1]$ | Portfolio Empire Score Per Capita (Productivity & Living Standards Intensity) |
| `macro_health_score_z` | Float (Z) | Portfolio Health Z-Score (9-Factor Dalio Longevity, Sanitation, Healthcare) |
| `macro_internal_conflict_z` | Float (Z) | Domestic Political Polarization & Strife Risk (Dalio Near-Term Risk #1) |
| `macro_external_conflict_z` | Float (Z) | International Great Power Conflict & Tariff Risk (Dalio Near-Term Risk #2) |
| `macro_future_10y_growth` | Float (%) | Expected 10-Year Forward Real GDP Growth Rate |
| `macro_working_age_growth` | Float (%) | Expected 10-Year Working-Age Population Growth Rate ($\Delta \text{Workers}$) |
| `macro_pillar_economic_output` | Float $[0, 1]$ | Economic Output Scale & Living Standards Percent Rank |
| `macro_pillar_trade` | Float $[0, 1]$ | Export Dominance, High-Tech Share & Trade Openness Percent Rank |
| `macro_pillar_innovation` | Float $[0, 1]$ | Patents, R&D Spending, Researchers & Tech Manufacturing Percent Rank |
| `macro_pillar_military` | Float $[0, 1]$ | Military Expenditure, Personnel & Strategic Capabilities Percent Rank |
| `macro_pillar_financial` | Float $[0, 1]$ | Stock Market Capitalization, Volume, FX Turnover & Depth Percent Rank |
| `macro_pillar_education` | Float $[0, 1]$ | Tertiary Enrollment, Pupil Volume & Expected Schooling Percent Rank |
| `macro_pillar_competitiveness_investment`| Float $[0, 1]$ | Capital Formation, National Savings, Energy Self-Sufficiency Percent Rank |
| `macro_pillar_reserve_currency`| Float $[0, 1]$ | Global FX Reserve Share (IMF COFER) & FX Turnover Currency Dominance |
| `macro_gauge_acts_of_nature` | Float (Score)| Climate Vulnerability & Disaster Readiness Adaptation Score (ND-GAIN) |
| `macro_country_coverage` | Float $[0, 1]$ | Diagnostic scalar: $\sum w_c$ (fraction of portfolio in vetted countries) |
| `macro_unclassified_weight`| Float $[0, 1]$ | Diagnostic scalar: $1.0 - \sum w_c$ (fraction shrunk to neutral median) |

### 7.4 Deletions & Cleanups in `panel.py`
1. **Remove `product_universe`**: Delete `CREATE OR REPLACE TEMP TABLE product_universe` and its join in Step 8.
2. **Exclude `country` from Panel Inserts**: Do not include `'country'` in `canonical_universe` or `default0_obs`. Zero raw `country_*` columns are emitted into `silver.monthly_panel`.
3. **Preserve `country` in Extraction**: Keep `'country'` in `DEFAULT_ZERO_FAMILIES` in `core/endpoints.py` so holdings extraction simplex normalisation in `prep/pipeline.py` and `extractors.py` continues functioning as designed.

---

## 8. Summary of File Modifications & Additions

| File | Status | Core Responsibility |
| :--- | :--- | :--- |
| [`etfportfolio/core/schema.sql`](file:///Users/alex/Documents/etfportfolio/etfportfolio/core/schema.sql) | Modify | Add DDL for `bronze.country_indicators`, `bronze.country_indicators_status`, and `cold_storage.country_indicators`. |
| [`etfportfolio/core/endpoints.py`](file:///Users/alex/Documents/etfportfolio/etfportfolio/core/endpoints.py) | Modify | Add `'macro'` to `SCALAR_FAMILIES`; declare `MACRO_GAUGE_CONFIGS` and `RESERVE_CURRENCY_SHARES`. |
| [`etfportfolio/ingest/country_indicators.py`](file:///Users/alex/Documents/etfportfolio/etfportfolio/ingest/country_indicators.py) | **Create** | Programmatic fetchers (WB, IMF WEO, EIA, V-Dem, ND-GAIN), COFER reference ingestion, vetting gates, freshness caching, cold storage restatements. |
| [`etfportfolio/ingest/pipeline.py`](file:///Users/alex/Documents/etfportfolio/etfportfolio/ingest/pipeline.py) | Modify | Add `country_indicators` sync phase and CLI entry point. |
| [`etfportfolio/prep/macro.py`](file:///Users/alex/Documents/etfportfolio/prep/macro.py) | **Create** | Total vs. Per-Capita Empire Scores, empirical percentile ranking, 9-indicator Health Index, Conflict Gauges, 10Y Growth Engine, medians. |
| [`etfportfolio/prep/panel.py`](file:///Users/alex/Documents/etfportfolio/prep/panel.py) | Modify | Call `macro.py`; join spine with unpruned country weights + median shrinkage; emit 18 `macro_*` features; delete `product_universe`. |
| [`tests/ingest/test_country_indicators.py`](file:///Users/alex/Documents/etfportfolio/tests/ingest/test_country_indicators.py) | **Create** | Unit tests for country resolver, mock API responses, cold storage triggers, and gating errors. |
| [`tests/prep/test_macro.py`](file:///Users/alex/Documents/etfportfolio/tests/prep/test_macro.py) | **Create** | Unit tests validating Ray Dalio benchmark calibration across USA, CHN, DEU, JPN, IND, SGP. |
| [`tests/prep/test_panel.py`](file:///Users/alex/Documents/etfportfolio/tests/prep/test_panel.py) | Modify | Unit tests verifying absence of raw `country_*` features, median prior shrinkage, and presence of 18 `macro_*` features. |
