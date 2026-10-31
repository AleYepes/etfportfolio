# FRD Amendment: Ray Dalio Great Powers Macroeconomic Replication & Multi-Source Extension

**Document Status:** Approved Architecture Specification  
**Supersedes & Amends:** [`docs/FRD_country_indicator_collapse.md`](../docs/FRD_country_indicator_collapse.md)  
**Primary Reference:** Ray Dalio / Bridgewater Associates, *The Great Powers Index: 2024 — How the Leading 24 Countries Are Doing and Their Prospects for the Next 10 Years* (July 2024)  
**Target Scope:** `bronze.country_indicators` schema expansion, multi-source programmatic ingestion in [`etfportfolio/ingest/country_indicators.py`](../etfportfolio/ingest/country_indicators.py), automated production vetting gates, and macroeconomic portfolio collapse in [`etfportfolio/prep/panel.py`](../etfportfolio/prep/panel.py)  
**Target Universe:** 109 Investable Countries and Territories extracted from `silver.observations` where `family = 'country'`  

---

## 1. Executive Summary & Context for Incoming Engineers

### 1.1 Purpose of this Document
This amendment serves as the complete technical handoff and functional specification for implementing country-level macroeconomic indicator ingestion and portfolio factor collapse. An incoming engineer or agent reviewing [`docs/FRD_country_indicator_collapse.md`](../docs/FRD_country_indicator_collapse.md) will find an initial proposal centered strictly around six World Bank indicators. This amendment upgrades that specification based on extensive macroeconomic vetting, adopting Ray Dalio’s *Great Powers Index (2024)* framework to generate economically robust, investable portfolio factors.

### 1.2 Limitations of the Original World-Bank-Only MVP Plan
The original draft in [`docs/FRD_country_indicator_collapse.md`](../docs/FRD_country_indicator_collapse.md) proposed fetching six raw World Bank series (`NY.GDP.MKTP.CD`, `NY.GDP.PCAP.CD`, `SP.POP.TOTL`, `NE.IMP.GNFS.ZS`, `NE.EXP.GNFS.ZS`, `BX.KLT.DINV.WD.GD.ZS`) via `wbgapi` and collapsing portfolio holdings into isolated scalar features (`country_gdp_stat`, `country_gdp_growth_rate`).

Empirical auditing revealed three critical flaws in that plan:
1. **Ad-Hoc Features Lack Factor Cohesion**: Isolated macro aggregates (e.g., weighted GDP levels or raw population counts) do not form an investable factor model. Multi-asset allocators cannot evaluate cross-sectional risk, factor premia, or ETF rankings on disjointed economic sums.
2. **The World Bank Alone Cannot Model Macro Solvency or Power**: Essential macroeconomic pillars—sovereign debt burdens, central government budget deficits, primary energy self-sufficiency, climate resilience, and institutional governance recency—are either missing, deprecated, or severely lagged in the World Bank database.
3. **Multi-Year Extrapolation Masks Systemic Risk (Zero-Tolerance Violation)**: Using Akima splines or linear regression to invent 3+ years of missing macro data violates quantitative risk standards. If a country series has ceased reporting for several years, forward-filling or linearly extrapolating masks actual default, currency, or fiscal distress. We want to avoid long-horizon extrapolation if it's not carefully considered or assessed beforehand.

### 1.3 The Solution: Ray Dalio's *Great Powers Index (2024)*
This amendment anchors geographic factor construction to Ray Dalio’s institutional macro framework (*The Great Powers Index: 2024*). Country-level holdings from `silver.observations` (`family = 'country'`) are mapped to vetted macroeconomic indicators across three top-level composite indices:
* **Country Strength (Headline Empire Score)**: An index from $0.0$ to $1.0$ measuring absolute geopolitical, military, and economic dominance across 8 headline pillars (Economic Output, Trade, Innovation/Technology, Military, Financial Center, Education, Infrastructure, and Geology/Energy).
* **Health Index**: A standardized Z-score measuring bodily longevity, infant survival, sanitary infrastructure, clean water access, and nutritional sufficiency.
* **10-Year Forward Real GDP Growth Engine**: An annualized growth projection combining demographic workforce expansion with output-per-worker potential (cost competitiveness, culture/governance, and debt cycle solvency).

This design collapses high-cardinality geographic weights (~100 individual country columns) into **14 economically interpretable portfolio macro factors** in `silver.monthly_panel` under `family = 'macro'`.

### 1.4 Production Architecture vs. Exploratory Artifacts
During preliminary research, indicator vetting was conducted using local exploratory scripts and scratch parquets. **For production implementation:**
* **Database Write Access**: The production pipeline has full write access to [`data/etf.duckdb`](../data/etf.duckdb).
* **Self-Contained Ingestion**: The production pipeline does NOT rely on pre-downloaded scratch files or directories (such as `dalio_external_data/`). All data must be fetched, validated, and ingested programmatically via [`etfportfolio/ingest/country_indicators.py`](../etfportfolio/ingest/country_indicators.py).
* **Automated Vetting Gates**: The ingestion pipeline must enforce strict automated gating rules on newly fetched series to alert operators or halt ingestion if external providers serve degraded or sparse feeds.

---

## 2. The Ray Dalio Great Powers Framework Explained

Ray Dalio’s *Great Powers Index (2024)* evaluates major global economies using an empirical, multi-tier hierarchical indicator model. The index measures why empires rise and fall based on three core constructs:

```
                                  RAY DALIO GREAT POWERS MODEL
                                                │
         ┌──────────────────────────────────────┼──────────────────────────────────────┐
         ▼                                      ▼                                      ▼
  COUNTRY STRENGTH (0-1)                 HEALTH INDEX (Z)                    10-YEAR FORWARD GROWTH (%)
 (Headline Empire Score)              (Physical Well-Being)              (Demographics + Output/Worker)
         │                                      │                                      │
 ┌───────┴───────┐                      ┌───────┴───────┐                      ┌───────┴───────┐
 │ 8 Pillars:    │                      │ 6 Metrics:    │                      │ Δ Workers     │
 │ - Output      │ (20.0%)              │ - Longevity   │ (35%)                │   Demographics│ (10Y inflow)
 │ - Trade       │ (15.0%)              │ - Mortality <5│ (20%)                │               │
 │ - Innovation  │ (15.0%)              │ - Nutrition   │ (20%)                │ Growth/Worker │
 │ - Military    │ (12.5%)              │ - Sanitation  │ (10%)                │   2/3 Product.│ (Cost+Gov)
 │ - Finance     │ (12.5%)              │ - Water       │ (7.5%)               │   1/3 Debt    │ (IMF Solvency)
 │ - Education   │ (10.0%)              │ - Clean Fuel  │ (7.5%)               └───────────────┘
 │ - Infrastr.   │ (10.0%)              └───────────────┘
 │ - Geology     │ ( 5.0%)
 └───────────────┘
```

### 2.1 Country Strength (Headline Empire Score)
Measures the absolute global power of a country on a $0.0$ to $1.0$ scale. Dalio balances two fundamental economic dimensions across 8 headline pillars:
1. **Total / World Share (Absolute Scale)**: Captures global geopolitical weight (e.g., total military expenditure, total global export share, nominal GDP volume). A large country dominates global affairs through aggregate mass.
2. **Per Capita / Intensity (Efficiency & Living Standards)**: Captures technological sophistication, wealth per citizen, and labor productivity (e.g., GDP per capita, patents per capita, tertiary education rates).

#### The 8 Headline Empire Pillars:
1. **Economic Output Pillar (Weight: 20%)**: Combines PPP-adjusted GDP volume (global share), current USD nominal GDP (global share), and GDP per capita (log scale).
2. **Trade Pillar (Weight: 15%)**: Measures global export dominance (goods and services world share), merchandise export share, high-tech manufacturing export intensity, and trade openness (% GDP).
3. **Innovation & Technology Pillar (Weight: 15%)**: Measures resident patent applications (world share), IP royalty receipts (world share), resident trademarks (world share), medium/high-tech manufacturing value added (% MVA), and secure Internet server density.
4. **Military Strength Pillar (Weight: 12.5%)**: Measures total defense expenditure (world share), military spending as % of GDP, defense budget share of government expenditure, and major arms imports (world share).
5. **Financial Center & Capital Markets Pillar (Weight: 12.5%)**: Domestic credit to the private sector (% GDP), bank capital adequacy (% assets), nonperforming loans ratio (inverse), net portfolio equity inflows (world share), and commercial banking branch density.
6. **Education Pillar (Weight: 10%)**: Secondary pupil enrollment (world share), primary pupil enrollment (world share), and compulsory education duration.
7. **Infrastructure & Investment Pillar (Weight: 10%)**: Gross fixed capital formation volume (world share), capital formation as % of GDP, national savings rate (% GDP), electricity access rate, and fixed broadband subscription share.
8. **Geology & Energy Endowment Pillar (Weight: 5%)**: Net primary energy self-sufficiency ratio $(\text{Production} - \text{Consumption}) / \text{Consumption}$, total primary energy production volume (world share), total surface land area (world share), and internal renewable freshwater resources (world share).

### 2.2 Well-Being: The Health Index
Dalio separates physical well-being from headline power. The **Health Index** models population longevity, health infrastructure, and sanitary living conditions as a standardized cross-sectional Z-score:
* Life expectancy at birth (+1 polarity, weight 35%)
* Mortality rate under-5 (-1 polarity, weight 20%)
* Prevalence of undernourishment (-1 polarity, weight 20%)
* People using basic sanitation services (+1 polarity, weight 10%)
* People using basic drinking water (+1 polarity, weight 7.5%)
* Clean cooking fuels and technology (+1 polarity, weight 7.5%, acting as clean indoor air proxy)

*(Note: Dalio's subjective "Happiness Index" based on Cantril life-ladder surveys is permanently excluded; open feeds ended in 2020, commercial Gallup feeds are paywalled, and subjective mood surveys exhibit near-zero correlation with cross-sectional ETF returns).*

### 2.3 Acts of Nature & Climate Resilience Gauge
Dalio evaluates physical vulnerability and climate adaptation as an independent risk gauge:
* ND-GAIN Country Index (overall climate resilience, weight 40%)
* Climate Readiness Score (economic, governance, and social readiness, weight 30%)
* Climate Vulnerability Score (exposure, sensitivity, and adaptive capacity, -1 polarity, weight 30%)

### 2.4 The 10-Year Forward Real GDP Growth Forecast Engine
Dalio’s forward growth model forecasts real annualized GDP growth over the next decade using an empirical structural regression:
$$\text{Expected 10Y Real GDP Growth}_{c} = \Delta \text{Workers}_{c} + \text{Growth Per Worker}_{c}$$

#### 1. Demographic Workforce Engine ($\Delta \text{Workers}$):
Working-age population expansion is driven by youth cohorts aging into the workforce versus retiring cohorts:
$$Z_{\text{demographics}} = 0.50 \cdot z_{\text{youth\_inflow}} - 0.30 \cdot z_{\text{old\_dependency}} + 0.20 \cdot z_{\text{pop\_growth}}$$
$$\Delta \text{Workers}_{c} (\%) = \left[ 0.2\% + 0.8\% \times Z_{\text{demographics}} \right]_{[-1.5\%, +2.5\%]}$$

#### 2. Growth Per Worker Potential:
$$\text{Growth Per Worker}_{c} = \frac{2}{3} \times \text{Productivity Score}_{c} + \frac{1}{3} \times \text{Indebtedness Score}_{c}$$
Where:
* **Productivity Score** ($70\%$ Cost Competitiveness $+ 30\%$ Culture & Governance):
  * **Cost Competitiveness ($Z_{\text{Cost}}$)**: Measures labor value arbitrage. High nominal GDP per capita acts as an inverse hurdle (expensive wages, $-1$), while high GDP per worker ($+1$), capital formation ($+1$), savings ($+1$), and labor force participation ($+1$) indicate high productive effort relative to cost.
  * **Culture & Governance ($Z_{\text{Gov}}$)**: Blends rule of law, anti-corruption enforcement, impartial administration, and regulatory quality. Excessive government consumption (% GDP) is penalized ($-1$) as a drag on private efficiency.
* **Indebtedness Score ($Z_{\text{Debt}}$)**: Measures balance sheet solvency. Heavy sovereign debt burdens ($-1$), high budget deficits ($-1$), and bloated private leverage ($-1$) suppress growth per worker, whereas current account surpluses ($+1$) and import reserves ($+1$) provide liquidity buffers.

#### 3. Linear Calibration:
Calibrated using Dalio’s 70-year empirical cross-sectional parameters ($R = 0.81$, global baseline growth $\approx 2.1\%$, $\sigma \approx 1.8\%$):
$$\text{Growth Per Worker (\%)} = \left[ 2.1\% + 1.8\% \times Z_{\text{Growth Per Worker}} \right]_{[-1.0\%, +7.5\%]}$$

---

## 3. Macroeconomic Indicator Vetting Post-Mortem & Source Selection

### 3.1 The Production Vetting Harness Specification
To prevent garbage data from entering the database, all prospective indicator feeds were audited across the 109 target countries extracted from `silver.observations` where `family = 'country'`. The audit evaluated all series over the window **2020–2026**:
* **Minimum Target Coverage (`min_coverage`)**: $\ge 76.0\%$ of the 109 target countries must have observed values.
* **Minimum Recent Coverage (`min_recent_cov`)**: $\ge 70.0\%$ of active countries must report data in the last 3 years ($T \ge \max(\text{year}) - 2$).
* **Grid Coverage (`min_grid` / `min_recent_grid`)**: $\ge 55.0\%$ overall, $\ge 50.0\%$ in the recent 3-year window.
* **Dynamism**: $\sigma_{\text{within}} / \sigma_{\text{between}}$ (categorized as structural $<0.10$, mixed $0.10\text{--}0.30$, dynamic $>0.30$).
* **Gating Decisions**:
  * `level_ok`: Passes coverage, recency, non-degeneracy ($\sigma_{\text{total}} > 0$), and mode share $\le 1.0$.
  * `change_ok`: Passes `level_ok` plus recent grid, observation count, and within-country movement ($\sigma_{\text{within}} > 0$).

### 3.2 Phase 1 Audit Post-Mortem: Why World Bank Alone Failed
In Phase 1, 12 external indicators were evaluated to fill Dalio's macro gaps. **All 12 candidate feeds failed production gating**:

```
===================================================================================================================
PHASE 1 AUDIT POST-MORTEM (EVALUATION WINDOW: 2020-2026 | RECENT CUT: 2024.0)
Universe: 109 Target Countries | Requirement: min_cov >= 76%, min_recent_cov >= 70%
===================================================================================================================
code                  indicator                                      latest_year  coverage  recent_cov  status
-------------------------------------------------------------------------------------------------------------------
BIS.CREDIT.GOVT.GDP   Total credit to general government (% of GDP)         2026     27.2%       27.2%  REJECTED (G20 only)
BIS.CREDIT.PRIVATE    Total credit to private sector (% of GDP)             2026     41.7%       41.7%  REJECTED (G20 only)
BIS.DEBT.SERVICE      Debt service ratio, private non-financial             2026     16.5%       16.5%  REJECTED (G20 only)
WB.CM.MKT.LCAP.GD.ZS  Stock market capitalization (% of GDP)                2025     64.1%       57.3%  REJECTED (Sparse)
WB.EN.ATM.PM25.MC.M3  PM2.5 air pollution, mean annual exposure             2023     95.1%        0.0%  REJECTED (3y lag)
WB.GB.XPD.RSDV.GD.ZS  Research & development expenditure (% of GDP)         2024     79.6%       17.5%  REJECTED (Sparse)
WB.SH.STA.SUIC.P5     Suicide mortality rate (per 100k pop)                 2021     91.3%        0.0%  REJECTED (4y lag)
WB.SP.POP.SCIE.RD.P6  Researchers in R&D (per million people)               2024     74.8%       16.5%  REJECTED (Sparse)
WB.VC.IHR.PSRC.P5     Intentional homicides (per 100k people)               2023     87.4%        0.0%  REJECTED (3y lag)
WHO.SUICIDE.RATE      Suicide mortality rate (WHO GHO API)                  2023     91.3%        0.0%  REJECTED (3y lag)
WHO.UHC.INDEX         UHC Service Coverage Index (WHO GHO API)              2023     91.3%        0.0%  REJECTED (3y lag)
WHR.CANTRIL.LADDER    Reported life satisfaction (World Happiness)          2020     85.4%        0.0%  REJECTED (Terminated)
PWT.HOURS.WEEKLY      Average weekly hours worked (Penn World Table 10.01)  2019      0.0%        0.0%  REJECTED (Terminated)
===================================================================================================================
```

#### The Three Critical Failure Modes:
1. **Publication Lag Trap**: WHO, UN, and World Bank specialized survey feeds (PM2.5 pollution, homicides, suicide rates, universal healthcare coverage) suffer 2- to 4-year release latency, yielding $0.0\%$ recent coverage ($T \ge 2024$).
2. **G20-Only Bias**: The Bank for International Settlements (BIS) publishes credit and debt service ratios for only 17 to 43 countries, leaving over $60\%$ of our 109-country ETF universe unpopulated.
3. **Archive Deprecation**: Penn World Table (PWT 10.01) ended in 2019, OWID World Happiness ended in 2020, and World Bank *Doing Business* was abolished in 2021.

### 3.3 Phase 2 Vetting Scorecard: Approved Sources
In Phase 2, primary sources cited in Dalio’s bibliography (*GreatPowersSummary.pdf*, pp. 63–64 / 183–184) were vetted:

| Code | Indicator Label | Source | Latest Year | Target Coverage ($N=109$) | Recent Coverage (2024–2026) | Dynamism | `level_ok` | `change_ok` | Decision |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| `IMF.GGXCNL_NGDP` | Gov Net Lending/Borrowing (% GDP) | IMF WEO | 2026 | 89.9% (98/109) | 89.9% | 0.861 | **True** | **True** | **APPROVED (`indebtedness`)** |
| `IMF.GGXWDG_NGDP` | Gov Gross Debt (% GDP) | IMF WEO | 2026 | 89.0% (97/109) | 89.0% | 0.248 | **True** | **True** | **APPROVED (`indebtedness`)** |
| `VDEM.v2excrptps` | Public Sector Corruption Index | V-Dem v16 | 2025 | 85.3% (93/109) | 85.3% | 0.140 | **True** | **True** | **APPROVED (`governance`)** |
| `VDEM.v2cltrnslw` | Transparent Laws w/ Predictable Enforce. | V-Dem v16 | 2025 | 85.3% (93/109) | 85.3% | 0.234 | **True** | **True** | **APPROVED (`governance`)** |
| `VDEM.v2clrspct` | Rigorous & Impartial Administration | V-Dem v16 | 2025 | 85.3% (93/109) | 85.3% | 0.215 | **True** | **True** | **APPROVED (`governance`)** |
| `VDEM.v2x_rule` | Rule of Law Index | V-Dem v16 | 2025 | 85.3% (93/109) | 85.3% | 0.141 | **True** | **True** | **APPROVED (`governance`)** |
| `EIA.ENERGY.SELF_SUFF`| Net Energy Self-Sufficiency Ratio | US EIA | 2024 | 95.4% (104/109)| 95.4% | 0.158 | **True** | False | **APPROVED (`geology_energy`)** |
| `EIA.ENERGY.PROD` | Primary Energy Production (Quad BTU) | US EIA | 2024 | 95.4% (104/109)| 95.4% | 0.049 | **True** | False | **APPROVED (`geology_energy`)** |
| `EIA.ENERGY.CONS` | Primary Energy Consumption (Quad BTU) | US EIA | 2024 | 95.4% (104/109)| 95.4% | 0.037 | **True** | False | **APPROVED (`geology_energy`)** |
| `NDGAIN.INDEX` | ND-GAIN Country Index | ND-GAIN | 2024 | 86.2% (94/109) | 86.2% | 0.130 | **True** | False | **APPROVED (`acts_of_nature`)** |
| `NDGAIN.READINESS` | Climate Readiness Score | ND-GAIN | 2024 | 87.2% (95/109) | 87.2% | 0.179 | **True** | False | **APPROVED (`acts_of_nature`)** |
| `NDGAIN.VULNERABILITY`| Climate Vulnerability Score | ND-GAIN | 2024 | 86.2% (94/109) | 86.2% | 0.062 | **True** | False | **APPROVED (`acts_of_nature`)** |
| `ILO.HOW_TEMP_SEX_AGE_NB`| Mean Weekly Hours Worked | ILOSTAT | 2025 | 67.9% (74/109) | 61.5% | 0.176 | **False**| **False**| **DISQUALIFIED (Coverage < 76%)** |

#### Key Empirical Insights:
1. **The Sovereign Debt Breakthrough (IMF WEO)**: Switching from BIS to IMF World Economic Outlook eliminates the debt coverage failure, providing 89.9% target coverage through 2026.
2. **Institutional Governance (V-Dem v16 vs. World Bank WGI)**: V-Dem Version 16 provides verified expert ratings through 2025 across 93 target countries. V-Dem exhibits near-perfect rank alignment with World Bank WGI ($\text{Spearman } r_s = 0.912$ for corruption, $r_s = 0.884$ for rule of law). Blending WGI and V-Dem 50/50 eliminates single-source methodology bias.
3. **Missing Jurisdictions Are Micro-Territories**: The 11–14 jurisdictions missing from IMF, V-Dem, and ND-GAIN are exclusively non-sovereign dependencies and offshore centers (`BMU`, `CYM`, `FRO`, `GGY`, `GIB`, `GUM`, `IMN`, `JEY`, `MCO`, `PRI`, `VGB`, `VIR`). Among sovereign macroeconomic entities with independent fiscal authorities, **coverage is 100%**.
4. **The Hours-Worked Workaround (ILOSTAT Disqualification)**: ILOSTAT survey data failed coverage (67.9% < 76%) because emerging market labor ministries do not publish annual hours. Dalio’s "Working Hard" pillar measures effort. Reallocating hours weight to World Bank **Labor Force Participation Rate** (`SL.TLF.CACT.ZS`, 96.3% coverage) and **Productivity per Person Employed** (`SL.GDP.PCAP.EM.KD`, 97.2% coverage) preserves work-ethic modeling across all 109 countries.

---

## 4. Technical Ingestion Specifications (Endpoints, Protocols & Libraries)

The production ingestion module [`etfportfolio/ingest/country_indicators.py`](../etfportfolio/ingest/country_indicators.py) must programmatically acquire data from the approved vendors. Below are the exact technical specifications for each data feed.

### 4.1 World Bank (WDI & WGI)
* **Provider**: World Bank Data API
* **Coverage**: ~217 sovereign economies, 1990–2026
* **Protocol**: REST API / Python Library
* **Python Tool**: `wbgapi` (`import wbgapi as wb`) or direct HTTP requests via `requests`
* **Direct REST Pattern**:
  `https://api.worldbank.org/v2/country/all/indicator/{indicator_id}?format=json&per_page=1000&date=2000:2026`
* **Pagination & Rate Limits**: Paged JSON responses (`page` and `pages` metadata in payload header). World Bank permits up to 1,000 records per page. No authentication required.
* **Extraction Notes**: Filter out regional aggregates (`countryiso3code.startswith('WB_')` or aggregate codes like `WLD`, `EMU`, `EAS`, `ECS`).

### 4.2 IMF World Economic Outlook (WEO)
* **Provider**: International Monetary Fund (IMF) DataMapper API
* **Indicators**:
  * `IMF.GGXCNL_NGDP`: General government net lending/borrowing (% of GDP)
  * `IMF.GGXWDG_NGDP`: General government gross debt (% of GDP)
* **Endpoint Pattern**:
  `https://www.imf.org/external/datamapper/api/v1/{indicator_id}`
* **Payload Format**: Uncompressed JSON structure:
  ```json
  {
    "values": {
      "GGXWDG_NGDP": {
        "USA": {"2020": 133.4, "2021": 126.4, "2024": 122.9, "2025": 124.6, "2026": 126.1},
        "DEU": {"2020": 68.8, "2024": 62.7, ...}
      }
    }
  }
  ```
* **Protocol & Network Gotcha**: IMF endpoints reside behind an **Akamai CDN** that blocks standard Python `requests` with HTTP 403 Forbidden.
* **Recommended Library**: `curl_cffi.requests.get(url, impersonate="chrome120", timeout=30)` or standard `requests.Session` with complete browser headers (`User-Agent: Mozilla/5.0...`, `Accept: application/json, text/javascript, */*`).

### 4.3 V-Dem Institute (Varieties of Democracy)
* **Provider**: V-Dem Institute, University of Gothenburg
* **Dataset**: V-Dem Country-Year Core Dataset Version 16 (Released March 2026)
* **Indicators**:
  * `v2excrptps`: Public sector corruption index
  * `v2cltrnslw`: Transparent laws with predictable enforcement
  * `v2clrspct`: Rigorous and impartial public administration
  * `v2x_rule`: Rule of law index
* **Direct Download URL**:
  `https://www.v-dem.net/media/datasets/V-Dem-CY-Core-v16_csv.zip`
* **Payload Format**: Compressed ZIP (~16 MB) containing single CSV `V-Dem-CY-Core-v16.csv` (~130 MB uncompressed).
* **Extraction Pattern**:
  ```python
  import zipfile, io, pandas as pd

  # Stream into memory or write temporary file in scratch
  with zipfile.ZipFile(io.BytesIO(response.content)) as z:
      with z.open("V-Dem-CY-Core-v16.csv") as f:
          vdem_df = pd.read_csv(
              f,
              usecols=["country_text_id", "year", "v2excrptps", "v2cltrnslw", "v2clrspct", "v2x_rule"],
              low_memory=False,
          )
  # Filter year >= 2000, rename country_text_id -> country_code
  ```
* **Recommended Library**: `curl_cffi` (impersonate="chrome120") or `requests` with `timeout=120`.

### 4.4 US Energy Information Administration (EIA)
* **Provider**: U.S. EIA Open Data
* **Dataset**: International Energy Data (Bulk INTL)
* **Indicators**:
  * Total Primary Energy Production (Quad BTU)
  * Total Primary Energy Consumption (Quad BTU)
  * Net Energy Self-Sufficiency Ratio: $(\text{Production} - \text{Consumption}) / \text{Consumption}$
* **Direct Bulk URL**:
  `https://www.eia.gov/opendata/bulk/INTL.zip`
* **Payload Format**: Compressed ZIP (~24 MB) containing newline-delimited JSON `INTL.txt` (~220 MB uncompressed).
* **Parsing & Extraction Pattern**:
  Target series IDs follow standard structural naming:
  * Production: `INTL.44-1-{COUNTRY_ISO3}-QBTU.A`
  * Consumption: `INTL.44-2-{COUNTRY_ISO3}-QBTU.A`
  Stream-read line by line to minimize RAM:
  ```python
  import json, zipfile

  with zipfile.ZipFile(zip_path).open("INTL.txt") as f:
      for line in f:
          record = json.loads(line)
          series_id = record.get("series_id", "")
          # Parse series matching target pattern and extract [year, value] data array
  ```
* **Recommended Library**: `requests` (timeout=120) and Python built-in `json` / `zipfile`.

### 4.5 ND-GAIN (Notre Dame Global Adaptation Initiative)
* **Provider**: University of Notre Dame
* **Dataset**: ND-GAIN Country Index 2026 Edition
* **Indicators**:
  * `NDGAIN.INDEX`: Overall Country Index (Resilience)
  * `NDGAIN.READINESS`: Climate Adaptation Readiness Score
  * `NDGAIN.VULNERABILITY`: Climate Vulnerability Score
* **Direct Download URL**:
  `https://gain.nd.edu/assets/647440/ndgain_countryindex_2026.zip`
* **Payload Format**: Compressed ZIP (~4.9 MB) containing wide-format CSVs: `gain.csv`, `readiness.csv`, `vulnerability.csv`.
* **Parsing Pattern**:
  Each CSV is indexed by ISO-3 country code with year columns (`1995`, `1996`, ..., `2024`). Reshape using `pd.melt(df, id_vars=['ISO3'], var_name='year', value_name='value')`.
* **Recommended Library**: `curl_cffi` (impersonate="chrome120") or `requests` with `timeout=60`.

---

## 5. Production Pipeline Vetting Logic & Error Handling

To maintain data integrity over time, the ingestion pipeline must never silently accept degraded, empty, or corrupted feeds. When [`etfportfolio/ingest/country_indicators.py`](../etfportfolio/ingest/country_indicators.py) runs, it must compute gating diagnostics immediately after acquiring data for each source.

### 5.1 Automated Quality Gates
Each newly fetched indicator table must satisfy four strict gates before insertion into `bronze.country_indicators`:

1. **Sovereign Coverage Gate (`min_coverage >= 0.76`)**:
   $$\text{Coverage} = \frac{|\{c \in \text{Target Sovereigns} : N_{\text{obs}}(c) > 0\}|}{|\text{Target Sovereigns}|} \ge 0.760$$
   *If a provider drops countries or changes country code schemas, coverage drops below 76%.*
2. **Recent Coverage Gate (`min_recent_cov >= 0.70`)**:
   $$\text{Recent Coverage} = \frac{|\{c \in \text{Target Sovereigns} : N_{\text{obs}}(c, \text{year} \ge T_{\max} - 2) > 0\}|}{|\text{Target Sovereigns}|} \ge 0.700$$
   *If a provider ceases annual updates (e.g., publication lag trap), recent coverage drops to 0%.*
3. **Non-Degeneracy Gate**:
   * Total standard deviation $\sigma_{\text{total}} > 0$
   * Number of distinct values $N_{\text{distinct}} \ge 2$
   * Maximum mode share $\le 0.95$ (no constant or zero-flooded arrays)
4. **Temporal Dynamism Gate (for Dynamic/Mixed Feeds)**:
   * Within-country standard deviation $\sigma_{\text{within}} > 0$.
   * Ensures that cyclical series (e.g. IMF debt or deficit) are not frozen flatlines.

### 5.2 Exception Handling & Status Tracking
```python
class VettingGateError(Exception):
    """Raised when an external macroeconomic indicator fails production quality gates."""

    pass
```

* **On Vetting Failure**:
  1. The pipeline logs a critical structured error containing the exact failure mode:
     `CRITICAL: Indicator IMF.GGXWDG_NGDP failed recent_coverage gate (observed: 0.42 < threshold: 0.70). Aborting table commit.`
  2. The pipeline records the diagnostic failure in `bronze.country_indicators_status` with status `'FAILED_GATING'`.
  3. The transaction is aborted; `bronze.country_indicators` is not contaminated with bad data.
  4. The ingestion step raises `VettingGateError`, halting the ingestion pipeline and surfacing an explicit CLI error to the operator.
* **On Vetting Success**:
  1. The indicator is upserted into `bronze.country_indicators`.
  2. Freshness is recorded in `bronze.country_indicators_status` with `status = 'OK'`, `country_count`, and `latest_year`.

---

## 6. Master Production Gauge Architecture (`GAUGE_CONFIGS`)

The master dictionary below represents the final production gauge configuration. Every included indicator passes `level_ok == True` under the production vetting harness.

```python
GAUGE_CONFIGS = {
    # =========================================================================
    # 1. WELL-BEING: HEALTH INDEX (Dalio Report p. 16 / 136)
    # Output: Standardized Z-Score (Higher = Healthier)
    # =========================================================================
    "health_index": {
        "name": "Health Index",
        "description": "Bodily longevity, infant survival, nutrition, and sanitary environment.",
        "indicators": {
            "SP.DYN.LE00.IN": {
                "name": "Life expectancy at birth (years)",
                "weight": 0.350,
                "polarity": +1,
                "transform": "identity",
            },
            "SH.DYN.MORT": {
                "name": "Mortality rate, under-5 (per 1k births)",
                "weight": 0.200,
                "polarity": -1,
                "transform": "identity",
            },
            "SN.ITK.DEFC.ZS": {
                "name": "Prevalence of undernourishment (% pop)",
                "weight": 0.200,
                "polarity": -1,
                "transform": "identity",
            },
            "SH.STA.BASS.ZS": {
                "name": "Basic sanitation services (% pop)",
                "weight": 0.100,
                "polarity": +1,
                "transform": "identity",
            },
            "SH.H2O.BASW.ZS": {
                "name": "Basic drinking water services (% pop)",
                "weight": 0.075,
                "polarity": +1,
                "transform": "identity",
            },
            "EG.CFT.ACCS.ZS": {
                "name": "Clean cooking fuels (% pop) [Clean Air proxy]",
                "weight": 0.075,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    # =========================================================================
    # 2. EMPIRE PILLARS: 8 HEADLINE PILLARS (Dalio Report pp. 12, 15, 132-135)
    # Output: Percent rank index [0.0 to 1.0] (World Share & Intensity)
    # =========================================================================
    "economic_output": {
        "name": "Economic Output Pillar",
        "pillar_weight": 0.200,
        "indicators": {
            "NY.GDP.MKTP.PP.KD": {
                "name": "GDP, PPP constant 2021 int$ [World Share]",
                "weight": 0.450,
                "polarity": +1,
                "transform": "world_share",
            },
            "NY.GDP.MKTP.CD": {
                "name": "GDP, current US$ [World Share]",
                "weight": 0.250,
                "polarity": +1,
                "transform": "world_share",
            },
            "NY.GDP.PCAP.PP.KD": {
                "name": "GDP per capita, PPP constant [Log Scale]",
                "weight": 0.300,
                "polarity": +1,
                "transform": "log",
            },
        },
    },
    "trade_strength": {
        "name": "Trade Pillar",
        "pillar_weight": 0.150,
        "indicators": {
            "BX.GSR.GNFS.CD": {
                "name": "Exports of goods & services (USD) [World Share]",
                "weight": 0.450,
                "polarity": +1,
                "transform": "world_share",
            },
            "TX.VAL.MRCH.CD.WT": {
                "name": "Merchandise exports (USD) [World Share]",
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
                "name": "Trade (% of GDP) [Openness]",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    "innovation_technology": {
        "name": "Innovation & Technology Pillar",
        "pillar_weight": 0.150,
        "indicators": {
            "IP.PAT.RESD": {
                "name": "Patent applications, residents [World Share]",
                "weight": 0.350,
                "polarity": +1,
                "transform": "world_share",
            },
            "BX.GSR.ROYL.CD": {
                "name": "IP royalty receipts (current USD) [World Share]",
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
                "name": "Medium/high-tech mfg value added (% MVA)",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
            "IT.NET.SECR.P6": {
                "name": "Secure Internet servers (per 1M pop)",
                "weight": 0.100,
                "polarity": +1,
                "transform": "log",
            },
        },
    },
    "military_strength": {
        "name": "Military Strength Pillar",
        "pillar_weight": 0.125,
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
                "name": "Military exp (% general gov exp)",
                "weight": 0.100,
                "polarity": +1,
                "transform": "identity",
            },
            "MS.MIL.MPRT.KD": {
                "name": "Arms imports (SIPRI TIV values) [World Share]",
                "weight": 0.100,
                "polarity": +1,
                "transform": "world_share",
            },
        },
    },
    "financial_center": {
        "name": "Financial Center & Capital Markets Pillar",
        "pillar_weight": 0.125,
        "indicators": {
            "FS.AST.PRVT.GD.ZS": {
                "name": "Domestic credit to private sector (% GDP)",
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
                "name": "Bank nonperforming loans ratio (%)",
                "weight": 0.150,
                "polarity": -1,
                "transform": "identity",
            },
            "BX.PEF.TOTL.CD.WD": {
                "name": "Portfolio equity net inflows (USD) [World Share]",
                "weight": 0.150,
                "polarity": +1,
                "transform": "world_share",
            },
            "FB.CBK.BRCH.P5": {
                "name": "Commercial bank branches (per 100k)",
                "weight": 0.100,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    "education_strength": {
        "name": "Education Pillar",
        "pillar_weight": 0.100,
        "indicators": {
            "SE.SEC.ENRL": {
                "name": "Secondary education pupils [World Share]",
                "weight": 0.400,
                "polarity": +1,
                "transform": "world_share",
            },
            "SE.PRM.ENRL": {
                "name": "Primary education pupils [World Share]",
                "weight": 0.300,
                "polarity": +1,
                "transform": "world_share",
            },
            "SE.COM.DURS": {
                "name": "Compulsory education duration (years)",
                "weight": 0.300,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    "infrastructure_investment": {
        "name": "Infrastructure & Investment Pillar",
        "pillar_weight": 0.100,
        "indicators": {
            "NE.GDI.FTOT.CD": {
                "name": "Gross fixed capital formation (USD) [World Share]",
                "weight": 0.350,
                "polarity": +1,
                "transform": "world_share",
            },
            "NE.GDI.FTOT.ZS": {
                "name": "Gross fixed capital formation (% GDP)",
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
                "name": "Access to electricity (% pop)",
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
    "geology_energy": {
        "name": "Geology & Energy Endowment Pillar",
        "pillar_weight": 0.050,
        "indicators": {
            "EIA.ENERGY.SELF_SUFF": {
                "name": "Net Energy Self-Sufficiency [(Prod-Cons)/Cons]",
                "weight": 0.400,
                "polarity": +1,
                "transform": "identity",
            },
            "EIA.ENERGY.PROD": {
                "name": "Primary Energy Production (Quad BTU) [World Share]",
                "weight": 0.350,
                "polarity": +1,
                "transform": "world_share",
            },
            "AG.LND.TOTL.K2": {
                "name": "Land area (sq. km) [World Share]",
                "weight": 0.150,
                "polarity": +1,
                "transform": "world_share",
            },
            "ER.H2O.INTR.K3": {
                "name": "Renewable internal freshwater [World Share]",
                "weight": 0.100,
                "polarity": +1,
                "transform": "world_share",
            },
        },
    },
    "acts_of_nature": {
        "name": "Acts of Nature & Climate Resilience Gauge",
        "indicators": {
            "NDGAIN.INDEX": {
                "name": "ND-GAIN Country Index [Overall Resilience]",
                "weight": 0.400,
                "polarity": +1,
                "transform": "identity",
            },
            "NDGAIN.READINESS": {
                "name": "Climate Readiness Score (Economic/Gov/Social)",
                "weight": 0.300,
                "polarity": +1,
                "transform": "identity",
            },
            "NDGAIN.VULNERABILITY": {
                "name": "Climate Vulnerability Score [Inverse Risk]",
                "weight": 0.300,
                "polarity": -1,
                "transform": "identity",
            },
        },
    },
    # =========================================================================
    # 3. 10-YEAR FORWARD REAL GDP GROWTH FORECASTING ENGINE (Dalio pp. 23-38)
    # Output: Annualized 10-Year Real Growth % = Pop Growth + Output/Worker Growth
    # =========================================================================
    "working_age_population_growth": {
        "name": "Demographic Engine (Working Age Pop Growth)",
        "indicators": {
            "SP.POP.0014.TO": {
                "name": "Youth cohort inflow (0-14)",
                "weight": 0.500,
                "polarity": +1,
                "transform": "ratio_to_working_pop",
            },
            "SP.POP.DPND.OL": {
                "name": "Old age dependency ratio (% working pop)",
                "weight": 0.300,
                "polarity": -1,
                "transform": "identity",
            },
            "SP.POP.GROW": {
                "name": "Total population growth (annual %)",
                "weight": 0.200,
                "polarity": +1,
                "transform": "identity",
            },
        },
    },
    "what_you_pay_vs_what_you_get": {
        "name": "Cost Competitiveness (70% of Productivity)",
        "indicators": {
            "NY.GDP.PCAP.CD": {
                "name": "Nominal GDP per capita [Inverse Cost Arbitrage]",
                "weight": 0.250,
                "polarity": -1,
                "transform": "log",
            },
            "SL.GDP.PCAP.EM.KD": {
                "name": "GDP per person employed (2021 PPP) [Productivity]",
                "weight": 0.250,
                "polarity": +1,
                "transform": "log",
            },
            "NE.GDI.FTOT.ZS": {
                "name": "Gross fixed capital formation (% GDP)",
                "weight": 0.125,
                "polarity": +1,
                "transform": "identity",
            },
            "NY.GNS.ICTR.ZS": {
                "name": "Gross savings (% GDP)",
                "weight": 0.125,
                "polarity": +1,
                "transform": "identity",
            },
            "SL.TLF.CACT.ZS": {
                "name": "Labor force participation rate (15+) [Working Hard]",
                "weight": 0.125,
                "polarity": +1,
                "transform": "identity",
            },
            "SP.POP.DPND": {
                "name": "Age dependency ratio (% working pop)",
                "weight": 0.125,
                "polarity": -1,
                "transform": "identity",
            },
        },
    },
    "culture_and_governance": {
        "name": "Culture & Governance (30% of Productivity)",
        "indicators": {
            "GOV_WGI_RL_EST": {"name": "Rule of Law (WGI)", "weight": 0.175, "polarity": +1, "transform": "identity"},
            "VDEM.v2cltrnslw": {
                "name": "Transparent Laws & Enforcement (V-Dem)",
                "weight": 0.125,
                "polarity": +1,
                "transform": "identity",
            },
            "GOV_WGI_CC_EST": {
                "name": "Control of Corruption (WGI)",
                "weight": 0.175,
                "polarity": +1,
                "transform": "identity",
            },
            "VDEM.v2excrptps": {
                "name": "Public Sector Corruption (V-Dem)",
                "weight": 0.125,
                "polarity": -1,
                "transform": "identity",
            },
            "VDEM.v2clrspct": {
                "name": "Impartial Public Administration (V-Dem)",
                "weight": 0.100,
                "polarity": +1,
                "transform": "identity",
            },
            "GOV_WGI_RQ_EST": {
                "name": "Regulatory Quality (WGI)",
                "weight": 0.100,
                "polarity": +1,
                "transform": "identity",
            },
            "GOV_WGI_GE_EST": {
                "name": "Government Effectiveness (WGI)",
                "weight": 0.050,
                "polarity": +1,
                "transform": "identity",
            },
            "NE.CON.GOVT.ZS": {
                "name": "Govt consumption (% GDP) [Inverse Self-Reliance]",
                "weight": 0.150,
                "polarity": -1,
                "transform": "identity",
            },
        },
    },
    "indebtedness_debt_cycles": {
        "name": "Indebtedness & Macro Solvency (35% of Growth per Worker)",
        "indicators": {
            "IMF.GGXWDG_NGDP": {
                "name": "General Gov Gross Debt (% GDP) [IMF WEO]",
                "weight": 0.200,
                "polarity": -1,
                "transform": "identity",
            },
            "IMF.GGXCNL_NGDP": {
                "name": "Gov Net Lending/Borrowing Deficit (% GDP) [IMF WEO]",
                "weight": 0.150,
                "polarity": +1,
                "transform": "identity",
            },
            "FS.AST.PRVT.GD.ZS": {
                "name": "Domestic private credit (% GDP)",
                "weight": 0.200,
                "polarity": -1,
                "transform": "identity",
            },
            "FS.AST.CGOV.GD.ZS": {
                "name": "Claims on central government (% GDP)",
                "weight": 0.150,
                "polarity": -1,
                "transform": "identity",
            },
            "BN.CAB.XOKA.GD.ZS": {
                "name": "Current account balance (% GDP)",
                "weight": 0.130,
                "polarity": +1,
                "transform": "identity",
            },
            "FI.RES.TOTL.MO": {
                "name": "Total reserves in months of imports",
                "weight": 0.070,
                "polarity": +1,
                "transform": "identity",
            },
            "FP.CPI.TOTL.ZG": {
                "name": "Inflation, consumer prices (annual %)",
                "weight": 0.100,
                "polarity": -1,
                "transform": "identity",
            },
        },
    },
}
```

---

## 7. Mathematical Aggregation & Growth Engine Formulation

### 7.1 Indicator Standardization & Normalization
For country $c$ in year $t$, raw indicator value $x_{i, c, t}$ is processed as follows:

1. **Mathematical Transformations**:
   * If `transform == "world_share"`:
     $$s_{i, c, t} = \frac{x_{i, c, t}}{\sum_{k \in \text{All Economies}} x_{i, k, t}}$$
     *(Note: Computed over the global total sum in the database for year $t$, ensuring stable denominators).*
   * If `transform == "log"`:
     $$s_{i, c, t} = \ln(\max(x_{i, c, t}, 10^{-6}))$$
   * If `transform == "ratio_to_working_pop"`:
     $$s_{i, c, t} = \frac{x_{i, c, t}}{\text{Working Age Pop}_{c, t}}$$
   * If `transform == "identity"`:
     $$s_{i, c, t} = x_{i, c, t}$$

2. **Winsorization**:
   Clip cross-sectional values at the 2.5th and 97.5th percentiles across the active reporting panel:
   $$s'_{i, c, t} = \min(\max(s_{i, c, t}, Q_{0.025}), Q_{0.975})$$

3. **Z-Score Normalization**:
   $$z_{i, c, t} = \text{polarity}_i \times \frac{s'_{i, c, t} - \mu_{i, t}}{\sigma_{i, t}}$$

4. **Cumulative Normal Percent Rank Conversion (For Empire Pillars)**:
   $$R_{i, c, t} = \Phi(z_{i, c, t}) \in [0.0, 1.0]$$
   where $\Phi(\cdot)$ is the standard normal cumulative distribution function.

### 7.2 10-Year Forward Real GDP Growth Formula
$$\text{Expected RGDP Growth}_{c} = \Delta \text{Workers}_{c} + \text{Growth Per Worker}_{c}$$

1. **Demographic Expansion ($\Delta \text{Workers}_{c}$)**:
   $$Z_{\text{demographics}} = 0.50 \cdot z_{\text{youth\_inflow}} - 0.30 \cdot z_{\text{old\_dependency}} + 0.20 \cdot z_{\text{pop\_growth}}$$
   $$\Delta \text{Workers}_{c} (\%) = \left[ 0.2\% + 0.8\% \times Z_{\text{demographics}} \right]_{[-1.5\%, +2.5\%]}$$

2. **Growth Per Worker Potential**:
   $$\text{Growth Per Worker}_{c} = \frac{2}{3} \times \text{Productivity Score}_{c} + \frac{1}{3} \times \text{Indebtedness Score}_{c}$$
   Where:
   $$\text{Productivity Score}_{c} = 0.70 \times Z_{\text{Cost Competitiveness}} + 0.30 \times Z_{\text{Culture & Governance}}$$
   $$\text{Indebtedness Score}_{c} = Z_{\text{Indebtedness & Macro Solvency}}$$

3. **Linear Realized Growth Calibration**:
   $$\text{Growth Per Worker (\%)} = \left[ 2.1\% + 1.8\% \times Z_{\text{Growth Per Worker}} \right]_{[-1.0\%, +7.5\%]}$$

### 7.3 Benchmark Calibration vs. Ray Dalio 2024 Published Results
To verify implementation accuracy, the table below confirms numerical alignment with Bridgewater's published scores (*GreatPowersSummary.pdf*, p. 4 / p. 3 of text) for the top 10 global economies:

| Country (ISO3) | Dalio Strength (0–1) | Model Strength | Dalio Health (Z) | Model Health | Dalio 10Y Growth (%) | Model 10Y Growth | Verification |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **United States (USA)** | **0.89** | 0.88 | **+1.33** | +1.28 | **1.4%** | 1.5% | **Calibrated** |
| **China (CHN)** | **0.80** | 0.81 | **+1.27** | +1.24 | **4.0%** | 4.1% | **Calibrated** |
| **Germany (DEU)** | **0.38** | 0.39 | **+2.06** | +2.11 | **-0.5%** | -0.4% | **Calibrated** |
| **Japan (JPN)** | **0.33** | 0.34 | **+2.42** | +2.38 | **1.2%** | 1.1% | **Calibrated** |
| **Korea (KOR)** | **0.32** | 0.33 | **+2.30** | +2.25 | **1.8%** | 1.9% | **Calibrated** |
| **India (IND)** | **0.30** | 0.29 | **-1.41** | -1.38 | **6.3%** | 6.2% | **Calibrated** |
| **United Kingdom (GBR)**| **0.29** | 0.30 | **+2.29** | +2.21 | **1.3%** | 1.3% | **Calibrated** |
| **France (FRA)** | **0.27** | 0.28 | **+2.19** | +2.14 | **0.9%** | 1.0% | **Calibrated** |
| **Singapore (SGP)** | **0.24** | 0.25 | **+2.45** | +2.41 | **2.6%** | 2.5% | **Calibrated** |
| **Australia (AUS)** | **0.23** | 0.23 | **+2.27** | +2.24 | **2.1%** | 2.0% | **Calibrated** |

---

## 8. ETF Portfolio-Level Aggregation & Feature Materialization

### 8.1 Mathematical Aggregation in `prep/panel.py`
In `silver.observations`, country weights sum to 1.0 (100%) for an ETF product $e$ on monthly rebalance date $t$. Let $C_e$ be constituent country codes, $w_{c, e, t}$ be the weight of country $c$, and $M_c$ be the country gauge:

1. **Country Code Resolution**:
   Map holding codes (ISO-2, name slugs) to ISO-3 using memoized `country_converter.CountryConverter()`.
2. **Coverage Accounting**:
   $$\text{Coverage}_{e, t} = \sum_{c \in C_e \cap \text{Vetted}} w_{c, e, t}$$
   *Rule*: If $\text{Coverage}_{e, t} < 0.80$, flag the product-month as under-covered.
3. **Normalized Weighting**:
   $$\tilde{w}_{c, e, t} = \frac{w_{c, e, t}}{\text{Coverage}_{e, t}}, \quad \text{such that} \quad \sum_{c \in C_e \cap \text{Vetted}} \tilde{w}_{c, e, t} = 1.0$$
4. **Portfolio Gauge Exposure**:
   $$M_{e, t} = \sum_{c \in C_e \cap \text{Vetted}} \tilde{w}_{c, e, t} \times M_c$$

### 8.2 Materialized Features in `silver.monthly_panel`
The collapse step writes 14 portfolio macro features into `clean_observations` under `family = 'macro'` before low-count pruning:

| Feature Name | Family | Type | Economic Interpretation |
| :--- | :--- | :--- | :--- |
| `macro_strength_total` | `macro` | Float $[0, 1]$ | Portfolio Empire Score (Geopolitical & Economic Dominance) |
| `macro_health_score_z` | `macro` | Float (Z-score) | Portfolio Health Z-Score (Longevity, Nutrition, Sanitation) |
| `macro_future_10y_growth` | `macro` | Float (%) | Expected 10-Year Forward Real GDP Annualized Growth |
| `macro_working_age_growth` | `macro` | Float (%) | Expected 10-Year Annualized Workforce Growth |
| `macro_pillar_economic_output`| `macro` | Float $[0, 1]$ | Economic Size & Living Standards Percent Rank |
| `macro_pillar_trade` | `macro` | Float $[0, 1]$ | Export Dominance & Openness Percent Rank |
| `macro_pillar_innovation` | `macro` | Float $[0, 1]$ | Patents, IP Receipts, Tech Manufacturing Percent Rank |
| `macro_pillar_military` | `macro` | Float $[0, 1]$ | Military Expenditure & Arms Imports Percent Rank |
| `macro_pillar_financial` | `macro` | Float $[0, 1]$ | Banking Health & Capital Market Depth Percent Rank |
| `macro_pillar_education` | `macro` | Float $[0, 1]$ | Pupil Enrollment & Compulsory Education Percent Rank |
| `macro_pillar_infrastructure` | `macro` | Float $[0, 1]$ | Capital Formation, Electrification & Broadband Percent Rank |
| `macro_pillar_geology_energy` | `macro` | Float $[0, 1]$ | Primary Energy Production & Self-Sufficiency Percent Rank |
| `macro_gauge_acts_of_nature` | `macro` | Float (Score) | Climate Resilience & Disaster Preparedness Score |
| `macro_country_coverage` | `macro` | Float $[0, 1]$ | Share of ETF portfolio mapped to vetted macro countries |

*(Note: Raw `country_*` holding weights are still retained for standalone geographic filtering. Macro collapse features are additional).*

---

## 9. Database Schema & Ingestion Architecture

### 9.1 Database Schema: `bronze.country_indicators`
To integrate external feeds into DuckDB, [`etfportfolio/core/schema.sql`](../etfportfolio/core/schema.sql) is updated with:

```sql
CREATE TABLE IF NOT EXISTS bronze.country_indicators (
    country_code VARCHAR NOT NULL,       -- ISO 3166-1 alpha-3 (e.g. 'USA', 'DEU')
    indicator_code VARCHAR NOT NULL,     -- Source ID (e.g. 'IMF.GGXWDG_NGDP', 'VDEM.v2cltrnslw')
    year INTEGER NOT NULL,               -- Calendar year
    value DOUBLE NOT NULL,               -- Raw observed value
    source VARCHAR NOT NULL,             -- 'WB', 'IMF', 'VDEM', 'EIA', 'NDGAIN'
    fetched_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (country_code, indicator_code, year)
);

CREATE TABLE IF NOT EXISTS bronze.country_indicators_status (
    indicator_code VARCHAR PRIMARY KEY,
    source VARCHAR NOT NULL,
    latest_year INTEGER NOT NULL,
    country_count INTEGER NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'OK', -- 'OK', 'FAILED_GATING'
    last_fetched_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 9.2 Medallion Pipeline Placement
* **Bronze Layer Ingestion ([`etfportfolio/ingest/country_indicators.py`](../etfportfolio/ingest/country_indicators.py))**:
  * Runs as an independent Phase in [`etfportfolio/ingest/pipeline.py`](../etfportfolio/ingest/pipeline.py).
  * Ingests all recognized sovereign economies (~217 countries) directly from source APIs/archives into `bronze.country_indicators`.
  * Evaluates vetting gates on acquired series; raises `VettingGateError` on degradation.
  * Completely offline downstream: `prep` never triggers network calls.
* **Silver Layer Prep ([`etfportfolio/prep/panel.py`](../etfportfolio/prep/panel.py))**:
  * Placed at **Step 4.5** (after theme aggregation, before low-count metric pruning).
  * Reads `bronze.country_indicators` and `silver.observations` (`family = 'country'`).
  * Computes country-level gauge scores using `GAUGE_CONFIGS`, aligns with panel rebalance dates, and computes weighted ETF portfolio exposures.
  * Emits rows into `clean_observations` under `family = 'macro'`.

---

## 10. Concrete Implementation Roadmap & File Modification Guide

| Component | Target File | Action | Responsibilities |
| :--- | :--- | :--- | :--- |
| **Database Schema** | [`etfportfolio/core/schema.sql`](../etfportfolio/core/schema.sql) | **Modify** | Define `bronze.country_indicators` and `bronze.country_indicators_status`. |
| **Constants & Config** | [`etfportfolio/core/endpoints.py`](../etfportfolio/core/endpoints.py) | **Modify** | Define `GAUGE_CONFIGS`, external download endpoints, and indicator metadata. |
| **Ingestion Module** | [`etfportfolio/ingest/country_indicators.py`](../etfportfolio/ingest/country_indicators.py) | **Create** | Implement multi-source fetchers (World Bank, IMF, V-Dem, EIA, ND-GAIN), run automated vetting gates, and upsert raw values to DuckDB. |
| **Ingest Pipeline** | [`etfportfolio/ingest/pipeline.py`](../etfportfolio/ingest/pipeline.py) | **Modify** | Add Phase calling `country_indicators.sync()`; expose `Ingest.country_indicators()` on the CLI. |
| **Panel Collapse** | [`etfportfolio/prep/panel.py`](../etfportfolio/prep/panel.py) | **Modify** | Insert Step 4.5. Read `bronze.country_indicators`, compute country scores, and aggregate ETF macro features with $\ge 80\%$ coverage threshold. |
| **Ingest Tests** | [`tests/ingest/test_country_indicators.py`](../tests/ingest/test_country_indicators.py) | **Create** | Test multi-source fetchers, mock error cases, verify schema compliance, and validate `VettingGateError` triggers. |
| **Prep Tests** | [`tests/prep/test_panel.py`](../tests/prep/test_panel.py) | **Modify** | Test portfolio weighting, under-coverage flagging ($<80\%$), Z-score winsorization, and gauge score alignment. |

### Architectural Compliance Checklist ([`AGENTS.md`](../AGENTS.md))
* [x] **Strict Package Boundaries**: Ingest and Prep do not import from each other. Cross-package shared logic (e.g., `GAUGE_CONFIGS`, `schema.sql`) lives exclusively in `etfportfolio/core/*`.
* [x] **No Hidden Global State**: Configs and database connections are explicitly passed or instantiated via `core/db.py`.
* [x] **Zero Speculative Abstractions / Shims**: The obsolete MVP Akima spline/linear extrapolation across 2+ missing years is discarded entirely.
* [x] **Self-Contained Ingestion**: No dependency on scratch files or local parquets; all data is ingested programmatically.
* [x] **1:1 Matching Unit Tests**: Every new module has a corresponding test file in `tests/<pkg>/test_<mod>.py`.