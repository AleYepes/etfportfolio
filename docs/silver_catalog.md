# Silver Layer Catalog: `silver.product_metrics` & `silver.product_dimensions`

> **Scope & Purpose**: This document provides an exhaustive inventory, statistical distribution profile,
> and semantic specification of all fundamental metrics and dimensional breakdowns in the Silver medallion layer.
> It serves as the formal reference for designing downstream **monthly LOCF panels**, factor returns,
> and cross-sectional factor models.

---

## 1. Executive Summary & Architecture

- **Universe Size (`silver.products`)**: **20,376** tradeable ETFs with active price histories and fundamental observations.
- **`silver.product_metrics`**: **2,472,376** rows | **20,502** distinct products (100.6% universe coverage).
- **`silver.product_dimensions`**: **5,914,368** rows | **18,842** distinct products (92.5% universe coverage).

### Table Schemas & Relational Keys

```sql
CREATE TABLE silver.product_metrics (
    product_id             INTEGER NOT NULL,                  -- Links to silver.products.product_id
    source                 VARCHAR NOT NULL,                  -- Ingestion endpoint ('ratios', 'profile', 'mstar', etc.)
    metric_id              VARCHAR NOT NULL,                  -- Canonical metric identifier
    effective_date         DATE NOT NULL,                     -- Point-in-time assessment date
    effective_date_source  VARCHAR NOT NULL,                  -- Date provenance: 'payload', 'item', or 'snapshot'
    fetched_at             TIMESTAMP WITH TIME ZONE NOT NULL, -- Blob fetch timestamp
    value                  DOUBLE NOT NULL,                   -- Standardized numeric value for analytics
    raw_value              VARCHAR NOT NULL,                  -- Formatted display string from raw payload
    PRIMARY KEY (product_id, source, metric_id, effective_date)
);

CREATE TABLE silver.product_dimensions (
    product_id             INTEGER NOT NULL,                  -- Links to silver.products.product_id
    dimension_type         VARCHAR NOT NULL,                  -- Dimension category ('asset_class', 'industry', etc.)
    dimension_name         VARCHAR NOT NULL,                  -- Human-readable category or security name
    dimension_code         VARCHAR,                           -- ISO code, theme UUID, or IBKR conid(s)
    effective_date         DATE NOT NULL,                     -- Point-in-time assessment date
    effective_date_source  VARCHAR NOT NULL,                  -- Date provenance: 'payload', 'item', or 'snapshot'
    fetched_at             TIMESTAMP WITH TIME ZONE NOT NULL, -- Blob fetch timestamp
    value                  DOUBLE NOT NULL,                   -- Decimal weight (1.0 = 100%) or coordinate indicator
    raw_value              VARCHAR NOT NULL,                  -- Formatted percentage or coordinate string
    PRIMARY KEY (product_id, dimension_type, dimension_name, effective_date)
);
```

---

## 2. Metrics Summary by Ingestion Source (`silver.product_metrics`)

| Source Endpoint | Record Count | Distinct Metrics | Product Coverage | Universe % | Date Range | Date Sources |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `lipper` | 1,064,394 | 568 | 11,650 | 57.2% | 2010-10-29 to 2026-08-31 | `item` |
| `ratios` | 748,780 | 47 | 16,538 | 81.2% | 2015-01-31 to 2026-08-31 | `payload` |
| `esg` | 404,404 | 17 | 11,627 | 57.1% | 2026-08-22 to 2026-09-05 | `payload` |
| `profile` | 142,658 | 7 | 20,024 | 98.3% | 2018-07-12 to 2026-09-13 | `snapshot, item` |
| `mstar` | 84,221 | 10 | 16,454 | 80.8% | 2021-02-28 to 2026-09-10 | `item, snapshot` |
| `holdings` | 27,919 | 1 | 18,793 | 92.2% | 2015-01-31 to 2026-08-31 | `payload` |

---

## 3. Granular Metric Inventory (Non-Lipper Metrics)

This section profiles all **82 fundamental metrics** from `profile`, `holdings`, `ratios`, `mstar`, and `esg`.
Metrics are grouped into semantic domain families for panel design.

### 3.1 ESG (Refinitiv)

| Metric ID | Source | Coverage | Date Range | Min | Median (p50) | Max | Mean | Date Prov. | Sample Raw | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `esg_coverage` | `esg` | 11,624 (57.0%) | 2026-08-22..2026-09-05 | 0.7 | 0.942 | 2.68 | 0.924 | `payload` | `0.949818` | Portfolio ESG asset evaluation coverage ratio (evaluated assets / total AUM) (Ratio (0.0 - 1.0)) |
| `tresgccs` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 5 | 10 | 5.46 | `payload` | `8` | Refinitiv ESG Controversies Score (frequency and severity of negative media/legal events) (Score (0 - 10)) |
| `tresgcgbds` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 6 | 9 | 6.18 | `payload` | `6` | Governance: Management score (board independence, diversity, committee structures) (Score (0 - 10)) |
| `tresgcgs` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 6 | 9 | 6.03 | `payload` | `6` | Governance Pillar overall score (Score (0 - 10)) |
| `tresgcgsrs` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 5 | 9 | 5.17 | `payload` | `5` | Governance: Shareholders score (equal voting rights, anti-takeover defense restrictions) (Score (0 - 10)) |
| `tresgcgvss` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 7 | 9 | 6.69 | `payload` | `6` | Governance: CSR Strategy score (ESG reporting transparency, audited sustainability disclosures) (Score (0 - 10)) |
| `tresgcs` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 1 | 5 | 8 | 4.91 | `payload` | `6` | Refinitiv ESG Combined Score (ESG score discounted by active controversies) (Score (0 - 10)) |
| `tresgeners` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 7 | 9 | 6.48 | `payload` | `7` | Environmental: Emissions reduction score (GHG emissions, climate change targets) (Score (0 - 10)) |
| `tresgenpis` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 4 | 9 | 4.2 | `payload` | `5` | Environmental: Product Innovation score (clean technologies, eco-designed products) (Score (0 - 10)) |
| `tresgenrrs` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 7 | 9 | 6.74 | `payload` | `8` | Environmental: Resource Use score (energy, water efficiency, supply chain management) (Score (0 - 10)) |
| `tresgens` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 6 | 9 | 5.79 | `payload` | `7` | Environmental Pillar overall score (Score (0 - 10)) |
| `tresgs` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 1 | 6 | 8 | 6.06 | `payload` | `7` | Refinitiv Total ESG Score (composite of Environmental, Social, and Governance) (Score (0 - 10)) |
| `tresgsocos` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 7 | 9 | 6.73 | `payload` | `6` | Social: Community score (business ethics, public health, community relations) (Score (0 - 10)) |
| `tresgsohrs` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 6 | 9 | 5.96 | `payload` | `7` | Social: Human Rights score (fundamental human rights conventions, supply chain ethics) (Score (0 - 10)) |
| `tresgsoprs` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 6 | 9 | 5.67 | `payload` | `6` | Social: Product Responsibility score (customer privacy, product safety, truth in advertising) (Score (0 - 10)) |
| `tresgsos` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 1 | 6 | 9 | 6.25 | `payload` | `7` | Social Pillar overall score (Score (0 - 10)) |
| `tresgsowos` | `esg` | 11,627 (57.1%) | 2026-08-22..2026-09-05 | 0 | 6 | 9 | 6.38 | `payload` | `8` | Social: Workforce score (job satisfaction, safety, diversity, employment quality) (Score (0 - 10)) |

### 3.2 Holdings

| Metric ID | Source | Coverage | Date Range | Min | Median (p50) | Max | Mean | Date Prov. | Sample Raw | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `portfolio_top_10_concentration` | `holdings` | 18,793 (92.2%) | 2015-01-31..2026-08-31 | 0.0078 | 0.448 | 17.7 | 0.525 | `payload` | `12.93%` | Summed portfolio weight of the top 10 individual underlying assets (Decimal ratio (0.59 = 59.0%)) |

### 3.3 Morningstar Ratings

| Metric ID | Source | Coverage | Date Range | Min | Median (p50) | Max | Mean | Date Prov. | Sample Raw | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `mstar_medalist_rating_analyst` | `mstar` | 10,742 (52.7%) | 2022-05-19..2026-09-10 | 1 | 3 | 5 | 2.95 | `item` | `Neutral` | Morningstar Medalist Rating assigned by qualitative analyst (Gold=5, Silver=4, Bronze=3, Neutral=2, Negative=1) (Score (1.0 - 5.0)) |
| `mstar_medalist_rating_quant` | `mstar` | 4 (0.0%) | 2022-09-30..2023-02-28 | 3 | 4 | 5 | 4 | `item` | `Silver` | Morningstar Medalist Rating generated quantitatively by machine learning model (1.0 - 5.0) (Score (1.0 - 5.0)) |
| `mstar_morningstar_rating` | `mstar` | 9,233 (45.3%) | 2022-02-28..2026-08-31 | 1 | 3 | 5 | 3.23 | `item` | `3` | Overall Morningstar Star Rating (1 to 5 stars) (Score (1.0 - 5.0)) |
| `mstar_parent_analyst` | `mstar` | 9,264 (45.5%) | 2022-01-18..2026-08-28 | 2 | 3 | 5 | 3.44 | `item` | `Average` | Parent Pillar qualitative analyst score (High=5, Above Avg=4, Avg=3, Below Avg=2, Low=1) (Score (1.0 - 5.0)) |
| `mstar_parent_quant` | `mstar` | 1,483 (7.3%) | 2022-10-31..2026-09-03 | 1 | 2 | 5 | 2.49 | `snapshot, item` | `Below_Average` | Parent Pillar quantitative model score (High=5, Above Avg=4, Avg=3, Below Avg=2, Low=1) (Score (1.0 - 5.0)) |
| `mstar_people_analyst` | `mstar` | 6,546 (32.1%) | 2022-05-19..2026-09-10 | 2 | 4 | 5 | 3.88 | `item` | `Above_Average` | People Pillar qualitative analyst score (High=5, Above Avg=4, Avg=3, Below Avg=2, Low=1) (Score (1.0 - 5.0)) |
| `mstar_people_quant` | `mstar` | 4,202 (20.6%) | 2022-10-31..2026-09-03 | 1 | 3 | 5 | 2.97 | `item, snapshot` | `Average` | People Pillar quantitative model score (High=5, Above Avg=4, Avg=3, Below Avg=2, Low=1) (Score (1.0 - 5.0)) |
| `mstar_process_analyst` | `mstar` | 2,599 (12.8%) | 2022-04-22..2026-09-10 | 1 | 4 | 5 | 3.63 | `item` | `Average` | Process Pillar qualitative analyst score (High=5, Above Avg=4, Avg=3, Below Avg=2, Low=1) (Score (1.0 - 5.0)) |
| `mstar_process_quant` | `mstar` | 8,149 (40.0%) | 2022-10-31..2026-09-03 | 1 | 3 | 5 | 2.85 | `item, snapshot` | `Above_Average` | Process Pillar quantitative model score (High=5, Above Avg=4, Avg=3, Below Avg=2, Low=1) (Score (1.0 - 5.0)) |
| `mstar_sustainability_rating` | `mstar` | 15,742 (77.3%) | 2021-02-28..2026-07-31 | 1 | 3 | 5 | 3.08 | `item` | `Average` | Morningstar Sustainability Globe Rating (1 to 5 globes) (Score (1.0 - 5.0)) |

### 3.4 Profile

| Metric ID | Source | Coverage | Date Range | Min | Median (p50) | Max | Mean | Date Prov. | Sample Raw | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `audited_net_expense_ratio` | `profile` | 4,597 (22.6%) | 2023-09-30..2026-05-31 | 0 | 0.005 | 0.0509 | 0.00525 | `item` | `0.8086%` | Audited total net expense ratio extracted from official Annual Reports (Decimal ratio (0.0015 = 0.15%)) |
| `is_passive` | `profile` | 20,024 (98.3%) | 2026-09-03..2026-09-13 | 0 | 1 | 1 | 0.607 | `snapshot` | `Passive` | Fund management style: 1.0 = Passive (index tracking), 0.0 = Active (Binary (0/1)) |
| `management_expense_ratio` | `profile` | 4,560 (22.4%) | 2026-09-03..2026-09-13 | -95.1 | 1 | 1.96 | 0.884 | `snapshot` | `98.94%` | Management expense fee proportion from fee allocation breakdown (Decimal ratio / percentage) |
| `manager_tenure_years` | `profile` | 11,372 (55.8%) | 2026-09-03..2026-09-13 | 0.0192 | 3.67 | 33.9 | 5.81 | `snapshot` | `2017/01/01` | Lead portfolio manager tenure calculated from start date to snapshot (Years (float)) |
| `non_management_expense_ratio` | `profile` | 4,560 (22.4%) | 2026-09-03..2026-09-13 | -0.958 | -0 | 96.1 | 0.116 | `snapshot` | `1.06%` | Non-management operational fee proportion from fee breakdown (Decimal ratio / percentage) |
| `total_expense_ratio` | `profile` | 6,189 (30.4%) | 2026-09-03..2026-09-13 | 0 | 0.0055 | 0.135 | 0.00609 | `snapshot` | `0.8%` | Annual total expense ratio (TER) parsed from fund profile (Decimal ratio (0.0020 = 0.20%)) |
| `total_net_assets_local` | `profile` | 19,176 (94.1%) | 2018-07-12..2026-08-31 | 0 | 1.31e+08 | 3.36e+13 | 9.68e+09 | `item` | `$132.34M (2026/07/31)` | Total Net Assets (AUM) reported at month-end in fund local currency (Local currency amount) |

### 3.5 Fixed Income

| Metric ID | Source | Coverage | Date Range | Min | Median (p50) | Max | Mean | Date Prov. | Sample Raw | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `average_coupon` | `ratios` | 4,349 (21.3%) | 2021-08-31..2026-08-31 | 0.125 | 4.17 | 14.4 | 4.19 | `payload` | `2.99` | Weighted average bond coupon rate (Percentage (%)) |
| `average_quality` | `ratios` | 4,320 (21.2%) | 2015-01-31..2026-08-31 | 3 | 7.16 | 10 | 6.99 | `payload` | `BBB` | Weighted average credit quality mapped to numerical scale (Scale score (float)) |
| `effective_maturity` | `ratios` | 4,834 (23.7%) | 2021-08-31..2026-08-31 | -5.13 | 6.9 | 59 | 7.82 | `payload` | `5.98` | Weighted average effective maturity in years (Years (float)) |
| `nominal_maturity` | `ratios` | 4,834 (23.7%) | 2021-08-31..2026-08-31 | -0.962 | 7.44 | 96.2 | 8.72 | `payload` | `6.94` | Weighted average nominal maturity in years (Years (float)) |
| `yield_to_maturity` | `ratios` | 4,830 (23.7%) | 2021-08-31..2026-08-31 | -8.73 | 4.36 | 10 | 4.49 | `payload` | `3.63` | Weighted average Yield to Maturity (YTM) (Percentage (%)) |

### 3.6 Z-Scores

| Metric ID | Source | Coverage | Date Range | Min | Median (p50) | Max | Mean | Date Prov. | Sample Raw | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `average_final_composite_zscore` | `ratios` | 10,430 (51.2%) | 2019-02-28..2026-08-31 | -1.35 | -0.00576 | 1.74 | -0.00711 | `payload` | `-0.27` | Equal-weighted average final composite valuation Z-score (Standard deviations (Z)) |
| `latest_composite_z_score` | `ratios` | 10,430 (51.2%) | 2019-02-28..2026-08-31 | -1.47 | -0.0144 | 1.74 | -0.018 | `payload` | `-0.24` | Morningstar / IBKR latest valuation composite Z-score vs category peers (Standard deviations (Z)) |
| `latest_dividend_yield_zscore` | `ratios` | 10,430 (51.2%) | 2019-02-28..2026-08-31 | -6.65 | -0.0587 | 1.66 | -0.0781 | `payload` | `-0.24` | Dividend yield standardized Z-score relative to category peer group (Standard deviations (Z)) |
| `latest_price_sales_zscore` | `ratios` | 10,428 (51.2%) | 2019-02-28..2026-08-31 | -1.8 | -0.125 | 7.03 | -0.0405 | `payload` | `-0.32` | P/S multiple standardized Z-score relative to category peer group (Standard deviations (Z)) |
| `latest_price_to_book_zscore` | `ratios` | 10,430 (51.2%) | 2019-02-28..2026-08-31 | -1.33 | -0.0907 | 3.03 | -0.0543 | `payload` | `-0.28` | P/B multiple standardized Z-score relative to category peer group (Standard deviations (Z)) |
| `latest_price_to_earnings_zscore` | `ratios` | 10,429 (51.2%) | 2019-02-28..2026-08-31 | -1.33 | -0.0183 | 2.36 | 0.0333 | `payload` | `-0.38` | P/E multiple standardized Z-score relative to category peer group (Standard deviations (Z)) |
| `latest_return_on_equity_zscore` | `ratios` | 10,430 (51.2%) | 2019-02-28..2026-08-31 | -2.96 | -0.0704 | 4.15 | -0.094 | `payload` | `-0.11` | ROE standardized Z-score relative to category peer group (Standard deviations (Z)) |
| `latest_sps_growth_zscore` | `ratios` | 10,430 (51.2%) | 2019-02-28..2026-08-31 | -2.61 | -0.0704 | 3.74 | -0.0408 | `payload` | `-0.28` | Sales Per Share growth standardized Z-score relative to category peer group (Standard deviations (Z)) |
| `weighted_final_composite_zscore` | `ratios` | 10,430 (51.2%) | 2019-02-28..2026-08-31 | -1.35 | -0.008 | 1.74 | -0.0106 | `payload` | `-0.25` | Factor-weighted final composite valuation Z-score (Standard deviations (Z)) |

### 3.7 Dividend

| Metric ID | Source | Coverage | Date Range | Min | Median (p50) | Max | Mean | Date Prov. | Sample Raw | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dividend_per_share_1yr` | `ratios` | 11,931 (58.6%) | 2018-05-31..2026-08-31 | -100 | 15.2 | 228 | 19.4 | `payload` | `16.07` | Weighted portfolio dividend per share growth rate, 1-year trailing (Percentage (%)) |
| `dividend_per_share_3yr` | `ratios` | 11,905 (58.4%) | 2018-05-31..2026-08-31 | -37 | 13.1 | 173 | 14.4 | `payload` | `6.71` | Weighted portfolio dividend per share growth rate, 3-year annualized (Percentage (%)) |
| `dividend_yield_weighted_average` | `ratios` | 10,430 (51.2%) | 2019-02-28..2026-08-31 | -0.0847 | 1.57 | 15.8 | 1.8 | `payload` | `3.05` | Fund weighted average dividend yield vs category (Percentage (%)) |
| `dividendpayoutratio5yr` | `ratios` | 11,966 (58.7%) | 2018-05-31..2026-08-31 | 0.61 | 50.3 | 4.47e+03 | 71.1 | `payload` | `90.17` | Weighted portfolio dividend payout ratio over 5-year trailing period (Percentage (%)) |

### 3.8 Financial Health

| Metric ID | Source | Coverage | Date Range | Min | Median (p50) | Max | Mean | Date Prov. | Sample Raw | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `ebit_to_interest` | `ratios` | 12,027 (59.0%) | 2019-02-28..2026-08-31 | -5.24e+03 | 76.7 | 1.3e+06 | 262 | `payload` | `43.42` | Weighted portfolio Interest Coverage ratio (EBIT / Interest Expense) (Ratio (x)) |
| `lt_debt_shareholders_equity` | `ratios` | 12,031 (59.0%) | 2018-05-31..2026-08-31 | 0.000832 | 0.746 | 26 | 0.832 | `payload` | `0.79` | Weighted portfolio Long-Term Debt to Shareholders' Equity ratio (Ratio (x)) |
| `sales_to_total_assets` | `ratios` | 12,052 (59.1%) | 2018-05-31..2026-08-31 | 0.00849 | 0.647 | 3.94 | 0.637 | `payload` | `0.7` | Weighted portfolio Asset Turnover ratio (Sales / Total Assets) (Ratio (x)) |
| `total_assets_total_equity` | `ratios` | 12,051 (59.1%) | 2018-05-31..2026-08-31 | 1.01 | 4.25 | 2.64e+05 | 58.8 | `payload` | `4.71` | Weighted portfolio Total Assets to Total Equity (Financial Leverage multiplier) (Ratio (x)) |
| `total_debt_total_capital` | `ratios` | 12,040 (59.1%) | 2018-05-31..2026-08-31 | 0.00181 | 0.95 | 26.1 | 1.05 | `payload` | `0.98` | Weighted portfolio Total Debt to Total Capital ratio (Ratio (x)) |
| `total_debt_total_equity` | `ratios` | 12,044 (59.1%) | 2018-05-31..2026-08-31 | 0.00181 | 0.393 | 3.83 | 0.393 | `payload` | `0.42` | Weighted portfolio Debt-to-Equity ratio (Ratio (x)) |

### 3.9 Growth

| Metric ID | Source | Coverage | Date Range | Min | Median (p50) | Max | Mean | Date Prov. | Sample Raw | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `eps_growth_1yr` | `ratios` | 12,053 (59.2%) | 2018-05-31..2026-08-31 | -50 | 22.7 | 100 | 23.1 | `payload` | `28.23` | Weighted portfolio Earnings Per Share (EPS) growth, 1-year trailing (Percentage (%)) |
| `eps_growth_3yr` | `ratios` | 12,007 (58.9%) | 2018-05-31..2026-08-31 | -50 | 15.9 | 100 | 15.7 | `payload` | `7.38` | Weighted portfolio Earnings Per Share (EPS) growth, 3-year annualized (Percentage (%)) |
| `eps_growth_5yr` | `ratios` | 12,014 (59.0%) | 2018-05-31..2026-08-31 | -43.7 | 19.6 | 100 | 19.8 | `payload` | `18.68` | Weighted portfolio Earnings Per Share (EPS) growth, 5-year annualized (Percentage (%)) |
| `operating_cash_flow_growth_rate_3yr` | `ratios` | 12,009 (58.9%) | 2018-05-31..2026-08-31 | -39.6 | 19.4 | 238 | 21 | `payload` | `15.89` | Weighted portfolio operating cash flow growth rate, 3-year annualized (Percentage (%)) |
| `sales_growth_1_year` | `ratios` | 12,044 (59.1%) | 2018-05-31..2026-08-31 | -18.5 | 13.1 | 100 | 13.8 | `payload` | `4.75` | Weighted portfolio sales / revenue growth rate, 1-year trailing (Percentage (%)) |
| `sales_growth_3_year` | `ratios` | 12,051 (59.1%) | 2018-05-31..2026-08-31 | -19.4 | 10.8 | 100 | 11.7 | `payload` | `2.27` | Weighted portfolio sales / revenue growth rate, 3-year annualized (Percentage (%)) |
| `sales_growth_5_yr` | `ratios` | 12,049 (59.1%) | 2018-05-31..2026-08-31 | -7.46 | 13.7 | 74.8 | 14.4 | `payload` | `8.63` | Weighted portfolio sales / revenue growth rate, 5-year annualized (Percentage (%)) |
| `sales_per_share_growth_1_year` | `ratios` | 12,044 (59.1%) | 2018-05-31..2026-08-31 | -28.6 | 14.6 | 1.99e+04 | 37.6 | `payload` | `4.86` | Weighted portfolio sales per share growth rate, 1-year trailing (Percentage (%)) |
| `sales_per_share_growth_3_year` | `ratios` | 12,051 (59.1%) | 2018-05-31..2026-08-31 | -27 | 10.8 | 249 | 12 | `payload` | `2.23` | Weighted portfolio sales per share growth rate, 3-year annualized (Percentage (%)) |

### 3.10 Valuation Multiples

| Metric ID | Source | Coverage | Date Range | Min | Median (p50) | Max | Mean | Date Prov. | Sample Raw | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `price_book` | `ratios` | 12,027 (59.0%) | 2018-05-31..2026-08-31 | 0.503 | 6.31 | 25 | 7.28 | `payload` | `3` | Weighted average price-to-book ratio (P/B) (Multiple (x)) |
| `price_cash` | `ratios` | 11,993 (58.9%) | 2018-05-31..2026-08-31 | 1.7 | 21 | 60 | 21.8 | `payload` | `9.93` | Weighted average price-to-cash flow ratio (P/CF) (Multiple (x)) |
| `price_earnings` | `ratios` | 12,041 (59.1%) | 2018-05-31..2026-08-31 | 0.676 | 28.5 | 60 | 28.8 | `payload` | `19.29` | Weighted average price-to-earnings ratio (P/E) (Multiple (x)) |
| `price_sales` | `ratios` | 11,998 (58.9%) | 2018-05-31..2026-08-31 | 0.0904 | 6.12 | 50 | 6.85 | `payload` | `2.11` | Weighted average price-to-sales ratio (P/S) (Multiple (x)) |
| `price_to_dividend` | `ratios` | 11,961 (58.7%) | 2018-05-31..2026-08-31 | 7.29 | 159 | 5.02e+03 | 270 | `payload` | `67.92` | Weighted average price-to-dividend ratio (Multiple (x)) |

### 3.11 Technical / Momentum

| Metric ID | Source | Coverage | Date Range | Min | Median (p50) | Max | Mean | Date Prov. | Sample Raw | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `relative_strength` | `ratios` | 12,056 (59.2%) | 2018-05-31..2026-08-31 | -49.2 | 2.2 | 155 | 2.39 | `payload` | `-1.86` | Fund price performance relative to category benchmark over trailing period (Spread / Index (%)) |

### 3.12 Profitability

| Metric ID | Source | Coverage | Date Range | Min | Median (p50) | Max | Mean | Date Prov. | Sample Raw | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `return_on_assets_1yr` | `ratios` | 12,054 (59.2%) | 2018-05-31..2026-08-31 | -91.9 | 8.57 | 102 | 10.5 | `payload` | `5.14` | Weighted portfolio Return on Assets (ROA), 1-year trailing (Percentage (%)) |
| `return_on_assets_3yr` | `ratios` | 11,992 (58.9%) | 2018-05-31..2026-08-31 | -58.4 | 7.5 | 105 | 8.85 | `payload` | `4.37` | Weighted portfolio Return on Assets (ROA), 3-year annualized (Percentage (%)) |
| `return_on_capital` | `ratios` | 12,042 (59.1%) | 2018-05-31..2026-08-31 | -902 | 16.4 | 4.28e+03 | 19.4 | `payload` | `3.78` | Weighted portfolio Return on Capital (ROC) (Percentage (%)) |
| `return_on_capital_3yr` | `ratios` | 11,914 (58.5%) | 2018-05-31..2026-08-31 | -174 | 15.3 | 235 | 18.9 | `payload` | `9.99` | Weighted portfolio Return on Capital (ROC), 3-year annualized (Percentage (%)) |
| `return_on_equity_1yr` | `ratios` | 12,053 (59.2%) | 2018-05-31..2026-08-31 | -2.1e+03 | 24 | 1.87e+03 | 48.3 | `payload` | `15.39` | Weighted portfolio Return on Equity (ROE), 1-year trailing (Percentage (%)) |
| `return_on_equity_3yr` | `ratios` | 12,042 (59.1%) | 2018-05-31..2026-08-31 | -171 | 19.2 | 2.97e+06 | 632 | `payload` | `12.8` | Weighted portfolio Return on Equity (ROE), 3-year annualized (Percentage (%)) |
| `return_on_investment_1yr` | `ratios` | 12,053 (59.2%) | 2018-05-31..2026-08-31 | -198 | 14.6 | 994 | 17.3 | `payload` | `8.99` | Weighted portfolio Return on Investment (ROI), 1-year trailing (Percentage (%)) |
| `return_on_investment_3yr` | `ratios` | 11,991 (58.8%) | 2018-05-31..2026-08-31 | -65.5 | 12.8 | 1.4e+03 | 14.3 | `payload` | `7.79` | Weighted portfolio Return on Investment (ROI), 3-year annualized (Percentage (%)) |

---

## 4. Lipper Ratings Architecture (`source = 'lipper'`)

Lipper ratings contain **568 distinct `metric_id`s** in `silver.product_metrics`. They follow a structured compositional naming convention:

```
metric_id = lipper_{tag}_{horizon}_{country_universe}
```

- **Total Lipper Observations**: **1,064,394** across **11,650** funds.
- **Date Horizon**: 2010-10-29 to 2026-08-31.
- **Value Range**: Standardized ordinal integer scores from **1 (Lowest)** to **5 (Highest)**.

### Lipper Metric Dimensions Breakdown

#### A. Core Metric Tags (5 Evaluated Dimensions)
| Metric Tag | Description | Total Rows | Product Coverage |
| :--- | :--- | :--- | :--- |
| `preservation` | Capital preservation / downside risk resilience | 271,907 | 11,650 |
| `consistent_return` | Risk-adjusted return consistency vs peer category | 259,928 | 11,481 |
| `total_return` | Absolute total return percentile rank | 259,928 | 11,481 |
| `expense` | Fund fee efficiency / low expense relative to peers | 251,790 | 9,363 |
| `tax_efficiency` | Post-tax yield retention efficiency | 20,841 | 3,458 |

#### B. Time Horizons (4 Trailing Windows)
| Horizon Suffix | Meaning | Total Rows | Product Coverage |
| :--- | :--- | :--- | :--- |
| `overall` | Full-cycle multi-period blended evaluation | 328,714 | 11,650 |
| `3yr` | 3-Year annualized trailing performance window | 328,714 | 11,650 |
| `5yr` | 5-Year annualized trailing performance window | 266,990 | 9,214 |
| `10yr` | 10-Year annualized trailing performance window | 139,976 | 4,884 |

#### C. Top Geographic Universes (Out of 37 Distinct Universes)
| Universe / Country | Product Count | Row Count | Panel Modeling Guidance |
| :--- | :--- | :--- | :--- |
| `germany` | 5,388 | 68,036 | European / UCITS peer group |
| `uk` | 5,236 | 65,891 | European / UCITS peer group |
| `italy` | 5,138 | 64,473 | European / UCITS peer group |
| `luxembourg` | 5,109 | 64,600 | European / UCITS peer group |
| `france` | 5,050 | 63,939 | European / UCITS peer group |
| `netherlands` | 5,027 | 63,419 | Regional share-class peer group |
| `spain` | 4,998 | 63,089 | European / UCITS peer group |
| `sweden` | 4,949 | 61,981 | Regional share-class peer group |
| `austria` | 4,912 | 62,184 | Regional share-class peer group |
| `finland` | 4,904 | 61,461 | Regional share-class peer group |
| `switzerland` | 4,780 | 61,036 | Regional share-class peer group |
| `denmark` | 4,585 | 57,397 | Regional share-class peer group |
| `norway` | 4,258 | 53,237 | Regional share-class peer group |
| `united_states` | 3,462 | 102,947 | Primary US peer group |
| `singapore` | 2,218 | 28,384 | Regional share-class peer group |

---

## 5. Dimensions Overview (`silver.product_dimensions`)

| Dimension Type | Record Count | Distinct Names | Distinct Codes | Product Coverage | Universe % | Date Range | Date Provenance | Value Range |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `theme` | 5,096,954 | 491 | 491 | 10,884 | 53.4% | 2026-09-03..2026-09-13 | `snapshot` | [-0.0852, 2] |
| `top_holding` | 263,186 | 30,128 | 13,259 | 18,793 | 92.2% | 2015-01-31..2026-08-31 | `payload` | [-422, 420] |
| `country` | 181,379 | 107 | 103 | 18,792 | 92.2% | 2015-01-31..2026-08-31 | `payload` | [-420, 421] |
| `industry` | 137,885 | 14 | 0 | 13,026 | 63.9% | 2017-12-31..2026-08-31 | `payload` | [-0.407, 3.37] |
| `asset_class` | 76,011 | 4 | 0 | 18,792 | 92.2% | 2015-01-31..2026-08-31 | `payload` | [-421, 421] |
| `debt_type` | 55,331 | 110 | 0 | 5,691 | 27.9% | 2021-08-31..2026-08-31 | `payload` | [-0.897, 10.6] |
| `maturity` | 40,755 | 8 | 0 | 5,687 | 27.9% | 2021-08-31..2026-08-31 | `payload` | [-0.889, 11.3] |
| `credit_rating` | 38,379 | 12 | 12 | 5,691 | 27.9% | 2021-08-31..2026-08-31 | `payload` | [-1.27, 10.4] |
| `style_box_hist` | 20,698 | 9 | 9 | 8,427 | 41.4% | 2026-09-03..2026-09-13 | `snapshot` | [1, 1] |
| `style_box` | 3,790 | 9 | 9 | 1,388 | 6.8% | 2026-09-03..2026-09-13 | `snapshot` | [1, 1] |

---

## 6. Granular Dimension Breakdowns

### 6.1 `theme`
- **Purpose**: Factor-adjusted thematic exposure weights across 491 themes. dimension_code holds bronze.themes.theme_id UUID.
- **Value Semantics**: Rank-adjusted weight (factor sensitivity float; unadjusted weight omitted to avoid multicollinearity)
- **Code Usage (`dimension_code`)**: Holds canonical thematic UUIDs matching `bronze.themes.theme_id`. (491 distinct codes populated)

Top 8 most common categories in `theme`:
- `Digital Transformation`
- `Automotive Telematics`
- `Big Data`
- `5G Technology Support`
- `Warehouse Management`
- `Communication Tools`
- `AI In Healthcare`
- `Internet Of Things`

### 6.2 `top_holding`
- **Purpose**: Top 10 constituent holdings. dimension_name is '{ticker} - {name}' or '{name}'. dimension_code contains IBKR conids.
- **Value Semantics**: Fractional weight (0.0 to 1.0; negative for short positions)
- **Code Usage (`dimension_code`)**: Holds comma-separated IBKR contract IDs (conids) for instrument identification. (13259 distinct codes populated)

Top 8 most common categories in `top_holding`:
- `Other Assets less Liabilities`
- `NVDA - NVIDIA CORPORATION`
- `MSFT - MICROSOFT CORPORATION`
- `USD Cash`
- `AAPL - APPLE INC.`
- `AMZN - AMAZON.COM, INC.`
- `AVGO - BROADCOM INC.`
- `GOOGL - ALPHABET INC.`

### 6.3 `country`
- **Purpose**: Portfolio asset exposure partitioned across 107 countries. Includes 2-letter ISO country code in dimension_code.
- **Value Semantics**: Fractional weight (0.0 to 1.0; short exposure < 0.0)
- **Code Usage (`dimension_code`)**: Holds 2-letter ISO country codes (e.g., 'US', 'JP', 'GB'). (103 distinct codes populated)

Top 8 most common categories in `country`:
- `United States`
- `Unidentified`
- `United Kingdom`
- `Canada`
- `Netherlands`
- `France`
- `Ireland`
- `Switzerland`

### 6.4 `industry`
- **Purpose**: Equity allocation partitioned across 14 economic sectors (Technology, Financials, Healthcare, etc.).
- **Value Semantics**: Fractional weight (0.0 to 1.0; can be negative for short exposure)
- **Code Usage (`dimension_code`)**: Unused (`NULL`). (0 distinct codes populated)

| Category Name (`dimension_name`) | Observations | Product Count | Min Weight | Max Weight |
| :--- | :--- | :--- | :--- | :--- |
| `Technology` | 15,691 | 10,781 | -0.4075 | 2.001 |
| `Industrials` | 15,148 | 10,449 | -0.1414 | 1.293 |
| `Consumer Cyclicals` | 14,274 | 9,860 | -0.2619 | 2.001 |
| `Financials` | 13,473 | 9,261 | -0.211 | 2.001 |
| `Basic Materials` | 12,989 | 9,066 | -0.06909 | 1.301 |
| `Healthcare` | 12,893 | 8,888 | -0.1387 | 1.296 |
| `Consumer Non-Cyclicals` | 12,889 | 8,938 | -0.2924 | 1.265 |
| `Energy` | 12,372 | 8,559 | -0.04675 | 1.283 |
| `Utilities` | 11,373 | 7,912 | -0.1984 | 1.001 |
| `Real Estate` | 9,779 | 6,826 | -0.05121 | 1.011 |
| `Not Classified - Non Equity` | 5,117 | 3,230 | 1e-06 | 3.375 |
| `Non Classified Equity` | 1,445 | 1,194 | -0.001196 | 1.25 |
| `Academic & Educational Services` | 413 | 276 | -0.05207 | 0.113 |
| `Telecommunication Services-Discontinued eff 09/19/2020` | 29 | 29 | 8.6e-05 | 0.05616 |

### 6.5 `asset_class`
- **Purpose**: Broad portfolio allocation across macro asset classes (Cash, Equity, Fixed Income, Other).
- **Value Semantics**: Fractional weight (sum ~ 1.0; leveraged funds exceed 1.0; short hedges < 0.0)
- **Code Usage (`dimension_code`)**: Unused (`NULL`). (0 distinct codes populated)

| Category Name (`dimension_name`) | Observations | Product Count | Min Weight | Max Weight |
| :--- | :--- | :--- | :--- | :--- |
| `Cash` | 23,805 | 15,750 | -4.167 | 420.9 |
| `Other` | 20,637 | 14,136 | -421 | 2.914 |
| `Equity` | 18,283 | 12,525 | -1.118 | 2.162 |
| `Fixed Income` | 13,286 | 8,611 | -0.333 | 17.35 |

### 6.6 `debt_type`
- **Purpose**: Detailed debt classification across 110 debt instrument types (Corporate, Treasury, Senior Note, Muni, etc.).
- **Value Semantics**: Fractional weight (0.0 to 1.0)
- **Code Usage (`dimension_code`)**: Unused (`NULL`). (0 distinct codes populated)

Top 8 most common categories in `debt_type`:
- `Sovereign Bond`
- `CORP`
- `Bond`
- `Corporate Medium Term Notes`
- `Unsecured Note`
- `Senior Note`
- `Agencies`
- `Fixed Income`

### 6.7 `maturity`
- **Purpose**: Bond portfolio breakdown across 8 maturity duration buckets (< 1Y, 1-3Y, 3-5Y, 5-10Y, 10-20Y, 20-30Y, > 30Y, Other).
- **Value Semantics**: Fractional weight (0.0 to 1.0)
- **Code Usage (`dimension_code`)**: Unused (`NULL`). (0 distinct codes populated)

| Category Name (`dimension_name`) | Observations | Product Count | Min Weight | Max Weight |
| :--- | :--- | :--- | :--- | :--- |
| `% Maturity 1 to 3 Years` | 7,157 | 4,647 | -0.00026 | 6.009 |
| `% Maturity Less than 1 Year` | 7,148 | 4,647 | -0.889 | 11.33 |
| `% Maturity 3 to 5 Years` | 5,523 | 3,574 | -0.000385 | 1.001 |
| `% Maturity 5 to 10 Years` | 5,464 | 3,559 | -0.06774 | 3.232 |
| `% Maturity 10 to 20 Years` | 4,705 | 3,083 | -0.000374 | 1.914 |
| `% Maturity 20 to 30 Years` | 4,497 | 2,960 | -0.333 | 1.241 |
| `% Maturity Greater than 30 Years` | 3,652 | 2,411 | -0.04003 | 0.9998 |
| `% Maturity Other` | 2,609 | 1,719 | -0.2643 | 1 |

### 6.8 `credit_rating`
- **Purpose**: Bond holdings breakdown across standardized S&P/Moody's credit tiers (AAA down to D, Not Rated, Not Available).
- **Value Semantics**: Fractional weight (0.0 to 1.0)
- **Code Usage (`dimension_code`)**: Mirrors cleaned credit rating tier (e.g., `AAA`, `BBB`). (12 distinct codes populated)

| Category Name (`dimension_name`) | Observations | Product Count | Min Weight | Max Weight |
| :--- | :--- | :--- | :--- | :--- |
| `Not Available` | 7,472 | 4,832 | -0.333 | 6.943 |
| `AA` | 7,321 | 4,713 | -0.6266 | 10.41 |
| `BBB` | 4,912 | 3,176 | -0.03816 | 1.935 |
| `A` | 4,594 | 2,988 | -0.03577 | 2.187 |
| `AAA` | 3,912 | 2,511 | -1.274 | 1.914 |
| `BB` | 3,514 | 2,296 | -0.000485 | 1 |
| `Not Rated` | 2,082 | 1,431 | -1e-06 | 0.9998 |
| `B` | 1,780 | 1,174 | -0.000315 | 0.8563 |
| `CCC` | 1,397 | 930 | -0.000133 | 0.7269 |
| `D` | 725 | 499 | -1.4e-05 | 0.0773 |
| `CC` | 431 | 310 | -3e-06 | 0.02288 |
| `C` | 239 | 176 | 3e-06 | 0.04025 |

### 6.9 `style_box_hist`
- **Purpose**: Historical frequency coordinates occupied by fund in Morningstar style box grid.
- **Value Semantics**: Binary indicator flag (1.0 = occupied coordinate)
- **Code Usage (`dimension_code`)**: Holds normalized coordinate slugs (e.g., `large_core`, `mid_growth`). (9 distinct codes populated)

Top 8 most common categories in `style_box_hist`:
- `Mid Core`
- `Mid Growth`
- `Multi Core`
- `Multi Growth`
- `Large Core`
- `Multi Value`
- `Small Growth`
- `Small Value`

### 6.10 `style_box`
- **Purpose**: Active Morningstar 3x3 style coordinate (Size: Large/Mid/Small/Multi x Style: Value/Core/Growth).
- **Value Semantics**: Binary indicator flag (1.0 = active coordinate in selected grid)
- **Code Usage (`dimension_code`)**: Holds normalized coordinate slugs (e.g., `large_core`, `mid_growth`). (9 distinct codes populated)

| Category Name (`dimension_name`) | Observations | Product Count | Min Weight | Max Weight |
| :--- | :--- | :--- | :--- | :--- |
| `Mid Core` | 1,301 | 479 | 1 | 1 |
| `Mid Growth` | 716 | 267 | 1 | 1 |
| `Multi Core` | 527 | 194 | 1 | 1 |
| `Multi Growth` | 405 | 152 | 1 | 1 |
| `Small Growth` | 375 | 144 | 1 | 1 |
| `Large Core` | 184 | 69 | 1 | 1 |
| `Small Core` | 112 | 42 | 1 | 1 |
| `Small Value` | 99 | 37 | 1 | 1 |
| `Multi Value` | 71 | 27 | 1 | 1 |

---

## 7. Panel Creation Insights & LOCF Engineering Guidelines

When designing monthly LOCF (Last Observation Carried Forward) panels, consider the following structural nuances:

### 1. Effective Date Mechanics & Alignment
- **Month-End Dates (`payload`)**: Metrics from `ratios` and `holdings` are populated with month-end dates (e.g., `2026-07-31`, `2026-08-31`). These align naturally with monthly panel horizons.
- **Snapshot-Dated Dimensions (`snapshot`)**: `theme` weights and `style_box` coordinates use the date when the snapshot blob was ingested (early September 2026). In monthly LOCF, propagate these observations to all subsequent monthly panels.
- **Heterogeneous Report Dates (`item`)**: `profile.audited_net_expense_ratio` and `profile.total_net_assets_local` use fiscal year-end or report-level dates. LOCF carrying forward is required.

### 2. Multi-Currency Normalization for AUM
- `profile.total_net_assets_local`: Stored in the fund's **local denomination currency** (USD, EUR, JPY, GBP, etc.).
- **Recommendation**: To build cross-sectional size factors (log AUM), join with `silver.products.currency` and FX rates, or use within-currency percentile rankings.

### 3. Leveraged Funds & Negative Weights
- In `product_dimensions` (`asset_class`, `country`, `top_holding`), leveraged ETFs (e.g., 2x / 3x bull/bear) exhibit weights of **200% to 400%+** (`value > 2.0`), balanced by large negative cash or collateral offsets.
- Short hedge positions appear as negative weights (`value < 0.0`).
- **Recommendation**: Factor pipelines should either filter out inverse/leveraged ETFs (`stock_type` or leverage flags) or winsorize/normalize allocation exposures.

### 4. Lipper Geographic Partitioning
- Lipper ratings are evaluated relative to distinct geographical peer universes (`united_states`, `germany`, `uk`, etc.).
- **Recommendation**: For US-focused ETF factor models, select `lipper_*_*_united_states`. For global or UCITS universes, construct a composite by falling back across domicile-relevant universes.

### 5. Multicollinearity Prevention (Design Safeguards Already in Silver)
- `theme_weights`: Raw weight was excluded during extraction; only `rank_adjusted_weight` is present in silver.
- `holdings`: Redundant `currency` and `geographic` maps were omitted in favor of canonical `country` allocations.
- `mstar`: Pillars are segregated into `_analyst` vs `_quant` identifiers, enabling distinct factor testing.
