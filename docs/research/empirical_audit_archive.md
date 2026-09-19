# Empirical Audit Archive — Silver Prep & Factor Panel

- **Status:** Research archive (not an implementation spec)
- **Origin:** FRD §4.x.1 payload-exploration sessions (and empirical subsections of §4.5)
- **Implementation specs:** [../specs/00_silver_contracts.md](../specs/00_silver_contracts.md) · [../specs/01_silver_extractors.md](../specs/01_silver_extractors.md) · [../specs/02_factor_panel.md](../specs/02_factor_panel.md)

This file retains the empirical audit trail that **justifies** the cleaning rules. Downstream coding agents should not load it unless they need to re-validate a bound, cap, or cadence choice.

Guiding hierarchy used during exploration: Data Accuracy / Correctness >> Simplicity > Storage Efficiency > Runtime Performance >> Auth Security.

---

## Session 1 — `profile` & Fees

- **Endpoint:** `/tws.proxy/fundamentals/mf_profile_and_fees/`
- **Bronze:** 42,406 snapshots

### A. Metric statistical distributions

| Metric ID | Table Source Key | Obs | Unique ETFs | Date Source | Min | P25 | Median | P75 | Max | Anomalies & Outliers |
| :--- | :--- | ---: | ---: | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `total_net_assets_local` | `fund_and_profile` (`Total_Net_Assets_Month_End`) | 33,625 | 19,176 | `item` (100%) | 0.0 | 2.22e+07 | 1.31e+08 | 7.84e+08 | 3.36e+13 | 1 zero (liquidated `CAD0`); min positive $19.08 (seed); mega-AUMs in local FX include ¥33.64T JPY (NEXT FUNDS TOPIX 1306), ₩24.92T KRW (SAMSUNG KODEX 200 069500), ¥17.15T JPY (NF NIKKEI 225 1321). Dollar-prefixed records reach $1.05T. |
| `total_expense_ratio` | `fund_and_profile` (`Total_Expense_Ratio`) | 16,806 | 6,189 | `snapshot` (100%) | 0.0000 | 0.0031 | 0.0055 | 0.0079 | 0.1349 | 35 zero-fee ETFs (100% waivers, e.g. HODL); max 13.49% (PBDC) and 12.44% (FBDC) are valid SEC AFFE for BDCs. |
| `management_expense_ratio` | `expenses_allocation` (`Management Expenses`) | 12,213 | 4,560 | `snapshot` (100%) | −95.0531 | 0.9785 | 1.0000 | 1.0000 | 1.9584 | 191 negative ratios down to −95.05 (−9505.31%) from advisor waivers exceeding gross fees on low-net-expense funds (FPAS, TAGS). |
| `non_management_expense_ratio` | `expenses_allocation` (`Non-Management Expenses`) | 12,213 | 4,560 | `snapshot` (100%) | −0.9584 | 0.0000 | 0.0000 | 0.0215 | 96.0531 | 78 negative (down to −95.84%); 6,330 zeros; compensating positives up to 96.05 ensuring sum with management = 1.0. |
| `audited_net_expense_ratio` | `reports` (`Annual Report` → `Total Net Expense`) | 4,830 | 4,597 | `item` (100%) | 0.0000 | 0.0039 | 0.0069 | 0.0105 | 0.0509 | 37 zero ratios (full waivers); max 5.0911% (leveraged/alternative). |
| `manager_tenure_years` | `fund_and_profile` (`Manager_Tenure`) | 23,277 | 11,372 | `snapshot` (100%) | 0.0192 | 1.4565 | 3.6715 | 8.6708 | 33.9493 | 100% parseable `YYYY/MM/DD`; min ~7 days; max 33.95 years (lead manager since Oct 1992). |
| `is_passive` | `fund_and_profile` (`Management_Approach`) | 39,694 | 20,024 | `snapshot` (100%) | 0.0 | 0.0 | 1.0 | 1.0 | 1.0 | 24,087 Passive (60.7%), 15,607 Active (39.3%). No unknown categories. |

AUM currency tokens observed: 26 distinct prefixes. `$` disambiguation: 23 CAD (TSE/Canadian) vs 14,986 USD-domiciled; UCITS often report AUM in USD base despite EUR/GBP/MXN/CHF listings (2,552 / 2,364 / 1,028 / 452). Australian ETFs: 877 snapshots, all explicit `AUD`, zero `$`.

### B. Style-box coverage

From `mstar` in profile payloads: 3 value tiers × 4 cap tiers → 9 observed coordinates across 23,888 dimension rows. Unobserved: `large_growth`, `large_value`, `mid_value`.

| Coordinate | selected | hist | Distinct ETFs (union) |
| :--- | ---: | ---: | ---: |
| `mid_core` | 1,301 | 7,834 | 5,010 |
| `multi_core` | 527 | 3,151 | 2,147 |
| `mid_growth` | 716 | 3,213 | 2,074 |
| `multi_growth` | 405 | 2,578 | 1,663 |
| `large_core` | 184 | 2,556 | 1,550 |
| `multi_value` | 71 | 599 | 379 |
| `small_growth` | 375 | 378 | 346 |
| `small_value` | 99 | 266 | 184 |
| `small_core` | 112 | 123 | 108 |

### C. Cadence & update deltas

Consecutive point-in-time deltas per `product_id`:

| Metric group | N pairs | Min | P25 | Median | P75 | P95 | Max | Empirical frequency |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `total_net_assets_local` | 14,449 | 14d | 31d | **31.0d** | 31d | 31d | 31d | Strict monthly (12,898 Δ=31d, 1,549 Δ=28d) |
| `audited_net_expense_ratio` | 233 | 214d | 365d | **365.0d** | 365d | 365d | 792d | Annual (225/233 exactly 365d) |
| Snapshot attrs (TER, tenure, approach) | ~10k–20k | 2d | 2d | **7.0–7.3d** | 9d | 10d | 10d | Weekly crawler snapshot cadence |

**Staleness caps chosen:** AUM 180d; TER / audited NER / expense split 540d; tenure 365d; `is_passive` perpetual; style box 365d.

---

## Session 2 — `ratios` Fundamentals

- **Endpoint:** `/tws.proxy/fundamentals/mf_ratios_fundamentals/`
- **Bronze:** 42,055 snapshots / 20,024 products
- **Silver baseline:** 748,780 metric rows / 16,538 products / 47 `metric_id`s

### A. Payload structure & date provenance

- Top-level `as_of_date` present in 38,064 / 42,055 (90.5%). The 3,991 without it contain **zero metric items** — empty payloads. Therefore **100% of metric-bearing payloads have a valid `as_of_date`.**
- Format: Unix epoch ms (e.g. `1785470400000` → `2026-07-31 UTC`).
- Silver `effective_date_source`: 100% `'payload'`. Fallback never exercised on metric-bearing payloads.
- Effective dates: month-end business days; 39 distinct dates `2015-01-31` … `2026-08-31`. Concentration: 14,316 products on `2026-07-31`, 7,568 on `2026-08-31`.
- Error payloads (`IDENTIFICATION_PROBLEM`): **zero** in current Bronze. Empty payloads: 3,991, correctly yield zero Silver rows.
- `title_vs` present in 15,949 snapshots (peer label). Not extracted. Peer columns `vs/min/max/avg/percentile` not extracted.
- Item `value` is already a float; `value_fmt` is the display string. No nulls or missing tags in the entire Bronze store. `currency` always NULL.

### B. Valuation multiples (`ratios` section)

| Metric ID | Human name | Obs | Unique ETFs | Min | P25 | Median | P75 | Max | Neg | Zero | Provider cap | Notes |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `price_sales` | Price/Sales | 17,540 | 11,998 | 0.09 | 3.88 | 6.12 | 8.77 | **50.00** | 0 | 0 | 50.0 | 7 at exact cap |
| `price_cash` | Price/Cash | 17,529 | 11,993 | 1.70 | 15.77 | 20.97 | 26.19 | **60.00** | 0 | 0 | 60.0 | 29 at exact cap |
| `price_book` | Price/Book | 17,587 | 12,027 | 0.50 | 4.26 | 6.31 | 10.15 | **25.00** | 0 | 0 | 25.0 | 43 at exact cap |
| `price_earnings` | Price/Earnings | 17,602 | 12,041 | 0.68 | 23.95 | 28.52 | 32.85 | **60.00** | 0 | 0 | 60.0 | 70 at exact cap |
| `price_to_dividend` | Price to Dividend | 17,470 | 11,961 | 7.29 | 87.98 | 158.90 | 354.07 | 5,018.75 | 0 | 0 | none | Extreme highs from near-zero-dividend funds |
| `relative_strength` | Relative Strength | 17,628 | 12,056 | −49.17 | 0.01 | 2.20 | 4.61 | 155.44 | 4,391 | 0 | none | Signed; dislocations |

**Finding:** Morningstar/IBKR applies hard upper bounds on P/S, P/Cash, P/B, P/E. No negative valuation multiples exist. Caps are upstream — retain as ceiling-censored.

### C. EPS & revenue growth (percentage-point scale)

| Metric ID | Human name | Obs | Min | P25 | Median | P75 | Max | Neg | Provider caps |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `eps_growth_1yr` | EPS Growth 1yr | 17,624 | **−50.00** | 14.82 | 22.72 | 29.92 | **100.00** | 511 | [−50, 100] (26 at −50, 61 at 100) |
| `eps_growth_3yr` | EPS Growth 3yr | 17,548 | **−50.00** | 9.72 | 15.86 | 21.76 | **100.00** | 1,064 | [−50, 100] (4 at −50, 17 at 100) |
| `eps_growth_5yr` | EPS Growth 5yr | 17,561 | −43.65 | 14.64 | 19.57 | 24.44 | **100.00** | 177 | [~, 100] (5 at 100) |
| `sales_growth_1_year` | Sales Growth 1yr | 17,608 | −18.46 | 7.72 | 13.10 | 17.42 | **100.00** | 218 | [~, 100] (5 at 100) |
| `sales_growth_3_year` | Sales Growth 3yr | 17,621 | −19.45 | 6.23 | 10.80 | 15.69 | **100.00** | 453 | [~, 100] (15 at 100) |
| `sales_growth_5_yr` | Sales Growth 5yr | 17,617 | −7.46 | 9.77 | 13.68 | 16.95 | 74.85 | 14 | none observed |
| `sales_per_share_growth_1_year` | SPS Growth 1yr | 17,608 | −28.59 | 8.68 | 14.59 | 20.46 | **19,862.17** | 290 | none (uncapped) |
| `sales_per_share_growth_3_year` | SPS Growth 3yr | 17,621 | −26.95 | 6.76 | 10.79 | 16.03 | 249.16 | 554 | none |
| `operating_cash_flow_growth_rate_3yr` | OCF Growth 3yr | 17,553 | −39.60 | 12.07 | 19.37 | 26.53 | 237.53 | 515 | none |

**Finding:** All growth rates are **percentage points**, not decimals. Decision: `/100.0` at extraction. SPS 1yr outlier 19,862% = GROW (Schroder Real Return, Aug 2021, restructuring artefact).

### D. Profitability & returns

| Metric ID | Obs | Min | P01 | Median | P99 | Max | Neg | Notes |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `return_on_assets_1yr` | 17,624 | −91.94 | −10.15 | 8.57 | 31.57 | 102.13 | 617 | Loss-making biotech/cannabis |
| `return_on_assets_3yr` | 17,548 | −58.35 | — | 7.50 | — | 104.59 | 672 | |
| `return_on_equity_1yr` | 17,624 | **−2,103.39** | −30.36 | 24.01 | 372.54 | **1,865.97** | 703 | Levered split-corp tails |
| `return_on_equity_3yr` | 17,603 | −170.92 | — | 19.16 | — | **2,971,172.92** | 709 | SPLT (Brompton Split Corp Pref) |
| `return_on_investment_1yr` | 17,622 | −197.56 | — | 14.55 | — | 993.60 | 535 | |
| `return_on_investment_3yr` | 17,546 | −65.54 | — | 12.77 | — | 1,397.67 | 612 | |
| `return_on_capital` | 17,606 | −901.57 | — | 16.39 | — | 4,277.95 | 57 | |
| `return_on_capital_3yr` | 17,418 | −174.30 | — | 15.32 | — | 235.48 | 16 | |

**Finding:** ROE 3yr 2,971,173% on SPLT is mechanically correct (TA/TE = 263,821×). Winsorize at panel, store raw in Silver.

### E. Leverage / financial health

| Metric ID | Obs | Min | P25 | Median | P75 | Max | Neg | Notes |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `ebit_to_interest` | 17,582 | −5,235.26 | 26.12 | 76.69 | 138.77 | **1,303,456.78** | 500 | Max: Chinese small-cap ETF |
| `lt_debt_shareholders_equity` | 17,587 | 0.0008 | 0.51 | 0.75 | 0.95 | 25.98 | 0 | |
| `total_assets_total_equity` | 17,622 | 1.01 | 3.17 | 4.25 | 5.65 | **263,821.09** | 0 | SPLT |
| `total_debt_total_capital` | 17,604 | 0.002 | 0.69 | 0.95 | 1.22 | 26.14 | 0 | >1.0 when negative equity |
| `total_debt_total_equity` | 17,609 | 0.002 | 0.33 | 0.39 | 0.45 | 3.83 | 0 | |
| `sales_to_total_assets` | 17,622 | 0.008 | 0.53 | 0.65 | 0.75 | 3.94 | 0 | |

### F. Dividend metrics

| Metric ID | Obs | Min | Median | Max | Neg | Zero | Notes |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `dividend_yield_weighted_average` | 15,091 | −0.08 | 1.57 | 15.79 | 1 | 21 | 1 negative (PGRX −0.08%); 21 zeros (biotech/cannabis/crypto). Equity peer-universe only (present in `ratios_equity.json`, absent from `ratios_complete.json`). |
| `dividendpayoutratio5yr` | 17,474 | 0.61 | 50.34 | **4,473.22** | 0 | 0 | Clean-energy extremes (TAN 2,195%, ACES 2,664%) |
| `price_to_dividend` | 17,470 | 7.29 | 158.90 | 5,018.75 | 0 | 0 | Near-zero-yield ETFs |
| `dividend_per_share_1yr` | 17,427 | −100.00 | 15.17 | 228.24 | 589 | 0 | −100% = dividend elimination; percentage points |
| `dividend_per_share_3yr` | 17,384 | −37.03 | 13.13 | 172.69 | 281 | 0 | |

### G. Fixed-income metrics

| Metric ID | Obs | Unique ETFs | Min | Median | Max | Neg | Notes |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `yield_to_maturity` | 7,460 | 4,830 | −8.73 | 4.36 | **10.00** | 24 | Cap at 10.0 (15 records, e.g. IPRV, BLTN). Negative YTM: convertibles (CVRT −8.73%, MCVT −4.78%). |
| `nominal_maturity` | 7,466 | 4,834 | −0.96 | 7.44 | 96.25 | 4 | Years. Negative: crypto/deriv ETFs (SETH, BETE, BETH −0.96yr). |
| `effective_maturity` | 7,466 | 4,834 | −5.13 | 6.90 | 58.97 | 5 | Years. Negative: Fubon S&P US Pref (−5.13yr) + crypto. |
| `average_coupon` | 6,673 | 4,349 | 0.13 | 4.17 | 14.38 | 0 | Percentage points |
| `average_quality` | 6,592 | 4,320 | 3.00 | 7.16 | **10.00** | 0 | **Non-numeric `value_fmt`** — only such metric |

**`average_quality` dual representation** (numeric `value` vs letter `raw_value`):

| `raw_value` | Numeric range | Nominal midpoint |
| :--- | :--- | ---: |
| `-` (Not Rated / N/A) | 10.0 | 10 |
| `AAA` | 9.0 | 9 |
| `AA` | [8.0, 9.0) | 8 |
| `A` | [7.0, 8.0) | 7 |
| `BBB` | [6.0, 7.0) | 6 |
| `BB` | [5.0, 6.0) | 5 |
| `B` | [4.0, 5.0) | 4 |
| `CCC` | [3.0, 4.0) | 3 |
| `CC` | [2.99, 3.0) | — |

33 records: `raw_value='-'`, `value=10.0` — commodity/crypto with no rated bonds (IGLN, GOLD, Bitcoin ETFs). Provider assigns max score 10.0. Panel must **not** treat as best quality.

**FI metrics on equity-dominant ETFs** (latest snapshot Equity weight > 0.5): 205–254 unique ETFs (coupon 205, quality 206, YTM 253, effective/nominal maturity 254). Historical union across all dates inflated to 305 via multi-snapshot accumulation. These are balanced / target-date / multi-asset — retain.

### H. Z-scores

| Metric ID | Obs | Min | P25 | Median | P75 | Max |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `average_final_composite_zscore` | 15,091 | −1.35 | −0.16 | −0.01 | 0.14 | 1.74 |
| `latest_composite_z_score` | 15,091 | −1.47 | −0.19 | −0.01 | 0.13 | 1.74 |
| `latest_dividend_yield_zscore` | 15,091 | −6.65 | −0.24 | −0.06 | 0.25 | 1.66 |
| `latest_price_to_book_zscore` | 15,091 | −1.33 | −0.25 | −0.09 | 0.09 | 3.03 |
| `latest_price_to_earnings_zscore` | 15,089 | −1.33 | −0.17 | −0.02 | 0.21 | 2.36 |
| `latest_price_sales_zscore` | 15,087 | −1.80 | −0.27 | −0.13 | 0.05 | 7.03 |
| `latest_return_on_equity_zscore` | 15,091 | −2.96 | −0.23 | −0.07 | 0.05 | 4.15 |
| `latest_sps_growth_zscore` | 15,091 | −2.61 | −0.19 | −0.07 | 0.06 | 3.74 |
| `weighted_final_composite_zscore` | 15,091 | −1.35 | −0.17 | −0.01 | 0.13 | 1.74 |

**Naming trap:** `latest_composite_z_score` uses `_z_score`; others use `_zscore`. `LIKE '%zscore'` drops it.

7 of 9 z-scores have exactly 15,091 obs — matching `dividend_yield_weighted_average` (Morningstar equity peer universe). P/E z drops 2; P/S z drops 4.

### I. Product-date completeness cohorts

| Cohort | Metric count | Product-dates | Description |
| :--- | ---: | ---: | :--- |
| Bond-only | 5 | 6,105 | Only the 5 `fixed_income` metrics |
| Equity (no z-scores) | 32 | 1,801 | `ratios` + `financials` + `dividend` |
| Equity (full) | 42 | 14,899 | All sections including z-score + div yield |
| Mixed / balanced | 37–47 | ~560 | Partial FI + full equity |

### J. Cadence

| | |
| :--- | :--- |
| Product-date pairs with prior obs | 7,992 |
| Min / P5 / P25 / **median** / P75 / P95 / max | 30 / 31 / 31 / **31.0** / 31 / 31 / 853 days |
| Mode | 31d (7,925 / 7,992 = **99.2%**) |
| Long tail | 62d (19 pairs, skip 1 mo); 91–92d (12 pairs, skip 2 mo); 822–853d (4 pairs, ~2.3y halted/reorg) |

**Staleness:** uniform **180d** for all 47 ratios metrics (single `as_of_date`, no metric-level cadence variation).

---

## Session 3 — `holdings` Allocations

- **Endpoint:** `/tws.proxy/fundamentals/mf_holdings/`
- **Bronze:** 47,401 snapshots / 22,634 products
- **Silver baseline:** `portfolio_top_10_concentration` 27,919 rows / 18,793 products; dimensions: `asset_class` 76,011; `country` 181,379; `industry` 137,885; `credit_rating` 38,379; `debt_type` 55,331; `maturity` 40,755; deprecated `top_holding` 263,186 / 30,128 names

### A. Payload structure & date provenance

- `as_of_date` present in 27,919 / 47,401 (58.9%), all positive Unix ms.
- Remaining 19,482 lack `as_of_date` or have `{"as_of_date": 0}` with empty lists → zero Silver rows.
- `effective_date_source`: 100% `'payload'` on populated metric/dimension rows.
- Dates `2015-01-31` … `2026-08-31`, calendar month-ends. Metrics↔dimensions alignment: 2 product-dates have concentration without dimensions; 0 the reverse.
- Discarded: `currency` breakdown (collinear with country / reporting FX); `geographic` regions (collinear with country).

### B. `portfolio_top_10_concentration`

| Obs | Unique ETFs | Min | P01 | P25 | Median | P75 | P99 | Max |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 27,919 | 18,793 | 0.0078 | 0.0256 | 0.2509 | 0.4477 | 0.7971 | 1.3245 | **17.6872** |

990 observations (671 ETFs, 3.5%) exceed 1.0.

Root causes:

1. **Leveraged & inverse:** Direxion 3× (`EDC` 3.93, `MEXX` 3.58, `YINN` 3.52) — TRS notional 300% of NAV.
2. **Gross collateral overlays:** `JMXT`/`JMEX` (Janus Henderson Mexican Sovereign, 17.69); `THFA`/`TFGD` (JH FA ESG Active Core, 7.38) — sovereign debt + repos + FX-hedge collateral.

No negatives or zeros. Min 0.78% (broad equal-weight).

### C. Allocation sum-to-1.0 audit

| Dimension | Snapshots | Unique ETFs | Exact ±0.1% | Valid ±2.0% | Lev >105% | Under <95% | Min sum | Max sum | Dates w/ shorts |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `asset_class` | 27,917 | 18,792 | **100.0%** | **100.0%** | 0 | 0 | 1.0000 | 1.0000 | 6,792 (24.3%) |
| `country` | 27,917 | 18,792 | **63.58%** | **74.18%** | 87 | 5,365 | 0.0297 | 2.1306 | 3,052 (10.9%) |
| `industry` | 19,156 | 13,026 | **19.81%** | **75.72%** | 524 | 2,532 | −0.4331 | 3.3748 | 41 (0.2%) |
| `credit_rating` | 8,839 | 5,691 | **14.79%** | **53.76%** | 192 | 2,922 | −0.3330 | 17.3540 | 36 (0.4%) |
| `debt_type` | 8,839 | 5,691 | **10.44%** | **45.51%** | 159 | 3,315 | −0.3330 | 17.3540 | 27 (0.3%) |
| `maturity` | 8,835 | 5,687 | **14.63%** | **53.63%** | 190 | 2,917 | −0.3330 | 17.3540 | 38 (0.4%) |

#### `asset_class` (strict invariant)

100.0% of product-dates sum to \(1.000000 \pm 0.000015\).

| Sleeve | Present | Negatives | Notes |
| :--- | ---: | ---: | :--- |
| `Cash` | 23,805 dates | 2,080 (7.4%) down to −4.17 | 429 dates > +1.0 (up to +420.87 in inverse levered: 100% cash + short swaps) |
| `Other` | 20,637 | 5,037 (18.0%) down to −420.97 | MtM liability of short derivative swaps |
| `Equity` | 18,283 | 17 dates down to −1.12 | Median 0.9960; 1,173 dates > 1.0 (up to 2.16) |
| `Fixed Income` | 13,286 | 20 dates (short hedges) | Median 0.7700; 889 dates > 1.0 (up to 17.35, e.g. `UDN`) |

**Levered short exemplar:** `RKLZ` (Defiance Daily Target 2X Short RKLB, 2026-08-31): Cash = +420.87, FI = +1.11, Other = −420.97, sum = 1.000000.

#### `country`

Median sum 1.0; 70.3% exact 1.0; remainder truncated (P01 sum 0.68). 3,052 dates have negatives, primarily `Unidentified` (down to −420.22) as derivative offset.

107 names; 104 mapped codes; 3 with `dimension_code = NULL` before remap:

| Name | Rows | Median weight | Issue |
| :--- | ---: | ---: | :--- |
| `Unidentified` | 17,546 | — | Keep code NULL (swaps/synthetics) |
| `Guam` | 121 | 0.31% | ISO `GU` |
| `Uzbekistan` | 6 | 1.26% | ISO `UZ` |

Collisions / non-standard:

- `CR` assigned to both Costa Rica (keep `CR`, 25 rows) and Croatia (remap `HR`, 1 row).
- Bulgaria emitted as `BGR` (alpha-3) → remap `BG` (5 rows).
- `Korea` (`KR`) = South Korea; `Virgin Islands (U.S.)` (`VI`) = USVI.

#### `industry`

Populated 19,156 product-dates (equity + mixed; pure bond has none). Median sum 0.9963; 64.6% sum to 1.0 ± 0.01.

Residuals:

- `Not Classified - Non Equity`: 5,117 rows, median 2.26%, max 3.37 (bond/cash/commodity sleeve of multi-asset).
- `Non Classified Equity`: 1,445 rows, median 0.85%, max 1.25.
- `Telecommunication Services-Discontinued eff 09/19/2020`: 29 rows (pre-GICS reclass).

#### `credit_rating`

8,839 product-dates (FI). Median sum 0.9861; 39.6% within 1.0 ± 0.01. High-yield: `D` in 725 dates, `C` in 239, `CC` in 431.

#### `maturity`

8,835 product-dates (matches credit). Median sum 0.9860.

| Bucket | Rows |
| :--- | ---: |
| `% Maturity Less than 1 Year` | 7,148 |
| `% Maturity 1 to 3 Years` | 7,157 |
| `% Maturity 3 to 5 Years` | 5,523 |
| `% Maturity 5 to 10 Years` | 5,464 |
| `% Maturity 10 to 20 Years` | 4,705 |
| `% Maturity 20 to 30 Years` | 4,497 |
| `% Maturity Greater than 30 Years` | 3,652 |
| `% Maturity Other` | 2,609 |

#### `debt_type`

8,839 product-dates; **110 distinct unstructured names** (e.g. `Bundesanleihen`, `Dutch State Loan`, `Gilt Treasury Stock`, `Sovereign Bond`, `CORP`, `Corporate Medium Term Notes`, `ABSY`, `CMO Whole Loan`). 110 sparse columns → rank deficiency. 9-cluster map covers 100% (see panel spec).

### D. `top_holding` burden

263,186 rows / 18,793 ETFs / 30,128 names. 52.1% are single-fund singletons. Zero cross-sectional factor value. Concentration preserved by `portfolio_top_10_concentration`.

### E. Cadence

| | |
| :--- | :--- |
| N consecutive pairs | 9,126 |
| Min / P1 / P5 / P25 / **median** / P75 / P95 / P99 / max | 30 / 31 / 31 / 31 / **31.0** / 31 / 31 / 31 / 853 days |
| Mode | 31d (9,044 / 9,126 = **99.1%**) |
| Long tail | 62d (23 pairs); 91–92d (17 pairs); 822–853d (4 pairs) |

**Staleness:** uniform **180d** for concentration + all 6 allocation families (synchronous monthly `as_of_date`).

---

## Session 4 — `mstar` & `lipper` Evaluative Ratings

- **Morningstar Bronze:** 35,072 snapshots / 22,634 products (32,541 metric-bearing, 2,531 empty, 0 HTTP errors)
- **Lipper Bronze:** 25,544 snapshots / 22,634 products (14,519 metric-bearing, 11,025 empty)
- **Silver baseline:** mstar 84,221 rows / 16,454 products / 10 `metric_id`s; lipper 1,064,394 rows / 11,650 products / **568** `metric_id`s across 37 universes

### A. Morningstar payload architecture

Valid keys: `as_of_date`, `commentary`, `q_full_report_id`, `summary`.

- Top-level `as_of_date`: 17,157 snapshots as 8-digit `"20260731"`; absent in 15,384 valid payloads.
- Pillar `publish_date`: **99.996%** of Silver rows use `effective_date_source='item'` (84,218 / 84,221); 3 fall back to snapshot.
- Asynchronous pillars on one snapshot (SPY / `mstar_equity.json` example):
  - `morningstar_rating` 2026-07-31 (monthly)
  - `sustainability_rating` 2026-06-30 (monthly)
  - `medalist_rating` / `process` 2026-04-27 (analyst report)
  - `parent` 2025-07-16 (firm annual review, 9 months earlier)
- Discarded: `commentary` (narrative), `q_full_report_id` (hex), `category` (512 labels), `category_index` (415 benchmarks).
- Non-numeric skips observed: `medalist_rating='Under_Review'` (2×), `people='Not_Applicable'` (2×), `process='Not_Applicable'` (2×).

### B. Medalist analyst-vs-quant bug — pillar composition

`medalist_rating` always arrives `q: false` / omitted. Legacy extractor labeled 10,755 as `_analyst` and 4 as `_quant` (obsolete `quantitative_rating` tag, early 2023).

Latest snapshot per fund (10,746 unique ETFs with Medalist) and historical snapshots (18,881):

| Profile (People-Process-Parent) | Category | Latest funds | % universe | Historical snaps | % snaps |
| :--- | :--- | ---: | ---: | ---: | ---: |
| A-Q-A | Hybrid | 4,119 | **38.3%** | 7,138 | 37.8% |
| Q-Q-A | Hybrid | 2,603 | **24.2%** | 4,711 | 25.0% |
| A-A-A | Pure analyst | 2,404 | **22.4%** | 4,124 | 21.8% |
| Q-Q-Q | Pure quant | 1,414 | **13.2%** | 2,567 | 13.6% |
| Q-A-A | Hybrid | 137 | **1.3%** | 234 | 1.2% |
| Q-A-Q | Hybrid | 46 | **0.4%** | 69 | 0.4% |
| A-A-Q | Hybrid | 12 | **0.1%** | 21 | 0.1% |
| A-Q-Q | Hybrid | 11 | **0.1%** | 17 | 0.1% |
| **Total** | | **10,746** | **100%** | **18,881** | **100%** |

64.4% hybrid / 22.4% analyst / 13.2% quant. Hybrid dominance: institutional parents (BLK, Vanguard, STT) get firm-level qualitative reviews; index processes are algorithmic.

### C. Morningstar distributions (current Silver, pre-fix IDs)

| Metric ID | Obs | Unique ETFs | Min | P05 | P25 | Median | P75 | P95 | Max |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `mstar_medalist_rating_analyst` (mislabeled) | 10,755 | 10,742 | 1.0 | 2.0 | 2.0 | **3.0** | 4.0 | 5.0 | 5.0 |
| `mstar_medalist_rating_quant` (legacy tag) | 4 | 4 | 3.0 | 3.2 | 3.8 | **4.0** | 4.3 | 4.9 | 5.0 |
| `mstar_morningstar_rating` | 14,067 | 9,233 | 1.0 | 1.0 | 3.0 | **3.0** | 4.0 | 5.0 | 5.0 |
| `mstar_sustainability_rating` | 27,128 | 15,742 | 1.0 | 1.0 | 2.0 | **3.0** | 4.0 | 5.0 | 5.0 |
| `mstar_people_analyst` | 6,557 | 6,546 | 2.0 | 3.0 | 4.0 | **4.0** | 4.0 | 4.0 | 5.0 |
| `mstar_people_quant` | 4,202 | 4,202 | 1.0 | 2.0 | 3.0 | **3.0** | 3.0 | 4.0 | 5.0 |
| `mstar_process_analyst` | 2,610 | 2,599 | 1.0 | 2.0 | 3.0 | **4.0** | 4.0 | 5.0 | 5.0 |
| `mstar_process_quant` | 8,149 | 8,149 | 1.0 | 1.0 | 2.0 | **3.0** | 3.0 | 4.0 | 5.0 |
| `mstar_parent_analyst` | 9,266 | 9,264 | 2.0 | 2.0 | 3.0 | **3.0** | 4.0 | 5.0 | 5.0 |
| `mstar_parent_quant` | 1,483 | 1,483 | 1.0 | 1.0 | 2.0 | **2.0** | 3.0 | 4.0 | 5.0 |

```
mstar_medalist_rating:
  Gold     (5.0):  1,773 ( 9.4%)  ███
  Silver   (4.0):  1,371 ( 7.3%)  ██
  Bronze   (3.0):  2,716 (14.4%)  █████
  Neutral  (2.0):  4,391 (23.3%)  ████████
  Negative (1.0):    508 ( 2.7%)  █
  [Unrated/None]:  8,122 (43.0%)  (active/uncovered)

mstar_morningstar_rating:
  5 Stars  (5.0):  1,794 (12.8%)  ████
  4 Stars  (4.0):  4,025 (28.6%)  █████████
  3 Stars  (3.0):  4,888 (34.7%)  ███████████
  2 Stars  (2.0):  2,368 (16.8%)  █████
  1 Star   (1.0):    992 ( 7.1%)  ██

mstar_sustainability_rating:
  High          (5.0):  3,022 (11.1%)  ████
  Above Average (4.0):  5,653 (20.8%)  ███████
  Average       (3.0): 11,088 (40.9%)  ██████████████
  Below Average (2.0):  5,260 (19.4%)  ███████
  Low           (1.0):  2,105 ( 7.8%)  ███
```

Pillar asymmetry:

- Analyst People: 5,600 / 6,557 = 85.4% `Above_Average` (institutional desks). Quant People symmetric around `Average` (65.5%).
- Analyst Process median 4.0; Quant Process median 3.0 with 31.3% `Below_Average`.
- Analyst People/Parent min = 2.0; quant extends to 1.0.

### D. Lipper cardinality explosion

Universes per payload:

- 1 universe: 5,522 products (47.6%) — domestic US/CA/JP/AU
- 2–10: 1,220 (10.5%)
- 11–22: 4,865 (41.9%) — EU UCITS cross-registered

Legacy `metric_id = f"lipper_{tag}_{horizon}_{country_name}"` → 37 universes × 5 tags × 4 horizons = **568 IDs**, 1,064,394 rows. Extreme sparsity (~80–90% empty per country column) and EU cross-listing multicollinearity.

Cross-universe consistency (6,231 multi-universe snapshots):

| | Snapshots | Share |
| :--- | ---: | ---: |
| Identical ratings across countries | 3,205 | 51.4% |
| Differ by ±1 notch | 2,439 | 39.1% |
| Differ by 2 notches | — | 7.0% |
| Differ by 3–4 notches | — | 1.1% (almost only US vs tiny satellite, e.g. Peru N≈15) |

### E. Canonical Lipper distributions (pre-collapse, pooled)

| Metric tag | Obs | Unique ETFs | Min | P25 | Median | P75 | Max | % Leader (5) | % Score=1 |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `total_return` | 80,485 | 11,481 | 1.0 | 2.0 | **3.0–4.0** | 5.0 | 5.0 | 28.6–34.2% | 9.7–14.6% |
| `consistent_return` | 80,485 | 11,481 | 1.0 | 2.0 | **3.0–4.0** | 5.0 | 5.0 | 28.9–35.5% | 14.0–16.6% |
| `preservation` | 83,635 | 11,650 | 1.0 | 2.0 | **3.0** | 4.0 | 5.0 | 24.4–26.6% | 19.4–24.1% |
| `expense` | 77,786 | 9,363 | 1.0 | 5.0 | **5.0** | 5.0 | 5.0 | **82.1–85.7%** | 0.4–0.6% |
| `tax_efficiency` | 6,323 | 3,458 | 1.0 | 2.0 | **4.0** | 5.0 | 5.0 | 37.6–43.8% | 10.0–14.6% |

Anomalies:

1. **Expense clustering:** 82–85% of ETFs score 5 because Lipper peers mix active open-end MFs with ETFs. Near-zero cross-sectional variance among ETFs; `total_expense_ratio` is the superior fee factor.
2. **Tax efficiency:** US-domiciled only (3,458 ETFs). Non-US universes empty.
3. **Preservation:** vs broad asset class, not narrow category → equities lower (median 3, 24% at 1), FI higher.

Max-peer-count selection (11,611 products): US cross-listings → United States (N≈1,500 vs CL≈27 / PE≈15); UCITS → Germany (≈2,780) or Luxembourg (≈2,806); single-market CA≈1,083, JP≈220, AU≈313, CN≈297, TW≈187, IN≈159 preserved.

Of 568 variants, **only 20** (5 tags × 4 horizons in the primary domestic universe) have longitudinal history (≥2 obs on any product). The other 548 are single-shot cross-registration snapshots — LOCF is useless until collapse.

### F. Cadence

| Endpoint / group | N pairs | Min | P25 | Median | P75 | P95 | Max | Mode | Cadence |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| All Lipper | 45,739 | 31d | 31d | **31.0d** | 31d | 31d | 31d | 31d (**100%**) | Strict monthly |
| `mstar_morningstar_rating` | 4,834 | 31d | 31d | **31.0d** | 31d | 31d | 31d | 31d (**100%**) | Strict monthly |
| `mstar_sustainability_rating` | 11,386 | 31d | 31d | **31.0d** | 31d | 31d | 243d | 31d (**99.8%**) | Strict monthly |
| `mstar_medalist_rating_analyst` | 13 | 39d | 129d | **130.0d** | 136d | 136d | 136d | 136d | Event / annual |
| `mstar_process_analyst` / people | 11 | 129d | 129d | **136.0d** | 136d | 136d | 136d | 136d | Event / annual |
| `mstar_parent_analyst` | 2 | 22d | 22d | **22.0d** | 22d | 22d | 22d | 22d | Annual firm review |

**Staleness chosen:** Lipper + stars + ESG globes 180d; medalist / coverage / pillars 540d.

---

## Session 5 — `themes` & `esg`

- **Themes Bronze:** 27,631 snapshots across 5 fetch dates (Sep 3–13, 2026); 15,786 with data, 11,845 empty `{}`
- **ESG Bronze:** 43,437 snapshots / same 5 dates; 23,789 with data, 19,648 empty `{}`
- **Silver:** themes 5,096,954 dimension rows / 10,884 products / 491 children; ESG 404,404 metric rows / 11,627 products / 17 `metric_id`s
- **Overlap:** 904 products ESG-only (860 FI + 9 Other + 4 None + 1 Cash, 0 Equity); 161 products theme-only

### A. Theme payload architecture (empirical)

Invariant top-level keys: `conid`, `name`, `symbol`, `exchange`, `assetType`, `coverage`, `themes`. Theme item keys: `key` (UUID), `name`, `weight`, `rank_adjusted_weight`.

**No date field** exists (`asOfDate` / `as_of_date` absent). Effective date = snapshot date.

`rank_adjusted_weight` vs `weight` (7,353 theme instances, 20 sample payloads):

| | Min | Median | Max |
| :--- | ---: | ---: | ---: |
| ratio `raw / weight` | 0.0133 | 0.5604 | 1.0000 |

`rank_adjusted_weight ≤ weight` always. Extractor stores rank-adjusted (penalizes weak theme coverage vs universe median).

**Cardinality:** 491 children across 19 parents. Themes per `(product_id, effective_date)`: min 1, median 370, max 491, mean 322.88. ≤3 themes: 10 products (single-stock levered ETPs). VT carries all 491; BOTZ ~430–540 across listings.

**`dimension_code` integrity:** 0 orphans vs `bronze.themes`; 0 parent IDs in Silver (children only).

### B. Theme coverage

Present in 15,786 / 15,786 non-empty payloads.

| Min | P01 | P25 | Median | P75 | P99 | Max |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.700 | 0.711 | 0.871 | 0.945 | 0.982 | 1.219 | 3.844 |

Coverage > 1.0: 139 products (P99 = 1.175) — exclusively leveraged ETPs (2× Leverage Shares, yield-enhanced). **Not currently extracted** as a Silver metric — that is a specified gap (`theme_coverage`).

### C. Theme weight anomalies

**Negatives:** 3,153 rows (0.062%) from 20 products, all long/short or alt (LBAY, HDGE, NLSI, FLSE). Range \([−0.0852, 0)\). Net short thematic exposure.

**Weights > 1.0:** 1,877 rows (0.037%) from 94 products, all leveraged ETPs. Max 2.0007 (Leverage Shares 2× Goldman Sachs). Scales with effective exposure.

**Sum of theme weights (NOT a partition):** min 0.052, median 7.303, max 110.865, mean 9.636. Themes overlap (NVDA ∈ AI Inference ∩ Semiconductor Chips ∩ Accelerated Computing). No sum-to-1.0 constraint.

### D. Parent-theme rollup weights (empirical)

| Parent theme name | Children | Median rollup weight | Max rollup weight |
| :--- | ---: | ---: | ---: |
| Technology and Innovation | 60 | 1.358 | 10.037 |
| Technology Hardware and Semiconductors | 20 | 0.791 | 3.332 |
| Consumer Goods and Retail | 44 | 0.504 | 1.805 |
| Healthcare and Biotechnology | 50 | 0.478 | 1.546 |
| Energy and Utilities | 48 | 0.429 | 1.533 |
| Entertainment and Media | 24 | 0.413 | 2.430 |
| Financial Services and FinTech | 40 | 0.409 | 1.262 |
| Telecommunications and Connectivity | 14 | 0.387 | 1.217 |
| Automotive and Mobility | 24 | 0.332 | 1.221 |
| Transportation and Logistics | 26 | 0.234 | 0.722 |
| Industrial Products and Services | 19 | 0.187 | 0.560 |
| Environmental and Sustainability Solutions | 17 | 0.176 | 0.561 |
| Aerospace and Defense | 15 | 0.103 | 0.402 |
| Construction and Infrastructure | 11 | 0.095 | 0.375 |
| Real Estate and Property Management | 22 | 0.077 | 0.311 |
| Food and Beverage | 14 | 0.069 | 0.231 |
| Mining and Metals | 18 | 0.044 | 0.317 |
| Hospitality and Leisure | 13 | 0.040 | 0.168 |
| Agriculture and Food Production | 12 | 0.029 | 0.142 |

### E. ESG payload architecture (empirical)

Invariant keys on 23,789 non-empty payloads: `asOfDate`, `content`, `coverage`, `no_settings`, `source`, `symbol`, `title`. `source` always `"CALCULATED"`; `title` always `"ESG"`.

`asOfDate` = `"YYYYMMDD"` present in 100% of non-empty payloads. Distinct Silver dates: `2026-08-22`, `2026-08-29`, `2026-09-05` — all Fridays (weekly recalc).

All 16 scores are discrete integers on \([0, 10]\). No fractional scores. Higher = better, including controversies (`tresgccs`: higher = fewer controversies).

### F. ESG coverage

| Min | P05 | P25 | Median | P75 | P95 | P99 | Max | Mean | Std |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.7000 | 0.7592 | 0.8826 | **0.9419** | 0.9740 | 0.9982 | 1.1749 | 2.6804 | 0.92 | 0.10 |

Coverage > 1.0: 281 obs / 139 products (1.18%) — leveraged ETPs. 9 snapshot payloads (3 products) miss the `coverage` key while still having score trees (`tresgs` obs 23,789 vs `esg_coverage` 23,780).

### G. ESG zero-score edge cases

| Metric | Zeros | % of obs | Interpretation |
| :--- | ---: | ---: | :--- |
| `tresgenpis` (Env. Innovation) | 375 | 1.58% | Negligible eco-innovation |
| `tresgccs` (Controversies) | 155 | 0.65% | Maximum controversies (worst decile) |
| `tresgcgsrs` (Shareholders) | 80 | 0.34% | Weakest shareholder governance |
| `tresgcgbds` (Management) | 41 | 0.17% | Weakest management |
| `tresgsohrs` (Human Rights) | 37 | 0.16% | Worst HR compliance |
| `tresgcgvss` (CSR Strategy) | 31 | 0.13% | No CSR strategy |
| `tresgenrrs` (Resource Use) | 30 | 0.13% | Lowest resource efficiency |
| `tresgens` (Environmental) | 27 | 0.11% | Lowest env pillar |
| `tresgeners` (Emissions) | 20 | 0.08% | Worst emissions |
| `tresgcgs` (Governance) | 11 | 0.05% | Lowest gov pillar |
| `tresgsowos` (Workforce) | 10 | 0.04% | Worst workforce |
| `tresgsocos` (Community) | 2 | 0.01% | Lowest community |
| `tresgsoprs` (Product Resp.) | 2 | 0.01% | Worst product responsibility |

`tresgs` / `tresgcs` never reach 0 (min 1); `tresgsos` min 1.

### H. ESG score distributions

| Metric ID | Description | Obs | Unique ETFs | Min | P05 | P25 | Median | P75 | P95 | Max | Mean | Std |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `esg_coverage` | Coverage ratio | 23,780 | 11,624 | 0.70 | 0.76 | 0.88 | **0.94** | 0.97 | 1.00 | 2.68 | 0.92 | 0.10 |
| `tresgs` | ESG Score | 23,789 | 11,627 | 1 | 4 | 6 | **6** | 7 | 7 | 8 | 6.06 | 0.92 |
| `tresgcs` | Combined | 23,789 | 11,627 | 1 | 4 | 4 | **5** | 6 | 6 | 8 | 4.91 | 0.88 |
| `tresgccs` | Controversies | 23,789 | 11,627 | 0 | 3 | 4 | **5** | 7 | 9 | 10 | 5.46 | 2.09 |
| `tresgens` | Environmental | 23,789 | 11,627 | 0 | 3 | 5 | **6** | 7 | 7 | 9 | 5.79 | 1.24 |
| `tresgenrrs` | Resource Use | 23,789 | 11,627 | 0 | 4 | 6 | **7** | 8 | 8 | 9 | 6.74 | 1.43 |
| `tresgeners` | Emissions | 23,789 | 11,627 | 0 | 4 | 6 | **7** | 7 | 8 | 9 | 6.48 | 1.25 |
| `tresgenpis` | Env. Innovation | 23,789 | 11,627 | 0 | 1 | 4 | **4** | 5 | 6 | 9 | 4.20 | 1.42 |
| `tresgsos` | Social | 23,789 | 11,627 | 1 | 4 | 6 | **6** | 7 | 8 | 9 | 6.25 | 0.99 |
| `tresgsowos` | Workforce | 23,789 | 11,627 | 0 | 4 | 6 | **6** | 7 | 8 | 9 | 6.38 | 1.25 |
| `tresgsohrs` | Human Rights | 23,789 | 11,627 | 0 | 3 | 6 | **6** | 7 | 8 | 9 | 5.96 | 1.31 |
| `tresgsocos` | Community | 23,789 | 11,627 | 0 | 5 | 6 | **7** | 7 | 8 | 9 | 6.73 | 0.87 |
| `tresgsoprs` | Product Resp. | 23,789 | 11,627 | 0 | 4 | 5 | **6** | 6 | 7 | 9 | 5.67 | 1.08 |
| `tresgcgs` | Governance | 23,789 | 11,627 | 0 | 5 | 6 | **6** | 7 | 7 | 9 | 6.03 | 0.90 |
| `tresgcgbds` | Management | 23,789 | 11,627 | 0 | 5 | 6 | **6** | 7 | 7 | 9 | 6.18 | 1.02 |
| `tresgcgsrs` | Shareholders | 23,789 | 11,627 | 0 | 4 | 5 | **5** | 6 | 6 | 9 | 5.17 | 0.96 |
| `tresgcgvss` | CSR Strategy | 23,789 | 11,627 | 0 | 4 | 6 | **7** | 8 | 8 | 9 | 6.69 | 1.29 |

```
tresgs (ESG Score) — concentrated around 6:
  1:     14 ( 0.1%)
  2:     49 ( 0.2%)
  3:    253 ( 1.1%)  █
  4:  1,359 ( 5.7%)  ███
  5:  2,855 (12.0%)  ██████
  6: 11,474 (48.2%)  ████████████████████████
  7:  7,606 (32.0%)  ████████████████
  8:    179 ( 0.8%)

tresgccs (Controversies) — wide symmetric:
  0:    155 ( 0.7%)
  1:    181 ( 0.8%)
  2:    832 ( 3.5%)  ██
  3:  3,840 (16.1%)  ████████
  4:  3,701 (15.6%)  ████████
  5:  3,493 (14.7%)  ███████
  6:  3,910 (16.4%)  ████████
  7:  3,074 (12.9%)  ██████
  8:  2,335 ( 9.8%)  █████
  9:  2,067 ( 8.7%)  ████
  10:   201 ( 0.8%)
```

Aggregates (`tresgs`, `tresgcs`) cluster tightly (std < 1.0) around 5–6 (diversified ETF averages). Sub-pillars spread more (std 1.0–1.4). Controversies is the most discriminating single signal (std 2.09, full 0–10 range).

### I. Asset-class coverage (latest snapshot, dominant sleeve > 60%)

| Asset class | Total products | w/ themes | % themes | w/ ESG | % ESG |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Equity** | 11,753 | 10,005 | **85.1%** | 10,658 | **90.7%** |
| **Fixed Income** | 4,495 | 873 | **19.4%** | 961 | **21.4%** |

Theme/ESG features are predominantly Equity. FI products will be mostly `0.0` (theme default) or `NULL` (ESG) — filter out of FI-specific regressions.

### J. Cadence

**ESG:** 100% of 12,162 consecutive Silver `effective_date` deltas are exactly **7 days**. Provider recalculates every Friday. Cap chosen: **90d**.

**Themes:** snapshot-driven; Bronze fetch deltas 2–10 days. No provider recalc date. Cap chosen: **180d**.

---

## Cross-session snapshot of Bronze / Silver volumes

| Endpoint | Bronze snapshots | Unique products | Notes |
| :--- | ---: | ---: | :--- |
| `profile` | 42,406 | — | 7 metrics + 2 style dims |
| `ratios` | 42,055 | 20,024 | 47 metrics; 3,991 empty |
| `holdings` | 47,401 | 22,634 | 27,919 populated |
| `mstar` | 35,072 | 22,634 | 32,541 populated |
| `lipper` | 25,544 | 22,634 | 14,519 populated; 568→20 IDs specified |
| `themes` | 27,631 | — | 15,786 populated; 491 children |
| `esg` | 43,437 | — | 23,789 populated; 17 metrics |
