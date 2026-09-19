# Silver Extractors — Bronze → Silver Observation Engine

- **Status:** Unified implementation specification (condensed from FRD §4.1–§4.5, checklist §5.1–§5.3)
- **Module:** `etfportfolio/prep/extractors.py`
- **Contracts:** [00_silver_contracts.md](00_silver_contracts.md)
- **Panel (Stage 2):** [02_factor_panel.md](02_factor_panel.md)
- **Empirical justification:** [../research/empirical_audit_archive.md](../research/empirical_audit_archive.md)

This spec is the single source of extraction rules. Percentile tables, cadence deltas, and ASCII charts live in the research archive — not here.

---

## 1. Extractor Map

| Extractor | Endpoint | Fixtures | Metrics | Dimensions |
| :--- | :--- | :--- | :--- | :--- |
| `extract_profile` | `/tws.proxy/fundamentals/mf_profile_and_fees/` | `tests/fixtures/profile_*.json` | 7 | `style_box`, `style_box_hist` |
| `extract_ratios` | `/tws.proxy/fundamentals/mf_ratios_fundamentals/` | `tests/fixtures/ratios_*.json` | 47 | — |
| `extract_holdings` | `/tws.proxy/fundamentals/mf_holdings/` | `tests/fixtures/holdings_*.json` | 1 | 6 types (no `top_holding`) |
| `extract_mstar` | `/tws.proxy/mstar/fund/detail?conid=` | `tests/fixtures/mstar_*.json` | 8 + coverage | — |
| `extract_lipper` | `/tws.proxy/fundamentals/mf_lip_ratings/` | `tests/fixtures/lipper_*.json` | 20 canonical | — |
| `extract_theme_weights` | `/tws.proxy/knowledge-graph/ui/fund?conid=` | `tests/fixtures/theme_weights.json` | `theme_coverage` | `theme` |
| `extract_esg` | `/tws.proxy/impact/esg/` | `tests/fixtures/esg.json` | 17 | — |

Empty payloads (`{}`, empty section lists, `as_of_date: 0`) and error payloads (`{"type": "IDENTIFICATION_PROBLEM", ...}`) return an empty `ExtractionResult`. No extra error handling is required beyond `if not payload: return result` plus iterating missing keys as empty lists.

---

## 2. Shared Extraction Primitives

### 2.1 Date precedence — `parse_effective_date`

Accepted raw forms: Unix epoch **milliseconds** (`1785470400000`), `YYYYMMDD`, `YYYY/MM/DD`, `YYYY-MM-DD`, parenthesized `(YYYY/MM/DD)`. Regex: `(\d{4}[-/]?\d{2}[-/]?\d{2}|\d{8})`. Unix ms → `datetime.fromtimestamp(ms / 1000.0, tz=UTC).date()`. Treat `None`, `0`, and negative as missing.

| Rank | Condition | `effective_date_source` |
| :---: | :--- | :--- |
| 1 | Per-item date present (`publish_date`, parenthesized AUM date, report `as_of_date`) | `'item'` |
| 2 | Top-level payload date present (`as_of_date`, `asOfDate`) | `'payload'` |
| 3 | Else `snapshot_created_at.date()` | `'snapshot'` |

Invariant: `effective_date <= fetched_at::DATE` ([00_silver_contracts.md](00_silver_contracts.md) §4.2).

### 2.2 Metric ID — `sanitize_metric_id(tag)`

Lowercase; replace non-alphanumeric runs with `_`; strip leading/trailing `_`.

Examples: `EPS_growth_1yr` → `eps_growth_1yr`; `LT_Debt_Shareholders_Equity` → `lt_debt_shareholders_equity`; `DividendPayoutRatio5yr` → `dividendpayoutratio5yr`; `TRESGS` → `tresgs`.

### 2.3 Percentage → decimal

\[
\text{value} = \frac{\text{float}(\text{raw.replace}(\%,''))}{100.0}
\]

Used for profile expense ratios, holdings weights, `top_10_weight`, and the **24** ratios percentage-point metrics (§4.2). Display string stays in `raw_value` unscaled.

### 2.4 Magnitude scaling (AUM)

Normalize European separators: if both `,` and `.` appear, the **earlier** separator is the thousands grouping mark. Then:

\[
\text{Multiplier} = \begin{cases}
10^3 & \text{suffix } \in \{k, K\} \\
10^6 & \text{suffix } \in \{m, M\} \\
10^9 & \text{suffix } \in \{b, B\} \\
10^{12} & \text{suffix } \in \{t, T\} \\
1.0 & \text{otherwise}
\end{cases}
\qquad
\text{value} = \text{base} \times \text{Multiplier}
\]

### 2.5 ISO-4217 currency disambiguation

Applies **only** to `total_net_assets_local`. All other metrics emit `currency = NULL`.

Exactly 26 prefix tokens resolve as:

| Rule | Tokens | ISO |
| :--- | :--- | :--- |
| Symbol | `€` `£` `¥` `₹` | `EUR` `GBP` `JPY` `INR` |
| Explicit ISO prefix | `CAD` `AUD` `CNY` `TWD` `HKD` `CHF` `BRL` `SGD` `MXN` `KRW` `MYR` `CNH` `AED` `SEK` `ZAR` `ILS` `SAR` `NOK` `HUF` `DKK` `VND` | token itself |
| Dollar `$` | `silver.products.currency == 'CAD'` **or** listing exchange `TSE` / Canadian | `CAD` |
| Dollar `$` | otherwise | `USD` |

`$` → `USD` covers US-domiciled funds, European UCITS whose **fund base reporting currency** is USD (even when listed in EUR/GBP/MXN/CHF), and Asian cross-listings. Australian ETFs always use an explicit `AUD` prefix — never `$`.

### 2.6 Discrete rating maps

Normalize with `str(raw).strip().lower()` **before** lookup. Skip (do not emit, do not raise) if the token is in:

```
{under_review, under review, not_applicable, not applicable, na, n/a, -, ''}
```

Unrecognized **non-empty** tokens **raise `ValueError`**.

**Medalist (`_MSTAR_MEDALIST_MAP`)** — `gold→5`, `silver→4`, `bronze→3`, `neutral→2`, `negative→1`.

**Pillar (`_MSTAR_PILLAR_MAP`)** — people/process/parent: `high→5`, `above_average→4`, `average→3`, `below_average→2`, `low→1`.

**Stars (`_MSTAR_STAR_MAP`)** — `morningstar_rating`: `float(raw) ∈ {1,2,3,4,5}`.

**Sustainability (`_MSTAR_SUSTAINABILITY_MAP`)** — `high|5→5`, `above_average|4→4`, `average|3→3`, `below_average|2→2`, `low|1→1`.

**Passive flag** — `'passive'→1.0`, `'active'→0.0`; anything else raises `ValueError`.

**Lipper** — integer scores \(v \in \{1,2,3,4,5\}\). No `/100`.

**ESG TRESG** — integer scores on \([0, 10]\). No `/100`. Higher = better (including `tresgccs` controversies: higher = fewer controversies).

---

## 3. `extract_profile`

**Source:** `'profile'`. Payload sections: `fund_and_profile`, `expenses_allocation`, `reports`, `mstar`.

### 3.1 Scalars

| `metric_id` | Payload location | Date source | Transform | Currency |
| :--- | :--- | :--- | :--- | :--- |
| `total_net_assets_local` | `fund_and_profile` `name_tag="Total_Net_Assets_Month_End"` **or** `name` begins `"Total Net Assets"` | Parenthesized date → `'item'`; else snapshot | Magnitude scale (§2.4) + currency (§2.5) | ISO-4217 |
| `total_expense_ratio` | `fund_and_profile` `name_tag="Total_Expense_Ratio"` | snapshot | `% / 100` | `NULL` |
| `management_expense_ratio` | `expenses_allocation` `name="Management Expenses"` | snapshot | float `ratio` **as-is** (allocation, **not** % of AUM) | `NULL` |
| `non_management_expense_ratio` | `expenses_allocation` `name="Non-Management Expenses"` | snapshot | float `ratio` **as-is** | `NULL` |
| `audited_net_expense_ratio` | `reports` where `name="Annual Report"`, field `"Total Net Expense"` | report `as_of_date` Unix ms → `'item'`; else snapshot | `% / 100` | `NULL` |
| `manager_tenure_years` | `fund_and_profile` `name_tag="Manager_Tenure"` | snapshot | see formula | `NULL` |
| `is_passive` | `fund_and_profile` `name_tag="Management_Approach"` | snapshot | closed map (§2.6) | `NULL` |

**AUM zeros & micros:** `value = 0.0` (e.g. `CAD0 (2020/08/31)`) is a valid liquidated-fund observation. Micro-AUM (e.g. $19.08) is retained. Mask at factor-weighting time, not here.

**TER bounds:** valid range \([0.0000, 0.2000]\). `0.0%` is legitimate (promotional waivers, e.g. HODL). Ratios \(> 5\%\) (PBDC 13.49%, FBDC 12.44%) are valid SEC **AFFE** for BDCs — **do not clamp or reject**.

**Expense allocation invariant:** \(\text{management\_expense\_ratio} + \text{non\_management\_expense\_ratio} = 1.0\). Advisor waivers produce negatives (down to \(-95.05\)) and complements \(> 1.0\) (up to \(+96.05\)). **Silver retains the unconstrained float.** Panel may winsorize to \([-1, 2]\) or form `management_fee_rate = total_expense_ratio × management_expense_ratio`.

**Tenure (Silver stores snapshot-frozen years):** parse `raw_value` as `%Y/%m/%d` (100% of live cases) or `%Y-%m-%d` / `%Y`.

\[
\text{tenure\_years} = \mathrm{round}\!\left(\max\!\left(0,\; \frac{\text{snapshot\_date} - \text{start\_date}}{365.25}\right),\; 4\right)
\]

Panel advances tenure along the calendar spine from `raw_value` (see [02_factor_panel.md](02_factor_panel.md) §5).

### 3.2 Style-box dimensions

Payload: `payload["mstar"]`. `selected` → `dimension_type='style_box'`; `hist` → `'style_box_hist'`.

| Axis | Tags |
| :--- | :--- |
| X (`x_axis_tag`) | `{0: value, 1: core, 2: growth}` |
| Y (`y_axis_tag`) | `{0: large, 1: multi, 2: mid, 3: small}` |

- `dimension_code = f"{y_tag}_{x_tag}"` (e.g. `mid_core`)
- `dimension_name = f"{y_tag.title()} {x_tag.title()}"` (e.g. `Mid Core`)
- `value = 1.0` (active indicator)

Extractor emits only observed coordinates. Panel emits all 12 theoretical cells, defaulting the 3 unobserved (`style_large_growth`, `style_large_value`, `style_mid_value`) to `0.0`. When both `style_box` and `style_box_hist` exist for `(product_id, effective_date)`, **`style_box` wins**.

---

## 4. `extract_ratios`

**Source:** `'ratios'`. Five sections: `ratios`, `financials`, `fixed_income`, `dividend`, `zscore`. **0 dimensions.** `currency` always `NULL`.

Per item: skip if `value is None` or `name_tag` missing. `metric_id = sanitize_metric_id(name_tag)`. `raw_value = str(value_fmt if value_fmt is not None else value)`. Date: top-level `payload["as_of_date"]` Unix ms → `'payload'` (100% of metric-bearing payloads have it). Do **not** extract `title_vs` or peer columns (`vs`, `min`, `max`, `avg`, `percentile`).

### 4.1 The 24 metrics that **must** be divided by 100.0

Provider delivers these as **percentage points** (`18.13` = 18.13%). Convert to decimal fractions for consistency with profile expense ratios.

| Category | `metric_id` |
| :--- | :--- |
| EPS growth (3) | `eps_growth_1yr`, `eps_growth_3yr`, `eps_growth_5yr` |
| Sales growth (3) | `sales_growth_1_year`, `sales_growth_3_year`, `sales_growth_5_yr` |
| SPS growth (2) | `sales_per_share_growth_1_year`, `sales_per_share_growth_3_year` |
| OCF growth (1) | `operating_cash_flow_growth_rate_3yr` |
| Profitability (8) | `return_on_assets_1yr`, `return_on_assets_3yr`, `return_on_equity_1yr`, `return_on_equity_3yr`, `return_on_investment_1yr`, `return_on_investment_3yr`, `return_on_capital`, `return_on_capital_3yr` |
| Dividend (4) | `dividend_yield_weighted_average`, `dividendpayoutratio5yr`, `dividend_per_share_1yr`, `dividend_per_share_3yr` |
| FI yield/coupon (2) | `yield_to_maturity`, `average_coupon` |
| Relative return (1) | `relative_strength` |

### 4.2 The 23 metrics stored as native floats (NO `/100`)

| Category | `metric_id` |
| :--- | :--- |
| Valuation multiples (5) | `price_sales`, `price_cash`, `price_book`, `price_earnings`, `price_to_dividend` |
| Leverage (6) | `ebit_to_interest`, `lt_debt_shareholders_equity`, `total_assets_total_equity`, `total_debt_total_capital`, `total_debt_total_equity`, `sales_to_total_assets` |
| FI tenor / grade (3) | `nominal_maturity` (years), `effective_maturity` (years), `average_quality` (score 3.0–10.0) |
| Z-scores (9) | `average_final_composite_zscore`, `latest_composite_z_score`, `latest_dividend_yield_zscore`, `latest_price_to_book_zscore`, `latest_price_to_earnings_zscore`, `latest_price_sales_zscore`, `latest_return_on_equity_zscore`, `latest_sps_growth_zscore`, `weighted_final_composite_zscore` |

**Naming trap:** `latest_composite_z_score` contains `_z_score` (two tokens). Filters `LIKE '%zscore'` silently omit it.

### 4.3 Edge cases — retain in Silver; do not clamp

| Case | Rule |
| :--- | :--- |
| Provider caps | P/S ≤ 50, P/Cash ≤ 60, P/B ≤ 25, P/E ≤ 60, YTM ≤ 10, EPS growth \(\in [-50, 100]\) pp (pre-scale). Exact-cap records are valid ceiling-censored observations. |
| Split-corp / leverage extremes | ROE 3yr ~2,971,173%, TA/TE ~263,821× (SPLT), EBIT/Interest ~1.3e6. Mathematically correct. **Winsorize at panel, not here.** |
| Uncapped SPS growth | `sales_per_share_growth_1_year` outlier 19,862% (GROW, restructuring). Retain. |
| `average_quality` | Only metric whose `value_fmt` is a **letter grade** (`AAA`…`CC`, `-`). Store provider continuous score in `value`; letter in `raw_value`. `raw_value='-'` with `value=10.0` = commodity/crypto, no rated bonds — panel treats as `NULL` quality, not "best". |
| Negative YTM / maturity | Convertible-bond and crypto/derivative ETFs. Retain. |
| Negative dividend yield | One case (PGRX −0.08%). Retain. |
| FI metrics on equity-dominant funds | 205–254 balanced / target-date / multi-asset ETFs. Legitimate; retain. |
| `dividend_yield_weighted_average` | Equity peer-universe only (absent from some fixtures). Extract when present. |

---

## 5. `extract_holdings`

**Source:** `'holdings'`. Date: `payload["as_of_date"]` Unix ms → `'payload'`. Discard payload keys `currency` and `geographic` (collinear with `country` / fund reporting currency).

### 5.1 Prune `top_holding`

**Delete** the `for item in payload.get("top_10", []):` loop. Drops ~263,186 sparse rows / 30,128 names. Concentration is fully captured by the scalar below.

### 5.2 Scalar `portfolio_top_10_concentration`

`payload["top_10_weight"]` (e.g. `"30.47%"`) → `% / 100`. Retain values \(> 1.0\) (leveraged/inverse notional, gross collateral overlays; max observed 17.69). No negatives or zeros exist. Panel may clip to \([0, 1]\) or emit `is_leveraged = 1.0` when \(> 1.0\).

### 5.3 Allocation dimensions

| Payload key | `dimension_type` |
| :--- | :--- |
| `allocation_self` | `asset_class` |
| `investor_country` | `country` |
| `industry` | `industry` |
| `debtor` | `credit_rating` |
| `debt_type` | `debt_type` |
| `maturity` | `maturity` |

\[
\text{weight} = \frac{\text{float}(\text{item}[\text{"weight"}])}{100.0}
\qquad
\text{raw\_str} = \mathrm{str}(\text{item.get}(\text{"formatted\_weight"},\; f\text{"\{weight\_val\}\%"}))
\]

**Negatives retained** (cash overdrafts, short-swap mark-to-market in `Other` / `Unidentified`, down to \(\approx -421\)). They preserve the `asset_class` partition-of-unity. Panel may zero-floor and renormalize for long-only models.

`asset_class` closed set: `Equity`, `Fixed Income`, `Cash`, `Other`. 100% of product-dates sum to \(1.0 \pm 0.000015\).

### 5.4 Country ISO-3166 remaps (extraction-time `dimension_code`)

107 names. Apply these 133-row anomaly remaps; leave all other codes as emitted.

| `dimension_name` | Provider code | Required `dimension_code` | Rows |
| :--- | :--- | :--- | ---: |
| `Croatia` | `CR` (collides with Costa Rica) | `HR` | 1 |
| `Bulgaria` | `BGR` (alpha-3) | `BG` | 5 |
| `Guam` | `NULL` | `GU` | 121 |
| `Uzbekistan` | `NULL` | `UZ` | 6 |
| `Unidentified` | — | **`NULL`** (strict) | 17,546 |
| `Costa Rica` | `CR` | `CR` (keep) | 25 |
| `Korea` | `KR` | `KR` (South Korea) | — |
| `Virgin Islands (U.S.)` | `VI` | `VI` | — |

`Unidentified` is the derivative/swap/unassigned residual (weights down to −420.22). Never invent a code for it.

### 5.5 Industry remap

Closed set of 14 sectors. Historical discontinued category (29 rows):

`Telecommunication Services-Discontinued eff 09/19/2020` → `Communication Services`

Residual buckets retained as named: `Not Classified - Non Equity`, `Non Classified Equity`.

### 5.6 Credit rating (already applied)

`clean_credit_rating()` maps `"% Quality/AAA"`, `"% Quality AA"`, `"% Quality-A"` → `AAA`, `AA`, `A`, `BBB`, `BB`, `B`, `CCC`, `CC`, `C`, `D`, `Not Rated`, `Not Available`. Invariant: `dimension_code == dimension_name`.

### 5.7 Maturity slugs (`dimension_code`, pending)

| Raw `dimension_name` | `dimension_code` |
| :--- | :--- |
| `% Maturity Less than 1 Year` | `mat_lt_1y` |
| `% Maturity 1 to 3 Years` | `mat_1_to_3y` |
| `% Maturity 3 to 5 Years` | `mat_3_to_5y` |
| `% Maturity 5 to 10 Years` | `mat_5_to_10y` |
| `% Maturity 10 to 20 Years` | `mat_10_to_20y` |
| `% Maturity 20 to 30 Years` | `mat_20_to_30y` |
| `% Maturity Greater than 30 Years` | `mat_gt_30y` |
| `% Maturity Other` | `mat_other` |

### 5.8 Debt type

Store all **110 raw names** in Silver (`dimension_name` as emitted, `dimension_code` as provided). Clustering 110 → 9 macroeconomic features is a **panel** operation ([02_factor_panel.md](02_factor_panel.md) §6). Do not collapse at extraction.

---

## 6. `extract_mstar`

**Source:** `'mstar'`. Valid payload keys: `as_of_date`, `commentary`, `q_full_report_id`, `summary`. Discard `commentary`, `q_full_report_id`, and summary tags `category`, `category_index`.

### 6.1 Per-pillar date

For each item in `payload["summary"]`:

1. `pillar["publish_date"]` present → parse → `'item'`
2. Else top-level `payload["as_of_date"]` (`YYYYMMDD`) → `'payload'`
3. Else snapshot → `'snapshot'`

Pillars on the same snapshot are **asynchronous** (stars monthly, medalist on analyst-report date, parent possibly 9 months earlier). Each metric keeps its own `effective_date`.

### 6.2 Unified medalist (bug fix)

**Bug:** `medalist_rating` always arrives with `q: false` (or `q` omitted). The legacy check `is_quant = bool(pillar.get("q") is True or pillar_key.startswith("q_"))` mislabeled 10,755 medalists as `mstar_medalist_rating_analyst`.

**Fix:** Extract `medalist_rating` (or legacy `quantitative_rating`) as canonical `metric_id = "mstar_medalist_rating"` using `_MSTAR_MEDALIST_MAP`. Store the provider string (`"Gold"`, `"Silver"`, …) in `raw_value`. Do **not** suffix `_analyst` / `_quant` on the medalist itself.

### 6.3 Analyst coverage

Inspect the 3 foundational pillars `{people, process, parent}` present in `summary`:

\[
\text{analyst\_count} = \sum_{p} \mathbb{I}(p \in \text{summary} \;\wedge\; \neg\text{is\_quant}(p))
\qquad
\text{coverage} = \text{analyst\_count} / 3.0
\]

Emit `metric_id = "mstar_analyst_coverage_pct"`, `value = coverage`, `raw_value = f"{analyst_count}/3 analyst pillars"`. Observed values: \(\{0.0, 0.3333, 0.6667, 1.0\}\). Empirical mix: 22.4% pure analyst (3A), 13.2% pure quant (3Q), 64.4% hybrid.

`is_quant = bool(pillar.get("q") is True or pillar_key.startswith("q_"))` remains correct for **people / process / parent**.

### 6.4 Pillar segregation (Silver provenance)

Continue emitting `mstar_{people|process|parent}_{analyst|quant}` via `_MSTAR_PILLAR_MAP`. Also emit:

| `metric_id` | Map |
| :--- | :--- |
| `mstar_morningstar_rating` | stars 1–5 |
| `mstar_sustainability_rating` | globes 1–5 |

Panel synthesizes unified `pillar_*` scores with analyst-priority fallback ([02_factor_panel.md](02_factor_panel.md) §7).

---

## 7. `extract_lipper`

**Source:** `'lipper'`. Collapse 568 geographic variants → **20 canonical metrics** via Max-Peer-Count selection **inside the extractor** (eliminates ~860k duplicate Silver rows).

### 7.1 Max-Peer-Count universe selection

Given `payload["universes"]`:

1. If `len(universes) == 1`: select that universe.
2. If `len > 1`: for each universe \(U_i\), \(N(U_i) = \max_{\text{item} \in U_i}(\text{regex\_int}(\text{item.rating.name}))\) (peer-group fund count). Select \(U^* = \arg\max N(U_i)\).
3. Tie-break, in order: `['United States', 'Germany', 'UK', 'Canada', 'Japan', 'Australia']`.

Empirical: US funds registered in CL/PE → United States (\(N \approx 1500\) vs 15–27); UCITS 16-nation → Germany (\(\approx 2780\)) or Luxembourg (\(\approx 2806\)); single-market CA/JP/AU/CN/TW/IN preserved.

### 7.2 Canonical metric IDs (20)

Tags: `total_return`, `consistent_return`, `preservation`, `expense`, `tax_efficiency`.  
Horizons: `overall`, `3yr`, `5yr`, `10yr`.

`metric_id = f"lipper_{tag}_{horizon}"`  
`value` = integer 1–5 as float.  
`raw_value = f"{val} ({universe_name}: {fund_count} funds)"`  
`currency = NULL`.

`tax_efficiency` is US-domiciled only; other universes have empty lists — emit only when present.

**Expense clustering note:** 82–85% of ETFs score `5.0` (ETFs vs mixed MF+ETF peer groups). Still extract; `total_expense_ratio` is the superior continuous fee factor.

---

## 8. `extract_theme_weights`

**Source:** `'theme_weights'`. Payload has **no date field** → `effective_date = snapshot_created_at.date()`, `source = 'snapshot'`. Empty `{}` → empty result.

### 8.1 Dimension rows (`dimension_type = 'theme'`)

Per `payload["themes"][]`:

| Field | Source |
| :--- | :--- |
| `dimension_name` | `name.strip()` |
| `dimension_code` | `str(key).strip()` (child UUID; must exist in `bronze.themes`) |
| `value` | `float(rank_adjusted_weight)` — **not** raw `weight` |
| `raw_value` | `str(rank_adjusted_weight)` |

Discard `weight` (monotone with `rank_adjusted_weight`; would collinear-split the panel). `rank_adjusted_weight ≤ weight` always.

Retain negatives (long/short net short thematic exposure, down to −0.085) and values \(> 1.0\) (leveraged ETPs, max ≈ 2.00). Themes **do not** sum to 1.0 (overlapping; median sum ≈ 7.3). Missing themes default to 0.0 at panel. **Never emit parent theme IDs** — only children.

### 8.2 Required addition — `theme_coverage` metric

```python
coverage = payload.get("coverage")
if coverage is not None:
    result.metrics.append((
        product_id,
        "theme_weights",          # source
        "theme_coverage",         # metric_id
        eff_date,
        "snapshot",
        snapshot_created_at,
        float(coverage),
        str(coverage),
        None,                     # currency
    ))
```

`coverage` is always present on non-empty payloads. Values \(> 1.0\) are leveraged ETPs (effective equity > NAV). Retain; do not clip. Panel quality-gates `theme_coverage >= 0.7`.

---

## 9. `extract_esg`

**Source:** `'esg'`. **No extractor logic changes** beyond ensuring `currency=NULL` on the 9-tuple. Date: `payload["asOfDate"]` as `YYYYMMDD` → `'payload'` (100% of non-empty payloads). Empty `{}` → empty result. `source` field is always `"CALCULATED"`.

Traverse `content[]` and each node's `children[]` (2-level tree). `metric_id = sanitize_metric_id(node["name"])`. `value = float(node["value"])` — raw integer, **no `/100`**. Zero is a valid lowest-decile score, not missing.

Also emit top-level `coverage` as `metric_id = "esg_coverage"` (retain values \(> 1.0\); 9 payloads miss the key while still having scores — skip coverage only, keep scores).

| Raw Silver `metric_id` | Meaning | Scale |
| :--- | :--- | :--- |
| `esg_coverage` | Portfolio holdings coverage | continuous \([0, \infty)\) |
| `tresgs` | ESG Score (L0) | int \([0,10]\) |
| `tresgcs` | ESG Combined Score (L0) | int \([0,10]\) |
| `tresgccs` | Controversies Score (L0) | int \([0,10]\) |
| `tresgens` | Environmental pillar (L1) | int \([0,10]\) |
| `tresgenrrs` / `tresgeners` / `tresgenpis` | Resource Use / Emissions / Env. Innovation (L2) | int \([0,10]\) |
| `tresgsos` | Social pillar (L1) | int \([0,10]\) |
| `tresgsowos` / `tresgsohrs` / `tresgsocos` / `tresgsoprs` | Workforce / Human Rights / Community / Product Resp. (L2) | int \([0,10]\) |
| `tresgcgs` | Governance pillar (L1) | int \([0,10]\) |
| `tresgcgbds` / `tresgcgsrs` / `tresgcgvss` | Management / Shareholders / CSR Strategy (L2) | int \([0,10]\) |

Human-readable aliases (`esg_score`, `esg_environmental`, …) apply **only** in the panel layer. Silver retains raw TRESG tags.

`tresgs` / `tresgcs` never reach 0 (min 1); `tresgsos` min 1. Sub-pillars and controversies may be 0.

---

## 10. Implementation Checklist (§5.1–§5.3)

### 10.1 Schema & storage

- [ ] **`currency` column:** Amend `silver.product_metrics` in `etfportfolio/core/schema.sql`.
- [ ] **AUM currency disambiguation:** Populate ISO-4217 on `total_net_assets_local` per §2.5; all other metrics `NULL`.
- [ ] **Prune `top_holding`:** Delete the `top_10` loop in `extract_holdings()`.

### 10.2 Operable numeric cleaning

- [ ] **Ratios `/100.0`:** Scale the 24 metrics in §4.1; leave the 23 in §4.2 native.
- [ ] **Unified `mstar_medalist_rating`:** Canonical ID; stop suffixing `_analyst` from `q: false`.
- [ ] **`mstar_analyst_coverage_pct`:** `analyst_count / 3.0` across people/process/parent.
- [ ] **Lipper Max-Peer-Count:** 20 canonical IDs; universe + peer count in `raw_value`.
- [ ] **`theme_coverage`:** Extract `payload["coverage"]` as a `theme_weights` metric.

### 10.3 Dimensional standardization

- [x] **Credit rating codes:** Standardized letter grades (verified).
- [ ] **Country ISO remaps:** HR / BG / GU / UZ; `Unidentified` code `NULL`.
- [ ] **Industry discontinued remap:** Telecom-discontinued → Communication Services.
- [ ] **Maturity slugs:** Populate `dimension_code` with the 8 `mat_*` values.

Panel construction (`silver.monthly_panel`) is **not** in this file — see [02_factor_panel.md](02_factor_panel.md).
