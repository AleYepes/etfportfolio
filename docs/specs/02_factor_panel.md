# Factor Panel Construction — `silver.monthly_panel`

- **Status:** Implementation specification (condensed from FRD §3, §4.x.3 staleness caps, §4.x.5 panel rules, §5.4)
- **Module:** `etfportfolio/prep/panel.py`
- **Contracts:** [00_silver_contracts.md](00_silver_contracts.md)
- **Extractors (Stage 1):** [01_silver_extractors.md](01_silver_extractors.md)
- **Empirical justification:** [../research/empirical_audit_archive.md](../research/empirical_audit_archive.md)

Silver observation tables remain long and un-winsorized. This spec is the **only** place LOCF, flattening, clustering, quality gates, and winsorization occur.

---

## 1. Target Table

```sql
CREATE TABLE silver.monthly_panel (
    product_id   INTEGER NOT NULL,
    as_of_date   DATE NOT NULL,          -- month-end calendar spine
    asset_class  VARCHAR NOT NULL,       -- primary partition from latest asset_class weights
    feature_id   VARCHAR NOT NULL,
    value        DOUBLE NOT NULL,
    PRIMARY KEY (product_id, as_of_date, feature_id)
);
```

Physical long storage. Wide pivot is in-memory only.

`asset_class` on the panel row is the **dominant** sleeve at that date (the `asset_class` dimension with maximum weight; typically `Equity` or `Fixed Income`). It is a filter attribute, not part of the PK.

---

## 2. Month-End Calendar Spine

1. Build a contiguous month-end date spine covering the union of all Silver `effective_date` values (observed range `2015-01-31` … `2026-08-31`).
2. For each `product_id`, restrict the spine to `[first_observation, last_observation + staleness_cap]` — do not carry a dead product forever except `is_passive` (perpetual).
3. Align every feature onto the same `as_of_date` via LOCF (last observation carried forward) subject to the per-feature staleness cap in §4.
4. An observation is eligible to fill `as_of_date` iff `effective_date <= as_of_date` and `(as_of_date - effective_date) <= cap_days`.
5. If multiple observations fall inside the window, take the latest `effective_date` (then latest `fetched_at` as tie-break).

---

## 3. Dimension Flattening & Normalization

When flattening `silver.product_dimensions` into panel features:

| Dimension Type | Input distinct names | Panel `feature_id` | Normalization |
| :--- | :--- | :--- | :--- |
| `asset_class` | 4 (`Equity`, `Fixed Income`, `Cash`, `Other`) | Filter attribute on the panel row **and** optional weight features | Strict partition-of-unity: 100% of product-dates sum to \(1.0 \pm 0.0001\). Isolate/drop `Other` during factor modeling. |
| `country` | 107 (103 ISO + `Unidentified`) | `country_{iso}` lowercase 2-letter (e.g. `country_us`, `country_jp`) | Weights ~sum to 1.0 (top-N truncation). ISO remaps already applied in Silver. `Unidentified` (`dimension_code IS NULL`) is a short/swap residual — **drop from regressions** (multicollinearity). |
| `industry` | 14 sectors | `sector_{slug}` (e.g. `sector_technology`, `sector_financials`) | Discontinued telecom already remapped to Communication Services. Residual buckets `Not Classified - Non Equity`, `Non Classified Equity` handled explicitly (keep as named features or fold into `sector_unclassified_*`). |
| `style_box` / `style_box_hist` | 9 observed of 12 grid cells | 12 canonical booleans `style_{size}_{style}` | Emit **all 12** theoretical Morningstar cells to freeze the schema. Unobserved `style_large_growth`, `style_large_value`, `style_mid_value` default `0.0`. Precedence: `style_box` over `style_box_hist` on the same `(product_id, effective_date)`. |
| `credit_rating` | 12 tiers | 12 weights: `AAA`…`D`, `Not Rated`, `Not Available` | Other-like categories isolated. `Not Rated` / `Not Available` dropped or residualized in FI regressions. |
| `maturity` | 8 buckets | `mat_lt_1y`, `mat_1_to_3y`, `mat_3_to_5y`, `mat_5_to_10y`, `mat_10_to_20y`, `mat_20_to_30y`, `mat_gt_30y`, `mat_other` | Use Silver `dimension_code` slugs, not verbose `% Maturity *` names. |
| `debt_type` | 110 raw types | **9** clusters `debt_*` (§6) | Deterministic 110 → 9 map; 0 unmapped. Resolves rank deficiency. |
| `theme` | 491 children | `theme_{uuid}` (491) **plus** `theme_parent_{uuid}` (19) | Non-exclusive overlapping exposures. Missing → `0.0`. No sum-to-1.0. Gate on `theme_coverage >= 0.7`. |
| `top_holding` | 30,128 names | **Dropped** | Concentration via scalar `portfolio_top_10_concentration` only. |

---

## 4. Unified LOCF Staleness Matrix

Caps are inclusive day counts. "Perpetual" means carry until the product leaves the universe.

| Feature / metric group | Cadence | Cap (days) | Rationale |
| :--- | :--- | ---: | :--- |
| All 16 ESG scores + `esg_coverage` | Weekly (Fri) | **90** | 12+ missed weeks before stale; weekly series should not have large gaps unless delisted. |
| All 47 `ratios` metrics (5 sections) | Monthly | **180** | Co-reported on one `as_of_date`; 99.2% of deltas are 31d. |
| `portfolio_top_10_concentration` | Monthly | **180** | Monthly rebalance. |
| `asset_class`, `country`, `industry`, `credit_rating`, `debt_type`, `maturity` | Monthly | **180** | Synchronous with holdings `as_of_date`. |
| All 20 canonical Lipper metrics | Monthly | **180** | 100% of deltas are 31d. |
| `mstar_morningstar_rating` | Monthly | **180** | Mathematical month-end recalc. |
| `mstar_sustainability_rating` | Monthly | **180** | Monthly ESG globe refresh. |
| `total_net_assets_local` | Monthly | **180** | 99.9% report within 60d; >6mo ⇒ halted/liquidated. |
| All 491 theme weights + `theme_coverage` | Snapshot-driven | **180** | No provider date; quarterly rebalance covered. |
| `mstar_medalist_rating` | Event / annual (median 130d) | **540** | 12-month review + 6-month buffer. |
| `mstar_analyst_coverage_pct` | Event / annual | **540** | Co-moves with medalist. |
| `mstar_people_*`, `mstar_process_*`, `mstar_parent_*` | Event / annual (median 136d) | **540** | Persist until next analyst/quant review. |
| `total_expense_ratio`, `management_expense_ratio`, `non_management_expense_ratio` | Annual prospectus | **540** | 12-month fiscal + 6-month filing buffer. |
| `audited_net_expense_ratio` | Annual (median 365d) | **540** | Official annual report lag. |
| `manager_tenure_years` | Continuous | **365** | Re-verify if profile not refreshed in 12 months. Value itself is **recomputed** along the spine (§5), not frozen. |
| `style_box` / `style_box_hist` | Semi-annual / annual | **365** | Style drifts slowly. |
| `is_passive` | Mandate (rare) | **perpetual** | Strategy changes require a shareholder vote. |

Quick lookup: **ESG 90d · monthly holdings/ratios/Lipper/stars/AUM/themes 180d · medalist/fees 540d · tenure/style 365d · `is_passive` perpetual.**

---

## 5. Dynamic Manager Tenure

Silver `product_metrics.value` is snapshot-frozen. Panel **must not** LOCF that frozen number.

1. Parse `silver.product_metrics.raw_value` as manager start date (`YYYY/MM/DD`).
2. \(\text{panel\_tenure\_years} = \max\!\left(0,\; \dfrac{\text{as\_of\_date} - \text{start\_date}}{365.25}\right)\)
3. If `raw_value` is not a parseable date, fallback: \(\text{value} + \dfrac{\text{as\_of\_date} - \text{effective\_date}}{365.25}\).
4. Still subject to the 365-day staleness cap on the underlying profile observation.

---

## 6. Debt Type 110 → 9 Macroeconomic Clusters

Aggregate Silver `dimension_type='debt_type'` rows by summing weights of raw names that map into each cluster. Unmapped names are a **bug** (map is exhaustive; 0 unmapped in the audit).

| Cluster `feature_id` | Constituent raw debt types (all 110) |
| :--- | :--- |
| `debt_sovereign` | `Sovereign Bond`, `Bundesanleihen`, `Dutch State Loan`, `Gilt Treasury Stock`, `Irish Govt Bond`, `Japanese Govt Bond`, `Danish Govt Bond`, `Notas do Tesouro Nacional F`, `Obligaciones del Estado`, `Obligation Assimilable du Tresor`, `Oblig Assim Tresor Indexee I'Indice`, `Oblig Assim Tresor Indexee I'Inflation`, `Obligation Lineaire`, `Obrigacoes do Tesouro`, `Titulos de Tesoreria TES B`, `Treasury Bills`, `Treasury Notes/Bonds`, `Treasury STRIPS`, `MXBONO`, `UDIBONO`, `OMAN`, `Govt Guaranteed`, `Government other` |
| `debt_agency_supranational` | `Agencies`, `Small Business Administration` |
| `debt_municipal` | `MUNI`, `Certificates of Obligation`, `Certificates of Participation`, `Grant Antic Notes`, `Tax And Rev Antic Notes`, `Tax Antic Notes`, `Unknown Antic Types` |
| `debt_corporate_senior` | `CORP`, `Corporate Medium Term Notes`, `Senior Note`, `Senior Debenture`, `Senior Bank Note`, `Senior Secured`, `Secured Bond`, `Secured Note`, `First Mortgage Bond`, `First Mortgage Note`, `First & Refunding Mortgage Bond`, `Covered Bond`, `Hypothekenpfandbrief`, `Pfandbrief Anleihe`, `Oeffentliche Pfandbrief`, `HPF Jumbo`, `Jumbo Landesschatzanweisung`, `Sakerstallda Obligationer`, `Obligations Foncieres`, `Collateral Trust`, `Collateral Debt`, `Collateralized Notes` |
| `debt_corporate_subordinated` | `Subordinated Note`, `Senior Subordinated Note`, `Subordinated Bank Note`, `Subordinated Debenture`, `Senior Subordinated Debenture`, `Junior Subordinated Note`, `Junior Subordinated Debenture`, `Mezzanine Debt`, `Trust Preferred Security`, `Participaciones Preferentes` |
| `debt_securitized_mbs` | `Mortgage Pools`, `Mortgages`, `Mortgage Bond`, `Mortgage Note`, `Second Mortgage Bond`, `Commercial Mortgage-Backed Security`, `Collateralized Mortgage Obligation`, `CMOs`, `CMO Whole Loan`, `CMO Agricultural MBS`, `TBA`, `Pass Through Certificate` |
| `debt_securitized_abs` | `ABSY`, `Asset Backed Tranches`, `Credit Card Receivables`, `Auto/Installment Loans`, `Auto Lease Loans`, `Auto Floorplan/Wholesale Loans`, `Equipment Backed Loan`, `Aircraft Lease`, `Student Loan` |
| `debt_unsecured_general` | `Bond`, `Note`, `Unsecured Note`, `Debenture`, `Fixed Income`, `Global Bonds`, `Inhaberschuldverschreibung`, `Certificate`, `Certificates Of Indebtness`, `Other Certificates`, `Deposit Note`, `Depositary Share`, `Depository Receipts (Thailand)`, `Bank Debt`, `Bankers Acceptance`, `Trust` |
| `debt_specialty_derivatives` | `Index Linked Security`, `Index-Linked Gilt`, `Islamic Sukuk`, `Derivative`, `Interest only`, `Principal only`, `Warrants`, `Preferred Stock`, `OTHER` |

---

## 7. Morningstar Panel Synthesis

From Silver analyst/quant-split pillars, emit:

| Panel `feature_id` | Rule |
| :--- | :--- |
| `pillar_people`, `pillar_process`, `pillar_parent` | Analyst score if present, else quant score, else omit (no row) |
| `pillar_people_is_quant`, `pillar_process_is_quant`, `pillar_parent_is_quant` | `1.0` if the active score is quant, `0.0` if analyst |
| `mstar_medalist_rating` | Canonical 1–5; condition on `mstar_analyst_coverage_pct` in research (1.0 = pure analyst, 0.0 = pure quant) |
| `mstar_analyst_coverage_pct` | Carry as-is |
| `mstar_morningstar_rating`, `mstar_sustainability_rating` | Carry as-is |

---

## 8. Style-Box 12 Canonical Flags

Grid: size ∈ `{large, multi, mid, small}` × style ∈ `{value, core, growth}`.

`feature_id = style_{size}_{style}`. Active cell `1.0`, others `0.0`. Always emit all 12 so the wide schema cannot drift.

Unobserved in the entire universe (default `0.0` always): `style_large_growth`, `style_large_value`, `style_mid_value`.

Precedence: `style_box` (selected) overrides `style_box_hist` on the same date.

---

## 9. Theme Panel Features

`bronze.themes` is a 2-level hierarchy: 19 parents → 491 children. Each child maps to exactly one parent via `parent_id`. Silver stores **children only**.

### 9.1 Child features (491)

- `feature_id = theme_{dimension_code}` (UUID, e.g. `theme_dd8b04fb-8529-47df-b179-4c68d78f34e5`)
- `value` = Silver `rank_adjusted_weight` (negatives and \(> 1.0\) retained)
- Missing child → `0.0` (no exposure)
- **No sum-to-1.0**, no renormalization

### 9.2 Parent rollups (19)

- `feature_id = theme_parent_{parent_theme_id}`
- \(\text{value} = \sum_{\text{child} \in \text{parent}} \text{child\_rank\_adjusted\_weight}\) via JOIN `silver.product_dimensions` → `bronze.themes`
- Missing parent → `0.0`

| Parent theme name | Children |
| :--- | ---: |
| Technology and Innovation | 60 |
| Technology Hardware and Semiconductors | 20 |
| Consumer Goods and Retail | 44 |
| Healthcare and Biotechnology | 50 |
| Energy and Utilities | 48 |
| Entertainment and Media | 24 |
| Financial Services and FinTech | 40 |
| Telecommunications and Connectivity | 14 |
| Automotive and Mobility | 24 |
| Transportation and Logistics | 26 |
| Industrial Products and Services | 19 |
| Environmental and Sustainability Solutions | 17 |
| Aerospace and Defense | 15 |
| Construction and Infrastructure | 11 |
| Real Estate and Property Management | 22 |
| Food and Beverage | 14 |
| Mining and Metals | 18 |
| Hospitality and Leisure | 13 |
| Agriculture and Food Production | 12 |

Parent **names** are documentation; `feature_id` uses the parent UUID from `bronze.themes`.

### 9.3 Quality gate

Exclude (or null-out) theme features when `theme_coverage < 0.7`. All current products pass; implement the guard anyway. Themes are primarily Equity (85% coverage) vs Fixed Income (19%) — filter FI-specific regressions accordingly.

---

## 10. ESG Panel Features

Alias mapping applies **only** here. Silver keeps raw TRESG tags.

| Silver `metric_id` | Panel `feature_id` |
| :--- | :--- |
| `tresgs` | `esg_score` |
| `tresgcs` | `esg_combined_score` |
| `tresgccs` | `esg_controversies` |
| `tresgens` | `esg_environmental` |
| `tresgenrrs` | `esg_resource_use` |
| `tresgeners` | `esg_emissions` |
| `tresgenpis` | `esg_env_innovation` |
| `tresgsos` | `esg_social` |
| `tresgsowos` | `esg_workforce` |
| `tresgsohrs` | `esg_human_rights` |
| `tresgsocos` | `esg_community` |
| `tresgsoprs` | `esg_product_responsibility` |
| `tresgcgs` | `esg_governance` |
| `tresgcgbds` | `esg_management` |
| `tresgcgsrs` | `esg_shareholders` |
| `tresgcgvss` | `esg_csr_strategy` |
| `esg_coverage` | `esg_coverage` |

Values remain integer-origin DOUBLE on \([0, 10]\). Zero is a legitimate floor — no imputation. Gate: `esg_coverage >= 0.7` (all observed records pass; 9 snapshots lack coverage but have scores — drop those from gated regressions). Coverage \(> 1.0\) is informational (levered ETP), not an error. ESG is primarily Equity (91%) vs FI (21%).

---

## 11. Lipper & Profile Scalars on the Panel

Carry canonical IDs unchanged: `lipper_{total_return|consistent_return|preservation|expense|tax_efficiency}_{overall|3yr|5yr|10yr}`.

Profile scalars: `total_net_assets_local`, `total_expense_ratio`, `management_expense_ratio`, `non_management_expense_ratio`, `audited_net_expense_ratio`, `is_passive`, plus derived:

\[
\text{management\_fee\_rate} = \text{total\_expense\_ratio} \times \text{management\_expense\_ratio}
\]

(optional operable absolute fee; allocation ratios remain available).

---

## 12. Winsorization, Clipping & Residual Handling

**Silver is unconstrained.** These rules apply only when writing `silver.monthly_panel` (or a documented modeling view on top of it).

| Feature | Panel treatment |
| :--- | :--- |
| `management_expense_ratio`, `non_management_expense_ratio` | Winsorize to \([-1.0, 2.0]\) **or** replace with `management_fee_rate` |
| `portfolio_top_10_concentration` | Clip to \([0.0, 1.0]\) for unlevered models, **or** emit `is_leveraged = 1.0` when raw \(> 1.0\) and keep raw |
| Allocation negatives (`Cash`, `Other`, country, industry, …) | Long-only: \(\max(0, w)\) then renormalize remaining to 1.0; **or** emit dedicated `short_exposure` features. Partition-of-unity on `asset_class` must still hold in the unclipped diagnostic. |
| Profitability / leverage extremes (ROE, TA/TE, EBIT/Interest) | Winsorize at panel (e.g. cross-sectional 1st/99th pct). Raw Silver retained for audit. |
| Valuation multiples at provider cap | Optional right-censored treatment (Tobit / cap dummy). Do not alter the stored value without a dummy. |
| Theme weights \(> 1.0\) or \(< 0\) | Retain; optional long-only \(\max(0,w)\) or leveraged indicator. |
| `average_quality` with `raw_value = '-'` (`value = 10.0`) | Map to **NULL** (exclude from FI quality regressions). Do not treat as AAA. |
| `country` `Unidentified` | Drop from factor regressions. |
| `asset_class` `Other` | Isolate/drop during factor modeling. |
| Credit `Not Rated` / `Not Available`, maturity `mat_other` | Residual columns; isolate in FI models. |
| `total_net_assets_local = 0.0` | Retain the row; mask out of AUM-weighted calculations. |

### 12.1 Quality gates (filter, do not rewrite)

- Theme regressions: `theme_coverage >= 0.7`
- ESG regressions: `esg_coverage >= 0.7`
- FI-only models: drop products whose dominant `asset_class` is Equity (and vice versa) when the feature family is sleeve-specific (themes/ESG ≈ equity; credit/maturity/debt ≈ FI).

### 12.2 Partition-of-unity validation

On every `(product_id, as_of_date)`, the four `asset_class` weights must sum to \(1.0 \pm 0.0001\). Fail the panel build (or quarantine the product-date) if this breaks. Sub-allocation families are **not** required to sum to 1.0; residual buckets absorb truncation.

---

## 13. Implementation Checklist (§5.4)

- [ ] **Panel pipeline (`etfportfolio/prep/panel.py`)** producing physical long `silver.monthly_panel`.
- [ ] Unified month-end calendar spine.
- [ ] LOCF with the staleness matrix in §4 (ESG 90d; monthly 180d; medalist/fees 540d; tenure/style 365d; `is_passive` perpetual).
- [ ] 12 style-box flags, 3 unobserved default `0.0`, `style_box` ≻ `style_box_hist`.
- [ ] 110 → 9 debt clusters (exhaustive map in §6).
- [ ] 491 child themes + 19 parent rollups via `bronze.themes`.
- [ ] Dynamic tenure formula (§5).
- [ ] ESG TRESG → human-readable aliases (§10).
- [ ] Morningstar pillar synthesis + quant indicators (§7).
- [ ] `asset_class` sum-to-1.0 validation; residual handling (`Other`, `Unidentified`, `Not Rated`).
- [ ] Panel-layer winsorization / quality gates (§12) — never in extractors.
