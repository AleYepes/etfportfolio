# Functional Requirements Document: Dalio Gauge Emulation MVP

*Revision 4. Supersedes all prior FRDs. Self-contained specification for refactoring and validating the exploratory Jupyter notebook that emulates the country gauges in the Appendix of Ray Dalio's "The Great Powers Index: 2024" (pages 132–184).*

---

## 1. Purpose, Scope, and Working Principles

**Purpose.** A single Jupyter notebook ingests macroeconomic, governance, health, and social indicators for every ISO3 sovereign economy (`UNIVERSE`, ~250 economies) and aggregates them into declarative "gauge trees" that emulate the Appendix gauges of *The Great Powers Index: 2024*. The MVP exists to define, test, and document the gauge tree, data-quality screen, and scoring rules cleanly enough that a separate production pipeline (not visible here) can re-implement them for the evolving country cohort in `silver.observations` (currently ~109 countries).

**Priorities (in strict order):**
1. **Structural fidelity:** Every Appendix-cited source or indicator is either fetched programmatically or recorded in a derived ledger as a coded `GAP` or `DROPPED`; published Appendix weights match exactly; every component carries explicit provenance, page reference, and citations.
2. **Ordinal & numeric validation:** Rank correlations and numeric sanity checks against the Appendix's published 2024 tables (printed as status lines/warnings, never hard failures).
3. **Resilient cross-country coverage:** Indicators are screened across `UNIVERSE` using observation-weighted (`OBS_WEIGHTS`) coverage and recency rules so gauges remain stable as `silver.observations` evolves.

**In scope:**
- **Phase 1:** Tree structure, exclusions, all 15 Appendix weight locks, closed `GAP` reason codes, and derived source ledger (`W01`–`W20` + 64 A.7 sources + concept rows).
- **Stage 2:** Fetch repairs (`F-2`), bounded replacement fetches and recurring open-data probes (`F-6`), and hygiene cleanup (`F-7`).
- **Phase 1.5:** Raw coverage panel (`S-1`), dynamic outlier + rankable-recency screen (`S-2`), deterministic single-series resolve step (`S-3`), simplified non-stitching engine with internal linear interpolation (`S-4`), and updated diagnostic reports (`S-5`).
- **Phase 2:** Print-only structural/data checks (`V-1`), Appendix D reference registry & Spearman comparisons (`V-2`), Empire Score & sensitivity tables (`V-3`), `Dalio-35` validation pass (`V-4`), and all 8 p.135 per-gauge comparisons (`V-5`).

**Out of scope (parked for future work—do not implement):**
- **Phase 3:** Income-adjusted scoring ("relative to similar income" peer adjustments or regressions) and historical 10-year growth back-testing/linear calibration.
- Production database writes (`data/etf.duckdb` is read-only), manual file downloads, forward/backward extrapolation past observed endpoints, tuning weights to fit Appendix outputs, and bilateral (pairwise) modelling for External Conflict (`D11`).

**Working principles:**
1. **Correctness over coverage:** Never stitch incompatible indicators (`D20`). Each component resolves to at most one series.
2. **Lean & non-blocking:** One notebook, plain dicts/namedtuples (no custom classes, `D26`), no `raise`, no `assert`, and no defensive try/except blocks masking logic bugs. Checks print a status line and continue (`D4`, `D31`).
3. **Display, do not save reports:** Diagnostic tables are displayed DataFrames. Only raw caches (`data/dalitest/raw/<source>/data.parquet`) and the three engine outputs (`component_scores.parquet`, `node_scores.parquet`, `rank_support.parquet` under `data/dalitest/out/`) persist (`D5`).
4. **Screen raw, interpolate internal gaps only:** Coverage reporting and screening inspect raw actual observations only; the engine applies internal linear interpolation (`limit_area="inside"`) per country with **zero extrapolation** (`D19a`).
5. **Preserve secret handling:** Keep the existing `EIA_API_KEY` handling in Cell 0 untouched; never print or copy keys into outputs.

---

## 2. Settled Owner Decisions and Rationale

| ID | Decision | Rationale |
| --- | --- | --- |
| **D1–D4** | Structural fidelity first; print-only checks (`no assert`, `no raise`); simplify engine/cell order freely while keeping `STD_COLS` and `cached()` semantics. | MVP defines a legible spec for production re-implementation; Dalio uses proprietary inputs so exact numerical replication is neither expected nor tuned for. |
| **D5–D8** | Persist only raw parquets and 3 engine parquets. Ingest for `UNIVERSE` (~250 ISO3); report `cov` (share of eligible `UNIVERSE`) and `cov_obs` (weighted by `OBS_WEIGHTS` from `silver.observations`). Rank against `UNIVERSE` (`MIN_CROSS_SECTION = 30`). Add a validation-only `Dalio-35` pass (`min_cross_section = 20`, `write_outputs = False`). | Keeps scores invariant when `silver.observations` changes while letting Phase 2 compare like-with-like against the Appendix's 35-country validation cohort. |
| **D9** | **Universal transform:** Signed annual cross-sectional percent rank `(rank - 1) / (n - 1) * 100` (`0–100`, higher is better) for every gauge. No z-scores. | Eliminates transform mismatches, handles extreme small-state outliers cleanly, and matches the p.135 Empire Score scale. |
| **D10–D11** | One top-level tree per Appendix gauge (14 trees; drop legacy cycle wrappers). Descope **External Conflict** (gauge #19). | Appendix lists gauges flat; External Conflict is pairwise across country-pairs and out of scope for a single-country MVP. |
| **D12** | **Cited-only rule:** Keep a component only if the Appendix cites its indicator, source, or concept; record omissions in `EXCLUDED_COMPONENTS`. | Preserves clear PDF-to-code lineage. |
| **D13–D14** | **Size/intensity model:** Eight gauges (`education`, `innovation_and_technology`, `economic_output`, `trade`, `military`, `financial_center`, `infrastructure_and_investment`, `geology`) carry flat components tagged `kind` = `size` (raw totals: USD, counts, tonnes, kWh) or `intensity` (per-capita, % GDP, per-worker, index, test score). `size_share` = `0.75` for `economic_output`, `trade`, `military`; `0.50` for the other five. Node score = **Total**; post-hoc **Quality-only** uses `intensity` components only. | Captures Dalio's distinction between scale and capability without duplicating trees; matches Appendix prose ("about half" vs. "primarily/mostly absolute"). |
| **D15** | **Empire Score determinant weights (`EMPIRE_RAW` / `0.94`):** `economic_output` `0.15`, `financial_center` `0.15`, `reserve_currency` `0.12`, `military` `0.12`, `innovation_and_technology` `0.12`, `trade` `0.10`, `education` `0.10`, `competitiveness` `0.08`. Sensitivity vs. equal `1/8` in `V-3`. | Appendix publishes no Empire Score weights; keeps owner heuristics transparent and testable. |
| **D17–D18** | **Derived ledger & retention:** Compute ledger status from tree + resolved state + data (`no manual intended column`). Keep fetching flagged series (`df_full`) for auditability; filter `df_master` via `KEEP_CODES`; unconsumed A.7 sources (`NO_CONSUMER`) are not fetched. | Prevents ledger drift and keeps screen evidence visible until production pruning. |
| **D19a** | **No extrapolation; internal linear interpolation only (amends prior no-fill rule):** Screen raw non-projected observations. In the engine (`S-4`), apply per-country internal linear interpolation (`limit_area="inside"`) between observed years on resolved series; never extrapolate forward or backward past observed endpoints. | Prevents multi-year ($\ge 3\text{y}$) extrapolation (the original motivation behind `STALE_BEFORE = 2023`) while cleanly bridging minor internal gaps in recurring series without fabricating coverage metrics. |
| **D20–D21** | **Single series per component (no stitching):** Order candidates `exact` $\rightarrow$ `substitute_source` $\rightarrow$ `proxy_concept` (list order within tier) and select the first candidate passing `S-2`. | Stitching heterogeneous concepts (e.g., ILO vs. PWT weekly hours, $r \approx 0.0$) mixes methodologies silently. |
| **D22–D23** | **Strict uniform screen + rankable recency:** Flag any candidate if: (1) `weighted_cov_% < fence_cov` (`median - 3 * IQR`), OR (2) `weighted_recent_% < fence_recent` (`median - 3 * IQR`), OR (3) it has **no rankable year (`n_reporting >= 30`) at or after `STALE_BEFORE` (`2023`)**. Dropped components are removed across **all years** (`DISCONTINUED` if latest actual `< STALE_BEFORE`, else `LOW_COVERAGE`). | Preserves dynamic outlier detection while preventing collapsed recent series (e.g., `ILO.STRIKES_*`, `FR.INR.LEND`) from passing a ~6.9% recency fence with zero or near-zero rankable coverage in 2023+. Survey-cycle series (`Doing Business`, `Barro-Lee`, `HCI`, `LPI`, `PISA` if stale) are dropped unless recurring `2023+` replacements land (`Q2`, `Q6`). |
| **D24–D25** | Bounded programmatic replacement search (`F-6`) including recurring open-data targets (`WIPO GII`, `OECD` hours/retirement, `PISA 2022`/`UNESCO UIS`) before dropping flagged series. Closed 9-code `GAP_REASONS`: `PROPRIETARY`, `NO_PUBLIC_ROUTE`, `MANUAL_DOWNLOAD`, `PROBED_FAILED`, `HEAVY`, `OWNER_SKIP`, `NO_CONSUMER`, `DISCONTINUED`, `LOW_COVERAGE`. Remove `PROBE_NEXT`. | Gives recurring open alternatives a fair chance under strict recency while keeping heavy/manual pipelines out of scope (`Q7`), and standardizes every missing item with verifiable evidence. |
| **D27–D29** | **Structural exclusions (`INELIGIBLE`) & panel rules:** Euro-area members (`EURO_AREA`) receive no reserve-currency rows and are excluded from the denominator of coverage/screen calculations for `IMF.COFER_SHARE`, `BIS.FX_TURNOVER_SHARE`, `BIS.INTL_DEBT_CCY_SHARE`. Landlocked economies are excluded for `UNCTAD` shipping if wired. `df_cov` has no thresholds and includes projections (`is_projection = True`); scored gauges use actuals only. | Appendix leaves Eurozone countries as dashes on p.135; prevents intentional structural exclusions from triggering coverage flags, while keeping projections visible in coverage diagnostics without mixing forecasts into scores. |
| **D30–D33** | `V-2`–`V-5` validate world-relative Appendix D tables via Spearman at `ASOF_YEAR` (`2023`) as print-only checks. Direct inspection of Appendix p.135 confirms all 8 column pairs (`V-5` unblocked). Keep `competitiveness` mapped to `@productivity_gauge.what_you_pay_vs_what_you_get` in the tree, and test the `Competitiveness + Culture + Investment` composite as a `V-3` validation sensitivity. **Phase 3 is parked** (deferred). | Preserves p.132 prose fidelity while empirically testing p.135's `"Competitiveness, Culture, Investment"` header (`Q4`). Keeps the MVP lean, regression-free, and focused on extending weighted-sum `0–100` gauges to the `UNIVERSE` / `109+` country set (`Q12`, `Q13`). |

---

## 3. Notebook Architecture & Mechanics

The notebook runs top-to-bottom in a single file with master long schema `STD_COLS = ["country_code", "indicator_code", "year", "value", "source", "is_projection"]`.

- **Caching (`cached(source, fetch_fn, refresh)`):** Reads/writes `data/dalitest/raw/<source>/data.parquet`. If a fetch cell is modified, add its source key to `REFRESH` (or delete its cache folder) so new data is pulled. On network failure, `cached` retains the prior snapshot and prints a warning.
- **Tree schema:** Plain nested dicts + `Alt(code, tier, direction, status, reason)` namedtuples built with `alt()` / `gap()`. Nodes hold `weight`, `provenance` (`"appendix"` or `"heuristic"`), and one of `children`, `components`, or `reference` (`"@tree.node"`). Tagged leaves also hold `size_share` (`0.50` or `0.75`).
- **Component schema:** `{name, weight, alts, basis, page, cites, kind?, note?}`, where `basis` is `"appendix"`, `"appendix_concept"`, or `"appendix_source"`. Effective provenance of a component is `"appendix"` only when both its node `provenance == "appendix"` and its `basis == "appendix"`.
- **Current defects to fix from the baseline notebook:**
  1. Cell 4 references an undefined `df_probes` in `all_raw_frames` (crashes a clean run).
  2. Cell 6b (`_engine_selftest`) uses `assert`s (`D4`).
  3. Cell 0 retains `ALLOW_PROXY_FALLBACK` and Cell 5 retains `PROBE_NEXT`.
  4. Cell 2a fails on 10 WDI codes; Cell 2b requests invalid `ENF.CONT.COEN.DB0415`; Cell 2i (`COVID.EXCESS_MORTALITY_PSCORE`) returns 0 rows silently; Cell 2l lacks `WHR.POSITIVE_AFFECT` & pre-2019 `WHR.SOCIAL_SUPPORT`.
  5. Cell 6 stitches alternatives (`alt_used`) instead of using `S-1`–`S-4`; `Agreement between alternatives` ignores `direction` signs; `APPENDIX_WEIGHTS` checks only 3 of 15 paths; `APPENDIX_SOURCES` lacks `W01`–`W20`.
  6. `GFP.POWERINDEX` currently carries `PROPRIETARY` instead of `OWNER_SKIP` (`D25`), `WAGE` stand-ins carry `exact` instead of `proxy_concept`, and `conflict_media_discontent` bundles 4 GAPs into one component.

---

## 4. Detailed Requirements

### 4.1 Model and Tree (`M`)

- **M-1 Tree Shape (MUST):** Keep the exact 14 top-level trees: `health_index`, `happiness_index`, `productivity_gauge` (`what_you_pay_vs_what_you_get` [4 leaves] + `culture` [6 sub-gauges]), `indebtedness_gauge` (`debt_levels_and_flow` + `monetary_policy`), `future_growth` (`productivity_impact -> @productivity_gauge`, `indebtedness_impact -> @indebtedness_gauge`), `strength_determinants` (`economic_output`, `financial_center`, `reserve_currency`, `military`, `innovation_and_technology`, `trade`, `education`, `competitiveness -> @productivity_gauge.what_you_pay_vs_what_you_get`), `infrastructure_and_investment`, `resource_allocation_efficiency`, `geology`, `character_determination_civility`, `governance_rule_of_law`, `acts_of_nature`, `internal_conflict`, `wealth_values_gap`. Do not rename nodes.
- **M-2 Component & Tier Refinements (MUST):**
  - Re-tag the `WAGE` stand-in candidates (`IMF.NGDPDPC`, `NY.GDP.PCAP.CD` in `GDP_PC_COST()`) under `education_cost_quality_adj` and `labor_productivity_cost_adj` to `proxy_concept` (so `WAGE` reports `proxy` in the ledger and Productivity's nominal tier breakdown reflects the stand-in).
  - Split `internal_conflict/conflict_media_discontent` (weight `1/6`) into four components at `1/24` each (keeping the other five `internal_conflict` components at `1/6` each): one each for Cline Center (`11`, `CLINE.CONFLICT_PER_CAPITA`), GDELT media discontent (`14`, `GDELT.DISCONTENT_TERMS`), GDELT govt disapproval (`23`, `GDELT.GOVT_DISAPPROVAL`), and GDELT protest intensity (`45`, `GDELT.PROTEST_INTENSITY`), so each source reports its own accurate `GAP` reason in the ledger.
  - Add `DERIVED.PER_CAPITA.NE.EXP.GNFS.CD` as a `substitute_source` candidate under `strength_determinants/trade/exports_pc` (with `DERIVED.PER_CAPITA.BX.GSR.GNFS.CD` as primary `exact`).
  - Set `GFP.POWERINDEX` `GAP` reason to `OWNER_SKIP` (`D25`).
- **M-3 Exclusions & Cited-Only Check (MUST):** Confirm every component cites an Appendix indicator, A.7 source, or concept, and verify `EXCLUDED_COMPONENTS` documents all 8 removals: `external_order_cycle` (descoped pairwise gauge); legacy cycle wrappers (`economic_financial_cycle`, `internal_order_cycle`, `additional_measures`); `governance_rule_of_law` WGI `GE.EST`/`RQ.EST`; `internal_conflict` WGI `PV.EST`; `geology` OWID fossil fuel production; `economic_output` nominal GDP/GDP per capita; `financial_center` private credit; `capital_stock_growth` perpetual-inventory alternative.
- **M-4 Size/Intensity & Quality-Only (`df_kinds`) (MUST):** Keep `size_share` (`0.75` for `economic_output`, `trade`, `military`; `0.50` for the other 5 tagged leaves), `kind` tags, and `df_kinds` display unchanged. Update Cell 6c (`quality_only_scores`) to read the simplified `df_comp_scores` schema (`code` column, no `alt_used`) and compute **Empire Score Quality-only** using renormalised `D15` weights across the 6 determinants that have `intensity` tags (`competitiveness` and `reserve_currency` have no Quality-only view and are excluded, stated explicitly in the output).
- **M-5 / M-6 Weight Locks (MUST):** Populate `APPENDIX_WEIGHTS` in Cell 7b for all **15 Appendix paths** (compared against the **nominal** tree by component/child name within `0.005`, print-only):
  1. `health_index` (p.136): `life_expectancy` `0.30`, `food_insecurity` `0.15`, `essential_health_services` `0.15`, `child_mortality` `0.10`, `homicide` `0.075`, `displaced_population` `0.075`, `air_pollution` `0.05`, `safely_managed_drinking_water` `0.05`, `basic_sanitation` `0.05`
  2. `happiness_index` (p.136): `share_reported_happy` `0.225`, `share_reported_daily_enjoyment` `0.225`, `life_satisfaction` `0.225`, `good_social_support_network` `0.225`, `suicide_rate` `0.10`
  3. `future_growth` (p.143): `productivity_impact` `0.65`, `indebtedness_impact` `0.35`
  4. `productivity_gauge` (p.158/169): `what_you_pay_vs_what_you_get` `0.70`, `culture` `0.30`
  5. `productivity_gauge/what_you_pay_vs_what_you_get` (p.158): `0.25` each (`education_cost_quality_adj`, `labor_productivity_cost_adj`, `working_hard`, `investing`)
  6. `.../working_hard` (p.158): `avg_hours` `2/3`, `dependency_ratio_10y_fwd_change` `1/3`
  7. `.../investing` (p.158): `[3.1, 3.1, 3.1, 3.1, 2.6, 2.6] / 17.6` (normalized via `norm()`, as the Appendix's printed components sum to `17.6` against a `17.5` header)
  8. `productivity_gauge/culture` (p.169): `1/6` each (6 sub-gauges)
  9. `.../culture/corruption` (p.171): `transparency_index` `0.5`, 3 survey items `1/6` each
  10. `.../culture/bureaucracy` (p.173): `1/3` each (3 items)
  11. `.../culture/rule_of_law` (p.175): `1/4` each (4 items)
  12. `.../culture/savoring_life_vs_achieving` (p.177): `observed_outcomes` `0.5`, 7 WVS items `0.5/7` each
  13. `.../culture/innovation` (p.179): 4 inputs `0.125` each, `outputs_patents` `0.125`, `outputs_royalties` `0.125`, 4 remaining outputs `0.0625` each
  14. `.../culture/self_sufficiency` (p.181): `hard_work_hours` `0.25`, `hard_work_lfpr`/`retire_age`/`vacation` `1/12` each, `gov_support_transfers`/`outlays` `0.125` each, `rigidity_collective_bargaining`/`hiring_firing`/`min_wage` `1/12` each
  15. `indebtedness_gauge` (p.143): `debt_levels_and_flow` `0.50`, `monetary_policy` `0.50`
- **M-7 Gauge Map (MUST):** Keep `APPENDIX_GAUGE_MAP` (21 entries; `External Conflict -> None`). Print a notice if any mapped node has zero scores after resolution.
- **M-8 Effective-Weights Export (MUST):** Update the tree export in Cell 7a to display `gauge`, `leaf_path`, `component`, `kind`, `basis`, `page`, `cites`, `nominal_weight`, `effective_weight`, `effective_provenance` (`"appendix"` iff node provenance and component basis are both `"appendix"`), `resolved_code`, `resolved_tier`, `state` (`live`/`gap`/`dead`/`dropped`), `drop_reason`, and `alts`, plus a per-gauge nominal-weight share summary by `state`, `tier`, `basis`, and `dropped` share.
- **M-9 Directions (MUST):** Keep all existing `direction` signs unchanged. Retain the ambiguous-signs display table.

### 4.2 Data, Fetches, and Ledger (`F`)

- **F-1 Re-baseline (MUST):** Run a clean top-to-bottom baseline after fixing `df_probes` (or read current caches if offline) and record baseline counts before Stage 2 edits.
- **F-2 Fetch Repairs (MUST):** Diagnose, fix, add source to `REFRESH`, and verify rows (or mark `PROBED_FAILED` with URL/HTTP error):
  - **F-2.1 (Cell 2a WDI):** Print per-code exceptions and add a paged World Bank v2 REST fallback (`https://api.worldbank.org/v2/country/all/indicator/{code}?format=json&per_page=20000&date=1950:2025`, mapping `countryiso3code` to `UNIVERSE`) for the 10 failing codes (`AG.LND.TOTL.K2`, `BX.GSR.GNFS.CD`, `EN.ATM.PM25.MC.M3`, `ER.H2O.INTR.K3`, `FM.LBL.BMNY.ZG`, `FP.CPI.TOTL.ZG`, `GC.DOD.TOTL.GD.ZS`, `SE.TER.CUAT.BA.ZS`, `SE.XPD.TOTL.GD.ZS`, `SH.STA.SUIC.P5`).
  - **F-2.2 (Cell 2i OWID COVID):** Debug and repair date/column parsing for `excess-mortality-p-scores-average-baseline` (2020–2022 mean stamped `2022`), or mark `PROBED_FAILED`.
  - **F-2.3 (Cell 2b WB-DB):** Fix or prune invalid archive code `ENF.CONT.COEN.DB0415`.
  - **F-2.4 (Cell 2l WHR):** Probe the WHR Table 2.1 country-year panel on `worldhappiness.report` / `files.worldhappiness.report` for `WHR.POSITIVE_AFFECT` and pre-2019 `WHR.SOCIAL_SUPPORT`; verify every row uses its real year (never silently defaulting missing years to `2024`).
  - **F-2.5 (Cell 3 Derived):** Verify `DERIVED.INFLATION_VOL10` and `DERIVED.PER_CAPITA.BX.GSR.GNFS.CD` build once F-2.1 lands, and build `DERIVED.PER_CAPITA.NE.EXP.GNFS.CD`.
- **F-3 GAP Protocol (MUST):** Update `GAP_REASONS` to the closed 9-code set (`D25`), removing `PROBE_NEXT`. Every `GAP` candidate's `reason` attribute MUST be an exact code from `GAP_REASONS` (so Cell 7b's membership check validates cleanly), while supporting evidence (vendor name for `PROPRIETARY`, URL for `NO_PUBLIC_ROUTE`/`MANUAL_DOWNLOAD`, URL + HTTP error for `PROBED_FAILED`, route + size for `HEAVY`) is recorded in the component's `note` (or `SOURCE_REASON`/evidence field).
- **F-4 Derived Ledger (MUST):**
  - Append `W01`–`W20` (Appendix C) to `APPENDIX_SOURCES` and add them to consuming components' `cites`.
  - Add `#49` (*Real growth rate*) to `productivity_gauge/.../investing/income_volatility`'s `cites`.
  - Compute ledger status from `df_resolution` (`S-3`) + `df_master`: `live` / `substitute` / `proxy` (from resolved tier), `dropped` (data existed, all candidates failed `S-2`, carrying `DISCONTINUED` or `LOW_COVERAGE`), `gap` (all candidates declared `GAP`), `dead` (promised candidate has no data), `skipped` (unconsumed: `NO_CONSUMER` or `OWNER_SKIP`).
- **F-5 Retention (MUST):** Keep all existing fetch cells and lists; keep `KEEP_CODES` retention on `df_master` while preserving `df_full`. Append all new `cached()` frames to Cell 4.
- **F-6 Replacements & Bounded Recurring Probes (MUST):**
  1. **UNDP HDR (`hdr`):** Fetch `https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Composite_indices_complete_time_series.csv` (`encoding="latin-1"`). Wire `HDR.MYS` (`substitute_source` after `BAR.SCHL.25UP`, cites `16`) and `HDR.EYS` (`proxy_concept`). Verify Spearman agreement with `BAR.SCHL.25UP` in `2000`, `2005`, `2010`.
  2. **UNCTAD Shipping (`unctad`):** Probe and parse `https://unctadstat-api.unctad.org/bulkdownload/US.LSCI/US_LSCI`. If valid and `>= 2023`, wire `UNCTAD.LSCI` (`substitute_source` after `IS.SHP.GCNW.XQ` or `exact` if preferred for recency—keep `IS.SHP.GCNW.XQ` ahead only if `exact`, otherwise `S-3` skips discontinued `IS.SHP.GCNW.XQ` and picks `UNCTAD.LSCI`) and declare landlocked countries in `INELIGIBLE`.
  3. **Indebtedness (`imf_fin` / `wb_debt`):** One bounded probe of IMF `MFS_IR` (lending/policy/real rates), IMF `IIP`, and WB databases `54` (JEDH), `22`/`23` (QEDS) for gross external debt `% GDP`. Wire passing series as `substitute_source` or `proxy_concept` (`lending_rate`, `real_interest_rate`, `foreign_ccy_debt`).
  4. **Targeted Recurring Open-Data Probes:** One bounded HTTP check each for (a) `WIPO GII` annual spreadsheets/tables (`WIPO.GII_SCORE`, `WIPO.NEW_MAJOR_WEBSITES`), (b) `OECD` SDMX effective retirement age / annual hours worked, and (c) `PISA 2022` / `UNESCO UIS` (`api.uis.unesco.org`) education quality. Leave `WVS`, `GEM`, `GDELT`, and `USGS` as coded GAPs (`MANUAL_DOWNLOAD` / `HEAVY` / `NO_PUBLIC_ROUTE`).
- **F-7 Hygiene (MUST):** Remove `df_probes` from Cell 4, delete Cell 6b, remove `ALLOW_PROXY_FALLBACK`, keep cache fallbacks on network errors, and document in a comment that `DALIO_24` holds 23 ISO3 codes (Eurozone excluded).

### 4.3 Coverage Panel, Screen, Resolve, and Engine (`S`)

Implement in order `S-1` $\rightarrow$ `S-2` $\rightarrow$ `S-3` $\rightarrow$ `S-4` $\rightarrow$ `S-5` (e.g., new Cell `5c` for `S-1`–`S-3`, Cell `6` for `S-4`, updated `6c`/`7a`).

- **S-1 Coverage Panel `df_cov` (MUST):** Built from raw `df_master` (all years per indicator, including projections flagged `is_projection`). Columns: `indicator_code`, `year`, `n_eligible`, `n_reporting`, `cov` (`n_reporting / n_eligible`), `cov_obs` (sum of `OBS_WEIGHTS` over reporting eligible countries divided by sum over all eligible countries), `is_projection`. Use `INELIGIBLE = {"IMF.COFER_SHARE": EURO_AREA, "BIS.FX_TURNOVER_SHARE": EURO_AREA, "BIS.INTL_DEBT_CCY_SHARE": EURO_AREA, ...}`. Display an example aggregating `df_cov` across a year range and across a gauge's components.
- **S-2 Coverage Screen (MUST):** Computed strictly on **actual (`~is_projection`), non-interpolated** data for every indicator in `df_master` (if a series has zero actuals, use all rows):
  - `weighted_cov_%` = $100 \times (\sum \text{OBS\_WEIGHTS}_{\text{eligible ever reported}}) / (\sum \text{OBS\_WEIGHTS}_{\text{all eligible}})$
  - `weighted_recent_%` = same over eligible countries with an actual observation in `year >= STALE_BEFORE`
  - `rankable_recent` = `True` iff at least one year `>= STALE_BEFORE` has eligible `n_reporting >= MIN_CROSS_SECTION` (`30`).
  - Compute `fence_cov` and `fence_recent` as `median - 3 * IQR` across all retained indicators in `df_master`; print both fences.
  - Flag any series where `weighted_cov_% < fence_cov` OR `weighted_recent_% < fence_recent` OR `not rankable_recent`.
  - Reason code: `DISCONTINUED` if `latest_actual_year < STALE_BEFORE`, else `LOW_COVERAGE`. Compare flagged set against Appendix E and print differences (informational).
- **S-3 Resolve Step (MUST):** Run once on `UNIVERSE` before any engine call to build `trees_resolved` and displayed `df_resolution`:
  - Sort each component's candidates by `(tier_rank, list_index)` where `tier_rank = {"exact": 0, "substitute_source": 1, "proxy_concept": 2}`. Skip `GAP` candidates and candidates with 0 rows in `df_master`.
  - Pick the first remaining candidate not flagged by `S-2`.
  - If none pass, drop the component: `state = "dropped"` if any candidate had data (reason = highest-tier flagged candidate's reason), `"gap"` if all candidates are `GAP`, else `"dead"`.
  - In `trees_resolved`, keep `alts = [chosen_alt]`, preserve `nominal_weight`, and renormalise `weight` across surviving components within each leaf (and across surviving leaves/children within each parent). Update `@reference` targets to resolved nodes.
  - `df_resolution` columns: `gauge`, `leaf_path`, `component`, `kind`, `nominal_weight`, `resolved_code`, `resolved_tier`, `state`, `reason`, `rejected`.
  - Print summary counts (`resolved`, `dropped`, `gap`, `dead`), per-gauge share of nominal weight dropped, and post-drop effective `size_share` for each of the 8 tagged gauges.
- **S-4 Engine `run_gauge_engine` (MUST):**
  - Signature: `run_gauge_engine(df, trees, year_floor=1950, min_cross_section=30, universe=None, write_outputs=True)`.
  - Consume `trees_resolved` (`alts[0]` only; remove `alt_used`, coalescing loop, and `allow_proxy_fallback`).
  - Build consecutive integer `years = list(range(year_floor, last_actual_year + 1))` (or only years with any actuals if consecutive) from non-projected rows (`~is_projection`), pivot each resolved indicator into `years x countries`, and apply **internal linear interpolation per country** (`w.interpolate(method="linear", axis=0, limit_area="inside")`) *before* ranking—never extrapolate outside a country's `[first_actual_year, last_actual_year]`.
  - Rank within each year over countries with values iff `n >= min_cross_section`: `(rk - 1) / (n - 1) * 100` (average ties, signed by `direction`).
  - Aggregate bottom-up with weight renormalisation over present components/nodes, tracking `weight_present`.
  - Persist `component_scores.parquet` (`country_code, year, leaf_path, component_name, code, score, observed`), `node_scores.parquet`, and `rank_support.parquet` (recording `n_ranked` post-interpolation) to `OUT_DIR` when `write_outputs=True`.
- **S-5 Cell 7a Reports (MUST):** Display: (1) Series health (from `df_cov` + `S-2`), (2) Registry (`wired`/`stand_in`/`derived_input`/`carried`/`no_consumer`), (3) Candidate coverage & dead candidates, (4) Effective-weights tree export (`M-8`), (5) Direction map, (6) Agreement between alternatives computed on **direction-signed** values (`value * direction`), (7) Resolution table (`S-3`). Delete `Alternative switches` and `Proxy candidates`.

### 4.4 Validation (`V`)

- **V-1 Cell 7b Print-Only Checks (MUST):** Print with one-word status (`OK`/`WARN`/`INFO`), never raising:
  - Structure: `@reference` resolution, `GAP_REASONS` validity, `cites` resolution in `APPENDIX_SOURCES`, `basis`/`page` completeness, `APPENDIX_GAUGE_MAP` non-empty scores, zero overlap with `EXCLUDED_COMPONENTS`.
  - Weights: all 15 `APPENDIX_WEIGHTS` locks (`max_abs_diff <= 0.005`), `65/35` and `70/30` splits, hours-worked effective weight (`15.45%` nominal in Appendix), `D15` normalized weights.
  - Ledger: counts by status, list of `dropped` and `dead` items, `PROBE_NEXT` count (`== 0`).
  - Data sanity: `IMF.NGDPDPC` (2024) vs `PDF_NGDPDPC_35` already embedded in Cell 7b (median rel dev; flag `> 30%`), `DERIVED.WORKING_AGE_GROWTH_FWD10` (2024) vs `PDF_WA_GROWTH_FWD10` already embedded in Cell 7b (share within `0.3 pp`).
  - Empire Score (`ASOF_YEAR = 2023`): Spearman for Total (`strength_determinants`) and Quality-only vs Appendix p.135 across all 23 countries and filtered to `weight_present >= 0.6`.
- **V-2 / V-5 Appendix D Reference Registry (MUST):** Embed `APPENDIX_REFERENCE` from Appendix D (mapping Appendix codes via A.4) and display a validation table of Spearman correlations at `ASOF_YEAR` (with `n`, `rho`, and label `strong >= 0.7`, `moderate >= 0.4`, `weak < 0.4`):
  - **p.135 All 8 Determinants + Empire Score (`V-5`):** Compare `empT`/`empQ` (`strength_determinants`), `EduT`/`EduQ` (`education`), `InnT`/`InnQ` (`innovation_and_technology`), `CmpT` (`competitiveness` Total only), `MilT`/`MilQ` (`military`), `TrdT`/`TrdQ` (`trade`), `EcoT`/`EcoQ` (`economic_output`), `FinT`/`FinQ` (`financial_center`), and `ResT` (`reserve_currency` Total only; note `ResQ` has no Quality-only equivalent in the tree per `D13`/`M-4`).
  - **pp.146–170 Impact & Growth Vectors:** Compare `productivity_gauge` vs p.150; `indebtedness_gauge` vs p.151 (note proprietary stats); `what_you_pay_vs_what_you_get` vs p.159; `culture` vs p.170; `future_growth` vs p.146/149 (informational); the two cost leaves vs p.164/165 (informational).
  - **pp.172–182 World-Relative Orderings:** Compare the 6 `culture` sub-gauges against their World-relative ranked lists (position = rank, first = best).
- **V-3 Empire Score Sensitivities (MUST):** Compare `D15` Total weights against equal `1/8` weights (and note `n` of 23 covered), plus test a validation-only composite for p.135's 4th determinant (`"Competitiveness, Culture, Investment"`—e.g., equal-weighted average of `what_you_pay_vs_what_you_get`, `culture`, and `infrastructure_and_investment`) against p.135's `CmpT` column.
- **V-4 Dalio-35 Validation Pass (MUST):** Run `run_gauge_engine(df_master, trees_resolved, year_floor=YEAR_FLOOR, min_cross_section=20, universe=[c for c in DALIO_35 if c in UNIVERSE], write_outputs=False)` and display its `ASOF_YEAR` Empire Score & p.135 Spearman correlations side-by-side with the full-`UNIVERSE` run.

---

## Appendix A. Ground Truth from *The Great Powers Index: 2024* (pp.132–184)

### A.1 Gauge Descriptions (pp.132–134)
1. **Education:** Basic & higher education split evenly; ~half absolute quantity, ~half quality (higher-ed rankings, test scores, avg years of schooling).
2. **Innovation & Technology:** ~Half absolute share (patents, researchers, R&D spend, VC funding); ~half external rankings & per-capita innovation.
3. **Cost Competitiveness:** Quality-adjusted and productivity-adjusted labor costs plus productivity measures. Only Total shown on p.135.
4. **Infrastructure & Investment:** Share of global investment, overall infrastructure quality, investment & savings `% GDP`, logistics performance.
5. **Economic Output:** Primarily PPP-adjusted GDP as share of world; some weight on PPP GDP per capita.
6. **Productivity/Output Growth:** 10-year growth per worker = `65%` Productivity + `35%` Indebtedness (p.143 table overrides 2/3–1/3 prose).
7. **Trade:** Absolute exports as share of world.
8. **Military Strength:** Mostly absolute share of spending, personnel, nuclear weapons, external capability indices.
9. **Financial Markets & Financial Center:** Absolute transaction shares & market caps + qualitative financial-center city indices.
10. **Reserve Status:** Currency shares of FX transactions, international debts, and official reserves. Euro-area countries are not scored individually (`---` on p.135).
11. **Indebtedness:** Debt levels vs assets/incomes, debt growth vs income growth, internal/external deficits, debt service `% GDP`, own- vs foreign-currency debt, domestic vs foreign holders, credit rating, monetary policy stance (easier policy = higher 10y growth). Underlying stats proprietary (p.157).
12. **Internal Conflict:** Conflict events (protests/strikes), political partisanship, survey/media discontent.
13. **Rule of Law/Governance:** Rule of law + corruption (business surveys + external indices).
14. **Gaps in Wealth, Opportunity & Values:** Top 1%/10% wealth & income inequality + political/ideological legislative split.
15. **Geology:** Land area, resource rents/value, production of energy/food/industrial metals, net exports (self-reliance), freshwater.
16. **Character/Civility/Determination:** Hard-work/success surveys + self-sufficiency proxies (hours worked, LFPR, transfers, retirement age).
17. **Resource Allocation Efficiency:** Chronic unemployment, debt growth vs income growth, labor rigidity, ease of getting loans.
18. **Acts of Nature:** Climate-change GDP vulnerability, disaster readiness, COVID outcomes.
19. **External Conflict:** Pairwise conflict (descoped per `D11`).
20. **Health Index (p.136):** Life expectancy `30%`, food insecurity `15%`, essential health services `15%`, child mortality `10%`, homicide `7.5%`, displaced `7.5%`, air pollution `5%`, drinking water `5%`, basic sanitation `5%`.
21. **Happiness Index (p.136):** Reported happy `22.5%`, daily enjoyment `22.5%`, life satisfaction `22.5%`, social support `22.5%`, suicide rate `10%`.

### A.2 Productivity & Culture Breakdown (pp.143, 155, 158, 169–181)
- **Productivity (`100%`):** `What You Pay vs. What You Get` (`70%`) + `Culture` (`30%`).
- **What You Pay vs. What You Get (`17.5%` each of Productivity):**
  - *Education:* Cost of quality-adjusted educated worker vs USA (wage cohort stand-in + PISA + attainment).
  - *Labor Productivity:* Cost of productivity-adjusted educated worker (wage cohort stand-in + output/worker).
  - *Working Hard:* Avg male weekly hours (`11.7/17.5 = 2/3`) + 10y forward dependency ratio change (`5.8/17.5 = 1/3`).
  - *Investing:* Investment `%` (`3.1`), Savings rate (`3.1`), Capital productivity (`3.1`), Capital stock growth (`3.1`), Income volatility (`2.6`), Inflation volatility (`2.6`) (normalized over `17.6`).
- **Culture (`16.7%` of Culture = `5%` of Productivity per sub-gauge):**
  - *Corruption:* Transparency Index (`50%`) + 3 WEF surveys (diversion, bribes, favoritism at `1/6` each).
  - *Bureaucracy:* Starting a business, construction permits, burden of govt regulation (`1/3` each).
  - *Rule of Law:* Efficiency of legal framework, property rights, protecting investors, enforcing contracts (`1/4` each).
  - *Savoring vs Achieving:* Avg hours worked (`50%`) + 7 WVS survey items (`0.5/7` each).
  - *Innovation:* 4 Inputs at `0.125` each (R&D spend, researchers, fear of failure, entrepreneurship); Outputs: patents `0.125`, royalty **payments** `0.125`, new businesses `0.0625`, `%` creating businesses `0.0625`, new major websites `0.0625`, trademarks `0.0625`.
  - *Self-Sufficiency:* Avg hours (`0.25`), LFPR (`1/12`), effective retirement age (`1/12`), vacation days (`1/12`), transfer payments (`0.125`), govt outlays (`0.125`), collective bargaining (`1/12`), hiring/firing (`1/12`), minimum wage (`1/12`).
- *Check figure:* Hours worked appears in `Working Hard` (`11.67%`), `Savoring` (`2.5%`), and `Self-Sufficiency` (`1.25%`) = `15.42%` (Appendix states `15.45%` rounded).

### A.3 The 64 Listed Data Sources (pp.183–184)
`1` Access to essential health services (SPI); `2` Aerospace/defense returns (TR); `3` Ag/forestry/fishing value added (WDI); `4` AI companies/patents/citations (CSET); `5` Banking claims (BIS); `6` Bloomberg Innovation Index; `7` Book titles/cap (CLIO); `8` Child mortality (SPI); `9` Climate vulnerability & preparedness (ND-GAIN); `10` CO2 emissions (CLIO); `11` Conflict intensity/count per cap (Cline Center); `12` Construction permits (DB); `13` Days lost to labor disputes (CLIO); `14` Media discontent/populism (GDELT); `15` Education spending (WDI); `16` Educational attainment (Barro-Lee); `17` Electricity production (CHAT); `18` Energy production/consumption (EIA); `19` Enforcing contracts (DB); `20` GINI (WDI); `21` Google searches conflict; `22` Google searches discontent; `23` Govt disapproval in news (GDELT); `24` Gross R&D expenditure (WIPO GII); `25` Aviation passengers/cargo (CHAT); `26` Historical tech adoption (CHAT); `27` Homicide rate (SPI); `28` Hours worked/week (WID); `29` Human Capital Index (WB); `30` Income distributions (WB); `31` Internal armed conflict (CLIO); `32` Internet users (CHAT); `33` Male LFPR (WB); `34` Metals/ores production (CLIO); `35` Nuclear stockpile (FAS); `36` New patents (WIPO GII); `37` Numeracy (CLIO); `38` OTC FX turnover (BIS); `39` Particulate matter pollution (SPI); `40` Patent applications (WDI); `41` Displaced by conflict (WDI); `42` Safely managed drinking water (WB); `43` Prevalence of undernourishment (WB); `44` Protecting investors (DB); `45` Protest intensity (GDELT); `46` Public sector corruption & theft (V-Dem); `47` Rail lines/cargo (CHAT); `48` Real GDP per worker (PWT); `49` Real growth rate (WB); `50` Renewable freshwater (WDI); `51` Rigorous & impartial govt (V-Dem); `52` Royalty & license fees (WIPO GII); `53` Top 500 universities share (ARWU); `54` Shipping tonnage (CHAT); `55` Starting a business (DB); `56` Suicide mortality (WB); `57` Tariff rate (WB); `58` Top 1%/10% income shares (WID); `59` Top 1%/10% wealth shares (WID); `60` Total labor disputes (CLIO); `61` Transparent laws/enforcement (V-Dem); `62` Universities founded (CLIO); `63` New businesses & % creating businesses (WIPO GII); `64` New major websites (WIPO GII).

### A.4 Appendix Country Code Mapping (A.10)
`BRZ -> BRA`, `IDR -> IDN`, `SAR -> SAU`, `SAF -> ZAF`, `UAE -> ARE`, `PLD -> POL`, `TLD -> THA`, `CHI -> CHL`, `CZK -> CZE`, `EUR -> None (Eurozone)`. All other codes (`USA, CHN, DEU, JPN, KOR, IND, GBR, FRA, RUS, SGP, AUS, TUR, CAN, CHE, NLD, ITA, ESP, MEX, ARG, COL, SWE, HUN, PRT, BEL, NOR, GRC`) are standard ISO3.

---

## Appendix B. Verified Replacement Leads & Probe Guidance

- **UNDP HDR (verified HTTP 200, ~2 MB):** `https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Composite_indices_complete_time_series.csv` (`encoding="latin-1"`). Columns: `iso3`, `mys_1990`..`mys_2023` (193 countries in 2023), `eys_1990`..`eys_2023` (195 countries in 2023).
- **UNCTAD LSCI (verified HTTP 200, ~206 KB):** `https://unctadstat-api.unctad.org/bulkdownload/US.LSCI/US_LSCI` (`application/octet-stream`; inspect zip/gzip/csv). Exclude landlocked countries via `INELIGIBLE`.
- **IMF SDMX 3.0 / Catalogue:** `https://api.imf.org/external/sdmx/2.1/dataflow/all` lists `MFS_IR` (Monetary & Financial Statistics, Interest Rate) and `IIP` / `IIPCC` (International Investment Position). Legacy `IFS` returns HTTP 403.
- **World Bank Specialized DBs:** DB `54` (Joint External Debt Hub), `22`/`23` (QEDS SDDS/GDDS), `20` (Quarterly Public Sector Debt), `12` (EdStats—`LO.PISA.*` ends 2018; OECD SDMX catalogue has no PISA flow).
- **WHR Panel (`F-2.4`):** WHR Figure 2.1 workbooks hold ladder scores and "Explained by" columns; probe the WHR "Data for Table 2.1" country-year panel spreadsheet on `worldhappiness.report` / `files.worldhappiness.report` for raw `Social support` and `Positive affect`.
- **Survey-cycle / Non-public:** `LPI` only exists in survey years (2007–2023; WDI ends 2022). `WVS`, `GEM`, `WIPO GII` (check annual downloadable tables first), `GDELT` (BigQuery/`HEAVY`), `GFCI` (PDF/`NO_PUBLIC_ROUTE`), `Bloomberg` / `VentureSource` / `WEF` / `Credit Ratings` (`PROPRIETARY`).

---

## Appendix C. Weight-Table Rows (`W01`–`W20`) to Add to `APPENDIX_SOURCES`

Add each `(id, indicator, source, page)` using the Appendix indicator name and the provider named in the Appendix (or substituted in the notebook), and cite `id` on its consuming tree component:

| ID | Appendix Indicator | Source | Page | Consuming Component (Code) |
| --- | --- | --- | --- | --- |
| `W01` | Life expectancy at birth | World Bank | 136 | `health_index/life_expectancy` (`SP.DYN.LE00.IN`) |
| `W02` | Basic sanitation access | World Bank | 136 | `health_index/basic_sanitation` (`SH.STA.BASS.ZS`) |
| `W03` | Reported life satisfaction (1–10) | World Happiness Report | 136 | `happiness_index/life_satisfaction` (`WHR.LIFE_LADDER`) |
| `W04` | Change in dependency ratio, 10y fwd | World Bank | 158 | `working_hard/dependency_ratio_10y_fwd_change` (`DERIVED.DEPENDENCY_RATIO_CHG_FWD10`) |
| `W05` | Investment % (non-residential fixed) | World Bank | 158 | `investing/investment_pct_gdp` (`NE.GDI.FTOT.ZS`) |
| `W06` | Income volatility | World Bank | 158 | `investing/income_volatility` (`DERIVED.GDP_GROWTH_VOL10`) |
| `W07` | Inflation volatility | World Bank | 158 | `investing/inflation_volatility` (`DERIVED.INFLATION_VOL10`) |
| `W08` | Productivity of capital investments | World Bank | 158 | `investing/capital_productivity` (`DERIVED.CAPITAL_PRODUCTIVITY`) |
| `W09` | Growth in capital stock | Penn World Table | 158 | `investing/capital_stock_growth` (`PWT.RKNA_GROWTH`) |
| `W10` | Transparency index | Transparency International | 171 | `corruption/transparency_index` (`TI.CPI`) |
| `W11` | WEF: diversion of public funds | V-Dem | 171 | `corruption/surveys_diversion_funds` (`VDEM.v2exthftps`) |
| `W12` | WEF: irregular payments and bribes | V-Dem | 171 | `corruption/surveys_irregular_payments` (`VDEM.v2excrptps`) |
| `W13` | WEF: favoritism in govt decisions | V-Dem | 171 | `corruption/surveys_favoritism` (`VDEM.v2clrspct`) |
| `W14` | WEF: burden of govt regulation | World Bank WGI | 173 | `bureaucracy/regulation_burden` (`RQ.EST`) |
| `W15` | WEF: legal framework efficiency | V-Dem | 175 | `rule_of_law/legal_frameworks_efficiency` (`VDEM.v2cltrnslw`) |
| `W16` | WEF: property rights | V-Dem | 175 | `rule_of_law/property_rights` (`VDEM.v2xcl_prpty`) |
| `W17` | Researchers | WDI | 179 | `innovation/inputs_researchers` (`SP.POP.SCIE.RD.P6`) |
| `W18` | New trademark creation | WDI | 179 | `innovation/outputs_trademarks` (`IP.TMK.RSCT`) |
| `W19` | Transfer payments to households | WDI | 181 | `self_sufficiency/gov_support_transfers` (`GC.XPN.TRFT.ZS`) |
| `W20` | Government outlays | WDI | 181 | `self_sufficiency/gov_support_outlays` (`GC.XPN.TOTL.GD.ZS`) |

---

## Appendix D. Published Country Tables (`Appendix.pdf`) for Validation (`V-2`–`V-5`)

*(Note: `PDF_NGDPDPC_35`, `PDF_WA_GROWTH_FWD10`, and `PDF_EMPIRE_SCORES` are already embedded in Cell 7b of the notebook.)*

### D.1 Headline Empire Scores Across Major Countries (p.135, as of July 2024)
Columns confirmed from p.135 layout: `(Total, Quality-only)` pairs except `Competitiveness, Culture, Investment` (Total only). Reserve Currency displays `---` for Euro-area states.

```
code  empT empQ | EduT EduQ | InnT InnQ | CmpT | MilT MilQ | TrdT TrdQ | EcoT EcoQ | FinT FinQ | ResT ResQ
USA   0.89 0.71 | 87%  96%  | 68%  70%  | 52%  | 86%  84%  | 73%  32%  | 49%  61%  | 99%  100% | 100% 83%
CHN   0.80 0.30 | 72%  56%  | 64%  54%  | 86%  | 63%  6%   | 100% 8%   | 45%  16%  | 44%  39%  | 5%   1%
EUR   0.56 0.43 | 59%  80%  | 36%  43%  | 46%  | 38%  25%  | 79%  37%  | 38%  45%  | 40%  34%  | 36%  33%
DEU   0.38 0.54 | 45%  82%  | 35%  60%  | 39%  | 20%  21%  | 51%  100% | 18%  52%  | 25%  36%  | ---  ---
JPN   0.33 0.40 | 48%  82%  | 34%  55%  | 43%  | 27%  11%  | 13%  29%  | 19%  40%  | 29%  49%  | 10%  23%
KOR   0.32 0.54 | 50%  89%  | 38%  70%  | 54%  | 33%  56%  | 7%   59%  | 6%   42%  | 15%  29%  | 0%   1%
IND   0.30 0.07 | 36%  5%   | 10%  16%  | 42%  | 51%  1%   | 6%   0%   | 20%  6%   | 6%   5%   | 0%   0%
GBR   0.29 0.46 | 48%  89%  | 26%  43%  | 32%  | 34%  26%  | 15%  58%  | 11%  44%  | 25%  39%  | 10%  41%
FRA   0.27 0.45 | 41%  81%  | 25%  47%  | 33%  | 33%  34%  | 14%  59%  | 11%  43%  | 25%  45%  | ---  ---
RUS   0.26 0.28 | 38%  61%  | 15%  24%  | 27%  | 53%  41%  | 0%   14%  | 14%  26%  | 0%   1%   | 0%   0%
SGP   0.24 0.89 | 35%  71%  | 26%  59%  | 61%  | 14%  92%  | 11%  100% | 1%   100% | 19%  39%  | 0%   7%
AUS   0.23 0.56 | 44%  89%  | 24%  51%  | 31%  | 22%  37%  | 0%   31%  | 3%   48%  | 17%  47%  | 3%   37%
TUR   0.21 0.28 | 20%  37%  | 17%  37%  | 49%  | 28%  16%  | 0%   12%  | 10%  29%  | 0%   0%   | 0%   0%
CAN   0.21 0.50 | 45%  89%  | 22%  45%  | 27%  | 16%  21%  | 4%   68%  | 4%   47%  | 18%  52%  | 4%   28%
CHE   0.19 0.66 | 40%  83%  | 32%  69%  | 45%  | 0%   18%  | 2%   100% | 1%   68%  | 18%  84%  | 0%   9%
BRZ   0.18 0.14 | 17%  25%  | 11%  25%  | 22%  | 27%  6%   | 0%   4%   | 10%  13%  | 1%   2%   | 0%   0%
NLD   0.17 0.55 | 37%  76%  | 29%  65%  | 39%  | 0%   25%  | 12%  100% | 2%   55%  | 19%  50%  | ---  ---
IDR   0.17 0.13 | 13%  13%  | 8%   20%  | 34%  | 25%  0%   | 0%   2%   | 8%   11%  | 0%   0%   | 0%   0%
ITA   0.17 0.31 | 32%  63%  | 15%  31%  | 26%  | 27%  28%  | 6%   48%  | 8%   40%  | 14%  28%  | ---  ---
ESP   0.17 0.34 | 33%  65%  | 11%  23%  | 39%  | 18%  19%  | 0%   46%  | 5%   38%  | 19%  31%  | ---  ---
SAR   0.15 0.45 | 19%  45%  | 11%  26%  | 33%  | 25%  100% | 0%   38%  | 4%   45%  | 0%   5%   | 0%   1%
MEX   0.14 0.15 | 24%  38%  | 5%   11%  | 28%  | 14%  2%   | 2%   17%  | 9%   19%  | 0%   1%   | 0%   0%
SAF   0.10 0.12 | 12%  30%  | 7%   17%  | 23%  | 11%  0%   | 0%   6%   | 3%   12%  | 0%   1%   | 0%   0%
ARG   0.07 0.14 | 12%  24%  | 6%   16%  | 1%   | 15%  2%   | 0%   6%   | 3%   20%  | 0%   0%   | 0%   0%
```

### D.2 Growth and Impact Vectors (`%/yr`, 35 Countries)
*(Pairs give `error_adjusted unadjusted` where two numbers appear.)*
- **p.146 Real GDP growth (10y):** `IND 6.3 8.1; UAE 5.5 5.7; IDR 5.5 6.8; SAR 4.6 5.4; TUR 4.0 4.0; CHN 4.0 3.7; PLD 2.9 2.5; SAF 2.9 4.5; RUS 2.9 3.3; COL 2.8 4.3; SGP 2.6 2.8; MEX 2.5 3.9; CHI 2.4 2.8; SWE 2.3 1.8; AUS 2.1 2.1; ARG 2.0 2.9; TLD 1.9 2.8; HUN 1.9 1.7; CZK 1.9 1.9; KOR 1.8 1.3; BRZ 1.7 2.6; USA 1.4 1.5; GBR 1.3 1.1; NLD 1.2 0.9; JPN 1.2 1.1; CAN 1.2 1.3; PRT 1.1 0.9; BEL 0.9 0.2; FRA 0.9 0.3; NOR 0.8 -0.2; ESP 0.3 -0.2; CHE 0.2 0.2; GRC 0.0 -0.1; DEU -0.5 -0.7; ITA -0.5 -1.1`
- **p.149 Growth per worker (10y):** `IND 5.3 7.1; IDR 4.8 6.1; UAE 4.8 5.0; CHN 4.3 4.0; TUR 3.6 3.7; PLD 3.5 3.1; RUS 3.1 3.6; SAR 3.0 3.8; KOR 3.0 2.5; SGP 2.8 3.1; TLD 2.7 3.5; HUN 2.4 2.2; COL 2.3 3.8; CHI 2.2 2.6; JPN 1.9 1.9; SWE 1.9 1.5; SAF 1.9 3.5; PRT 1.9 1.7; CZK 1.8 1.9; MEX 1.7 3.1; BRZ 1.6 2.5; AUS 1.5 1.5; NLD 1.4 1.0; USA 1.2 1.3; ARG 1.2 2.1; BEL 1.1 0.3; FRA 1.0 0.4; ESP 1.0 0.6; GBR 0.9 0.7; GRC 0.9 0.8; CAN 0.9 1.0; NOR 0.6 -0.4; ITA 0.6 -0.0; DEU 0.6 0.4; CHE 0.5 0.5`
- **p.150 Productivity impact:** `IND 6.1; IDR 5.0; CHN 5.0; TUR 4.5; UAE 4.2; SGP 3.7; KOR 3.6; SAR 3.3; RUS 3.2; TLD 3.0; COL 2.7; PLD 2.7; JPN 2.6; CHI 2.5; HUN 2.4; SWE 2.3; SAF 2.2; ARG 2.1; MEX 1.9; BRZ 1.6; USA 1.6; GBR 1.6; NLD 1.5; PRT 1.5; CZK 1.2; FRA 1.1; DEU 1.1; AUS 1.0; GRC 0.9; BEL 0.9; ESP 0.9; CAN 0.8; CHE 0.4; ITA 0.3; NOR 0.2`
- **p.151 Indebtedness impact:** `UAE 5.2 5.7; IDR 4.4 5.0; PLD 4.3 4.1; IND 3.8 4.0; CHN 3.1 1.4; RUS 2.7 3.2; PRT 2.6 2.6; AUS 2.5 2.4; CZK 2.1 2.7; TLD 2.0 2.0; TUR 2.0 2.1; HUN 1.9 1.5; SAR 1.8 3.7; KOR 1.8 0.9; CHI 1.6 1.7; COL 1.5 3.1; BEL 1.5 1.2; NOR 1.4 1.2; MEX 1.4 2.9; ESP 1.3 1.4; NLD 1.3 1.2; SWE 1.3 1.0; SGP 1.2 1.2; BRZ 1.1 2.2; ITA 1.1 1.3; SAF 1.0 3.1; CAN 1.0 1.3; CHE 1.0 1.0; FRA 0.9 1.0; JPN 0.7 0.5; GRC 0.7 1.0; USA 0.4 0.5; DEU -0.3 0.0; GBR -0.4 -0.3; ARG -0.6 1.0`
- **p.159 What-you-pay-vs-what-you-get impact:** `IND 7.2; IDR 5.6; TUR 4.5; COL 4.4; CHN 4.3; RUS 4.0; SAF 4.0; TLD 3.7; BRZ 3.7; MEX 3.7; ARG 3.4; SAR 3.2; HUN 3.1; UAE 3.1; PLD 3.1; CHI 3.0; SGP 2.3; KOR 2.3; CZK 2.3; JPN 2.2; GRC 2.0; PRT 1.9; SWE 1.3; USA 1.2; AUS 1.2; ESP 1.2; CAN 1.1; GBR 1.1; ITA 1.0; FRA 0.8; BEL 0.7; NLD 0.5; DEU 0.5; CHE -0.4; NOR -0.9`
- **p.170 Culture impact:** `SGP 1.7; IND 1.6; UAE 1.5; CHN 1.1; IDR 1.0; KOR 1.0; TLD 0.6; SAR 0.6; CHE 0.5; USA 0.5; JPN 0.4; NLD 0.4; SWE 0.4; GBR 0.2; CHI 0.1; TUR 0.1; DEU 0.0; AUS -0.2; RUS -0.2; CAN -0.2; SAF -0.3; COL -0.3; NOR -0.3; MEX -0.4; PLD -0.5; HUN -0.5; FRA -0.6; PRT -0.7; ARG -0.8; BEL -0.8; CZK -0.9; BRZ -1.0; ESP -1.1; GRC -1.3; ITA -1.7`

### D.3 Educated Worker Cost vs USA (`%`, `USA = 0`; lower cost = higher competitiveness)
- **p.164 left (Quality-adjusted, 34 countries):** `IND -92; IDR -85; CHN -83; COL -83; RUS -82; MEX -75; BRZ -73; ARG -71; CHI -70; TLD -69; TUR -67; PLD -66; KOR -65; HUN -63; UAE -58; SGP -53; JPN -52; SAR -45; CZK -42; GRC -37; CAN -17; PRT -13; GBR -12; ESP -6; USA 0; AUS 4; ITA 4; SWE 13; DEU 33; BEL 38; FRA 59; NLD 64; CHE 92; NOR 106`
- **p.164 right (Non-adjusted, 35 countries):** `IND -95; IDR -89; COL -87; SAF -85; RUS -83; CHN -82; MEX -81; BRZ -79; TLD -75; CHI -71; ARG -71; TUR -69; PLD -65; UAE -65; HUN -63; KOR -61; SAR -59; GRC -45; JPN -43; SGP -43; CZK -39; PRT -15; CAN -13; GBR -9; ESP -8; USA 0; ITA 1; AUS 8; SWE 13; DEU 31; BEL 39; FRA 55; NLD 60; CHE 94; NOR 106`
- **p.165 (Productivity-adjusted, 35 countries):** `TUR -97; IND -95; IDR -90; COL -89; RUS -87; SAF -87; HUN -87; BRZ -84; SAR -82; PLD -81; MEX -80; ARG -80; UAE -79; TLD -78; CHI -78; CHN -78; SGP -66; KOR -65; CZK -61; GRC -60; JPN -52; PRT -41; ESP -38; ITA -26; CAN -22; SWE -21; DEU -18; GBR -17; BEL -5; FRA -1; USA 0; AUS 0; NLD 2; NOR 20; CHE 81`

### D.4 World-Relative Culture Sub-Gauge Orderings (pp.172–182, highest score first)
- **p.172 Corruption:** `SGP CHE NLD NOR SWE UAE DEU AUS GBR JPN CAN BEL USA FRA CHI SAR PRT KOR CHN ESP PLD IND CZK ITA GRC IDR TUR TLD HUN SAF RUS ARG COL BRZ MEX`
- **p.174 Bureaucracy:** `SGP UAE SAR GBR USA CHN CHE NLD NOR DEU JPN CAN AUS SWE TLD KOR IND FRA TUR RUS CHI IDR BEL PRT PLD GRC ESP HUN MEX SAF COL ITA CZK ARG BRZ`
- **p.176 Rule of Law:** `SGP UAE NOR SAR GBR USA AUS JPN CHE SWE KOR CAN NLD FRA DEU CHN BEL TLD ESP CHI SAF TUR IDR PRT IND CZK RUS HUN PLD MEX BRZ ITA COL ARG GRC`
- **p.178 Savoring vs Achieving:** `UAE IND SAR SGP IDR CHN MEX KOR USA RUS PLD TLD TUR JPN SAF COL ARG HUN AUS SWE CAN CHI CZK DEU ESP BRZ GBR NOR NLD PRT CHE ITA GRC FRA BEL`
- **p.180 Innovation:** `KOR CHE SWE NLD USA SGP JPN DEU GBR FRA BEL NOR AUS CAN HUN CZK UAE PRT TUR GRC ITA CHI ESP CHN TLD BRZ PLD ARG IDR SAR RUS COL IND SAF MEX`
- **p.182 Self-Sufficiency:** `UAE SGP IDR SAR MEX IND CHN TLD KOR USA RUS ARG CHE COL CHI TUR JPN HUN CAN CZK SAF PLD AUS GBR NLD BRZ NOR PRT SWE DEU GRC ESP BEL FRA ITA`

---

## Appendix E. Screen Baseline & Expected Diagnostics

At baseline (`190` retained series): median `weighted_cov_% = 95.1` (fence `~78.6`) and median `weighted_recent_% = 94.6` (fence `~6.925`).
- **Expected `DISCONTINUED` drops (`latest < 2023`):** Doing Business (`IC.*`, `PROT.*`, `ENF.*`), `BAR.SCHL.25UP` (replaced by `HDR.MYS` if wired), `HD.HCI.*`, `LO.PISA.*` / `DERIVED.PISA_MEAN`, `LP.LPI.OVRL.XQ`, `IS.SHP.GCNW.XQ` (replaced by `UNCTAD.LSCI` if wired), `IS.RRS.*`, `MS.MIL.TOTL.P1` & per-capita twin, `NY.GDP.TOTL.RT.ZS`, `ILO.COLLECTIVE_BARGAINING_COVERAGE`.
- **Expected `LOW_COVERAGE` drops (below `fence_cov` or lacking a rankable year `>= 2023`):** `DT.DOD.DECT.GN.ZS`, `DT.TDS.DECT.EX.ZS`, `FR.INR.LEND`, `FR.INR.RINR` (19.2% recent, <30 reporters in 2023+), `ILO.HOURS_WEEKLY_*` (falls back to `DERIVED.PWT_HOURS_WEEKLY`), `ILO.STRIKES_*` (never hits 30 reporters in any year), `OECD.HH_SAVINGS_RATE` (falls back to `IMF.NGSD_NGDP`), `OECD.MIN_WAGE_RATIO`.
- **Expected Keeps via `INELIGIBLE`:** `IMF.COFER_SHARE`, `BIS.FX_TURNOVER_SHARE`, `BIS.INTL_DEBT_CCY_SHARE` (69.4% raw coverage, passes once `EURO_AREA` is excluded).
- **Watch items for `rankable_recent` (`n_reporting >= 30` in `2023+`):** Verify actual `rankable_recent` outcomes for `BIS.CBPOL.POLICY_RATE_*` (48 countries ever, 57.9% recent), `BIS.LBS.CROSSBORDER_CLAIMS` (48 countries), and `BIS.FX_TURNOVER_VALUE` (56 countries), and report any differences vs. this baseline.

---

## Definition of Done

1. Clean kernel restart & top-to-bottom run succeeds with zero uncaught errors (`df_probes` and Cell `6b` removed; `ALLOW_PROXY_FALLBACK` and `PROBE_NEXT` removed).
2. All `F-2` repairs and `F-6` bounded probes are executed and logged; every missing component carries an exact `GAP_REASONS` code (with evidence in `note`) or a `DISCONTINUED`/`LOW_COVERAGE` drop reason.
3. `APPENDIX_SOURCES` includes 64 A.7 items + concept rows + `W01`–`W20`, and all 15 `APPENDIX_WEIGHTS` locks pass print-only checks in Cell `7b`.
4. `df_cov`, `S-2` screen output (with Appendix E diff), and `df_resolution` (with dropped nominal weights & effective `size_share`) are displayed; the simplified engine applies internal linear interpolation (`limit_area="inside"`), writes the 3 parquet files, and displays updated `6c` and `7a` tables.
5. Cell `7b` and Phase 2 validation (`V-2`–`V-5`, including `Dalio-35` pass and `Competitiveness + Culture + Investment` sensitivity) print all comparisons cleanly without raising.
6. Final summary report lists before/after counts, fetch outcomes, screen fences & diffs, dropped share per gauge, and all Phase 2 Spearman correlations.