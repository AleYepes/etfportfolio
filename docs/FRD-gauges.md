# FRD: Dalio Gauge Emulation MVP — Source Fidelity and Minimal Gauge Engine

## 1. Purpose, scope and how to use this document

**Purpose.** One Jupyter notebook ingests macro, governance, health and social indicators for every ISO3 country and groups them into “gauge trees” that emulate the gauges in the Appendix of The Great Powers Index 2024. This MVP exists to explore and define a bespoke gauge tree. It will later be re-implemented in a production pipeline that you cannot see, so keep it quick, lean and simple.

**Primary goal: three audit checks.**

1. **Completeness.** Every source or indicator the Appendix cites is fetched programmatically, or appears in a ledger as an explicit GAP with a reason. “Cited” means the 64-item source list (§13) plus every component named in the gauge descriptions and tables (§11).
2. **Nothing uncited.** No series is fetched or wired into the tree unless it maps to a cited item or is an explicitly registered stand-in (D18).
3. **Weights.** Gauge weights in the tree match the Appendix. Where the Appendix is silent, weights are kept as drafted and tagged as heuristics (D19).

**Secondary goal.** A minimal engine (§6) that turns the tree plus the data into annual gauge series, so the three checks can be verified end to end.

**Out of scope (the owner’s later sessions; do not start them).** Vetting individual sources; dropping stale or sparse series; finding replacement indicators; imputation or interpolation; recalibrating weights; income-adjusted or log transforms (“relative to similar income”, §11.4); the growth back-test and error adjustment; integration into the production pipeline; writing to any database; any step that needs a hand-downloaded file.

**Working principles.** (1) Correctness over coverage: never silently mix methodologies. (2) Flags over deletions: this phase labels stale, sparse or proxy data and never drops it. (3) Programmatic only. (4) MVP lean: one notebook, no package, no tests framework (use inline `assert` cells).

**Reader assumptions.** You have the notebook source (cells 0–17, summarised in §9.1) and a network connection. Priorities used in this document: **P0** correctness bug or blocker; **P1** needed for gauge fidelity; **P2** cleanup or nice-to-have.

## 2. Decision register

Every decision below was settled with the owner. The rationale is part of the requirement: when something is ambiguous, choose what serves the rationale.

| ID | Decision | Why |
| --- | --- | --- |
| D1 | Single notebook; all logic in cells; no package, no modules, no test suite (inline asserts instead). | MVP to explore a bespoke tree; the production pipeline will re-implement it later. |
| D2 | Build a minimal scoring engine (§6). Keep cell 16’s `df_health` table and extend it into the series registry. Replace `leaf_coverage` with per-alternative coverage (§7). | Cell 16 is a data-health diagnostic, not an engine (no weighted gauge exists yet). `leaf_coverage` counts components equally, ignores item weights and `@` references, hides dead alternatives behind live ones, and applies a 2019 cutoff that contradicts D7. |
| D3 | **Programmatic ingestion only.** Never rely on a manual download. A source with no programmatic route becomes a GAP row in the ledger with the reason stated. | The owner wants a reproducible pipeline and will vet sources in a later session. |
| D4 | Skip fragile or scrape-only sources entirely: Google Trends, Global Firepower, CSET. List them in the ledger as `skipped`. | Fragility outweighs value for an MVP. |
| D5 | Cache each source’s fetch as a parquet snapshot in `data/dalitest/raw/<source>/`. A per-source `REFRESH` flag forces refetch. A failed refresh keeps the previous snapshot and prints WARNING. No run history, no merging of runs: a refresh overwrites. | Thousands of HTTP calls per run (EIA 3,094; ILO 2,180; OECD rate-limited to about 60 per hour) make iteration impractical without caching. |
| D6 | Do not read from or write to any database. `data/etf.duckdb` is no longer used (the old universe came from it). Outputs are files under `data/dalitest/out/`. | The MVP is self-contained. |
| D7 | **Annual gauge series, full history.** Fetch every indicator over its full available history (no year windows) and compute gauges for every year from the floor year (1950) to the last actual year. Projections feed only forward-looking derived series (§9.3). | The owner wants series reaching as far back as data allows and, above all, to the present. |
| D8 | A series is **stale** if its latest non-projection year is before 2023 (constant `STALE_BEFORE`). Flag only; do not drop. | Data lags a few years; older endings make a series useless for present-day gauges, but dropping is the owner’s vetting decision. |
| D9 | Universe = every ISO3 code in country\_converter’s data (ISO 3166-1), fetched everywhere. Providers’ aggregates are dropped. | The production pipeline will do the same; a complete country set allows a faithful world picture. |
| D10 | Percentile ranks are computed over every ISO3 that has data in that (indicator, year). No curated rank set. | Owner choice: try ranking over everything and inspect. |
| D11 | No aggregates: no `EMU`, no `WLD`, no special World Bank codes; no world total is built unless something consumes it. | Percentile ranks do not need a world denominator (a common denominator does not change ranks). |
| D12 | Reserve currency: IMF COFER euro share is assigned to no entity, so euro-area members have no reserve-currency rows (null, never zero). CHF share goes to CHE and LIE. Other currencies unchanged. Non-issuers keep the structural 0. | The PDF scores only the Eurozone entity and shows dashes for members; with no `EMU` entity, members are simply unscored. |
| D13 | **Standardisation = percentile rank, for every gauge.** Apply direction first, ties get the average rank, scale 0–100, ranked within each (indicator, year) across countries with data. One function with a mode switch so z-score can be swapped in later. | Consistency and simplicity; matches the PDF for strength gauges. The PDF’s −4 to +4 scale belongs to the later income-adjusted layer. |
| D14 | Minimum cross-section: a rank is valid only if at least `MIN_CROSS_SECTION = 30` countries have a value in that (indicator, year); otherwise NaN. Global hard-coded constant, logged. | Percentiles among 12 countries in 1962 are not comparable with percentiles among 150 in 2020. |
| D15 | **No gap filling**: exact-year observations only, no forward-fill, interpolation or imputation. Keep an `observed` mask for the owner’s later imputation step. | Raw responses must be vetted first; jaggedness is itself a diagnostic. |
| D16 | **Coalescing rule.** Within a component, alternatives are ranked separately, then per country-year the first alternative in priority order with a valid rank supplies the value. The first alternative is always eligible. Later alternatives are eligible only if their tier is `exact` or `substitute_source`; `proxy_concept` fallbacks are excluded unless `ALLOW_PROXY_FALLBACK = True` (default False). Record `alt_used` and report switch counts and an agreement report (§7). | Different sources can measure materially different things; the owner’s priority is a consistent indicator over time and across countries. The switch lets the owner see what proxies would buy. |
| D17 | Three tiers per alternative: `exact` (the measure, from the source the PDF names), `substitute_source` (same measure from a different provider), `proxy_concept` (a different concept standing in). Tag every alternative and report effective weight by tier. | Makes fidelity measurable and vetting easy. |
| D18 | **Reverse-trace rule.** Every series that is fetched and wired must map to a cited item (tier exact or substitute\_source) or be a registered stand-in (named concept it replaces, tier proxy\_concept). Everything else is deleted from the tree and trimmed from the fetch. The KEEP set is computed programmatically (§4.6). ND-GAIN’s unused series stay with status `pdf_listed_unwired` (PDF #9 says “& Subcomponents”). | Directly serves audit check 2 while keeping deliberate stand-ins visible. |
| D19 | Weights not given by the PDF stay as the author drafted them but every node carries a provenance tag: `pdf`, `pdf_implied`, `heuristic`. Composite gauges are still computed. | Equal weights would be as invented as the drafts; composites let the owner calibrate later against the PDF’s published scores (§14.1). |
| D20 | **Missing data: renormalise bottom-up at every node** over children that have data. Every node score carries `weight_present` (share of nominal weight backed by data). Structural-GAP weight (components with no fetched alternative) is reported separately. No minimum-coverage threshold in this phase. | The engine always yields a score for inspection and makes the distortion visible. |
| D21 | Direction (+1/−1) is a field on each alternative. The implementer drafts the map from the Appendix’s wording (§6.5); ambiguous items default to +1 and are listed for owner review. | Percentile ranks are meaningless without direction; income adjustment stays out of scope. |
| D22 | Delivery in vertical slices (§8): shared plumbing first, then one gauge at a time. | Health is fully specified and checkable; it proves the engine on the simplest gauge. |
| D23 | External conflict: placeholders only (§12.7); the target shape is recorded as a country-level exposure score versus the USA and CHN. | The PDF gauge is pairwise between major powers; no free country-level source is defined. |
| D24 | Done = hard gates in §7.3. Appendix B comparisons beyond two data sanity checks are printed, not gated. | Rank correlations will move after vetting and recalibration, so they must not drive MVP choices. |
| D25 | **Parked ideas (do not build).** Always including both raw and per-capita versions of every indicator when constructing gauges; z-score standardisation; minimum-coverage thresholds; imputation. | The owner wants accurate source replication first. |

## 3. Conventions and data model

- **Codes** are `PROVIDER.CODE` exactly as stored in the master table (for example `WB` indicators keep their native WDI ids such as `SP.DYN.LE00.IN`, V-Dem is `VDEM.<var>`, derived series are `DERIVED.<name>`).
- **Master schema** (long): `country_code` (ISO3), `indicator_code`, `year` (int), `value` (float), `source`, `is_projection` (bool). Duplicate (country, code, year): the first frame in the single priority-ordered list wins.
- **Weights** are fractions of the stated parent node. Productivity-gauge weights in §11 are percentages of the productivity gauge; multiply by 0.65 for the share of the growth-per-worker estimate. Where the PDF prints “culture-basis” numbers (whole culture = 100, for example corruption = 16.7), multiply by 0.30 to reach productivity-gauge basis.
- **An alt** is an ordered tuple of alternatives for one component. Notation in this document: a plain code is tier `exact`; `[S]` = `substitute_source`; `[P]` = `proxy_concept`; `(GAP)` = no alternative is fetchable yet (kept in the tree as a placeholder with its reason).
- **PDF entity codes** differ from ISO3: BRZ=BRA, IDR=IDN, SAR=SAU, SAF=ZAF, UAE=ARE, PLD=POL, TLD=THA, CHI=CHL, CZK=CZE, EUR=EMU (the Eurozone has no entity here, D11).
- **Headline sets** (for validation only; never for ranking): “the 24” = USA, CHN, EUR, DEU, JPN, KOR, IND, GBR, FRA, RUS, SGP, AUS, TUR, CAN, CHE, BRA, NLD, IDN, ITA, ESP, SAU, MEX, ZAF, ARG (23 usable, no EUR). “The 35” = the 23 plus ARE, POL, COL, THA, CHL, SWE, HUN, CZE, PRT, BEL, NOR, GRC. Constants `DALIO_24` and `DALIO_35` exist in the notebook; remove `EMU` from them.
- **Constants block** (cell 0): `YEAR_FLOOR = 1950`, `STALE_BEFORE = 2023`, `MIN_CROSS_SECTION = 30`, `ALLOW_PROXY_FALLBACK = False`, `PROJ_FROM = 2026` (years from which sources that mix actuals and projections are forward-looking), `REFRESH = set()` (sources to refetch), `EIA_API_KEY` read from the environment.

## 4. Notebook architecture and shared plumbing (“Step 0”)

### 4.1 Layout

```
data/dalitest/raw/<source>/   parquet snapshots (D5)
data/dalitest/out/            reports and outputs (§7)
```

Notebook cell order: (0) constants and helpers; (1) universe; (2…) one fetch cell per source, each wrapped by the cache helper; (3) derived series; (4) master assembly; (5) the tree, including direction, tier and provenance; (6) engine; (7) QA reports and gates.

### 4.2 Requirements

**P-1 Universe (P0).** `UNIVERSE = sorted(ISO3 codes from coco.CountryConverter().data, excluding not-found)`. No database read. Every fetcher maps provider codes to ISO3 and drops rows outside `UNIVERSE` (this removes provider aggregates). Countries a provider does not cover simply have no rows.

**P-2 Fetch contract (P0).** Each source is a function returning a master-schema frame, run through `cached(source, fn, refresh=source in REFRESH)`. After every fetch print row count, country count and series count against the expected series count, and print `WARNING` when rows = 0 or fewer series than expected arrive. Reset `FETCH_LOG` at the top of the notebook.

**P-3 Full history (P0).** Remove every hard-coded fetch window (WDI 2018–2025, WGI 2015–2025, PWT ≥1995, the 2015–2040 population window; see P-4 for projections). The separate 1990+ “long history” pull (cell 1c) is merged into the main WDI pull.

**P-4 Efficient routes (P1).** Prefer one multi-country or bulk request over per-country loops: World Bank by indicator with all economies; IMF SDMX with multi-country keys; UNHCR and PIP with all countries; ILOSTAT bulk files as the primary route; OWID one download per chart; EIA fixed product list (44 total primary energy; 2 electricity) instead of probing; Doing Business (db 1) through the raw per-series route directly. Population projections (db 40): fetch years 2026–2040 only, as `PROJ.*` with `is_projection = True` (history comes from WDI).

**P-5 Single assembly (P1).** One `all_dfs` list, in priority order. Delete the first (dead) `all_dfs` definition.

**P-6 Retention (P1).** After the tree is final, compute `KEEP` = codes in any tree alternative ∪ inputs of derived series ∪ codes named in the Appendix source list (§13) ∪ registered stand-ins. Restrict each fetch to `KEEP` where the API allows it, otherwise filter after download. Series outside `KEEP` must not remain in the master. Exception: ND-GAIN keeps all series (status `pdf_listed_unwired`).

**P-7 Secrets (P0).** The EIA API key is hard-coded in cell 4 and has been shared in plain text. Read it from the environment variable `EIA_API_KEY`, fail with a clear message if absent, and tell the owner to rotate the key.

**P-8 Loud failures (P1).** See P-2. Also: any alternative in the tree whose code is missing from the master and not marked GAP is an error in the QA output (§7.1).

## 5. Tree model

One declarative definition in one cell; **no patch layer** (delete `extend_alt` and the 14c patches), **no wildcards** (`*`), no `.PER.RNK` codes.

- A **node** has children and a weight. A **leaf** holds **components**. Each component has an optional weight (default: equal share) and an ordered list of **alternatives**. Each alternative carries `code`, `tier` (`exact`, `substitute_source`, `proxy_concept`), `direction` (+1 or −1), and optionally `status='GAP'` with a `reason`.
- Every node and leaf carries `provenance` ∈ {`pdf`, `pdf_implied`, `heuristic`}.
- A component may be a **reference** string `@path` to another node (dotted path, for example `@productivity_gauge.what_you_pay_vs_what_you_get`). The engine resolves references before evaluation. Every `@` string must resolve.
- Strength determinants are `{total: leaf, per_capita: leaf}` pairs with 50/50 halves, except those marked “total only”.
- Shared alternatives may be defined as constants (for example `HOURS_WORKED_WEEKLY`, `GDP_PC_COST`, `PISA_MEAN`) and reused by name.
- Helper utilities required: `iter_leaves` (yields path, components, effective weight), `check_weights` (every dict-node’s children sum to 1 within 1e-3; the known exceptions are the productivity “investing” items, which sum to 1.0057 because the PDF rounds, and the strength key-eight, which sums to 0.94), `code_leaves` (code → leaf paths).
- The full target tree is in §12; where §12 and any other section differ, §12 wins.

## 6. Engine contract

The engine takes the master table and the tree and produces annual scores for every node. Years run from `YEAR_FLOOR` to the last actual year (the latest non-projection year found in the wired series).

### 6.1 Steps

1. **Wide matrices.** For every code in the tree build country × year raw values from non-projection rows. (Forward-looking derived series are dated at their base year t, so they are non-projection rows.)
2. **Direction and rank.** For each (code, year): if the number of countries with a value is below `MIN_CROSS_SECTION`, all ranks are NaN. Otherwise multiply by the alternative’s direction (−1 reverses) and compute `rank_pct = (average_rank − 1) / (n − 1) × 100` (range 0–100, ties averaged). Record `n_ranked`.
3. **Coalesce** per component and country-year per D16. Record `alt_used` (index of the alternative) and an `observed` mask (True where the component has a valid value). The first alternative is always eligible regardless of tier.
4. **Leaf score** = weighted mean of its component scores over components with a value in that country-year (weights renormalised). `weight_present(leaf)` = sum of weights of present components ÷ sum of all component weights. GAP components count as absent.
5. **Node score** = weighted mean of child scores over children with a value; `weight_present(node)` = Σ child weight × child `weight_present` ÷ Σ child weights. Absent children have `weight_present` 0.
6. **References** are resolved before step 5 (a reference node’s score is used as a component or child).
7. **Static report.** `gap_weight(node)` = nominal weight under components that are GAP (no data in any country-year), computed from the tree alone.

### 6.2 Outputs (parquet under `data/dalitest/out/`)

- `component_scores`: country, year, leaf\_path, component\_id, score, alt\_used, observed.
- `node_scores`: country, year, node\_path, score, weight\_present.
- `rank_support`: code, year, n\_ranked.

### 6.3 Not in the engine

No imputation, no re-ranking of composites, no income adjustment, no log transform, no back-test, no error adjustment.

### 6.4 Future-growth node (MVP form)

The PDF’s growth identity is additive in percentage points (working-age growth + growth per worker + error). Converting gauge scores into percentage-point impacts needs the later calibration. In this MVP the node `future_growth` is the rank-scale composite `0.65 × productivity_gauge + 0.35 × indebtedness_gauge` (growth-per-worker score). `DERIVED.WORKING_AGE_GROWTH_FWD10` is carried as a separate series in percentage points for the calibration phase and is validated against the PDF (§14.2). `error_adj` is a documented TODO.

### 6.5 Direction map

The sign means: for strength gauges, more of the quantity means more power; for health and happiness, the PDF’s stated implied direction (§11.3); for productivity and indebtedness items, a higher score means a higher expected 10-year growth (more hours, higher investment, lower corruption, lower debt, lower volatility, higher quality, lower cost). Print the full map as a table (`direction_map.csv`) with a column `ambiguous`. Items whose sign you cannot defend from the PDF text default to +1 and are flagged `ambiguous = True` for the owner (known candidates: inflation and policy-rate legs of monetary policy, “easier policy predicts faster growth”; employment-protection and minimum-wage rigidity; transfers and government outlays in self-sufficiency; GDP per capita as a cost proxy, which is −1 in the cost leaves and +1 in strength gauges).

## 7. QA reports and gates

All reports are written to `data/dalitest/out/` as CSV and displayed in the notebook. Flags only: nothing is dropped.

### 7.1 Reports

- **`df_health` (keep and extend; the owner finds it useful).** Existing columns (gauge, indicator, source, reporting countries, coverage, recency, latest year, degenerate flag, projection flag) plus: `tier`, `stale` (latest year < `STALE_BEFORE`), `first_valid_year` (first year with ≥ `MIN_CROSS_SECTION` countries), `pdf_ids`, `status`. Replace `RECENT_FROM = 2023` by `STALE_BEFORE`. Replace the ETF-observation weights behind `weighted_cov_%` by weights equal to each country’s latest PPP GDP share (`IMF.PPPGDP`, falling back to `NY.GDP.MKTP.PP.KD`).
- **Series registry `df_registry` (FR-51).** One row per code in the master: source, n\_countries, first\_year, last\_year, in\_tree (with leaf paths), in\_derived, pdf\_ids, status ∈ {`wired`, `pdf_listed_unwired`, `stand_in`, `no_consumer`}. After P-6 `no_consumer` must be empty.
- **Per-alternative coverage `alt_coverage` (replaces `leaf_coverage`).** One row per (leaf, component, alternative): countries covered, latest year, row count, tier, flags `dead` (0 countries and not GAP), `stale`, `proxy`, `gap`. Also the coalesced-union coverage per component.
- **PDF ledger `pdf_ledger.csv`.** One row per item in §13 plus the non-list inputs of §11 (WVS items, GEM, rating, and so on) with columns: item, PDF source, our codes, tier, n\_countries, status ∈ {`live`, `substitute`, `proxy`, `gap`, `skipped`}, reason (mandatory for gap/skipped). Dead alternatives must not hide behind live ones (items #20, #30, #58).
- **Effective-weight report `effective_weights.csv`.** Per gauge and code: effective weight after node weights, component weights and renormalisation; plus per gauge the share of nominal weight by tier of each component’s first alternative (`exact`, `substitute_source`, `proxy_concept`, `gap`) and by provenance.
- **Switch report.** Per leaf and component: number of country-years supplied by each alternative, and how many countries switch alternative at least once.
- **Agreement report.** For each pair of alternatives within a component: overlapping country-years, Spearman rank correlation, median ratio of raw levels. This lets the owner judge empirically whether alternatives measure the same thing.
- **Proxy candidates report.** For each `proxy_concept` fallback excluded by D16: how many country-years it would add if `ALLOW_PROXY_FALLBACK` were True.
- **`direction_map.csv`** (§6.5).

### 7.2 Asserted structure checks

Inline `assert` cells (the unit tests of this MVP): weights of every node sum to 1 (known exceptions listed in §5); two alternatives on different scales land on the same percentile scale after coalescing (small synthetic test); renormalisation test (a leaf with one missing component of weight 0.25 re-weights the rest to sum 1 and reports `weight_present` 0.75); no code appears in two leaves of the Health or Happiness gauge; no code appears in both halves of one strength determinant; every `@` reference resolves.

### 7.3 Hard gates (definition of done)

1. **Ledger gate.** Every item in §13 and §11 has a status; every `gap` or `skipped` row has a reason.
2. **Registry gate.** `no_consumer` is empty; every fetched series maps to a leaf, a derived input, a Appendix-listed item or a registered stand-in; no dead alternative outside the GAP set.
3. **Weight gate.** Hours-worked effective weight in the productivity gauge is 15.45% ± 0.1% (all components counted as present; see §12.3 and Appendix C of this document); health weights sum to 1 (nine items); happiness weights sum to 1 (five items); growth split 0.65/0.35; productivity top split 0.70/0.30; no code in two leaves of one gauge.
4. **Data sanity gates (two only).** (a) `IMF.NGDPDPC` for the 35 versus §14.3: median absolute relative deviation ≤ 10% and none above 30% (tolerances are drafting values; adjust only with owner agreement). (b) `DERIVED.WORKING_AGE_GROWTH_FWD10` at base year 2024 for the 35 versus §14.2: at least 90% within 0.3 percentage points; list outliers.
5. **Printed, not gated:** rank correlation of the engine’s strength-gauge outputs versus §14.1 for the 23 usable entities, and the qualitative checks in §14.4.

## 8. Delivery order (vertical slices)

This replaces the phase plan of FRD v1.0. Work in this order; each slice ends with its gates (§7.3) for the gauge in question. Do not start vetting, pruning or recalibration.

| Step | Content | Exit |
| --- | --- | --- |
| 0. Plumbing | P-1 to P-8; constants; universe; cached fetch contract; single assembly; empty tree skeleton; engine skeleton with the §7.2 asserts; registry and ledger skeletons; direction-map table; EIA key from environment. | Notebook runs end to end on cached data; every fetch prints a loud summary. |
| 1. Health | Fix or confirm every Health input (§12.1); engine run; direction table. | Weights sum to 1 (nine items); no code reused; printed check: Japan and Australia rank among the highest and South Africa and India among the lowest of the 23 headline entities (§14.4). |
| 2. Happiness | WHR panel (FD-04), WVS happy item (§10), suicide rate, Cantril substitute. | `WHR.LIFE_LADDER`, `WHR.SOCIAL_SUPPORT`, `WHR.POSITIVE_AFFECT` each have data for ≥ 80 countries in the last three years, or the ledger documents a GAP with its reason. No code appears in two leaves. |
| 3. Productivity, Indebtedness, future growth | All productivity feeds and fixes (§9, §10), weighted leaves, hours reweighting, derived series (§9.3), indebtedness leaves, `future_growth` node (§6.4). | Hours effective weight 15.45% ± 0.1%; growth split and 70/30 split asserted; §7.3 gates 4(a) and 4(b) computed. |
| 4. Strength determinants | Reserve currency (COFER fix), financial center (BIS, GFCI probe), innovation (WIPO GII), education (ARWU), military, trade, economic output, competitiveness reference; `DERIVED.PER_CAPITA` and `DERIVED.ABS` builders. | No code in both halves of one determinant; each per-capita component is a ratio, rate or index. |
| 5. Remaining gauges and cycles | Additional measures (infrastructure, resource allocation, geology, character, governance, acts of nature), internal order cycle (wealth gap, internal conflict), economic-financial cycle, external order cycle placeholders. | Every `@` reference resolves; registry gate passes. |
| 6. Close-out | Run P-6 (compute `KEEP`, trim fetches), regenerate caches, run all reports and every gate in §7.3. | `no_consumer` empty; ledger complete; all gates pass. |

## 9. Existing notebook: inventory, defects and required fixes

### 9.1 Cell inventory (as audited)

| Cell | Purpose |
| --- | --- |
| 0 | Helpers (`tidy`, `http_get`, `to_iso3`, `slug`, `log_fetch`, `FETCH_LOG`), constants; builds `target_countries` (109) from `data/etf.duckdb` (to be replaced by P-1). |
| 1a, 1b, 1c | World Bank: WDI (db 2, 185 codes, per-economy loop; adds `EMU` and `WLD`, to be removed); other databases (WGI db 3, Doing Business db 1, UHC db 58, HCI db 63, population projections db 40); long-history pull. |
| 2 | EdStats (db 12): five Barro-Lee and three PISA series. |
| 3, 3b | IMF WEO via SDMX 3.0 (22 series, prefix `IMF.`, years ≥ 2026 projections); IMF COFER currency shares. |
| 4 | US EIA international (13 products per country). |
| 5, 6 | V-Dem v16 (24 series, prefix `VDEM.`); ND-GAIN 2026 (177 series, prefix `NDGAIN.`). |
| 7 | Our World in Data: 16 charts. |
| 8 | ILOSTAT: 4 modelled indicators plus catalogue-discovered extras. |
| 9 | BIS bulk files: `WS_TC` (credit to the non-financial sector), `WS_CBPOL` (policy rates). |
| 10, 11, 11b | World Happiness Report; UNHCR displacement; World Bank PIP (Gini, deciles). |
| 12, 12b, 13 | OECD National Accounts at a Glance (households); ARWU (Shanghai ranking); Penn World Table 11.0. |
| 14, 14b, 14c | Gauge trees (`RAY_DALIO_GAUGE_TREES`), tree utilities, tree patches (all replaced by §5). |
| 15, 15b | `assemble_master`; `build_derived` (replaced by §9.3). |
| 16 | `df_health` indicator health table (keep and extend, §7.1). |
| 17 | Report-coverage check against the 64-item list (superseded by the ledger, §7.1). |

### 9.2 Feed defects and required fixes

**FD-01 (P0) OWID, cell 7.** `parse_owid_csv` never checks the `Code` column. Charts report data for 109 of 109 countries even where OWID cannot cover them (TI corruption, WHR, WID wealth; for example Faroes, Gibraltar, Isle of Man, BVI, Guernsey, Jersey), which suggests the per-country filter is ignored for unknown codes and the same global rows are stamped with every ISO3. Change: download each chart once (full-data CSV variant; confirm the parameter on a test chart), parse `Entity`, `Code`, `Year` or `Day`, slice by `Code` locally, keep the annual mean for `Day` charts, never assign a row to a country the `Code` column does not name. Acceptance: an assert that every emitted row’s source `Code` equals its `country_code`; a per-chart table of countries with data; 16 requests (or fewer). Keep only these charts: nuclear-warhead stockpiles (FAS), Cantril ladder (substitute for life satisfaction), electricity generation, primary energy, oil, gas and coal production, TI corruption perception index, COVID excess mortality, WID series, deaths in armed conflicts. Drop `annual-working-hours-per-worker` (duplicates `PWT.AVH`), the two CO2 charts (WDI has CO2) and `labor-share-of-gdp` (no PDF basis). Stop minting extra series for secondary value columns.

**FD-02 (P0) PIP returns 0 rows, cell 11b.** All countries parse empty although HTTP 200 came back (44 countries gave 404). First suspect: `fill_gaps=true` returns line-up rows with null Gini and decile columns. Change: print `reporting_year, gini, decile1, decile10` for one known country (for example IND); request survey years (`fill_gaps=false`) or keep rows where `gini` is non-null; a single `country=all` request. Acceptance: `PIP.GINI`, `PIP.DECILE1`, `PIP.DECILE10` have rows. PIP is registered as `substitute_source` for the Gini and decile items (#20, #30) because it is not named in the PDF.

**FD-03 (P0) ARWU returns 0 rows, cell 12b.** Years 2019–2025 parse to empty frames and 2026 raises “unhashable type: list”. country\_converter also warns on ambiguous matches (China-Taiwan, China-Macau). Change: print a sample of `region` values and the `to_iso3` output; map region names to ISO3 through an explicit dictionary first (Chinese Mainland, Taiwan Region, Hong Kong SAR, Macau SAR) then country\_converter; parse rank bands (“101-150”, “401-500”) by the leading integer; find and guard the list-valued field in the 2026 payload. Acceptance: `ARWU.TOP100_COUNT` and `ARWU.TOP500_COUNT` exist for every year fetched; countries without entries are zero-filled for years the ranking exists; Taiwan is TWN.

**FD-04 (P0) WHR: only life ladder loads, cell 10.** The three `files.worldhappiness.report` workbooks fail (403/404) and the loader falls back to an OWID CSV carrying life ladder only. As a result `WHR.SOCIAL_SUPPORT` and `WHR.POSITIVE_AFFECT` are empty and 67.5% of the happiness gauge has no data. Change: probe programmatic routes in order (the WHR “Data for Figure 2.1” workbook URLs with a browser user-agent; OWID Grapher charts for social support and positive affect; any other stable public file of the Gallup World Poll country panel), print which URL succeeded and which columns matched, warn when fewer than three required columns are found. Keep only life ladder, social support and positive affect. If no route works, record a GAP with the failure reasons (never ask for a manual download).

**FD-05 (P0) COVID excess mortality returns 0 rows, cell 7.** Chart `excess-mortality-p-scores-average-baseline` yields no parsable country; `OWID_WINDOWS` is keyed by a different slug (`excess-mortality-p-scores-projected-baseline`) that is not in `OWID_CHARTS`, so the window logic never runs. Change: find a working excess-mortality chart and the P-score column; define `COVID.EXCESS_MORTALITY_PSCORE` as the mean P-score over 2020-01-01 to 2022-12-31 (one value per country, stored with year 2022); use the same slug in `OWID_CHARTS` and `OWID_WINDOWS`. Note that a single year of data cannot satisfy `MIN_CROSS_SECTION` unless at least 30 countries report it, and it yields one gauge-year; that is accepted.

**FD-06 (P0) WID inequality series.** `share-of-the-top-10-in-pre-tax-national-income` returns 404 (`WID.TOP10_INCOME` empty); the content of `wealth-share-richest` (`WID.TOP1_WEALTH`) is unconfirmed. PDF items #58 and #59 need top-1% and top-10% shares of income and wealth. Change: confirm which OWID slugs (or the WID bulk data) provide these four series; emit `WID.TOP1_INCOME`, `WID.TOP10_INCOME`, `WID.TOP1_WEALTH`, `WID.TOP10_WEALTH`; record the slug definitions in a code comment.

**FD-07 (P0) Reserve currency, cell 3b.** `parse_cofer` gives the EUR share to every euro-area member and to `EMU`. Change per D12: assign EUR to nobody; members receive no rows; CHF to CHE and LIE; other currencies unchanged; non-issuers keep the structural 0. Acceptance: no `IMF.COFER_SHARE` rows for DEU, FRA, ITA, ESP, NLD, AUT, BEL or other euro members; USA, JPN, GBR, CHN, AUS, CAN, CHE keep theirs.

**FD-08 (P0/P1) ILOSTAT, cell 8.** The parser keeps total sex only; the PDF uses men’s hours and male labor-force participation. Add `ILO.HOURS_WEEKLY_MODELLED_MALE` (id `HOW_2EMP_SEX_NB_A`, male) and `ILO.LFPR_MODELLED_MALE` (id `EAP_2WAP_SEX_AGE_RT_A`, male, age 15+); keep the total series as fallbacks. Add `ILO.UNEMP_LONGTERM_SHARE` = unemployed for 12 months or more ÷ total unemployed, from `UNE_TUNE_SEX_AGE_DUR_NB_A` (inspect the classification labels for the duration band; the current series are counts by band and must not be averaged). Strikes: `ILO.STRIKES_DAYS_NOT_WORKED` (id `STR_DAYS_ECO_NB_A`, economic-activity total; PDF #13) and `ILO.STRIKES_COUNT` (id `STR_TSTR_ECO_NB_A`; PDF #60); optionally scale per 1,000 workers using `SL.TLF.TOTL.IN`. Collective bargaining: the single total-coverage series. Minimum wage: nominal levels are not comparable; keep `ILO.MIN_WAGE_RATIO` (minimum wage over mean or median monthly earnings) only if ILOSTAT supplies both, otherwise drop the component and renormalise. Delete all `ILO.*` wildcards and the “12 most frequent variants” cap. Use the bulk file as the primary route.

**FD-09 (P1) WGI.** The tree lists `.PER.RNK` codes that are never fetched. Delete them; use `.EST` only: `CC.EST`, `GE.EST`, `PV.EST`, `RL.EST`, `RQ.EST` (drop `VA.EST`). WGI has no rows for non-economies; that is expected.

**FD-10 (P1) Other retention changes** (P-6 computes the exact set; these are the known ones). PWT: variables `rgdpo, emp, avh, rkna` plus derived `PWT.RGDPO_PER_WORKER` (rgdpo ÷ emp × 1e6) and `PWT.RKNA_GROWTH` (year-on-year % change of rkna); drop `rgdpna, pop, hc, ctfp, rtfpna, labsh, rnna, delta, csh_i, irr`; PWT 11.0 appears to end in 2023 (verify). OECD NAAG: keep only the household net saving rate (the series aliased `OECD.HH_SAVINGS_RATE`; verify the dimension ids); drop the other 24 series. BIS: keep `TC.G`, `TC.P`, `TC.N` (percent of GDP) and `CBPOL` end-of-period and average; drop `TC.H`, `TC.C`. IMF WEO: keep the series referenced by the tree plus `NGDP_RPCH` (PDF #49) and `GGXWDN_NGDP` (net debt, stand-in for debt relative to assets); drop `LP, GGR_NGDP, BCA, TXG_RPCH, TMG_RPCH, NGAP_NPGDP, PPPSH, GGXONLB_NGDP`. V-Dem: keep the referenced series; drop `v2caviol, v2x_polyarchy, v2x_libdem, v2x_accountability, v2xeg_eqprotec`. EdStats: keep all eight (Barro-Lee shares serve the cohort weights of §10 wages). HCI db 63: `HD.HCI.OVRL`, `HD.HCI.EYRS`, `HD.HCI.HLOS`. UHC db 58: `SH.UHC.SRVS.CV.XD`. Doing Business db 1: the eight referenced series. Population db 40: `SP.POP.1564.TO`, `SP.POP.DPND`. EIA: products 44 and 2; replace the `EIA.ELECTRICITY*` wildcard with the exact generation code (probably `EIA.ELECTRICITY.GENERATION.BKWH`; verify). WDI additions (all-economy pull): `NY.GDP.TOTL.RT.ZS`, `TX.VAL.FUEL.ZS.UN`, `TM.VAL.FUEL.ZS.UN`, `TX.VAL.AGRI.ZS.UN`, `TM.VAL.AGRI.ZS.UN`, `TX.VAL.FOOD.ZS.UN`, `TM.VAL.FOOD.ZS.UN`, `TX.VAL.MMTL.ZS.UN`, `TM.VAL.MMTL.ZS.UN`, `TM.VAL.MRCH.CD.WT`, `BM.GSR.ROYL.CD`, `SL.TLF.TOTL.IN`, `NE.GDI.FTOT.KD`; PDF-listed WDI codes kept even if unwired (`NV.AGR.TOTL.ZS`, `NV.AGR.TOTL.CD`, `EN.GHG.CO2.MT.CE.AR5`, `EN.GHG.CO2.PC.CE.AR5`, `SE.XPD.TOTL.GD.ZS`, `SI.POV.GINI`, `SI.DST.FRST.20`, `SI.DST.05TH.20`, `SI.DST.10TH.10`, `NY.GDP.MKTP.KD.ZG`). Expect roughly 100 WDI codes to remain from 185.

### 9.3 Derived series (annual, dated at base year t; no `last_actual` argument)

Compute each for every year t where its inputs exist. Rows are non-projection rows (forward-looking ones use projected inputs but are dated at t).

| Series | Definition |
| --- | --- |
| `DERIVED.WORKING_AGE_GROWTH_YOY` | % change of `SP.POP.1564.TO`. |
| `DERIVED.WORKING_AGE_GROWTH_FWD10` | ((WA(t+10) ÷ WA(t))^(1/10) − 1) × 100, with WA = `SP.POP.1564.TO` actuals and `PROJ.SP.POP.1564.TO` where actuals end. Defined for t with t+10 ≤ 2040. |
| `DERIVED.DEPENDENCY_RATIO_CHG_FWD10` | `SP.POP.DPND`(t+10) − `SP.POP.DPND`(t), same actual/projection rule. |
| `DERIVED.GDP_GROWTH_VOL10`, `DERIVED.INFLATION_VOL10` | Standard deviation of `NY.GDP.MKTP.KD.ZG` and `FP.CPI.TOTL.ZG` over t−9..t, at least 8 observations. |
| `DERIVED.CAPITAL_PRODUCTIVITY` | Mean GDP growth ÷ mean `NE.GDI.FTOT.ZS` over t−9..t, at least 8 observations each, positive denominator. |
| `DERIVED.CAPITAL_STOCK_GROWTH_PIM` | Perpetual inventory on `NE.GDI.FTOT.KD` (depreciation 5%, K0 = I0 ÷ (g + δ), g = 5%), 5-year trailing mean of stock growth; needs 20 consecutive years. |
| `DERIVED.DEBT_GROWTH_MINUS_INCOME_GROWTH` | Debt/GDP(t) − Debt/GDP(t−5); per country use `BIS.TC.N.PCT_GDP` if available at both ends, otherwise `FS.AST.PRVT.GD.ZS` + `IMF.GGXWDG_NGDP` (the same source at both ends). |
| `UNHCR.DISPLACED_PCT_POP` | `UNHCR.DISPLACED_TOTAL` ÷ `SP.POP.TOTL` × 100. |
| `FAS.NUCLEAR_STOCKPILE` | Fetched values plus structural zeros for non-nuclear states in every year the series exists. |
| `DERIVED.PWT_HOURS_WEEKLY` | `PWT.AVH` ÷ 52. |
| `DERIVED.PISA_MEAN` | Mean of available `LO.PISA.MAT`, `LO.PISA.REA`, `LO.PISA.SCI` for the same country-year. |
| `DERIVED.PER_CAPITA.<code>` | value ÷ `SP.POP.TOTL` (for absolute quantities in per-capita halves). |
| `DERIVED.ABS.RD_SPEND_USD` | `GB.XPD.RSDV.GD.ZS` ÷ 100 × `NY.GDP.MKTP.CD`. |
| `DERIVED.ABS.RESEARCHERS` | `SP.POP.SCIE.RD.P6` × `SP.POP.TOTL` ÷ 1e6. |
| `DERIVED.NET_EXPORTS_<FUEL\|AGRI\|FOOD\|MMTL>_PCT_GDP` | (`TX.VAL.<X>.ZS.UN` ÷ 100 × `TX.VAL.MRCH.CD.WT` − `TM.VAL.<X>.ZS.UN` ÷ 100 × `TM.VAL.MRCH.CD.WT`) ÷ `NY.GDP.MKTP.CD` × 100. |

Caveat for the later back-test: forward-looking series for past years use actual later population where it exists, which is look-ahead. Accepted for the MVP.

### 9.4 Tree corrections (all realised in the §12 target tree)

- **Hours of work.** `HOURS_WORKED_WEEKLY` = (`ILO.HOURS_WEEKLY_MODELLED_MALE`, `ILO.HOURS_WEEKLY_MODELLED` \[S\], `DERIVED.PWT_HOURS_WEEKLY` \[P\]). The old `HOURS` coalesced weekly hours with annual hours and used all-sex data; it entered the productivity gauge as an extra equal-weight component in the education and labor-productivity leaves (about 25.6% of the gauge) where the PDF uses hours only to deflate wages. Remove it from those two leaves. It stays in `working_hard` (two-thirds), in `savoring_life_vs_achieving/observed_outcomes` (all) and in `self_sufficiency/hard_work` (50%). Result: 15.42% of the productivity gauge (the PDF’s rounded figures give 15.45%; Appendix C).
- **Weighted leaves.** Components carry optional weights (D-model §5). Required: `self_sufficiency/hard_work` hours 50%, male labor-force participation, effective retirement age and vacation days 16.7% each; `innovation_outputs` patents 25%, royalty payments 25%, and 12.5% each for new businesses, people creating businesses, new major websites and trademarks; `expressed_values` seven equal WVS items.
- **Happiness duplicate.** `share_reported_daily_enjoyment` ← `WHR.POSITIVE_AFFECT`; `share_reported_happy` ← `WVS.HAPPY` (GAP until fetched). Never reuse positive affect.
- **Strength halves.** `GB.XPD.RSDV.GD.ZS` and `SP.POP.SCIE.RD.P6` must not sit in both halves of innovation; `MS.MIL.TOTL.P1` not in both halves of military. Absolute halves use the `DERIVED.ABS` builders; per-capita halves use ratios, rates or indices.
- **ND-GAIN.** Use `NDGAIN.VULNERABILITY` (stand-in for climate impact on GDP) and `NDGAIN.READINESS` (preparedness) plus COVID in `acts_of_nature`; remove `NDGAIN.GAIN` (a composite of the two).
- **Components with no PDF basis, removed from every leaf:** `FI.RES.TOTL.MO`, `MS.MIL.XPRT.KD`, `MS.MIL.XPND.ZS`, `NE.TRD.GNFS.ZS`, `TX.VAL.TECH.MF.ZS`, `TX.MNF.TECH.ZS.UN`, `IT.NET.SECR.P6`, `IP.JRN.ARTC.SC`, `EG.ELC.ACCS.ZS`, `EG.USE.ELEC.KH.PC`, `IT.NET.BBND.P2`, `ILO.EMP_POP_RATIO_MODELLED`, `SL.EMP.TOTL.SP.ZS`, `VDEM.v2smpolhate` and `VDEM.v2smpolsoc` in the character leaf (keep `v2cacamps` and `v2smpolsoc` in `wealth_values_gap`), `MS.MIL.XPND.GD.ZS` in external conflict, `NDGAIN.GAIN`. Also drop other health and innovation alternatives that measure inputs rather than the PDF item (`SH.XPD.CHEX.GD.ZS`, `SH.MED.PHYS.ZS`, `SP.DYN.IMRT.IN`, `IP.PAT.NRES`, `IC.REG.STRT.BUS.DFRN`, which is never fetched).
- **Royalties.** The PDF says “royalty and license fees, payments”: use `BM.GSR.ROYL.CD` (payments) first and `BX.GSR.ROYL.CD` (receipts) as `[P]`.

## 10. New data to fetch (programmatic only)

**Probe policy.** For each item try the candidate routes below in order, print which route was tried and its outcome, and stop at the first that yields usable country-year data. If every route fails, record a GAP with the reasons in the ledger. Do not ask for a manual download. Skipped (D4): Google Trends, Global Firepower, CSET.

| Item (PDF ref) | Target codes | Candidate route | Priority |
| --- | --- | --- | --- |
| BIS OTC FX turnover (#38), banking claims by currency (#5), international debt securities | `BIS.FX_TURNOVER_VALUE`, `BIS.FX_TURNOVER_SHARE`, `BIS.INTL_DEBT_CCY_SHARE`, `BIS.LBS.CROSSBORDER_CLAIMS`, `BIS.DEBT_SEC.OUTSTANDING` | BIS bulk files `WS_DER_OTC_TOV`, `WS_LBS_D_PUB`, `WS_DEBT_SEC2_PUB` through the existing `bis_download` helper; first run an inspect step that prints the dimensions to choose the right series keys | P1 |
| WIPO Global Innovation Index (#64, source of record for #24, #36, #52, #63) | `WIPO.GII_SCORE`, `WIPO.NEW_MAJOR_WEBSITES`, trademark counts if separate | Public GII indicator data via a stable URL or API | P1 |
| World Values Survey: seven culture items and the happy item | `WVS.<ITEM>` × 7, `WVS.HAPPY` | WVS online data or an OWID Grapher chart built on WVS; identify question variables from the codebook; compute % agreeing | P1 (GAP if no route) |
| WHR country panel | `WHR.SOCIAL_SUPPORT`, `WHR.POSITIVE_AFFECT` | FD-04 | P1 |
| WID inequality | four `WID.*` codes | FD-06 | P1 |
| OECD effective retirement age; employment-protection rigidity index | `OECD.EFFECTIVE_RETIREMENT_AGE`, `OECD.EPL` | OECD SDMX API (Pensions at a Glance; employment protection) | P2 |
| Vacation days (paid leave plus public holidays) | `VACATION.DAYS` | Search for a programmatic dataset; otherwise GAP | P2 |
| GEM fear of failure and entrepreneurship prevalence | `GEM.FEAR_OF_FAILURE`, `GEM.ENTREPRENEURSHIP_PREVALENCE` | GEM public national data if reachable without registration; otherwise GAP | P2 |
| Financial-center index | `GFCI.SCORE` | Programmatic route to Z/Yen city scores aggregated to country; otherwise GAP | P2 |
| Metals and ores production (#34) | `USGS.METALS_PRODUCTION` | USGS Mineral Commodity Summaries data release | P2 |
| Conflict-event and discontent feeds (#11, #14, #21, #22, #23, #45) | `GDELT.PROTEST_INTENSITY`, `GDELT.GOVT_DISAPPROVAL`, `GDELT.DISCONTENT_TERMS`, `CLINE.CONFLICT_PER_CAPITA` | GDELT public API (annual per-country counts normalised by total media volume); Cline Center public dataset. Google Trends items (#21, #22) are skipped | P2 |
| Wages by education cohort (35% of the productivity gauge) | `WAGE.EDUCATED_HOURLY_USD` and cohort variants | ILOSTAT earnings tables, OECD earnings by attainment, Eurostat | P2 |
| Historical CLIO Infra items (#7 book titles, #26 technology adoption, #37 numeracy, #54 shipping tonnage, #62 universities founded, #10 CO2, #13 and #31 where not otherwise covered) | CLIO codes | Attempt only if a stable programmatic route exists; they have no 2024-gauge consumer, so status is `pdf_listed_unwired` or a GAP with the reason; #54 keeps the proxy `IS.SHP.GCNW.XQ` | P2 |
| Paywalled or unavailable | `VC.TOTAL_FUNDING`, `INFRA.QUALITY_INDEX`, `RATING.SOVEREIGN`, `DEBT.FOREIGN_HELD_SHARE`, Thomson Reuters sector returns (#2), Bloomberg Innovation Index (#6) | No fetch; kept as GAP placeholders in the tree and ledger with the reason | P3 |
| Pairwise external conflict | `DOTS.BILATERAL_TRADE_CONCENTRATION`, `UN.VOTING_DISTANCE`, `SANCTIONS.GSDB`, `UCDP.INTERSTATE` | No fetch (D23); placeholders only | P3 |

**Wage formula (target, interpretation of PDF pp. 162–165).** For cohort k in {tertiary, upper secondary, below secondary}: cost\_k = hourly wage\_k relative to USA; quality-adjusted cost\_k = cost\_k ÷ (country PISA ÷ USA PISA); productivity-adjusted cost\_k = cost\_k ÷ (country GDP per hour worked ÷ USA GDP per hour worked); country value = Σ share\_k × value\_k, with shares from Barro-Lee (`BAR.*.CMPT.25UP.ZS`, `BAR.NOED.25UP.ZS`). Until wage data exist, the cost leaves use the PDF’s own cost proxy (nominal GDP per capita).

## 11. What the Appendix specifies

This section records the PDF’s specification (pp. 132–184) so the PDF is not needed.

### 11.1 Gauge catalogue (pp. 132–134)

**Strength determinants**

- **Education:** basic and higher education, split about evenly. About half measures the absolute quantity of educated people at various levels; about half measures quality (higher-education rankings, test scores, average years of education).
- **Innovation & Technology:** about half is absolute share of key metrics (patents, researchers, R&D spending, venture-capital funding); half is external rankings plus per-capita innovation measures.
- **Cost Competitiveness** (“what you pay vs. what you get”): quality-adjusted and productivity-adjusted labor costs plus other productivity measures. Shown as a total score only.
- **Infrastructure & Investment:** share of world investment, overall infrastructure quality, investment and savings as % of GDP, logistics performance.
- **Economic Output:** GDP as a share of world (adjusted for price differences, i.e. PPP), with some weight on GDP per capita.
- **Productivity/Output Growth:** forward 10-year growth estimate; two-thirds weight on productivity predictors, one-third on indebtedness predictors (the tables use 65/35).
- **Trade:** absolute exports as a share of world exports.
- **Military Strength:** mostly absolute share of military spending, plus personnel, nuclear weapons and external indices of military capability.
- **Financial Markets & Financial Center:** absolute transaction shares and market capitalisations, plus qualitative external indices of financial-center cities.
- **Reserve Status:** share of transactions, debts and reserves denominated or held in the country’s currency. Euro-area members are not scored individually (only the Eurozone entity is).

**Other gauges**

- **Indebtedness (debt cycles)** combines (a) debt levels relative to asset levels, (b) sizes of external and internal surpluses and deficits, (c) debt-service cost relative to GDP, (d) share of debt in own vs foreign currency, (e) share held by citizens vs foreigners, (f) credit rating. It is built to exclude reserve-currency status. The PDF says the underlying statistics are proprietary.
- **Internal Conflict:** actual conflict events (for example protests), political conflict (partisanship), general discontent (surveys).
- **Rule of Law/Governance:** rule-of-law measures (mainly business surveys) plus corruption measures (external indices and surveys).
- **Gaps in Wealth, Opportunity & Values:** wealth and income inequality (for example top 1% vs the rest) plus political conflict (how split the legislature is on ideology).
- **Geology:** land size, value of natural resources, total production of energy, agriculture and industrial metals (absolute capacity), net exports of each (relative self-reliance), freshwater.
- **Character/Civility/Determination:** surveys on attitudes to hard work and success, plus proxies for self-sufficiency (size of government transfers, effective retirement age).
- **Resource Allocation Efficiency:** chronically high unemployment, whether debt growth generates commensurate income growth, external indices and surveys on labor-market rigidity and ease of getting loans.
- **Acts of Nature:** expert assessments of future climate impact on GDP, assessments of disaster preparedness, COVID outcomes (the PDF rates this gauge “so-so”).
- **External Conflict:** economic, political/cultural and military conflict between pairs of major countries; structural indicators (bilateral trade, tariffs) plus timely ones (news on sanctions, trade wars).
- **Health Index** and **Happiness Index:** §11.3.

### 11.2 Growth formula (pp. 143–147, 157)

10-year real GDP growth = working-age population growth + growth per worker (+ error adjustment). Growth per worker = 65% productivity + 35% indebtedness (the p. 143 table; the p. 149 text says “two-thirds” and “one-third”). The error adjustment is the formula’s average past error (about 1 percentage point) assumed to persist. Reported fit: 81% correlation with subsequent 10-year growth over 218 observations (35 countries, every 5 years, 70+ years); productivity alone 74%, indebtedness alone 53%; within 1 pp of actual 61% of the time and within 2 pp 90% of the time. Reported correlations of components with subsequent growth (for later sanity checks): productivity 64%, culture 49%, what-you-pay-vs-get 60%, education 64%, labor productivity 49%, working hard 61%, investing 51%, indebtedness 34%, debt levels and flow 32%, monetary policy 15%.

### 11.3 Explicit weights

**Health Index (p. 136; sums to 100%)**

| Component | Weight | Direction (implied) |
| --- | --- | --- |
| Life expectancy at birth | 30.0% | higher is better |
| Share facing food insecurity | 15.0% | lower |
| Share with access to essential health services | 15.0% | higher |
| Child mortality (deaths per 1,000 live births) | 10.0% | lower |
| Homicide rate (per 100,000) | 7.5% | lower |
| Share of population displaced by conflict | 7.5% | lower |
| Air pollution exposure (PM2.5) | 5.0% | lower |
| Share using safely managed drinking water | 5.0% | higher |
| Share with basic sanitation facilities | 5.0% | higher |

**Happiness Index (p. 136; sums to 100%)**

| Component | Weight |
| --- | --- |
| Share of population reporting being happy | 22.5% |
| Share reporting daily enjoyment | 22.5% |
| Reported life satisfaction (1–10) | 22.5% |
| Share reporting a good social support network | 22.5% |
| Suicide rate per 100,000 (lower is better) | 10.0% |

**Productivity gauge (pp. 143, 155, 158, 169–181; % of the productivity gauge)**

| Node | Weight | Items (PDF names) |
| --- | --- | --- |
| What you pay vs. what you get | 70.0 | four leaves below |
|  Education | 17.5 | Cost of quality-adjusted educated worker, relative to USA |
|  Labor productivity | 17.5 | Cost of productivity-adjusted educated worker |
|  Working hard | 17.5 | Average hours worked 11.7; change in dependency ratio, 10 years forward 5.8 |
|  Investing | 17.5 | Investment % 3.1; savings rate 3.1; productivity of capital investments 3.1; growth in capital stock 3.1; income volatility 2.6; inflation volatility 2.6 (sums to 17.6 through rounding) |
| Culture | 30.0 | six pillars of 5.0 each, below |
|  Corruption | 5.0 | Surveys of corruption 2.5; transparency index 2.5 |
|  Bureaucracy | 5.0 | Starting a business 1.7; construction permits 1.7; burden of government regulation 1.7 |
|  Rule of law | 5.0 | Efficiency of legal frameworks, property rights, protecting investors, enforcing contracts: 1.25 each |
|  Savoring life vs achieving | 5.0 | Observed outcomes (average hours worked) 2.5; expressed values (survey) 2.5 |
|  Innovation | 5.0 | Inputs 2.5; outputs 2.5 |
|  Self-sufficiency | 5.0 | Hard work 2.5; government support 1.25; rigidity of labor market 1.25 |

**Item-level detail inside culture pillars** (the PDF prints these on a culture basis, whole culture = 100; multiply by 0.30 for productivity basis):

- Corruption (16.7): surveys of corruption 8.3 = three WEF sub-indices of 2.8 each (diversion of public funds; irregular payments and bribes; favoritism in government officials’ decisions); transparency index (Transparency International) 8.3.
- Savoring life vs achieving (16.7): observed outcomes = average hours worked 8.3; expressed values 8.3 = seven World Values Survey items of about 1.2 each: children having responsibilities is important; growth is important for the future of a country; having a good time is important; importance of being successful; economic growth should be a priority over the environment; competition is not harmful to business; hard work leads to success.
- Innovation (16.7): inputs 8.3 (listed items: gross expenditure on R&D 1.0; researchers 1.0; fear of business failure 1.0; entrepreneurship prevalence 1.0, which sum to only 4.0, see §11.5); outputs 8.3 = new patents 2.1; royalty and license fee payments 2.1; number of new businesses 1.0; % of people creating new businesses 1.0; new major websites 1.0; new trademark creation 1.0.
- Self-sufficiency (16.7): hard work 8.3 = average hours worked 4.2, labor-force participation 1.4, effective retirement age 1.4, actual vacation days 1.4; government support 4.2 = transfer payments to households 2.1, government outlays 2.1; rigidity of labor market 4.2 = collective bargaining as % of workforce 1.4, ease of hiring/firing index 1.4, minimum wage 1.4.

**Indebtedness gauge (pp. 141, 143, 157).** 35% of growth per worker, split 50/50 (17.5 each) between debt levels and flow and monetary policy. Rationale: low debt relative to income, debt growing slower than income, and easier monetary policy predict faster growth over 10 years; the exception is where adequate financial intermediaries do not exist.

### 11.4 Scoring conventions stated in the PDF (for the later phase; not implemented now)

- Strength determinants are shown as the weighted average percent rank (0–100%) of their underlying indicators across countries, as “Total” (all measures) and “Quality only / Per capita” (only quality or per-capita measures). Where no quality measure exists, a per-capita version of the quantity measure is created. Competitiveness shows a total only. Reserve-currency scores are not given to euro members.
- Productivity-side measures are scored “relative to countries with similar income” and “relative to the world”, on a roughly −4 to +4 scale. Income adjustment matters: for example the raw corruption-to-growth relationship is slightly negative before it.
- Cost proxy (p. 160): nominal GDP per capita relative to the developed-world average, in logs (a $2,000 gap matters more between $500 and $2,500 than between $40,000 and $42,000).
- Education cost (pp. 162–165): relative cost per hour worked of educated and uneducated cohorts (college, high school, none), adjusted for vacation, then adjusted for education quality using an international test; aggregated with weights equal to each cohort’s share of the population. The labor-productivity leaf uses the same cohort costs adjusted for observed output per hour instead of test quality.
- Working hard (p. 166): average weekly hours of actual work **by men** in the labor force, adjusted for vacation and holidays, plus shifts in the share of the whole population that works.
- Investing (p. 168): total non-residential fixed investment rate and the household savings rate.
- Future working-age growth: forward 10-year change in the working-age population; dependency-ratio change over the next 10 years.

### 11.5 Ambiguities and inconsistencies in the PDF (decisions recorded)

1. Productivity vs indebtedness is “two-thirds / one-third” in prose and 65/35 in tables. **Decision: 65/35.**
2. The listed innovation-input items sum to 4.0 of a stated 8.3, so the list is probably truncated. **Decision: implement the four listed items with equal weight.**
3. Corruption surveys are attributed to WEF in the text, but V-Dem appears in the source list (items #46, #51, #61). **Decision: V-Dem is primary; WGI is a stand-in fallback.**
4. Source #28 names the World Inequality Database for hours worked, which is not an hours source. **Decision: ILO weekly hours (male) first; PWT annual hours as a stand-in fallback.**
5. p. 157 says rankings are “among the 20 countries shown” while the tables cover 35. No action.
6. Several raw correlations to growth are negative (for example transparency index −18%); signs are only meaningful after the income adjustment. Do not infer direction from them.

## 12. Target gauge trees

This is the authoritative tree. Notation: a code with no suffix is tier `exact`; `[S]` = `substitute_source`; `[P]` = `proxy_concept`; `(GAP)` = placeholder, not yet fetchable (kept in the tree, reported as GAP; becomes live automatically once the code appears in the master). Direction follows each code: `(+)` or `(−)`; `(+?)` or `(−?)` marks an ambiguous sign to list in `direction_map.csv` (§6.5). Alternatives are listed in priority order. An alternative with no data anywhere in the master is skipped when choosing the primary (§15.2). Provenance tags: `pdf`, `pdf_implied`, `heuristic`. Where the PDF fixes a weight it is shown; unless stated, components of a leaf have equal weight.

Shared alternative sets (define once, reuse by name):

- `HOURS_WORKED_WEEKLY` = `ILO.HOURS_WEEKLY_MODELLED_MALE` (+), `ILO.HOURS_WEEKLY_MODELLED` \[S\] (+), `DERIVED.PWT_HOURS_WEEKLY` \[P\] (+).
- `GDP_PC_COST` = `IMF.NGDPDPC` (−), `NY.GDP.PCAP.CD` \[S\] (−). (The PDF’s own cost proxy; direction − means cheaper labor scores higher.)
- `PISA_MEAN` = `DERIVED.PISA_MEAN` (+), `HD.HCI.HLOS` \[P\] (+).

### 12.1 Health index (provenance `pdf`)

| Component | Weight | Alternatives |
| --- | --- | --- |
| Life expectancy | 0.300 | `SP.DYN.LE00.IN` (+) |
| Food insecurity | 0.150 | `SN.ITK.DEFC.ZS` (−), `SN.ITK.MSFI.ZS` \[P\] (−) |
| Essential health services | 0.150 | `SH.UHC.SRVS.CV.XD` \[S\] (+) |
| Child mortality | 0.100 | `SH.DYN.MORT` \[S\] (−) |
| Homicide | 0.075 | `VC.IHR.PSRC.P5` \[S\] (−) |
| Displaced population | 0.075 | `UNHCR.DISPLACED_PCT_POP` \[S\] (−) |
| Air pollution | 0.050 | `EN.ATM.PM25.MC.M3` \[S\] (−) |
| Safely managed drinking water | 0.050 | `SH.H2O.SMDW.ZS` (+), `SH.H2O.BASW.ZS` \[P\] (+) |
| Basic sanitation | 0.050 | `SH.STA.BASS.ZS` (+), `SH.STA.SMSS.ZS` \[P\] (+) |

No code may appear in this gauge twice. (The old tree reused `SP.DYN.IMRT.IN`, `SH.XPD.CHEX.GD.ZS`, `SH.MED.PHYS.ZS`; they are removed.)

### 12.2 Happiness index (provenance `pdf`)

| Component | Weight | Alternatives |
| --- | --- | --- |
| Share reported happy | 0.225 | `WVS.HAPPY` (+) (GAP until fetched) |
| Share reported daily enjoyment | 0.225 | `WHR.POSITIVE_AFFECT` \[P\] (+) |
| Life satisfaction | 0.225 | `WHR.LIFE_LADDER` (+), `WHR.CANTRIL_LADDER` \[S\] (+) (OWID chart; same Gallup question) |
| Good social support network | 0.225 | `WHR.SOCIAL_SUPPORT` \[S\] (+) |
| Suicide rate | 0.100 | `SH.STA.SUIC.P5` (−) |

### 12.3 Growth and productivity gauge

`growth_per_worker_estimate` = 0.65 × `productivity_gauge` + 0.35 × `indebtedness_gauge` (provenance `pdf`). `future_growth` follows §6.4. `productivity_gauge` = 0.70 `what_you_pay_vs_what_you_get` + 0.30 `culture` (`pdf`).

**`what_you_pay_vs_what_you_get`: four leaves, 0.25 each (`pdf`)**

| Leaf | Component (weight) | Alternatives |
| --- | --- | --- |
| `education_cost_quality_adj` (components `heuristic`: interim stand-in for the PDF’s cohort cost ratio) | cost proxy (1/3) | `GDP_PC_COST` |
|  | quality (1/3) | `PISA_MEAN` |
|  | attainment (1/3) | `BAR.SCHL.25UP` (+), `HD.HCI.EYRS` \[P\] (+), `SE.TER.CUAT.BA.ZS` \[P\] (+) |
| `labor_productivity_cost_adj` (`heuristic`) | cost proxy (1/2) | `GDP_PC_COST` |
|  | output per worker (1/2) | `PWT.RGDPO_PER_WORKER` (+), `SL.GDP.PCAP.EM.KD` \[S\] (+) |
| `working_hard` (`pdf`) | average hours (2/3) | `HOURS_WORKED_WEEKLY` |
|  | dependency-ratio change, 10 years forward (1/3) | `DERIVED.DEPENDENCY_RATIO_CHG_FWD10` (−) |
| `investing` (`pdf`; weights 3.1, 3.1, 3.1, 3.1, 2.6, 2.6 each ÷ 17.5, sum 1.0057; the engine renormalises) | non-residential fixed investment rate | `NE.GDI.FTOT.ZS` (+), `IMF.NID_NGDP` \[P\] (+), `NE.GDI.TOTL.ZS` \[P\] (+) |
|  | household savings rate | `OECD.HH_SAVINGS_RATE` (+), `IMF.NGSD_NGDP` \[P\] (+), `NY.GNS.ICTR.ZS` \[P\] (+), `NY.GDS.TOTL.ZS` \[P\] (+) |
|  | productivity of capital | `DERIVED.CAPITAL_PRODUCTIVITY` (+) |
|  | growth in capital stock | `PWT.RKNA_GROWTH` (+), `DERIVED.CAPITAL_STOCK_GROWTH_PIM` \[S\] (+) |
|  | income volatility | `DERIVED.GDP_GROWTH_VOL10` (−) |
|  | inflation volatility | `DERIVED.INFLATION_VOL10` (−) |

**`culture`: six pillars, 1/6 each (`pdf`)**

| Pillar | Component (weight) | Alternatives |
| --- | --- | --- |
| `corruption` | transparency index (0.5) | `TI.CPI` (+), `CC.EST` \[P\] (+) |
|  | surveys: diversion of public funds (1/6) | `VDEM.v2exthftps` (+), `VDEM.v2x_pubcorr` \[P\] (−) |
|  | surveys: irregular payments (1/6) | `VDEM.v2excrptps` (+), `VDEM.v2x_execorr` \[P\] (−) |
|  | surveys: favoritism (1/6) | `VDEM.v2clrspct` (+), `GE.EST` \[P\] (+) |
| `bureaucracy` | starting a business | `IC.REG.DFRN.PC.DFRN` (+) |
|  | construction permits | `IC.CNST.PRMT.DFRN.DB1619` (+), `IC.CNST.PRMT.DFRN.DB0615` \[S\] (+) |
|  | burden of government regulation | `RQ.EST` \[P\] (+) |
| `rule_of_law` | efficiency of legal frameworks | `VDEM.v2cltrnslw` (+), `VDEM.v2x_jucon` \[P\] (+) |
|  | property rights | `VDEM.v2xcl_prpty` \[P\] (+), `RL.EST` \[P\] (+) |
|  | protecting investors | `PROT.MINOR.INV.DFRN.DB1519` (+), `PROT.MINOR.INV.DFRN.DB0614` \[S\] (+) |
|  | enforcing contracts | `ENF.CONT.COEN.DB1719.DFRN` (+), `ENF.CONT.COEN.DB0415` \[S\] (+) |
| `savoring_life_vs_achieving` | observed outcomes (0.5) | `HOURS_WORKED_WEEKLY` |
|  | expressed values (0.5; seven equal items) | `WVS.CHILDREN_RESPONSIBILITY`, `WVS.GROWTH_IMPORTANT`, `WVS.GOOD_TIME_IMPORTANT`, `WVS.SUCCESS_IMPORTANT`, `WVS.GROWTH_OVER_ENVIRONMENT`, `WVS.COMPETITION_NOT_HARMFUL`, `WVS.HARD_WORK_LEADS_TO_SUCCESS` (each (GAP) until fetched; signs ambiguous: `(+?)`, but “having a good time is important” is probably (−?)) |
| `innovation` | inputs (0.5; four equal) | `GB.XPD.RSDV.GD.ZS` \[S\] (+); `SP.POP.SCIE.RD.P6` (+); `GEM.FEAR_OF_FAILURE` (−) (GAP); `GEM.ENTREPRENEURSHIP_PREVALENCE` (+) (GAP) |
|  | outputs (0.5): patents 0.25 | `IP.PAT.RESD` (+) |
|  | royalty and license fee payments 0.25 | `BM.GSR.ROYL.CD` (+), `BX.GSR.ROYL.CD` \[P\] (+) |
|  | new businesses 0.125 | `IC.BUS.NREG` \[S\] (+) |
|  | % of people creating businesses 0.125 | `IC.BUS.NDNS.ZS` \[P\] (+) |
|  | new major websites 0.125 | `WIPO.NEW_MAJOR_WEBSITES` (+) (GAP until fetched) |
|  | new trademarks 0.125 | `IP.TMK.RSCT` (+) |
| `self_sufficiency` | hard work (0.5): hours 50% | `HOURS_WORKED_WEEKLY` |
|  | labor-force participation 16.7% | `SL.TLF.CACT.MA.ZS` (+), `ILO.LFPR_MODELLED_MALE` \[S\] (+) |
|  | effective retirement age 16.7% | `OECD.EFFECTIVE_RETIREMENT_AGE` (+) (GAP until fetched) |
|  | vacation days 16.7% | `VACATION.DAYS` (−?) (GAP) |
|  | government support (0.25): transfers 50% | `GC.XPN.TRFT.ZS` (−?) |
|  | government outlays 50% | `GC.XPN.TOTL.GD.ZS` (−?), `IMF.GGX_NGDP` \[S\] (−?) |
|  | rigidity (0.25): collective bargaining | `ILO.COLLECTIVE_BARGAINING_COVERAGE` (−?) |
|  | hiring/firing | `OECD.EPL` \[P\] (−?) (GAP until fetched) |
|  | minimum wage | `ILO.MIN_WAGE_RATIO` (−?) (drop component if not buildable) |

The observed-outcomes component of `savoring_life_vs_achieving` and the hours component of `self_sufficiency` deliberately reuse hours (the PDF does). This is within one gauge on purpose; the gate “no code in two leaves of one gauge” (§7.3) therefore applies to Health, Happiness and strength halves, and `HOURS_WORKED_WEEKLY` is an allowed exception (listed in the gate code).

### 12.4 Indebtedness gauge

Provenance `heuristic` for the equal split inside each leaf (the PDF gives only the 50/50 between leaves, which is `pdf`). The PDF says the underlying statistics are proprietary, so each component’s concept is defined by its first alternative; these components are registered stand-ins as a group (status `stand_in`), and `[S]` here means the same concept from another provider.

| Leaf (0.5 each) | Component | Alternatives |
| --- | --- | --- |
| `debt_levels_and_flow` | government debt / GDP | `BIS.TC.G.PCT_GDP` (−), `IMF.GGXWDG_NGDP` \[S\] (−), `GC.DOD.TOTL.GD.ZS` \[S\] (−) |
|  | private debt / GDP | `BIS.TC.P.PCT_GDP` (−), `BIS.TC.N.PCT_GDP` \[P\] (−), `FS.AST.PRVT.GD.ZS` \[P\] (−) |
|  | debt growth minus income growth | `DERIVED.DEBT_GROWTH_MINUS_INCOME_GROWTH` (−) |
|  | budget balance | `IMF.GGXCNL_NGDP` (+), `GC.NLD.TOTL.GD.ZS` \[S\] (+) |
|  | current account | `IMF.BCA_NGDPD` (+), `BN.CAB.XOKA.GD.ZS` \[S\] (+) |
|  | debt service | `GC.XPN.INTP.RV.ZS` (−), `GC.XPN.INTP.ZS` \[P\] (−) |
|  | foreign-currency debt share | `DT.DOD.DECT.GN.ZS` \[P\] (−), `DT.TDS.DECT.EX.ZS` \[P\] (−) |
|  | debt relative to assets | `IMF.GGXWDN_NGDP` \[P\] (−) (net debt) |
|  | credit rating | `RATING.SOVEREIGN` (+) (GAP) |
|  | debt held by foreigners | `DEBT.FOREIGN_HELD_SHARE` (−) (GAP) |
| `monetary_policy` | policy rate | `BIS.CBPOL.POLICY_RATE_END` (−), `BIS.CBPOL.POLICY_RATE_AVG` \[S\] (−) |
|  | real interest rate | `FR.INR.RINR` (−) |
|  | lending rate | `FR.INR.LEND` (−) |
|  | money growth | `FM.LBL.BMNY.ZG` (+) |
|  | inflation | `IMF.PCPIPCH` (+?), `FP.CPI.TOTL.ZG` \[S\] (+?) |

Remove `FI.RES.TOTL.MO` (no PDF basis).

### 12.5 Strength determinants: key eight (weights `heuristic`)

Weights: economic\_output 0.15, financial\_center 0.15, reserve\_currency 0.12, military 0.12, innovation\_and\_technology 0.12, trade 0.10, education 0.10, competitiveness 0.08 (sum 0.94, the author’s draft). The key-eight versus additional-measures split is 80/20 (`heuristic`). Each determinant is `{total, per_capita}` with 50/50 halves (`heuristic`) except reserve\_currency and competitiveness (total only). All alternatives are (+) (more is stronger). **Total** halves hold absolute quantities (shares of world); **per-capita** halves hold only ratios, rates, indices or `DERIVED.PER_CAPITA` quantities. No code may appear in both halves.

| Determinant | Total half | Per-capita half |
| --- | --- | --- |
| `economic_output` | `IMF.PPPGDP`, `NY.GDP.MKTP.PP.KD` \[S\]; `IMF.NGDPD` \[P\], `NY.GDP.MKTP.CD` \[P\] (nominal GDP is kept from the original design although the PDF text names only the PPP measure; flag for vetting) | `IMF.PPPPC`, `NY.GDP.PCAP.PP.KD` \[S\]; `IMF.NGDPDPC` \[P\], `NY.GDP.PCAP.CD` \[S of that\] |
| `financial_center` | `CM.MKT.LCAP.CD`, `CM.MKT.TRAD.CD`, `BIS.DEBT_SEC.OUTSTANDING`, `BIS.LBS.CROSSBORDER_CLAIMS`, `BIS.FX_TURNOVER_VALUE` | `CM.MKT.LCAP.GD.ZS`, `CM.MKT.TRNR`, `FS.AST.PRVT.GD.ZS` \[P\], `GFCI.SCORE` (GAP until fetched) |
| `reserve_currency` (total only) | `IMF.COFER_SHARE`, `BIS.FX_TURNOVER_SHARE`, `BIS.INTL_DEBT_CCY_SHARE` | n/a |
| `military` | `MS.MIL.XPND.CD`, `MS.MIL.TOTL.P1`, `FAS.NUCLEAR_STOCKPILE`, `GFP.POWERINDEX` (GAP, skipped by D4) | `DERIVED.PER_CAPITA.MS.MIL.XPND.CD`, `DERIVED.PER_CAPITA.MS.MIL.TOTL.P1` |
| `innovation_and_technology` | `IP.PAT.RESD`, `DERIVED.ABS.RD_SPEND_USD`, `DERIVED.ABS.RESEARCHERS`, `BX.GSR.ROYL.CD` \[P\], `VC.TOTAL_FUNDING` (GAP), `CSET.AI_OUTPUT` (GAP, skipped by D4) | `GB.XPD.RSDV.GD.ZS`, `SP.POP.SCIE.RD.P6`, `DERIVED.PER_CAPITA.IP.PAT.RESD`, `WIPO.GII_SCORE` (GAP until fetched), `BLOOMBERG.INNOVATION_INDEX` (GAP, paywalled) |
| `trade` | `BX.GSR.GNFS.CD`, `NE.EXP.GNFS.CD` \[S\]; `TX.VAL.MRCH.CD.WT` | `DERIVED.PER_CAPITA.BX.GSR.GNFS.CD` |
| `education` | `SE.SEC.ENRL`, `SE.PRM.ENRL`, tertiary enrolment count (use the WDI tertiary enrolment code if present, otherwise GAP), `ARWU.TOP500_COUNT` | `PISA_MEAN`, `BAR.SCHL.25UP` (with `HD.HCI.EYRS` \[P\]), `SE.TER.ENRR`, `SE.SEC.CUAT.UP.ZS`, `SE.XPD.TOTL.GD.ZS`, `ARWU.TOP100_COUNT` (as per-capita-eligible only after division by population: use `DERIVED.PER_CAPITA.ARWU.TOP100_COUNT`) |
| `competitiveness` (total only) | `@productivity_gauge.what_you_pay_vs_what_you_get` | n/a |

### 12.6 Additional measures and cycles

**`additional_measures` (weight 0.20 of strength; sub-weights `heuristic`):** infrastructure\_and\_investment 0.20, resource\_allocation\_efficiency 0.20, geology 0.20, character\_determination\_civility 0.15, governance\_rule\_of\_law 0.15, acts\_of\_nature 0.10. Equal component weights inside leaves.

| Leaf | Components |
| --- | --- |
| `infrastructure_and_investment` | `NE.GDI.FTOT.CD` / `NE.GDI.TOTL.CD` \[P\]; `NE.GDI.FTOT.ZS` / `IMF.NID_NGDP` \[P\]; `NY.GNS.ICTR.ZS` / `IMF.NGSD_NGDP` \[S\]; `LP.LPI.OVRL.XQ`; `INFRA.QUALITY_INDEX` (GAP); `OWID.ELECTRICITY_GEN_TWH` / `EIA.ELECTRICITY.GENERATION.BKWH` \[S\]; `IS.AIR.PSGR`; `IS.AIR.GOOD.MT.K1`; `IS.RRS.TOTL.KM`; `IS.RRS.GOOD.MT.K6`; `IS.SHP.GCNW.XQ` \[P\] (stand-in for shipping tonnage, #54); `IT.NET.USER.ZS` |
| `resource_allocation_efficiency` | `ILO.UNEMP_RATE_MODELLED` (−) / `SL.UEM.TOTL.ZS` \[S\] / `IMF.LUR` \[S\]; `ILO.UNEMP_LONGTERM_SHARE` (−); `DERIVED.DEBT_GROWTH_MINUS_INCOME_GROWTH` (−); `IC.CRED.ACC.LGL.RGHT.012.XD.DB1519.DFRN` \[P\] (ease of getting loans); `OECD.EPL` (−?) |
| `geology` | `AG.LND.TOTL.K2`; `EIA.ENERGY.PRODUCTION_QBTU` / `OWID.PRIMARY_ENERGY_TWH` \[P\]; `OWID.OIL_PRODUCTION_TWH`; `OWID.GAS_PRODUCTION_TWH`; `OWID.COAL_PRODUCTION_TWH`; `AG.PRD.CREL.MT` / `AG.PRD.FOOD.XD` \[P\]; `USGS.METALS_PRODUCTION` (GAP until fetched); `NY.GDP.TOTL.RT.ZS`; `EIA.ENERGY.SELF_SUFF` / `EG.IMP.CONS.ZS` \[P\]; the four `DERIVED.NET_EXPORTS_*_PCT_GDP`; `ER.H2O.INTR.K3` / `ER.H2O.INTR.PC` \[P\] |
| `character_determination_civility` | `HOURS_WORKED_WEEKLY`; `SL.TLF.CACT.MA.ZS` / `ILO.LFPR_MODELLED_MALE` \[S\]; `GC.XPN.TRFT.ZS` (−?); `OECD.EFFECTIVE_RETIREMENT_AGE`; `WVS.HARD_WORK_LEADS_TO_SUCCESS` (GAP); `WVS.SUCCESS_IMPORTANT` (GAP) |
| `governance_rule_of_law` | `RL.EST` \[S\]; `CC.EST` \[S\]; `GE.EST` \[P\]; `RQ.EST` \[P\]; `VDEM.v2x_rule` \[S\]; `VDEM.v2x_corr` \[P\] (−) |
| `acts_of_nature` | `NDGAIN.VULNERABILITY` \[P\] (−); `NDGAIN.READINESS` \[S\] (+); `COVID.EXCESS_MORTALITY_PSCORE` \[S\] (−) |

**`internal_order_cycle` (0.5 `wealth_values_gap`, 0.5 `internal_conflict`; `heuristic`)**

| Leaf | Components |
| --- | --- |
| `wealth_values_gap` | Gini: `SI.POV.GINI` (−), `PIP.GINI` \[S\] (−); `WID.TOP1_INCOME` (−), `WID.TOP10_INCOME` (−), `WID.TOP1_WEALTH` (−), `WID.TOP10_WEALTH` (−) (each with `SI.DST.10TH.10` \[P\] or `PIP.DECILE10` \[P\] as fallback only for the top-10 income component); `VDEM.v2xeg_eqdr` \[P\] (+); polarisation: `VDEM.v2cacamps` \[P\] (−), `VDEM.v2smpolsoc` \[P\] (−) |
| `internal_conflict` | protests: `VDEM.v2cagenmob` \[P\] (−), `VDEM.v2cademmob` \[P\] (−); political violence: `VDEM.v2x_clphy` \[P\] (+); `PV.EST` \[P\] (+); `UCDP.INTERNAL_CONFLICT_DEATHS` \[S\] (−); `ILO.STRIKES_DAYS_NOT_WORKED` (−); `ILO.STRIKES_COUNT` (−); `GDELT.PROTEST_INTENSITY` (−), `GDELT.GOVT_DISAPPROVAL` (−), `GDELT.DISCONTENT_TERMS` (−), `CLINE.CONFLICT_PER_CAPITA` (−) (GDELT and Cline are GAP until fetched) |

**`economic_financial_cycle`** (0.5 `debt_burden` = `@indebtedness_gauge`; 0.5 `expected_growth` = `@future_growth`; `heuristic`).

### 12.7 External order cycle

Single leaf (`heuristic`): `TM.TAX.MRCH.WM.AR.ZS` (−?) (tariff rate, #57) plus placeholders `DOTS.BILATERAL_TRADE_CONCENTRATION`, `UN.VOTING_DISTANCE`, `SANCTIONS.GSDB`, `UCDP.INTERSTATE` (all GAP, reason “pairwise major-power gauge; no country-level free source defined”). **Recorded target shape (not built):** a per-country exposure score relative to the USA and CHN (trade concentration with the two, UN voting distance from each, sanctions exposure, interstate conflict), computed for every ISO3.

## 13. The PDF’s 64-item source list and its target ledger status

The PDF introduces this list as “a selection” of the few hundred indicators behind its gauges. Target status after this phase: `live` (fetched and wired, exact), `substitute` (same concept from another provider), `proxy`, `gap` (reason required), `skipped` (D4), `pdf_listed_unwired` (fetched, no leaf uses it). “Attempt” means probe per §10 and record `gap` with the reason if no programmatic route works.

| # | Concept | PDF source | Our codes and target status |
| --- | --- | --- | --- |
| 1 | Access to essential health services | Social Progress Imperative | `SH.UHC.SRVS.CV.XD` substitute |
| 2 | Aerospace and defense sector returns | Thomson Reuters | gap (paywalled) |
| 3 | Agriculture, forestry, fishing value added | WDI | `NV.AGR.TOTL.ZS`, `NV.AGR.TOTL.CD` pdf\_listed\_unwired |
| 4 | AI: new companies, patents, citations | CSET | skipped; placeholder `CSET.AI_OUTPUT` |
| 5 | Banking claims by currency and source country | BIS | `BIS.LBS.CROSSBORDER_CLAIMS` attempt |
| 6 | Bloomberg Innovation Index | Bloomberg | gap (paywalled) |
| 7 | Book titles per capita | CLIO Infra | attempt (historical); else gap |
| 8 | Child mortality rate | Social Progress Imperative | `SH.DYN.MORT` substitute |
| 9 | Climate vulnerability and preparedness, subcomponents | ND-GAIN | all `NDGAIN.*` live; two used, rest pdf\_listed\_unwired |
| 10 | CO2 emissions | CLIO Infra | WDI `EN.GHG.CO2.*` substitute, unwired |
| 11 | Conflict intensity and count per capita | Cline Center | `CLINE.CONFLICT_PER_CAPITA` attempt |
| 12 | Construction permits score | WB Doing Business | `IC.CNST.PRMT.DFRN.DB*` live (stale) |
| 13 | Days lost to labor disputes | CLIO Infra | `ILO.STRIKES_DAYS_NOT_WORKED` substitute |
| 14 | Discontent, populism, conflict terms in media | GDELT | `GDELT.DISCONTENT_TERMS` attempt |
| 15 | Education spending | WDI | `SE.XPD.TOTL.GD.ZS` live |
| 16 | Educational attainment | Barro-Lee | `BAR.*` live (stale, ends 2010) |
| 17 | Electricity production | NBER CHAT | OWID and EIA substitute |
| 18 | Energy production and consumption | EIA | `EIA.*` live |
| 19 | Enforcing contracts score | WB Doing Business | `ENF.CONT.COEN.DB*` live (stale) |
| 20 | GINI coefficient | WDI | `SI.POV.GINI` live; `PIP.GINI` substitute |
| 21 | Google searches for political conflict terms | Google Trends | skipped |
| 22 | Google searches indicating discontent | Google Trends | skipped |
| 23 | Government disapproval, verbal opposition in news | GDELT | `GDELT.GOVT_DISAPPROVAL` attempt |
| 24 | Gross expenditure on R&D | WIPO GII | `GB.XPD.RSDV.GD.ZS` substitute |
| 25 | Historical aviation passengers and cargo | NBER CHAT | `IS.AIR.*` substitute |
| 26 | Historical technology adoption | NBER CHAT | attempt (historical); else gap |
| 27 | Homicide rate | Social Progress Imperative | `VC.IHR.PSRC.P5` substitute |
| 28 | Hours worked per week | World Inequality Database (see §11.5) | `ILO.HOURS_WEEKLY_MODELLED_MALE` live; ILO total substitute; PWT proxy |
| 29 | Human Capital Index | World Bank | `HD.HCI.OVRL`, `EYRS`, `HLOS` live |
| 30 | Income distributions | World Bank | `SI.DST.*` live; `PIP.DECILE1`, `PIP.DECILE10` substitute |
| 31 | Internal armed conflict | CLIO Infra | UCDP deaths via OWID substitute |
| 32 | Internet users | NBER CHAT | `IT.NET.USER.ZS` substitute |
| 33 | Male labor force participation | World Bank | `SL.TLF.CACT.MA.ZS` live |
| 34 | Metals and ores production | CLIO Infra | `USGS.METALS_PRODUCTION` attempt |
| 35 | Nuclear weapon stockpile | Federation of American Scientists | `FAS.NUCLEAR_STOCKPILE` live (via OWID, plus structural zeros) |
| 36 | Number of new patents | WIPO GII | `IP.PAT.RESD` substitute |
| 37 | Numeracy | CLIO Infra | attempt (historical); else gap |
| 38 | OTC FX turnover | BIS | `BIS.FX_TURNOVER_VALUE` attempt |
| 39 | Particulate matter pollution | Social Progress Imperative | `EN.ATM.PM25.MC.M3` substitute |
| 40 | Patent applications | WDI | `IP.PAT.RESD`, `IP.PAT.NRES` live |
| 41 | People displaced by conflict and violence | WDI | `UNHCR.DISPLACED_PCT_POP` substitute |
| 42 | People using safely managed drinking water | World Bank | `SH.H2O.SMDW.ZS` live |
| 43 | Prevalence of undernourishment | World Bank | `SN.ITK.DEFC.ZS` live |
| 44 | Protecting investors value score | WB Doing Business | `PROT.MINOR.INV.DFRN.DB*` live (stale) |
| 45 | Protest intensity index | GDELT | `GDELT.PROTEST_INTENSITY` attempt |
| 46 | Public sector corruption and theft | V-Dem | `VDEM.v2exthftps`, `VDEM.v2excrptps` live |
| 47 | Rail lines opened and rail cargo | NBER CHAT | `IS.RRS.*` substitute |
| 48 | Real GDP per worker | Penn World Table | `PWT.RGDPO_PER_WORKER` live |
| 49 | Real growth rate | World Bank | `NY.GDP.MKTP.KD.ZG`, `IMF.NGDP_RPCH` live |
| 50 | Renewable freshwater resources | WDI | `ER.H2O.INTR.K3`, `ER.H2O.INTR.PC` live |
| 51 | Rigorous and impartial government score | V-Dem | `VDEM.v2clrspct` live |
| 52 | Royalty and license fees | WIPO GII | `BM.GSR.ROYL.CD`, `BX.GSR.ROYL.CD` substitute |
| 53 | Share of universities in top 500 | Shanghai Ranking | `ARWU.TOP100_COUNT`, `ARWU.TOP500_COUNT` live after FD-03 |
| 54 | Shipping tonnage by sail, steam, motor and total | NBER CHAT | `IS.SHP.GCNW.XQ` proxy |
| 55 | Starting a business score | WB Doing Business | `IC.REG.DFRN.PC.DFRN` live (stale) |
| 56 | Suicide mortality rate | World Bank | `SH.STA.SUIC.P5` live |
| 57 | Tariff rate | World Bank | `TM.TAX.MRCH.WM.AR.ZS` live |
| 58 | Top 1% and top 10% income shares | World Inequality Database | `WID.TOP1_INCOME`, `WID.TOP10_INCOME` live after FD-06 |
| 59 | Top 1% and top 10% wealth shares | World Inequality Database | `WID.TOP1_WEALTH`, `WID.TOP10_WEALTH` live after FD-06 |
| 60 | Total labor disputes | CLIO Infra | `ILO.STRIKES_COUNT` substitute |
| 61 | Transparent laws with predictable enforcement | V-Dem | `VDEM.v2cltrnslw` live |
| 62 | Universities founded | CLIO Infra | attempt (historical); else gap |
| 63 | Number of new businesses, people creating businesses | WIPO GII | `IC.BUS.NREG`, `IC.BUS.NDNS.ZS` substitute |
| 64 | Number of new major websites | WIPO GII | `WIPO.NEW_MAJOR_WEBSITES` attempt |

**Non-list inputs named in §11 that also need ledger rows:** WVS items (eight codes), GEM (two), GFCI, vacation days, effective retirement age, employment protection (EPL), wages by education cohort, sovereign rating, foreign-held debt share, VC funding, infrastructure quality index, BIS international debt securities and FX share, WIPO GII score, OECD household savings, collective bargaining, minimum wage, IMF COFER, WHR social support and positive affect, COVID excess mortality. For each, the ledger shows source, codes, tier, countries covered and status as above.

## 14. Validation data from the PDF

These are the report’s own published figures (as of July 2024). They are sanity checks, not fitting targets (D24).

### 14.1 Headline Empire Scores (p. 135; print only)

Column reading inferred from a garbled header: Total, then Quality only / per capita.

| Entity | Total | Quality | Entity | Total | Quality |
| --- | --- | --- | --- | --- | --- |
| USA | 0.89 | 0.71 | AUS | 0.23 | 0.56 |
| CHN | 0.80 | 0.30 | TUR | 0.21 | 0.28 |
| EUR | 0.56 | 0.43 (not scoreable here) | CAN | 0.21 | 0.50 |
| DEU | 0.38 | 0.54 | CHE | 0.19 | 0.66 |
| JPN | 0.33 | 0.40 | BRA | 0.18 | 0.14 |
| KOR | 0.32 | 0.54 | NLD | 0.17 | 0.55 |
| IND | 0.30 | 0.07 | IDN | 0.17 | 0.13 |
| GBR | 0.29 | 0.46 | ITA | 0.17 | 0.31 |
| FRA | 0.27 | 0.45 | ESP | 0.17 | 0.34 |
| RUS | 0.26 | 0.28 | SAU | 0.15 | 0.45 |
| SGP | 0.24 | 0.89 | MEX | 0.14 | 0.15 |
| ZAF | 0.10 | 0.12 | ARG | 0.07 | 0.14 |

### 14.2 Ten-year growth decomposition (pp. 146–151, error-adjusted, % per year)

Columns: real GDP growth / working-age population growth / growth per worker / productivity impact / indebtedness impact.

IND 6.3/0.9/5.3/6.1/3.8; ARE 5.5/0.8/4.8/4.2/5.2; IDN 5.5/0.7/4.8/5.0/4.4; SAU 4.6/1.6/3.0/3.3/1.8; TUR 4.0/0.4/3.6/4.5/2.0; CHN 4.0/−0.4/4.3/5.0/3.1; POL 2.9/−0.6/3.5/2.7/4.3; ZAF 2.9/1.0/1.9/2.2/1.0; RUS 2.9/−0.3/3.1/3.2/2.7; COL 2.8/0.5/2.3/2.7/1.5; SGP 2.6/−0.2/2.8/3.7/1.2; MEX 2.5/0.8/1.7/1.9/1.4; CHL 2.4/0.2/2.2/2.5/1.6; SWE 2.3/0.4/1.9/2.3/1.3; AUS 2.1/0.6/1.5/1.0/2.5; ARG 2.0/0.8/1.2/2.1/−0.6; THA 1.9/−0.7/2.7/3.0/2.0; HUN 1.9/−0.5/2.4/2.4/1.9; CZE 1.9/0.0/1.8/1.2/2.1; KOR 1.8/−1.2/3.0/3.6/1.8; BRA 1.7/0.1/1.6/1.6/1.1; USA 1.4/0.2/1.2/1.6/0.4; GBR 1.3/0.4/0.9/1.6/−0.4; NLD 1.2/−0.2/1.4/1.5/1.3; JPN 1.2/−0.8/1.9/2.6/0.7; CAN 1.2/0.3/0.9/0.8/1.0; PRT 1.1/−0.8/1.9/1.5/2.6; BEL 0.9/−0.2/1.1/0.9/1.5; FRA 0.9/−0.1/1.0/1.1/0.9; NOR 0.8/0.1/0.6/0.2/1.4; ESP 0.3/−0.7/1.0/0.9/1.3; CHE 0.2/−0.3/0.5/0.4/1.0; GRC 0.0/−0.9/0.9/0.9/0.7; DEU −0.5/−1.1/0.6/1.1/−0.3; ITA −0.5/−1.1/0.6/0.3/1.1.

Identities visible in the table (for checking a later implementation): growth ≈ working-age growth + growth per worker (within about 0.1 pp); growth per worker ≈ 0.65 × productivity impact + 0.35 × indebtedness impact (within about 0.25 pp for 34 of 35). **Gate 4(b)** uses the working-age growth column at base year 2024.

### 14.3 Nominal GDP per capita used as the cost proxy (p. 161, US$ thousands; **gate 4(a)**)

IND 2.5; IDN 4.8; ZAF 6.2; THA 7.0; COL 7.9; BRA 10.0; ARG 10.4; CHN 12.7; MEX 14.8; TUR 14.8; RUS 14.9; CHL 16.3; POL 22.7; HUN 23.0; GRC 24.3; PRT 29.1; JPN 30.8; CZE 32.5; SAU 33.1; ESP 34.5; KOR 35.6; ITA 38.9; FRA 47.0; GBR 51.2; ARE 54.0; DEU 55.1; CAN 55.3; BEL 55.5; SWE 57.1; NLD 66.6; AUS 67.6; USA 83.1; NOR 87.4; SGP 91.6; CHE 101.1. Compare with `IMF.NGDPDPC` in 2024.

### 14.4 Qualitative checks stated in the PDF (printed, not gated)

- Health index: Japan and Australia rank highest and South Africa and India lowest among the countries the PDF reports (p. 136).
- Trade: China scores highest, then the Eurozone, then the US (p. 132; the Eurozone entity is not scoreable here, so check China above the US).
- Productivity impact on growth: India highest (6.1), Norway lowest (0.2). Indebtedness impact: UAE highest (5.2), Argentina lowest (−0.6). Culture impact: Singapore highest (+1.7), Italy lowest (−1.7).
- Cost of a quality-adjusted educated worker relative to USA (USA = 0%): India −92%, Norway +106%.

## 15. Drafting assumptions, clarifications and parked items

### 15.1 Assumptions made while drafting (the owner has not explicitly confirmed these; reviewable)

1. `YEAR_FLOOR = 1950` (the earliest year any source covers; PWT starts there).
2. `MIN_CROSS_SECTION = 30` and gate tolerances in §7.3 (10% median, 30% maximum, 90% within 0.3 pp) are starting values.
3. Tier and direction assignments in §12 are drafts; `direction_map.csv` and the effective-weight report exist so the owner can review them in one pass. V-Dem variable directions must be verified against the V-Dem codebook (the notation assumes higher = cleaner for `v2exthftps`, `v2excrptps`, `v2clrspct`, `v2cltrnslw`, higher = more corrupt for `v2x_pubcorr`, `v2x_execorr`, `v2x_corr`).
4. `future_growth` is a rank-scale composite (§6.4); conversion to percentage points awaits calibration.
5. The education and labor-productivity leaves use an interim decomposition (§12.3) until cohort wage data exist.
6. Nominal-GDP components in `economic_output` and `BX.GSR.ROYL.CD` in strength innovation are retained from the original design as `proxy_concept`; the owner may remove them in vetting.
7. Two WVS items (`HARD_WORK_LEADS_TO_SUCCESS`, `SUCCESS_IMPORTANT`) stand for the “attitudes to hard work and success” in the character leaf.
8. Historical CLIO Infra items are “attempt, else gap”.
9. PWT hours are `proxy_concept` (annual hours of all persons engaged versus weekly hours of men).
10. `weighted_cov_%` uses PPP GDP shares in place of ETF observation counts.
11. Look-ahead in forward-looking derived series is accepted for the MVP.
12. Directory layout under `data/dalitest/` and file names are the implementer’s choice within the pattern of §4.1.

### 15.2 Clarifications to §6 and §7

- **Primary alternative.** Alternatives with no data anywhere in the master (GAP or failed fetch) are skipped. The first remaining alternative is the primary and is always eligible; later alternatives must satisfy D16’s tier rule. A rank is valid only if `n_ranked ≥ MIN_CROSS_SECTION` for that (code, year); when the primary has no valid rank for a country-year, an eligible later alternative may supply it.
- **Duplicate-code gate.** “No code in two leaves of one gauge” applies to Health, Happiness and to the two halves of each strength determinant. `HOURS_WORKED_WEEKLY` and the shared constants `GDP_PC_COST` and `PISA_MEAN` are allowed to appear in several productivity leaves by design (the PDF reuses them).
- **Weight gate base.** The 15.45% ± 0.1% check uses nominal tree weights with every component counted as present (Appendix C).
- **Single-year series.** Series with one observation per country (COVID excess mortality) contribute only in that year; this is expected.
- **Aggregates.** If a provider returns an aggregate with an ISO3-like code that is not in `UNIVERSE` it is dropped silently with a count in the fetch summary.

### 15.3 Parked for the owner’s later sessions

Vetting each source and choosing replacements; dropping stale or sparse series; imputation; always including both raw and per-capita values when building gauges; z-score standardisation; income-adjusted and log transforms; cohort-wage cost model; back-test and error adjustment; minimum-coverage thresholds per gauge; implementation in the production pipeline; a country-level external-conflict gauge (§12.7).

## Appendix C. Effective-weight arithmetic for hours worked

Old code, as % of the productivity gauge, with the retirement-age and vacation placeholders absent (so hard work renormalised over two components): education leaf 0.70 × 0.25 ÷ 4 = 4.375; labor-productivity leaf 0.70 × 0.25 ÷ 3 = 5.833; working hard 0.70 × 0.25 × 2/3 = 11.667; savoring observed outcomes 0.30 × 1/6 × 0.5 = 2.5; self-sufficiency hard work 0.30 × 1/6 × 0.5 × 1/2 = 1.25. Total 25.6%, too high.

Target, all components present: working hard 11.667 + savoring 2.5 + self-sufficiency (hours are 50% of the 2.5 hard-work block) 1.25 = 15.42%. The PDF’s rounded figures give 15.45%. The gate band 15.45% ± 0.1% covers both.
