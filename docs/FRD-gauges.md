# Functional Requirements Document: Dalio Gauge Emulation MVP

*Revision 2: raising the notebook’s fidelity to the Appendix*

## 1. Purpose, scope and how to use this document

**Purpose.** One Jupyter notebook ingests macro, governance, health and social indicators for every ISO3 country and groups them into “gauge trees” that emulate the gauges in the Appendix of The Great Powers Index 2024. This MVP exists to explore and define a bespoke gauge tree. It will later be re-implemented in a production pipeline that you cannot see, so keep it quick, lean and simple.

**Primary goal: three audit checks.**

1. **Completeness.** Every source or indicator the Appendix cites is fetched programmatically, or appears in a ledger as an explicit GAP with a reason.
2. **Correctness.** Gauge weights in the tree match the Appendix. Where the Appendix is silent, weights are kept as drafted and tagged as heuristics.
3. **Traceability.** Every tree component states why it is there (stated in the Appendix, concept named only, or heuristic) with a page reference, and every gauge the Appendix names can be read from a named node of the tree.

**Secondary goal.** A minimal engine that turns the tree plus the data into annual gauge series, so the three checks can be verified end to end.

**Out of scope (the owner’s later sessions; do not start them).** Vetting individual sources; dropping stale or sparse series; finding replacement indicators; imputation or interpolation; recalibrating weights; income-adjusted or log transforms; the growth back-test and error adjustment; integration into the production pipeline; writing to any database; any step that needs a hand-downloaded file. Section 8 adds specifics.

**Working principles.** (1) Correctness over coverage: never silently mix methodologies. (2) Flags over deletions: this phase labels stale, sparse or proxy data and never drops it. (3) Programmatic only. (4) MVP lean: one notebook, no tests, no db write-access.

**What this revision is for.** An audit of the notebook against the Appendix found that its structure is close (the Health, Happiness and Productivity weights already match) but that it is not yet auditable. 47 of its 189 leaf components have no data. 22 of the 64 sources the Appendix lists deliver nothing. 17 GAPs carry the reason ‘pending’ (never probed). The ledger omits about 20 indicators that appear in the Appendix’s weight tables. Six tree components have no counterpart in the Appendix. The Indebtedness gauge’s sub-weights are labelled ‘pdf’ although the Appendix calls them proprietary. This document specifies the changes that close those gaps. It does not ask you to improve data quality or make the gauges ‘better’. It asks you to make the notebook faithful and to make every deviation visible.

**How to use this document.** You have the notebook but not the Appendix PDF and not the conversation that produced this document. Appendix A reproduces every Appendix fact you need (weights, indicator lists, the source list, reference values) and is the ground truth for what the Appendix says. Appendix B lists starting points for probing GAPs. Appendix C lists ledger rows to add. Requirement IDs: C = completeness, K = correctness, T = traceability, G = gates and diagnostics. MUST means required for done. SHOULD means do it unless it costs more than it is worth, and say so in your report. Comments in the notebook that cite ‘FRD §…’, ‘D4’, ‘FD-01’ or ‘P-7’ refer to an earlier FRD you do not have; treat them as informational only.

## 2. Background you need

### 2.1 The Appendix

The Appendix is pages 132–184 of The Great Powers Index: 2024 (Ray Dalio). It contains: one-paragraph descriptions of 21 gauges (A.2); an Empire Score table for 24 major countries (A.8); how the Health and Happiness indices are built, with weights (A.3); a growth formula in which 10-year growth per worker is 65% productivity plus 35% indebtedness (A.4); weight tables for the productivity sub-gauges and for Culture (A.4, A.5); and a list of 64 data sources (A.7). Three properties shape this work.

1. **Weights are published for only some gauges:** Health, Happiness, Productivity (including Culture) and the 50/50 split inside Indebtedness. For every other gauge (the eight strength determinants, Infrastructure, Resource Allocation, Geology, Character, Governance, Acts of Nature, Internal Conflict, External Conflict, Gaps) the Appendix gives descriptions only. Weights there are the owner’s heuristics.
2. **The source list is a selection.** The Appendix says it shares “a selection” of “a few hundred” indicators. An indicator missing from the Appendix may exist in the full report, so ‘not in the Appendix’ means exactly that and nothing stronger. The rest of the report (pages 1–131) is not available.
3. **It contains small internal inconsistencies** (A.9). The resolution for each is specified there. Do not resolve them differently.

The Appendix uses its own three-letter country codes (BRZ is Brazil, CHI is Chile, TLD is Thailand). The notebook uses ISO3. The mapping is in A.10.

### 2.2 The notebook today

One file, 28 code cells, run top to bottom. All data sit in a long ‘master’ table with columns `country_code, indicator_code, year, value, source, is_projection` (`STD_COLS`).

| Cell | Role |
| --- | --- |
| 0 | Constants (`YEAR_FLOOR` 1950, `STALE_BEFORE` 2023, `MIN_CROSS_SECTION` 30, `ALLOW_PROXY_FALLBACK` False, `PROJ_FROM` 2026, `REFRESH` empty set), `STD_COLS`, helpers `log_fetch`, `cached`, `http_get`, `to_iso3`, `tidy`. Leave secret handling (the EIA key) exactly as it is |
| 1 | `UNIVERSE` (250 ISO3 codes from country\_converter). Coverage weights `OBS_WEIGHTS` and `TARGET_SET` come from a read-only DuckDB (`data/etf.duckdb`) and fall back to DALIO\_35 if the file is absent. Used for coverage reporting only |
| 2a–2r | One fetch cell per source, each wrapped in `cached()`: wb\_wdi (103 WDI codes), wb\_other (WGI, Doing Business archive, UHC, HCI, population projections), wb\_edstats, imf\_weo, imf\_cofer, eia, vdem, ndgain, owid, ilostat, bis, whr, unhcr, pip, oecd, arwu, pwt, probes (an empty placeholder) |
| 3 | `build_derived()`: 21 derived series (volatilities, per-capita values, net exports, dependency-ratio change, and so on) |
| 4 | Master assembly. Sources are concatenated in list order; on a duplicate (country, indicator, year) the first source wins |
| 5 | `RAY_DALIO_GAUGE_TREES`, the `Alt` tuple and `alt()` helper, `iter_leaves`, `check_tree_weights`, and the hours-worked weight assertion |
| 5b | `LEDGER_DATA` (the 64 listed sources plus extra rows), `CARRIED_SERIES`, `DERIVED_INPUTS`, `PDF_LISTED_CODES`, `KEEP_CODES` and the retention filter that trims series with no consumer from `df_master` |
| 6, 6b | `run_gauge_engine()` and its synthetic self-test |
| 7a | QA reports written as CSV to `data/dalitest/out`: df\_health, pdf\_ledger, df\_registry, alt\_coverage, effective\_weights, direction\_map, switch\_report, agreement\_report, proxy\_candidates |
| 7b | Structural asserts, Gates 1–4, and a printed Spearman diagnostic |

Mechanics you must understand before editing:

- **Tree schema.** Each top-level key is a tree. A node has `weight`, `provenance` (`pdf` or `heuristic`) and exactly one of: `children` (dict of nodes), `components` (list) or `reference` (a string such as `@productivity_gauge.what_you_pay_vs_what_you_get` that reuses another node’s score). A component is `{name, weight, alts}`. `alts` is an ordered list of `alt(code, tier, direction, status, reason)`. `direction` is +1 (higher is better) or -1. Extra keys on a component are ignored by the engine, so new metadata fields are safe.
- **Ten trees:** health\_index, happiness\_index, productivity\_gauge, indebtedness\_gauge, future\_growth (reference-only: 0.65 productivity, 0.35 indebtedness), strength\_determinants, additional\_measures, internal\_order\_cycle, economic\_financial\_cycle (reference-only) and external\_order\_cycle. Together they hold 189 leaf components: health 9, happiness 5, productivity 51, indebtedness 15, strength 45, additional 45, internal 14, external 5.
- **Alternatives and tiers.** For each component the engine takes the first alt that has any data anywhere as the primary (any tier). Later alts fill only country-years the primary leaves empty, and only if their tier is `exact` or `substitute_source` (`proxy_concept` is excluded unless `ALLOW_PROXY_FALLBACK`). `status=GAP` on an alt is a QA label only: the engine ignores it and skips any alt whose code has no data.
- **Scoring.** Within each year a series becomes a percent rank (0–100) across countries that have a value, provided at least 30 do; direction flips the ranking. Component scores are averaged with weights renormalised over the components present; node scores likewise. `weight_present` records the share of nominal weight actually present.
- **Caching.** `cached(source, fetch_fn, refresh)` stores `data/dalitest/raw/<source>/data.parquet`. If the file exists it is returned and the fetch does not run; if a refetch returns nothing, the old snapshot is kept. After editing any fetch cell you MUST add its source name to `REFRESH` (or delete its folder), otherwise your change has no effect.
- **Paired blocks in cell 7a.** The ledger, registry and effective-weights reports are each implemented twice (the second block is labelled ‘alternative?’). The later block overwrites the earlier one and writes the same CSV. Make your changes in the later block and delete the earlier one so they cannot diverge.
- **Saved outputs are unreliable.** The notebook’s saved cell outputs come from different runs and contradict each other (for example the WDI log says 103 series landed while Gate 1 says four WDI codes returned nothing). Trust only a fresh top-to-bottom run.

### 2.3 Baseline from the last saved run (re-measure first, see C-1)

- 189 leaf components; 47 have no data: 39 declared GAP and 8 dead (promised, fetch ran, nothing arrived).
- Of the 64 listed sources, 42 deliver data (26 as named, 15 through a substitute provider, 1 proxy) and 22 do not: 7 never wired (#2, 7, 21, 22, 26, 37, 62); 10 placeholders never fetched (#4, 5, 6, 11, 14, 23, 34, 38, 45, 64); 5 fetched but empty (#1, 3, 10, 58, 59).
- 23 further indicators the Appendix names are GAP placeholders (8 World Values Survey items, 2 GEM items, effective retirement age, vacation days, employment-protection rigidity, minimum-wage ratio, credit rating, foreign-held debt share, venture-capital funding, infrastructure quality, GFCI, WIPO GII score, wages by education cohort, BIS FX share, BIS debt-currency share). 3 more are promised but empty (WHR social support, WHR positive affect, COVID excess mortality).
- 14 dead alternatives (listed in C-2).
- Gates: G1 FAIL (7 regressions), G2 FAIL (14 dead alts), G3 PASS, G4(a) FAIL (Argentina deviates 30.3% against a 30% limit), G4(b) not evaluated (series never built), Spearman diagnostic −0.339.

Already correct and not to be changed: Health (9 components), Happiness (5) and Productivity including Culture (51) match the Appendix weights. K-1 locks that in.

## 3. Vocabulary

| Term | Meaning |
| --- | --- |
| Tier | Per alternative. `exact` = the Appendix’s own series. `substitute_source` = same concept from a different provider. `proxy_concept` = related concept, excluded from fallback by default |
| State | Computed per component from the data. `live` = the primary alt has data. `gap` = the first alt is a declared GAP that was never fetched. `dead` = promised, fetch ran, nothing arrived |
| Basis | New field on every tree component (K-2). `appendix` = the component and its weight are stated in an Appendix table. `appendix_concept` = the Appendix names the concept in a gauge description or prose but gives no indicator or weight. `heuristic` = the owner’s construction, the Appendix is silent |
| Provenance | Existing node field, `pdf` or `heuristic`. A component’s effective provenance is `pdf` only if its node is `pdf` and its basis is `appendix` |
| Ledger status | `live`, `substitute`, `proxy`, `gap`, `skipped`. A regression is a ledger row that promised data and received none |
| Appendix gauge | One of the 21 gauges listed in A.2 |
| The Appendix / the report | The Appendix is pages 132–184 of the report. The remainder of the report is not available |

## 4. Completeness requirements

**C-1 Re-baseline (MUST).** Restart the kernel and run all cells. Record the numbers in section 2.3 as the ‘before’ figures in your final report. If a number differs, believe the fresh run.

**C-2 Repair failing fetches (MUST).** For each row below: diagnose with a small probe, fix, add the source to `REFRESH`, and confirm rows arrive. If a row cannot be fixed, convert it to a GAP with a reason code (C-3) and record what you tried. Likely causes are hypotheses; verify them before relying on them.

| ID | Cell | Symptom | Likely cause | Required outcome |
| --- | --- | --- | --- | --- |
| C-2.1 | 2b | `SH.UHC.SRVS.CV.XD` (database 58) fails with KeyError `['indicator_code']` | wbgapi omits the series column when a single code is requested, so `tidy()` has no `indicator_code`. The WDI cell already handles this case | Rows exist for `SH.UHC.SRVS.CV.XD`. Log row counts per block so a silent failure in another block becomes visible |
| C-2.2 | 2a | Gate 1 reports no data for `NV.AGR.TOTL.ZS`, `NV.AGR.TOTL.CD`, `EN.GHG.CO2.MT.CE.AR5`, `EN.GHG.CO2.PC.CE.AR5` although the cell requests them | Unknown: invalid code, wrong World Bank database, or stale cache | Confirm in a fresh run. Land the data (from the right database if needed) or GAP. These serve ledger items #3 and #10 only and have no tree consumer |
| C-2.3 | 2i | The OWID slugs for WID top-1% and top-10% income and wealth shares return HTTP 404 (`WID.TOP1_INCOME`, `WID.TOP10_INCOME`, `WID.TOP1_WEALTH`, `WID.TOP10_WEALTH`) | Slugs changed or never existed | A working programmatic route (current OWID slugs, or the World Inequality Database bulk download, see Appendix B) using one methodology only: pre-tax national income shares and net personal wealth shares. Feeds four Gaps-in-Wealth components |
| C-2.4 | 2l | `WHR.POSITIVE_AFFECT` and `WHR.SOCIAL_SUPPORT` are empty; only `WHR.LIFE_LADDER` lands | The Figure 2.1 workbook holds the ladder score and ‘explained by’ columns (which the parser deliberately skips), not the raw affect and support series | Probe the WHR panel dataset that carries social support and positive affect (Appendix B). Also confirm every WHR row gets its real year: the parser silently assigns 2024 when a sheet has no year column |
| C-2.5 | 2i | COVID excess mortality fails with KeyError `Year` | The chart is daily or weekly, or its slug changed, and the high-frequency branch assumes columns that are absent | One value per country (mean P-score 2020-01-01 to 2022-12-31, stamped year 2022, as designed) or GAP |
| C-2.6 | 2i | `primary-energy-consumption` returns 404, so alt `OWID.PRIMARY_ENERGY_TWH` is dead | Slug changed | Fix the slug or mark the alt GAP |
| C-2.7 | 2f | `EIA.ENERGY.SELF_SUFF` is never built. The fetch asks for `length=5000` per product without paging and without filtering by activity or unit. 8,305 rows for 218 countries and two series is consistent with truncation and with mixed activities | Truncation and unfiltered facets | First verify the API’s facet names and IDs with one small request. Then page through all results, filter each series to one activity and one unit, add total primary energy consumption, and derive `EIA.ENERGY.SELF_SUFF` = production ÷ consumption in a single unit. Never mix activities or units in one series |
| C-2.8 | 2k, 3, 2b, 2j | Dead alts with no builder: `BIS.CBPOL.POLICY_RATE_AVG`, `DERIVED.CAPITAL_STOCK_GROWTH_PIM`, `ENF.CONT.COEN.DB0415` (an archive code that may be wrong), `ILO.UNEMP_LONGTERM_SHARE` (never fetched) | Never implemented | Build the first (annual mean of the policy-rate series already loaded in cell 2k). For the other three, build or fetch if trivial, otherwise mark the alt GAP with a reason code |
| C-2.9 | 3 | `DERIVED.WORKING_AGE_GROWTH_FWD10` and `_YOY` are never built (cell 5b warns; Gate 4(b) sees an empty series) | `SP.POP.1564.TO` (WDI) or `PROJ.SP.POP.1564.TO` (database 40) is missing | Diagnose which input is empty and repair it. The 2024 value needs 2034 from the projection block |

The 14 dead alternatives in the baseline are: `SH.UHC.SRVS.CV.XD` (health\_index/essential\_health\_services); `WHR.POSITIVE_AFFECT` and `WHR.SOCIAL_SUPPORT` (happiness\_index); `DERIVED.CAPITAL_STOCK_GROWTH_PIM` (investing/capital\_stock\_growth, second alt); `ENF.CONT.COEN.DB0415` (rule\_of\_law/enforcing\_contracts, second alt); `BIS.CBPOL.POLICY_RATE_AVG` (monetary\_policy/policy\_rate, second alt); `ILO.UNEMP_LONGTERM_SHARE` (resource\_allocation\_efficiency/longterm\_unemp\_share); `OWID.PRIMARY_ENERGY_TWH` (geology/energy\_production, second alt); `EIA.ENERGY.SELF_SUFF` (geology/energy\_self\_suff, first alt); `COVID.EXCESS_MORTALITY_PSCORE` (acts\_of\_nature); and the four WID codes (wealth\_values\_gap: top1\_income, top10\_income, top1\_wealth, top10\_wealth).

**C-3 GAP protocol and reason codes (MUST).** Every ledger row and every tree alt that has no data must either land data or be an explicit GAP carrying one code from the closed list below. The reason text begins with the code and then gives the evidence. The word ‘pending’ is no longer an acceptable reason.

| Code | Use when | Evidence required |
| --- | --- | --- |
| `PROPRIETARY` | Commercial or paywalled | Name the vendor |
| `NO_PUBLIC_ROUTE` | Published only as a PDF or web page, or no API exists | URL |
| `MANUAL_DOWNLOAD` | Needs registration, a form or a click-through | URL |
| `PROBED_FAILED` | You tried and it failed | URL and HTTP status or error |
| `HEAVY` | A route exists but needs more than one notebook (BigQuery billing, multi-GB bulk files) | Name the route and its size |
| `OWNER_SKIP` | Skipped by the owner in an earlier phase: CSET (#4), Google Trends (#21, #22), Global Firepower | None (not probed) |
| `NO_CONSUMER` | Cited only for historical analysis and feeds no 2024 gauge: #7, #26, #37, #62 | One line saying why |

Probe rule: for every GAP that is not obviously `PROPRIETARY`, `OWNER_SKIP` or `NO_CONSUMER`, make one bounded attempt (a few HTTP calls, about 15 minutes of effort). Appendix B lists unverified starting points. Anything that lands becomes a normal fetch cell, a ledger row and an honest tier. Anything that does not gets a coded reason. After the work, each ledger row’s `intended` status must match reality (for example #1 becomes live once C-2.1 works).

**C-4 Ledger rows for every Appendix-cited indicator (MUST).** The ledger holds the 64 listed sources plus extra rows, but it omits indicators that appear in the Appendix’s weight tables. Add one row for each entry W01–W20 in Appendix C, using the existing tuple `(item, concept, pdf_source, codes, tier, intended, reason)`. Rule going forward: every indicator in an Appendix weight table (A.3–A.5) or in the 64-source list has a ledger row. Indicators named only in the prose descriptions of gauges (A.2) are covered by tree components and basis tags (K-2) instead.

## 5. Correctness requirements

**K-1 Lock the weights that already match (MUST).** Add `APPENDIX_WEIGHTS` to cell 5: a dict mapping a tree path to the expected weights of that node’s children or components, as fractions normalised within the parent, populated from A.3–A.5. Extend Gate 3 to compare every entry with the tree within ±0.005 and to name the first mismatch. Use the component names as they appear in the tree; where a name differs from the concept names below, map it explicitly in the dict. Entries:

| Path | Expected weights (fraction of parent) |
| --- | --- |
| `health_index` | life expectancy 0.30; child mortality 0.10; air pollution 0.05; safe drinking water 0.05; basic sanitation 0.05; food insecurity 0.15; homicide 0.075; displaced by conflict 0.075; essential health services 0.15 |
| `happiness_index` | reported happy 0.225; daily enjoyment 0.225; life satisfaction 0.225; social support 0.225; suicide 0.10 |
| `future_growth` | productivity 0.65; indebtedness 0.35 |
| `productivity_gauge` | what\_you\_pay\_vs\_what\_you\_get 0.70; culture 0.30 |
| `what_you_pay_vs_what_you_get` | education leaf 0.25; labor-productivity leaf 0.25; working\_hard 0.25; investing 0.25 |
| `working_hard` | average hours 2/3; change in dependency ratio 1/3 |
| `investing` | investment % 3.1/17.5; savings rate 3.1/17.5; productivity of capital 3.1/17.5; capital-stock growth 3.1/17.5; income volatility 2.6/17.5; inflation volatility 2.6/17.5 |
| `culture` | six sub-gauges at 1/6 each |
| corruption | Transparency index 0.5; three survey items 1/6 each |
| bureaucracy | three items 1/3 each |
| rule\_of\_law | four items 1/4 each |
| savoring\_life\_vs\_achieving | average hours 0.5; seven values items 0.5/7 each |
| innovation | four input items 0.125 each; patents 0.125; royalty payments 0.125; four other output items 0.0625 each |
| self\_sufficiency | average hours 0.25; labor-force participation, effective retirement age, vacation days 1/12 each; transfer payments 0.125; government outlays 0.125; collective bargaining, hiring/firing, minimum wage 1/12 each |
| `indebtedness_gauge` | debt\_levels\_and\_flow 0.5; monetary\_policy 0.5 |

Documented exceptions, to leave as they are because the engine renormalises: `investing` components sum to 1.0057 (the Appendix prints 17.6 against 17.5), and `strength_determinants` children sum to 0.94 (heuristic, asserted in the notebook). Not covered by K-1: the components inside the two wage-based leaves, everything under Indebtedness below the 50/50 split, and every tree other than those above. Expected result today: all entries pass. K-1 exists so later edits cannot drift.

**K-2 Add a `basis` tag to every component and compute effective provenance from it (MUST).** Add to each component dict: `basis` (one of `appendix`, `appendix_concept`, `heuristic`), `page` (a string such as `p.158`; required when basis is `appendix`) and an optional `note`. Assign as follows.

- **health\_index and happiness\_index (14 components):** `appendix`, page `p.136`.
- **productivity\_gauge:** the `working_hard` and `investing` components are `appendix`, `p.158`. Culture components are `appendix` with the page of their table in A.5 (`p.171`, `p.173`, `p.175`, `p.177`, `p.179`, `p.181`). The components inside `education_cost_quality_adj` and `labor_productivity_cost_adj` are `heuristic`, with the note: stand-in for wage-by-education-cohort data, which the notebook cannot build; the Appendix (p.160) calls GDP per capita a simple measure of cost.
- **indebtedness\_gauge:** the ten `debt_levels_and_flow` components are `appendix_concept`. Seven are named on p.133 (`debt_rel_assets`, `budget_balance`, `current_account`, `debt_service`, `foreign_ccy_debt`, `foreign_held_debt`, `credit_rating`) and three come from the prose on pp.141–143 (`gov_debt_gdp`, `private_debt_gdp`, `debt_growth_minus_income`). The five `monetary_policy` components are `heuristic`: the Appendix names the pillar and its 17.5% weight (p.143) but not its contents.
- **strength\_determinants, additional\_measures, internal\_order\_cycle, external\_order\_cycle:** `appendix_concept` when the component matches a concept named in the A.2 description of its gauge or a source in the A.7 list; otherwise `heuristic`. These are definitely `heuristic`: the seven Infrastructure components `electricity_gen`, `air_passengers`, `air_freight`, `rail_lines`, `rail_freight`, `port_connectivity`, `internet_users` (their sources appear in A.7 as NBER CHAT series, but the Appendix assigns them to no gauge); `resource_allocation_efficiency/longterm_unemp_share`; and `external_order_cycle/un_voting_distance`.

Effective provenance of a component is `pdf` only if its node is `pdf` and its basis is `appendix`; otherwise `heuristic`. Replace the node-only provenance logic in the effective-weights code (cell 7a) with this rule. After the change the by-provenance summary must show Indebtedness as essentially all heuristic and Productivity as about 65% `pdf` and 35% `heuristic`. Also set the top-level `weight` of `additional_measures` from 0.20 to 1.0 so that effective weights sum to 1 within each tree (cosmetic; nothing else reads that value).

**K-3 Exclude components that have no Appendix counterpart (MUST).** Move the components below out of the scored tree into a new list `EXCLUDED_COMPONENTS` (dicts with `path`, `name`, `codes`, `reason`) in cell 5. Do not delete cached data or fetch code. Reset the remaining equal weights as shown.

| Path | Excluded component | Codes | Why | Remaining weights |
| --- | --- | --- | --- | --- |
| additional\_measures/governance\_rule\_of\_law | gov\_effectiveness\_wgi | WGI GE.EST | The Appendix defines this gauge as rule of law plus corruption only | 4 components at 0.25 |
| additional\_measures/governance\_rule\_of\_law | regulatory\_quality\_wgi | WGI RQ.EST | Same | (as above) |
| additional\_measures/geology | oil\_production, gas\_production, coal\_production | OWID energy series | The Appendix asks for total energy production, which `energy_production` already covers; these three double count | 11 components at 1/11 after K-4 |
| internal\_order\_cycle/internal\_conflict | political\_violence\_wgi | WGI PV.EST | No counterpart in the description (events, partisanship, survey discontent) | 6 components at 1/6 |

The WGI codes `GE.EST` and `RQ.EST` stay fetched because other components use them as proxies. Retention may trim series that no longer have a consumer.

**K-4 Wire net exports of industrial metals into Geology (MUST).** The Appendix asks for net exports in each category (energy, agriculture, industrial metals). The notebook builds `DERIVED.NET_EXPORTS_MMTL_PCT_GDP` but no tree component uses it. Add component `net_exports_metals` to `additional_measures/geology`, direction +1, tier `exact`, basis `appendix_concept`, page `p.134`. Weights follow K-3 (1/11 each).

**K-5 Annotate conflicts between heuristic weights and Appendix prose (MUST, annotation only).** Do not change any weight. Add a `note` string to these nodes so the owner sees the conflict when recalibrating:

- `strength_determinants/trade`: Appendix describes absolute exports as a share of world only; the tree splits total and per-capita 50/50.
- `strength_determinants/military`: Appendix says the gauge is driven mostly by absolute share; tree is 50/50.
- `strength_determinants/economic_output`: Appendix says primarily GDP levels with some weight on GDP per capita; tree is 50/50.
- `strength_determinants`: children sum to 0.94 by design; Appendix gives no weights for these eight gauges.
- `indebtedness_gauge`: Appendix publishes only the 50/50 split; all sub-weights are equal splits.

**K-6 Housekeeping (SHOULD).** Delete the duplicated first blocks in cell 7a (see 2.2). Keep one implementation per report.

## 6. Traceability requirements

**T-1 Gauge map (MUST).** Add `APPENDIX_GAUGE_MAP` to cell 5: the 21 Appendix gauges mapped to node paths, so each can be read directly from `df_node_scores`.

| # | Appendix gauge | Node path |
| --- | --- | --- |
| 1 | Education | strength\_determinants/education |
| 2 | Innovation & Technology | strength\_determinants/innovation\_and\_technology |
| 3 | Cost Competitiveness | strength\_determinants/competitiveness |
| 4 | Infrastructure & Investment | additional\_measures/infrastructure\_and\_investment |
| 5 | Economic Output | strength\_determinants/economic\_output |
| 6 | Productivity/Output Growth | future\_growth |
| 7 | Trade | strength\_determinants/trade |
| 8 | Military Strength | strength\_determinants/military |
| 9 | Financial Markets and Financial Center | strength\_determinants/financial\_center |
| 10 | Reserve Status | strength\_determinants/reserve\_currency |
| 11 | Indebtedness (Debt Cycles) | indebtedness\_gauge |
| 12 | Internal Conflict | internal\_order\_cycle/internal\_conflict |
| 13 | Rule of Law/Governance | additional\_measures/governance\_rule\_of\_law |
| 14 | Gaps in Wealth, Opportunity & Values | internal\_order\_cycle/wealth\_values\_gap |
| 15 | Geology | additional\_measures/geology |
| 16 | Character/Civility/Determination | additional\_measures/character\_determination\_civility |
| 17 | Resource Allocation Efficiency | additional\_measures/resource\_allocation\_efficiency |
| 18 | Acts of Nature | additional\_measures/acts\_of\_nature |
| 19 | External Conflict | external\_order\_cycle |
| 20 | Health Index | health\_index |
| 21 | Happiness Index | happiness\_index |

If a node name in the tree differs from the path above, correct the map to the tree’s real name; do not rename tree nodes. The Productivity gauge of Part 1 of the growth formula is `productivity_gauge`.

**T-2 Effective-weights output (MUST).** `effective_weights.csv` gains the columns `basis`, `page`, `state` and `provenance_effective`. The summary printed at the end of the notebook shows, per tree, the share of nominal weight by state (live, gap, dead) and by basis.

## 7. Gates and diagnostics

Cell 7b keeps its structural asserts and Gates 1–4, with the changes below, plus two additions.

| Gate | Check | Required end state |
| --- | --- | --- |
| G-1 Ledger | Statuses valid; zero regressions; every gap or skipped row carries an allowed reason code (C-3); every Appendix-cited indicator has a row (C-4) | PASS |
| G-2 Registry | No dead alt (a non-GAP alt with no data); no unconsumed series other than those flagged carried, derived-input or pdf-listed | PASS |
| G-3 Weights | Existing checks plus the K-1 `APPENDIX_WEIGHTS` comparison | PASS |
| G-4a | `IMF.NGDPDPC` 2024 against the Appendix p.161 values already embedded as `PDF_NGDPDPC_35`. Fail if the median relative deviation exceeds 10%. List any country above 30% as a WARNING, not a failure. Change from before: the 30% maximum was a hard fail, and Argentina alone breaches it at 30.3%, most likely because the Appendix figure uses a different exchange-rate vintage | PASS |
| G-4b | `DERIVED.WORKING_AGE_GROWTH_FWD10` at 2024 against the p.148 values embedded as `PDF_WA_GROWTH_FWD10`: at least 90% of countries within 0.3 percentage points | PASS |
| G-5 Traceability | Every component has a valid `basis` (and `page` when basis is `appendix`); every path in `APPENDIX_GAUGE_MAP` exists in the node scores (thin coverage is a warning only); no tree component matches an `EXCLUDED_COMPONENTS` entry | PASS |
| G-6 Diagnostic | Spearman correlation between `strength_determinants` at year `ASOF_YEAR` and the Empire Score Total column (A.8), over the 23 countries present in both. Also print it restricted to countries with `weight_present` of at least 0.6 | No threshold |

**G-6 details.** Add constant `ASOF_YEAR = 2023` to cell 0 (the report is dated July 2024; 2023 is the latest year with broad actuals). The current diagnostic uses the maximum year in the scores, where only a few indicators exist and per-capita measures dominate. That is a likely reason it prints −0.339. The Appendix ranks countries against the major economies; the engine ranks against every country with data, so scores are comparable only in rank order, which is why Spearman is used. If the correlation is zero or negative after the as-of-year change, investigate: check the direction sign of every component in the strength tree against its meaning, and check whether the balance of total and per-capita components explains it. Report your findings. Do not change the engine, directions or weights to push the number up.

## 8. Out of scope and parked decisions

In addition to the list in section 1, do not:

- change the engine’s ranking, coalescing or renormalisation logic;
- change any heuristic weight (K-5 is annotation only);
- add indicators beyond Appendix C and the repairs in C-2;
- remove fetches for series that merely lack a consumer: the nine unconsumed series (WDI `SP.POP.GROW`, `SL.TLF.TOTL.IN`, `NY.GDP.MKTP.KD`, `NY.GDP.DEFL.KD.ZG`, `NE.GDI.FTOT.KD`; EdStats `BAR.NOED.25UP.ZS`, `BAR.PRM.CMPT.25UP.ZS`, `BAR.SEC.CMPT.25UP.ZS`, `BAR.TER.CMPT.25UP.ZS`), the oil, gas and coal series and WGI `PV.EST`. The retention filter handles them.

Parked for the owner (record in your final report; do not act):

1. Whether the seven unassigned Infrastructure components should stay (K-2 tags them heuristic).
2. Whether to replace the 50/50 total and per-capita splits for Trade, Military and Economic Output (K-5).
3. The wage-by-education-cohort gap that forces the two stand-in leaves (35% of the Productivity gauge).
4. The Barro-Lee completion series (`BAR.*CMPT*`) may fit the Education gauge’s ‘quantity of educated people’ half better than the enrolment series now used.
5. The Appendix’s income-adjusted scores (‘relative to countries with similar income’, pp.163–182) are not implemented.
6. Whether to rank against the 24 major economies as the Appendix appears to do.

## 9. Delivery

**Constraints.** Edit the notebook in place and keep it one notebook. Do not change `STD_COLS`, `cached()` semantics or the engine. No database writes (the DuckDB is read-only). No manual downloads. Network access is needed for C-2 and C-3. Keep the cell order; add new constants and dicts to the cell named in each requirement.

**Suggested order.** (1) C-1 baseline. (2) Tree edits: K-1 to K-5, T-1 (offline, quick). (3) Fetch repairs C-2.1 to C-2.9. (4) GAP probing C-3 and ledger rows C-4. (5) Gate edits G-1 to G-6 and the T-2 outputs. (6) Restart, run all, and confirm every gate.

**Definition of done.**

1. The notebook runs top to bottom after a kernel restart, with no errors.
2. G-1 to G-5 print PASS; G-6 prints its correlations.
3. Zero regressions and zero dead alternatives. Every GAP carries a code from C-3.
4. Every component carries `basis`; all 21 gauges resolve; the K-3 exclusions and the K-4 addition are in place.
5. The ledger holds the 64 listed sources, the existing extra rows and W01–W20, and each row’s `intended` status matches the data.
6. A final summary cell prints ledger status counts, components by state and by basis (share of nominal weight per tree) and every gate result.

**Final report to the owner (in chat).** Before and after figures for section 2.3; which fetches landed and which became GAPs, with reason codes; every tree change; the G-6 findings; the parked decisions of section 8 that need an answer; and any place you deviated from this document, with the reason.

## Appendix A. What the Appendix says (reference data)

This appendix reproduces every Appendix fact the requirements rely on. Page numbers are the printed page numbers of the report. Unless stated, percentages are as printed.

### A.1 Conventions

Two percentage conventions appear. Tables on pp.158 and 169 give shares of the Productivity gauge (so What-you-pay-vs-what-you-get totals 70% and Culture totals 30%). Tables on pp.171, 177, 179 and 181 give shares of Culture (each of the six Culture sub-gauges totals 16.7%). The Appendix states that all of this was computer generated with the same factors for every country and period, and that the underlying statistics for Indebtedness are proprietary (p.157).

### A.2 The 21 gauges (pp.132–134)

| # | Gauge | What the Appendix says it measures |
| --- | --- | --- |
| 1 | Education | Quantity of educated people at different levels and quality of education. Basic and higher education split about evenly. About half the weight on absolute quantity of educated people, about half on quality (higher-education rankings, test scores, average years of education) |
| 2 | Innovation & Technology | Inventiveness, technological advancement, entrepreneurship. About half the weight on absolute share of key innovation metrics (patents, researchers, R&D spending, venture-capital funding); half on external rankings and per-capita innovation measures |
| 3 | Cost Competitiveness | What one gets for what one pays: quality-adjusted and productivity-adjusted labor costs plus other productivity measures. Only a Total score is shown (p.135) |
| 4 | Infrastructure & Investment | Share of global investment; overall infrastructure quality; investment and savings as a share of GDP; logistics performance |
| 5 | Economic Output | GDP as a share of world, adjusted for price differences, primarily; some weight on GDP per capita |
| 6 | Productivity/Output Growth | Forward 10-year growth: two-thirds productivity metrics, one-third indebtedness metrics (prose). The weight table on p.143 prints 65% and 35%; use the table |
| 7 | Trade | Absolute exports as a share of the world. China highest, then the Eurozone and the US |
| 8 | Military Strength | Absolute share of military spending, personnel, nuclear weapons, external indices of military capability. Does not capture regional or technology-specific advantages or alliances |
| 9 | Financial Markets and Financial Center | Absolute transaction shares and market capitalisations; external indices of financial-center cities |
| 10 | Reserve Status | Share of transactions, debts and reserves denominated in the currency. Euro-area countries are not scored individually (dashes on p.135) |
| 11 | Indebtedness (Debt Cycles) | The six groups in A.6. Built without reserve-currency status |
| 12 | Internal Conflict | Actual conflict events (for example protests), political conflict (for example partisanship), general discontent (from surveys) |
| 13 | Rule of Law/Governance | Rule of law (mainly business surveys) plus corruption (external indices and business surveys) |
| 14 | Gaps in Wealth, Opportunity & Values | Wealth and income inequality (for example the top 1% against the rest) plus political conflict (for example the ideological split of the legislature) |
| 15 | Geology | Land size; value of natural resources; total production of energy, agriculture and industrial metals; net exports in each category (self-reliance); freshwater supply |
| 16 | Character/Civility/Determination | Surveys on attitudes to hard work and success; proxies for valuing self-sufficiency and work (size of government transfer payments, effective retirement age) |
| 17 | Resource Allocation Efficiency | Chronically high unemployment; whether debt growth generates commensurate income growth; external indices and surveys on labor-market rigidity and ease of getting loans |
| 18 | Acts of Nature | Expert assessments of future climate-change impact on GDP; external assessments of disaster preparedness; COVID outcomes. The author calls it so-so and low quality |
| 19 | External Conflict | Economic, political/cultural and military conflict between pairs of major countries; structural indicators for a baseline and timely indicators for escalations. Economic: bilateral trade, tariff rates, news on sanctions and trade wars |
| 20 | Health Index | A.3 |
| 21 | Happiness Index | A.3 |

### A.3 Health and Happiness (p.136)

| Health Index component | Weight |
| --- | --- |
| Life expectancy at birth | 30.0% |
| Child mortality (deaths per 1,000 live births) | 10.0% |
| Air pollution exposure | 5.0% |
| Share using safely managed drinking water | 5.0% |
| Share with basic sanitation | 5.0% |
| Share facing food insecurity | 15.0% |
| Homicide rate per 100,000 | 7.5% |
| Share displaced by conflict | 7.5% |
| Share with access to essential health services | 15.0% |

| Happiness Index component | Weight |
| --- | --- |
| Share reporting happy | 22.5% |
| Share reporting daily enjoyment | 22.5% |
| Reported life satisfaction (1–10) | 22.5% |
| Share reporting a good social support network | 22.5% |
| Suicide rate per 100,000 | 10.0% |

### A.4 Growth formula and the Productivity gauge (pp.143, 154–168)

Growth per worker over 10 years = Productivity 65.0% + Indebtedness 35.0%. Productivity splits into What-you-pay-vs-what-you-get 70% (45.5% of growth) and Culture 30% (19.5%). Indebtedness splits 50/50 into Debt levels and flow (17.5%) and Monetary policy (17.5%). The Appendix gives no components or weights below that split.

| What-you-pay-vs-what-you-get (p.158), share of Productivity gauge | Share | Indicators (share) |
| --- | --- | --- |
| Education | 17.5% | Cost of quality-adjusted educated worker relative to USA (17.5) |
| Labor productivity | 17.5% | Cost of productivity-adjusted educated worker (17.5) |
| Working hard | 17.5% | Average hours worked (11.7); change in dependency ratio, 10 years forward (5.8) |
| Investing | 17.5% | Investment % (3.1); savings rate (3.1); productivity of capital investments (3.1); growth in capital stock (3.1); income volatility (2.6); inflation volatility (2.6) |

How the Appendix builds them. Education cost: hourly cost of educated workers by cohort (college, high school, none), adjusted for hours actually worked (vacation, holidays) and for education quality using international tests, weighted by each cohort’s share of the population. Labor productivity: the same cohorts adjusted for output per hour instead of test quality. Working hard: average weekly hours of actual work by men in the labor force, plus shifts in the share of the population that works (dependency). Investing: non-residential fixed investment rate and household savings rate, with the other items above. Cost measure (pp.160–161): nominal GDP per capita against the developed-world average in log terms, described as a naive baseline that is combined with what a country offers.

Hours worked appears three times (11.7% here, 2.5% of the gauge through Savoring, 1.25% through Self-sufficiency), so its total effective weight is 15.45% of the Productivity gauge. The notebook asserts this.

### A.5 Culture (pp.169–181)

Culture is six equal sub-gauges (each 5.0% of the gauge, 16.7% of Culture). Shares below are of Culture.

| Sub-gauge (page) | Indicators and shares |
| --- | --- |
| Corruption (p.171) | Transparency index 8.3 (Transparency International). Surveys of corruption 8.3, being three items at 2.8 each: diversion of public funds; irregular payments and bribes; favoritism in decisions of government officials (World Economic Forum sub-indices) |
| Bureaucracy (p.173) | Starting a business (World Bank/IFC); construction permits (World Bank/IFC); burden of government regulation (WEF). One third each (5.6) |
| Rule of law (p.175) | Efficiency of legal framework in settling disputes (WEF); property rights (WEF); protecting investors (World Bank/IFC); enforcing contracts (World Bank/IFC). One quarter each (4.2) |
| Savoring life vs achieving (p.177) | Observed outcomes 8.3: average hours worked. Expressed values 8.3, being seven survey items at 1.2 each: children having responsibilities is important; growth is important for the future of a country; having a good time is important; importance of being successful; economic growth should be a priority over the environment; competition is not harmful to business; hard work leads to success |
| Innovation (p.179) | Inputs 8.3: gross expenditure on R\&D; researchers; fear of business failure; entrepreneurship prevalence. Outputs 8.3: number of new patents 2.1; royalty and license fees, payments 2.1; number of new businesses 1.0; percent of people creating new businesses 1.0; number of new major websites 1.0; new trademark creation 1.0 |
| Self-sufficiency (p.181) | Hard work 8.3: average hours worked 4.2; labor force participation 1.4; effective retirement age 1.4; actual vacation days 1.4. Government support 4.2: transfer payments to households 2.1; government outlays 2.1. Rigidity of labor market 4.2: collective bargaining as a percent of workforce 1.4; ease of hiring/firing index 1.4; minimum wage 1.4 |

### A.6 Indebtedness gauge description (p.133)

Six groups: (a) debt levels relative to asset levels; (b) the sizes of external and internal surpluses and deficits; (c) debt service costs relative to GDP; (d) debt in own currency against foreign currency; (e) debt held by citizens against foreigners; (f) credit rating. The growth-formula text (pp.141–143) adds: debt levels relative to incomes, debt growth relative to income growth, and monetary policy stance.

### A.7 The 64-source list (pp.183–184)

| # | Indicator | Source |
| --- | --- | --- |
| 1 | Access to essential health services | The Social Progress Imperative |
| 2 | Aerospace and defense sector returns | Thomson Reuters |
| 3 | Agriculture, forestry and fishing, value added | World Development Indicators (WDI) |
| 4 | Artificial intelligence: new companies, patents, citations | Center for Security and Emerging Technology |
| 5 | Banking claims in country (by currency and source country) | Bank for International Settlements (BIS) |
| 6 | Bloomberg Innovation Index | Bloomberg |
| 7 | Book titles per capita | CLIO Infra |
| 8 | Child mortality rate | Social Progress Imperative |
| 9 | Climate vulnerability and preparedness index and subcomponents | Notre Dame Global Adaptation Initiative |
| 10 | CO2 emissions | CLIO Infra |
| 11 | Conflict intensity and count per capita | Cline Center |
| 12 | Construction permits score | World Bank Doing Business |
| 13 | Days lost to labor disputes | CLIO Infra |
| 14 | Discontent, populism and conflict terms in media | GDELT Project |
| 15 | Education spending | WDI |
| 16 | Educational attainment | Barro-Lee |
| 17 | Electricity production | NBER CHAT |
| 18 | Energy production and consumption | US Energy Information Administration |
| 19 | Enforcing contracts score | Doing Business |
| 20 | GINI coefficient | WDI |
| 21 | Google searches for political conflict terms | Google Trends |
| 22 | Google searches indicating economic, political, social or emotional discontent | Google Trends |
| 23 | Government disapproval and verbal opposition in news and media | GDELT |
| 24 | Gross expenditure on R&D | WIPO Global Innovation Index (GII) |
| 25 | Historical aviation passengers and cargo | NBER CHAT |
| 26 | Historical technology adoption (milking machine to internet) | NBER CHAT |
| 27 | Homicide rate | Social Progress Imperative |
| 28 | Hours worked per week | World Inequality Database (WID) |
| 29 | Human capital index | World Bank |
| 30 | Income distributions | World Bank |
| 31 | Internal armed conflict | CLIO Infra |
| 32 | Internet users | NBER CHAT |
| 33 | Male labor force participation rate | World Bank |
| 34 | Metals and ores production | CLIO Infra |
| 35 | Nuclear weapon stockpile | Federation of American Scientists |
| 36 | Number of new patents | WIPO GII |
| 37 | Numeracy | CLIO Infra |
| 38 | OTC FX turnover | BIS |
| 39 | Particulate matter pollution | Social Progress Imperative |
| 40 | Patent applications | WDI |
| 41 | People displaced by conflict and violence | WDI |
| 42 | People using safely managed drinking water services | World Bank |
| 43 | Prevalence of undernourishment | World Bank |
| 44 | Protecting investors value score | Doing Business |
| 45 | Protest intensity index | GDELT |
| 46 | Public sector corruption and theft score | V-Dem |
| 47 | Rail lines opened and rail cargo | NBER CHAT |
| 48 | Real GDP per worker | Penn World Tables |
| 49 | Real growth rate | World Bank |
| 50 | Renewable freshwater resources | WDI |
| 51 | Rigorous and impartial government score | V-Dem |
| 52 | Royalty and license fees | WIPO GII |
| 53 | Share of universities in top 500 | Shanghai Ranking Consultancy |
| 54 | Shipping tonnage by sail, steam, motor and in total | NBER CHAT |
| 55 | Starting a business score | Doing Business |
| 56 | Suicide mortality rate | World Bank |
| 57 | Tariff rate | World Bank |
| 58 | Top 1% and top 10% income shares | WID |
| 59 | Top 1% and top 10% wealth shares | WID |
| 60 | Total labor disputes | CLIO Infra |
| 61 | Transparent laws with predictable enforcement score | V-Dem |
| 62 | Universities founded | CLIO Infra |
| 63 | Number of new businesses and number of people creating new businesses | WIPO GII |
| 64 | Number of new major websites | WIPO GII |

The notebook’s ledger uses these numbers as row IDs. The notebook already substitutes providers for some rows (for example ILOSTAT for #28 and #13, UNHCR for #41, OWID series for #17, #31 and others); substitute rows keep their tier `substitute_source` and are not errors.

### A.8 Empire Score table (p.135), as of July 2024

Total uses all underlying measures; Quality-only uses per-capita and quality measures. Scores are weighted-average percent ranks (0 = worst on every measure, 1 = best). The Appendix does not state the set of countries ranked against. EUR is the Eurozone and has no ISO3 code; the notebook’s check uses the 23 others.

| Country | Total | Quality-only | Country | Total | Quality-only |
| --- | --- | --- | --- | --- | --- |
| USA | 0.89 | 0.71 | CAN | 0.21 | 0.50 |
| CHN | 0.80 | 0.30 | CHE | 0.19 | 0.66 |
| EUR | 0.56 | 0.43 | BRZ | 0.18 | 0.14 |
| DEU | 0.38 | 0.54 | NLD | 0.17 | 0.55 |
| JPN | 0.33 | 0.40 | IDR | 0.17 | 0.13 |
| KOR | 0.32 | 0.54 | ITA | 0.17 | 0.31 |
| IND | 0.30 | 0.07 | ESP | 0.17 | 0.34 |
| GBR | 0.29 | 0.46 | SAR | 0.15 | 0.45 |
| FRA | 0.27 | 0.45 | MEX | 0.14 | 0.15 |
| RUS | 0.26 | 0.28 | SAF | 0.10 | 0.12 |
| SGP | 0.24 | 0.89 | ARG | 0.07 | 0.14 |
| AUS | 0.23 | 0.56 | TUR | 0.21 | 0.28 |

### A.9 Appendix inconsistencies and how to resolve them

1. **Growth split.** Prose on pp.132 and 153 says two-thirds and one-third; the table on p.143 says 65% and 35%. Use 0.65 and 0.35.
2. **Investing weights** add to 17.6 against a stated 17.5. Keep the printed ratios; the engine renormalises.
3. **Innovation inputs** are printed as four items at 1.0 each under an 8.3 subtotal (sum 4.0). Treat them as four equal items, 0.125 of Innovation each. Outputs add to 8.2 against 8.3 and are used as printed.
4. **Duplicate rows on p.158.** ‘Education’ and ‘Cost of quality-adjusted educated worker relative to USA’ are one measure; so are ‘Labor productivity’ and ‘Cost of a productivity-adjusted educated worker’. One tree leaf each.
5. **p.133 lettering** uses (d) twice; there are six groups (A.6).
6. **p.157** mentions ranking among ‘the 20 countries shown’ while 35 are shown elsewhere. Ignore it.
7. **Hours worked.** The source list (#28) names the World Inequality Database; p.166 describes average weekly hours of actual work by men in the labor force. The notebook uses ILO male weekly hours, a `substitute_source`.
8. **Royalty and license fees** on p.179 are payments, not receipts. Payments are the `exact` series; receipts are a proxy.

### A.10 Appendix country codes to ISO3

| Appendix | ISO3 | Appendix | ISO3 | Appendix | ISO3 |
| --- | --- | --- | --- | --- | --- |
| BRZ | BRA | IDR | IDN | SAR | SAU |
| SAF | ZAF | UAE | ARE | PLD | POL |
| TLD | THA | CHI | CHL | CZK | CZE |
| EUR | none (Eurozone) |  |  |  |  |

All other codes in the Appendix (USA, CHN, DEU, JPN, KOR, IND, GBR, FRA, RUS, SGP, AUS, TUR, CAN, CHE, NLD, ITA, ESP, MEX, ARG, COL, SWE, HUN, PRT, BEL, NOR, GRC) are already ISO3.

## Appendix B. Starting points for probing GAPs (unverified)

These are leads from general knowledge, not tested routes. Dataset names and URLs change; confirm each on the provider’s own site before relying on it. A lead that fails is recorded with `PROBED_FAILED` and the URL tried. Nothing here is a requirement to land the data.

| Item | Lead | Likely outcome |
| --- | --- | --- |
| #5 Banking claims by currency and source country | BIS bulk download host already used for credit and policy rates (`WS_TC`, `WS_CBPOL`). Look for the Locational Banking Statistics file (`WS_LBS_D_PUB`) on the BIS bulk download page | Likely lands |
| #38 OTC FX turnover; BIS FX currency share | BIS triennial survey dataset in the same bulk list (look for `WS_DER_OTC_TOV` or similar). Currency shares sum to 200% because each trade has two legs | Likely lands |
| BIS debt-currency share | International debt securities by currency, same bulk list (`WS_DEBT_SEC2_PUB`) | Likely lands |
| #58, #59 WID income and wealth shares | Current OWID chart slugs (search OWID’s catalogue); or the WID bulk download. WID variables to look for: pre-tax national income share (`sptinc`) and net personal wealth share (`shweal`), percentiles p99p100 and p90p100 | Probably lands |
| WHR social support and positive affect | The WHR panel dataset behind its Table 2.1 (country-year social support and positive affect), on the WHR data page | Probably lands |
| #11 Cline Center | Societal Conflict Event Dataset download on the Cline Center or Illinois Data Bank page | Unknown |
| #14, #23, #45 GDELT | The DOC 2.0 API only covers recent months. Full history needs BigQuery (billing) or large bulk event files | Likely `HEAVY` |
| #34 USGS metals and ores | USGS mineral commodity data releases (ScienceBase) hold country production by commodity. Needs a decision on which commodities count as industrial metals | `HEAVY` or lands |
| Effective retirement age; ease of hiring/firing (employment-protection rigidity) | OECD SDMX, the same API as cell 2o. Find the dataflows in the OECD dataflow catalogue. OECD-member coverage only | Probably lands, narrow coverage |
| Minimum-wage ratio | ILOSTAT statutory minimum wage against mean earnings, or an OECD minimum-to-average-wage dataflow | Unknown |
| ILO long-term unemployment share | ILOSTAT indicator catalogue (unemployment by duration) | Probably lands |
| Vacation days | ILO working-conditions laws database or OECD | Likely `NO_PUBLIC_ROUTE` |
| Foreign-held share of government debt | World Bank Quarterly Public Sector Debt; IMF investor-base data | Unknown |
| WVS items (8) and GEM items (2) | Both distribute data behind registration or click-through | Likely `MANUAL_DOWNLOAD` |
| WIPO GII score and #64 new major websites | GII data tables (spreadsheet) linked from the WIPO GII page; the file URL changes yearly | Unknown |
| Infrastructure quality index | The WEF Global Competitiveness Index 4.0 was last published in 2019; data are spreadsheets or PDF | `NO_PUBLIC_ROUTE` or `MANUAL_DOWNLOAD` |
| GFCI | Published as a PDF report | `NO_PUBLIC_ROUTE` |
| #2 Thomson Reuters; #6 Bloomberg; venture-capital funding; sovereign credit rating | Commercial products | `PROPRIETARY` |
| Wages by education cohort | No open cross-country source known. ILOSTAT wages by education and the OECD education-at-a-glance earnings tables cover part of it | `PROBED_FAILED` or partial; partial coverage is not a basis for rebuilding the two leaves in this phase |

## Appendix C. Ledger rows to add (C-4)

Add one ledger row for each entry using the existing tuple `(item, concept, pdf_source, codes, tier, intended, reason)`, with item IDs W01 to W20. Where the tree already holds a component for the indicator, take the codes from the tree; do not invent new codes. Where this table gives a code, check it against the tree first.

| ID | Appendix indicator | Page | Tree component and codes | Expected status |
| --- | --- | --- | --- | --- |
| W01 | Life expectancy at birth | p.136 | health\_index life-expectancy component; `SP.DYN.LE00.IN` | live |
| W02 | Basic sanitation access | p.136 | health\_index basic sanitation; `SH.STA.BASS.ZS`, fallback `SH.STA.SMSS.ZS` (proxy) | live |
| W03 | Reported life satisfaction (1–10) | p.136 | happiness\_index life satisfaction; `WHR.LIFE_LADDER`, fallback `WHR.CANTRIL_LADDER` | live, substitute\_source (the Appendix names no survey provider) |
| W04 | Change in dependency ratio, 10 years forward | p.158 | working\_hard dependency component; `DERIVED.DEPENDENCY_RATIO_CHG_FWD10` | live |
| W05 | Investment % (non-residential fixed investment) | p.158 | investing investment component; as in tree (`NE.GDI.FTOT.ZS`) | live |
| W06 | Income volatility | p.158 | investing; derived series built in cell 3 | live |
| W07 | Inflation volatility | p.158 | investing; derived series built in cell 3 | live |
| W08 | Productivity of capital investments | p.158 | investing; derived series built in cell 3 | live |
| W09 | Growth in capital stock | p.158 | investing; `PWT.RKNA_GROWTH` (second alt per C-2.8) | live |
| W10 | Transparency index | p.171 | corruption; `TI.CPI` | live |
| W11 | WEF: diversion of public funds | p.171 | corruption survey item; `VDEM.v2exthftps` | substitute\_source (V-Dem is the source named in list item #46) |
| W12 | WEF: irregular payments and bribes | p.171 | corruption survey item; `VDEM.v2excrptps` | substitute\_source (#46) |
| W13 | WEF: favoritism in decisions of government officials | p.171 | corruption survey item; `VDEM.v2clrspct` | substitute\_source (#51) |
| W14 | WEF: burden of government regulation | p.173 | bureaucracy; WGI `RQ.EST` | proxy |
| W15 | WEF: efficiency of legal framework in settling disputes | p.175 | rule\_of\_law; `VDEM.v2cltrnslw` | substitute\_source (#61) |
| W16 | WEF: property rights | p.175 | rule\_of\_law; `VDEM.v2xcl_prpty`, fallback WGI `RL.EST` | proxy |
| W17 | Researchers | p.179 | innovation inputs; as in tree (`SP.POP.SCIE.RD.P6`) | live |
| W18 | New trademark creation | p.179 | innovation outputs; as in tree (`IP.TMK.RSCT`) | live |
| W19 | Transfer payments to households | p.181 | self\_sufficiency government support; as in tree | live |
| W20 | Government outlays | p.181 | self\_sufficiency government support; as in tree | live |

Status words follow the ledger’s own vocabulary. If a row’s data turn out to be missing after the repairs in C-2, change its `intended` status to `gap` with an allowed reason code rather than leaving it as live.
