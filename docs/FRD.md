# Functional Design Record (FDR): Profile Metrics Expansion & Two-Tier Prep Telemetry

**Status:** Approved for Implementation  
**Audience:** Implementer / Agent with direct read/write access to `etfportfolio`  
**Supersedes/Extends:** Extends `silver_prep_spec.md` (which is already implemented) with structured execution telemetry and expanded fund expense / fee scalar metrics.

---

## 1. Executive Summary & Problem Context

The core pipeline from `silver_prep_spec.md` is fully functional:
- Micro-cleaned point-in-time rows are ingested into `silver.observations`.
- Monthly LOCF panels are densified with stored zeros for default-0 families and interpolated under p99/singleton caps for scalars in `silver.monthly_panel`.

However, two architectural gaps remain:

1. **Telemetry & Auditability:** The post-ingest phases (`prep/pipeline.py` and `prep/panel.py`) emit minimal console messages and lack diagnostic execution telemetry. Operators have no visibility into chunk timings, counts of staged observations, or granular reasons when observations (such as AUM with unresolved currencies) are omitted.
2. **Profile Ratios & Fee Friction Coverage:** `profile.json` payloads contain critical economic fee disclosures that are currently discarded: audited annual report expenses (gross expense, management fee, non-management fee); prospectus contractual fee ceilings (gross/net expense, gross/net management fees, fee waivers, 12b-1 distribution fees); and trading frictions (maximum and actual redemption charges).

This FDR settles the exact specifications for implementing the two-tier logging system and refactoring `extract_profile` to ingest these additional scalar metrics.

---

## 2. Settled Decision Log & Rationale

| Decision ID | Decision | Rationale |
|---|---|---|
| **D-TEL-1** | **Strict Two-Tier Logging Separation** | Maintain strict separation between human-facing narrative (`etfportfolio.console`, stdout) and diagnostic telemetry (`logging.getLogger(__name__)`, stderr + timestamped run log file). Keep console output uncluttered while providing a rich file audit trail. |
| **D-TEL-2** | **Granular Diagnostic Points** | In `pipeline.py`, record per-chunk execution times, staged observation counts, and explicit `DEBUG` reasons when rows are dropped. In `panel.py`, log spine product counts, date boundaries, row generation breakdowns (default-0 vs. scalar LOCF), and gap statistics. |
| **D-PROF-1** | **Single Family (`profile`) for Report Metrics** | Ingest all annual and prospectus report expense metrics under `family = 'profile'` using distinct prefixed metric names (e.g., `audited_*` and `prospectus_*`). Avoids introducing unnecessary schema tables or family splits while preventing PK collisions between audited backward actuals and forward prospectus limits. |
| **D-PROF-2** | **Closed Summary Fee Vocabulary** | Restrict report extraction to high-level, economically meaningful summary expense ratios. Silently ignore itemized, idiosyncratic accounting line items (`Audit Expenses`, `Postage and Printing`, `Custodian Expenses`, `Transfer Agent Reimbursement`). |
| **D-PROF-3** | **Symmetric Gross & Net Expense Breakdowns** | Extract both gross and net management fees, and both gross and net 12b-1 fees, via 1-to-1 dictionary lookups without conditional fallback shims. |
| **D-PROF-4** | **Unchanged `expenses_allocation` Representation** | Keep `expenses_allocation` as profile scalars (`management_expense_ratio`, `non_management_expense_ratio`) at depth 0 without bespoke micro-cleaning or simplex conversion. |
| **D-PROF-5** | **Redemption Charges as Percentage Scalars** | Extract `redemption_charge_max` and `redemption_charge_actual` via `parse_percentage`. Store valid `0.0` fees as observations (explicit 0% fee disclosure). |
| **D-PROF-6** | **Omit Static Fund Dates from Observations** | Do not ingest `Inception_Date` or `Maturity_Date` into `silver.observations`. Static contract metadata belongs in `bronze.contracts` (`issue_date`, `maturity`). `silver.observations` is strictly reserved for time-series rates, ratios, and composition weights with `value DOUBLE`. |
| **D-PROF-7** | **Modular Sub-Extractor Refactor** | Refactor `extract_profile` into private, dedicated helper functions in `prep/extractors.py` (`_extract_profile_expenses_allocation`, `_extract_profile_fund_tags`, `_extract_profile_reports`, `_extract_style_box_dimensions`). |
| **D-PANEL-1** | **Standard §6.3 Scalar Rules for New Features** | All new `profile_*` metrics interpolate as standard scalars on the product trading spine. No synthetic composites, no shims, and no cross-metric fallbacks. |

---

## 3. Telemetry Specification

### 3.1 Logger Channels & Responsibilities

- **`console = logging.getLogger("etfportfolio.console")`**:
  - Writes to `sys.stdout` via `TqdmLoggingHandler`.
  - Used only for top-level operational narrative: phase start banners, major step transitions, and final batch completion summaries.
  - Plain text, no timestamps, no diagnostic clutter.
- **`logger = logging.getLogger(__name__)`**:
  - Writes to `sys.stderr` (`INFO` with `--verbose`, `WARNING` by default) and the debug run log file in `data/logs/<timestamp>.log` (`DEBUG` level).
  - Records execution timings, intermediate record counts, warnings, and dropped row audit messages.

### 3.2 Phase 1: Observations Pipeline (`prep/pipeline.py`)

#### Start of Phase
- `console.info("=== Starting Silver Observations Extraction ===")`
- `logger.info("Queued %d pending snapshots for extraction (batch_size=%d, force=%s)", len(pending_snapshots), BATCH_SIZE, force)`

#### Per Chunk Batch (Inside Chunk Loop)
Measure chunk execution time using `time.perf_counter()`. After committing the chunk transaction:
```python
logger.info(
    "Processed chunk %d/%d (snapshots %d–%d) in %.2fs: %d staged observations, %d watermarked, %d empty, %d failed",
    chunk_idx,
    total_chunks,
    chunk[0][0],
    chunk[-1][0],
    chunk_elapsed,
    len(staged_observations),
    len(succeeded_snapshot_ids),
    chunk_empty_count,
    chunk_failed_count,
)
```

#### Dropped Row / Extraction Diagnostics (`DEBUG` & `ERROR`)
- When an AUM row cannot be extracted due to currency ambiguity (logged from `_extract_profile_fund_tags` in `prep/extractors.py`):
  ```python
  logger.debug(
      "Product %d (snapshot %d): Dropped total_net_assets_local due to unresolved currency (raw='%s', contract_currency='%s')",
      product_id, snapshot_id, raw_str, contract_currency
  )
  ```
- When an extractor encounters a malformed or failed payload:
  ```python
  logger.error(
      "Snapshot %d (product %d, endpoint '%s') failed extraction: %s",
      snapshot_id, product_id, ep_name, exc, exc_info=True
  )
  ```

#### End of Phase
- `console.info(f"Observations finished: {watermarked_total} watermarked, {failed_count} failed, {empty_count} empty.")`
- `logger.info("Observations phase completed in %.2fs. Total watermarked: %d, Total failed: %d, Total empty: %d", total_elapsed, watermarked_total, failed_count, empty_count)`

### 3.3 Phase 2: Monthly Panel Construction (`prep/panel.py`)

#### Stage Announcements & Telemetry
1. **Spine Bounds:**
   - `console.info("Building per-product trading spines from bronze.prices…")`
   - `logger.info("Resolved trading bounds for %d products spanning %s to %s (%d total product-months)", len(product_bounds), min_spine_date, max_spine_date, len(spine_rows))`
2. **Default-0 Sleeves:**
   - `console.info("Building default-0 family panels with stored zeros…")`
   - After building `panel_default0`:
     ```python
     n_def0 = conn.execute("SELECT COUNT(*) FROM panel_default0").fetchone()[0]
     logger.info("Constructed default-0 densified panel: %d long rows across %d families", n_def0, len(DEFAULT_ZERO_FAMILIES))
     ```
3. **Scalar LOCF & USD AUM:**
   - `console.info("Interpolating scalar families and computing USD AUM…")`
   - After computing `aum_usd_monthly`:
     ```python
     n_aum = conn.execute("SELECT COUNT(*) FROM aum_usd_monthly").fetchone()[0]
     logger.info("Converted and averaged %d product-months of USD AUM", n_aum)
     ```
   - After building `panel_scalar`:
     ```python
     n_scalar = conn.execute("SELECT COUNT(*) FROM panel_scalar").fetchone()[0]
     logger.info("Interpolated scalar panel: %d long rows", n_scalar)
     ```
4. **Completion Summary:**
   - `console.info(f"Wrote {n_rows} rows to silver.monthly_panel.")`
   - `logger.info("Monthly panel rebuild complete in %.2fs: %d rows written (%d default-0, %d scalar)", elapsed, n_rows, n_def0, n_scalar)`

---

## 4. Endpoints & Vocabulary Specification (`core/endpoints.py`)

### 4.1 Report & Redemption Metric Dictionaries

Define the closed mapping dictionaries directly in `etfportfolio/core/endpoints.py`:

```python
ANNUAL_REPORT_METRICS: dict[str, str] = {
    "Total Net Expense": "audited_net_expense_ratio",
    "Total Gross Expense": "audited_gross_expense_ratio",
    "Management Fees": "audited_management_fee_ratio",
    "Non-Management Expenses": "audited_non_management_fee_ratio",
}

PROSPECTUS_REPORT_METRICS: dict[str, str] = {
    "Prospectus Net Expense Ratio": "prospectus_net_expense_ratio",
    "Prospectus Gross Expense Ratio": "prospectus_gross_expense_ratio",
    "Prospectus Net Management Fee Ratio": "prospectus_net_management_fee_ratio",
    "Prospectus Gross Management Fee Ratio": "prospectus_gross_management_fee_ratio",
    "Prospectus Fee Waiver Ratio": "prospectus_fee_waiver_ratio",
    "Prospectus Net 12b-1 Fee Ratio": "prospectus_net_12b1_fee_ratio",
    "Prospectus Gross 12b-1 Fee": "prospectus_gross_12b1_fee_ratio",
}

FUND_PROFILE_REDEMPTION_METRICS: dict[str, str] = {
    "Redemption_Charge_Max": "redemption_charge_max",
    "Redemption Charge Max": "redemption_charge_max",
    "Redemption_Charge_Actual": "redemption_charge_actual",
    "Redemption Charge Actual": "redemption_charge_actual",
}
```

### 4.2 Updated `PROFILE_METRICS` Frozenset

Update `PROFILE_METRICS` in `core/endpoints.py` to include all 23 scalar metrics (11 base + 12 new):

```python
PROFILE_METRICS = frozenset(
    {
        # Core profile
        "total_expense_ratio",
        "total_net_assets_local",
        "is_passive",
        "manager_tenure_years",
        # Coverage scalars
        "top_10_weight",
        "theme_coverage",
        "esg_coverage",
        "mstar_coverage",
        # Expense allocation
        "management_expense_ratio",
        "non_management_expense_ratio",
        # Audited annual report (audited_net_expense_ratio preserved from base spec)
        "audited_net_expense_ratio",
        "audited_gross_expense_ratio",
        "audited_management_fee_ratio",
        "audited_non_management_fee_ratio",
        # Prospectus report
        "prospectus_net_expense_ratio",
        "prospectus_gross_expense_ratio",
        "prospectus_net_management_fee_ratio",
        "prospectus_gross_management_fee_ratio",
        "prospectus_fee_waiver_ratio",
        "prospectus_net_12b1_fee_ratio",
        "prospectus_gross_12b1_fee_ratio",
        # Trading frictions
        "redemption_charge_max",
        "redemption_charge_actual",
    }
)
```

---

## 5. Profile Extractor Refactoring Specification (`prep/extractors.py`)

### 5.1 Architecture

Refactor `extract_profile` into private, single-responsibility helper functions:

```python
logger = logging.getLogger(__name__)

def extract_profile(
    product_id: int,
    payload: dict[str, Any],
    fetched_at: datetime,
    date_ctx: DateContext | None = None,
    contract_currency: str | None = None,
    known_currencies: set[str] | None = None,
) -> list[Observation]:
    if not payload or not isinstance(payload, dict):
        return []

    if date_ctx is None:
        date_ctx = DateContext(fetched_at.date())

    observations: list[Observation] = []

    # 1. Expenses Allocation (depth 0)
    _extract_profile_expenses_allocation(
        observations, product_id, payload.get("expenses_allocation", []), fetched_at, date_ctx
    )

    # 2. Fund and Profile Tags (depth 0, except AUM at depth 3)
    _extract_profile_fund_tags(
        observations,
        product_id,
        payload.get("fund_and_profile", []),
        fetched_at,
        date_ctx,
        contract_currency,
        known_currencies,
    )

    # 3. Reports (depth 2)
    _extract_profile_reports(
        observations, product_id, payload.get("reports", []), fetched_at, date_ctx
    )

    # 4. Morningstar Style Box Dimensions
    mstar = payload.get("mstar")
    if isinstance(mstar, dict):
        observations.extend(_extract_style_box_dimensions(product_id, mstar, fetched_at, date_ctx))

    return observations
```

### 5.2 Private Helper Details

#### `_extract_profile_expenses_allocation`
- Iterate over `expenses_allocation` items.
- If `name == "Management Expenses"`: `metric = "management_expense_ratio"`, `value = float(ratio)`.
- If `name == "Non-Management Expenses"`: `metric = "non_management_expense_ratio"`, `value = float(ratio)`.
- Emit observation at `date_ctx.current` (depth 0).

#### `_extract_profile_fund_tags`
- Iterate over `fund_and_profile` items. Skip if `value is None`.
- **Total Expense Ratio:** `name_tag == "Total_Expense_Ratio" or name == "Total Expense Ratio"`: `parse_percentage(val)` → `total_expense_ratio`.
- **Management Approach:** `name_tag == "Management_Approach" or name == "Management Approach"`: Passive = 1.0, Active = 0.0 → `is_passive`. Skip tokens in `SKIP_RATING_TOKENS`.
- **Manager Tenure:** `name_tag == "Manager_Tenure" or name == "Manager Tenure"`: `parse_manager_tenure(val, date_ctx.current.effective_date)` → `manager_tenure_years`.
- **Total Net Assets (AUM):** `name_tag == "Total_Net_Assets_Month_End" or name.startswith("Total Net Assets")`:
  - `parse_net_assets(val)` → `(aum_val, raw_str, aum_date_str)`.
  - `disambiguate_aum_currency(...)` → ISO currency code.
  - If currency is `None`, log debug message:
    ```python
    logger.debug(
        "Product %d: Dropped total_net_assets_local due to unresolved currency (raw='%s', contract_currency='%s')",
        product_id, raw_str, contract_currency,
    )
    ```
    and omit row.
  - Else `with date_ctx.scope(aum_date_str, 3):` → emit `total_net_assets_local`.
- **Redemption Charges (New):**
  - Check `name_tag` or `name` in `FUND_PROFILE_REDEMPTION_METRICS`.
  - If matched, parse via `parse_percentage(val)`.
  - If parsed successfully: emit `Observation` with metric `redemption_charge_max` or `redemption_charge_actual` at depth 0. (Note: A parsed value of `0.0` is valid and must be emitted.)
- **Static Dates (New D-PROF-6 Rule):**
  - Explicitly skip `Inception_Date` and `Maturity_Date`. Do not emit observations for them.
- Skip all unparsed descriptive text fields (`Asset_Type`, `Classification`, `Domicile`, etc.).

#### `_extract_profile_reports` (New)
- Iterate over `reports` list.
- For each report:
  - `report_name = report.get("name")`
  - If `report_name == "Annual Report"`: `active_map = ANNUAL_REPORT_METRICS`.
  - Else if `report_name == "Prospectus Report"`: `active_map = PROSPECTUS_REPORT_METRICS`.
  - Else: skip unrecognized report types.
  - `with date_ctx.scope(report.get("as_of_date"), 2):`
    - Iterate over `report.get("fields", [])`:
      - `field_name = field.get("name")`
      - If `field_name in active_map`:
        - `metric = active_map[field_name]`
        - `parsed = parse_percentage(field.get("value"))`
        - If `parsed is not None`:
          - `val_float, raw_str = parsed`
          - Emit observation (`family="profile"`, `metric=metric`, `value=val_float`, `raw_value=raw_str`) at current depth 2 scope.
      - If `field_name not in active_map`: silently ignore (do not raise).

---

## 6. Panel Behavior (`prep/panel.py`)

No custom SQL branches are required for the new profile metrics. By virtue of being registered in `PROFILE_METRICS`:
1. They are extracted into `silver.observations` with `family = 'profile'`.
2. They are ingested into `scalar_obs` in `prep/panel.py` as: `feature_id = 'profile_' || metric`.
3. They are interpolated across the product trading spine using standard §6.3 scalar interpolation:
   - Singleton series (`n_gaps = 0`) paint the full spine.
   - Multi-observation series (`n_gaps >= 1`) apply the per-series p99 cap, with no pre-first fill.
   - Missing observations result in absent rows (no zero-fill).

### Generated `feature_id` Names in `silver.monthly_panel`
- `profile_audited_net_expense_ratio`
- `profile_audited_gross_expense_ratio`
- `profile_audited_management_fee_ratio`
- `profile_audited_non_management_fee_ratio`
- `profile_prospectus_net_expense_ratio`
- `profile_prospectus_gross_expense_ratio`
- `profile_prospectus_net_management_fee_ratio`
- `profile_prospectus_gross_management_fee_ratio`
- `profile_prospectus_fee_waiver_ratio`
- `profile_prospectus_net_12b1_fee_ratio`
- `profile_prospectus_gross_12b1_fee_ratio`
- `profile_redemption_charge_max`
- `profile_redemption_charge_actual`

---

## 7. Verification & Testing Requirements

### 7.1 Unit Tests to Add/Update

1. **`tests/core/test_endpoints.py` — `test_profile_metrics_membership`:**
   - Assert all 12 new metrics are present in `PROFILE_METRICS` (total length 23).
   - Assert all keys in `ANNUAL_REPORT_METRICS`, `PROSPECTUS_REPORT_METRICS`, and `FUND_PROFILE_REDEMPTION_METRICS` resolve to metrics in `PROFILE_METRICS`.
2. **`tests/prep/test_extractors.py` — `test_extract_profile_annual_report_metrics`:**
   - Verify Annual Report yields `audited_net_expense_ratio`, `audited_gross_expense_ratio`, `audited_management_fee_ratio`, `audited_non_management_fee_ratio` at depth 2 with exact decimal fractions.
3. **`tests/prep/test_extractors.py` — `test_extract_profile_prospectus_report_metrics`:**
   - Verify Prospectus Report yields gross/net management fees, gross/net 12b-1 fees, gross/net expense ratios, and fee waivers at depth 2.
4. **`tests/prep/test_extractors.py` — `test_extract_profile_redemption_charges`:**
   - Verify `"5%"` parses to `0.05` and `"0%"` parses to `0.0` at depth 0.
   - Verify missing/empty redemption charges produce no row.
5. **`tests/prep/test_extractors.py` — `test_extract_profile_ignores_itemized_accounting_fields_and_static_dates`:**
   - Verify itemized fields (`"Custodian Expenses"`, `"Misc. Expenses"`, `"Postage and Printing Expenses"`) do not raise and emit no rows.
   - Verify `"Inception_Date"` and `"Maturity_Date"` are ignored and emit no rows.
6. **`tests/prep/test_pipeline.py` — `test_pipeline_diagnostic_telemetry`:**
   - Verify pipeline executes with two-tier logging without breaking active progress bars.
   - Verify log file captures per-chunk execution timing and staged observation count.
7. **`tests/prep/test_panel.py` — `test_panel_interpolates_new_profile_scalars`:**
   - Seed observations with `profile_audited_gross_expense_ratio` and `profile_redemption_charge_max`.
   - Assert they appear as `profile_*` features in `silver.monthly_panel` under §6.3 scalar rules.

---

## 8. Implementation Sequence

1. **Endpoints (`core/endpoints.py`):**
   - Add `ANNUAL_REPORT_METRICS`, `PROSPECTUS_REPORT_METRICS`, `FUND_PROFILE_REDEMPTION_METRICS`.
   - Update `PROFILE_METRICS` with the new scalar metrics.
   - Update `tests/core/test_endpoints.py` to assert new constants and set members. Run pytest on core.
2. **Extractors (`prep/extractors.py`):**
   - Refactor `extract_profile` into private helpers (`_extract_profile_expenses_allocation`, `_extract_profile_fund_tags`, `_extract_profile_reports`, `_extract_style_box_dimensions`).
   - Add report matching and redemption charge parsing. Add debug log for unresolved AUM currency.
   - Add new extractor unit tests in `tests/prep/test_extractors.py`. Run pytest on prep extractors.
3. **Pipeline Telemetry (`prep/pipeline.py`):**
   - Instrument chunk timing with `time.perf_counter()`.
   - Add `logger.info` chunk summaries and `logger.debug` diagnostic traces.
   - Ensure existing tests pass.
4. **Panel Telemetry & Panel Tests (`prep/panel.py`):**
   - Instrument panel phase logging with `logger.info` (spine bounds, default-0 row count, scalar row count, USD AUM conversion count).
   - Add test in `tests/prep/test_panel.py` verifying the new profile features populate `silver.monthly_panel`.
5. **Full Suite Verification:**
   - Run `uv run pytest` to ensure complete green test suite across all packages.