# Functional Requirements Document (FRD): Resilient Historical Price Ingestion & Overlap Validation

**Status:** Approved for Implementation  
**Target Modules:** `etfportfolio/ingestion/prices.py`, `scripts/restore_truncated_prices.py`, `tests/`  
**Layer:** Bronze & Cold Storage (Medallion Architecture)  
**Author:** Quantitative Architecture & Engineering  
**Date:** September 2026  

---

## 1. Executive Summary & Problem Context

### 1.1 Incremental Price Fetch Architecture
The pipeline ingests daily historical OHLCV prices across 22,500+ global ETFs from Interactive Brokers (IBKR Gateway `clientId=2`) into DuckDB (`bronze.prices`). When corporate actions or structural discontinuities occur, older series are archived into `cold_storage.prices`.

Incremental updates proceed as follows:
1. **Initial Fill:** Fetches a 30-year baseline (`duration="30 Y"`).
2. **Incremental Update:** For products with existing history ending on $t_{\text{last}}$, fetches the elapsed window plus a 7-day overlap window $W = [t_{\text{last}} - 7\text{d}, t_{\text{last}}]$ and a 2-day margin.
3. **Overlap Validation:** Validates incoming bars in $W$ against stored bronze bars using trading date equality and floating-point value checks.
   - **Pass:** The overlap window and incremental tail ($t \ge t_{\text{overlap\_start}}$) are upserted into `bronze.prices` via `upsert_series`.
   - **Fail (Mismatch):** Refetches complete 30-year history (`"30 Y"`), archives existing bronze rows to `cold_storage.prices`, and replaces `bronze.prices`.

### 1.2 The Production Incident
During the pipeline run on September 12, 2026 (7 days after baseline fill), 5,501 products were processed:
- **4,897 products (89.0%)** succeeded with incremental updates.
- **604 products (11.0%)** failed overlap validation and triggered full 30-year refetches:
  - **601 products** failed due to `value_mismatch`.
  - **3 products** failed due to `date_mismatch` (`229325937`, `236798101`, `332383610`).

An 11% refetch rate severely exhausts IBKR pacing limits, bloats runtime by dozens of hours, and introduces severe data corruption risks.

### 1.3 Empirical Findings & Root Causes

Audit of `cold_storage.prices` vs. replacement series revealed four distinct failure modes:

1. **Volume & VWAP Telemetry Drift (245 products / 40.8% of refetches):**  
   `PRICES_SPEC.value_columns` included `volume` and `average` alongside OHLC prices. Post-market Form T prints, off-exchange ADF clearing, and weekend trade bust reconciliations routinely adjust Friday volume by a few dozen shares on Monday. Testing volume with a strict $10^{-4}$ tolerance tripped `value_mismatch` even when $O, H, L, C$ were 100% identical across 30 years.
2. **Seam-Date Closing Cross / Auction Revisions (178 products / 29.6% of refetches):**  
   Discrepancies existed **only on the final seam date** ($t_{\text{last}}$) and were typically $\le \$0.02$ on `close` or `high` (e.g. continuous close updated to official auction settlement). With `rel_tol=1e-4, abs_tol=1e-4`, a 1-cent shift on a \$50 ETF equals $2.0 \times 10^{-4} > 10^{-4}$, treating normal primary auction settlement as corruption.
3. **Catastrophic Asymmetric Truncation (3 products / critical data loss):**  
   For products `229325937` (`RFDI`), `236798101` (`RFEM`), and `332383610` (`EASG`), a transient IBKR Gateway response returned an incomplete incremental bar list missing window $W$, triggering `date_mismatch`. During the subsequent 30-year refetch, IBKR returned only a single dummy bar (1 day, 0 volume). Because `len(full_bars) > 0`, the engine wiped out 2,000–2,600 historical bars from `bronze.prices`, replaced them with the 1 dummy bar, and marked `status="ok"`.
4. **Legitimate Corporate Actions (173 products / 28.8% of refetches):**  
   The pipeline requests `WHAT_TO_SHOW = "ADJUSTED_LAST"`. Dividends and splits cause IBKR to retroactively scale past prices by a uniform factor $R$ across all historical bars. For these products, **full refetch and archive was necessary and correct**.

---

## 2. Design Principles & Decisions

1. **Simplicity:** No speculative abstractions or extra metadata layers. Module-specific logic stays inside the module.
2. **Replacement Over Deprecation:** Obsolete composite test files and outdated docstrings are deleted and replaced cleanly.
3. **Preserve Validation Signature:** `validate_overlap` preserves the 2-tuple return contract `tuple[bool, str | None]` (`is_valid, mismatch_type`). When a benign seam revision is absorbed, it returns `(True, None)` and logs an `INFO` notice. Downstream `upsert_series` already writes points $\ge \text{overlap\_start}$, automatically updating the seam bar.
4. **Defensive Anti-Truncation Guard:** Refetch replacements that lose $> 10\%$ of historical data are rejected, preserving bronze rows and setting status to `'error'`. The guard remains strictly active even under `--force`.
5. **Standalone Migration Script:** The 3 corrupted products are restored via a standalone script in `scripts/` with idempotent pre-flight checks, keeping one-off repair logic out of runtime pipeline modules.
6. **Clean Domain Separation in Tests:** Tests mirror source modules 1:1 (`prices.py` $\leftrightarrow$ `tests/test_prices.py`, `products.py` $\leftrightarrow$ `tests/test_products.py`, `utils.py` $\leftrightarrow$ `tests/test_utils.py`).

---

## 3. Detailed Implementation Specifications

```
                     Incoming Incremental Bars
                                │
                 ┌──────────────┴──────────────┐
                 ▼                             ▼
           Bars Returned?               No Bars Returned
                 │                             │
                 │                      Record 'no_data'
                 ▼
       Overlap Validation [W]
                 │
       ┌─────────┼─────────────────────────────┐
       │         │                             │
   Clean Pass  Bounded Seam Drift        Structural Diff
(Matches in W) (t = t_last only;       (Interior d < t_last,
       │        all OHLC bounds met)    or seam bound exceeded)
       │         │                             │
       └────┬────┘                             ▼
            │                         IBKR 30Y Refetch
            │                                  │
            ▼                         ┌────────┴────────┐
       upsert_series                  ▼                 ▼
   (Overlap W + Tail)           len < 90% exist   len >= 90% exist
[Overwrites seam bar with             │                 │
  auction-cleared values]       ABORT REPLACE     Archive to Cold &
            │                   Preserve Bronze   Replace Bronze
            │                   Record 'error'          │
            └─────────────────────────┬─────────────────┘
                                      ▼
                                 Record 'ok'
```

---

### Spec 1: Decouple Price Validation from Volume & VWAP Telemetry
**Location:** `etfportfolio/ingestion/prices.py`

Modify `PRICES_SPEC` to restrict `value_columns` strictly to pricing fields (`open`, `high`, `low`, `close`):

```python
PRICES_SPEC = SeriesSpec(
    bronze_table="bronze.prices",
    cold_table="cold_storage.prices",
    columns=("open", "high", "low", "close", "volume", "average", "bar_count"),
    value_columns=("open", "high", "low", "close"),  # Exclude volume, average, bar_count
)
```

*Effect:* Eliminates 245 false-positive refetches. Volume and VWAP revisions will still be written to DuckDB during normal upserting without triggering refetches.

---

### Spec 2: Calibrated Tolerances & Module Constants
**Location:** `etfportfolio/ingestion/prices.py`

Define tolerances as module-level constants:

```python
PRICE_REL_TOL = 1e-3  # 0.10% (10 basis points)
PRICE_ABS_TOL = 0.02  # $0.02 (2 cents)

SEAM_REL_TOL = 0.01  # 1.00% max seam shift
SEAM_ABS_TOL = 0.05  # $0.05 max seam shift

MIN_REFETCH_RETENTION_RATIO = 0.90  # Refetch must contain >= 90% of existing bars
```

*Effect:* Accommodates normal 1-cent clearing differences across all overlap days. Keeping them as constants in `prices.py` avoids polluting global `Settings`.

---

### Spec 3: Two-Tier Overlap Validation & Bounded Seam Shift
**Location:** `etfportfolio/ingestion/prices.py`

Refactor `validate_overlap` to maintain the 2-tuple `tuple[bool, str | None]` signature while differentiating interior corporate actions from seam-only auction drift.

#### Rules:
1. **Date set equality:** `existing_dates == new_dates` in $W = [t_{\text{last}} - 7\text{d}, t_{\text{last}}]$. If not equal $\implies$ return `(False, "date_mismatch")`.
2. **Value check:** Compare `spec.value_columns` using `PRICE_REL_TOL` and `PRICE_ABS_TOL`. Collect all dates with differences in `mismatched_dates`.
3. **Clean pass:** If `len(mismatched_dates) == 0` $\implies$ return `(True, None)`.
4. **Interior mismatch:** If any mismatched date is strictly prior to $t_{\text{last}}$ (`d < last_date`) $\implies$ return `(False, "value_mismatch")`.
5. **Seam-only mismatch ($d = t_{\text{last}}$):**
   - For **every** pricing field in `spec.value_columns` that differs on $t_{\text{last}}$, check:
     $$\text{abs\_diff} \le \text{SEAM\_ABS\_TOL} \quad \text{OR} \quad \text{rel\_diff} \le \text{SEAM\_REL\_TOL}$$
     where $\text{rel\_diff} = \frac{|v_{\text{new}} - v_{\text{old}}|}{|v_{\text{old}}|}$ (if $v_{\text{old}} \ne 0$ else $\infty$).
   - If **all** differing fields on $t_{\text{last}}$ satisfy this condition: log an `INFO` message and return `(True, None)`.
   - If **any** pricing field exceeds the envelope: return `(False, "value_mismatch")`.

```python
def validate_overlap(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    product_id: int,
    new_points: dict[datetime, dict[str, Any]],
    last_date: datetime,
) -> tuple[bool, str | None]:
    start = overlap_start_for(last_date)
    col_sql = ", ".join(("date", *spec.columns))
    existing_rows = conn.execute(
        f"""
        SELECT {col_sql}
        FROM {spec.bronze_table}
        WHERE product_id = $1 AND date >= $2 AND date <= $3
        """,
        [product_id, start, last_date],
    ).fetchall()

    existing_dates: set[datetime] = set()
    existing_vals: dict[datetime, dict[str, Any]] = {}
    for row in existing_rows:
        d = row[0]
        existing_dates.add(d)
        existing_vals[d] = {col: row[i + 1] for i, col in enumerate(spec.columns)}

    new_in_w = {d: vals for d, vals in new_points.items() if start <= d <= last_date}
    new_dates = set(new_in_w)

    if existing_dates != new_dates:
        return False, "date_mismatch"

    mismatched_dates: set[datetime] = set()
    for d, new_vals in new_in_w.items():
        old_vals = existing_vals[d]
        for key in spec.value_columns:
            v1, v2 = old_vals.get(key), new_vals.get(key)
            if v1 is not None and v2 is not None:
                if not math.isclose(float(v1), float(v2), rel_tol=PRICE_REL_TOL, abs_tol=PRICE_ABS_TOL):
                    mismatched_dates.add(d)
                    break

    if not mismatched_dates:
        return True, None

    # Discrepancies prior to last_date indicate a historical corporate action (split/dividend)
    interior_mismatches = {d for d in mismatched_dates if d < last_date}
    if interior_mismatches:
        return False, "value_mismatch"

    # Mismatch is isolated to last_date (auction cross settlement drift)
    # Ensure ALL mismatched pricing columns on last_date fall within the bounded seam envelope
    old_seam = existing_vals[last_date]
    new_seam = new_in_w[last_date]

    for key in spec.value_columns:
        v1, v2 = old_seam.get(key), new_seam.get(key)
        if v1 is not None and v2 is not None:
            f1, f2 = float(v1), float(v2)
            if not math.isclose(f1, f2, rel_tol=PRICE_REL_TOL, abs_tol=PRICE_ABS_TOL):
                abs_diff = abs(f2 - f1)
                rel_diff = abs_diff / abs(f1) if f1 != 0 else float("inf")
                if abs_diff > SEAM_ABS_TOL and rel_diff > SEAM_REL_TOL:
                    return False, "value_mismatch"

    logger.info(
        "Product %d: accepted bounded seam revision on %s; overwriting with official settlement.",
        product_id,
        last_date.date(),
    )
    return True, None
```

*Note on Persistence:* When `validate_overlap` returns `(True, None)`, `_fetch_and_store` continues to:
```python
overlap_start = overlap_start_for(last_date)
points_to_store = {d: pt for d, pt in new_bars.items() if d >= overlap_start}
await worker.submit(upsert_series, PRICES_SPEC, product.product_id, points_to_store)
```
Because $t_{\text{last}} \ge \text{overlap\_start}$, the incoming auction-cleared bar automatically overwrites the seam bar in `bronze.prices`.

---

### Spec 4: Anti-Truncation Safety Guard on Replacement
**Location:** `etfportfolio/ingestion/prices.py`

#### Helper Function:
```python
def _get_series_count(conn: duckdb.DuckDBPyConnection, product_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM bronze.prices WHERE product_id = $1",
        [product_id],
    ).fetchone()
    return row[0] if row else 0
```

#### Guard Implementation in `_fetch_and_store`:
```python
    if not valid:
        logger.warning(
            "Product %d: %s detected. Replacing with full refetch and archiving...",
            product.product_id,
            mismatch_type,
        )
        existing_count = await worker.submit(_get_series_count, product.product_id)

        full_bars_raw = await _fetch_historical(ib, product, "30 Y", end_datetime="")
        full_bars = _extract_bars(full_bars_raw, max_date=yesterday)

        min_expected_bars = (
            max(1, math.floor(existing_count * MIN_REFETCH_RETENTION_RATIO))
            if existing_count > 5
            else 1
        )

        if len(full_bars) < min_expected_bars:
            err_msg = (
                f"Truncated refetch: received {len(full_bars)} bars, "
                f"expected >= {min_expected_bars} (existing: {existing_count})"
            )
            logger.error(
                "Product %d: %s. Aborting replace to prevent data loss. Preserving existing bronze rows.",
                product.product_id,
                err_msg,
            )
            await worker.submit(_record_price_status, product.product_id, "error", err_msg)
            return

        if full_bars:
            await worker.submit(
                replace_series,
                PRICES_SPEC,
                product.product_id,
                full_bars,
                archive=True,
                reason=mismatch_type,
            )
            await worker.submit(_record_price_status, product.product_id, "ok", None)
            logger.info(
                "Product %d: mismatch refetch archived and replaced (%d bars)",
                product.product_id,
                len(full_bars),
            )
        else:
            await worker.submit(_record_price_status, product.product_id, "no_data", None)
            logger.warning(
                "Product %d: full refetch returned no bars after mismatch. Preserving existing rows.",
                product.product_id,
            )
        return
```

*Guarantees:*
- Prevents replacing thousands of verified historical bars with transient 1-bar stubs.
- Marking `status="error"` ensures the product is retried on subsequent runs rather than dampened as fresh.
- Strict guard remains active even under `--force`.

---

### Spec 5: Standalone Migration Script for Truncated Products
**Target File:** `scripts/restore_truncated_prices.py`

Implement a standalone, idempotent recovery script to restore products `229325937`, `236798101`, and `332383610` from `cold_storage.prices` back to `bronze.prices`.

```python
"""
One-off migration script to restore truncated price series for products 229325937, 236798101, 332383610.
Idempotent and safe to run multiple times.
"""

import logging
from etfportfolio.core.config import settings
from etfportfolio.core.db import db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TARGET_PRODUCTS = [229325937, 236798101, 332383610]


def restore_products() -> None:
    with db_connection(settings.db_path) as conn:
        for pid in TARGET_PRODUCTS:
            cold_count_row = conn.execute(
                "SELECT COUNT(*) FROM cold_storage.prices WHERE product_id = $1 AND reason = 'date_mismatch'",
                [pid],
            ).fetchone()
            cold_count = cold_count_row[0] if cold_count_row else 0

            if cold_count == 0:
                logger.info("Product %d: No archived date_mismatch rows in cold_storage. Skipping.", pid)
                continue

            bronze_count_row = conn.execute(
                "SELECT COUNT(*) FROM bronze.prices WHERE product_id = $1",
                [pid],
            ).fetchone()
            bronze_count = bronze_count_row[0] if bronze_count_row else 0

            if bronze_count > 1:
                logger.warning(
                    "Product %d: bronze.prices already contains %d rows (> 1). Aborting restore to avoid data loss.",
                    pid,
                    bronze_count,
                )
                continue

            logger.info("Product %d: Restoring %d historical bars from cold_storage...", pid, cold_count)
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("DELETE FROM bronze.prices WHERE product_id = $1", [pid])
                conn.execute(
                    """
                    INSERT INTO bronze.prices (product_id, date, open, high, low, close, volume, average, bar_count, updated_at)
                    SELECT product_id, date, open, high, low, close, volume, average, bar_count, now()
                    FROM cold_storage.prices
                    WHERE product_id = $1 AND reason = 'date_mismatch'
                    """,
                    [pid],
                )
                conn.execute(
                    "DELETE FROM cold_storage.prices WHERE product_id = $1 AND reason = 'date_mismatch'",
                    [pid],
                )
                conn.execute(
                    """
                    INSERT INTO bronze.price_status (product_id, last_checked_at, status, error_message)
                    VALUES ($1, now(), 'error', 'Restored from cold_storage truncation; awaiting incremental extension')
                    ON CONFLICT (product_id) DO UPDATE SET
                        last_checked_at = EXCLUDED.last_checked_at,
                        status = EXCLUDED.status,
                        error_message = EXCLUDED.error_message
                    """,
                    [pid],
                )
                conn.execute("COMMIT")
                logger.info("Product %d: Successfully restored.", pid)
            except Exception:
                conn.execute("ROLLBACK")
                logger.exception("Product %d: Failed to restore. Transaction rolled back.", pid)
                raise


if __name__ == "__main__":
    restore_products()
```

---

### Spec 6: Documentation & Docstring Alignment
**Location:** `etfportfolio/ingestion/prices.py`

Update the `upsert_series` docstring to reflect that overlap bars are written to update values and timestamps:

```python
def upsert_series(
    conn: duckdb.DuckDBPyConnection,
    spec: SeriesSpec,
    product_id: int,
    points: dict[datetime, dict[str, Any]],
) -> None:
    """Upsert points (overlap window bars and incremental tail).

    Overlapping bars are updated in place with fresh auction settlement prints
    and volume reconciliations, updating their updated_at timestamp.
    """
```

---

### Spec 7: Domain Test Suite Reorganization

Apply clean domain separation across test files:

1. **Delete** `tests/test_prices_and_filtering.py`.
2. **Create `tests/test_products.py`:**  
   Houses contract and target product resolution tests extracted from `test_prices_and_filtering.py`:
   - `test_resolve_target_products_empty_contracts`
   - `test_resolve_target_products_filtering`
   - `test_resolve_target_products_all_blocked`
3. **Clean `tests/test_utils.py`:**  
   Remove the 6 price-series test functions from `tests/test_utils.py` (`test_validate_overlap_prices`, `test_replace_series_with_archive`, `test_upsert_series`, `test_is_series_fresh`, `test_upsert_series_updates_updated_at_on_overlap`, `test_overlap_start_for_margin_trimming`). Keep only generic utility tests:
   - `test_is_fresh`
   - `test_content_addressing_determinism`
   - `test_landing_stamp_rule`
   - `test_store_blob_and_gc`
4. **Create `tests/test_prices.py`:**  
   Consolidate all price tests:
   - `test_format_duration`
   - `test_is_series_fresh` (all branches and status dampening)
   - `test_overlap_start_for_margin_trimming`
   - `test_replace_series_with_archive`
   - `test_upsert_series` & `test_upsert_series_updates_updated_at_on_overlap`
   - `test_record_and_load_price_status`
   - `test_fetch_and_store_preserves_prices_on_zero_bars`
   - `test_fetch_and_store_mismatch_zero_bars_preserves`
   - `test_run_price_ingestion_error_capture`
   - `test_run_price_ingestion_ib_connection_error_aborts`
   - `test_cli_signatures_reject_unused_flags`
   - **New Acceptance Scenarios:**
     - `test_validate_overlap_volume_vwap_drift_accepted`: Volume and VWAP differ on $t_{\text{last}}$, but OHLC match $\implies$ passes validation.
     - `test_validate_overlap_seam_penny_shift_accepted`: $\$0.01$ shift on Close or High on $t_{\text{last}}$ $\implies$ passes validation.
     - `test_validate_overlap_interior_shift_triggers_mismatch`: Price difference on $d < t_{\text{last}}$ $\implies$ returns `(False, "value_mismatch")`.
     - `test_validate_overlap_extreme_seam_shift_triggers_mismatch`: 50% shift on $t_{\text{last}}$ exceeds seam envelope $\implies$ returns `(False, "value_mismatch")`.
     - `test_fetch_and_store_anti_truncation_guard_preserves_bronze`: 1-bar refetch against 2,500 existing bars aborts replacement, preserves bronze, and sets status `'error'`.

---

## 4. Verification & Acceptance Criteria

| Criteria | Verification Method | Expected Outcome |
| :--- | :--- | :--- |
| **Volume Drift Immunity** | Unit test in `test_prices.py` | Volume/average shift on $t_{\text{last}}$ returns `is_valid=True`, no refetch triggered. |
| **Seam Shift Immunity** | Unit test in `test_prices.py` | 1-cent shift on seam date returns `is_valid=True`, updates bronze seam bar. |
| **Corporate Action Trigger** | Unit test in `test_prices.py` | Price shift on interior date ($d < t_{\text{last}}$) returns `(False, "value_mismatch")`. |
| **Anti-Truncation Protection** | Unit test in `test_prices.py` | IBKR 1-bar refetch against existing series leaves bronze intact and logs error status. |
| **Migration Idempotency** | Run `scripts/restore_truncated_prices.py` twice | 1st run restores products; 2nd run skips safely without error. |
| **Full Regression Suite** | `uv run pytest tests/` | 100% tests pass cleanly across `test_prices.py`, `test_products.py`, and `test_utils.py`. |