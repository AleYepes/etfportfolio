#!/usr/bin/env python3
"""
Silver Layer Catalog & Panel Validation Profiler
------------------------------------------------
Profiles the Silver Layer to audit data integrity and validate panel design:
  1. Structural integrity invariants (PKs, nulls/infinities, causality).
  2. Ingestion telemetry (payload mutation cadence, stability dwell time, lag).
  3. Empirical effective_date update cadence (inter-arrival quantiles for LOCF).
  4. First-observation reach & backward-fill feasibility against price inception.
  5. Currency distribution for total_net_assets_local (normalization readiness).
  6. LOCF coverage simulation (current hardcoded caps vs p95 vs backward-fill).
  7. Sleeve & fee allocation distributions, debt clusters, and style box.
  8. Migration checklist verification.
  9. Comprehensive statistical catalog exported to data/silver_catalog.sqlite.

Usage:
    uv run scripts/catalog_silver.py
"""

from __future__ import annotations

import sqlite3
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

# ==============================================================================
# VOCABULARIES & DOCUMENTED DOMAINS
# ==============================================================================

DATE_SOURCES = {"payload", "item", "snapshot"}
ASSET_CLASSES = {"Equity", "Fixed Income", "Cash", "Other"}

STYLE_SIZES = ["large", "multi", "mid", "small"]
STYLE_STYLES = ["value", "core", "growth"]
STYLE_BOX_GRID = {f"{sz}_{st}" for sz in STYLE_SIZES for st in STYLE_STYLES}

AUM_PREFIX_TOKENS = {
    "$", "€", "£", "¥", "₹", "CAD", "AUD", "CNY", "TWD", "HKD", "CHF",
    "BRL", "SGD", "MXN", "KRW", "MYR", "CNH", "AED", "SEK", "ZAR",
    "ILS", "SAR", "NOK", "HUF", "DKK", "VND",
}

CREDIT_RATING_GRADES = {
    "AAA", "AA", "A", "BBB", "BB", "B", "CCC", "CC", "C", "D", "Not Rated", "Not Available",
}

MATURITY_SLUGS = {
    "mat_lt_1y", "mat_1_to_3y", "mat_3_to_5y", "mat_5_to_10y",
    "mat_10_to_20y", "mat_20_to_30y", "mat_gt_30y", "mat_other",
}

DEBT_CLUSTERS: dict[str, tuple[str, ...]] = {
    "debt_sovereign": (
        "Sovereign Bond", "Bundesanleihen", "Dutch State Loan", "Gilt Treasury Stock",
        "Irish Govt Bond", "Japanese Govt Bond", "Danish Govt Bond",
        "Notas do Tesouro Nacional F", "Obligaciones del Estado",
        "Obligation Assimilable du Tresor", "Oblig Assim Tresor Indexee I'Indice",
        "Oblig Assim Tresor Indexee I'Inflation", "Obligation Lineaire",
        "Obrigacoes do Tesouro", "Titulos de Tesoreria TES B", "Treasury Bills",
        "Treasury Notes/Bonds", "Treasury STRIPS", "MXBONO", "UDIBONO", "OMAN",
        "Govt Guaranteed", "Government other",
    ),
    "debt_agency_supranational": ("Agencies", "Small Business Administration"),
    "debt_municipal": (
        "MUNI", "Certificates of Obligation", "Certificates of Participation",
        "Grant Antic Notes", "Tax And Rev Antic Notes", "Tax Antic Notes",
        "Unknown Antic Types",
    ),
    "debt_corporate_senior": (
        "CORP", "Corporate Medium Term Notes", "Senior Note", "Senior Debenture",
        "Senior Bank Note", "Senior Secured", "Secured Bond", "Secured Note",
        "First Mortgage Bond", "First Mortgage Note", "First & Refunding Mortgage Bond",
        "Covered Bond", "Hypothekenpfandbrief", "Pfandbrief Anleihe",
        "Oeffentliche Pfandbrief", "HPF Jumbo", "Jumbo Landesschatzanweisung",
        "Sakerstallda Obligationer", "Obligations Foncieres", "Collateral Trust",
        "Collateral Debt", "Collateralized Notes",
    ),
    "debt_corporate_subordinated": (
        "Subordinated Note", "Senior Subordinated Note", "Subordinated Bank Note",
        "Subordinated Debenture", "Senior Subordinated Debenture",
        "Junior Subordinated Note", "Junior Subordinated Debenture", "Mezzanine Debt",
        "Trust Preferred Security", "Participaciones Preferentes",
    ),
    "debt_securitized_mbs": (
        "Mortgage Pools", "Mortgages", "Mortgage Bond", "Mortgage Note",
        "Second Mortgage Bond", "Commercial Mortgage-Backed Security",
        "Collateralized Mortgage Obligation", "CMOs", "CMO Whole Loan",
        "CMO Agricultural MBS", "TBA", "Pass Through Certificate",
    ),
    "debt_securitized_abs": (
        "ABSY", "Asset Backed Tranches", "Credit Card Receivables",
        "Auto/Installment Loans", "Auto Lease Loans", "Auto Floorplan/Wholesale Loans",
        "Equipment Backed Loan", "Aircraft Lease", "Student Loan",
    ),
    "debt_unsecured_general": (
        "Bond", "Note", "Unsecured Note", "Debenture", "Fixed Income", "Global Bonds",
        "Inhaberschuldverschreibung", "Certificate", "Certificates Of Indebtness",
        "Other Certificates", "Deposit Note", "Depositary Share",
        "Depository Receipts (Thailand)", "Bank Debt", "Bankers Acceptance", "Trust",
    ),
    "debt_specialty_derivatives": (
        "Index Linked Security", "Index-Linked Gilt", "Islamic Sukuk", "Derivative",
        "Interest only", "Principal only", "Warrants", "Preferred Stock", "OTHER",
    ),
}

BENCHMARK_LOCF_TARGETS = (
    ("total_expense_ratio", "metrics", 540),
    ("return_on_equity_1yr", "metrics", 180),
    ("mstar_medalist_rating", "metrics", 540),
    ("portfolio_top_10_concentration", "metrics", 180),
    ("tresgs", "metrics", 90),
    ("Equity", "dimensions:asset_class", 180),
)


# ==============================================================================
# FINDINGS & FORMATTERS
# ==============================================================================

@dataclass(frozen=True, slots=True)
class Finding:
    category: str
    section: str
    name: str
    status: str
    details: str = ""


def record(findings: list[Finding], category: str, section: str, name: str, status: str, details: str = "") -> None:
    findings.append(Finding(category, section, name, status, details))
    extra = f" — {details}" if details else ""
    print(f"  [{status}] {name}{extra}")


def invariant(findings: list[Finding], section: str, name: str, ok: bool, details: str = "") -> None:
    record(findings, "INVARIANT", section, name, "PASS" if ok else "FAIL", details)


def skip(findings: list[Finding], section: str, name: str, reason: str) -> None:
    record(findings, "INVARIANT", section, name, "SKIP", reason)


def watchlist(findings: list[Finding], section: str, name: str, unexpected: Iterable[str], context: str = "") -> None:
    unexpected_list = sorted(set(unexpected))
    status = "NEW" if unexpected_list else "PASS"
    detail = f"new values: {unexpected_list}" if unexpected_list else "no values outside documented set"
    if context:
        detail += f" — {context}"
    record(findings, "WATCHLIST", section, name, status, detail)


def pending(findings: list[Finding], section: str, name: str, landed: bool, details: str = "") -> None:
    record(findings, "PENDING", section, name, "LANDED" if landed else "PENDING", details)


def print_section(title: str, subtitle: str | None = None) -> None:
    print(f"\n{'=' * 35} {title} {'=' * 35}")
    if subtitle:
        print(f"  ({subtitle})")


def render_table(
    title: str,
    headers: list[str],
    rows: list[list[Any]],
    align_right: list[int] | None = None,
) -> None:
    align_indices = set(align_right or [])
    if not rows:
        print(f"\n--- {title} ---\n(no records)")
        return

    print(f"\n--- {title} ---")
    widths = [
        max(len(str(x)) for x in [h] + [r[i] for r in rows])
        for i, h in enumerate(headers)
    ]
    fmt = "  ".join(f"{{:>{w}}}" if i in align_indices else f"{{:<{w}}}" for i, w in enumerate(widths))
    print(fmt.format(*headers))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print(fmt.format(*[str(c) if c is not None else "NULL" for c in row]))


def fmt_f(val: Any, decimals: int = 4) -> str:
    if val is None:
        return "NULL"
    try:
        f = float(val)
        return f"{f:,.{decimals}f}"
    except (ValueError, TypeError):
        return str(val)


def _fmt_days(val: Any) -> str:
    return f"{int(val)}d" if val is not None else "NULL"


def _int_or_none(val: Any) -> int | None:
    return int(val) if val is not None else None


def has_column(con: duckdb.DuckDBPyConnection, schema: str, table: str, column: str) -> bool:
    res = con.execute(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?",
        [schema, table, column],
    ).fetchone()
    return res[0] > 0 if res else False


def has_table(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    res = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return res[0] > 0 if res else False


# ==============================================================================
# 1. HARD STRUCTURAL INVARIANTS
# ==============================================================================

def run_invariants(con: duckdb.DuckDBPyConnection, findings: list[Finding]) -> None:
    print_section(
        "1. STRUCTURAL INVARIANTS",
        "Primary keys, nullability, numeric finiteness, temporal causality",
    )

    pk_dup_m = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT product_id, source, metric_id, effective_date FROM silver.product_metrics
            GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    invariant(findings, "Structural", "PK uniqueness in silver.product_metrics", pk_dup_m == 0, f"{pk_dup_m} duplicate groups")

    pk_dup_d = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT product_id, dimension_type, dimension_name, effective_date FROM silver.product_dimensions
            GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    invariant(findings, "Structural", "PK uniqueness in silver.product_dimensions", pk_dup_d == 0, f"{pk_dup_d} duplicate groups")

    cross_source_dups = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT product_id, metric_id, effective_date
            FROM silver.product_metrics
            GROUP BY product_id, metric_id, effective_date
            HAVING COUNT(DISTINCT source) > 1
        )
    """).fetchone()[0]
    invariant(findings, "Structural", "No cross-source metric_id collisions", cross_source_dups == 0,
              f"{cross_source_dups} duplicate groups across sources")

    null_m = con.execute("""
        SELECT COUNT(*) FILTER (WHERE value IS NULL),
               COUNT(*) FILTER (WHERE raw_value IS NULL),
               COUNT(*) FILTER (WHERE isnan(value) OR isinf(value))
        FROM silver.product_metrics
    """).fetchone()
    invariant(findings, "Structural", "value/raw_value NOT NULL, finite (product_metrics)",
              null_m[0] == 0 and null_m[1] == 0 and null_m[2] == 0,
              f"null value={null_m[0]}, null raw={null_m[1]}, non-finite={null_m[2]}")

    null_d = con.execute("""
        SELECT COUNT(*) FILTER (WHERE value IS NULL),
               COUNT(*) FILTER (WHERE raw_value IS NULL),
               COUNT(*) FILTER (WHERE isnan(value) OR isinf(value))
        FROM silver.product_dimensions
    """).fetchone()
    invariant(findings, "Structural", "value/raw_value NOT NULL, finite (product_dimensions)",
              null_d[0] == 0 and null_d[1] == 0 and null_d[2] == 0,
              f"null value={null_d[0]}, null raw={null_d[1]}, non-finite={null_d[2]}")

    future = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT 1 FROM silver.product_metrics WHERE effective_date > fetched_at::DATE
            UNION ALL
            SELECT 1 FROM silver.product_dimensions WHERE effective_date > fetched_at::DATE
        )
    """).fetchone()[0]
    invariant(findings, "Structural", "Temporal causality (effective_date <= fetched_at::DATE)", future == 0, f"{future} future-dated rows")

    if has_table(con, "bronze", "contracts"):
        orphans = con.execute("""
            SELECT COUNT(DISTINCT product_id) FROM (
                SELECT product_id FROM silver.product_metrics WHERE product_id NOT IN (SELECT product_id FROM bronze.contracts)
                UNION ALL
                SELECT product_id FROM silver.product_dimensions WHERE product_id NOT IN (SELECT product_id FROM bronze.contracts)
            )
        """).fetchone()[0]
        invariant(findings, "Structural", "Every product_id resolves to bronze.contracts", orphans == 0, f"{orphans} orphan product_ids")
    else:
        skip(findings, "Structural", "bronze.contracts foreign key checks", "bronze.contracts table not found")

    bad_src = con.execute("""
        SELECT DISTINCT effective_date_source FROM silver.product_metrics
        WHERE effective_date_source NOT IN ('payload','item','snapshot')
        UNION
        SELECT DISTINCT effective_date_source FROM silver.product_dimensions
        WHERE effective_date_source NOT IN ('payload','item','snapshot')
    """).fetchall()
    invariant(findings, "Structural", "effective_date_source is exactly {payload, item, snapshot}",
              len(bad_src) == 0, f"unexpected values: {[r[0] for r in bad_src]}")

    # Watchlists
    ac_names = {r[0] for r in con.execute(
        "SELECT DISTINCT dimension_name FROM silver.product_dimensions WHERE dimension_type = 'asset_class'"
    ).fetchall()}
    watchlist(findings, "Vocabulary", "Asset class categories", ac_names - ASSET_CLASSES,
              f"{len(ac_names)} observed vs {len(ASSET_CLASSES)} documented")

    tokens = {r[0] for r in con.execute(r"""
        SELECT DISTINCT REGEXP_EXTRACT(raw_value, '^([^0-9\s]+)', 1)
        FROM silver.product_metrics WHERE metric_id = 'total_net_assets_local'
    """).fetchall() if r[0]}
    watchlist(findings, "Vocabulary", "AUM currency prefix tokens", tokens - AUM_PREFIX_TOKENS,
              f"{len(tokens)} observed vs {len(AUM_PREFIX_TOKENS)} documented")

    cr_names = {r[0] for r in con.execute(
        "SELECT DISTINCT dimension_name FROM silver.product_dimensions WHERE dimension_type = 'credit_rating'"
    ).fetchall()}
    watchlist(findings, "Vocabulary", "Credit rating grade names", cr_names - CREDIT_RATING_GRADES,
              f"{len(cr_names)} observed vs {len(CREDIT_RATING_GRADES)} documented")

    mat_codes = {r[0] for r in con.execute(
        "SELECT DISTINCT dimension_code FROM silver.product_dimensions WHERE dimension_type = 'maturity' AND dimension_code IS NOT NULL"
    ).fetchall()}
    watchlist(findings, "Vocabulary", "Maturity bucket slugs", mat_codes - MATURITY_SLUGS,
              f"{len(mat_codes)} observed vs {len(MATURITY_SLUGS)} documented")

    sb_codes = {r[0] for r in con.execute(
        "SELECT DISTINCT dimension_code FROM silver.product_dimensions WHERE dimension_type IN ('style_box', 'style_box_hist') AND dimension_code IS NOT NULL"
    ).fetchall()}
    watchlist(findings, "Vocabulary", "Style box dimension codes", sb_codes - STYLE_BOX_GRID,
              f"{len(sb_codes)} observed vs {len(STYLE_BOX_GRID)} documented")


# ==============================================================================
# 2. PAYLOAD INGESTION & STORAGE CADENCE
# ==============================================================================

def run_payload_cadence(con: duckdb.DuckDBPyConnection) -> tuple[list[tuple], list[tuple], list[tuple], list[tuple]]:
    print_section(
        "2. PAYLOAD INGESTION & STORAGE CADENCE",
        "Analyzing bronze.snapshots: payload mutation frequency, dwell time, and vendor publication lag",
    )

    if not has_table(con, "bronze", "snapshots"):
        print("  bronze.snapshots not present in this database — skipping payload cadence.")
        return [], [], [], []

    ep_summary = con.execute("""
        SELECT
            url_prefix,
            COUNT(*) AS total_snapshots,
            COUNT(DISTINCT product_id) AS total_products,
            COUNT(DISTINCT hash) AS distinct_payload_hashes,
            ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT product_id), 0), 2) AS avg_snapshots_per_product,
            MIN(created_at) AS first_seen,
            MAX(created_at) AS last_seen
        FROM bronze.snapshots
        GROUP BY url_prefix
        ORDER BY total_snapshots DESC
    """).fetchall()

    render_table(
        "Bronze Snapshots by Endpoint URL Prefix",
        ["URL Prefix", "Total Snapshots", "Products", "Unique Hashes", "Snaps/Product", "First Seen", "Last Seen"],
        [[r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]:,}", str(r[4]), str(r[5])[:16], str(r[6])[:16]] for r in ep_summary],
        align_right=[1, 2, 3, 4],
    )

    payload_deltas = con.execute("""
        WITH ordered_snapshots AS (
            SELECT
                url_prefix,
                product_id,
                created_at,
                DATE_DIFF('hour', LAG(created_at) OVER (
                    PARTITION BY product_id, url_prefix ORDER BY created_at
                ), created_at) AS delta_hours
            FROM bronze.snapshots
        )
        SELECT
            url_prefix,
            COUNT(*) FILTER (WHERE delta_hours IS NOT NULL) AS payload_updates_observed,
            ROUND(MEDIAN(delta_hours) / 24.0, 1) AS med_days_between_payloads,
            ROUND(quantile_cont(delta_hours, 0.25) / 24.0, 1) AS p25_days,
            ROUND(quantile_cont(delta_hours, 0.75) / 24.0, 1) AS p75_days,
            ROUND(quantile_cont(delta_hours, 0.95) / 24.0, 1) AS p95_days,
            ROUND(MAX(delta_hours) / 24.0, 1) AS max_days,
            ROUND(COUNT(*) FILTER (WHERE delta_hours BETWEEN 144 AND 192) * 100.0 / NULLIF(COUNT(*) FILTER (WHERE delta_hours IS NOT NULL), 0), 1) AS pct_approx_weekly,
            ROUND(COUNT(*) FILTER (WHERE delta_hours BETWEEN 672 AND 768) * 100.0 / NULLIF(COUNT(*) FILTER (WHERE delta_hours IS NOT NULL), 0), 1) AS pct_approx_monthly
        FROM ordered_snapshots
        GROUP BY url_prefix
        HAVING COUNT(*) FILTER (WHERE delta_hours IS NOT NULL) > 0
        ORDER BY payload_updates_observed DESC
    """).fetchall()

    render_table(
        "Payload Mutation Frequency (Interval between successive bronze.snapshots records)",
        ["URL Prefix", "Update Events", "Median Interval", "p25", "p75", "p95", "Max Gap", "% Weekly (~7d)", "% Monthly (~30d)"],
        [
            [
                r[0], f"{r[1]:,}", f"{r[2]}d", f"{r[3]}d", f"{r[4]}d", f"{r[5]}d", f"{r[6]}d",
                f"{r[7]}%" if r[7] is not None else "0.0%",
                f"{r[8]}%" if r[8] is not None else "0.0%",
            ]
            for r in payload_deltas
        ],
        align_right=list(range(1, 9)),
    )

    dwell_summary = con.execute("""
        WITH dwell AS (
            SELECT
                url_prefix,
                DATE_DIFF('day', created_at, last_checked_at) AS dwell_days
            FROM bronze.snapshots
        )
        SELECT
            url_prefix,
            COUNT(*) AS total_snapshots,
            COUNT(*) FILTER (WHERE dwell_days = 0) AS single_poll_snaps,
            ROUND(COUNT(*) FILTER (WHERE dwell_days = 0) * 100.0 / COUNT(*), 1) AS pct_single_poll,
            ROUND(MEDIAN(dwell_days), 1) AS median_dwell_days,
            ROUND(quantile_cont(dwell_days, 0.75), 1) AS p75_dwell_days,
            MAX(dwell_days) AS max_dwell_days
        FROM dwell
        GROUP BY url_prefix
        ORDER BY total_snapshots DESC
    """).fetchall()

    render_table(
        "Payload Dwell Time / Stability Window (How long a payload hash remains current)",
        ["URL Prefix", "Snapshots", "Single-Poll Snaps", "% Transient", "Median Dwell", "p75 Dwell", "Max Dwell"],
        [
            [r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]}%", f"{r[4]}d", f"{r[5]}d", f"{r[6]}d"]
            for r in dwell_summary
        ],
        align_right=list(range(1, 7)),
    )

    pub_lag = con.execute("""
        WITH lags AS (
            SELECT
                source,
                effective_date_source,
                DATE_DIFF('day', effective_date, fetched_at::DATE) AS lag_days
            FROM silver.product_metrics
            WHERE effective_date_source != 'snapshot'
        )
        SELECT
            source,
            effective_date_source,
            COUNT(*) AS n_obs,
            ROUND(MIN(lag_days), 0) AS min_lag,
            ROUND(quantile_cont(lag_days, 0.25), 0) AS p25_lag,
            ROUND(MEDIAN(lag_days), 0) AS median_lag,
            ROUND(quantile_cont(lag_days, 0.75), 0) AS p75_lag,
            ROUND(quantile_cont(lag_days, 0.95), 0) AS p95_lag,
            MAX(lag_days) AS max_lag,
            COUNT(*) FILTER (WHERE lag_days < 0) AS negative_lag_cnt
        FROM lags
        GROUP BY source, effective_date_source
        ORDER BY n_obs DESC
    """).fetchall()

    render_table(
        "Publication Lag (fetched_at - effective_date: vendor reporting latency)",
        ["Source", "Date Provenance", "Observations", "Min Lag", "p25 Lag", "Median Lag", "p75 Lag", "p95 Lag", "Max Lag", "Future Date (<0)"],
        [
            [
                r[0], r[1], f"{r[2]:,}", _fmt_days(r[3]), _fmt_days(r[4]), _fmt_days(r[5]),
                _fmt_days(r[6]), _fmt_days(r[7]), _fmt_days(r[8]), f"{r[9]:,}",
            ]
            for r in pub_lag
        ],
        align_right=list(range(2, 10)),
    )

    return ep_summary, payload_deltas, dwell_summary, pub_lag


# ==============================================================================
# 3. EMPIRICAL EFFECTIVE DATE CADENCE (LOCF Horizon Calibration)
# ==============================================================================

def run_effective_date_cadence(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    print_section(
        "3. EMPIRICAL EFFECTIVE_DATE CADENCE",
        "Inter-arrival distribution of distinct observation dates per product (data-driven LOCF staleness caps)",
    )

    cadence_rows = con.execute("""
        WITH metric_transitions AS (
            SELECT
                source,
                metric_id AS feature_id,
                product_id,
                effective_date,
                DATE_DIFF('day', LAG(effective_date) OVER (
                    PARTITION BY product_id, source, metric_id ORDER BY effective_date
                ), effective_date) AS gap_days
            FROM (
                SELECT DISTINCT product_id, source, metric_id, effective_date
                FROM silver.product_metrics
            )
        ),
        dim_transitions AS (
            SELECT
                'dimension' AS source,
                dimension_type AS feature_id,
                product_id,
                effective_date,
                DATE_DIFF('day', LAG(effective_date) OVER (
                    PARTITION BY product_id, dimension_type ORDER BY effective_date
                ), effective_date) AS gap_days
            FROM (
                SELECT DISTINCT product_id, dimension_type, effective_date
                FROM silver.product_dimensions
            )
        ),
        all_transitions AS (
            SELECT * FROM metric_transitions
            UNION ALL
            SELECT * FROM dim_transitions
        )
        SELECT
            source,
            feature_id,
            COUNT(*) AS total_transitions,
            COUNT(DISTINCT product_id) AS funds_with_updates,
            ROUND(MIN(gap_days), 0) AS min_gap_days,
            ROUND(quantile_cont(gap_days, 0.25), 0) AS p25_gap_days,
            ROUND(MEDIAN(gap_days), 0) AS median_gap_days,
            ROUND(quantile_cont(gap_days, 0.75), 0) AS p75_gap_days,
            ROUND(quantile_cont(gap_days, 0.90), 0) AS p90_gap_days,
            ROUND(quantile_cont(gap_days, 0.95), 0) AS p95_gap_days,
            ROUND(quantile_cont(gap_days, 0.99), 0) AS p99_gap_days,
            ROUND(MAX(gap_days), 0) AS max_gap_days
        FROM all_transitions
        WHERE gap_days IS NOT NULL AND gap_days > 0
        GROUP BY source, feature_id
        ORDER BY total_transitions DESC, source, feature_id
    """).fetchall()

    # Display benchmark and top 25 transitions on console
    display_rows = cadence_rows[:25]
    render_table(
        f"Empirical effective_date Update Cadence ({len(cadence_rows)} total features, top {len(display_rows)} displayed)",
        ["Source", "Feature / Type", "Transitions", "Funds", "Min", "p25", "Median", "p75", "p90", "p95 (Cap Target)", "p99", "Max"],
        [
            [
                r[0], r[1], f"{r[2]:,}", f"{r[3]:,}", _fmt_days(r[4]), _fmt_days(r[5]), _fmt_days(r[6]),
                _fmt_days(r[7]), _fmt_days(r[8]), _fmt_days(r[9]), _fmt_days(r[10]), _fmt_days(r[11]),
            ]
            for r in display_rows
        ],
        align_right=list(range(2, 12)),
    )

    return cadence_rows


# ==============================================================================
# 4. FIRST OBSERVATION REACH (Backward-Fill Feasibility)
# ==============================================================================

def run_backward_fill_audit(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    print_section(
        "4. FIRST-OBSERVATION REACH & BACKWARD-FILL AUDIT",
        "Assessing price inception vs first fundamental observation date (quantifying backward-fill reach)",
    )

    if not has_table(con, "bronze", "prices"):
        print("  bronze.prices not present in this database — skipping backward fill audit.")
        return []

    reach_rows = con.execute("""
        WITH first_price AS (
            SELECT product_id, MIN(date::DATE) AS first_price_date
            FROM bronze.prices
            GROUP BY product_id
        ),
        family_first AS (
            SELECT
                product_id,
                source AS family,
                MIN(effective_date) AS first_eff_date
            FROM silver.product_metrics
            GROUP BY product_id, source
            UNION ALL
            SELECT
                product_id,
                dimension_type AS family,
                MIN(effective_date) AS first_eff_date
            FROM silver.product_dimensions
            WHERE dimension_type IN ('asset_class', 'country', 'industry', 'debt_type', 'credit_rating', 'maturity', 'style_box')
            GROUP BY product_id, dimension_type
        ),
        gaps AS (
            SELECT
                ff.family,
                ff.product_id,
                DATE_DIFF('day', p.first_price_date, ff.first_eff_date) AS pre_fund_days
            FROM family_first ff
            JOIN first_price p ON p.product_id = ff.product_id
        )
        SELECT
            family,
            COUNT(*) AS total_funds,
            COUNT(*) FILTER (WHERE pre_fund_days > 0) AS funds_with_pre_price,
            ROUND(COUNT(*) FILTER (WHERE pre_fund_days > 0) * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct_pre_price,
            ROUND(MEDIAN(pre_fund_days) FILTER (WHERE pre_fund_days > 0), 0) AS median_pre_days,
            ROUND(quantile_cont(pre_fund_days, 0.75) FILTER (WHERE pre_fund_days > 0), 0) AS p75_pre_days,
            ROUND(quantile_cont(pre_fund_days, 0.95) FILTER (WHERE pre_fund_days > 0), 0) AS p95_pre_days,
            MAX(pre_fund_days) AS max_pre_days
        FROM gaps
        GROUP BY family
        ORDER BY total_funds DESC
    """).fetchall()

    render_table(
        "First Observation Latency (Days from first price trade to first fundamental observation)",
        ["Family", "Funds", "Pre-Dated Funds", "% Pre-Dated", "Median Gap", "p75 Gap", "p95 Gap", "Max Gap"],
        [
            [
                r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]}%" if r[3] is not None else "0.0%",
                _fmt_days(r[4]), _fmt_days(r[5]), _fmt_days(r[6]), _fmt_days(r[7]),
            ]
            for r in reach_rows
        ],
        align_right=list(range(1, 8)),
    )

    return reach_rows


# ==============================================================================
# 5. CURRENCY DISTRIBUTION (total_net_assets_local Normalization Readiness)
# ==============================================================================

def run_currency_profile(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    print_section(
        "5. CURRENCY EXPOSURE PROFILE",
        "Distribution of reported currencies in total_net_assets_local (validating FX standardization prerequisites)",
    )

    has_ccy = has_column(con, "silver", "product_metrics", "currency")
    if not has_ccy:
        print("  currency column missing in silver.product_metrics.")
        return []

    ccy_rows = con.execute("""
        SELECT
            COALESCE(currency, '[NULL]') AS currency,
            COUNT(*) AS obs_count,
            COUNT(DISTINCT product_id) AS fund_count,
            ROUND(MIN(value), 0) AS min_local_val,
            ROUND(MEDIAN(value), 0) AS median_local_val,
            ROUND(MAX(value), 0) AS max_local_val
        FROM silver.product_metrics
        WHERE metric_id = 'total_net_assets_local'
        GROUP BY currency
        ORDER BY obs_count DESC
    """).fetchall()

    render_table(
        "total_net_assets_local Currency Breakdown",
        ["Currency", "Observations", "Funds", "Min Local AUM", "Median Local AUM", "Max Local AUM"],
        [
            [r[0], f"{r[1]:,}", f"{r[2]:,}", fmt_f(r[3], 0), fmt_f(r[4], 0), fmt_f(r[5], 0)]
            for r in ccy_rows
        ],
        align_right=[1, 2, 3, 4, 5],
    )

    return ccy_rows


# ==============================================================================
# 6. LOCF COVERAGE SIMULATION (Current Caps vs p95 vs Backward Fill)
# ==============================================================================

def run_locf_coverage_simulation(con: duckdb.DuckDBPyConnection, cadence_rows: list[tuple]) -> list[tuple]:
    print_section(
        "6. LOCF COVERAGE SIMULATION",
        "Comparing monthly panel fill-rates under current caps vs empirical p95 vs backward-fill",
    )

    if not has_table(con, "bronze", "prices"):
        print("  bronze.prices not present in this database — skipping coverage simulation.")
        return []

    # Map (source, feature_id) -> p95_days from Profile 3
    p95_map: dict[tuple[str, str], int] = {}
    for r in cadence_rows:
        if r[9] is not None:
            p95_map[(str(r[0]), str(r[1]))] = int(r[9])

    sim_results: list[tuple] = []

    con.execute("""
        CREATE TEMP TABLE active_spine AS
        SELECT DISTINCT product_id, LAST_DAY(date::DATE) AS as_of_date
        FROM bronze.prices
        GROUP BY product_id, LAST_DAY(date::DATE)
    """)

    for target_name, target_type, current_cap in BENCHMARK_LOCF_TARGETS:
        if target_type == "metrics":
            con.execute("""
                CREATE OR REPLACE TEMP TABLE sim_obs AS
                SELECT product_id, effective_date
                FROM silver.product_metrics
                WHERE metric_id = $1
                GROUP BY product_id, effective_date
            """, [target_name])
            p95_cap = p95_map.get(("ratios", target_name)) or p95_map.get(("profile", target_name)) or p95_map.get(("mstar", target_name)) or p95_map.get(("holdings", target_name)) or p95_map.get(("esg", target_name)) or (current_cap * 2)
        else:
            dim_type = target_type.split(":", 1)[1]
            con.execute("""
                CREATE OR REPLACE TEMP TABLE sim_obs AS
                SELECT product_id, effective_date
                FROM silver.product_dimensions
                WHERE dimension_type = $1 AND dimension_name = $2
                GROUP BY product_id, effective_date
            """, [dim_type, target_name])
            p95_cap = p95_map.get(("dimension", dim_type)) or (current_cap * 2)

        # Enforce that p95_cap is at least current_cap for generous evaluation
        p95_cap = max(current_cap, p95_cap)

        sim_row = con.execute("""
            WITH eligible_spine AS (
                SELECT s.product_id, s.as_of_date, f.first_eff_date
                FROM active_spine s
                JOIN (
                    SELECT product_id, MIN(effective_date) AS first_eff_date
                    FROM sim_obs
                    GROUP BY product_id
                ) f ON f.product_id = s.product_id
            ),
            locf_matches AS (
                SELECT
                    e.product_id,
                    e.as_of_date,
                    MAX(CASE
                        WHEN o.effective_date <= e.as_of_date
                         AND DATE_DIFF('day', o.effective_date, e.as_of_date) <= $1
                        THEN 1 ELSE 0 END) AS has_current_cap,
                    MAX(CASE
                        WHEN o.effective_date <= e.as_of_date
                         AND DATE_DIFF('day', o.effective_date, e.as_of_date) <= $2
                        THEN 1 ELSE 0 END) AS has_p95_cap,
                    MAX(CASE
                        WHEN e.as_of_date < e.first_eff_date
                         AND DATE_DIFF('day', e.as_of_date, e.first_eff_date) <= $2
                        THEN 1 ELSE 0 END) AS has_bfill
                FROM eligible_spine e
                LEFT JOIN sim_obs o
                    ON o.product_id = e.product_id
                   AND o.effective_date <= e.as_of_date
                   AND DATE_DIFF('day', o.effective_date, e.as_of_date) <= $2
                GROUP BY e.product_id, e.as_of_date, e.first_eff_date
            )
            SELECT
                COUNT(*) AS eligible_months,
                SUM(has_current_cap) AS current_cap_months,
                SUM(has_p95_cap) AS p95_cap_months,
                SUM(CASE WHEN has_p95_cap = 1 OR has_bfill = 1 THEN 1 ELSE 0 END) AS total_bfill_months
            FROM locf_matches
        """, [current_cap, p95_cap]).fetchone()

        if sim_row and sim_row[0] > 0:
            elig = sim_row[0]
            c_months = sim_row[1] or 0
            p_months = sim_row[2] or 0
            b_months = sim_row[3] or 0
            c_pct = round(c_months * 100.0 / elig, 1)
            p_pct = round(p_months * 100.0 / elig, 1)
            b_pct = round(b_months * 100.0 / elig, 1)
            b_gain = b_months - p_months

            sim_results.append((
                target_name,
                target_type,
                current_cap,
                p95_cap,
                elig,
                c_months,
                c_pct,
                p_months,
                p_pct,
                b_months,
                b_pct,
                b_gain,
            ))

    con.execute("DROP TABLE IF EXISTS active_spine")
    con.execute("DROP TABLE IF EXISTS sim_obs")

    render_table(
        "LOCF Coverage Simulation Across Benchmark Features",
        ["Feature", "Type", "Current Cap", "p95 Cap", "Spine Mos", "Current Mos", "Current %", "p95 Mos", "p95 %", "p95+Bfill Mos", "Total %", "Bfill Gain"],
        [
            [
                r[0], r[1], f"{r[2]}d", f"{r[3]}d", f"{r[4]:,}", f"{r[5]:,}", f"{r[6]}%",
                f"{r[7]:,}", f"{r[8]}%", f"{r[9]:,}", f"{r[10]}%", f"+{r[11]:,}",
            ]
            for r in sim_results
        ],
        align_right=list(range(2, 12)),
    )

    return sim_results


# ==============================================================================
# 7. EMPIRICAL PROFILES (Sleeves, Fees, Debt Clusters)
# ==============================================================================

def run_empirical_profiles(con: duckdb.DuckDBPyConnection) -> tuple[list[tuple], list[tuple], list[tuple]]:
    print_section(
        "7. EMPIRICAL BEHAVIORAL PROFILES",
        "Descriptive distributions: sleeve sums, fee allocations, debt clusters",
    )

    sleeve_rows = con.execute("""
        WITH snap_sums AS (
            SELECT dimension_type, product_id, effective_date,
                   SUM(value) AS sum_wgt,
                   SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END) AS neg_wgt_cnt
            FROM silver.product_dimensions
            WHERE dimension_type IN ('asset_class', 'country', 'industry', 'credit_rating', 'maturity', 'debt_type')
            GROUP BY 1, 2, 3
        )
        SELECT
            dimension_type,
            COUNT(*) AS total_snapshots,
            COUNT(*) FILTER (WHERE ABS(sum_wgt - 1.0) <= 0.001) AS exact_one_cnt,
            ROUND(COUNT(*) FILTER (WHERE ABS(sum_wgt - 1.0) <= 0.001) * 100.0 / COUNT(*), 2) AS exact_one_pct,
            COUNT(*) FILTER (WHERE sum_wgt > 1.05) AS levered_cnt,
            COUNT(*) FILTER (WHERE sum_wgt < 0.95) AS under_cnt,
            ROUND(MIN(sum_wgt), 4) AS min_sum,
            ROUND(MEDIAN(sum_wgt), 4) AS med_sum,
            ROUND(MAX(sum_wgt), 4) AS max_sum,
            COUNT(*) FILTER (WHERE neg_wgt_cnt > 0) AS with_neg_cnt
        FROM snap_sums
        GROUP BY dimension_type
        ORDER BY total_snapshots DESC
    """).fetchall()

    render_table(
        "Allocation Weights Profile by Dimension Type",
        ["Dimension", "Snapshots", "Exact (±0.1%)", "Exact %", "Lev (>1.05)", "Under (<0.95)", "Min Sum", "Median Sum", "Max Sum", "With Shorts"],
        [
            [r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]}%", f"{r[4]:,}", f"{r[5]:,}", fmt_f(r[6]), fmt_f(r[7]), fmt_f(r[8]), f"{r[9]:,}"]
            for r in sleeve_rows
        ],
        align_right=list(range(1, 10)),
    )

    fee_alloc = con.execute("""
        WITH pairs AS (
            SELECT product_id, effective_date,
                   SUM(CASE WHEN metric_id = 'management_expense_ratio' THEN value ELSE 0 END) AS mgt,
                   SUM(CASE WHEN metric_id = 'non_management_expense_ratio' THEN value ELSE 0 END) AS non_mgt
            FROM silver.product_metrics
            WHERE source = 'profile' AND metric_id IN ('management_expense_ratio', 'non_management_expense_ratio')
            GROUP BY 1, 2 HAVING COUNT(DISTINCT metric_id) = 2
        )
        SELECT
            COUNT(*) AS total_pairs,
            COUNT(*) FILTER (WHERE ABS(mgt + non_mgt - 1.0) <= 0.001) AS exact_one_cnt,
            ROUND(COUNT(*) FILTER (WHERE ABS(mgt + non_mgt - 1.0) <= 0.001) * 100.0 / COUNT(*), 2) AS exact_one_pct,
            COUNT(*) FILTER (WHERE mgt < 0 OR non_mgt < 0) AS with_subsidies,
            COUNT(*) FILTER (WHERE non_mgt = 0) AS zero_non_mgt,
            ROUND(MIN(mgt), 4) AS min_mgt,
            ROUND(MAX(mgt), 4) AS max_mgt,
            ROUND(MIN(non_mgt), 4) AS min_non_mgt,
            ROUND(MAX(non_mgt), 4) AS max_non_mgt
        FROM pairs
    """).fetchone()

    fee_alloc_rows: list[tuple] = []
    if fee_alloc and fee_alloc[0] > 0:
        fee_alloc_rows.append(fee_alloc)
        render_table(
            "Expense Ratio Allocation (Management + Non-Management)",
            ["Paired Snaps", "Exact = 1.0", "% Exact", "Fee Subsidies (<0)", "Zero Non-Mgt", "Min Mgt", "Max Mgt", "Min Non-Mgt", "Max Non-Mgt"],
            [[
                f"{fee_alloc[0]:,}", f"{fee_alloc[1]:,}", f"{fee_alloc[2]}%", f"{fee_alloc[3]:,}", f"{fee_alloc[4]:,}",
                fmt_f(fee_alloc[5]), fmt_f(fee_alloc[6]), fmt_f(fee_alloc[7]), fmt_f(fee_alloc[8]),
            ]],
            align_right=list(range(9)),
        )

    cases = []
    for cluster, names in DEBT_CLUSTERS.items():
        for name in names:
            escaped = name.replace("'", "''")
            cases.append(f"WHEN dimension_name = '{escaped}' THEN '{cluster}'")
    case_sql = " ".join(cases)

    debt_cluster_rows = con.execute(f"""
        WITH mapped AS (
            SELECT dimension_name, product_id, value,
                   CASE {case_sql} ELSE 'debt_unmapped_new' END AS cluster_id
            FROM silver.product_dimensions
            WHERE dimension_type = 'debt_type'
        )
        SELECT
            cluster_id,
            COUNT(DISTINCT dimension_name) AS n_types,
            COUNT(*) AS n_obs,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(SUM(value), 2) AS sum_weight,
            ROUND(AVG(value), 4) AS avg_weight
        FROM mapped
        GROUP BY cluster_id
        ORDER BY n_obs DESC
    """).fetchall()

    render_table(
        "Debt Type 9-Cluster Coverage Profile",
        ["Cluster", "Distinct Types", "Observations", "Funds", "Total Weight", "Mean Weight"],
        [[r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]:,}", fmt_f(r[4], 2), fmt_f(r[5], 4)] for r in debt_cluster_rows],
        align_right=list(range(1, 6)),
    )

    return sleeve_rows, debt_cluster_rows, fee_alloc_rows


# ==============================================================================
# 8. MIGRATION CHECKLIST
# ==============================================================================

def run_pending(con: duckdb.DuckDBPyConnection, findings: list[Finding]) -> None:
    print_section("8. MIGRATION CHECKLIST", "Implementation-readiness verification (schema migrations, cleanups)")

    has_ccy = has_column(con, "silver", "product_metrics", "currency")
    pending(findings, "Schema", "silver.product_metrics.currency column exists", has_ccy)

    top_holding_rows = con.execute(
        "SELECT COUNT(*) FROM silver.product_dimensions WHERE dimension_type = 'top_holding'"
    ).fetchone()[0]
    pending(findings, "Schema", "top_holding extraction pruned", top_holding_rows == 0,
            f"{top_holding_rows:,} rows remain" if top_holding_rows else "0 rows (pruned)")

    med_growth = con.execute(
        "SELECT MEDIAN(value) FROM silver.product_metrics WHERE source = 'ratios' AND metric_id = 'eps_growth_1yr'"
    ).fetchone()[0]
    is_decimal = med_growth is not None and abs(float(med_growth)) < 2.0
    pending(findings, "Ratios", "growth/return/yield metrics converted to decimals (/100)", is_decimal,
            f"median eps_growth={fmt_f(med_growth)}")

    medalist_n = con.execute(
        "SELECT COUNT(*) FROM silver.product_metrics WHERE metric_id = 'mstar_medalist_rating'"
    ).fetchone()[0]
    pending(findings, "Morningstar", "unified mstar_medalist_rating extracted", medalist_n > 0, f"{medalist_n:,} rows")

    coverage_n = con.execute(
        "SELECT COUNT(*) FROM silver.product_metrics WHERE metric_id = 'mstar_analyst_coverage_pct'"
    ).fetchone()[0]
    pending(findings, "Morningstar", "mstar_analyst_coverage_pct extracted", coverage_n > 0, f"{coverage_n:,} rows")

    lip_variants = con.execute(
        "SELECT COUNT(DISTINCT metric_id) FROM silver.product_metrics WHERE source = 'lipper'"
    ).fetchone()[0]
    pending(findings, "Lipper", "Max-Peer-Count reduction applied (<=25 canonical ids)", lip_variants <= 25, f"{lip_variants} distinct metric_ids")

    theme_cov_n = con.execute(
        "SELECT COUNT(*) FROM silver.product_metrics WHERE metric_id = 'theme_coverage'"
    ).fetchone()[0]
    pending(findings, "Themes", "theme_coverage extracted in product_metrics", theme_cov_n > 0, f"{theme_cov_n:,} rows")

    theme_dim_n = con.execute(
        "SELECT COUNT(*) FROM silver.product_dimensions WHERE dimension_type = 'theme'"
    ).fetchone()[0]
    pending(findings, "Themes", "theme weights extracted in product_dimensions", theme_dim_n > 0, f"{theme_dim_n:,} rows")

    sb_dim_n = con.execute(
        "SELECT COUNT(*) FROM silver.product_dimensions WHERE dimension_type IN ('style_box', 'style_box_hist')"
    ).fetchone()[0]
    pending(findings, "Style Box", "style_box coordinates extracted in product_dimensions", sb_dim_n > 0, f"{sb_dim_n:,} rows")

    is_passive_n = con.execute(
        "SELECT COUNT(*) FROM silver.product_metrics WHERE source = 'profile' AND metric_id = 'is_passive'"
    ).fetchone()[0]
    pending(findings, "Profile", "is_passive approach metric extracted", is_passive_n > 0, f"{is_passive_n:,} rows")

    audited_fee_n = con.execute(
        "SELECT COUNT(*) FROM silver.product_metrics WHERE source = 'profile' AND metric_id = 'audited_net_expense_ratio'"
    ).fetchone()[0]
    pending(findings, "Profile", "audited_net_expense_ratio metric extracted", audited_fee_n > 0, f"{audited_fee_n:,} rows")

    tenure_n = con.execute(
        "SELECT COUNT(*) FROM silver.product_metrics WHERE source = 'profile' AND metric_id = 'manager_tenure_years'"
    ).fetchone()[0]
    pending(findings, "Profile", "manager_tenure_years metric extracted", tenure_n > 0, f"{tenure_n:,} rows")

    top10_n = con.execute(
        "SELECT COUNT(*) FROM silver.product_metrics WHERE source = 'holdings' AND metric_id = 'portfolio_top_10_concentration'"
    ).fetchone()[0]
    pending(findings, "Holdings", "portfolio_top_10_concentration metric extracted", top10_n > 0, f"{top10_n:,} rows")

    unmapped_countries = con.execute("""
        SELECT COUNT(*) FROM silver.product_dimensions
        WHERE dimension_type = 'country' AND (
            (dimension_name = 'Croatia' AND COALESCE(dimension_code, '') != 'HR') OR
            (dimension_name = 'Bulgaria' AND COALESCE(dimension_code, '') != 'BG') OR
            (dimension_name = 'Guam' AND COALESCE(dimension_code, '') != 'GU') OR
            (dimension_name = 'Uzbekistan' AND COALESCE(dimension_code, '') != 'UZ') OR
            (dimension_name = 'Unidentified' AND dimension_code IS NOT NULL)
        )
    """).fetchone()[0]
    pending(findings, "Country", "ISO remaps applied (HR/BG/GU/UZ; Unidentified->NULL)", unmapped_countries == 0,
            f"{unmapped_countries} unmapped rows" if unmapped_countries else "clean")

    legacy_telecom = con.execute("""
        SELECT COUNT(*) FROM silver.product_dimensions
        WHERE dimension_type = 'industry' AND dimension_name = 'Telecommunication Services-Discontinued eff 09/19/2020'
    """).fetchone()[0]
    pending(findings, "Industry", "discontinued telecom remapped to Communication Services", legacy_telecom == 0,
            f"{legacy_telecom} legacy rows remain" if legacy_telecom else "clean")

    mat_null = con.execute(
        "SELECT COUNT(*) FROM silver.product_dimensions WHERE dimension_type = 'maturity' AND dimension_code IS NULL"
    ).fetchone()[0]
    pending(findings, "Maturity", "dimension_code populated with mat_* slugs", mat_null == 0,
            f"{mat_null:,} rows still NULL" if mat_null else "clean")

    panel_exists = has_table(con, "silver", "monthly_panel")
    panel_detail = ""
    if panel_exists:
        n_rows = con.execute("SELECT COUNT(*) FROM silver.monthly_panel").fetchone()[0]
        panel_detail = f"{n_rows:,} rows"
    pending(findings, "Panel", "silver.monthly_panel built", panel_exists, panel_detail or "not found")


# ==============================================================================
# 9. STATISTICAL CATALOG
# ==============================================================================

def run_catalog(con: duckdb.DuckDBPyConnection) -> tuple[
    dict[str, Any], list[tuple], list[tuple], list[tuple], list[tuple], list[tuple]
]:
    print_section("9. STATISTICAL CATALOG", "Distributional profiling across all metrics and dimensions")

    contracts_n = con.execute("SELECT COUNT(DISTINCT product_id) FROM bronze.contracts").fetchone()[0] if has_table(con, "bronze", "contracts") else 0
    priced_n = con.execute("SELECT COUNT(DISTINCT product_id) FROM silver.products").fetchone()[0] if has_table(con, "silver", "products") else 0
    m_rows, m_funds = con.execute("SELECT COUNT(*), COUNT(DISTINCT product_id) FROM silver.product_metrics").fetchone()
    d_rows, d_funds = con.execute("SELECT COUNT(*), COUNT(DISTINCT product_id) FROM silver.product_dimensions").fetchone()

    univ_base = priced_n or contracts_n or max(m_funds, d_funds, 1)

    footprint_meta = {
        "contracts_n": contracts_n,
        "priced_n": priced_n,
        "metric_rows": m_rows,
        "metric_funds": m_funds,
        "dimension_rows": d_rows,
        "dimension_funds": d_funds,
    }

    render_table(
        "Universe Footprint",
        ["Contracts", "Priced Products", "Metric Rows", "Metric Funds", "Dim Rows", "Dim Funds"],
        [[f"{contracts_n:,}", f"{priced_n:,}", f"{m_rows:,}", f"{m_funds:,}", f"{d_rows:,}", f"{d_funds:,}"]],
        align_right=[0, 1, 2, 3, 4, 5],
    )

    dim_summary_rows = con.execute(f"""
        SELECT
            dimension_type,
            COUNT(*) AS n_rows,
            COUNT(DISTINCT dimension_name) AS n_names,
            COUNT(DISTINCT dimension_code) AS n_codes,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(COUNT(DISTINCT product_id) * 100.0 / {univ_base}, 1) AS pct_universe,
            MIN(effective_date) AS min_dt,
            MAX(effective_date) AS max_dt,
            ROUND(MIN(value), 4) AS min_v,
            ROUND(MEDIAN(value), 4) AS med_v,
            ROUND(MAX(value), 4) AS max_v,
            COUNT(*) FILTER (WHERE value < 0) AS n_neg,
            COUNT(*) FILTER (WHERE value > 1.0) AS n_gt1
        FROM silver.product_dimensions
        GROUP BY dimension_type
        ORDER BY n_rows DESC
    """).fetchall()

    render_table(
        "Dimensions Rollup by dimension_type",
        ["Dimension Type", "Rows", "Names", "Codes", "Funds", "Univ %", "First Date", "Last Date", "Min", "Median", "Max", "Neg", ">1.0"],
        [
            [r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]:,}", f"{r[4]:,}", f"{r[5]}%", str(r[6]), str(r[7]),
             fmt_f(r[8]), fmt_f(r[9]), fmt_f(r[10]), f"{r[11]:,}", f"{r[12]:,}"]
            for r in dim_summary_rows
        ],
        align_right=[1, 2, 3, 4, 5, 8, 9, 10, 11, 12],
    )

    has_ccy = has_column(con, "silver", "product_metrics", "currency")
    ccy_expr = "COUNT(DISTINCT currency)" if has_ccy else "0"

    metrics_rows = con.execute(f"""
        SELECT
            source,
            metric_id,
            COUNT(*) AS n_obs,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(COUNT(DISTINCT product_id) * 100.0 / {univ_base}, 1) AS pct_universe,
            MIN(effective_date) AS min_dt,
            MAX(effective_date) AS max_dt,
            ROUND(MIN(value), 4) AS min_v,
            ROUND(quantile_cont(value, 0.25), 4) AS q25_v,
            ROUND(MEDIAN(value), 4) AS med_v,
            ROUND(quantile_cont(value, 0.75), 4) AS q75_v,
            ROUND(MAX(value), 4) AS max_v,
            ROUND(AVG(value), 4) AS mean_v,
            ROUND(STDDEV_SAMP(value), 4) AS std_v,
            COUNT(*) FILTER (WHERE value = 0) AS n_zero,
            COUNT(*) FILTER (WHERE value < 0) AS n_neg,
            {ccy_expr} AS n_ccy,
            ROUND(COUNT(*) FILTER (WHERE effective_date_source='item') * 100.0 / COUNT(*), 0) AS pct_item,
            ROUND(COUNT(*) FILTER (WHERE effective_date_source='payload') * 100.0 / COUNT(*), 0) AS pct_payload,
            ROUND(COUNT(*) FILTER (WHERE effective_date_source='snapshot') * 100.0 / COUNT(*), 0) AS pct_snap
        FROM silver.product_metrics
        GROUP BY source, metric_id
        ORDER BY source, metric_id
    """).fetchall()

    render_table(
        f"silver.product_metrics Profile ({len(metrics_rows)} metrics)",
        ["Source", "Metric ID", "Obs", "Funds", "Univ %", "First Date", "Last Date", "Min", "Median", "Max", "Mean", "StdDev", "Negs", "Ccys"],
        [
            [r[0], r[1], f"{r[2]:,}", f"{r[3]:,}", f"{r[4]}%", str(r[5]), str(r[6]),
             fmt_f(r[7]), fmt_f(r[9]), fmt_f(r[11]), fmt_f(r[12]), fmt_f(r[13]), f"{r[15]:,}", str(r[16])]
            for r in metrics_rows
        ],
        align_right=[2, 3, 4, 7, 8, 9, 10, 11, 12, 13],
    )

    dims_rows = con.execute(f"""
        SELECT
            dimension_type,
            dimension_name,
            COALESCE(ANY_VALUE(dimension_code), '[NONE]') AS sample_code,
            COUNT(*) AS n_obs,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(COUNT(DISTINCT product_id) * 100.0 / {univ_base}, 1) AS pct_universe,
            MIN(effective_date) AS min_dt,
            MAX(effective_date) AS max_dt,
            ROUND(MIN(value), 4) AS min_v,
            ROUND(quantile_cont(value, 0.25), 4) AS q25_v,
            ROUND(MEDIAN(value), 4) AS med_v,
            ROUND(quantile_cont(value, 0.75), 4) AS q75_v,
            ROUND(MAX(value), 4) AS max_v,
            ROUND(AVG(value), 4) AS mean_v,
            ROUND(STDDEV_SAMP(value), 4) AS std_v,
            COUNT(*) FILTER (WHERE value = 0) AS n_zero,
            COUNT(*) FILTER (WHERE value < 0) AS n_neg,
            COUNT(*) FILTER (WHERE value > 1.0) AS n_gt1
        FROM silver.product_dimensions
        GROUP BY dimension_type, dimension_name
        ORDER BY dimension_type, n_obs DESC
    """).fetchall()

    style_box_raw = con.execute("""
        SELECT dimension_code,
               COUNT(*) FILTER (WHERE dimension_type='style_box') AS selected,
               COUNT(*) FILTER (WHERE dimension_type='style_box_hist') AS hist,
               COUNT(DISTINCT product_id) AS n_funds
        FROM silver.product_dimensions WHERE dimension_type IN ('style_box','style_box_hist')
        GROUP BY 1
    """).fetchall()
    occ = {r[0]: r for r in style_box_raw}

    style_box_rows = [
        (
            cell,
            occ[cell][1] if cell in occ else 0,
            occ[cell][2] if cell in occ else 0,
            occ[cell][3] if cell in occ else 0,
            1 if cell in occ else 0,
        )
        for cell in sorted(STYLE_BOX_GRID)
    ]

    render_table(
        "Style Box Grid Occupancy (12 standard cells)",
        ["Cell", "Selected", "Hist", "Funds", "Observed?"],
        [
            [r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]:,}", "YES" if r[4] else "NO"]
            for r in style_box_rows
        ],
        align_right=[1, 2, 3],
    )

    aum_tokens = con.execute(r"""
        WITH parsed AS (
            SELECT REGEXP_EXTRACT(raw_value, '^([^0-9\s]+)', 1) AS token, value
            FROM silver.product_metrics WHERE metric_id = 'total_net_assets_local'
        )
        SELECT COALESCE(token, '[NONE]'), COUNT(*), ROUND(MIN(value), 0), ROUND(MAX(value), 0)
        FROM parsed GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()

    return footprint_meta, metrics_rows, dims_rows, dim_summary_rows, style_box_rows, aum_tokens


# ==============================================================================
# 10. SQLITE EXPORT
# ==============================================================================

def export_to_sqlite(
    db_path: Path,
    footprint: dict[str, Any],
    metrics_rows: list[tuple],
    dims_rows: list[tuple],
    dim_summary_rows: list[tuple],
    style_box_rows: list[tuple],
    aum_token_rows: list[tuple],
    debt_cluster_rows: list[tuple],
    sleeve_rows: list[tuple],
    fee_alloc_rows: list[tuple],
    ep_summary: list[tuple],
    payload_deltas: list[tuple],
    dwell_summary: list[tuple],
    pub_lag: list[tuple],
    cadence_rows: list[tuple],
    reach_rows: list[tuple],
    ccy_rows: list[tuple],
    sim_results: list[tuple],
    findings: list[Finding],
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    scon = sqlite3.connect(str(db_path))
    cur = scon.cursor()

    cur.execute("""
        CREATE TABLE universe (
            contracts_n INTEGER,
            priced_n INTEGER,
            metric_rows INTEGER,
            metric_funds INTEGER,
            dimension_rows INTEGER,
            dimension_funds INTEGER
        )
    """)
    cur.execute("INSERT INTO universe VALUES (?,?,?,?,?,?)", (
        footprint.get("contracts_n", 0), footprint.get("priced_n", 0),
        footprint.get("metric_rows", 0), footprint.get("metric_funds", 0),
        footprint.get("dimension_rows", 0), footprint.get("dimension_funds", 0),
    ))

    cur.execute("""
        CREATE TABLE metrics (
            source TEXT,
            metric_id TEXT,
            n_obs INTEGER,
            n_funds INTEGER,
            pct_universe REAL,
            min_date TEXT,
            max_date TEXT,
            min_val REAL,
            q25_val REAL,
            median_val REAL,
            q75_val REAL,
            max_val REAL,
            mean_val REAL,
            std_val REAL,
            n_zero INTEGER,
            n_neg INTEGER,
            n_ccy INTEGER,
            pct_item REAL,
            pct_payload REAL,
            pct_snapshot REAL
        )
    """)
    cur.executemany(
        "INSERT INTO metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                r[0], r[1], r[2], r[3], r[4], str(r[5]), str(r[6]),
                r[7], r[8], r[9], r[10], r[11], r[12], r[13],
                r[14], r[15], r[16], r[17], r[18], r[19],
            )
            for r in metrics_rows
        ],
    )

    cur.execute("""
        CREATE TABLE dimensions (
            dimension_type TEXT,
            dimension_name TEXT,
            sample_code TEXT,
            n_obs INTEGER,
            n_funds INTEGER,
            pct_universe REAL,
            min_date TEXT,
            max_date TEXT,
            min_val REAL,
            q25_val REAL,
            median_val REAL,
            q75_val REAL,
            max_val REAL,
            mean_val REAL,
            std_val REAL,
            n_zero INTEGER,
            n_neg INTEGER,
            n_gt1 INTEGER
        )
    """)
    cur.executemany(
        "INSERT INTO dimensions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                r[0], r[1], r[2], r[3], r[4], r[5], str(r[6]), str(r[7]),
                r[8], r[9], r[10], r[11], r[12], r[13], r[14],
                r[15], r[16], r[17],
            )
            for r in dims_rows
        ],
    )

    cur.execute("""
        CREATE TABLE dimensions_summary (
            dimension_type TEXT,
            n_rows INTEGER,
            n_names INTEGER,
            n_codes INTEGER,
            n_funds INTEGER,
            pct_universe REAL,
            min_date TEXT,
            max_date TEXT,
            min_val REAL,
            median_val REAL,
            max_val REAL,
            n_neg INTEGER,
            n_gt1 INTEGER
        )
    """)
    cur.executemany(
        "INSERT INTO dimensions_summary VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                r[0], r[1], r[2], r[3], r[4], r[5], str(r[6]), str(r[7]),
                r[8], r[9], r[10], r[11], r[12],
            )
            for r in dim_summary_rows
        ],
    )

    cur.execute("""
        CREATE TABLE findings (
            category TEXT,
            section TEXT,
            name TEXT,
            status TEXT,
            details TEXT
        )
    """)
    cur.executemany(
        "INSERT INTO findings VALUES (?,?,?,?,?)",
        [(f.category, f.section, f.name, f.status, f.details) for f in findings],
    )

    cur.execute("""
        CREATE TABLE style_box (
            cell TEXT,
            selected INTEGER,
            hist INTEGER,
            funds INTEGER,
            is_observed INTEGER
        )
    """)
    cur.executemany("INSERT INTO style_box VALUES (?,?,?,?,?)", style_box_rows)

    cur.execute("""
        CREATE TABLE aum_tokens (
            token TEXT,
            n_obs INTEGER,
            min_aum REAL,
            max_aum REAL
        )
    """)
    cur.executemany("INSERT INTO aum_tokens VALUES (?,?,?,?)", aum_token_rows)

    cur.execute("""
        CREATE TABLE debt_clusters (
            cluster_id TEXT,
            n_types INTEGER,
            n_obs INTEGER,
            n_funds INTEGER,
            sum_weight REAL,
            avg_weight REAL
        )
    """)
    cur.executemany("INSERT INTO debt_clusters VALUES (?,?,?,?,?,?)", debt_cluster_rows)

    cur.execute("""
        CREATE TABLE sleeve_allocation_summary (
            dimension_type TEXT,
            total_snapshots INTEGER,
            exact_one_cnt INTEGER,
            exact_one_pct REAL,
            levered_cnt INTEGER,
            under_cnt INTEGER,
            min_sum REAL,
            med_sum REAL,
            max_sum REAL,
            with_neg_cnt INTEGER
        )
    """)
    cur.executemany("INSERT INTO sleeve_allocation_summary VALUES (?,?,?,?,?,?,?,?,?,?)", sleeve_rows)

    cur.execute("""
        CREATE TABLE fee_allocation (
            total_pairs INTEGER,
            exact_one_cnt INTEGER,
            exact_one_pct REAL,
            with_subsidies INTEGER,
            zero_non_mgt INTEGER,
            min_mgt REAL,
            max_mgt REAL,
            min_non_mgt REAL,
            max_non_mgt REAL
        )
    """)
    cur.executemany("INSERT INTO fee_allocation VALUES (?,?,?,?,?,?,?,?,?)", fee_alloc_rows)

    cur.execute("""
        CREATE TABLE payload_endpoint_summary (
            url_prefix TEXT,
            total_snapshots INTEGER,
            total_products INTEGER,
            distinct_hashes INTEGER,
            avg_snaps_per_product REAL,
            first_seen TEXT,
            last_seen TEXT
        )
    """)
    cur.executemany("INSERT INTO payload_endpoint_summary VALUES (?,?,?,?,?,?,?)", [
        (r[0], r[1], r[2], r[3], r[4], str(r[5]), str(r[6])) for r in ep_summary
    ])

    cur.execute("""
        CREATE TABLE payload_mutation_cadence (
            url_prefix TEXT,
            update_events INTEGER,
            med_days_between_payloads REAL,
            p25_days REAL,
            p75_days REAL,
            p95_days REAL,
            max_days REAL,
            pct_weekly REAL,
            pct_monthly REAL
        )
    """)
    cur.executemany("INSERT INTO payload_mutation_cadence VALUES (?,?,?,?,?,?,?,?,?)", payload_deltas)

    cur.execute("""
        CREATE TABLE payload_dwell_stability (
            url_prefix TEXT,
            total_snapshots INTEGER,
            single_poll_snaps INTEGER,
            pct_single_poll REAL,
            median_dwell_days REAL,
            p75_dwell_days REAL,
            max_dwell_days REAL
        )
    """)
    cur.executemany("INSERT INTO payload_dwell_stability VALUES (?,?,?,?,?,?,?)", dwell_summary)

    cur.execute("""
        CREATE TABLE publication_lag (
            source TEXT,
            effective_date_source TEXT,
            n_obs INTEGER,
            min_lag INTEGER,
            p25_lag INTEGER,
            median_lag INTEGER,
            p75_lag INTEGER,
            p95_lag INTEGER,
            max_lag INTEGER,
            negative_lag_cnt INTEGER
        )
    """)
    cur.executemany(
        "INSERT INTO publication_lag VALUES (?,?,?,?,?,?,?,?,?,?)",
        [
            (
                r[0],
                r[1],
                r[2],
                _int_or_none(r[3]),
                _int_or_none(r[4]),
                _int_or_none(r[5]),
                _int_or_none(r[6]),
                _int_or_none(r[7]),
                _int_or_none(r[8]),
                r[9],
            )
            for r in pub_lag
        ],
    )

    cur.execute("""
        CREATE TABLE effective_date_cadence (
            source TEXT,
            feature_id TEXT,
            total_transitions INTEGER,
            funds_with_updates INTEGER,
            min_gap_days INTEGER,
            p25_gap_days INTEGER,
            median_gap_days INTEGER,
            p75_gap_days INTEGER,
            p90_gap_days INTEGER,
            p95_gap_days INTEGER,
            p99_gap_days INTEGER,
            max_gap_days INTEGER
        )
    """)
    cur.executemany(
        "INSERT INTO effective_date_cadence VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                r[0], r[1], r[2], r[3],
                _int_or_none(r[4]), _int_or_none(r[5]), _int_or_none(r[6]),
                _int_or_none(r[7]), _int_or_none(r[8]), _int_or_none(r[9]),
                _int_or_none(r[10]), _int_or_none(r[11]),
            )
            for r in cadence_rows
        ],
    )

    cur.execute("""
        CREATE TABLE first_observation_reach (
            family TEXT,
            total_funds INTEGER,
            funds_with_pre_price INTEGER,
            pct_pre_price REAL,
            median_pre_days INTEGER,
            p75_pre_days INTEGER,
            p95_pre_days INTEGER,
            max_pre_days INTEGER
        )
    """)
    cur.executemany(
        "INSERT INTO first_observation_reach VALUES (?,?,?,?,?,?,?,?)",
        [
            (
                r[0], r[1], r[2], r[3],
                _int_or_none(r[4]), _int_or_none(r[5]), _int_or_none(r[6]), _int_or_none(r[7]),
            )
            for r in reach_rows
        ],
    )

    cur.execute("""
        CREATE TABLE aum_currency_distribution (
            currency TEXT,
            obs_count INTEGER,
            fund_count INTEGER,
            min_local_val REAL,
            median_local_val REAL,
            max_local_val REAL
        )
    """)
    cur.executemany("INSERT INTO aum_currency_distribution VALUES (?,?,?,?,?,?)", ccy_rows)

    cur.execute("""
        CREATE TABLE locf_coverage_simulation (
            feature_name TEXT,
            target_type TEXT,
            current_cap_days INTEGER,
            p95_cap_days INTEGER,
            eligible_spine_months INTEGER,
            current_cap_months INTEGER,
            current_cap_pct REAL,
            p95_cap_months INTEGER,
            p95_cap_pct REAL,
            bfill_months INTEGER,
            bfill_pct REAL,
            bfill_gain_months INTEGER
        )
    """)
    cur.executemany("INSERT INTO locf_coverage_simulation VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", sim_results)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_metrics_source ON metrics(source);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_metrics_metric_id ON metrics(metric_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dimensions_type ON dimensions(dimension_type);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_findings_category ON findings(category);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cadence_feature ON effective_date_cadence(feature_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sim_feature ON locf_coverage_simulation(feature_name);")

    scon.commit()
    scon.close()

    print(f"\nSaved untruncated SQLite catalog (18 analytical tables) -> {db_path}")


# ==============================================================================
# 11. SCORECARD
# ==============================================================================

def print_scorecard(findings: list[Finding]) -> int:
    print_section("11. SCORECARD", "Structural invariants pass/fail & open migration items")

    inv = [f for f in findings if f.category == "INVARIANT"]
    watch = [f for f in findings if f.category == "WATCHLIST"]
    pend = [f for f in findings if f.category == "PENDING"]

    inv_fail = [f for f in inv if f.status == "FAIL"]
    watch_new = [f for f in watch if f.status == "NEW"]
    pend_open = [f for f in pend if f.status == "PENDING"]

    rows = [[f.category, f.section, f.name, f.status, f.details] for f in findings]
    render_table(
        f"Summary: Invariants {len(inv) - len(inv_fail)}/{len(inv)} PASS | "
        f"Watchlist {len(watch_new)} NEW | "
        f"Pending {len(pend) - len(pend_open)}/{len(pend)} LANDED",
        ["Category", "Section", "Rule / Check", "Status", "Details"],
        rows,
    )

    summary = (
        f"INVARIANTS: {len(inv) - len(inv_fail)}/{len(inv)} passed, {len(inv_fail)} failed. "
        f"WATCHLIST: {len(watch_new)} new domain entries. "
        f"PENDING: {len(pend) - len(pend_open)}/{len(pend)} landed."
    )
    print(f"\n{summary}\n")
    return 1 if inv_fail else 0


# ==============================================================================
# MAIN
# ==============================================================================

def main() -> int:
    db_path = Path("data/etf.duckdb")
    sqlite_path = Path("data/silver_catalog.sqlite")

    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        return 1

    print(f"Connected to DuckDB: {db_path} (READ_ONLY)")
    t0 = time.time()
    con = duckdb.connect(str(db_path), read_only=True)
    findings: list[Finding] = []

    try:
        run_invariants(con, findings)
        ep_summary, payload_deltas, dwell_summary, pub_lag = run_payload_cadence(con)
        cadence_rows = run_effective_date_cadence(con)
        reach_rows = run_backward_fill_audit(con)
        ccy_rows = run_currency_profile(con)
        sim_results = run_locf_coverage_simulation(con, cadence_rows)
        sleeve_rows, debt_cluster_rows, fee_alloc_rows = run_empirical_profiles(con)
        run_pending(con, findings)
        footprint, metrics_rows, dims_rows, dim_summary_rows, style_box_rows, aum_tokens = run_catalog(con)

        rc = print_scorecard(findings)

        export_to_sqlite(
            sqlite_path,
            footprint,
            metrics_rows,
            dims_rows,
            dim_summary_rows,
            style_box_rows,
            aum_tokens,
            debt_cluster_rows,
            sleeve_rows,
            fee_alloc_rows,
            ep_summary,
            payload_deltas,
            dwell_summary,
            pub_lag,
            cadence_rows,
            reach_rows,
            ccy_rows,
            sim_results,
            findings,
        )

        elapsed = time.time() - t0
        print(f"Finished in {elapsed:.2f}s (exit {rc}).\n")
        return rc
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())