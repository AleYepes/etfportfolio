#!/usr/bin/env python3
"""
Silver Layer Catalog & Data Contract Audit Profiler
---------------------------------------------------
Profiles the Silver Layer to audit data integrity, calibrate panel LOCF horizons,
and provide a comprehensive accounting of metrics/dimensions for Phase 1 refactoring.

Outputs:
  1. Terminal console: Formatted summaries, watchlists, invariants, and scorecard.
  2. Log file (data/silver_catalog.log): Untruncated, structured Markdown report
     specifically designed for LLM agents to cross-reference with FRD_architecture.md.
  3. Disposable SQLite database (data/silver_catalog.sqlite): Analytical tables
     for ad-hoc SQL validation.

Usage:
    uv run scripts/catalog_silver.py
"""

from __future__ import annotations

import datetime
import re
import sqlite3
import sys
import time
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

# ==============================================================================
# FRD TARGET ENUMS, VOCABULARIES & DOCUMENTED DOMAINS
# ==============================================================================

DATE_SOURCES = {"payload", "item", "snapshot"}
ASSET_CLASSES = {"Equity", "Fixed Income", "Cash", "Other"}

STYLE_SIZES = ["large", "multi", "mid", "small"]
STYLE_STYLES = ["value", "core", "growth"]
STYLE_BOX_GRID = {f"{sz}_{st}" for sz in STYLE_SIZES for st in STYLE_STYLES}

AUM_PREFIX_TOKENS = {
    "$",
    "€",
    "£",
    "¥",
    "₹",
    "CAD",
    "AUD",
    "CNY",
    "TWD",
    "HKD",
    "CHF",
    "BRL",
    "SGD",
    "MXN",
    "KRW",
    "MYR",
    "CNH",
    "AED",
    "SEK",
    "ZAR",
    "ILS",
    "SAR",
    "NOK",
    "HUF",
    "DKK",
    "VND",
}

CREDIT_RATING_GRADES = {
    "AAA",
    "AA",
    "A",
    "BBB",
    "BB",
    "B",
    "CCC",
    "CC",
    "C",
    "D",
    "Not Rated",
    "Not Available",
}

MATURITY_SLUGS = {
    "mat_lt_1y",
    "mat_1_to_3y",
    "mat_3_to_5y",
    "mat_5_to_10y",
    "mat_10_to_20y",
    "mat_20_to_30y",
    "mat_gt_30y",
    "mat_other",
}

DEBT_CLUSTERS: dict[str, tuple[str, ...]] = {
    "debt_sovereign": (
        "Sovereign Bond",
        "Bundesanleihen",
        "Dutch State Loan",
        "Gilt Treasury Stock",
        "Irish Govt Bond",
        "Japanese Govt Bond",
        "Danish Govt Bond",
        "Notas do Tesouro Nacional F",
        "Obligaciones del Estado",
        "Obligation Assimilable du Tresor",
        "Oblig Assim Tresor Indexee I'Indice",
        "Oblig Assim Tresor Indexee I'Inflation",
        "Obligation Lineaire",
        "Obrigacoes do Tesouro",
        "Titulos de Tesoreria TES B",
        "Treasury Bills",
        "Treasury Notes/Bonds",
        "Treasury STRIPS",
        "MXBONO",
        "UDIBONO",
        "OMAN",
        "Govt Guaranteed",
        "Government other",
    ),
    "debt_agency_supranational": ("Agencies", "Small Business Administration"),
    "debt_municipal": (
        "MUNI",
        "Certificates of Obligation",
        "Certificates of Participation",
        "Grant Antic Notes",
        "Tax And Rev Antic Notes",
        "Tax Antic Notes",
        "Unknown Antic Types",
    ),
    "debt_corporate_senior": (
        "CORP",
        "Corporate Medium Term Notes",
        "Senior Note",
        "Senior Debenture",
        "Senior Bank Note",
        "Senior Secured",
        "Secured Bond",
        "Secured Note",
        "First Mortgage Bond",
        "First Mortgage Note",
        "First & Refunding Mortgage Bond",
        "Covered Bond",
        "Hypothekenpfandbrief",
        "Pfandbrief Anleihe",
        "Oeffentliche Pfandbrief",
        "HPF Jumbo",
        "Jumbo Landesschatzanweisung",
        "Sakerstallda Obligationer",
        "Obligations Foncieres",
        "Collateral Trust",
        "Collateral Debt",
        "Collateralized Notes",
    ),
    "debt_corporate_subordinated": (
        "Subordinated Note",
        "Senior Subordinated Note",
        "Subordinated Bank Note",
        "Subordinated Debenture",
        "Senior Subordinated Debenture",
        "Junior Subordinated Note",
        "Junior Subordinated Debenture",
        "Mezzanine Debt",
        "Trust Preferred Security",
        "Participaciones Preferentes",
    ),
    "debt_securitized_mbs": (
        "Mortgage Pools",
        "Mortgages",
        "Mortgage Bond",
        "Mortgage Note",
        "Second Mortgage Bond",
        "Commercial Mortgage-Backed Security",
        "Collateralized Mortgage Obligation",
        "CMOs",
        "CMO Whole Loan",
        "CMO Agricultural MBS",
        "TBA",
        "Pass Through Certificate",
    ),
    "debt_securitized_abs": (
        "ABSY",
        "Asset Backed Tranches",
        "Credit Card Receivables",
        "Auto/Installment Loans",
        "Auto Lease Loans",
        "Auto Floorplan/Wholesale Loans",
        "Equipment Backed Loan",
        "Aircraft Lease",
        "Student Loan",
    ),
    "debt_unsecured_general": (
        "Bond",
        "Note",
        "Unsecured Note",
        "Debenture",
        "Fixed Income",
        "Global Bonds",
        "Inhaberschuldverschreibung",
        "Certificate",
        "Certificates Of Indebtness",
        "Other Certificates",
        "Deposit Note",
        "Depositary Share",
        "Depository Receipts (Thailand)",
        "Bank Debt",
        "Bankers Acceptance",
        "Trust",
    ),
    "debt_specialty_derivatives": (
        "Index Linked Security",
        "Index-Linked Gilt",
        "Islamic Sukuk",
        "Derivative",
        "Interest only",
        "Principal only",
        "Warrants",
        "Preferred Stock",
        "OTHER",
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

# Reference definitions from FRD §5.1 & §6.1
FRD_PROFILE_METRICS = {
    "total_expense_ratio",
    "total_net_assets_local",
    "is_passive",
    "manager_tenure_years",
    "audited_net_expense_ratio",
    "management_expense_ratio",
    "non_management_expense_ratio",
}

FRD_HOLDINGS_METRICS = {
    "portfolio_top_10_concentration",
}

FRD_ESG_METRICS = {
    "esg_coverage",
    "tresgs",
    "tresgcs",
    "tresgccs",
    "tresgens",
    "tresgenrrs",
    "tresgeners",
    "tresgenpis",
    "tresgsos",
    "tresgsowos",
    "tresgsohrs",
    "tresgsocos",
    "tresgsoprs",
    "tresgcgs",
    "tresgcgbds",
    "tresgcgsrs",
    "tresgcgvss",
}

FRD_MSTAR_METRICS = {
    "mstar_medalist_rating",
    "mstar_analyst_coverage_pct",
    "mstar_morningstar_rating",
    "mstar_sustainability_rating",
    "mstar_people_analyst",
    "mstar_people_quant",
    "mstar_process_analyst",
    "mstar_process_quant",
    "mstar_parent_analyst",
    "mstar_parent_quant",
}

FRD_THEME_METRICS = {
    "theme_coverage",
}

FRD_RATIOS_PERCENTAGE_METRICS = {
    "eps_growth_1yr",
    "eps_growth_3yr",
    "eps_growth_5yr",
    "sales_growth_1_year",
    "sales_growth_3_year",
    "sales_growth_5_yr",
    "sales_per_share_growth_1_year",
    "sales_per_share_growth_3_year",
    "operating_cash_flow_growth_rate_3yr",
    "return_on_assets_1yr",
    "return_on_assets_3yr",
    "return_on_equity_1yr",
    "return_on_equity_3yr",
    "return_on_investment_1yr",
    "return_on_investment_3yr",
    "return_on_capital",
    "return_on_capital_3yr",
    "dividend_yield_weighted_average",
    "dividendpayoutratio5yr",
    "dividend_per_share_1yr",
    "dividend_per_share_3yr",
    "yield_to_maturity",
    "average_coupon",
    "relative_strength",
}

FRD_RATIOS_KNOWN_NON_PERCENTAGE = {
    "total_assets_total_equity",
    "total_debt_total_capital",
    "total_debt_total_equity",
    "lt_debt_shareholders_equity",
    "ebit_to_interest",
    "sales_to_total_assets",
}

FRD_STANDARD_INDUSTRIES = {
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Information Technology",
    "Materials",
    "Real Estate",
    "Utilities",
    "Not Classified - Non Equity",
    "Non Classified Equity",
}

INDUSTRY_NAME_REMAPS = {
    "Telecommunication Services-Discontinued eff 09/19/2020": "Communication Services",
}

SYMBOL_TO_CURRENCIES: dict[str, set[str]] = {
    "$": {"USD", "CAD", "AUD", "MXN", "SGD", "HKD", "NZD", "TWD"},
    "¥": {"JPY", "CNY", "CNH"},
    "£": {"GBP", "EGP", "LBP"},
    "₩": {"KRW", "KPW"},
    "€": {"EUR"},
    "₹": {"INR"},
}

# ==============================================================================
# LOGGING & REPORTING
# ==============================================================================


@dataclass(frozen=True, slots=True)
class Finding:
    category: str
    section: str
    name: str
    status: str
    details: str = ""


class CatalogLogger:
    """Manages dual logging to standard output and a comprehensive Markdown log file."""

    def __init__(self, log_path: Path):
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_file = open(self.log_path, "w", encoding="utf-8")

    def close(self) -> None:
        self.log_file.close()

    def write_header(self, db_path: Path, schema_mode: str) -> None:
        now_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        self.log_file.write(
            f"# Silver Layer Catalog & Data Contract Audit Log\n\n"
            f"- **Execution Timestamp**: {now_str}\n"
            f"- **DuckDB Database**: `{db_path}`\n"
            f"- **Detected Schema Mode**: `{schema_mode}`\n"
            f"- **Target Architecture Reference**: `FRD_architecture.md` (Phase 1 Settled Design)\n\n"
            f"---\n\n"
        )

    def section(self, title: str, subtitle: str | None = None) -> None:
        print(f"\n{'=' * 35} {title} {'=' * 35}")
        self.log_file.write(f"\n## {title}\n")
        if subtitle:
            print(f"  ({subtitle})")
            self.log_file.write(f"*{subtitle}*\n\n")
        else:
            self.log_file.write("\n")

    def subsection(self, title: str, subtitle: str | None = None) -> None:
        print(f"\n--- {title} ---")
        self.log_file.write(f"\n### {title}\n")
        if subtitle:
            print(f"  ({subtitle})")
            self.log_file.write(f"*{subtitle}*\n\n")
        else:
            self.log_file.write("\n")

    def log_text(self, text: str, to_console: bool = True) -> None:
        if to_console:
            print(text)
        self.log_file.write(f"{text}\n")

    def render_table(
        self,
        title: str,
        headers: list[str],
        rows: list[list[Any]],
        align_right: list[int] | None = None,
        max_console_rows: int | None = None,
    ) -> None:
        align_indices = set(align_right or [])

        # Write to Markdown Log File (Untruncated)
        self.log_file.write(f"#### {title}\n\n")
        if not rows:
            self.log_file.write("*(no records)*\n\n")
        else:
            h_line = "| " + " | ".join(str(h) for h in headers) + " |"
            s_line = "| " + " | ".join("---:" if i in align_indices else ":---" for i in range(len(headers))) + " |"
            data_lines = []
            for r in rows:
                row_str = (
                    "| "
                    + " | ".join(str(c).replace("\n", " ").replace("|", "\\|") if c is not None else "NULL" for c in r)
                    + " |"
                )
                data_lines.append(row_str)
            self.log_file.write("\n".join([h_line, s_line] + data_lines) + "\n\n")

        # Write to Console (Optionally Truncated for Readability)
        if not rows:
            print(f"\n--- {title} ---\n(no records)")
            return

        print(f"\n--- {title} ---")
        display_rows = rows if max_console_rows is None else rows[:max_console_rows]
        widths = [max(len(str(x)) for x in [h] + [r[i] for r in display_rows]) for i, h in enumerate(headers)]
        fmt = "  ".join(f"{{:>{w}}}" if i in align_indices else f"{{:<{w}}}" for i, w in enumerate(widths))
        print(fmt.format(*headers))
        print("  ".join("-" * w for w in widths))
        for row in display_rows:
            print(fmt.format(*[str(c) if c is not None else "NULL" for c in row]))
        if max_console_rows is not None and len(rows) > max_console_rows:
            print(f"  ... ({len(rows) - max_console_rows} more records logged in log file)")


def record(
    findings: list[Finding],
    category: str,
    section: str,
    name: str,
    status: str,
    details: str = "",
    logger: CatalogLogger | None = None,
) -> None:
    f = Finding(category, section, name, status, details)
    findings.append(f)
    extra = f" — {details}" if details else ""
    line = f"  [{status}] {name}{extra}"
    print(line)
    if logger:
        logger.log_file.write(f"- **[{status}]** `{name}`{extra}\n")


def invariant(
    findings: list[Finding],
    section: str,
    name: str,
    ok: bool,
    details: str = "",
    logger: CatalogLogger | None = None,
) -> None:
    record(findings, "INVARIANT", section, name, "PASS" if ok else "FAIL", details, logger)


def skip(
    findings: list[Finding],
    section: str,
    name: str,
    reason: str,
    logger: CatalogLogger | None = None,
) -> None:
    record(findings, "INVARIANT", section, name, "SKIP", reason, logger)


def watchlist(
    findings: list[Finding],
    section: str,
    name: str,
    unexpected: Iterable[str],
    context: str = "",
    logger: CatalogLogger | None = None,
) -> None:
    unexpected_list = sorted(set(unexpected))
    status = "NEW" if unexpected_list else "PASS"
    detail = f"new values: {unexpected_list}" if unexpected_list else "no values outside documented set"
    if context:
        detail += f" — {context}"
    record(findings, "WATCHLIST", section, name, status, detail, logger)


def pending(
    findings: list[Finding],
    section: str,
    name: str,
    landed: bool,
    details: str = "",
    logger: CatalogLogger | None = None,
) -> None:
    record(findings, "PENDING", section, name, "LANDED" if landed else "PENDING", details, logger)


def fmt_f(val: Any, decimals: int = 4) -> str:
    if val is None:
        return "NULL"
    try:
        f = float(val)
        return f"{f:,.{decimals}f}"
    except ValueError, TypeError:
        return str(val)


def _fmt_days(val: Any) -> str:
    if val is None:
        return "NULL"
    try:
        return f"{int(round(float(val)))}d"
    except ValueError, TypeError:
        return str(val)


def _int_or_none(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(round(float(val)))
    except ValueError, TypeError:
        return None


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
# SCHEMA DETECTION & ADAPTATION
# ==============================================================================


def detect_and_setup_silver_views(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """
    Detects whether silver.observations exists or legacy product_metrics/dimensions exist.
    Creates temporary views (v_observations, v_product_metrics, v_product_dimensions)
    so all downstream statistical profilers run uniformly on both schemas.
    """
    has_obs = has_table(con, "silver", "observations")
    has_m = has_table(con, "silver", "product_metrics")
    has_d = has_table(con, "silver", "product_dimensions")

    info = {
        "is_unified": has_obs,
        "is_legacy": has_m and has_d,
        "has_observations": has_obs,
        "has_product_metrics": has_m,
        "has_product_dimensions": has_d,
    }

    if has_obs:
        con.execute("""
            CREATE OR REPLACE TEMP VIEW v_observations AS
            SELECT
                product_id,
                family,
                metric,
                code,
                effective_date,
                effective_date_source,
                fetched_at,
                value,
                raw_value
            FROM silver.observations;

            CREATE OR REPLACE TEMP VIEW v_product_metrics AS
            SELECT
                product_id,
                family AS source,
                metric AS metric_id,
                effective_date,
                effective_date_source,
                fetched_at,
                value,
                raw_value,
                code AS currency
            FROM silver.observations
            WHERE family IN ('profile', 'holdings', 'ratios', 'esg', 'mstar', 'lipper', 'theme_weights');

            CREATE OR REPLACE TEMP VIEW v_product_dimensions AS
            SELECT
                product_id,
                family AS dimension_type,
                metric AS dimension_name,
                code AS dimension_code,
                effective_date,
                effective_date_source,
                fetched_at,
                value,
                raw_value
            FROM silver.observations
            WHERE family IN ('asset_class', 'country', 'industry', 'credit_rating', 'debt_type', 'maturity', 'theme', 'style_box', 'style_box_hist');
        """)
    elif has_m and has_d:
        has_ccy = has_column(con, "silver", "product_metrics", "currency")
        ccy_col = "currency" if has_ccy else "NULL::VARCHAR AS currency"
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW v_product_metrics AS
            SELECT
                product_id,
                source,
                metric_id,
                effective_date,
                effective_date_source,
                fetched_at,
                value,
                raw_value,
                {ccy_col}
            FROM silver.product_metrics;

            CREATE OR REPLACE TEMP VIEW v_product_dimensions AS
            SELECT
                product_id,
                dimension_type,
                dimension_name,
                dimension_code,
                effective_date,
                effective_date_source,
                fetched_at,
                value,
                raw_value
            FROM silver.product_dimensions;

            CREATE OR REPLACE TEMP VIEW v_observations AS
            SELECT
                product_id,
                source AS family,
                metric_id AS metric,
                {ccy_col} AS code,
                effective_date,
                effective_date_source,
                fetched_at,
                value,
                raw_value
            FROM silver.product_metrics
            UNION ALL
            SELECT
                product_id,
                dimension_type AS family,
                dimension_name AS metric,
                dimension_code AS code,
                effective_date,
                effective_date_source,
                fetched_at,
                value,
                raw_value
            FROM silver.product_dimensions;
        """)
    else:
        # Empty database or uninitialized silver
        con.execute("""
            CREATE OR REPLACE TEMP VIEW v_observations AS
            SELECT
                1::INTEGER AS product_id,
                ''::VARCHAR AS family,
                ''::VARCHAR AS metric,
                ''::VARCHAR AS code,
                CURRENT_DATE AS effective_date,
                ''::VARCHAR AS effective_date_source,
                CURRENT_TIMESTAMP AS fetched_at,
                0.0::DOUBLE AS value,
                ''::VARCHAR AS raw_value
            WHERE 1 = 0;

            CREATE OR REPLACE TEMP VIEW v_product_metrics AS
            SELECT
                1::INTEGER AS product_id,
                ''::VARCHAR AS source,
                ''::VARCHAR AS metric_id,
                CURRENT_DATE AS effective_date,
                ''::VARCHAR AS effective_date_source,
                CURRENT_TIMESTAMP AS fetched_at,
                0.0::DOUBLE AS value,
                ''::VARCHAR AS raw_value,
                ''::VARCHAR AS currency
            WHERE 1 = 0;

            CREATE OR REPLACE TEMP VIEW v_product_dimensions AS
            SELECT
                1::INTEGER AS product_id,
                ''::VARCHAR AS dimension_type,
                ''::VARCHAR AS dimension_name,
                ''::VARCHAR AS dimension_code,
                CURRENT_DATE AS effective_date,
                ''::VARCHAR AS effective_date_source,
                CURRENT_TIMESTAMP AS fetched_at,
                0.0::DOUBLE AS value,
                ''::VARCHAR AS raw_value
            WHERE 1 = 0;
        """)

    return info


# ==============================================================================
# 1. HARD STRUCTURAL INVARIANTS
# ==============================================================================


def run_invariants(
    con: duckdb.DuckDBPyConnection,
    findings: list[Finding],
    schema_info: dict[str, Any],
    logger: CatalogLogger,
) -> None:
    logger.section(
        "1. STRUCTURAL INVARIANTS",
        "Primary keys, nullability, numeric finiteness, temporal causality, unified PK readiness",
    )

    is_unified = schema_info["is_unified"]

    if is_unified:
        pk_dup = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT product_id, family, metric, effective_date FROM silver.observations
                GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        invariant(
            findings,
            "Structural",
            "PK uniqueness in silver.observations (product_id, family, metric, effective_date)",
            pk_dup == 0,
            f"{pk_dup} duplicate PK groups",
            logger,
        )
    else:
        pk_dup_m = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT product_id, source, metric_id, effective_date FROM v_product_metrics
                GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        invariant(
            findings,
            "Structural",
            "PK uniqueness in silver.product_metrics",
            pk_dup_m == 0,
            f"{pk_dup_m} duplicate groups",
            logger,
        )

        pk_dup_d = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT product_id, dimension_type, dimension_name, effective_date FROM v_product_dimensions
                GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        invariant(
            findings,
            "Structural",
            "PK uniqueness in silver.product_dimensions",
            pk_dup_d == 0,
            f"{pk_dup_d} duplicate groups",
            logger,
        )

        cross_pk_dup = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT product_id, family, metric, effective_date FROM v_observations
                GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        invariant(
            findings,
            "Structural",
            "Unified PK feasibility (product_id, family, metric, effective_date) has 0 collisions",
            cross_pk_dup == 0,
            f"{cross_pk_dup} duplicate rows across combined tables",
            logger,
        )

    null_obs = con.execute("""
        SELECT COUNT(*) FILTER (WHERE value IS NULL),
               COUNT(*) FILTER (WHERE raw_value IS NULL),
               COUNT(*) FILTER (WHERE isnan(value) OR isinf(value))
        FROM v_observations
    """).fetchone()
    invariant(
        findings,
        "Structural",
        "value/raw_value NOT NULL, finite in observations",
        null_obs[0] == 0 and null_obs[1] == 0 and null_obs[2] == 0,
        f"null value={null_obs[0]}, null raw={null_obs[1]}, non-finite={null_obs[2]}",
        logger,
    )

    future = con.execute("""
        SELECT COUNT(*) FROM v_observations WHERE effective_date > fetched_at::DATE
    """).fetchone()[0]
    invariant(
        findings,
        "Structural",
        "Temporal causality (effective_date <= fetched_at::DATE)",
        future == 0,
        f"{future} future-dated rows",
        logger,
    )

    if has_table(con, "bronze", "contracts"):
        orphans = con.execute("""
            SELECT COUNT(DISTINCT product_id) FROM v_observations
            WHERE product_id NOT IN (SELECT product_id FROM bronze.contracts)
        """).fetchone()[0]
        invariant(
            findings,
            "Structural",
            "Every product_id resolves to bronze.contracts",
            orphans == 0,
            f"{orphans} orphan product_ids",
            logger,
        )
    else:
        skip(findings, "Structural", "bronze.contracts foreign key checks", "bronze.contracts table not found", logger)

    bad_src = con.execute("""
        SELECT DISTINCT effective_date_source FROM v_observations
        WHERE effective_date_source NOT IN ('payload','item','snapshot')
    """).fetchall()
    invariant(
        findings,
        "Structural",
        "effective_date_source is exactly {payload, item, snapshot}",
        len(bad_src) == 0,
        f"unexpected values: {[r[0] for r in bad_src]}",
        logger,
    )

    # Watchlists
    ac_names = {
        r[0] for r in con.execute("SELECT DISTINCT metric FROM v_observations WHERE family = 'asset_class'").fetchall()
    }
    watchlist(
        findings,
        "Vocabulary",
        "Asset class categories",
        ac_names - ASSET_CLASSES,
        f"{len(ac_names)} observed vs {len(ASSET_CLASSES)} documented",
        logger,
    )

    tokens = {
        r[0]
        for r in con.execute(r"""
            SELECT DISTINCT REGEXP_EXTRACT(raw_value, '^([^0-9\s]+)', 1)
            FROM v_observations WHERE metric = 'total_net_assets_local'
        """).fetchall()
        if r[0]
    }
    watchlist(
        findings,
        "Vocabulary",
        "AUM currency prefix tokens",
        tokens - AUM_PREFIX_TOKENS,
        f"{len(tokens)} observed vs {len(AUM_PREFIX_TOKENS)} documented",
        logger,
    )

    cr_names = {
        r[0]
        for r in con.execute("SELECT DISTINCT metric FROM v_observations WHERE family = 'credit_rating'").fetchall()
    }
    watchlist(
        findings,
        "Vocabulary",
        "Credit rating grade names",
        cr_names - CREDIT_RATING_GRADES,
        f"{len(cr_names)} observed vs {len(CREDIT_RATING_GRADES)} documented",
        logger,
    )

    mat_codes = {
        r[0]
        for r in con.execute(
            "SELECT DISTINCT code FROM v_observations WHERE family = 'maturity' AND code IS NOT NULL"
        ).fetchall()
    }
    watchlist(
        findings,
        "Vocabulary",
        "Maturity bucket slugs",
        mat_codes - MATURITY_SLUGS,
        f"{len(mat_codes)} observed vs {len(MATURITY_SLUGS)} documented",
        logger,
    )

    sb_codes = {
        r[0]
        for r in con.execute(
            "SELECT DISTINCT code FROM v_observations WHERE family IN ('style_box', 'style_box_hist') AND code IS NOT NULL"
        ).fetchall()
    }
    watchlist(
        findings,
        "Vocabulary",
        "Style box dimension codes",
        sb_codes - STYLE_BOX_GRID,
        f"{len(sb_codes)} observed vs {len(STYLE_BOX_GRID)} documented",
        logger,
    )


# ==============================================================================
# 2. PAYLOAD INGESTION & STORAGE CADENCE
# ==============================================================================


def run_payload_cadence(
    con: duckdb.DuckDBPyConnection,
    logger: CatalogLogger,
) -> tuple[list[tuple], list[tuple], list[tuple], list[tuple]]:
    logger.section(
        "2. PAYLOAD INGESTION & STORAGE CADENCE",
        "Analyzing bronze.snapshots: payload mutation frequency, dwell time, and vendor publication lag",
    )

    if not has_table(con, "bronze", "snapshots"):
        logger.log_text("  bronze.snapshots not present in this database — skipping payload cadence.")
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

    logger.render_table(
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

    logger.render_table(
        "Payload Mutation Frequency (Interval between successive bronze.snapshots records)",
        [
            "URL Prefix",
            "Update Events",
            "Median Interval",
            "p25",
            "p75",
            "p95",
            "Max Gap",
            "% Weekly (~7d)",
            "% Monthly (~30d)",
        ],
        [
            [
                r[0],
                f"{r[1]:,}",
                f"{r[2]}d",
                f"{r[3]}d",
                f"{r[4]}d",
                f"{r[5]}d",
                f"{r[6]}d",
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

    logger.render_table(
        "Payload Dwell Time / Stability Window (How long a payload hash remains current)",
        ["URL Prefix", "Snapshots", "Single-Poll Snaps", "% Transient", "Median Dwell", "p75 Dwell", "Max Dwell"],
        [[r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]}%", f"{r[4]}d", f"{r[5]}d", f"{r[6]}d"] for r in dwell_summary],
        align_right=list(range(1, 7)),
    )

    pub_lag = con.execute("""
        WITH lags AS (
            SELECT
                family AS source,
                effective_date_source,
                DATE_DIFF('day', effective_date, fetched_at::DATE) AS lag_days
            FROM v_observations
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

    logger.render_table(
        "Publication Lag (fetched_at - effective_date: vendor reporting latency)",
        [
            "Analytical Family",
            "Date Provenance",
            "Observations",
            "Min Lag",
            "p25 Lag",
            "Median Lag",
            "p75 Lag",
            "p95 Lag",
            "Max Lag",
            "Future Date (<0)",
        ],
        [
            [
                r[0],
                r[1],
                f"{r[2]:,}",
                _fmt_days(r[3]),
                _fmt_days(r[4]),
                _fmt_days(r[5]),
                _fmt_days(r[6]),
                _fmt_days(r[7]),
                _fmt_days(r[8]),
                f"{r[9]:,}",
            ]
            for r in pub_lag
        ],
        align_right=list(range(2, 10)),
    )

    return ep_summary, payload_deltas, dwell_summary, pub_lag


# ==============================================================================
# 3. EMPIRICAL EFFECTIVE DATE CADENCE (LOCF Horizon Calibration)
# ==============================================================================


def run_effective_date_cadence(con: duckdb.DuckDBPyConnection, logger: CatalogLogger) -> list[tuple]:
    logger.section(
        "3. EMPIRICAL EFFECTIVE_DATE CADENCE",
        "Inter-arrival distribution of distinct observation dates per product (data-driven LOCF staleness caps)",
    )

    cadence_rows = con.execute("""
        WITH transitions AS (
            SELECT
                family,
                metric AS feature_id,
                product_id,
                effective_date,
                DATE_DIFF('day', LAG(effective_date) OVER (
                    PARTITION BY product_id, family, metric ORDER BY effective_date
                ), effective_date) AS gap_days
            FROM (
                SELECT DISTINCT product_id, family, metric, effective_date
                FROM v_observations
            )
        )
        SELECT
            family,
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
        FROM transitions
        WHERE gap_days IS NOT NULL AND gap_days > 0
        GROUP BY family, feature_id
        ORDER BY total_transitions DESC, family, feature_id
    """).fetchall()

    logger.render_table(
        f"Empirical effective_date Update Cadence ({len(cadence_rows)} total features, top 25 in console)",
        [
            "Family",
            "Feature / Metric",
            "Transitions",
            "Funds",
            "Min",
            "p25",
            "Median",
            "p75",
            "p90",
            "p95 (Cap Target)",
            "p99",
            "Max",
        ],
        [
            [
                r[0],
                r[1],
                f"{r[2]:,}",
                f"{r[3]:,}",
                _fmt_days(r[4]),
                _fmt_days(r[5]),
                _fmt_days(r[6]),
                _fmt_days(r[7]),
                _fmt_days(r[8]),
                _fmt_days(r[9]),
                _fmt_days(r[10]),
                _fmt_days(r[11]),
            ]
            for r in cadence_rows
        ],
        align_right=list(range(2, 12)),
        max_console_rows=25,
    )

    return cadence_rows


# ==============================================================================
# 4. FIRST OBSERVATION REACH (Backward-Fill Feasibility)
# ==============================================================================


def run_backward_fill_audit(con: duckdb.DuckDBPyConnection, logger: CatalogLogger) -> list[tuple]:
    logger.section(
        "4. FIRST-OBSERVATION REACH & BACKWARD-FILL AUDIT",
        "Assessing price inception vs first fundamental observation date (quantifying backward-fill reach)",
    )

    if not has_table(con, "bronze", "prices"):
        logger.log_text("  bronze.prices not present in this database — skipping backward fill audit.")
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
                family,
                MIN(effective_date) AS first_eff_date
            FROM v_observations
            GROUP BY product_id, family
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

    logger.render_table(
        "First Observation Latency (Days from first price trade to first fundamental observation)",
        ["Family", "Funds", "Pre-Dated Funds", "% Pre-Dated", "Median Gap", "p75 Gap", "p95 Gap", "Max Gap"],
        [
            [
                r[0],
                f"{r[1]:,}",
                f"{r[2]:,}",
                f"{r[3]}%" if r[3] is not None else "0.0%",
                _fmt_days(r[4]),
                _fmt_days(r[5]),
                _fmt_days(r[6]),
                _fmt_days(r[7]),
            ]
            for r in reach_rows
        ],
        align_right=list(range(1, 8)),
    )

    return reach_rows


# ==============================================================================
# 5. CURRENCY DISTRIBUTION & FRD §6.1 DISAMBIGUATION SIMULATION
# ==============================================================================


def simulate_aum_resolution(
    raw_value: str | None,
    contract_currency: str | None,
    known_currencies: set[str],
) -> tuple[str | None, str]:
    """
    Implements the normative FRD §6.1 disambiguate_aum_currency algorithm:
      1. Strip raw_value. Empty -> None.
      2. Starts with 3-letter alpha token (word boundary) in known_currencies -> token.
      3. First char in SYMBOL_TO_CURRENCIES: if contract_currency in candidates -> contract_currency, else None.
      4. Bare number: if contract_currency is 3-letter alpha in known_currencies -> contract_currency, else None.
    """
    if not raw_value or not raw_value.strip():
        return None, "UNRESOLVED_EMPTY"

    s = raw_value.strip()

    # Step 2: 3-letter alpha prefix token
    m = re.match(r"^([A-Za-z]{3})\b", s)
    if m:
        token = m.group(1).upper()
        if token in known_currencies:
            return token, "RESOLVED_ISO_PREFIX"

    # Step 3: First character glyph
    first_char = s[0]
    prod_ccy = contract_currency.strip().upper() if contract_currency else None
    if first_char in SYMBOL_TO_CURRENCIES:
        candidates = SYMBOL_TO_CURRENCIES[first_char]
        if prod_ccy and prod_ccy in candidates:
            return prod_ccy, "RESOLVED_SYMBOL_MATCH"
        return None, f"OMITTED_SYMBOL_MISMATCH (glyph '{first_char}', contract='{prod_ccy}')"

    # Step 4: Bare number
    if prod_ccy and len(prod_ccy) == 3 and prod_ccy.isalpha() and prod_ccy in known_currencies:
        return prod_ccy, "RESOLVED_BARE_CONTRACT"

    return None, f"OMITTED_BARE_UNKNOWN (raw='{s[:12]}', contract='{prod_ccy}')"


def run_currency_profile(
    con: duckdb.DuckDBPyConnection,
    logger: CatalogLogger,
) -> tuple[list[tuple], list[tuple]]:
    logger.section(
        "5. CURRENCY EXPOSURE & FRD §6.1 AUM DISAMBIGUATION",
        "Current total_net_assets_local currency breakdown & simulation of normative FRD §6.1 resolution",
    )

    ccy_rows = con.execute("""
        SELECT
            COALESCE(code, '[NULL]') AS currency,
            COUNT(*) AS obs_count,
            COUNT(DISTINCT product_id) AS fund_count,
            ROUND(MIN(value), 0) AS min_local_val,
            ROUND(MEDIAN(value), 0) AS median_local_val,
            ROUND(MAX(value), 0) AS max_local_val
        FROM v_observations
        WHERE metric = 'total_net_assets_local'
        GROUP BY code
        ORDER BY obs_count DESC
    """).fetchall()

    logger.render_table(
        "total_net_assets_local Current Currency Distribution",
        ["Currency Code", "Observations", "Funds", "Min Local AUM", "Median Local AUM", "Max Local AUM"],
        [[r[0], f"{r[1]:,}", f"{r[2]:,}", fmt_f(r[3], 0), fmt_f(r[4], 0), fmt_f(r[5], 0)] for r in ccy_rows],
        align_right=[1, 2, 3, 4, 5],
    )

    # Simulation of FRD §6.1 disambiguate_aum_currency
    sim_summary_rows: list[tuple] = []
    if has_table(con, "bronze", "contracts"):
        known_ccys = {
            r[0].strip().upper()
            for r in con.execute("SELECT DISTINCT currency FROM bronze.contracts WHERE currency IS NOT NULL").fetchall()
            if r[0] and r[0].strip()
        }

        aum_raw_records = con.execute("""
            SELECT
                o.product_id,
                o.raw_value,
                c.currency AS contract_ccy
            FROM v_observations o
            LEFT JOIN bronze.contracts c ON c.product_id = o.product_id
            WHERE o.metric = 'total_net_assets_local'
        """).fetchall()

        outcome_counts: dict[str, int] = defaultdict(int)
        outcome_funds: dict[str, set[int]] = defaultdict(set)
        resolved_by_ccy: dict[str, int] = defaultdict(int)
        omitted_samples: dict[str, str] = {}

        for pid, raw_v, c_ccy in aum_raw_records:
            resolved_ccy, reason = simulate_aum_resolution(raw_v, c_ccy, known_ccys)
            outcome_counts[reason] += 1
            outcome_funds[reason].add(pid)
            if resolved_ccy:
                resolved_by_ccy[resolved_ccy] += 1
            else:
                if reason not in omitted_samples:
                    omitted_samples[reason] = f"pid={pid}, raw='{raw_v}', contract='{c_ccy}'"

        for outcome, cnt in sorted(outcome_counts.items(), key=lambda x: -x[1]):
            is_resolved = outcome.startswith("RESOLVED")
            action = "PERSIST (code=ISO)" if is_resolved else "OMIT (skip observation)"
            sample = omitted_samples.get(outcome, "clean")
            sim_summary_rows.append((outcome, cnt, len(outcome_funds[outcome]), action, sample))

        logger.render_table(
            "FRD §6.1 AUM Currency Disambiguation Simulation Results",
            ["Resolution Outcome", "Observations", "Funds", "Action (FRD D11)", "Sample Case"],
            [[r[0], f"{r[1]:,}", f"{r[2]:,}", r[3], r[4]] for r in sim_summary_rows],
            align_right=[1, 2],
        )

        resolved_rows = [[ccy, f"{cnt:,}"] for ccy, cnt in sorted(resolved_by_ccy.items(), key=lambda x: -x[1])]
        logger.render_table(
            "Simulated Resolved AUM by ISO-4217 Currency Code",
            ["Resolved Currency", "Retained Observations"],
            resolved_rows,
            align_right=[1],
        )

    return ccy_rows, sim_summary_rows


# ==============================================================================
# 6. LOCF COVERAGE SIMULATION
# ==============================================================================


def run_locf_coverage_simulation(
    con: duckdb.DuckDBPyConnection,
    cadence_rows: list[tuple],
    logger: CatalogLogger,
) -> list[tuple]:
    logger.section(
        "6. LOCF COVERAGE SIMULATION",
        "Comparing monthly panel fill-rates under current caps vs empirical p95 vs backward-fill",
    )

    if not has_table(con, "bronze", "prices"):
        logger.log_text("  bronze.prices not present in this database — skipping coverage simulation.")
        return []

    p95_map: dict[tuple[str, str], int] = {}
    for r in cadence_rows:
        if r[9] is not None:
            p95_map[(str(r[0]), str(r[1]))] = int(r[9])

    sim_results: list[tuple] = []

    con.execute("""
        CREATE OR REPLACE TEMP TABLE active_spine AS
        SELECT DISTINCT product_id, LAST_DAY(date::DATE) AS as_of_date
        FROM bronze.prices
        GROUP BY product_id, LAST_DAY(date::DATE)
    """)

    for target_name, target_type, current_cap in BENCHMARK_LOCF_TARGETS:
        if target_type == "metrics":
            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE sim_obs AS
                SELECT product_id, effective_date
                FROM v_observations
                WHERE metric = $1
                GROUP BY product_id, effective_date
            """,
                [target_name],
            )
            p95_cap = (
                p95_map.get(("ratios", target_name))
                or p95_map.get(("profile", target_name))
                or p95_map.get(("mstar", target_name))
                or p95_map.get(("holdings", target_name))
                or p95_map.get(("esg", target_name))
                or (current_cap * 2)
            )
        else:
            dim_type = target_type.split(":", 1)[1]
            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE sim_obs AS
                SELECT product_id, effective_date
                FROM v_observations
                WHERE family = $1 AND metric = $2
                GROUP BY product_id, effective_date
            """,
                [dim_type, target_name],
            )
            p95_cap = p95_map.get((dim_type, target_name)) or (current_cap * 2)

        p95_cap = max(current_cap, p95_cap)

        sim_row = con.execute(
            """
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
        """,
            [current_cap, p95_cap],
        ).fetchone()

        if sim_row and sim_row[0] > 0:
            elig = sim_row[0]
            c_months = sim_row[1] or 0
            p_months = sim_row[2] or 0
            b_months = sim_row[3] or 0
            c_pct = round(c_months * 100.0 / elig, 1)
            p_pct = round(p_months * 100.0 / elig, 1)
            b_pct = round(b_months * 100.0 / elig, 1)
            b_gain = b_months - p_months

            sim_results.append(
                (
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
                )
            )

    con.execute("DROP TABLE IF EXISTS active_spine")
    con.execute("DROP TABLE IF EXISTS sim_obs")

    logger.render_table(
        "LOCF Coverage Simulation Across Benchmark Features",
        [
            "Feature",
            "Type",
            "Current Cap",
            "p95 Cap",
            "Spine Mos",
            "Current Mos",
            "Current %",
            "p95 Mos",
            "p95 %",
            "p95+Bfill Mos",
            "Total %",
            "Bfill Gain",
        ],
        [
            [
                r[0],
                r[1],
                f"{r[2]}d",
                f"{r[3]}d",
                f"{r[4]:,}",
                f"{r[5]:,}",
                f"{r[6]}%",
                f"{r[7]:,}",
                f"{r[8]}%",
                f"{r[9]:,}",
                f"{r[10]}%",
                f"+{r[11]:,}",
            ]
            for r in sim_results
        ],
        align_right=list(range(2, 12)),
    )

    return sim_results


# ==============================================================================
# 7. EMPIRICAL PROFILES (Sleeves, Fees, Debt Clusters)
# ==============================================================================


def run_empirical_profiles(
    con: duckdb.DuckDBPyConnection,
    logger: CatalogLogger,
) -> tuple[list[tuple], list[tuple], list[tuple]]:
    logger.section(
        "7. EMPIRICAL BEHAVIORAL PROFILES",
        "Descriptive distributions: sleeve sums, fee allocations, debt clusters",
    )

    sleeve_rows = con.execute("""
        WITH snap_sums AS (
            SELECT family AS dimension_type, product_id, effective_date,
                   SUM(value) AS sum_wgt,
                   SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END) AS neg_wgt_cnt
            FROM v_observations
            WHERE family IN ('asset_class', 'country', 'industry', 'credit_rating', 'maturity', 'debt_type')
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

    logger.render_table(
        "Allocation Weights Profile by Sleeves (Simplex Partition-of-Unity Audit)",
        [
            "Sleeve Family",
            "Snapshots",
            "Exact (±0.1%)",
            "Exact %",
            "Lev (>1.05)",
            "Under (<0.95)",
            "Min Sum",
            "Median Sum",
            "Max Sum",
            "With Shorts",
        ],
        [
            [
                r[0],
                f"{r[1]:,}",
                f"{r[2]:,}",
                f"{r[3]}%",
                f"{r[4]:,}",
                f"{r[5]:,}",
                fmt_f(r[6]),
                fmt_f(r[7]),
                fmt_f(r[8]),
                f"{r[9]:,}",
            ]
            for r in sleeve_rows
        ],
        align_right=list(range(1, 10)),
    )

    fee_alloc = con.execute("""
        WITH pairs AS (
            SELECT product_id, effective_date,
                   SUM(CASE WHEN metric = 'management_expense_ratio' THEN value ELSE 0 END) AS mgt,
                   SUM(CASE WHEN metric = 'non_management_expense_ratio' THEN value ELSE 0 END) AS non_mgt
            FROM v_observations
            WHERE family = 'profile' AND metric IN ('management_expense_ratio', 'non_management_expense_ratio')
            GROUP BY 1, 2 HAVING COUNT(DISTINCT metric) = 2
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
        logger.render_table(
            "Expense Ratio Allocation (Management + Non-Management)",
            [
                "Paired Snaps",
                "Exact = 1.0",
                "% Exact",
                "Fee Subsidies (<0)",
                "Zero Non-Mgt",
                "Min Mgt",
                "Max Mgt",
                "Min Non-Mgt",
                "Max Non-Mgt",
            ],
            [
                [
                    f"{fee_alloc[0]:,}",
                    f"{fee_alloc[1]:,}",
                    f"{fee_alloc[2]}%",
                    f"{fee_alloc[3]:,}",
                    f"{fee_alloc[4]:,}",
                    fmt_f(fee_alloc[5]),
                    fmt_f(fee_alloc[6]),
                    fmt_f(fee_alloc[7]),
                    fmt_f(fee_alloc[8]),
                ]
            ],
            align_right=list(range(9)),
        )

    cases = []
    for cluster, names in DEBT_CLUSTERS.items():
        for name in names:
            escaped = name.replace("'", "''")
            cases.append(f"WHEN metric = '{escaped}' THEN '{cluster}'")
    case_sql = " ".join(cases)

    debt_cluster_rows = con.execute(f"""
        WITH mapped AS (
            SELECT metric, product_id, value,
                   CASE {case_sql} ELSE 'debt_unmapped_new' END AS cluster_id
            FROM v_observations
            WHERE family = 'debt_type'
        )
        SELECT
            cluster_id,
            COUNT(DISTINCT metric) AS n_types,
            COUNT(*) AS n_obs,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(SUM(value), 2) AS sum_weight,
            ROUND(AVG(value), 4) AS avg_weight
        FROM mapped
        GROUP BY cluster_id
        ORDER BY n_obs DESC
    """).fetchall()

    logger.render_table(
        "Debt Type 9-Cluster Coverage Profile",
        ["Cluster", "Distinct Types", "Observations", "Funds", "Total Weight", "Mean Weight"],
        [[r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]:,}", fmt_f(r[4], 2), fmt_f(r[5], 4)] for r in debt_cluster_rows],
        align_right=list(range(1, 6)),
    )

    return sleeve_rows, debt_cluster_rows, fee_alloc_rows


# ==============================================================================
# 8. MIGRATION CHECKLIST
# ==============================================================================


def run_pending(
    con: duckdb.DuckDBPyConnection,
    findings: list[Finding],
    schema_info: dict[str, Any],
    logger: CatalogLogger,
) -> None:
    logger.section("8. MIGRATION CHECKLIST", "Implementation-readiness verification (schema migrations, cleanups)")

    is_unified = schema_info["is_unified"]
    pending(
        findings,
        "Schema",
        "unified silver.observations table created",
        is_unified,
        "single table cutover landed" if is_unified else "pending migration from legacy split tables",
        logger,
    )

    top_holding_rows = con.execute("SELECT COUNT(*) FROM v_observations WHERE family = 'top_holding'").fetchone()[0]
    pending(
        findings,
        "Schema",
        "top_holding extraction pruned",
        top_holding_rows == 0,
        f"{top_holding_rows:,} rows remain" if top_holding_rows else "0 rows (pruned)",
        logger,
    )

    med_growth = con.execute(
        "SELECT MEDIAN(value) FROM v_observations WHERE family = 'ratios' AND metric = 'eps_growth_1yr'"
    ).fetchone()[0]
    is_decimal = med_growth is not None and abs(float(med_growth)) < 2.0
    pending(
        findings,
        "Ratios",
        "growth/return/yield metrics converted to decimals (/100)",
        is_decimal,
        f"median eps_growth={fmt_f(med_growth)}",
        logger,
    )

    medalist_n = con.execute("SELECT COUNT(*) FROM v_observations WHERE metric = 'mstar_medalist_rating'").fetchone()[0]
    pending(
        findings,
        "Morningstar",
        "unified mstar_medalist_rating extracted",
        medalist_n > 0,
        f"{medalist_n:,} rows",
        logger,
    )

    coverage_n = con.execute(
        "SELECT COUNT(*) FROM v_observations WHERE metric = 'mstar_analyst_coverage_pct'"
    ).fetchone()[0]
    pending(
        findings, "Morningstar", "mstar_analyst_coverage_pct extracted", coverage_n > 0, f"{coverage_n:,} rows", logger
    )

    lip_variants = con.execute("SELECT COUNT(DISTINCT metric) FROM v_observations WHERE family = 'lipper'").fetchone()[
        0
    ]
    pending(
        findings,
        "Lipper",
        "Max-Peer-Count reduction applied (<=25 canonical ids)",
        lip_variants <= 25,
        f"{lip_variants} distinct metric_ids",
        logger,
    )

    theme_cov_n = con.execute("SELECT COUNT(*) FROM v_observations WHERE metric = 'theme_coverage'").fetchone()[0]
    pending(findings, "Themes", "theme_coverage extracted in silver", theme_cov_n > 0, f"{theme_cov_n:,} rows", logger)

    theme_dim_n = con.execute("SELECT COUNT(*) FROM v_observations WHERE family = 'theme'").fetchone()[0]
    pending(findings, "Themes", "theme weights extracted", theme_dim_n > 0, f"{theme_dim_n:,} rows", logger)

    sb_dim_n = con.execute(
        "SELECT COUNT(*) FROM v_observations WHERE family IN ('style_box', 'style_box_hist')"
    ).fetchone()[0]
    pending(findings, "Style Box", "style_box coordinates extracted", sb_dim_n > 0, f"{sb_dim_n:,} rows", logger)

    is_passive_n = con.execute(
        "SELECT COUNT(*) FROM v_observations WHERE family = 'profile' AND metric = 'is_passive'"
    ).fetchone()[0]
    pending(
        findings, "Profile", "is_passive approach metric extracted", is_passive_n > 0, f"{is_passive_n:,} rows", logger
    )

    audited_fee_n = con.execute(
        "SELECT COUNT(*) FROM v_observations WHERE family = 'profile' AND metric = 'audited_net_expense_ratio'"
    ).fetchone()[0]
    pending(
        findings,
        "Profile",
        "audited_net_expense_ratio metric extracted",
        audited_fee_n > 0,
        f"{audited_fee_n:,} rows",
        logger,
    )

    tenure_n = con.execute(
        "SELECT COUNT(*) FROM v_observations WHERE family = 'profile' AND metric = 'manager_tenure_years'"
    ).fetchone()[0]
    pending(findings, "Profile", "manager_tenure_years metric extracted", tenure_n > 0, f"{tenure_n:,} rows", logger)

    top10_n = con.execute(
        "SELECT COUNT(*) FROM v_observations WHERE family = 'holdings' AND metric = 'portfolio_top_10_concentration'"
    ).fetchone()[0]
    pending(
        findings,
        "Holdings",
        "portfolio_top_10_concentration metric extracted",
        top10_n > 0,
        f"{top10_n:,} rows",
        logger,
    )

    unmapped_countries = con.execute("""
        SELECT COUNT(*) FROM v_observations
        WHERE family = 'country' AND (
            (metric = 'Croatia' AND COALESCE(code, '') != 'HR') OR
            (metric = 'Bulgaria' AND COALESCE(code, '') != 'BG') OR
            (metric = 'Guam' AND COALESCE(code, '') != 'GU') OR
            (metric = 'Uzbekistan' AND COALESCE(code, '') != 'UZ') OR
            (metric = 'Unidentified' AND code IS NOT NULL)
        )
    """).fetchone()[0]
    pending(
        findings,
        "Country",
        "ISO remaps applied (HR/BG/GU/UZ; Unidentified->NULL)",
        unmapped_countries == 0,
        f"{unmapped_countries} unmapped rows" if unmapped_countries else "clean",
        logger,
    )

    legacy_telecom = con.execute("""
        SELECT COUNT(*) FROM v_observations
        WHERE family = 'industry' AND metric = 'Telecommunication Services-Discontinued eff 09/19/2020'
    """).fetchone()[0]
    pending(
        findings,
        "Industry",
        "discontinued telecom remapped to Communication Services",
        legacy_telecom == 0,
        f"{legacy_telecom} legacy rows remain" if legacy_telecom else "clean",
        logger,
    )

    mat_null = con.execute("SELECT COUNT(*) FROM v_observations WHERE family = 'maturity' AND code IS NULL").fetchone()[
        0
    ]
    pending(
        findings,
        "Maturity",
        "code populated with mat_* slugs",
        mat_null == 0,
        f"{mat_null:,} rows still NULL" if mat_null else "clean",
        logger,
    )

    panel_exists = has_table(con, "silver", "monthly_panel")
    panel_detail = ""
    if panel_exists:
        n_rows = con.execute("SELECT COUNT(*) FROM silver.monthly_panel").fetchone()[0]
        panel_detail = f"{n_rows:,} rows"
    pending(findings, "Panel", "silver.monthly_panel built", panel_exists, panel_detail or "not found", logger)


# ==============================================================================
# 9. STATISTICAL CATALOG
# ==============================================================================


def run_catalog(
    con: duckdb.DuckDBPyConnection,
    logger: CatalogLogger,
) -> tuple[dict[str, Any], list[tuple], list[tuple], list[tuple], list[tuple], list[tuple]]:
    logger.section("9. STATISTICAL CATALOG", "Distributional profiling across all metrics and dimensions")

    contracts_n = (
        con.execute("SELECT COUNT(DISTINCT product_id) FROM bronze.contracts").fetchone()[0]
        if has_table(con, "bronze", "contracts")
        else 0
    )
    priced_n = (
        con.execute("SELECT COUNT(DISTINCT product_id) FROM silver.products").fetchone()[0]
        if has_table(con, "silver", "products")
        else 0
    )
    obs_rows, obs_funds = con.execute("SELECT COUNT(*), COUNT(DISTINCT product_id) FROM v_observations").fetchone()

    univ_base = priced_n or contracts_n or max(obs_funds, 1)

    footprint_meta = {
        "contracts_n": contracts_n,
        "priced_n": priced_n,
        "total_obs_rows": obs_rows,
        "total_obs_funds": obs_funds,
    }

    logger.render_table(
        "Universe Footprint",
        ["Bronze Contracts", "Priced Products", "Total Silver Observations", "Observational Funds", "Base Universe"],
        [[f"{contracts_n:,}", f"{priced_n:,}", f"{obs_rows:,}", f"{obs_funds:,}", f"{univ_base:,}"]],
        align_right=[0, 1, 2, 3, 4],
    )

    dim_summary_rows = con.execute(f"""
        SELECT
            family,
            COUNT(*) AS n_rows,
            COUNT(DISTINCT metric) AS n_metrics,
            COUNT(DISTINCT code) AS n_codes,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(COUNT(DISTINCT product_id) * 100.0 / {univ_base}, 1) AS pct_universe,
            MIN(effective_date) AS min_dt,
            MAX(effective_date) AS max_dt,
            ROUND(MIN(value), 4) AS min_v,
            ROUND(MEDIAN(value), 4) AS med_v,
            ROUND(MAX(value), 4) AS max_v,
            COUNT(*) FILTER (WHERE value < 0) AS n_neg,
            COUNT(*) FILTER (WHERE value > 1.0) AS n_gt1
        FROM v_observations
        GROUP BY family
        ORDER BY n_rows DESC
    """).fetchall()

    logger.render_table(
        "Observations Rollup by Analytical Family",
        [
            "Family",
            "Rows",
            "Metrics",
            "Codes",
            "Funds",
            "Univ %",
            "First Date",
            "Last Date",
            "Min",
            "Median",
            "Max",
            "Neg",
            ">1.0",
        ],
        [
            [
                r[0],
                f"{r[1]:,}",
                f"{r[2]:,}",
                f"{r[3]:,}",
                f"{r[4]:,}",
                f"{r[5]}%",
                str(r[6]),
                str(r[7]),
                fmt_f(r[8]),
                fmt_f(r[9]),
                fmt_f(r[10]),
                f"{r[11]:,}",
                f"{r[12]:,}",
            ]
            for r in dim_summary_rows
        ],
        align_right=[1, 2, 3, 4, 5, 8, 9, 10, 11, 12],
    )

    metrics_rows = con.execute(f"""
        SELECT
            family,
            metric,
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
            COUNT(DISTINCT code) AS n_codes,
            ROUND(COUNT(*) FILTER (WHERE effective_date_source='item') * 100.0 / COUNT(*), 0) AS pct_item,
            ROUND(COUNT(*) FILTER (WHERE effective_date_source='payload') * 100.0 / COUNT(*), 0) AS pct_payload,
            ROUND(COUNT(*) FILTER (WHERE effective_date_source='snapshot') * 100.0 / COUNT(*), 0) AS pct_snap
        FROM v_observations
        GROUP BY family, metric
        ORDER BY family, metric
    """).fetchall()

    logger.render_table(
        f"Detailed Observations Inventory ({len(metrics_rows)} distinct family:metric pairs)",
        [
            "Family",
            "Metric",
            "Obs",
            "Funds",
            "Univ %",
            "First Date",
            "Last Date",
            "Min",
            "Median",
            "Max",
            "Mean",
            "StdDev",
            "Negs",
            "Codes",
        ],
        [
            [
                r[0],
                r[1],
                f"{r[2]:,}",
                f"{r[3]:,}",
                f"{r[4]}%",
                str(r[5]),
                str(r[6]),
                fmt_f(r[7]),
                fmt_f(r[9]),
                fmt_f(r[11]),
                fmt_f(r[12]),
                fmt_f(r[13]),
                f"{r[15]:,}",
                str(r[16]),
            ]
            for r in metrics_rows
        ],
        align_right=[2, 3, 4, 7, 8, 9, 10, 11, 12, 13],
        max_console_rows=30,
    )

    dims_rows = con.execute(f"""
        SELECT
            family AS dimension_type,
            metric AS dimension_name,
            COALESCE(ANY_VALUE(code), '[NONE]') AS sample_code,
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
        FROM v_observations
        WHERE family IN ('asset_class', 'country', 'industry', 'credit_rating', 'debt_type', 'maturity', 'theme', 'style_box', 'style_box_hist')
        GROUP BY family, metric
        ORDER BY family, n_obs DESC
    """).fetchall()

    style_box_raw = con.execute("""
        SELECT code,
               COUNT(*) FILTER (WHERE family='style_box') AS selected,
               COUNT(*) FILTER (WHERE family='style_box_hist') AS hist,
               COUNT(DISTINCT product_id) AS n_funds
        FROM v_observations WHERE family IN ('style_box','style_box_hist')
        GROUP BY 1
    """).fetchall()
    occ = {r[0]: r for r in style_box_raw if r[0]}

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

    logger.render_table(
        "Style Box Grid Occupancy (12 standard cells)",
        ["Cell", "Selected", "Hist", "Funds", "Observed?"],
        [[r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]:,}", "YES" if r[4] else "NO"] for r in style_box_rows],
        align_right=[1, 2, 3],
    )

    aum_tokens = con.execute(r"""
        WITH parsed AS (
            SELECT REGEXP_EXTRACT(raw_value, '^([^0-9\s]+)', 1) AS token, value
            FROM v_observations WHERE metric = 'total_net_assets_local'
        )
        SELECT COALESCE(token, '[NONE]'), COUNT(*), ROUND(MIN(value), 0), ROUND(MAX(value), 0)
        FROM parsed GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()

    return footprint_meta, metrics_rows, dims_rows, dim_summary_rows, style_box_rows, aum_tokens


# ==============================================================================
# 10. COMPREHENSIVE FRD ENUM & CONTRACT ACCOUNTING
# ==============================================================================


def run_frd_contract_reconciliation(
    con: duckdb.DuckDBPyConnection,
    logger: CatalogLogger,
    findings: list[Finding],
) -> tuple[list[tuple], list[tuple], list[tuple]]:
    """
    Exhaustively reconciles extracted metrics against FRD §5.1 StrEnums and §5.2 open domains.
    Flags any extracted metric missing from proposed enums (which would raise ValueError).
    """
    logger.section(
        "10. FRD DATA CONTRACTS & VOCABULARY AUDIT",
        "Reconciliation of extracted metrics vs FRD §5.1 StrEnums, §5.2 Open Vocabularies, and §5.3 Simplex rules",
    )

    recon_records: list[tuple] = []
    debt_type_inventory: list[tuple] = []
    industry_inventory: list[tuple] = []

    # 1. ESG METRICS
    esg_db = con.execute("""
        SELECT
            metric,
            COUNT(*) AS n_obs,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(MIN(value), 2) AS min_val,
            ROUND(MEDIAN(value), 2) AS med_val,
            ROUND(MAX(value), 2) AS max_val
        FROM v_observations WHERE family = 'esg'
        GROUP BY metric ORDER BY metric
    """).fetchall()
    esg_slugs = {r[0] for r in esg_db}
    extra_esg = esg_slugs - FRD_ESG_METRICS
    unobserved_esg = FRD_ESG_METRICS - esg_slugs

    for r in esg_db:
        status = "EXTRA_IN_DB_ADD_TO_ENUM" if r[0] in extra_esg else "MATCH_FRD"
        recon_records.append(("esg", r[0], "StrEnum", status, r[1], r[2], r[3], r[4], r[5], ""))
    for u in sorted(unobserved_esg):
        recon_records.append(("esg", u, "StrEnum", "FRD_MEMBER_UNOBSERVED", 0, 0, None, None, None, ""))

    logger.subsection("10.1 ESG Metrics (FRD §5.1 EsgMetric)")
    logger.log_text(f"- Observed in DB: {len(esg_slugs)} | FRD Documented: {len(FRD_ESG_METRICS)}")
    if extra_esg:
        logger.log_text(
            f"  **ACTION REQUIRED**: Found {len(extra_esg)} extra ESG slugs in DB: {sorted(extra_esg)}. Add to EsgMetric!"
        )
    watchlist(
        findings,
        "Data Contract",
        "ESG metrics vs FRD EsgMetric",
        extra_esg,
        "Extra slugs to add to EsgMetric before freeze",
        logger,
    )
    logger.render_table(
        "ESG Metrics Accounting",
        ["Metric Slug", "Status", "Obs", "Funds", "Min", "Median", "Max"],
        [
            [
                r[0],
                "EXTRA_IN_DB_ADD_TO_ENUM" if r[0] in extra_esg else "MATCH",
                f"{r[1]:,}",
                f"{r[2]:,}",
                fmt_f(r[3]),
                fmt_f(r[4]),
                fmt_f(r[5]),
            ]
            for r in esg_db
        ],
        align_right=[2, 3, 4, 5, 6],
    )

    # 2. RATIOS METRICS
    ratios_db = con.execute("""
        SELECT
            metric,
            COUNT(*) AS n_obs,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(MIN(value), 4) AS min_val,
            ROUND(MEDIAN(value), 4) AS med_val,
            ROUND(MAX(value), 4) AS max_val
        FROM v_observations WHERE family = 'ratios'
        GROUP BY metric ORDER BY metric
    """).fetchall()
    ratios_slugs = {r[0] for r in ratios_db}
    all_known_ratios = FRD_RATIOS_PERCENTAGE_METRICS | FRD_RATIOS_KNOWN_NON_PERCENTAGE
    extra_ratios = ratios_slugs - all_known_ratios

    for r in ratios_db:
        name = r[0]
        if name in FRD_RATIOS_PERCENTAGE_METRICS:
            status = "MATCH_PERCENTAGE_SUBSET"
        elif name in FRD_RATIOS_KNOWN_NON_PERCENTAGE:
            status = "MATCH_STANDARD_RATIO"
        else:
            status = "EXTRA_IN_DB_ADD_TO_ENUM"
        recon_records.append(("ratios", name, "StrEnum", status, r[1], r[2], r[3], r[4], r[5], ""))

    logger.subsection("10.2 Ratios Fundamentals (FRD §5.1 RatiosMetric)")
    logger.log_text(
        f"- Observed in DB: {len(ratios_slugs)} | FRD Percentage Subset: {len(FRD_RATIOS_PERCENTAGE_METRICS)} | Known Non-Percentage: {len(FRD_RATIOS_KNOWN_NON_PERCENTAGE)}"
    )
    if extra_ratios:
        logger.log_text(
            f"  **ACTION REQUIRED**: Found {len(extra_ratios)} extra ratios in DB: {sorted(extra_ratios)}. Add to RatiosMetric!"
        )
    watchlist(
        findings,
        "Data Contract",
        "Ratios metrics vs FRD RatiosMetric catalog",
        extra_ratios,
        "Extra ratio tags to add to RatiosMetric before freeze",
        logger,
    )
    logger.render_table(
        f"Ratios Metrics Accounting ({len(ratios_db)} total ratios)",
        ["Metric Tag", "FRD Classification", "Obs", "Funds", "Min", "Median", "Max"],
        [
            [
                r[0],
                "PERCENTAGE (/100)"
                if r[0] in FRD_RATIOS_PERCENTAGE_METRICS
                else ("STANDARD" if r[0] in FRD_RATIOS_KNOWN_NON_PERCENTAGE else "EXTRA_NEW"),
                f"{r[1]:,}",
                f"{r[2]:,}",
                fmt_f(r[3]),
                fmt_f(r[4]),
                fmt_f(r[5]),
            ]
            for r in ratios_db
        ],
        align_right=[2, 3, 4, 5, 6],
        max_console_rows=25,
    )

    # 3. INDUSTRY METRICS
    ind_db = con.execute("""
        SELECT
            metric,
            COUNT(*) AS n_obs,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(SUM(value), 2) AS sum_weight,
            ROUND(AVG(value), 4) AS avg_weight
        FROM v_observations WHERE family = 'industry'
        GROUP BY metric ORDER BY n_obs DESC
    """).fetchall()

    unmapped_industries = []
    for r in ind_db:
        raw_name = r[0]
        remapped_name = INDUSTRY_NAME_REMAPS.get(raw_name, raw_name)
        is_std = remapped_name in FRD_STANDARD_INDUSTRIES
        if not is_std:
            unmapped_industries.append(raw_name)
        status = (
            "MATCH_STANDARD"
            if raw_name in FRD_STANDARD_INDUSTRIES
            else ("REMAPPED" if raw_name in INDUSTRY_NAME_REMAPS else "EXTRA_NEW_ADD_TO_ENUM")
        )
        industry_inventory.append((raw_name, remapped_name, 1 if is_std else 0, r[1], r[2], r[3], r[4]))
        recon_records.append(
            ("industry", raw_name, "StrEnum", status, r[1], r[2], None, None, None, f"Remaps to: {remapped_name}")
        )

    logger.subsection("10.3 Holdings Sleeve: Industry Sectors (FRD §5.1 IndustryMetric)")
    logger.log_text(f"- Observed in DB: {len(ind_db)} distinct industry names")
    watchlist(
        findings,
        "Data Contract",
        "Industry names vs standard GICS",
        unmapped_industries,
        "Industries requiring remap or enum addition",
        logger,
    )
    logger.render_table(
        "Industry Sectors Accounting",
        ["Raw Extracted Industry", "Remapped Target", "Standard?", "Obs", "Funds", "Total Weight", "Mean Weight"],
        [
            [r[0], r[1], "YES" if r[2] else "NO", f"{r[3]:,}", f"{r[4]:,}", fmt_f(r[5], 2), fmt_f(r[6], 4)]
            for r in industry_inventory
        ],
        align_right=[3, 4, 5, 6],
    )

    # 4. DEBT TYPES & 9-CLUSTER COVERAGE
    debt_db = con.execute("""
        SELECT
            metric,
            COUNT(*) AS n_obs,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(SUM(value), 4) AS sum_weight,
            ROUND(AVG(value), 4) AS avg_weight
        FROM v_observations WHERE family = 'debt_type'
        GROUP BY metric ORDER BY n_obs DESC
    """).fetchall()

    reverse_debt_map = {name: cluster for cluster, names in DEBT_CLUSTERS.items() for name in names}
    unmapped_debt = []
    for r in debt_db:
        v_name = r[0]
        cluster = reverse_debt_map.get(v_name)
        is_mapped = cluster is not None
        if not is_mapped:
            unmapped_debt.append(v_name)
        cluster_id = cluster or "UNMAPPED_NEW_WILL_RAISE"
        debt_type_inventory.append((v_name, cluster_id, 1 if is_mapped else 0, r[1], r[2], r[3], r[4]))
        recon_records.append(
            (
                "debt_type",
                v_name,
                "Open/Cluster",
                "MAPPED" if is_mapped else "UNMAPPED_NEW",
                r[1],
                r[2],
                None,
                None,
                None,
                f"Cluster: {cluster_id}",
            )
        )

    logger.subsection("10.4 Holdings Sleeve: Debt Types & 9-Cluster Alignment (DEBT_CLUSTERS)")
    logger.log_text(f"- Observed in DB: {len(debt_db)} vendor debt types across all fixed income portfolios")
    if unmapped_debt:
        logger.log_text(
            f"  **ACTION REQUIRED**: Found {len(unmapped_debt)} unmapped debt types in DB: {unmapped_debt}. Add to DEBT_CLUSTERS before panel runs!"
        )
    watchlist(
        findings,
        "Data Contract",
        "Debt types vs DEBT_CLUSTERS",
        unmapped_debt,
        "Unmapped debt types that will raise in panel flatten",
        logger,
    )
    logger.render_table(
        f"Debt Type to 9-Cluster Mapping Accounting ({len(debt_type_inventory)} vendor types)",
        ["Vendor Debt Name", "Assigned 9-Cluster", "Mapped?", "Obs", "Funds", "Total Weight", "Mean Weight"],
        [
            [r[0], r[1], "YES" if r[2] else "NO", f"{r[3]:,}", f"{r[4]:,}", fmt_f(r[5], 2), fmt_f(r[6], 4)]
            for r in debt_type_inventory
        ],
        align_right=[3, 4, 5, 6],
        max_console_rows=25,
    )

    # 5. PROFILE & HOLDINGS SCALAR METRICS
    profile_db = con.execute("""
        SELECT
            metric,
            COUNT(*) AS n_obs,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(MIN(value), 4) AS min_val,
            ROUND(MEDIAN(value), 4) AS med_val,
            ROUND(MAX(value), 4) AS max_val
        FROM v_observations WHERE family = 'profile'
        GROUP BY metric ORDER BY metric
    """).fetchall()
    prof_slugs = {r[0] for r in profile_db}
    extra_prof = prof_slugs - FRD_PROFILE_METRICS
    for r in profile_db:
        status = "EXTRA_IN_DB_ADD_TO_ENUM" if r[0] in extra_prof else "MATCH_FRD"
        recon_records.append(("profile", r[0], "StrEnum", status, r[1], r[2], r[3], r[4], r[5], ""))

    logger.subsection("10.5 Profile & Holdings Metrics (FRD §5.1 ProfileMetric, HoldingsMetric)")
    watchlist(
        findings,
        "Data Contract",
        "Profile metrics vs FRD ProfileMetric",
        extra_prof,
        "Extra profile metrics to review",
        logger,
    )
    logger.render_table(
        "Profile Metrics Accounting",
        ["Metric Slug", "Status", "Obs", "Funds", "Min", "Median", "Max"],
        [
            [
                r[0],
                "EXTRA_IN_DB_ADD_TO_ENUM" if r[0] in extra_prof else "MATCH",
                f"{r[1]:,}",
                f"{r[2]:,}",
                fmt_f(r[3]),
                fmt_f(r[4]),
                fmt_f(r[5]),
            ]
            for r in profile_db
        ],
        align_right=[2, 3, 4, 5, 6],
    )

    # 6. CREDIT RATINGS
    cr_db = con.execute("""
        SELECT
            metric,
            COUNT(*) AS n_obs,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(SUM(value), 2) AS sum_weight,
            ROUND(AVG(value), 4) AS avg_weight
        FROM v_observations WHERE family = 'credit_rating'
        GROUP BY metric ORDER BY n_obs DESC
    """).fetchall()
    cr_slugs = {r[0] for r in cr_db}
    extra_cr = cr_slugs - CREDIT_RATING_GRADES
    for r in cr_db:
        status = "EXTRA_IN_DB_ADD_TO_ENUM" if r[0] in extra_cr else "MATCH_FRD"
        recon_records.append(("credit_rating", r[0], "StrEnum", status, r[1], r[2], None, None, None, ""))

    logger.subsection("10.6 Credit Ratings (FRD §5.1 CreditRatingMetric)")
    watchlist(
        findings,
        "Data Contract",
        "Credit ratings vs FRD CreditRatingMetric",
        extra_cr,
        "Extra grades to add to CreditRatingMetric before freeze",
        logger,
    )
    logger.render_table(
        "Credit Rating Grades Accounting",
        ["Grade Name", "Status", "Obs", "Funds", "Total Weight", "Mean Weight"],
        [
            [
                r[0],
                "EXTRA_NEW" if r[0] in extra_cr else "MATCH",
                f"{r[1]:,}",
                f"{r[2]:,}",
                fmt_f(r[3], 2),
                fmt_f(r[4], 4),
            ]
            for r in cr_db
        ],
        align_right=[2, 3, 4, 5],
    )

    # 7. MATURITY BUCKETS
    mat_db = con.execute("""
        SELECT
            metric,
            code,
            COUNT(*) AS n_obs,
            COUNT(DISTINCT product_id) AS n_funds,
            ROUND(SUM(value), 2) AS sum_weight,
            ROUND(AVG(value), 4) AS avg_weight
        FROM v_observations WHERE family = 'maturity'
        GROUP BY metric, code ORDER BY n_obs DESC
    """).fetchall()
    unmapped_mat = [r[0] for r in mat_db if r[1] not in MATURITY_SLUGS or r[1] is None]
    for r in mat_db:
        c = r[1]
        status = "MATCH_FRD" if c in MATURITY_SLUGS else ("CODE_IS_NULL" if c is None else "UNKNOWN_SLUG")
        recon_records.append(("maturity", r[0], "StrEnumCodes", status, r[2], r[3], None, None, None, f"Code: {c}"))

    logger.subsection("10.7 Maturity Buckets (FRD §5.1 MaturityMetric)")
    watchlist(
        findings,
        "Data Contract",
        "Maturity display names vs MATURITY_SLUGS map",
        unmapped_mat,
        "Display names missing mat_* code slug",
        logger,
    )
    logger.render_table(
        "Maturity Buckets Accounting",
        [
            "Display Metric Name",
            "Dimension Code (FRD Slug)",
            "Valid Slug?",
            "Obs",
            "Funds",
            "Total Weight",
            "Mean Weight",
        ],
        [
            [
                r[0],
                r[1] or "[NULL]",
                "YES" if r[1] in MATURITY_SLUGS else "NO",
                f"{r[2]:,}",
                f"{r[3]:,}",
                fmt_f(r[4], 2),
                fmt_f(r[5], 4),
            ]
            for r in mat_db
        ],
        align_right=[3, 4, 5, 6],
    )

    return recon_records, debt_type_inventory, industry_inventory


# ==============================================================================
# 11. SQLITE EXPORT
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
    recon_records: list[tuple],
    debt_type_inventory: list[tuple],
    industry_inventory: list[tuple],
    aum_sim_rows: list[tuple],
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
            total_obs_rows INTEGER,
            total_obs_funds INTEGER
        )
    """)
    cur.execute(
        "INSERT INTO universe VALUES (?,?,?,?)",
        (
            footprint.get("contracts_n", 0),
            footprint.get("priced_n", 0),
            footprint.get("total_obs_rows", 0),
            footprint.get("total_obs_funds", 0),
        ),
    )

    cur.execute("""
        CREATE TABLE observations_profile (
            family TEXT,
            metric TEXT,
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
            n_codes INTEGER,
            pct_item REAL,
            pct_payload REAL,
            pct_snapshot REAL
        )
    """)
    cur.executemany(
        "INSERT INTO observations_profile VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                r[0],
                r[1],
                r[2],
                r[3],
                r[4],
                str(r[5]),
                str(r[6]),
                r[7],
                r[8],
                r[9],
                r[10],
                r[11],
                r[12],
                r[13],
                r[14],
                r[15],
                r[16],
                r[17],
                r[18],
                r[19],
            )
            for r in metrics_rows
        ],
    )

    # Legacy metrics table compatibility
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
                r[0],
                r[1],
                r[2],
                r[3],
                r[4],
                str(r[5]),
                str(r[6]),
                r[7],
                r[8],
                r[9],
                r[10],
                r[11],
                r[12],
                r[13],
                r[14],
                r[15],
                r[16],
                r[17],
                r[18],
                r[19],
            )
            for r in metrics_rows
        ],
    )

    # Legacy dimensions table compatibility
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
                r[0],
                r[1],
                r[2],
                r[3],
                r[4],
                r[5],
                str(r[6]),
                str(r[7]),
                r[8],
                r[9],
                r[10],
                r[11],
                r[12],
                r[13],
                r[14],
                r[15],
                r[16],
                r[17],
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
                r[0],
                r[1],
                r[2],
                r[3],
                r[4],
                r[5],
                str(r[6]),
                str(r[7]),
                r[8],
                r[9],
                r[10],
                r[11],
                r[12],
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
        CREATE TABLE frd_reconciliation (
            family TEXT,
            metric_name TEXT,
            domain_type TEXT,
            status TEXT,
            n_obs INTEGER,
            n_funds INTEGER,
            min_val REAL,
            med_val REAL,
            max_val REAL,
            notes TEXT
        )
    """)
    cur.executemany(
        "INSERT INTO frd_reconciliation VALUES (?,?,?,?,?,?,?,?,?,?)",
        recon_records,
    )

    cur.execute("""
        CREATE TABLE debt_types_inventory (
            vendor_name TEXT,
            cluster_id TEXT,
            is_mapped INTEGER,
            n_obs INTEGER,
            n_funds INTEGER,
            total_weight REAL,
            avg_weight REAL
        )
    """)
    cur.executemany("INSERT INTO debt_types_inventory VALUES (?,?,?,?,?,?,?)", debt_type_inventory)

    cur.execute("""
        CREATE TABLE industry_inventory (
            raw_name TEXT,
            remapped_name TEXT,
            is_standard INTEGER,
            n_obs INTEGER,
            n_funds INTEGER,
            total_weight REAL,
            avg_weight REAL
        )
    """)
    cur.executemany("INSERT INTO industry_inventory VALUES (?,?,?,?,?,?,?)", industry_inventory)

    cur.execute("""
        CREATE TABLE aum_disambiguation_simulation (
            outcome TEXT,
            n_obs INTEGER,
            n_funds INTEGER,
            action TEXT,
            sample_case TEXT
        )
    """)
    cur.executemany("INSERT INTO aum_disambiguation_simulation VALUES (?,?,?,?,?)", aum_sim_rows)

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
    cur.executemany(
        "INSERT INTO payload_endpoint_summary VALUES (?,?,?,?,?,?,?)",
        [(r[0], r[1], r[2], r[3], r[4], str(r[5]), str(r[6])) for r in ep_summary],
    )

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
                r[0],
                r[1],
                r[2],
                r[3],
                _int_or_none(r[4]),
                _int_or_none(r[5]),
                _int_or_none(r[6]),
                _int_or_none(r[7]),
                _int_or_none(r[8]),
                _int_or_none(r[9]),
                _int_or_none(r[10]),
                _int_or_none(r[11]),
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
                r[0],
                r[1],
                r[2],
                r[3],
                _int_or_none(r[4]),
                _int_or_none(r[5]),
                _int_or_none(r[6]),
                _int_or_none(r[7]),
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

    cur.execute("CREATE INDEX IF NOT EXISTS idx_obs_family ON observations_profile(family);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_obs_metric ON observations_profile(metric);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_recon_family ON frd_reconciliation(family);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_recon_status ON frd_reconciliation(status);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_debt_cluster ON debt_types_inventory(cluster_id);")

    scon.commit()
    scon.close()

    print(f"\nSaved untruncated SQLite catalog (23 analytical tables) -> {db_path}")


# ==============================================================================
# 12. SCORECARD
# ==============================================================================


def print_scorecard(findings: list[Finding], logger: CatalogLogger) -> int:
    logger.section("12. SCORECARD", "Structural invariants pass/fail & open migration items")

    inv = [f for f in findings if f.category == "INVARIANT"]
    watch = [f for f in findings if f.category == "WATCHLIST"]
    pend = [f for f in findings if f.category == "PENDING"]

    inv_fail = [f for f in inv if f.status == "FAIL"]
    watch_new = [f for f in watch if f.status == "NEW"]
    pend_open = [f for f in pend if f.status == "PENDING"]

    rows = [[f.category, f.section, f.name, f.status, f.details] for f in findings]
    logger.render_table(
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
    logger.log_text(f"\n{summary}\n")
    return 1 if inv_fail else 0


# ==============================================================================
# MAIN
# ==============================================================================


def main() -> int:
    db_path = Path("data/etf.duckdb")
    sqlite_path = Path("data/silver_catalog.sqlite")
    log_path = Path("data/silver_catalog.log")

    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        return 1

    print(f"Connected to DuckDB: {db_path} (READ_ONLY)")
    t0 = time.time()
    con = duckdb.connect(str(db_path), read_only=True)

    logger = CatalogLogger(log_path)
    findings: list[Finding] = []

    try:
        schema_info = detect_and_setup_silver_views(con)
        mode_str = (
            "UNIFIED (silver.observations)"
            if schema_info["is_unified"]
            else "LEGACY (silver.product_metrics & product_dimensions)"
        )
        print(f"Silver schema mode: {mode_str}")
        logger.write_header(db_path, mode_str)

        run_invariants(con, findings, schema_info, logger)
        ep_summary, payload_deltas, dwell_summary, pub_lag = run_payload_cadence(con, logger)
        cadence_rows = run_effective_date_cadence(con, logger)
        reach_rows = run_backward_fill_audit(con, logger)
        ccy_rows, aum_sim_rows = run_currency_profile(con, logger)
        sim_results = run_locf_coverage_simulation(con, cadence_rows, logger)
        sleeve_rows, debt_cluster_rows, fee_alloc_rows = run_empirical_profiles(con, logger)
        run_pending(con, findings, schema_info, logger)
        footprint, metrics_rows, dims_rows, dim_summary_rows, style_box_rows, aum_tokens = run_catalog(con, logger)

        recon_records, debt_type_inventory, industry_inventory = run_frd_contract_reconciliation(con, logger, findings)

        rc = print_scorecard(findings, logger)

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
            recon_records,
            debt_type_inventory,
            industry_inventory,
            aum_sim_rows,
        )

        elapsed = time.time() - t0
        logger.log_text(f"Detailed Markdown audit log saved -> {log_path.resolve()}")
        print(f"Finished in {elapsed:.2f}s (exit {rc}).\n")
        return rc
    finally:
        con.close()
        logger.close()


if __name__ == "__main__":
    sys.exit(main())
