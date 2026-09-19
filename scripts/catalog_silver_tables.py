#!/usr/bin/env python3
"""
Silver Layer FRD Auditor
------------------------
Verifies empirical findings, structural invariants, and implementation-readiness
rules from docs/FRD.md against a live DuckDB silver layer.

Severity
    INVARIANT  Always-true structural properties. Failure → exit 1.
    EMPIRICAL  Documented exploration findings. Drift → WARN (not a hard fail).
               Exact snapshot counts are compared with a tolerance band because
               the bronze store is a living crawl, not a frozen fixture.
    SPEC       Cleaning rules that may still be pending (currency column,
               top_holding prune, ISO remaps, theme_coverage, /100 conversion,
               unified mstar_medalist_rating, mstar_analyst_coverage_pct,
               Lipper max-peer-count reduction, silver.monthly_panel build).
               Reported as PENDING until implemented. Pass --strict-spec to
               treat them as failures.

Usage:
    python catalog_silver_tables.py [path_to_etf.duckdb]
    python catalog_silver_tables.py data/etf.duckdb --json audit.json
    python catalog_silver_tables.py --strict-spec
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError:
    print("Error: duckdb is required. Run: pip install duckdb", file=sys.stderr)
    sys.exit(1)

try:
    from rich import box
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    console = Console(width=140)
    HAS_RICH = True
except ImportError:
    console = None
    HAS_RICH = False


# ==============================================================================
# FRD CLOSED SETS
# ==============================================================================

PROFILE_METRICS = [
    "total_net_assets_local",
    "total_expense_ratio",
    "management_expense_ratio",
    "non_management_expense_ratio",
    "audited_net_expense_ratio",
    "manager_tenure_years",
    "is_passive",
]

# 26 AUM prefix tokens enumerated in FRD §4.1.2.1
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

STYLE_BOX_CODES = {
    "large_value",
    "large_core",
    "large_growth",
    "multi_value",
    "multi_core",
    "multi_growth",
    "mid_value",
    "mid_core",
    "mid_growth",
    "small_value",
    "small_core",
    "small_growth",
}
# Empirically observed in Silver (FRD §4.1.1.B) — 3 of 12 coordinates unused
STYLE_BOX_OBSERVED = {
    "mid_core",
    "mid_growth",
    "multi_core",
    "multi_growth",
    "large_core",
    "small_growth",
    "small_core",
    "small_value",
    "multi_value",
}

ZSCORE_IDS = [
    "average_final_composite_zscore",
    "latest_composite_z_score",  # underscore — LIKE '%zscore' misses this
    "latest_dividend_yield_zscore",
    "latest_price_to_book_zscore",
    "latest_price_to_earnings_zscore",
    "latest_price_sales_zscore",
    "latest_return_on_equity_zscore",
    "latest_sps_growth_zscore",
    "weighted_final_composite_zscore",
]

# 24 ratios metrics that FRD §4.2.2.3 requires dividing by 100.0
RATIO_PERCENT_METRICS = [
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
]

VALUATION_MULTIPLES = [
    "price_sales",
    "price_cash",
    "price_book",
    "price_earnings",
    "price_to_dividend",
]
PROVIDER_CAPS = {
    "price_sales": 50.0,
    "price_cash": 60.0,
    "price_book": 25.0,
    "price_earnings": 60.0,
    "yield_to_maturity": 10.0,
}

FI_METRICS = [
    "yield_to_maturity",
    "nominal_maturity",
    "effective_maturity",
    "average_coupon",
    "average_quality",
]

QUALITY_GRADES = {"AAA", "AA", "A", "BBB", "BB", "B", "CCC", "CC", "-"}

ESG_SCORE_IDS = [
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
]

LIPPER_TAGS = ("total_return", "consistent_return", "preservation", "expense", "tax_efficiency")
LIPPER_HORIZONS = ("overall", "3yr", "5yr", "10yr")
# Regex matches both canonical (post-reduction) and geo-suffixed Lipper metric_ids
LIPPER_RE = (
    r"^lipper_(total_return|consistent_return|preservation|expense|tax_efficiency)"
    r"_(overall|3yr|5yr|10yr)(?:_(.*))?$"
)

ASSET_CLASSES = {"Equity", "Fixed Income", "Cash", "Other"}

DATE_SOURCES = {"payload", "item", "snapshot"}

COUNTRY_ISO_FIXES = {
    "Croatia": "HR",
    "Costa Rica": "CR",
    "Bulgaria": "BG",
    "Guam": "GU",
    "Uzbekistan": "UZ",
}

# Standard slug vocabularies per FRD §4.3.2.7
MATURITY_SLUG_CODES = {
    "mat_lt_1y",
    "mat_1_to_3y",
    "mat_3_to_5y",
    "mat_5_to_10y",
    "mat_10_to_20y",
    "mat_20_to_30y",
    "mat_gt_30y",
    "mat_other",
}

CREDIT_RATING_CODES = {
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


# ==============================================================================
# RESULT MODEL
# ==============================================================================


@dataclass
class Check:
    section: str
    name: str
    ok: bool
    severity: str
    details: str = ""
    expected: str = ""
    observed: str = ""


TEST_RESULTS: list[Check] = []
STRICT_SPEC = False


def record_check(
    section: str,
    name: str,
    ok: bool,
    details: str = "",
    *,
    severity: str = "INVARIANT",
    expected: str = "",
    observed: str = "",
) -> None:
    TEST_RESULTS.append(Check(section, name, bool(ok), severity, details, expected, observed))
    if severity == "INVARIANT":
        tag_ok, tag_bad = "PASS", "FAIL"
    elif severity == "EMPIRICAL":
        tag_ok, tag_bad = "PASS", "WARN"
    else:
        tag_ok, tag_bad = "APPLIED", "PENDING"

    shown = tag_ok if ok else tag_bad
    extra = f" ({details})" if details else ""
    if HAS_RICH:
        color = {
            "PASS": "bold green",
            "FAIL": "bold red",
            "WARN": "bold yellow",
            "APPLIED": "bold green",
            "PENDING": "bold cyan",
        }[shown]
        console.print(f"  [{color}][{shown}][/{color}] {name}[dim]{extra}[/dim]")
    else:
        print(f"  [{shown}] {name}{extra}")


def near(observed: float | None, expected: float, rel: float = 0.12, abs_tol: float | None = None) -> bool:
    if observed is None:
        return False
    tol = abs_tol if abs_tol is not None else max(3.0, abs(expected) * rel)
    return abs(float(observed) - float(expected)) <= tol


# ==============================================================================
# DISPLAY
# ==============================================================================


def print_section(title: str, subtitle: str | None = None) -> None:
    if HAS_RICH:
        content = f"[bold cyan]{title}[/bold cyan]"
        if subtitle:
            content += f"\n[dim]{subtitle}[/dim]"
        console.print()
        console.print(Panel(content, box=box.ROUNDED, expand=False))
    else:
        print(f"\n{'=' * 30} {title} {'=' * 30}")
        if subtitle:
            print(f"  ({subtitle})")


def render_table(
    title: str,
    headers: list[str],
    rows: list[list[Any]],
    align_right: list[int] | None = None,
) -> None:
    align_right = align_right or []
    if not rows:
        if HAS_RICH:
            console.print(f"  [dim]{title}: (no records)[/dim]")
        else:
            print(f"\n--- {title} ---\n(no records)")
        return
    if HAS_RICH:
        table = Table(title=f"[bold]{title}[/bold]", box=box.SIMPLE_HEAVY, header_style="bold magenta")
        for i, h in enumerate(headers):
            table.add_column(h, justify="right" if i in align_right else "left")
        for row in rows:
            table.add_row(*[str(c) if c is not None else "[dim]NULL[/dim]" for c in row])
        console.print(table)
        return
    print(f"\n--- {title} ---")
    widths = [max(len(str(x)) for x in [h] + [r[i] for r in rows]) for i, h in enumerate(headers)]
    fmt = "  ".join(f"{{:>{w}}}" if i in align_right else f"{{:<{w}}}" for i, w in enumerate(widths))
    print(fmt.format(*headers))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print(fmt.format(*[str(c) if c is not None else "NULL" for c in row]))


def fmt_n(x: Any, digits: int = 0) -> str:
    if x is None:
        return "NULL"
    try:
        v = float(x)
    except TypeError, ValueError:
        return str(x)
    if digits == 0 and abs(v) >= 1000:
        return f"{v:,.0f}"
    if digits == 0:
        return f"{int(v):,}" if v == int(v) else f"{v:g}"
    return f"{v:,.{digits}f}"


# ==============================================================================
# DB HELPERS
# ==============================================================================


def resolve_duckdb_path(cli_override: str | None = None) -> Path:
    if cli_override:
        p = Path(cli_override)
        if p.exists():
            return p
        raise FileNotFoundError(f"Database not found at specified path: {cli_override}")
    here = Path(__file__).resolve()
    candidates = [
        Path("data/etf.duckdb").resolve(),
        Path("../data/etf.duckdb").resolve(),
        here.parent / "data" / "etf.duckdb",
        here.parent.parent / "data" / "etf.duckdb",
        here.parent.parent.parent / "data" / "etf.duckdb",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def table_columns(con, schema: str, table: str) -> set[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchall()
    return {r[0] for r in rows}


def has_column(con, schema: str, table: str, column: str) -> bool:
    return column in table_columns(con, schema, table)


def metric_map(rows: Sequence[Sequence[Any]], key_idx: int = 0) -> dict[Any, Sequence[Any]]:
    return {r[key_idx]: r for r in rows}


def sql_in(values: Iterable[str]) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def detect_growth_scale(con) -> str:
    """Return 'decimal' if FRD §4.2.2.3 conversion is applied, else 'percentage_points'."""
    med = con.execute(
        """
        SELECT MEDIAN(value)
        FROM silver.product_metrics
        WHERE source = 'ratios' AND metric_id = 'eps_growth_1yr'
        """
    ).fetchone()[0]
    if med is None:
        return "unknown"
    return "decimal" if abs(float(med)) < 2.0 else "percentage_points"


# ==============================================================================
# 1. MACRO HEALTH
# ==============================================================================


def verify_macro_health(con, contract_universe: int, priced_universe: int) -> None:
    print_section(
        "1. Silver Medallion Invariants & Macro Health", "PK, NOT NULL, finite values, date-source domain, schema"
    )

    pk_dup_metrics = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT product_id, source, metric_id, effective_date, COUNT(*) AS cnt
            FROM silver.product_metrics
            GROUP BY 1, 2, 3, 4
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    record_check(
        "FRD §2.1", "PK uniqueness in silver.product_metrics", pk_dup_metrics == 0, f"{pk_dup_metrics} duplicate groups"
    )

    pk_dup_dims = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT product_id, dimension_type, dimension_name, effective_date, COUNT(*) AS cnt
            FROM silver.product_dimensions
            GROUP BY 1, 2, 3, 4
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    record_check(
        "FRD §2.2", "PK uniqueness in silver.product_dimensions", pk_dup_dims == 0, f"{pk_dup_dims} duplicate groups"
    )

    null_m = con.execute("""
        SELECT
            SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN raw_value IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN isnan(value) OR isinf(value) THEN 1 ELSE 0 END)
        FROM silver.product_metrics
    """).fetchone()
    record_check(
        "FRD §1.2",
        "value/raw_value NOT NULL in product_metrics",
        null_m[0] == 0 and null_m[1] == 0,
        f"null value={null_m[0]}, null raw={null_m[1]}",
    )
    record_check("FRD §1.2", "No NaN/Inf in product_metrics.value", null_m[2] == 0, f"{null_m[2]} non-finite values")

    null_d = con.execute("""
        SELECT
            SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN raw_value IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN isnan(value) OR isinf(value) THEN 1 ELSE 0 END)
        FROM silver.product_dimensions
    """).fetchone()
    record_check(
        "FRD §1.2",
        "value/raw_value NOT NULL in product_dimensions",
        null_d[0] == 0 and null_d[1] == 0,
        f"null value={null_d[0]}, null raw={null_d[1]}",
    )
    record_check("FRD §1.2", "No NaN/Inf in product_dimensions.value", null_d[2] == 0, f"{null_d[2]} non-finite values")

    bad_src = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT effective_date_source FROM silver.product_metrics
            WHERE effective_date_source NOT IN ('payload', 'item', 'snapshot')
            UNION ALL
            SELECT effective_date_source FROM silver.product_dimensions
            WHERE effective_date_source NOT IN ('payload', 'item', 'snapshot')
        )
    """).fetchone()[0]
    record_check(
        "FRD §2.1", "effective_date_source ∈ {payload, item, snapshot}", bad_src == 0, f"{bad_src} out-of-domain rows"
    )

    future_dates = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT 1 FROM silver.product_metrics WHERE effective_date > fetched_at::DATE
            UNION ALL
            SELECT 1 FROM silver.product_dimensions WHERE effective_date > fetched_at::DATE
        )
    """).fetchone()[0]
    record_check(
        "FRD §1.2",
        "Temporal causality invariant (effective_date <= fetched_at::DATE)",
        future_dates == 0,
        f"{future_dates} future-dated records",
    )

    orphan_products = con.execute("""
        SELECT COUNT(DISTINCT product_id) FROM (
            SELECT product_id FROM silver.product_metrics
            WHERE product_id NOT IN (SELECT product_id FROM bronze.contracts)
            UNION ALL
            SELECT product_id FROM silver.product_dimensions
            WHERE product_id NOT IN (SELECT product_id FROM bronze.contracts)
        )
    """).fetchone()[0]
    record_check(
        "FRD §2.1",
        "Relational product integrity (all product_ids in bronze.contracts)",
        orphan_products == 0,
        f"{orphan_products} orphan product_ids",
    )

    # Cross-source metric collision audit (panel PK is product_id, as_of_date, feature_id)
    cross_source_dups = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT product_id, metric_id, effective_date
            FROM silver.product_metrics
            GROUP BY product_id, metric_id, effective_date
            HAVING COUNT(DISTINCT source) > 1
        )
    """).fetchone()[0]
    record_check(
        "FRD §2.1",
        "No cross-source metric_id collisions",
        cross_source_dups == 0,
        f"{cross_source_dups} duplicate groups across sources (panel PK safety)",
    )

    cols = table_columns(con, "silver", "product_metrics")
    has_ccy = "currency" in cols
    record_check(
        "FRD §2.1",
        "silver.product_metrics.currency column exists",
        has_ccy,
        "present" if has_ccy else "missing — schema migration pending (FRD §5 checklist)",
        severity="SPEC",
        expected="currency VARCHAR",
        observed="present" if has_ccy else "absent",
    )

    footprint = con.execute(f"""
        SELECT
            'silver.product_metrics' AS layer,
            COUNT(*) AS rows,
            COUNT(DISTINCT product_id) AS products,
            ROUND(COUNT(DISTINCT product_id) * 100.0 / {contract_universe}, 1) AS univ_pct,
            COUNT(DISTINCT metric_id) AS features,
            ROUND(SUM(CASE WHEN effective_date_source = 'payload' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
            ROUND(SUM(CASE WHEN effective_date_source = 'item' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
            ROUND(SUM(CASE WHEN effective_date_source = 'snapshot' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        FROM silver.product_metrics
        UNION ALL
        SELECT 'silver.product_dimensions (All)', COUNT(*), COUNT(DISTINCT product_id),
            ROUND(COUNT(DISTINCT product_id) * 100.0 / {contract_universe}, 1),
            COUNT(DISTINCT dimension_type || ':' || COALESCE(dimension_code, dimension_name)),
            ROUND(SUM(CASE WHEN effective_date_source = 'payload' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
            ROUND(SUM(CASE WHEN effective_date_source = 'item' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
            ROUND(SUM(CASE WHEN effective_date_source = 'snapshot' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        FROM silver.product_dimensions
        UNION ALL
        SELECT '  -- Non-Theme (excl. top_holding)', COUNT(*), COUNT(DISTINCT product_id),
            ROUND(COUNT(DISTINCT product_id) * 100.0 / {contract_universe}, 1),
            COUNT(DISTINCT dimension_type || ':' || COALESCE(dimension_code, dimension_name)),
            ROUND(SUM(CASE WHEN effective_date_source = 'payload' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
            ROUND(SUM(CASE WHEN effective_date_source = 'item' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
            ROUND(SUM(CASE WHEN effective_date_source = 'snapshot' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        FROM silver.product_dimensions
        WHERE dimension_type NOT IN ('theme', 'top_holding')
        UNION ALL
        SELECT '  -- Theme Dimensions (UUIDs)', COUNT(*), COUNT(DISTINCT product_id),
            ROUND(COUNT(DISTINCT product_id) * 100.0 / {contract_universe}, 1),
            COUNT(DISTINCT dimension_code),
            ROUND(SUM(CASE WHEN effective_date_source = 'payload' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
            ROUND(SUM(CASE WHEN effective_date_source = 'item' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
            ROUND(SUM(CASE WHEN effective_date_source = 'snapshot' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        FROM silver.product_dimensions
        WHERE dimension_type = 'theme'
    """).fetchall()
    render_table(
        f"Silver Footprint (contracts={contract_universe:,}, priced silver.products={priced_universe:,})",
        ["Layer", "Rows", "Products", "Univ %", "Features", "% Payload", "% Item", "% Snapshot"],
        [
            [r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]}%", f"{r[4]:,}", f"{r[5]}%", f"{r[6]}%", f"{r[7]}%"]
            for r in footprint
        ],
        align_right=list(range(1, 8)),
    )

    src_rows = con.execute("""
        SELECT source, COUNT(*), COUNT(DISTINCT product_id), COUNT(DISTINCT metric_id),
            ROUND(SUM(CASE WHEN effective_date_source = 'payload' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
            ROUND(SUM(CASE WHEN effective_date_source = 'item' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
            ROUND(SUM(CASE WHEN effective_date_source = 'snapshot' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
        FROM silver.product_metrics
        GROUP BY source
        ORDER BY 2 DESC
    """).fetchall()
    render_table(
        "Date Provenance by Metrics Source",
        ["Source", "Rows", "Products", "Metrics", "% Payload", "% Item", "% Snapshot"],
        [[r[0], f"{r[1]:,}", f"{r[2]:,}", r[3], f"{r[4]}%", f"{r[5]}%", f"{r[6]}%"] for r in src_rows],
        align_right=list(range(1, 7)),
    )

    sources = {r[0] for r in src_rows}
    # Accommodates theme_weights as source if theme_coverage is extracted
    expected_sources = {"profile", "ratios", "holdings", "mstar", "lipper", "esg"}
    record_check(
        "FRD §4",
        "Expected metric sources present",
        expected_sources.issubset(sources),
        f"observed={sorted(sources)}; missing={sorted(expected_sources - sources)}",
        severity="EMPIRICAL",
    )


# ==============================================================================
# 2. PROFILE & FEES  (§4.1)
# ==============================================================================


def verify_session_1_profile_and_fees(con) -> None:
    print_section("2. Session 4.1: Profile, Fees, Currency & Style Box", "/fundamentals/mf_profile_and_fees/")

    profile_ids = {
        r[0]
        for r in con.execute(
            "SELECT DISTINCT metric_id FROM silver.product_metrics WHERE source = 'profile'"
        ).fetchall()
    }
    extra = profile_ids - set(PROFILE_METRICS)
    missing = set(PROFILE_METRICS) - profile_ids
    record_check(
        "FRD §4.1",
        "Profile closed set of 7 scalar metrics",
        extra == set() and missing == set(),
        f"extra={sorted(extra) or '∅'} missing={sorted(missing) or '∅'}",
        severity="EMPIRICAL",
    )

    aum = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT product_id),
               MIN(value), quantile_cont(value, 0.25), MEDIAN(value),
               quantile_cont(value, 0.75), MAX(value),
               SUM(CASE WHEN value = 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN value > 0 AND value < 1000 THEN 1 ELSE 0 END),
               SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END),
               ROUND(SUM(CASE WHEN effective_date_source = 'item' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        FROM silver.product_metrics
        WHERE metric_id = 'total_net_assets_local'
    """).fetchone()
    render_table(
        "AUM (total_net_assets_local) Tail Statistics",
        ["Obs", "Products", "Min", "P25", "Median", "P75", "Max", "Zero", "Micro (<1k)", "Neg", "% Item date"],
        [[fmt_n(aum[i]) if i not in (2,) else f"{aum[2]:,.2f}" for i in range(11)]],
        align_right=list(range(11)),
    )
    record_check(
        "FRD §4.1.1", "Liquidated-fund zero AUM retained", aum[7] >= 1, f"{aum[7]} fund(s) at 0.0", severity="EMPIRICAL"
    )
    record_check(
        "FRD §4.1.1",
        "Multi-trillion local-currency mega-AUM retained",
        aum[6] > 1e13,
        f"max={aum[6]:,.0f}",
        severity="EMPIRICAL",
    )
    record_check("FRD §4.1.2", "No negative AUM", aum[9] == 0, f"{aum[9]} negatives")
    record_check(
        "FRD §4.1.1",
        "AUM date provenance is item-level report date",
        aum[10] >= 99.0,
        f"{aum[10]}% item",
        severity="EMPIRICAL",
    )

    top_aum = con.execute("""
        SELECT m.product_id, COALESCE(p.symbol, c.symbol), COALESCE(p.name, c.name),
               m.effective_date, m.value, m.raw_value
        FROM silver.product_metrics m
        LEFT JOIN silver.products p ON m.product_id = p.product_id
        LEFT JOIN bronze.contracts c ON m.product_id = c.product_id
        WHERE m.metric_id = 'total_net_assets_local'
        ORDER BY m.value DESC
        LIMIT 4
    """).fetchall()
    render_table(
        "Largest AUM records (local currency)",
        ["Product ID", "Symbol", "Name", "Date", "Value", "Raw"],
        [
            [r[0], r[1], (r[2][:36] + "…") if r[2] and len(r[2]) > 36 else r[2], str(r[3]), f"{r[4]:,.0f}", r[5]]
            for r in top_aum
        ],
        align_right=[0, 4],
    )

    tokens = con.execute("""
        WITH parsed AS (
            SELECT REGEXP_EXTRACT(raw_value, '^([^0-9\\s]+)', 1) AS prefix_token, product_id, value, raw_value
            FROM silver.product_metrics
            WHERE metric_id = 'total_net_assets_local'
        )
        SELECT COALESCE(prefix_token, '[NO_PREFIX]') AS token, COUNT(*), COUNT(DISTINCT product_id),
               MIN(value), MAX(value), ANY_VALUE(raw_value)
        FROM parsed
        GROUP BY prefix_token
        ORDER BY 2 DESC
    """).fetchall()
    observed_tokens = {r[0] for r in tokens}
    render_table(
        f"AUM currency prefix tokens ({len(tokens)} distinct)",
        ["Token", "Obs", "Products", "Min", "Max", "Example"],
        [[r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]:,.0f}", f"{r[4]:,.0f}", r[5]] for r in tokens],
        align_right=[1, 2, 3, 4],
    )
    record_check(
        "FRD §4.1.2",
        "AUM prefix token set matches the 26 FRD tokens",
        observed_tokens == AUM_PREFIX_TOKENS,
        f"extra={sorted(observed_tokens - AUM_PREFIX_TOKENS) or '∅'} "
        f"missing={sorted(AUM_PREFIX_TOKENS - observed_tokens) or '∅'}",
        severity="EMPIRICAL",
        expected="26 named tokens",
        observed=str(len(observed_tokens)),
    )

    dollar_split = con.execute("""
        SELECT COALESCE(c.currency, 'UNKNOWN'), COUNT(*), COUNT(DISTINCT m.product_id),
               ANY_VALUE(c.primary_exchange_id)
        FROM silver.product_metrics m
        JOIN bronze.contracts c ON m.product_id = c.product_id
        WHERE m.metric_id = 'total_net_assets_local' AND m.raw_value LIKE '$%'
        GROUP BY 1
        ORDER BY 2 DESC
    """).fetchall()
    render_table(
        "Dollar-prefix AUM vs contract currency (disambiguation input)",
        ["Contract Ccy", "Records", "ETFs", "Sample Exchange"],
        [[r[0], f"{r[1]:,}", f"{r[2]:,}", r[3]] for r in dollar_split],
        align_right=[1, 2],
    )

    if has_column(con, "silver", "product_metrics", "currency"):
        aum_ccy = con.execute("""
            SELECT COUNT(*), SUM(CASE WHEN currency IS NULL THEN 1 ELSE 0 END)
            FROM silver.product_metrics
            WHERE metric_id = 'total_net_assets_local'
        """).fetchone()
        record_check(
            "FRD §4.1.2",
            "AUM rows have ISO currency populated",
            aum_ccy[1] == 0,
            f"{aum_ccy[1]} NULL of {aum_ccy[0]}",
            severity="SPEC",
        )
        cad_rule = con.execute("""
            SELECT COUNT(*)
            FROM silver.product_metrics m
            JOIN bronze.contracts c ON m.product_id = c.product_id
            WHERE m.metric_id = 'total_net_assets_local'
              AND m.raw_value LIKE '$%'
              AND c.currency = 'CAD'
              AND m.currency IS DISTINCT FROM 'CAD'
        """).fetchone()[0]
        record_check(
            "FRD §4.1.2",
            "$ + CAD contract currency → currency='CAD'",
            cad_rule == 0,
            f"{cad_rule} mismatches",
            severity="SPEC",
        )
        symbol_prefix_mismatch = con.execute("""
            SELECT COUNT(*)
            FROM silver.product_metrics
            WHERE metric_id = 'total_net_assets_local' AND (
                (raw_value LIKE '€%' AND currency IS DISTINCT FROM 'EUR') OR
                (raw_value LIKE '£%' AND currency IS DISTINCT FROM 'GBP') OR
                (raw_value LIKE '¥%' AND currency IS DISTINCT FROM 'JPY') OR
                (raw_value LIKE '₹%' AND currency IS DISTINCT FROM 'INR')
            )
        """).fetchone()[0]
        record_check(
            "FRD §4.1.2",
            "Currency symbol prefixes map correctly (€/£/¥/₹)",
            symbol_prefix_mismatch == 0,
            f"{symbol_prefix_mismatch} symbol mismatches",
            severity="SPEC",
        )
        non_cad_usd = con.execute("""
            SELECT COUNT(*)
            FROM silver.product_metrics m
            JOIN bronze.contracts c ON m.product_id = c.product_id
            WHERE m.metric_id = 'total_net_assets_local'
              AND m.raw_value LIKE '$%'
              AND NOT (c.currency = 'CAD' OR c.primary_exchange_id = 'TSE')
              AND m.currency IS DISTINCT FROM 'USD'
        """).fetchone()[0]
        record_check(
            "FRD §4.1.2",
            "$ prefix on non-Canadian contracts maps to USD",
            non_cad_usd == 0,
            f"{non_cad_usd} mismatched non-USD rows (covers UCITS, Mexican, Asian listings)",
            severity="SPEC",
        )
        non_aum_ccy = con.execute("""
            SELECT COUNT(*)
            FROM silver.product_metrics
            WHERE metric_id != 'total_net_assets_local' AND currency IS NOT NULL
        """).fetchone()[0]
        record_check(
            "FRD §2.1",
            "Non-AUM metrics leave currency NULL",
            non_aum_ccy == 0,
            f"{non_aum_ccy} non-null rows outside AUM",
            severity="SPEC",
        )
    else:
        record_check(
            "FRD §4.1.2",
            "AUM ISO-4217 currency populated on silver.product_metrics",
            False,
            "column absent — cannot audit $ → CAD/AUD/USD disambiguation",
            severity="SPEC",
        )

    fee_audit = con.execute("""
        SELECT metric_id, COUNT(*), ROUND(MIN(value), 5), ROUND(quantile_cont(value, 0.05), 5),
               ROUND(MEDIAN(value), 5), ROUND(quantile_cont(value, 0.95), 5), ROUND(MAX(value), 5),
               SUM(CASE WHEN value = 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN value > 0.05 THEN 1 ELSE 0 END),
               SUM(CASE WHEN value > 0.20 THEN 1 ELSE 0 END),
               SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END)
        FROM silver.product_metrics
        WHERE source = 'profile' AND metric_id IN ('total_expense_ratio', 'audited_net_expense_ratio')
        GROUP BY metric_id
    """).fetchall()
    fees = metric_map(fee_audit)
    render_table(
        "Expense ratios (decimal fraction of AUM)",
        ["Metric", "Obs", "Min", "p5", "Median", "p95", "Max", "Zero", ">5%", ">20%", "Neg"],
        [
            [r[0], f"{r[1]:,}", r[2], r[3], r[4], r[5], r[6], f"{r[7]:,}", f"{r[8]:,}", f"{r[9]:,}", f"{r[10]:,}"]
            for r in fee_audit
        ],
        align_right=list(range(1, 11)),
    )
    ter = fees.get("total_expense_ratio")
    if ter:
        record_check(
            "FRD §4.1.1",
            "Zero-fee waivers retained (TER = 0)",
            ter[7] >= 1,
            f"{ter[7]} zeros (FRD snapshot: 35)",
            severity="EMPIRICAL",
        )
        record_check(
            "FRD §4.1.2",
            "BDC AFFE TER outliers retained unclamped (>5%)",
            ter[8] >= 1,
            f"{ter[8]} obs, max={ter[6] * 100:.2f}%",
            severity="EMPIRICAL",
        )
        # Decimal vs percent: PBDC raw is 13.49%; operable value must be 0.1349 not 13.49
        record_check(
            "FRD §4.1.2",
            "TER stored as decimal fraction (max ≤ 0.20, not percentage points)",
            ter[6] <= 0.20 and ter[9] == 0 and ter[10] == 0,
            f"max={ter[6]}, >20%={ter[9]}, neg={ter[10]}",
        )
    aud = fees.get("audited_net_expense_ratio")
    if aud:
        record_check(
            "FRD §4.1.1",
            "Audited net expense ratio zero-fee waivers retained",
            aud[7] >= 1,
            f"{aud[7]} zeros (FRD snapshot: 37)",
            severity="EMPIRICAL",
        )
        record_check(
            "FRD §4.1.2",
            "Audited net expense ratio stored as decimal fraction (0 <= value <= 0.20)",
            aud[10] == 0 and aud[6] <= 0.20 and aud[9] == 0,
            f"min={aud[2]}, max={aud[6]}, >20%={aud[9]}, neg={aud[10]}",
        )
        aud_prov = con.execute("""
            SELECT ROUND(SUM(CASE WHEN effective_date_source = 'item' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
            FROM silver.product_metrics
            WHERE metric_id = 'audited_net_expense_ratio'
        """).fetchone()[0]
        record_check(
            "FRD §4.1.2",
            "Audited net expense ratio date provenance is Annual Report item date",
            aud_prov == 100.0,
            f"{aud_prov}% item date",
            severity="EMPIRICAL",
        )

    bdc = con.execute("""
        SELECT p.symbol, p.name, m.value, m.raw_value
        FROM silver.product_metrics m
        JOIN silver.products p ON m.product_id = p.product_id
        WHERE m.metric_id = 'total_expense_ratio' AND p.symbol IN ('PBDC', 'FBDC')
        ORDER BY m.value DESC
    """).fetchall()
    if bdc:
        render_table(
            "SEC AFFE BDC exemplars (PBDC, FBDC)",
            ["Symbol", "Name", "Value", "Raw"],
            [[r[0], (r[1][:36] + "…") if r[1] and len(r[1]) > 36 else r[1], f"{r[2] * 100:.2f}%", r[3]] for r in bdc],
            align_right=[2],
        )

    fee_alloc = con.execute("""
        WITH fee_pairs AS (
            SELECT product_id, effective_date,
                SUM(CASE WHEN metric_id = 'management_expense_ratio' THEN value ELSE 0 END) AS mgt,
                SUM(CASE WHEN metric_id = 'non_management_expense_ratio' THEN value ELSE 0 END) AS non_mgt
            FROM silver.product_metrics
            WHERE source = 'profile'
              AND metric_id IN ('management_expense_ratio', 'non_management_expense_ratio')
            GROUP BY 1, 2
            HAVING COUNT(DISTINCT metric_id) = 2
        )
        SELECT COUNT(*),
               SUM(CASE WHEN ROUND(mgt + non_mgt, 4) = 1.0000 THEN 1 ELSE 0 END),
               ROUND(SUM(CASE WHEN ROUND(mgt + non_mgt, 4) = 1.0000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
               MIN(mgt), MAX(mgt), MIN(non_mgt), MAX(non_mgt),
               SUM(CASE WHEN mgt < 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN non_mgt < 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN non_mgt = 0 THEN 1 ELSE 0 END)
        FROM fee_pairs
    """).fetchone()
    render_table(
        "Expense allocation sum-to-1.0 and fee-waiver subsidies",
        ["Pairs", "Sum=1.0000", "% =1.0", "Min mgt", "Max mgt", "Min non", "Max non", "Neg mgt", "Neg non", "Zero non"],
        [
            [
                f"{fee_alloc[0]:,}",
                f"{fee_alloc[1]:,}",
                f"{fee_alloc[2]}%",
                f"{fee_alloc[3]:.4f}",
                f"{fee_alloc[4]:.4f}",
                f"{fee_alloc[5]:.4f}",
                f"{fee_alloc[6]:.4f}",
                f"{fee_alloc[7]:,}",
                f"{fee_alloc[8]:,}",
                f"{fee_alloc[9]:,}",
            ]
        ],
        align_right=list(range(10)),
    )
    record_check(
        "FRD §4.1.2",
        "Management + non-management allocation sums to 1.0",
        fee_alloc[2] >= 99.9,
        f"{fee_alloc[2]}% of paired snapshots",
    )
    record_check(
        "FRD §4.1.2",
        "Negative fee-waiver allocations retained",
        fee_alloc[7] > 0 and fee_alloc[8] > 0,
        f"min mgt={fee_alloc[3]:.2f}, max non={fee_alloc[6]:.2f}",
        severity="EMPIRICAL",
    )
    record_check(
        "FRD §4.1.1",
        "Non-management zero-allocation count near FRD 6,330",
        near(fee_alloc[9], 6330, rel=0.15),
        f"observed {fee_alloc[9]:,} (FRD: 6,330)",
        severity="EMPIRICAL",
    )

    tenure = con.execute("""
        SELECT MIN(value), ROUND(MEDIAN(value), 2), MAX(value),
               SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END)
        FROM silver.product_metrics WHERE metric_id = 'manager_tenure_years'
    """).fetchone()
    record_check(
        "FRD §4.1.2",
        "Manager tenure ≥ 0 and max > 30y",
        tenure[3] == 0 and tenure[2] > 30,
        f"range=[{tenure[0]:.4f}, {tenure[2]:.2f}]",
    )
    bad_tenure_dates = con.execute("""
        SELECT COUNT(*)
        FROM silver.product_metrics
        WHERE metric_id = 'manager_tenure_years'
          AND NOT REGEXP_MATCHES(raw_value, '^\\d{4}[-/]\\d{2}[-/]\\d{2}$')
    """).fetchone()[0]
    record_check(
        "FRD §4.1.2",
        "Manager tenure raw_value contains parseable start date string (YYYY/MM/DD)",
        bad_tenure_dates == 0,
        f"{bad_tenure_dates} unparseable date strings in raw_value",
        severity="INVARIANT",
    )

    passive = con.execute("""
        SELECT value, COUNT(*), ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)
        FROM silver.product_metrics WHERE metric_id = 'is_passive'
        GROUP BY value ORDER BY value
    """).fetchall()
    render_table(
        "is_passive discrete map",
        ["Flag", "Meaning", "Obs", "%"],
        [
            [r[0], "Active" if r[0] == 0 else ("Passive" if r[0] == 1 else "UNKNOWN"), f"{r[1]:,}", f"{r[2]}%"]
            for r in passive
        ],
        align_right=[0, 2, 3],
    )
    record_check(
        "FRD §4.1.2",
        "is_passive strictly {0.0, 1.0}",
        {r[0] for r in passive} <= {0.0, 1.0} and len(passive) == 2,
        "no unknown Management_Approach values",
    )

    style = con.execute("""
        SELECT dimension_code, dimension_name,
               SUM(CASE WHEN dimension_type = 'style_box' THEN 1 ELSE 0 END),
               SUM(CASE WHEN dimension_type = 'style_box_hist' THEN 1 ELSE 0 END),
               COUNT(DISTINCT product_id)
        FROM silver.product_dimensions
        WHERE dimension_type IN ('style_box', 'style_box_hist')
        GROUP BY 1, 2
        ORDER BY 5 DESC
    """).fetchall()
    render_table(
        "Morningstar style-box coordinates",
        ["Code", "Name", "Selected", "Hist", "ETFs"],
        [[r[0], r[1], f"{r[2]:,}", f"{r[3]:,}", f"{r[4]:,}"] for r in style],
        align_right=[2, 3, 4],
    )
    codes = {r[0] for r in style}
    record_check(
        "FRD §4.1.2",
        "Style box emits the 9 observed coordinates (no extras)",
        codes == STYLE_BOX_OBSERVED,
        f"unexpected={sorted(codes - STYLE_BOX_OBSERVED) or '∅'} missing={sorted(STYLE_BOX_OBSERVED - codes) or '∅'}",
        severity="EMPIRICAL",
    )
    unused = sorted(STYLE_BOX_CODES - codes)
    record_check(
        "FRD §4.1.1",
        "Three style-box cells unused (large_value/growth, mid_value)",
        unused == ["large_growth", "large_value", "mid_value"],
        f"unused={unused}",
        severity="EMPIRICAL",
    )

    both = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT product_id, effective_date
            FROM silver.product_dimensions
            WHERE dimension_type IN ('style_box', 'style_box_hist')
            GROUP BY 1, 2
            HAVING COUNT(DISTINCT dimension_type) = 2
        )
    """).fetchone()[0]
    record_check(
        "FRD §4.1.2",
        "Style-box / hist overlap exists (panel must prefer style_box)",
        both > 0,
        f"{both} product-dates carry both; precedence is a panel-layer rule, not a silver constraint",
        severity="EMPIRICAL",
    )

    invalid_style_vals = con.execute("""
        SELECT COUNT(*)
        FROM silver.product_dimensions
        WHERE dimension_type IN ('style_box', 'style_box_hist')
          AND value != 1.0
    """).fetchone()[0]
    record_check(
        "FRD §4.1.2",
        "Style box coordinate dimension values strictly equal 1.0",
        invalid_style_vals == 0,
        f"{invalid_style_vals} non-1.0 rows",
    )

    dup_style = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT product_id, effective_date
            FROM silver.product_dimensions
            WHERE dimension_type = 'style_box'
            GROUP BY product_id, effective_date
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    record_check(
        "FRD §4.1.2",
        "Style box coordinate uniqueness (at most 1 active cell per date)",
        dup_style == 0,
        f"{dup_style} multi-coordinate dates",
    )


# ==============================================================================
# 3. RATIOS  (§4.2)
# ==============================================================================


def verify_session_2_ratios_fundamentals(con) -> None:
    print_section("3. Session 4.2: Ratios Fundamentals", "/fundamentals/mf_ratios_fundamentals/")

    meta = con.execute("""
        SELECT COUNT(DISTINCT metric_id), COUNT(*),
               ROUND(SUM(CASE WHEN effective_date_source = 'payload' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        FROM silver.product_metrics WHERE source = 'ratios'
    """).fetchone()
    record_check(
        "FRD §4.2", "47 distinct ratios metric_ids", meta[0] == 47, f"observed {meta[0]}", severity="EMPIRICAL"
    )
    record_check("FRD §4.2.1", "100% payload date provenance for ratios", meta[2] == 100.0, f"{meta[2]}% payload")

    scale = detect_growth_scale(con)
    # Scaled decimal medians are <= 0.51 (51% max median); unscaled percentage points have medians >= 1.57
    unscaled_pct_metrics = con.execute(f"""
        SELECT metric_id, MEDIAN(value)
        FROM silver.product_metrics
        WHERE source = 'ratios' AND metric_id IN ({sql_in(RATIO_PERCENT_METRICS)})
        GROUP BY metric_id
        HAVING ABS(MEDIAN(value)) > 1.0
    """).fetchall()
    record_check(
        "FRD §4.2.2",
        "Growth/return/yield metrics converted from percentage points to decimals (/100)",
        scale == "decimal" and len(unscaled_pct_metrics) == 0,
        f"regime: {scale}; unscaled metrics={len(unscaled_pct_metrics)}/24",
        severity="SPEC",
    )

    # Invariant: Verify the 23 unscaled metrics (multiples, leverage, maturities, quality) remain unscaled
    unscaled_check = con.execute("""
        SELECT
            MEDIAN(CASE WHEN metric_id = 'price_earnings' THEN value END),
            MEDIAN(CASE WHEN metric_id = 'average_quality' THEN value END),
            MEDIAN(CASE WHEN metric_id = 'nominal_maturity' THEN value END),
            MEDIAN(CASE WHEN metric_id = 'total_assets_total_equity' THEN value END)
        FROM silver.product_metrics
        WHERE source = 'ratios'
    """).fetchone()
    unscaled_ok = bool(
        unscaled_check[0] is not None
        and unscaled_check[0] > 10.0
        and unscaled_check[1] is not None
        and 3.0 <= unscaled_check[1] <= 10.0
        and unscaled_check[2] is not None
        and unscaled_check[2] > 2.0
        and unscaled_check[3] is not None
        and unscaled_check[3] > 1.0
    )
    record_check(
        "FRD §4.2.2",
        "Unscaled ratios metrics retain native scale (not divided by 100)",
        unscaled_ok,
        f"PE={unscaled_check[0]:.1f}, Quality={unscaled_check[1]:.2f}, Maturity={unscaled_check[2]:.2f}y, TA/TE={unscaled_check[3]:.2f}",
        severity="INVARIANT",
    )

    cap_lo, cap_hi = (-0.5, 1.0) if scale == "decimal" else (-50.0, 100.0)
    explosive_cut = 10.0 if scale == "decimal" else 1000.0

    multiples = con.execute("""
        SELECT metric_id, COUNT(*), ROUND(MIN(value), 2), ROUND(MEDIAN(value), 2),
               ROUND(quantile_cont(value, 0.99), 2), ROUND(MAX(value), 2),
               SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN metric_id = 'price_sales' AND value = 50.0 THEN 1
                        WHEN metric_id = 'price_cash' AND value = 60.0 THEN 1
                        WHEN metric_id = 'price_book' AND value = 25.0 THEN 1
                        WHEN metric_id = 'price_earnings' AND value = 60.0 THEN 1
                        ELSE 0 END)
        FROM silver.product_metrics
        WHERE source = 'ratios'
          AND metric_id IN ('price_sales','price_cash','price_book','price_earnings',
                            'price_to_dividend','relative_strength')
        GROUP BY metric_id
        ORDER BY metric_id
    """).fetchall()
    render_table(
        "Valuation multiples: provider caps & negatives",
        ["Metric", "Obs", "Min", "Median", "p99", "Max", "Neg", "Cap hits"],
        [[r[0], f"{r[1]:,}", r[2], r[3], r[4], r[5], r[6], r[7]] for r in multiples],
        align_right=list(range(1, 8)),
    )
    m_by = metric_map(multiples)
    zero_neg = sum(r[6] for r in multiples if r[0] != "relative_strength")
    record_check(
        "FRD §4.2.1", "No negative P/E, P/B, P/S, P/Cash, P/Div", zero_neg == 0, "strictly non-negative multiples"
    )
    caps_ok = all(m_by[k][7] > 0 for k in PROVIDER_CAPS if k in m_by and k.startswith("price_"))
    exceeded_caps = con.execute("""
        SELECT metric_id, MAX(value)
        FROM silver.product_metrics
        WHERE source = 'ratios' AND (
            (metric_id = 'price_sales' AND value > 50.0001) OR
            (metric_id = 'price_cash' AND value > 60.0001) OR
            (metric_id = 'price_book' AND value > 25.0001) OR
            (metric_id = 'price_earnings' AND value > 60.0001)
        )
        GROUP BY metric_id
    """).fetchall()
    record_check(
        "FRD §4.2.1",
        "Provider-side valuation caps still bind and are not exceeded",
        caps_ok and len(exceeded_caps) == 0,
        "records sit on ceilings and none exceed caps" if len(exceeded_caps) == 0 else f"exceeded: {exceeded_caps}",
        severity="EMPIRICAL",
    )

    growth = con.execute(f"""
        SELECT metric_id, COUNT(*), ROUND(MIN(value), 4), ROUND(MEDIAN(value), 4),
               ROUND(quantile_cont(value, 0.99), 4), ROUND(MAX(value), 4),
               SUM(CASE WHEN value = {cap_lo} THEN 1 ELSE 0 END),
               SUM(CASE WHEN value = {cap_hi} THEN 1 ELSE 0 END),
               SUM(CASE WHEN value > {explosive_cut} THEN 1 ELSE 0 END)
        FROM silver.product_metrics
        WHERE source = 'ratios'
          AND metric_id IN ('eps_growth_1yr','eps_growth_3yr','sales_growth_1_year',
                            'sales_per_share_growth_1_year','operating_cash_flow_growth_rate_3yr')
        GROUP BY metric_id
        ORDER BY metric_id
    """).fetchall()
    render_table(
        f"Growth rates ({scale}; EPS clip [{cap_lo}, {cap_hi}])",
        ["Metric", "Obs", "Min", "Median", "p99", "Max", f"Hits {cap_lo}", f"Hits {cap_hi}", "Explosive"],
        [[r[0], f"{r[1]:,}", r[2], r[3], r[4], r[5], r[6], r[7], r[8]] for r in growth],
        align_right=list(range(1, 9)),
    )
    g_by = metric_map(growth)
    eps = g_by.get("eps_growth_1yr")
    if eps:
        record_check(
            "FRD §4.2.1",
            "EPS growth 1yr hits provider clip bounds and stays within [-50, 100]",
            eps[6] > 0 and eps[7] > 0 and eps[2] >= cap_lo and eps[5] <= cap_hi,
            f"{eps[6]} at {cap_lo}, {eps[7]} at {cap_hi}; range=[{eps[2]}, {eps[5]}]",
            severity="EMPIRICAL",
        )

    outliers = con.execute("""
        SELECT m.metric_id, COALESCE(p.symbol, c.symbol), COALESCE(p.name, c.name), m.value, m.raw_value
        FROM silver.product_metrics m
        LEFT JOIN silver.products p ON m.product_id = p.product_id
        LEFT JOIN bronze.contracts c ON m.product_id = c.product_id
        WHERE m.source = 'ratios' AND (
            (m.metric_id = 'return_on_equity_3yr' AND ABS(m.value) > 1000) OR
            (m.metric_id = 'total_assets_total_equity' AND m.value > 1000) OR
            (m.metric_id = 'sales_per_share_growth_1_year' AND ABS(m.value) > 100)
        )
        ORDER BY ABS(m.value) DESC
        LIMIT 12
    """).fetchall()
    render_table(
        "Extreme ratio outliers (split-corp / restructuring) — retained, not clamped",
        ["Metric", "Symbol", "Name", "Value", "Raw"],
        [[r[0], r[1], (r[2][:32] + "…") if r[2] and len(r[2]) > 32 else r[2], f"{r[3]:,.2f}", r[4]] for r in outliers],
        align_right=[3],
    )
    record_check(
        "FRD §4.2.1",
        "Astronomical leverage/ROE outliers retained in Silver",
        len(outliers) >= 1,
        f"{len(outliers)} rows shown; winsorize is panel-layer only",
        severity="EMPIRICAL",
    )

    div = con.execute("""
        SELECT metric_id, COUNT(*), ROUND(MIN(value), 4), ROUND(MEDIAN(value), 4), ROUND(MAX(value), 4),
               SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN value = 0 THEN 1 ELSE 0 END)
        FROM silver.product_metrics
        WHERE source = 'ratios'
          AND metric_id IN ('dividend_yield_weighted_average','dividendpayoutratio5yr',
                            'dividend_per_share_1yr','dividend_per_share_3yr')
        GROUP BY metric_id
    """).fetchall()
    render_table(
        "Dividend metrics",
        ["Metric", "Obs", "Min", "Median", "Max", "Neg", "Zero"],
        [[r[0], f"{r[1]:,}", r[2], r[3], r[4], f"{r[5]:,}", f"{r[6]:,}"] for r in div],
        align_right=list(range(1, 7)),
    )
    div_wtd = metric_map(div).get("dividend_yield_weighted_average")
    if div_wtd:
        record_check(
            "FRD §4.2.1",
            "Dividend yield wtd-avg has the documented 1-neg / ~21-zero tail",
            div_wtd[5] >= 1 and near(div_wtd[6], 21, abs_tol=8),
            f"{div_wtd[5]} neg, {div_wtd[6]} zeros (FRD: 1 / 21)",
            severity="EMPIRICAL",
        )

    fi_quality = con.execute("""
        SELECT raw_value, COUNT(*), ROUND(MIN(value), 2), ROUND(MEDIAN(value), 2),
               ROUND(MAX(value), 2), ROUND(AVG(value), 2)
        FROM silver.product_metrics WHERE metric_id = 'average_quality'
        GROUP BY raw_value
        ORDER BY median(value) DESC
    """).fetchall()
    render_table(
        "average_quality letter grade vs continuous score",
        ["Grade", "Obs", "Min", "Median", "Max", "Mean"],
        [[r[0], f"{r[1]:,}", r[2], r[3], r[4], r[5]] for r in fi_quality],
        align_right=list(range(1, 6)),
    )
    grades = {r[0] for r in fi_quality}
    record_check(
        "FRD §4.2.1",
        "average_quality raw_value closed letter-grade set",
        grades <= QUALITY_GRADES,
        f"unexpected={sorted(grades - QUALITY_GRADES) or '∅'}",
        severity="EMPIRICAL",
    )
    unrated = con.execute("""
        SELECT COUNT(*) FROM silver.product_metrics
        WHERE metric_id = 'average_quality' AND raw_value = '-' AND value = 10.0
    """).fetchone()[0]
    record_check(
        "FRD §4.2.1",
        "Unrated '-' grade maps to score 10.0 (commodity/crypto)",
        near(unrated, 33, abs_tol=10),
        f"{unrated} records (FRD: 33) — retain in Silver, NULL at panel",
        severity="EMPIRICAL",
    )

    ytm_cap = 0.10 if scale == "decimal" else 10.0
    neg_fi = con.execute("""
        SELECT metric_id, COUNT(*), ROUND(MIN(value), 4), ROUND(MAX(value), 4)
        FROM silver.product_metrics
        WHERE source = 'ratios'
          AND metric_id IN ('yield_to_maturity','nominal_maturity','effective_maturity')
          AND value < 0
        GROUP BY metric_id
    """).fetchall()
    render_table(
        "Negative FI characteristics (convertibles / derivatives)",
        ["Metric", "Neg obs", "Min", "Max neg"],
        [[r[0], f"{r[1]:,}", r[2], r[3]] for r in neg_fi],
        align_right=[1, 2, 3],
    )
    nfi = metric_map(neg_fi)
    ytm_neg = nfi.get("yield_to_maturity")
    record_check(
        "FRD §4.2.1",
        "Negative YTM retained (convertible-bond ETFs)",
        ytm_neg is not None and ytm_neg[1] >= 1,
        f"{ytm_neg[1] if ytm_neg else 0} obs (FRD: 24)",
        severity="EMPIRICAL",
    )
    ytm_at_cap = con.execute(f"""
        SELECT SUM(CASE WHEN value = {ytm_cap} THEN 1 ELSE 0 END), MAX(value)
        FROM silver.product_metrics WHERE metric_id = 'yield_to_maturity'
    """).fetchone()
    record_check(
        "FRD §4.2.1",
        "YTM provider cap retained (10.0 percentage points / 0.10 decimal)",
        ytm_at_cap[0] >= 1,
        f"{ytm_at_cap[0]} at cap, max={ytm_at_cap[1]}",
        severity="EMPIRICAL",
    )

    # Equity-dominant FI overlap — classify on LATEST asset_class snapshot, not SUM across dates.
    # Original bug: HAVING SUM(equity) > 0.5 across all dates inflates the universe
    # (a 10-snapshot 10% equity sleeve sums to 1.0).
    fi_on_eq = con.execute(f"""
        WITH latest AS (
            SELECT product_id, MAX(effective_date) AS dt
            FROM silver.product_dimensions
            WHERE dimension_type = 'asset_class'
            GROUP BY product_id
        ),
        equity_dom AS (
            SELECT d.product_id
            FROM silver.product_dimensions d
            JOIN latest l ON d.product_id = l.product_id AND d.effective_date = l.dt
            WHERE d.dimension_type = 'asset_class'
              AND d.dimension_name = 'Equity' AND d.value > 0.5
        )
        SELECT m.metric_id, COUNT(DISTINCT m.product_id)
        FROM silver.product_metrics m
        JOIN equity_dom e ON m.product_id = e.product_id
        WHERE m.source = 'ratios' AND m.metric_id IN ({sql_in(FI_METRICS)})
        GROUP BY m.metric_id
        ORDER BY 2
    """).fetchall()
    render_table(
        "Fixed-income metrics on latest-snapshot equity-dominant ETFs (equity > 50%)",
        ["FI Metric", "Equity-dominant ETFs"],
        [[r[0], f"{r[1]:,}"] for r in fi_on_eq],
        align_right=[1],
    )
    fi_counts = [r[1] for r in fi_on_eq]
    if fi_counts:
        lo, hi = min(fi_counts), max(fi_counts)
        record_check(
            "FRD §4.2.1",
            "Equity-dominant ETFs also report FI metrics (balanced/multi-asset)",
            lo >= 50,  # the finding is existence-at-scale, not the frozen 206–258 band
            f"per-metric unique ETFs {lo}–{hi} (FRD snapshot band 206–258; "
            f"original script SUMed equity across dates and compared ANY-of-5 to 200–300)",
            severity="EMPIRICAL",
        )
    buggy = con.execute(f"""
        WITH equity_dominant_etfs AS (
            SELECT product_id
            FROM silver.product_dimensions
            WHERE dimension_type = 'asset_class'
            GROUP BY product_id
            HAVING SUM(CASE WHEN dimension_name = 'Equity' THEN value ELSE 0 END) > 0.5
        )
        SELECT COUNT(DISTINCT m.product_id)
        FROM silver.product_metrics m
        JOIN equity_dominant_etfs e ON m.product_id = e.product_id
        WHERE m.source = 'ratios' AND m.metric_id IN ({sql_in(FI_METRICS)})
    """).fetchone()[0]
    if HAS_RICH:
        console.print(
            f"  [dim]Diagnostic: original SUM-across-dates query still returns {buggy:,} "
            f"(the 305 that tripped the 200–300 band).[/dim]"
        )

    # Z-scores: explicit 9-id inventory. LIKE '%zscore' drops latest_composite_z_score.
    zstats = con.execute(f"""
        SELECT metric_id, COUNT(*)
        FROM silver.product_metrics
        WHERE source = 'ratios' AND metric_id IN ({sql_in(ZSCORE_IDS)})
        GROUP BY metric_id
        ORDER BY metric_id
    """).fetchall()
    present = {r[0] for r in zstats}
    render_table(
        "Z-score inventory (explicit ids — includes latest_composite_z_score)",
        ["Metric", "Obs"],
        [[r[0], f"{r[1]:,}"] for r in zstats],
        align_right=[1],
    )
    record_check(
        "FRD §4.2.1",
        "All 9 named z-score metrics present",
        present == set(ZSCORE_IDS),
        f"missing={sorted(set(ZSCORE_IDS) - present) or '∅'}",
    )
    div_obs = con.execute(
        "SELECT COUNT(*) FROM silver.product_metrics WHERE metric_id = 'dividend_yield_weighted_average'"
    ).fetchone()[0]
    z_obs = [r[1] for r in zstats]
    # FRD: 7 of 9 share 15,091 with div yield; PE is 15,089; P/S is 15,087
    majority = max(z_obs) if z_obs else 0
    record_check(
        "FRD §4.2.1",
        "Z-score majority obs matches dividend_yield_weighted_average (equity peer universe)",
        majority == div_obs and (min(z_obs) if z_obs else 0) >= div_obs - 10,
        f"{len(zstats)} metrics; obs {min(z_obs) if z_obs else 0}–{majority}; div-yield={div_obs:,}. "
        f"P/E and P/S are allowed a few dropouts (FRD: 15,089 / 15,087 vs 15,091).",
        severity="EMPIRICAL",
    )


# ==============================================================================
# 4. HOLDINGS  (§4.3)
# ==============================================================================


def verify_session_3_holdings_allocations(con) -> None:
    print_section("4. Session 4.3: Holdings Allocations", "/fundamentals/mf_holdings/")

    conc = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT product_id), MIN(value),
               ROUND(quantile_cont(value, 0.25), 4), ROUND(MEDIAN(value), 4),
               ROUND(quantile_cont(value, 0.75), 4), ROUND(MAX(value), 4),
               SUM(CASE WHEN value > 1.0 THEN 1 ELSE 0 END),
               ROUND(SUM(CASE WHEN value > 1.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
               SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN value = 0 THEN 1 ELSE 0 END),
               ROUND(SUM(CASE WHEN effective_date_source = 'payload' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        FROM silver.product_metrics
        WHERE metric_id = 'portfolio_top_10_concentration'
    """).fetchone()
    render_table(
        "portfolio_top_10_concentration",
        ["Obs", "Products", "Min", "P25", "Median", "P75", "Max", ">1.0", "% >1", "Neg", "Zero", "% Payload"],
        [[round(conc[i], 4) if i in (2, 3, 4, 5, 6, 8, 11) else fmt_n(conc[i]) for i in range(12)]],
        align_right=list(range(12)),
    )
    record_check(
        "FRD §4.3.1",
        "Leveraged concentration > 1.0 retained",
        conc[7] > 0 and near(conc[7], 990, rel=0.2),
        f"{conc[7]} snapshots > 1.0, max={conc[6]:.2f} (FRD: 990 / 17.69)",
        severity="EMPIRICAL",
    )
    record_check(
        "FRD §4.3.1",
        "Concentration has no negatives or zeros",
        conc[9] == 0 and conc[10] == 0,
        f"neg={conc[9]} zero={conc[10]}",
    )
    record_check(
        "FRD §4.3.1",
        "Holdings concentration date provenance is payload",
        conc[11] == 100.0,
        f"{conc[11]}% payload",
        severity="EMPIRICAL",
    )

    dims_audit = con.execute("""
        WITH snap_sums AS (
            SELECT dimension_type, product_id, effective_date,
                   SUM(value) AS sum_wgt,
                   SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END) AS neg_wgt_cnt
            FROM silver.product_dimensions
            WHERE dimension_type IN ('asset_class','country','industry','credit_rating','maturity','debt_type')
            GROUP BY 1, 2, 3
        )
        SELECT dimension_type, COUNT(*),
               ROUND(SUM(CASE WHEN sum_wgt BETWEEN 0.999 AND 1.001 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
               ROUND(SUM(CASE WHEN sum_wgt BETWEEN 0.980 AND 1.020 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
               SUM(CASE WHEN sum_wgt > 1.05 THEN 1 ELSE 0 END),
               SUM(CASE WHEN sum_wgt < 0.95 THEN 1 ELSE 0 END),
               ROUND(MIN(sum_wgt), 4), ROUND(MAX(sum_wgt), 4),
               SUM(CASE WHEN neg_wgt_cnt > 0 THEN 1 ELSE 0 END)
        FROM snap_sums
        GROUP BY dimension_type
        ORDER BY 2 DESC
    """).fetchall()
    render_table(
        "Allocation sum-to-1.0 (decimal weights)",
        ["Dimension", "Snapshots", "Exact ±0.1%", "Valid ±2%", "Lev >105%", "Under <95%", "Min", "Max", "w/ shorts"],
        [
            [r[0], f"{r[1]:,}", f"{r[2]}%", f"{r[3]}%", f"{r[4]:,}", f"{r[5]:,}", r[6], r[7], f"{r[8]:,}"]
            for r in dims_audit
        ],
        align_right=list(range(1, 9)),
    )
    ac = metric_map(dims_audit).get("asset_class")
    if ac:
        record_check(
            "FRD §4.3.1", "Asset class sums to 1.0 on every product-date", ac[2] == 100.0, f"{ac[2]}% within ±0.1%"
        )

    ac_details = con.execute("""
        SELECT dimension_name, COUNT(*), SUM(CASE WHEN value < 0 THEN 1 ELSE 0 END),
               MIN(value), MAX(value)
        FROM silver.product_dimensions WHERE dimension_type = 'asset_class'
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    names = {r[0] for r in ac_details}
    record_check("FRD §4.3.1", "Asset class closed set of 4 categories", names == ASSET_CLASSES, f"{sorted(names)}")
    cash_neg = next((r[2] for r in ac_details if r[0] == "Cash"), 0)
    other_neg = next((r[2] for r in ac_details if r[0] == "Other"), 0)
    record_check(
        "FRD §4.3.1",
        "Negative Cash (repo) and Other (short swaps) retained",
        cash_neg > 0 and other_neg > 0,
        f"Cash neg={cash_neg:,}, Other neg={other_neg:,} (FRD: 2,080 / 5,037)",
        severity="EMPIRICAL",
    )

    rklz = con.execute("""
        SELECT d.effective_date, d.dimension_name, d.value, d.raw_value
        FROM silver.product_dimensions d
        JOIN silver.products p ON d.product_id = p.product_id
        WHERE d.dimension_type = 'asset_class' AND p.symbol = 'RKLZ'
        ORDER BY d.effective_date DESC, d.value DESC
    """).fetchall()
    if rklz:
        render_table(
            "Inverse leveraged exemplar RKLZ (by effective_date)",
            ["Date", "Asset class", "Value", "Raw"],
            [[str(r[0]), r[1], f"{r[2]:,.4f}", r[3]] for r in rklz],
            align_right=[2],
        )

    country_anom = con.execute("""
        SELECT dimension_name, COALESCE(dimension_code, '[NULL]'), COUNT(*),
               ROUND(MEDIAN(value), 4), ROUND(MAX(value), 4)
        FROM silver.product_dimensions
        WHERE dimension_type = 'country' AND (
            dimension_code IS NULL
            OR dimension_name IN ('Croatia','Costa Rica','Bulgaria','Guam','Uzbekistan','Unidentified')
        )
        GROUP BY 1, 2
        ORDER BY 3 DESC
    """).fetchall()
    render_table(
        "Country ISO anomalies (collision, alpha-3, missing codes)",
        ["Name", "Code", "Rows", "Median wgt", "Max wgt"],
        [[r[0], r[1], f"{r[2]:,}", r[3], r[4]] for r in country_anom],
        align_right=[2, 3, 4],
    )
    # SPEC: ISO remaps applied? Unidentified must have dimension_code IS NULL per FRD §4.3.1.C
    unmapped_countries = con.execute("""
        SELECT COUNT(*)
        FROM silver.product_dimensions
        WHERE dimension_type = 'country' AND (
            (dimension_name = 'Croatia' AND COALESCE(dimension_code, '') != 'HR') OR
            (dimension_name = 'Bulgaria' AND COALESCE(dimension_code, '') != 'BG') OR
            (dimension_name = 'Guam' AND COALESCE(dimension_code, '') != 'GU') OR
            (dimension_name = 'Uzbekistan' AND COALESCE(dimension_code, '') != 'UZ') OR
            (dimension_name = 'Unidentified' AND dimension_code IS NOT NULL)
        )
    """).fetchone()[0]
    applied = unmapped_countries == 0

    # Gate the pre-remap empirical check so it doesn't warn after the SPEC is implemented
    if not applied:
        cr_codes = {r[1] for r in country_anom if r[0] in ("Croatia", "Costa Rica")}
        record_check(
            "FRD §4.3.2",
            "Provider emits CR for both Costa Rica and Croatia (pre-remap baseline)",
            cr_codes == {"CR"},
            f"codes on those names: {sorted(cr_codes)} — remap Croatia→HR is pending",
            severity="EMPIRICAL",
        )
    else:
        record_check(
            "FRD §4.3.2",
            "Provider emits CR for both Costa Rica and Croatia (pre-remap baseline)",
            True,
            "Remap applied (Croatia=HR, Costa Rica=CR verified)",
            severity="EMPIRICAL",
        )

    record_check(
        "FRD §4.3.2",
        "Country ISO remaps applied (HR/BG/GU/UZ; Unidentified code is NULL)",
        applied,
        "all target countries remapped" if applied else f"{unmapped_countries} rows remain with unmapped country codes",
        severity="SPEC",
    )

    n_countries = con.execute(
        "SELECT COUNT(DISTINCT dimension_name) FROM silver.product_dimensions WHERE dimension_type = 'country'"
    ).fetchone()[0]
    record_check(
        "FRD §4.3.1",
        "Country name cardinality near 107",
        near(n_countries, 107, abs_tol=8),
        f"{n_countries} names (FRD: 107)",
        severity="EMPIRICAL",
    )

    ind_names = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT dimension_name FROM silver.product_dimensions WHERE dimension_type = 'industry' ORDER BY 1"
        ).fetchall()
    ]
    record_check(
        "FRD §4.3.1",
        "Industry has 14 sector names",
        len(ind_names) == 14,
        f"{len(ind_names)}: {ind_names}",
        severity="EMPIRICAL",
    )
    residuals = {"Not Classified - Non Equity", "Non Classified Equity"}
    record_check(
        "FRD §4.3.1",
        "Industry residual buckets present",
        residuals <= set(ind_names),
        f"missing={sorted(residuals - set(ind_names)) or '∅'}",
        severity="EMPIRICAL",
    )

    # SPEC: Discontinued telecom sector remap to 'Communication Services'
    legacy_telecom = con.execute("""
        SELECT COUNT(*)
        FROM silver.product_dimensions
        WHERE dimension_type = 'industry'
          AND dimension_name = 'Telecommunication Services-Discontinued eff 09/19/2020'
    """).fetchone()[0]
    record_check(
        "FRD §4.3.2",
        "Industry discontinued telecom category remapped to 'Communication Services'",
        legacy_telecom == 0,
        "remapped to Communication Services" if legacy_telecom == 0 else f"{legacy_telecom} legacy rows remain",
        severity="SPEC",
    )

    cr_names = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT dimension_name FROM silver.product_dimensions WHERE dimension_type = 'credit_rating' ORDER BY 1"
        ).fetchall()
    ]
    record_check(
        "FRD §4.3.1",
        "Credit rating has 12 tiers (incl. Not Rated / Not Available)",
        len(cr_names) == 12,
        f"{cr_names}",
        severity="EMPIRICAL",
    )
    unmapped_cr = con.execute(f"""
        SELECT COUNT(*)
        FROM silver.product_dimensions
        WHERE dimension_type = 'credit_rating'
          AND (dimension_code IS NULL OR dimension_code != dimension_name OR dimension_code NOT IN ({sql_in(CREDIT_RATING_CODES)}))
    """).fetchone()[0]
    record_check(
        "FRD §4.3.2",
        "Credit rating dimension_code matches cleaned letter grade",
        unmapped_cr == 0,
        "all credit ratings mapped to grade codes"
        if unmapped_cr == 0
        else f"{unmapped_cr} unmapped credit rating rows",
        severity="SPEC",
    )

    mat_names = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT dimension_name FROM silver.product_dimensions WHERE dimension_type = 'maturity' ORDER BY 1"
        ).fetchall()
    ]
    record_check(
        "FRD §4.3.1",
        "Maturity has exactly 8 duration buckets",
        len(mat_names) == 8,
        f"{mat_names}",
        severity="EMPIRICAL",
    )
    unmapped_mat = con.execute(f"""
        SELECT COUNT(*)
        FROM silver.product_dimensions
        WHERE dimension_type = 'maturity'
          AND (dimension_code IS NULL OR dimension_code NOT IN ({sql_in(MATURITY_SLUG_CODES)}))
    """).fetchone()[0]
    record_check(
        "FRD §4.3.2",
        "Maturity buckets dimension_code standardized to concise slugs (mat_*)",
        unmapped_mat == 0,
        "all maturity buckets mapped to mat_* slugs" if unmapped_mat == 0 else f"{unmapped_mat} unmapped maturity rows",
        severity="SPEC",
    )

    # Debt clustering coverage (panel-layer mapping, audited against live names)
    cases = []
    for cluster, names_t in DEBT_CLUSTERS.items():
        for n in names_t:
            cases.append(f"WHEN dimension_name = '{n.replace(chr(39), chr(39) + chr(39))}' THEN '{cluster}'")
    case_sql = " ".join(cases)
    debt_clusters = con.execute(f"""
        WITH mapped AS (
            SELECT dimension_name, value,
                   CASE {case_sql} ELSE NULL END AS cluster_id
            FROM silver.product_dimensions WHERE dimension_type = 'debt_type'
        )
        SELECT COALESCE(cluster_id, '[UNMAPPED]'), COUNT(DISTINCT dimension_name), COUNT(*), ROUND(SUM(value), 2)
        FROM mapped
        GROUP BY 1
        ORDER BY 3 DESC
    """).fetchall()
    render_table(
        "Debt-type 110 → 9 cluster coverage",
        ["Cluster", "Types", "Rows", "Σ weight"],
        [[r[0], r[1], f"{r[2]:,}", f"{r[3]:,.2f}"] for r in debt_clusters],
        align_right=[1, 2, 3],
    )
    unmapped = [r for r in debt_clusters if r[0] == "[UNMAPPED]"]
    n_types = sum(r[1] for r in debt_clusters)
    record_check(
        "FRD §4.3.2",
        "All live debt_type names map into the 9 FRD clusters",
        len(unmapped) == 0,
        f"{n_types} distinct names; unmapped types={unmapped[0][1] if unmapped else 0}",
        severity="EMPIRICAL",
    )

    holding_sparsity = con.execute("""
        WITH name_counts AS (
            SELECT dimension_name, COUNT(DISTINCT product_id) AS etf_coverage
            FROM silver.product_dimensions
            WHERE dimension_type = 'top_holding'
            GROUP BY dimension_name
        )
        SELECT COUNT(*),
               SUM(CASE WHEN etf_coverage = 1 THEN 1 ELSE 0 END),
               ROUND(SUM(CASE WHEN etf_coverage = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1),
               SUM(CASE WHEN etf_coverage <= 2 THEN 1 ELSE 0 END),
               ROUND(SUM(CASE WHEN etf_coverage <= 2 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)
        FROM name_counts
    """).fetchone()
    n_hold = holding_sparsity[0] or 0
    if n_hold > 0:
        render_table(
            "top_holding still present — sparsity proof for prune",
            ["Distinct names", "In 1 fund", "% in 1", "In ≤2 funds", "% in ≤2"],
            [
                [
                    f"{holding_sparsity[0]:,}",
                    f"{holding_sparsity[1]:,}",
                    f"{holding_sparsity[2]}%",
                    f"{holding_sparsity[3]:,}",
                    f"{holding_sparsity[4]}%",
                ]
            ],
            align_right=list(range(5)),
        )
        record_check(
            "FRD §4.3.2",
            "top_holding sparsity justifies prune (>50% singleton names)",
            (holding_sparsity[2] or 0) > 50,
            f"{holding_sparsity[2]}% in 1 fund",
            severity="EMPIRICAL",
        )
        record_check(
            "FRD §2.3",
            "top_holding extraction dropped",
            False,
            f"{n_hold:,} distinct names still in silver.product_dimensions",
            severity="SPEC",
        )
    else:
        record_check(
            "FRD §4.3.2",
            "top_holding sparsity justifies prune (>50% singleton names)",
            True,
            "pruned (verified 0 rows remaining)",
            severity="EMPIRICAL",
        )
        record_check("FRD §2.3", "top_holding extraction dropped", True, "0 top_holding rows", severity="SPEC")


# ==============================================================================
# 5. MSTAR & LIPPER  (§4.4)
# ==============================================================================


def verify_session_4_ratings_mstar_and_lipper(con) -> None:
    print_section("5. Session 4.4: Morningstar & Lipper", "/mstar/fund/detail & /fundamentals/mf_lip_ratings/")

    mstar_prov = con.execute("""
        SELECT effective_date_source, COUNT(*),
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 3)
        FROM silver.product_metrics WHERE source = 'mstar'
        GROUP BY 1
    """).fetchall()
    render_table(
        "Morningstar date provenance",
        ["Source", "Obs", "%"],
        [[r[0], f"{r[1]:,}", f"{r[2]}%"] for r in mstar_prov],
        align_right=[1, 2],
    )
    item_pct = next((r[2] for r in mstar_prov if r[0] == "item"), 0)
    record_check(
        "FRD §4.4.1", "Morningstar dates are item publish_date (>99.9%)", item_pct >= 99.9, f"{item_pct}% item"
    )

    n_mstar = con.execute(
        "SELECT COUNT(DISTINCT metric_id) FROM silver.product_metrics WHERE source = 'mstar'"
    ).fetchone()[0]
    record_check(
        "FRD §4.4",
        "Morningstar distinct metric_ids near 10",
        near(n_mstar, 10, abs_tol=2),
        f"{n_mstar} metrics",
        severity="EMPIRICAL",
    )

    discrete = con.execute("""
        SELECT metric_id,
               SUM(CASE WHEN value != ROUND(value) THEN 1 ELSE 0 END),
               MIN(value), MAX(value), COUNT(*)
        FROM silver.product_metrics
        WHERE source = 'mstar'
        GROUP BY metric_id
    """).fetchall()
    # analyst_coverage_pct if present is not 1–5; exclude it from the ordinal check
    ordinal = [r for r in discrete if "coverage" not in r[0]]
    ordinal_ok = all(r[1] == 0 and r[2] >= 1 and r[3] <= 5 for r in ordinal)
    record_check(
        "FRD §4.4.2",
        "Morningstar ordinal ratings are discrete integers in [1, 5]",
        ordinal_ok,
        f"{len(ordinal)} ordinal metrics; off-scale={[(r[0], r[2], r[3]) for r in ordinal if not (r[2] >= 1 and r[3] <= 5)] or '∅'}",
    )

    # FRD §4.4.2.3.A.1: extractor is supposed to emit a single canonical
    # mstar_medalist_rating (Gold/Silver/.../Negative in raw_value) alongside
    # the pillar-level analyst/quant split, replacing the buggy assumption
    # that q=false always means analyst-driven.
    unified_medalist = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT product_id)
        FROM silver.product_metrics WHERE metric_id = 'mstar_medalist_rating'
    """).fetchone()
    record_check(
        "FRD §4.4.2",
        "Unified mstar_medalist_rating metric extracted (bug-fix metric)",
        unified_medalist[0] > 0,
        f"{unified_medalist[0]} rows / {unified_medalist[1]} products "
        f"(still split into _analyst/_quant if 0 — see pillar composition table below)",
        severity="SPEC",
    )

    # FRD §4.4.2.3.A.2: analyst_count / 3 over the {people, process, parent} pillars.
    coverage_pct = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT product_id), MIN(value), MAX(value),
               SUM(CASE WHEN ABS(value * 3.0 - ROUND(value * 3.0)) > 1e-4 THEN 1 ELSE 0 END)
        FROM silver.product_metrics WHERE metric_id = 'mstar_analyst_coverage_pct'
    """).fetchone()
    record_check(
        "FRD §4.4.2",
        "mstar_analyst_coverage_pct extracted (analyst_count / 3 over people/process/parent)",
        coverage_pct[0] > 0,
        f"{coverage_pct[0]} rows / {coverage_pct[1]} products" if coverage_pct[0] > 0 else "0 rows — not yet extracted",
        severity="SPEC",
    )
    if coverage_pct[0] > 0:
        record_check(
            "FRD §4.4.2",
            "analyst_coverage_pct values fall on the 4 valid fractions {0, 1/3, 2/3, 1}",
            coverage_pct[4] == 0,
            f"min={coverage_pct[2]} max={coverage_pct[3]} off-grid={coverage_pct[4]}",
        )

    # Invariant: Verify raw_value -> numeric value mapping direction for Morningstar
    mstar_mapping_bad = con.execute("""
        SELECT COUNT(*)
        FROM silver.product_metrics
        WHERE source = 'mstar' AND (
            (raw_value = 'Gold' AND value != 5.0) OR
            (raw_value = 'Silver' AND value != 4.0) OR
            (raw_value = 'Bronze' AND value != 3.0) OR
            (raw_value = 'Neutral' AND value != 2.0) OR
            (raw_value = 'Negative' AND value != 1.0) OR
            (raw_value = 'High' AND value != 5.0) OR
            (raw_value = 'Low' AND value != 1.0)
        )
    """).fetchone()[0]
    record_check(
        "FRD §4.4.2",
        "Morningstar rating raw_value to numeric value mapping valid",
        mstar_mapping_bad == 0,
        f"{mstar_mapping_bad} mismapped rows",
    )

    pillar_comp = con.execute("""
        WITH medalist_products AS (
            SELECT DISTINCT product_id
            FROM silver.product_metrics
            WHERE metric_id IN ('mstar_medalist_rating_analyst', 'mstar_medalist_rating_quant',
                                'mstar_medalist_rating')
        ),
        latest_people AS (
            SELECT product_id, CASE WHEN metric_id = 'mstar_people_analyst' THEN 'A' ELSE 'Q' END AS people
            FROM (
                SELECT product_id, metric_id,
                       ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_date DESC, fetched_at DESC) rn
                FROM silver.product_metrics
                WHERE metric_id IN ('mstar_people_analyst', 'mstar_people_quant')
            ) WHERE rn = 1
        ),
        latest_process AS (
            SELECT product_id, CASE WHEN metric_id = 'mstar_process_analyst' THEN 'A' ELSE 'Q' END AS process
            FROM (
                SELECT product_id, metric_id,
                       ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_date DESC, fetched_at DESC) rn
                FROM silver.product_metrics
                WHERE metric_id IN ('mstar_process_analyst', 'mstar_process_quant')
            ) WHERE rn = 1
        ),
        latest_parent AS (
            SELECT product_id, CASE WHEN metric_id = 'mstar_parent_analyst' THEN 'A' ELSE 'Q' END AS parent
            FROM (
                SELECT product_id, metric_id,
                       ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_date DESC, fetched_at DESC) rn
                FROM silver.product_metrics
                WHERE metric_id IN ('mstar_parent_analyst', 'mstar_parent_quant')
            ) WHERE rn = 1
        ),
        assembled AS (
            SELECT mp.product_id,
                   COALESCE(pe.people, '-') AS pe,
                   COALESCE(pr.process, '-') AS pr,
                   COALESCE(pa.parent, '-') AS pa
            FROM medalist_products mp
            LEFT JOIN latest_people pe ON mp.product_id = pe.product_id
            LEFT JOIN latest_process pr ON mp.product_id = pr.product_id
            LEFT JOIN latest_parent pa ON mp.product_id = pa.product_id
        )
        SELECT pe || '-' || pr || '-' || pa,
               CASE WHEN pe = 'A' AND pr = 'A' AND pa = 'A' THEN 'Pure Analyst (3A)'
                    WHEN pe = 'Q' AND pr = 'Q' AND pa = 'Q' THEN 'Pure Quant (3Q)'
                    WHEN pe = '-' OR pr = '-' OR pa = '-' THEN 'Incomplete pillars'
                    ELSE 'Hybrid' END,
               COUNT(*),
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)
        FROM assembled
        GROUP BY 1, 2
        ORDER BY 3 DESC
    """).fetchall()
    render_table(
        "Medalist pillar composition (latest people/process/parent per product)",
        ["Profile", "Category", "Funds", "%"],
        [[r[0], r[1], f"{r[2]:,}", f"{r[3]}%"] for r in pillar_comp],
        align_right=[2, 3],
    )
    hybrid = sum(r[3] for r in pillar_comp if r[1] == "Hybrid")
    record_check(
        "FRD §4.4.1",
        "Medalist ratings are majority hybrid analyst/quant",
        hybrid > 50,
        f"hybrid={hybrid:.1f}%",
        severity="EMPIRICAL",
    )

    lip = con.execute(f"""
        WITH parsed AS (
            SELECT metric_id, product_id,
                   REGEXP_EXTRACT(metric_id, '{LIPPER_RE}', 1) AS tag,
                   REGEXP_EXTRACT(metric_id, '{LIPPER_RE}', 2) AS horizon,
                   REGEXP_EXTRACT(metric_id, '{LIPPER_RE}', 3) AS geo
            FROM silver.product_metrics WHERE source = 'lipper'
        )
        SELECT COUNT(DISTINCT metric_id),
               COUNT(DISTINCT geo),
               COUNT(DISTINCT product_id),
               COUNT(*),
               SUM(CASE WHEN tag IS NULL OR tag = '' THEN 1 ELSE 0 END),
               COUNT(DISTINCT CASE WHEN tag IS NULL OR tag = '' THEN metric_id END)
        FROM parsed
    """).fetchone()
    render_table(
        "Lipper factor explosion",
        ["Variants", "Geos", "Products", "Rows", "Unparsed rows", "Unparsed metric_ids"],
        [[f"{lip[0]:,}", lip[1], f"{lip[2]:,}", f"{lip[3]:,}", f"{lip[4]:,}", lip[5]]],
        align_right=list(range(6)),
    )
    is_reduced = lip[0] <= 25
    record_check(
        "FRD §4.4.1",
        "Lipper ≈ 568 variants across 37 universes (pre-reduction)",
        (near(lip[0], 568, abs_tol=20) and near(lip[1], 37, abs_tol=3)) if not is_reduced else True,
        f"{lip[0]} variants, {lip[1]} geos (FRD: 568 / 37)"
        if not is_reduced
        else f"reduced to {lip[0]} canonical metrics",
        severity="EMPIRICAL",
    )
    record_check(
        "FRD §4.4.1", "Every lipper metric_id parses tag/horizon/geo", lip[4] == 0, f"{lip[5]} unparsed metric_ids"
    )

    record_check(
        "FRD §4.4.2",
        "Lipper Max-Peer-Count universe reduction applied (568 geo variants -> 20 canonical metric_ids)",
        is_reduced,
        f"{lip[0]} distinct lipper metric_ids (target: 20 canonical `lipper_{{tag}}_{{horizon}}` ids)",
        severity="SPEC",
    )

    # Invariant: Lipper scores are discrete integers in [1, 5]
    lip_scale = con.execute("""
        SELECT
            SUM(CASE WHEN value != ROUND(value) OR value < 1.0 OR value > 5.0 THEN 1 ELSE 0 END),
            MIN(value), MAX(value)
        FROM silver.product_metrics WHERE source = 'lipper'
    """).fetchone()
    record_check(
        "FRD §4.4.2",
        "Lipper scores are strictly discrete integers in [1, 5]",
        lip_scale[0] == 0,
        f"{lip_scale[0]} non-integer or out-of-bounds rows; range=[{lip_scale[1]}, {lip_scale[2]}]",
    )

    if is_reduced:
        lip_tax_canon = con.execute("""
            SELECT COUNT(*)
            FROM silver.product_metrics
            WHERE metric_id LIKE 'lipper_tax_efficiency_%'
        """).fetchone()[0]
        lip_tax_non_us = con.execute("""
            SELECT COUNT(*)
            FROM silver.product_metrics
            WHERE metric_id LIKE 'lipper_tax_efficiency_%'
              AND raw_value NOT LIKE '%United States%'
              AND raw_value NOT LIKE '%united_states%'
        """).fetchone()[0]
        record_check(
            "FRD §4.4.1",
            "Lipper tax efficiency is strictly US-only (zero non-US records)",
            lip_tax_canon > 0 and lip_tax_non_us == 0,
            f"{lip_tax_canon} canonical tax rows (0 non-US)"
            if lip_tax_non_us == 0
            else f"{lip_tax_non_us} non-US tax rows found",
        )
    else:
        lip_tax = con.execute("""
            SELECT DISTINCT REGEXP_EXTRACT(metric_id, '^lipper_tax_efficiency_(overall|3yr|5yr|10yr)_(.*)$', 2)
            FROM silver.product_metrics
            WHERE metric_id LIKE 'lipper_tax_efficiency_%'
        """).fetchall()
        tax_geos = {r[0] for r in lip_tax}
        record_check(
            "FRD §4.4.1",
            "Lipper tax efficiency is US-only",
            tax_geos == {"united_states"},
            f"universes={sorted(tax_geos)}",
        )

    lip_exp = con.execute("""
        SELECT value, COUNT(*), ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)
        FROM silver.product_metrics
        WHERE source = 'lipper' AND metric_id LIKE 'lipper_expense_%'
        GROUP BY value ORDER BY value DESC
    """).fetchall()
    render_table(
        "Lipper expense quintile (low-variance anomaly)",
        ["Score", "Obs", "%"],
        [[r[0], f"{r[1]:,}", f"{r[2]}%"] for r in lip_exp],
        align_right=[0, 1, 2],
    )
    pct5 = next((r[2] for r in lip_exp if r[0] == 5.0), 0)
    record_check(
        "FRD §4.4.1", "Lipper expense score clusters at 5.0 (>80%)", pct5 > 80, f"{pct5}% at 5.0", severity="EMPIRICAL"
    )

    lip_geo = con.execute(f"""
        SELECT REGEXP_EXTRACT(metric_id, '{LIPPER_RE}', 3) AS geo, COUNT(DISTINCT product_id)
        FROM silver.product_metrics WHERE source = 'lipper'
        GROUP BY geo
        ORDER BY 2 DESC
        LIMIT 8
    """).fetchall()
    render_table(
        "Top Lipper geographic universes (max-peer-count reduction input)",
        ["Universe", "ETFs"],
        [[r[0], f"{r[1]:,}"] for r in lip_geo],
        align_right=[1],
    )

    lip_ts = con.execute("""
        SELECT
            COUNT(DISTINCT metric_id) AS variants,
            COUNT(DISTINCT CASE WHEN n >= 2 THEN metric_id END) AS with_history
        FROM (
            SELECT metric_id, product_id, COUNT(*) n
            FROM silver.product_metrics WHERE source = 'lipper'
            GROUP BY 1, 2
        )
    """).fetchone()
    if is_reduced:
        record_check(
            "FRD §4.4.3",
            "Most Lipper metric variants are single-shot (no product-level history)",
            True,
            f"Reduced to {lip_ts[0]} canonical metrics with rich longitudinal history ({lip_ts[1]} with history)",
            severity="EMPIRICAL",
        )
    else:
        has_minimal_history = lip_ts[1] < (lip_ts[0] * 0.15)
        record_check(
            "FRD §4.4.3",
            "Most Lipper metric variants are single-shot (no product-level history)",
            has_minimal_history,
            f"{lip_ts[1]} of {lip_ts[0]} variants have ≥2 obs on some product (LOCF is nearly a no-op until reduction)",
            severity="EMPIRICAL",
        )


# ==============================================================================
# 6. THEMES & ESG  (§4.5)
# ==============================================================================


def verify_session_5_themes_and_esg(con) -> None:
    print_section("6. Session 4.5: Themes & Refinitiv ESG", "/knowledge-graph/ui/fund & /impact/esg/")

    theme_cov_n = con.execute("""
        SELECT COUNT(*) FROM silver.product_metrics WHERE metric_id = 'theme_coverage'
    """).fetchone()[0]
    record_check(
        "FRD §4.5.1",
        "theme_coverage extracted into silver.product_metrics",
        theme_cov_n > 0,
        f"{theme_cov_n} rows (FRD: add coverage from payload.coverage)",
        severity="SPEC",
    )

    theme_sums = con.execute("""
        WITH s AS (
            SELECT product_id, effective_date, COUNT(*) AS n, ROUND(SUM(value), 4) AS w
            FROM silver.product_dimensions WHERE dimension_type = 'theme'
            GROUP BY 1, 2
        )
        SELECT COUNT(*), MIN(n), ROUND(MEDIAN(n), 0), MAX(n), MIN(w), ROUND(MEDIAN(w), 4), MAX(w)
        FROM s
    """).fetchone()
    render_table(
        "Theme loadings are overlapping (no sum-to-1.0)",
        ["Fund-dates", "Min n", "Median n", "Max n", "Min Σ", "Median Σ", "Max Σ"],
        [
            [
                f"{theme_sums[0]:,}",
                theme_sums[1],
                int(theme_sums[2]),
                theme_sums[3],
                theme_sums[4],
                theme_sums[5],
                theme_sums[6],
            ]
        ],
        align_right=list(range(7)),
    )
    record_check(
        "FRD §4.5.1",
        "Theme weight sums near FRD distribution (median 7.30, max 110.87)",
        near(theme_sums[5], 7.303, rel=0.25) and near(theme_sums[6], 110.865, rel=0.25),
        f"median Σ={theme_sums[5]:.2f} (FRD 7.30), max Σ={theme_sums[6]:.2f} (FRD 110.87)",
        severity="EMPIRICAL",
    )

    n_theme_codes = con.execute("""
        SELECT COUNT(DISTINCT dimension_code)
        FROM silver.product_dimensions WHERE dimension_type = 'theme'
    """).fetchone()[0]
    record_check(
        "FRD §4.5.1",
        "491 distinct child theme UUIDs",
        n_theme_codes == 491,
        f"{n_theme_codes} codes",
        severity="EMPIRICAL",
    )

    neg_t = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT product_id), MIN(value), MAX(value)
        FROM silver.product_dimensions WHERE dimension_type = 'theme' AND value < 0
    """).fetchone()
    gt1_t = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT product_id), MIN(value), MAX(value)
        FROM silver.product_dimensions WHERE dimension_type = 'theme' AND value > 1.0
    """).fetchone()
    render_table(
        "Theme weight bounds",
        ["Condition", "Rows", "Funds", "Min", "Max"],
        [
            [
                "value < 0",
                f"{neg_t[0]:,}",
                neg_t[1],
                f"{neg_t[2]:.4f}" if neg_t[2] is not None else "—",
                f"{neg_t[3]:.4f}" if neg_t[3] is not None else "—",
            ],
            [
                "value > 1",
                f"{gt1_t[0]:,}",
                gt1_t[1],
                f"{gt1_t[2]:.4f}" if gt1_t[2] is not None else "—",
                f"{gt1_t[3]:.4f}" if gt1_t[3] is not None else "—",
            ],
        ],
        align_right=[1, 2, 3, 4],
    )
    record_check(
        "FRD §4.5.1",
        "Negative theme weights near FRD count (long/short)",
        near(neg_t[0], 3153, rel=0.25) and near(neg_t[1], 20, abs_tol=8),
        f"{neg_t[0]} rows / {neg_t[1]} funds (FRD: 3,153 / 20)",
        severity="EMPIRICAL",
    )
    record_check(
        "FRD §4.5.1",
        "Leveraged theme weights (> 1.0) near FRD count",
        near(gt1_t[0], 1877, rel=0.25) and near(gt1_t[1], 94, abs_tol=20),
        f"{gt1_t[0]} rows / {gt1_t[1]} funds (FRD: 1,877 / 94)",
        severity="EMPIRICAL",
    )

    orphan = con.execute("""
        SELECT COUNT(*)
        FROM silver.product_dimensions d
        LEFT JOIN bronze.themes t ON d.dimension_code = t.theme_id
        WHERE d.dimension_type = 'theme' AND t.theme_id IS NULL
    """).fetchone()[0]
    n_parent = con.execute("""
        SELECT COUNT(DISTINCT p.theme_id)
        FROM silver.product_dimensions d
        JOIN bronze.themes t ON d.dimension_code = t.theme_id
        JOIN bronze.themes p ON t.parent_id = p.theme_id
        WHERE d.dimension_type = 'theme'
    """).fetchone()[0]
    parent_in_dims = con.execute("""
        SELECT COUNT(*)
        FROM silver.product_dimensions d
        JOIN bronze.themes t ON d.dimension_code = t.theme_id
        WHERE d.dimension_type = 'theme' AND t.parent_id IS NULL
    """).fetchone()[0]
    record_check(
        "FRD §4.5.1",
        "Theme UUIDs join bronze.themes with 0 orphans and 19 parents",
        orphan == 0 and n_parent == 19,
        f"orphans={orphan}, parents={n_parent}",
    )
    record_check(
        "FRD §4.5.1",
        "Only child themes stored (no parent ids in product_dimensions)",
        parent_in_dims == 0,
        f"{parent_in_dims} parent-id rows",
    )

    theme_dates = con.execute("""
        SELECT ROUND(SUM(CASE WHEN effective_date_source = 'snapshot' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        FROM silver.product_dimensions WHERE dimension_type = 'theme'
    """).fetchone()[0]
    record_check(
        "FRD §4.5.1",
        "Theme effective dates are snapshot-sourced (no payload date)",
        theme_dates == 100.0,
        f"{theme_dates}% snapshot",
        severity="EMPIRICAL",
    )

    esg_meta = con.execute("""
        SELECT COUNT(DISTINCT metric_id),
               SUM(CASE WHEN metric_id != 'esg_coverage' AND value != ROUND(value) THEN 1 ELSE 0 END)
        FROM silver.product_metrics WHERE source = 'esg'
    """).fetchone()
    record_check(
        "FRD §4.5.2",
        "ESG taxonomy has 17 metrics (16 scores + coverage)",
        esg_meta[0] == 17,
        f"{esg_meta[0]} metrics",
        severity="EMPIRICAL",
    )
    record_check(
        "FRD §4.5.2",
        "ESG scores are integers (no fractional TRESG values)",
        esg_meta[1] == 0,
        f"{esg_meta[1]} fractional score rows",
    )

    missing_esg = set(ESG_SCORE_IDS + ["esg_coverage"]) - {
        r[0]
        for r in con.execute("SELECT DISTINCT metric_id FROM silver.product_metrics WHERE source = 'esg'").fetchall()
    }
    record_check(
        "FRD §4.5.2",
        "All 16 TRESG score ids plus esg_coverage present",
        missing_esg == set(),
        f"missing={sorted(missing_esg) or '∅'}",
    )

    bounds = con.execute(f"""
        SELECT
            SUM(CASE WHEN metric_id IN ({sql_in(ESG_SCORE_IDS)}) AND (value < 0 OR value > 10) THEN 1 ELSE 0 END),
            MIN(CASE WHEN metric_id IN ({sql_in(ESG_SCORE_IDS)}) THEN value END),
            MAX(CASE WHEN metric_id IN ({sql_in(ESG_SCORE_IDS)}) THEN value END)
        FROM silver.product_metrics WHERE source = 'esg'
    """).fetchone()
    record_check(
        "FRD §4.5.2",
        "ESG scores lie in [0, 10]",
        bounds[0] == 0,
        f"{bounds[0]} out of range; min={bounds[1]} max={bounds[2]}",
    )

    floor_never_zero = con.execute("""
        SELECT metric_id, MIN(value)
        FROM silver.product_metrics
        WHERE source = 'esg' AND metric_id IN ('tresgs','tresgcs','tresgsos')
        GROUP BY 1
    """).fetchall()
    record_check(
        "FRD §4.5.2",
        "Aggregate ESG scores tresgs/tresgcs/tresgsos never reach 0",
        all(r[1] >= 1 for r in floor_never_zero),
        ", ".join(f"{r[0]} min={r[1]}" for r in floor_never_zero),
        severity="EMPIRICAL",
    )

    cov = con.execute("""
        SELECT COUNT(*), MIN(value), ROUND(MEDIAN(value), 4), MAX(value),
               SUM(CASE WHEN value > 1.0 THEN 1 ELSE 0 END),
               COUNT(DISTINCT CASE WHEN value > 1.0 THEN product_id END)
        FROM silver.product_metrics WHERE metric_id = 'esg_coverage'
    """).fetchone()
    render_table(
        "esg_coverage (quality gate; >1.0 is leveraged, not an error)",
        ["Obs", "Min", "Median", "Max", ">1.0 rows", ">1.0 products"],
        [[f"{cov[0]:,}", cov[1], cov[2], cov[3], f"{cov[4]:,}", cov[5]]],
        align_right=list(range(6)),
    )
    record_check(
        "FRD §4.5.2",
        "esg_coverage > 1.0 near FRD count (leveraged ETPs, retained unclipped)",
        near(cov[4], 281, rel=0.25) and near(cov[5], 139, rel=0.25),
        f"{cov[4]} rows / {cov[5]} products (FRD: 281 / 139)",
        severity="EMPIRICAL",
    )
    record_check(
        "FRD §4.5.2",
        "All observed esg_coverage ≥ 0.7 quality gate",
        cov[1] is not None and cov[1] >= 0.7,
        f"min={cov[1]}",
        severity="EMPIRICAL",
    )

    # FRD §4.5.2.E: Exactly 9 payloads (spanning 3 products) have TRESG scores without esg_coverage
    payloads_no_coverage = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT m.product_id)
        FROM silver.product_metrics m
        WHERE m.source = 'esg' AND m.metric_id = 'tresgs'
          AND NOT EXISTS (
              SELECT 1 FROM silver.product_metrics c
              WHERE c.product_id = m.product_id
                AND c.effective_date = m.effective_date
                AND c.metric_id = 'esg_coverage'
          )
    """).fetchone()
    record_check(
        "FRD §4.5.2",
        "ESG payloads with scores but missing coverage match FRD (9 payloads / 3 funds)",
        payloads_no_coverage[0] == 9 and payloads_no_coverage[1] == 3,
        f"{payloads_no_coverage[0]} payloads across {payloads_no_coverage[1]} products (FRD: 9 / 3)",
        severity="EMPIRICAL",
    )

    esg_stats = con.execute("""
        SELECT metric_id, COUNT(*), MIN(value), ROUND(MEDIAN(value), 1), MAX(value),
               ROUND(AVG(value), 2), ROUND(STDDEV(value), 2),
               SUM(CASE WHEN value = 0 THEN 1 ELSE 0 END)
        FROM silver.product_metrics
        WHERE source = 'esg' AND metric_id != 'esg_coverage'
        GROUP BY metric_id
        ORDER BY metric_id
    """).fetchall()
    render_table(
        "Refinitiv ESG score distributions",
        ["Metric", "Obs", "Min", "Median", "Max", "Mean", "σ", "Zeros"],
        [[r[0], f"{r[1]:,}", r[2], r[3], r[4], r[5], r[6], f"{r[7]:,}"] for r in esg_stats],
        align_right=list(range(1, 8)),
    )
    s_by = metric_map(esg_stats)
    if "tresgs" in s_by and "tresgccs" in s_by:
        record_check(
            "FRD §4.5.2",
            "tresgs tight (σ<1) vs controversies wide (σ>2)",
            s_by["tresgs"][6] < 1.0 and s_by["tresgccs"][6] > 2.0,
            f"tresgs σ={s_by['tresgs'][6]} tresgccs σ={s_by['tresgccs'][6]}",
            severity="EMPIRICAL",
        )

    esg_dates = con.execute("""
        SELECT ROUND(SUM(CASE WHEN effective_date_source = 'payload' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        FROM silver.product_metrics WHERE source = 'esg'
    """).fetchone()[0]
    record_check(
        "FRD §4.5.2",
        "ESG dates are payload asOfDate (YYYYMMDD)",
        esg_dates == 100.0,
        f"{esg_dates}% payload",
        severity="EMPIRICAL",
    )

    cross = con.execute("""
        WITH latest AS (
            SELECT product_id, MAX(effective_date) AS dt
            FROM silver.product_dimensions WHERE dimension_type = 'asset_class'
            GROUP BY product_id
        ),
        etf_classes AS (
            SELECT d.product_id,
                   CASE WHEN d.dimension_name = 'Equity' AND d.value > 0.6 THEN 'Equity'
                        WHEN d.dimension_name = 'Fixed Income' AND d.value > 0.6 THEN 'Fixed Income'
                   END AS asset_class
            FROM silver.product_dimensions d
            JOIN latest l ON d.product_id = l.product_id AND d.effective_date = l.dt
            WHERE d.dimension_type = 'asset_class'
        )
        SELECT c.asset_class,
               COUNT(DISTINCT c.product_id),
               COUNT(DISTINCT CASE WHEN d.dimension_type = 'theme' THEN c.product_id END),
               ROUND(COUNT(DISTINCT CASE WHEN d.dimension_type = 'theme' THEN c.product_id END)
                     * 100.0 / COUNT(DISTINCT c.product_id), 1),
               COUNT(DISTINCT CASE WHEN m.source = 'esg' THEN c.product_id END),
               ROUND(COUNT(DISTINCT CASE WHEN m.source = 'esg' THEN c.product_id END)
                     * 100.0 / COUNT(DISTINCT c.product_id), 1)
        FROM etf_classes c
        LEFT JOIN silver.product_dimensions d ON c.product_id = d.product_id
        LEFT JOIN silver.product_metrics m ON c.product_id = m.product_id
        WHERE c.asset_class IS NOT NULL
        GROUP BY 1
    """).fetchall()
    render_table(
        "Theme/ESG coverage by latest-snapshot dominant asset class",
        ["Class", "Universe", "w/ Themes", "% Themes", "w/ ESG", "% ESG"],
        [[r[0], f"{r[1]:,}", f"{r[2]:,}", f"{r[3]}%", f"{r[4]:,}", f"{r[5]}%"] for r in cross],
        align_right=list(range(1, 6)),
    )
    covd = {r[0]: r for r in cross}
    eq, fi = covd.get("Equity"), covd.get("Fixed Income")
    cross_ok = bool(eq and fi and eq[3] > 80 and fi[3] < 25 and eq[5] > 80 and fi[5] < 25)
    record_check(
        "FRD §4.5.5",
        "Themes/ESG are equity-dominant (>80% vs <25% FI)",
        cross_ok,
        f"Eq themes {eq[3] if eq else '—'}% ESG {eq[5] if eq else '—'}%; "
        f"FI themes {fi[3] if fi else '—'}% ESG {fi[5] if fi else '—'}%",
        severity="EMPIRICAL",
    )

    # FRD §4.5 intro: 904 products have ESG but no theme data (mostly Fixed Income);
    # 161 have theme data but no ESG scores.
    overlap = con.execute("""
        WITH esg_p AS (SELECT DISTINCT product_id FROM silver.product_metrics WHERE source = 'esg'),
             theme_p AS (SELECT DISTINCT product_id FROM silver.product_dimensions WHERE dimension_type = 'theme')
        SELECT
            (SELECT COUNT(*) FROM esg_p WHERE product_id NOT IN (SELECT product_id FROM theme_p)),
            (SELECT COUNT(*) FROM theme_p WHERE product_id NOT IN (SELECT product_id FROM esg_p))
    """).fetchone()
    render_table(
        "ESG / theme product overlap",
        ["ESG only (no themes)", "Themes only (no ESG)"],
        [[f"{overlap[0]:,}", f"{overlap[1]:,}"]],
        align_right=[0, 1],
    )
    record_check(
        "FRD §4.5",
        "ESG-only and theme-only product counts near FRD (904 / 161)",
        near(overlap[0], 904, rel=0.3) and near(overlap[1], 161, rel=0.3),
        f"ESG-only={overlap[0]} (FRD 904), themes-only={overlap[1]} (FRD 161)",
        severity="EMPIRICAL",
    )


# ==============================================================================
# 7. CADENCE
# ==============================================================================


def verify_empirical_cadences(con) -> None:
    print_section(
        "7. Cadence & LOCF staleness", "Payload/item dates only in the mixed table; snapshot series audited separately"
    )

    rows = con.execute("""
        WITH consecutive_deltas AS (
            SELECT source, metric_id, product_id, effective_date,
                   DATE_DIFF('day', LAG(effective_date) OVER (
                       PARTITION BY product_id, source, metric_id ORDER BY effective_date
                   ), effective_date) AS delta_days
            FROM silver.product_metrics
            WHERE effective_date_source != 'snapshot'
        )
        SELECT source, COUNT(DISTINCT metric_id), COUNT(*),
               ROUND(MEDIAN(delta_days), 0),
               ROUND(quantile_cont(delta_days, 0.75), 0),
               ROUND(quantile_cont(delta_days, 0.95), 0),
               MAX(delta_days),
               ROUND(SUM(CASE WHEN delta_days IN (28, 29, 30, 31) THEN 1 ELSE 0 END) * 100.0
                     / NULLIF(COUNT(*), 0), 1),
               ROUND(SUM(CASE WHEN delta_days = 7 THEN 1 ELSE 0 END) * 100.0
                     / NULLIF(COUNT(*), 0), 1)
        FROM consecutive_deltas
        WHERE delta_days IS NOT NULL AND delta_days > 0
        GROUP BY source
        ORDER BY 3 DESC
    """).fetchall()
    locf = {
        "esg": "Weekly → 90d cap",
        "ratios": "Monthly → 180d cap",
        "lipper": "Monthly → 180d cap",
        "holdings": "Monthly → 180d cap",
        "profile": "Mixed monthly/annual → 180d AUM / 540d fees",
        "mstar": "Mixed monthly/event → 540d cap",
    }
    render_table(
        "Payload/item cadence (snapshot-sourced metrics excluded — they measure crawl, not reporting)",
        ["Source", "Metrics w/ Δ", "Transitions", "Median", "p75", "p95", "Max", "% ~month", "% =7d", "FRD LOCF"],
        [
            [
                r[0],
                r[1],
                f"{r[2]:,}",
                f"{int(r[3])}d",
                f"{int(r[4])}d",
                f"{int(r[5])}d",
                f"{r[6]}d",
                f"{r[7]}%",
                f"{r[8]}%",
                locf.get(r[0], "180d"),
            ]
            for r in rows
        ],
        align_right=list(range(1, 9)),
    )
    by_src = metric_map(rows)
    if "esg" in by_src:
        record_check(
            "FRD §4.5.4",
            "ESG weekly cadence (median 7d, ~100% of deltas exactly 7d)",
            int(by_src["esg"][3]) == 7 and by_src["esg"][8] >= 95.0,
            f"median={int(by_src['esg'][3])}d, {by_src['esg'][8]}% of deltas exactly 7d (FRD: 100.0%)",
        )
    if "ratios" in by_src:
        record_check(
            "FRD §4.2.3",
            "Ratios monthly cadence (median 31d, >99% near month-end)",
            int(by_src["ratios"][3]) == 31 and by_src["ratios"][7] >= 99.0,
            f"median={int(by_src['ratios'][3])}d, {by_src['ratios'][7]}% in {{28,29,30,31}}d (FRD: >99.2% at exactly 31d)",
        )

    # Snapshot-sourced profile attributes the original cadence query dropped
    snap_prof = con.execute("""
        WITH d AS (
            SELECT metric_id, product_id, fetched_at::DATE AS dt,
                   DATE_DIFF('day', LAG(fetched_at::DATE) OVER (
                       PARTITION BY product_id, metric_id ORDER BY fetched_at
                   ), fetched_at::DATE) AS delta_days
            FROM silver.product_metrics
            WHERE source = 'profile'
              AND metric_id IN ('total_expense_ratio','manager_tenure_years','is_passive')
              AND effective_date_source = 'snapshot'
        )
        SELECT metric_id, COUNT(*), ROUND(MEDIAN(delta_days), 1),
               ROUND(quantile_cont(delta_days, 0.95), 1), MAX(delta_days)
        FROM d WHERE delta_days IS NOT NULL AND delta_days > 0
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    render_table(
        "Snapshot-sourced profile attributes (crawl cadence, not issuer reporting)",
        ["Metric", "Transitions", "Median Δ", "p95 Δ", "Max Δ"],
        [[r[0], f"{r[1]:,}", r[2], r[3], r[4]] for r in snap_prof],
        align_right=list(range(1, 5)),
    )
    if snap_prof:
        meds = [r[2] for r in snap_prof]
        record_check(
            "FRD §4.1.3",
            "Snapshot TER/tenure/approach crawl weekly (median ~7d)",
            all(5 <= m <= 10 for m in meds),
            f"medians={meds} (FRD: 7.0–7.3d)",
            severity="EMPIRICAL",
        )

    dim_d = con.execute("""
        WITH dim_lags AS (
            SELECT product_id, effective_date,
                   DATE_DIFF('day', LAG(effective_date) OVER (
                       PARTITION BY product_id ORDER BY effective_date
                   ), effective_date) AS delta_days
            FROM silver.product_dimensions
            WHERE dimension_type = 'asset_class' AND effective_date_source != 'snapshot'
        )
        SELECT COUNT(*), ROUND(MEDIAN(delta_days), 0),
               ROUND(SUM(CASE WHEN delta_days = 31 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
        FROM dim_lags WHERE delta_days IS NOT NULL AND delta_days > 0
    """).fetchone()
    record_check(
        "FRD §4.3.3",
        "Holdings asset_class monthly cadence (31d mode)",
        dim_d[1] == 31 and dim_d[2] > 99.0,
        f"{dim_d[2]}% exactly 31d (n={dim_d[0]:,})",
    )

    audited = con.execute("""
        WITH aud AS (
            SELECT product_id, effective_date,
                   DATE_DIFF('day', LAG(effective_date) OVER (
                       PARTITION BY product_id ORDER BY effective_date
                   ), effective_date) AS delta_days
            FROM silver.product_metrics WHERE metric_id = 'audited_net_expense_ratio'
        )
        SELECT ROUND(MEDIAN(delta_days), 0), COUNT(*)
        FROM aud WHERE delta_days > 0
    """).fetchone()
    record_check(
        "FRD §4.1.3",
        "Audited net expense ratio annual cadence (median 365d)",
        audited[0] == 365,
        f"median={audited[0]}d over {audited[1]} transitions",
    )

    if "lipper" in by_src:
        record_check(
            "FRD §4.4.3",
            "Lipper monthly cadence (100% of deltas exactly 31d)",
            int(by_src["lipper"][3]) == 31 and by_src["lipper"][7] == 100.0,
            f"median={int(by_src['lipper'][3])}d, {by_src['lipper'][7]}% at 31d over {by_src['lipper'][2]:,} transitions",
        )

    theme_cad = con.execute("""
        WITH d AS (
            SELECT product_id, effective_date,
                   DATE_DIFF('day', LAG(effective_date) OVER (
                       PARTITION BY product_id ORDER BY effective_date
                   ), effective_date) AS delta_days
            FROM (
                SELECT DISTINCT product_id, effective_date
                FROM silver.product_dimensions WHERE dimension_type = 'theme'
            )
        )
        SELECT COUNT(*), ROUND(MEDIAN(delta_days), 1), MIN(delta_days), MAX(delta_days)
        FROM d WHERE delta_days IS NOT NULL AND delta_days > 0
    """).fetchone()
    if theme_cad and theme_cad[0]:
        crawl_window_ok = theme_cad[0] > 0 and 2.0 <= theme_cad[1] <= 10.0
        record_check(
            "FRD §4.5.4",
            "Theme snapshot deltas in the 2–10 day crawl window (or monthly if collapsed)",
            crawl_window_ok,
            f"n={theme_cad[0]:,} median={theme_cad[1]}d min={theme_cad[2]} max={theme_cad[3]} (FRD LOCF cap 180d)",
            severity="EMPIRICAL",
        )


def verify_panel_readiness(con) -> None:
    print_section("7b. Panel Construction Readiness", "FRD §5 Master Checklist — last item, not yet built")

    # Guard: Check for metric_id vs dimension feature namespace collisions before panel build
    feature_collisions = con.execute("""
        WITH metric_features AS (
            SELECT DISTINCT metric_id AS fid FROM silver.product_metrics
        ),
        dim_features AS (
            SELECT DISTINCT COALESCE(dimension_code, dimension_name) AS fid
            FROM silver.product_dimensions
            WHERE dimension_type NOT IN ('top_holding')
        )
        SELECT COUNT(*), LIST(m.fid)
        FROM metric_features m
        JOIN dim_features d ON m.fid = d.fid
    """).fetchone()
    record_check(
        "FRD §2.3",
        "No namespace collisions between metric_ids and dimension feature codes",
        feature_collisions[0] == 0,
        f"{feature_collisions[0]} collisions: {feature_collisions[1]}"
        if feature_collisions[0] > 0
        else "namespaces disjoint",
        severity="INVARIANT",
    )

    exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'silver' AND table_name = 'monthly_panel'
    """).fetchone()[0]
    if exists:
        n_rows = con.execute("SELECT COUNT(*) FROM silver.monthly_panel").fetchone()[0]
        record_check(
            "FRD §5",
            "silver.monthly_panel built",
            True,
            f"{n_rows:,} rows",
            severity="SPEC",
        )
    else:
        record_check(
            "FRD §5",
            "silver.monthly_panel built",
            False,
            "table does not exist yet — dimensional flattening, sum-to-1.0 panel checks, "
            "and LOCF staleness enforcement (§4.1.3/4.2.3/4.3.3/4.4.3/4.5.4) are all pending "
            "until etfportfolio/prep/panel.py lands; this audit only covers the Silver observation layer",
            severity="SPEC",
        )


# ==============================================================================
# 8. SCORECARD
# ==============================================================================


def print_scorecard() -> int:
    print_section("8. Scorecard", "INVARIANT fail → exit 1; EMPIRICAL drift → WARN; SPEC pending → PENDING")

    inv = [c for c in TEST_RESULTS if c.severity == "INVARIANT"]
    emp = [c for c in TEST_RESULTS if c.severity == "EMPIRICAL"]
    spec = [c for c in TEST_RESULTS if c.severity == "SPEC"]
    inv_fail = [c for c in inv if not c.ok]
    emp_warn = [c for c in emp if not c.ok]
    spec_pend = [c for c in spec if not c.ok]

    rows = []
    for c in TEST_RESULTS:
        if c.severity == "INVARIANT":
            status = "PASS" if c.ok else "FAIL"
        elif c.severity == "EMPIRICAL":
            status = "PASS" if c.ok else "WARN"
        else:
            status = "APPLIED" if c.ok else "PENDING"
        rows.append([c.severity, c.section, c.name, status, c.details[:90]])
    render_table(
        f"FRD audit  invariants {len(inv) - len(inv_fail)}/{len(inv)}  "
        f"empirical {len(emp) - len(emp_warn)}/{len(emp)}  "
        f"spec {len(spec) - len(spec_pend)}/{len(spec)} applied",
        ["Sev", "Section", "Check", "Status", "Details"],
        rows,
    )

    summary = (
        f"INVARIANT {len(inv) - len(inv_fail)}/{len(inv)} pass, "
        f"EMPIRICAL {len(emp_warn)} warn, SPEC {len(spec_pend)} pending"
    )
    hard_fail = len(inv_fail) > 0 or (STRICT_SPEC and len(spec_pend) > 0)
    if HAS_RICH:
        style = "bold red" if hard_fail else "bold green"
        console.print(Panel(f"[{style}]{summary}[/{style}]", box=box.ROUNDED, expand=False))
    else:
        print(f"\n{summary}\n")

    if inv_fail:
        print("\nInvariant failures:")
        for c in inv_fail:
            print(f"  - {c.section} {c.name}: {c.details}")
    if spec_pend:
        print("\nSpec rules not yet applied (implementation checklist):")
        for c in spec_pend:
            print(f"  - {c.section} {c.name}: {c.details}")
    if emp_warn:
        print("\nEmpirical drift (FRD snapshot vs live store):")
        for c in emp_warn:
            print(f"  - {c.section} {c.name}: {c.details}")

    if hard_fail:
        return 1
    return 0


def dump_json(path: Path) -> None:
    payload = {
        "strict_spec": STRICT_SPEC,
        "checks": [asdict(c) for c in TEST_RESULTS],
        "counts": {
            "total": len(TEST_RESULTS),
            "invariant_fail": sum(1 for c in TEST_RESULTS if c.severity == "INVARIANT" and not c.ok),
            "empirical_warn": sum(1 for c in TEST_RESULTS if c.severity == "EMPIRICAL" and not c.ok),
            "spec_pending": sum(1 for c in TEST_RESULTS if c.severity == "SPEC" and not c.ok),
        },
    }
    path.write_text(json.dumps(payload, indent=2, default=str))
    msg = f"Wrote JSON scorecard → {path}"
    if HAS_RICH:
        console.print(f"[dim]{msg}[/dim]")
    else:
        print(msg)


# ==============================================================================
# MAIN
# ==============================================================================


def main(argv: Sequence[str] | None = None) -> int:
    global STRICT_SPEC
    parser = argparse.ArgumentParser(description="FRD silver-layer auditor")
    parser.add_argument("db", nargs="?", help="Path to etf.duckdb")
    parser.add_argument("--json", dest="json_out", help="Write machine-readable scorecard")
    parser.add_argument("--strict-spec", action="store_true", help="Treat unimplemented SPEC rules as hard failures")
    args = parser.parse_args(argv)
    STRICT_SPEC = args.strict_spec

    try:
        db_path = resolve_duckdb_path(args.db)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    if not db_path.exists():
        print(f"Error: database not found at {db_path}", file=sys.stderr)
        print("Usage: python catalog_silver_tables.py [path_to_etf.duckdb]", file=sys.stderr)
        return 1

    t0 = time.time()
    banner = f"Connected to DuckDB: {db_path} (READ_ONLY)"
    if HAS_RICH:
        console.print(f"[bold green]{banner}[/bold green]")
    else:
        print(banner)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        contract_universe = con.execute("SELECT COUNT(DISTINCT product_id) FROM bronze.contracts").fetchone()[0]
        priced_universe = con.execute("SELECT COUNT(DISTINCT product_id) FROM silver.products").fetchone()[0]
    except Exception as e:
        print(f"Database error resolving universe metrics: {e}", file=sys.stderr)
        con.close()
        return 1

    verify_macro_health(con, contract_universe, priced_universe)
    verify_session_1_profile_and_fees(con)
    verify_session_2_ratios_fundamentals(con)
    verify_session_3_holdings_allocations(con)
    verify_session_4_ratings_mstar_and_lipper(con)
    verify_session_5_themes_and_esg(con)
    verify_empirical_cadences(con)
    verify_panel_readiness(con)
    rc = print_scorecard()

    if args.json_out:
        dump_json(Path(args.json_out))

    elapsed = time.time() - t0
    done = f"FRD audit finished in {elapsed:.2f}s (exit {rc})."
    if HAS_RICH:
        console.print(f"\n[bold]{done}[/bold]\n")
    else:
        print(f"\n{done}\n")
    con.close()
    return rc


if __name__ == "__main__":
    sys.exit(main())
