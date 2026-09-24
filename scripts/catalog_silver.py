#!/usr/bin/env python3
"""Silver Layer Catalog & Data Contract Audit.

Audits `silver.observations` and `silver.monthly_panel` to validate invariants
and target contracts defined in silver_prep_spec.md (§7.1).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import duckdb

from etfportfolio.core.config import settings


def audit_observations(con: duckdb.DuckDBPyConnection) -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []

    # 1. Total row count and distinct products
    row = con.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT product_id),
            COUNT(DISTINCT family),
            COUNT(DISTINCT metric)
        FROM silver.observations
        """
    ).fetchone()
    total_obs, n_products, n_families, n_metrics = row
    results.append(
        (
            "Observations footprint",
            True,
            f"{total_obs:,} rows across {n_products:,} products, {n_families} families, {n_metrics} metrics",
        )
    )

    # 2. 0 duplicate PKs on (product_id, family, metric, effective_date)
    dup_pks = con.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT product_id, family, metric, effective_date
            FROM silver.observations
            GROUP BY product_id, family, metric, effective_date
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    results.append(
        (
            "0 duplicate PKs (product_id, family, metric, effective_date)",
            dup_pks == 0,
            f"{dup_pks} duplicates found",
        )
    )

    # 3. Temporal causality: effective_date <= fetched_at::DATE
    future_dates = con.execute(
        """
        SELECT COUNT(*)
        FROM silver.observations
        WHERE effective_date > fetched_at::DATE
        """
    ).fetchone()[0]
    results.append(
        (
            "Temporal causality (effective_date <= fetched_at::DATE)",
            future_dates == 0,
            f"{future_dates} future-dated rows found",
        )
    )

    # 4. Finite IEEE 754 floats (not isnan, not isinf, not null)
    non_finite = con.execute(
        """
        SELECT COUNT(*)
        FROM silver.observations
        WHERE value IS NULL OR isnan(value) OR isinf(value)
        """
    ).fetchone()[0]
    results.append(
        (
            "Finite IEEE 754 values",
            non_finite == 0,
            f"{non_finite} non-finite values found",
        )
    )

    # 5. Theme referential check (informational)
    has_themes = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'themes'"
    ).fetchone()[0]
    if has_themes:
        orphan_themes = con.execute(
            """
            SELECT COUNT(*) FROM silver.observations o
            LEFT JOIN bronze.themes t ON o.code = t.theme_id
            WHERE o.family IN ('theme', 'rank_adj_theme') AND t.theme_id IS NULL
            """
        ).fetchone()[0]
        results.append(
            (
                "Theme code referential check (bronze.themes)",
                True,  # Informational, must not crash
                f"{orphan_themes} theme codes not in bronze.themes",
            )
        )

    return results


def audit_panel(con: duckdb.DuckDBPyConnection) -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []

    # 1. Total row count and footprint
    row = con.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT product_id),
            COUNT(DISTINCT as_of_date),
            COUNT(DISTINCT feature_id)
        FROM silver.monthly_panel
        """
    ).fetchone()
    total_panel, n_products, n_dates, n_features = row
    results.append(
        (
            "Monthly panel footprint",
            True,
            f"{total_panel:,} rows across {n_products:,} products, {n_dates:,} dates, {n_features:,} features",
        )
    )

    # 2. No asset_class column
    cols = [
        r[0]
        for r in con.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'silver' AND table_name = 'monthly_panel'
            """
        ).fetchall()
    ]
    has_ac_col = "asset_class" in cols
    results.append(
        (
            "No asset_class column in silver.monthly_panel",
            not has_ac_col,
            f"Columns: {cols}",
        )
    )

    # 3. 0 rows with feature_id = 'profile_total_net_assets_local'
    local_aum_cnt = con.execute(
        "SELECT COUNT(*) FROM silver.monthly_panel WHERE feature_id = 'profile_total_net_assets_local'"
    ).fetchone()[0]
    results.append(
        (
            "0 rows with feature_id = 'profile_total_net_assets_local'",
            local_aum_cnt == 0,
            f"{local_aum_cnt} local AUM rows found",
        )
    )

    # 4. profile_total_net_assets_usd presence
    usd_aum_cnt = con.execute(
        "SELECT COUNT(*) FROM silver.monthly_panel WHERE feature_id = 'profile_total_net_assets_usd'"
    ).fetchone()[0]
    results.append(
        (
            "profile_total_net_assets_usd presence",
            True,
            f"{usd_aum_cnt:,} USD AUM rows present",
        )
    )

    # 5. No obsolete / forbidden features
    prohibited_patterns = [
        ("debt_* cluster features", "feature_id LIKE 'debt_%'"),
        ("style_box_hist_* features", "feature_id LIKE 'style_box_hist_%'"),
        ("asset_class_other residual", "feature_id = 'asset_class_other'"),
        ("country_unidentified residual", "feature_id = 'country_unidentified'"),
        ("mat_lt_1y legacy slug", "feature_id LIKE '%mat_lt_1y%'"),
        ("esg_score aliases", "feature_id LIKE 'esg_score%'"),
    ]
    for desc, sql_cond in prohibited_patterns:
        cnt = con.execute(f"SELECT COUNT(*) FROM silver.monthly_panel WHERE {sql_cond}").fetchone()[0]
        results.append(
            (
                f"No {desc}",
                cnt == 0,
                f"{cnt} prohibited rows found",
            )
        )

    return results


def print_table(title: str, headers: list[str], rows: list[list[Any]]) -> None:
    print(f"\n--- {title} ---")
    if not rows:
        print("  (no records)")
        return
    widths = [max(len(str(x)) for x in [h] + [r[i] for r in rows]) for i, h in enumerate(headers)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("  ".join("-" * w for w in widths))
    for r in rows:
        print(fmt.format(*[str(c) for c in r]))


def main(db_path_str: str | None = None) -> int:
    target_path = Path(db_path_str or settings.db_path)
    if not target_path.exists():
        print(f"Error: Database file does not exist at {target_path}")
        return 1

    print(f"Connecting to DuckDB: {target_path} (READ_ONLY)")
    con = duckdb.connect(str(target_path), read_only=True)

    all_passed = True
    try:
        print("\n==================== 1. SILVER.OBSERVATIONS AUDIT ====================")
        obs_results = audit_observations(con)
        for check, ok, detail in obs_results:
            status = "PASS" if ok else "FAIL"
            if not ok:
                all_passed = False
            print(f"  [{status}] {check}: {detail}")

        # Summary of families
        family_summary = con.execute(
            """
            SELECT
                family,
                COUNT(*) AS total_obs,
                COUNT(DISTINCT product_id) AS products,
                COUNT(DISTINCT metric) AS metrics,
                MIN(effective_date) AS min_date,
                MAX(effective_date) AS max_date
            FROM silver.observations
            GROUP BY family
            ORDER BY total_obs DESC
            """
        ).fetchall()
        print_table(
            "Observations by Family",
            ["Family", "Observations", "Products", "Metrics", "Min Date", "Max Date"],
            [[r[0], f"{r[1]:,}", f"{r[2]:,}", str(r[3]), str(r[4]), str(r[5])] for r in family_summary],
        )

        print("\n==================== 2. SILVER.MONTHLY_PANEL AUDIT ====================")
        panel_results = audit_panel(con)
        for check, ok, detail in panel_results:
            status = "PASS" if ok else "FAIL"
            if not ok:
                all_passed = False
            print(f"  [{status}] {check}: {detail}")

        # Sample panel features
        feature_summary = con.execute(
            """
            SELECT
                split_part(feature_id, '_', 1) AS family_prefix,
                COUNT(*) AS total_rows,
                COUNT(DISTINCT product_id) AS products,
                COUNT(DISTINCT feature_id) AS features,
                MIN(as_of_date) AS min_date,
                MAX(as_of_date) AS max_date
            FROM silver.monthly_panel
            GROUP BY 1
            ORDER BY total_rows DESC
            """
        ).fetchall()
        print_table(
            "Monthly Panel Features by Family Prefix",
            ["Family Prefix", "Rows", "Products", "Features", "Min Date", "Max Date"],
            [[r[0], f"{r[1]:,}", f"{r[2]:,}", str(r[3]), str(r[4]), str(r[5])] for r in feature_summary],
        )

        print("\n==================== VERDICT ====================")
        if all_passed:
            print("  ALL AUDIT CHECKS PASSED.")
            return 0
        else:
            print("  SOME AUDIT CHECKS FAILED.")
            return 1
    finally:
        con.close()


if __name__ == "__main__":
    db_arg = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(main(db_arg))
