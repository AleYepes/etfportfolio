"""Build `silver.monthly_panel` from clean Silver observations (Stage 2).

Aligned point-in-time monthly factor panel construction following FRD_panel_performance:
- LEAD-based precomputed validity intervals (eliminates LATERAL correlated subqueries).
- Bifurcated universe joins (strict equi-joins separating canonical and product open universes).
- Single-pass style box deduplication via QUALIFY.
- Direct streaming appends to silver.monthly_panel.
- Outlier detection via rolling MAD / 5*IQR with zero-IQR guardrail on continuous metrics.
- Architecture A theme aggregation: child themes rolled into parent themes.
- Global metric low-count pruning with safety floor (max(5, median - 3*IQR)).
"""

from __future__ import annotations

import logging
import time
from calendar import monthrange
from datetime import date

from etfportfolio.core.db import db_connection
from etfportfolio.core.endpoints import (
    ASSET_CLASS_METRICS,
    CREDIT_RATING_METRICS,
    INDUSTRY_METRICS,
    MATURITY_METRICS,
    STYLE_CELLS,
)
from etfportfolio.core.logging import console

logger = logging.getLogger(__name__)


def month_end_spine(start: date, end: date) -> list[date]:
    """Inclusive month-end dates covering every month from `start` through `end`."""
    if start > end:
        return []
    dates: list[date] = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        dates.append(date(year, month, monthrange(year, month)[1]))
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1
    return dates


def run_panel(db_path: str | None = None) -> int:
    """Rebuild `silver.monthly_panel` from current Silver observations.

    Always a full rebuild. Returns the number of long rows written.
    """
    with db_connection(db_path) as conn:
        return _build_panel(conn)


def _build_panel(conn) -> int:
    panel_start = time.perf_counter()
    n_obs = conn.execute("SELECT COUNT(*) FROM silver.observations").fetchone()[0]
    if n_obs == 0:
        conn.execute("DELETE FROM silver.monthly_panel")
        console.info("No Silver observations; monthly_panel cleared.")
        return 0

    # Determine per-product trading bounds from bronze.prices
    product_bounds = conn.execute(
        """
        SELECT
            p.product_id,
            MIN(p.date)::DATE AS first_price,
            MAX(p.date)::DATE AS last_price
        FROM bronze.prices p
        WHERE p.product_id IN (SELECT DISTINCT product_id FROM silver.observations)
        GROUP BY p.product_id
        """
    ).fetchall()

    if not product_bounds:
        conn.execute("DELETE FROM silver.monthly_panel")
        console.info("No products with both prices and observations; monthly_panel cleared.")
        return 0

    # Build per-product trading spine
    console.info("Building per-product trading spines from bronze.prices…")
    conn.execute("CREATE OR REPLACE TEMP TABLE product_spine (product_id INTEGER NOT NULL, as_of_date DATE NOT NULL)")
    spine_rows: list[tuple[int, date]] = []
    for pid, first_p, last_p in product_bounds:
        for m_date in month_end_spine(first_p, last_p):
            spine_rows.append((pid, m_date))

    conn.executemany("INSERT INTO product_spine VALUES (?, ?)", spine_rows)

    min_spine_date = min(r[1] for r in spine_rows)
    max_spine_date = max(r[1] for r in spine_rows)
    logger.info(
        "Resolved trading bounds for %d products spanning %s to %s (%d total product-months)",
        len(product_bounds),
        min_spine_date,
        max_spine_date,
        len(spine_rows),
    )

    # --------------------------------------------------------------------------
    # 2. INVARIANT VALIDATION & THEME MAPPING
    # --------------------------------------------------------------------------
    # 5.2.1 Invariant: Direct Parent Theme Check
    n_direct = conn.execute(
        """
        WITH theme_obs AS (
            SELECT DISTINCT metric, code
            FROM silver.observations
            WHERE family IN ('theme', 'rank_adj_theme')
        ),
        parent_themes AS (
            SELECT
                theme_id AS parent_code,
                trim(regexp_replace(lower(replace(trim(name), '&', 'and')), '[^a-z0-9]+', '_', 'g'), '_') AS parent_metric
            FROM bronze.themes
            WHERE parent_id IS NULL
        ),
        child_themes AS (
            SELECT
                theme_id AS child_code,
                trim(regexp_replace(lower(replace(trim(name), '&', 'and')), '[^a-z0-9]+', '_', 'g'), '_') AS child_metric
            FROM bronze.themes
            WHERE parent_id IS NOT NULL
        )
        SELECT COUNT(*)
        FROM theme_obs o
        JOIN parent_themes p
          ON (o.code IS NOT NULL AND o.code = p.parent_code)
          OR (o.metric = p.parent_metric AND NOT EXISTS (
              SELECT 1 FROM child_themes c
              WHERE (o.code IS NOT NULL AND o.code = c.child_code)
                 OR (o.code IS NULL AND o.metric = c.child_metric)
          ))
        """
    ).fetchone()[0]
    if n_direct > 0:
        raise ValueError(
            f"Found {n_direct} direct parent theme observations in silver.observations, violating vendor contract invariants"
        )

    # 5.2.2 Theme Mapping Table
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE theme_mapping AS
        SELECT
            c.theme_id AS child_code,
            trim(regexp_replace(lower(replace(trim(c.name), '&', 'and')), '[^a-z0-9]+', '_', 'g'), '_') AS child_metric,
            trim(regexp_replace(lower(replace(trim(p.name), '&', 'and')), '[^a-z0-9]+', '_', 'g'), '_') AS parent_metric
        FROM bronze.themes c
        JOIN bronze.themes p ON c.parent_id = p.theme_id
        WHERE c.parent_id IS NOT NULL
        """
    )

    # --------------------------------------------------------------------------
    # 3. DISTRIBUTION-AWARE TIME-SERIES OUTLIER DETECTION
    # --------------------------------------------------------------------------
    console.info("Running distribution-aware time-series outlier detection…")
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE candidate_metrics AS
        SELECT
            family,
            metric,
            COUNT(*)::FLOAT AS n_total,
            COUNT(DISTINCT value)::FLOAT / COUNT(*)::FLOAT AS distinct_ratio,
            skewness(value) AS skew_val,
            MIN(value) AS min_val,
            MAX(value) AS max_val
        FROM silver.observations
        WHERE family IN ('ratios', 'profile', 'esg', 'theme', 'rank_adj_theme')
          AND metric NOT IN ('is_passive')
        GROUP BY family, metric
        HAVING COUNT(DISTINCT value) > 10
           AND (COUNT(DISTINCT value)::FLOAT / COUNT(*)::FLOAT) >= 0.05
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE outlier_observations AS
        WITH transformed AS (
            SELECT
                s.product_id,
                s.family,
                s.metric,
                s.effective_date,
                s.value,
                s.raw_value,
                CASE
                    WHEN m.skew_val > 3.0 THEN ln(s.value - m.min_val + 1.0)
                    WHEN m.skew_val < -3.0 THEN ln(m.max_val - s.value + 1.0)
                    ELSE s.value
                END AS z
            FROM silver.observations s
            JOIN candidate_metrics m
              ON s.family = m.family AND s.metric = m.metric
        ),
        stats AS (
            SELECT
                product_id,
                family,
                metric,
                effective_date,
                value,
                raw_value,
                z,
                quantile_cont(z, 0.5) OVER w AS med_z,
                quantile_cont(z, 0.25) OVER w AS q25_z,
                quantile_cont(z, 0.75) OVER w AS q75_z
            FROM transformed
            WINDOW w AS (
                PARTITION BY product_id, family, metric
                ORDER BY effective_date
                ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING
            )
        )
        SELECT
            product_id,
            family,
            metric,
            effective_date,
            value,
            raw_value
        FROM stats
        WHERE (q75_z - q25_z) > 0.0
          AND abs(z - med_z) > 5.0 * (q75_z - q25_z)
        """
    )

    outlier_summary = conn.execute(
        """
        SELECT family, metric, COUNT(*) AS n_dropped, MIN(value) AS min_val, MAX(value) AS max_val
        FROM outlier_observations
        GROUP BY family, metric
        ORDER BY n_dropped DESC
        """
    ).fetchall()

    n_total_outliers = sum(r[2] for r in outlier_summary)
    if n_total_outliers > 0:
        logger.info(
            "Outlier preprocessor flagged %d observations across %d metrics", n_total_outliers, len(outlier_summary)
        )
        for fam, met, cnt, mn, mx in outlier_summary[:10]:
            logger.debug("Outlier drop: %s.%s (%d rows, range [%s, %s])", fam, met, cnt, mn, mx)

    # --------------------------------------------------------------------------
    # 4. THEME AGGREGATION & CLEAN OBSERVATIONS ASSEMBLY
    # --------------------------------------------------------------------------
    console.info("Assembling clean observations and rolling up themes…")
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE clean_observations AS
        -- 1. Non-theme observations (excluding outliers)
        SELECT
            s.product_id,
            s.family,
            s.metric,
            s.code,
            s.effective_date,
            s.date_source_depth,
            s.fetched_at,
            s.value
        FROM silver.observations s
        LEFT JOIN outlier_observations outl
          ON s.product_id = outl.product_id
         AND s.family = outl.family
         AND s.metric = outl.metric
         AND s.effective_date = outl.effective_date
        WHERE s.family NOT IN ('theme', 'rank_adj_theme')
          AND outl.product_id IS NULL

        UNION ALL

        -- 2. Theme observations aggregated to parent themes (excluding outliers and unmapped child themes)
        SELECT
            s.product_id,
            s.family,
            m.parent_metric AS metric,
            NULL AS code,
            s.effective_date,
            MAX(s.date_source_depth) AS date_source_depth,
            MAX(s.fetched_at) AS fetched_at,
            SUM(s.value) AS value
        FROM silver.observations s
        JOIN theme_mapping m
          ON (s.code IS NOT NULL AND s.code = m.child_code)
          OR (s.code IS NULL AND s.metric = m.child_metric)
        LEFT JOIN outlier_observations outl
          ON s.product_id = outl.product_id
         AND s.family = outl.family
         AND s.metric = outl.metric
         AND s.effective_date = outl.effective_date
        WHERE s.family IN ('theme', 'rank_adj_theme')
          AND outl.product_id IS NULL
        GROUP BY s.product_id, s.family, m.parent_metric, s.effective_date
        """
    )

    # --------------------------------------------------------------------------
    # 5. GLOBAL METRIC LOW-COUNT PRUNING
    # --------------------------------------------------------------------------
    console.info("Applying global low-count metric pruning…")
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE metric_counts AS
        SELECT family, metric, COUNT(*) AS obs_count
        FROM clean_observations
        GROUP BY family, metric
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE global_cutoff AS
        SELECT
            GREATEST(5, (
                quantile_cont(obs_count, 0.5) - 3.0 * (quantile_cont(obs_count, 0.75) - quantile_cont(obs_count, 0.25))
            ))::INTEGER AS min_threshold
        FROM metric_counts
        """
    )

    cutoff_row = conn.execute("SELECT min_threshold FROM global_cutoff").fetchone()
    min_threshold = cutoff_row[0] if cutoff_row and cutoff_row[0] is not None else 5

    pruned_metrics = conn.execute(
        """
        SELECT family, metric, obs_count
        FROM metric_counts
        WHERE obs_count < ?
        ORDER BY obs_count ASC
        """,
        [min_threshold],
    ).fetchall()
    if pruned_metrics:
        logger.info(
            "Global low-count pruning (threshold=%d) dropped %d metrics",
            min_threshold,
            len(pruned_metrics),
        )
        for fam, met, cnt in pruned_metrics[:10]:
            logger.debug("Pruned metric: %s.%s (%d observations)", fam, met, cnt)

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE surviving_observations AS
        SELECT o.*
        FROM clean_observations o
        JOIN metric_counts mc
          ON o.family = mc.family AND o.metric = mc.metric
        JOIN global_cutoff gc
          ON mc.obs_count >= gc.min_threshold
        """
    )

    # --------------------------------------------------------------------------
    # 6. DEFAULT-0 FAMILIES (Validity Intervals & Bifurcation)
    # --------------------------------------------------------------------------
    console.info("Building default-0 family panels with validity intervals and bifurcated joins…")

    # Style merge: QUALIFY
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE default0_obs AS
        SELECT
            product_id,
            family,
            metric,
            effective_date,
            date_source_depth,
            fetched_at,
            value
        FROM surviving_observations
        WHERE family IN (
            'asset_class', 'country', 'industry', 'credit_rating',
            'maturity', 'theme', 'rank_adj_theme'
        )

        UNION ALL

        SELECT
            product_id,
            'style_box' AS family,
            metric,
            effective_date,
            date_source_depth,
            fetched_at,
            value
        FROM surviving_observations
        WHERE family IN ('style_box', 'style_box_hist')
        QUALIFY family = CASE
            WHEN bool_or(family = 'style_box') OVER (PARTITION BY product_id, effective_date)
            THEN 'style_box'
            ELSE 'style_box_hist'
        END
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE default0_snaps AS
        SELECT DISTINCT product_id, family, effective_date
        FROM default0_obs
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE default0_gaps AS
        SELECT
            product_id,
            family,
            effective_date,
            date_diff('day',
                LAG(effective_date) OVER (PARTITION BY product_id, family ORDER BY effective_date),
                effective_date
            ) AS gap_days
        FROM default0_snaps
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE default0_caps AS
        SELECT
            product_id,
            family,
            COUNT(gap_days) AS n_gaps,
            quantile_cont(gap_days, 0.99)::INTEGER AS cap,
            MIN(effective_date) AS min_snap
        FROM default0_gaps
        WHERE gap_days IS NULL OR gap_days > 0
        GROUP BY product_id, family
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE default0_live AS
        -- Singleton case: valid across the product's entire price spine
        SELECT
            sp.product_id,
            sp.as_of_date,
            c.family,
            c.min_snap AS snap
        FROM product_spine sp
        JOIN default0_caps c
          ON c.product_id = sp.product_id
         AND c.n_gaps = 0

        UNION ALL

        -- Multi-obs case: precomputed validity intervals
        SELECT
            sp.product_id,
            sp.as_of_date,
            inv.family,
            inv.snap
        FROM product_spine sp
        JOIN (
            SELECT
                s.product_id,
                s.family,
                s.effective_date AS snap,
                s.effective_date AS valid_from,
                CASE
                    WHEN LEAD(s.effective_date) OVER w IS NOT NULL
                    THEN LEAST(s.effective_date + c.cap, LEAD(s.effective_date) OVER w - 1)
                    ELSE s.effective_date + c.cap
                END AS valid_to
            FROM default0_snaps s
            JOIN default0_caps c
              ON c.product_id = s.product_id
             AND c.family = s.family
             AND c.n_gaps >= 1
            WINDOW w AS (PARTITION BY s.product_id, s.family ORDER BY s.effective_date)
        ) inv
          ON sp.product_id = inv.product_id
         AND sp.as_of_date >= inv.valid_from
         AND sp.as_of_date <= inv.valid_to
        """
    )

    # 5.6.3 Bifurcated Universe Registration
    conn.execute("CREATE OR REPLACE TEMP TABLE canonical_universe (family VARCHAR NOT NULL, metric VARCHAR NOT NULL)")

    canonical_rows: list[tuple[str, str]] = []
    for k, is_res in ASSET_CLASS_METRICS.items():
        if not is_res:
            canonical_rows.append(("asset_class", k))
    for k, is_res in INDUSTRY_METRICS.items():
        if not is_res:
            canonical_rows.append(("industry", k))
    for k, is_res in CREDIT_RATING_METRICS.items():
        if not is_res:
            canonical_rows.append(("credit_rating", k))
    for k, is_res in MATURITY_METRICS.items():
        if not is_res:
            canonical_rows.append(("maturity", k))
    for cell in STYLE_CELLS:
        canonical_rows.append(("style_box", cell))

    conn.executemany("INSERT INTO canonical_universe VALUES (?, ?)", canonical_rows)

    conn.execute(
        """
        INSERT INTO canonical_universe (family, metric)
        SELECT DISTINCT 'theme', trim(regexp_replace(lower(replace(trim(name), '&', 'and')), '[^a-z0-9]+', '_', 'g'), '_')
        FROM bronze.themes
        WHERE parent_id IS NULL
        UNION ALL
        SELECT DISTINCT 'rank_adj_theme', trim(regexp_replace(lower(replace(trim(name), '&', 'and')), '[^a-z0-9]+', '_', 'g'), '_')
        FROM bronze.themes
        WHERE parent_id IS NULL
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE product_universe AS
        SELECT DISTINCT family, metric, product_id
        FROM surviving_observations
        WHERE family = 'country'
        """
    )

    # --------------------------------------------------------------------------
    # 7. SCALAR FAMILIES & FX CONVERSION
    # --------------------------------------------------------------------------
    console.info("Interpolating scalar families and computing USD AUM…")
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE monthly_fx AS
        SELECT
            source_currency AS currency,
            LAST_DAY(date::DATE) AS month_end,
            AVG(close) AS rate_to_usd
        FROM bronze.fx
        WHERE target_currency = 'USD'
        GROUP BY 1, 2
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE aum_usd_monthly AS
        SELECT
            o.product_id,
            LAST_DAY(o.effective_date) AS effective_month,
            AVG(
                o.value * CASE
                    WHEN o.code = 'USD' THEN 1.0
                    ELSE fx.rate_to_usd
                END
            ) AS value
        FROM surviving_observations o
        LEFT JOIN monthly_fx fx
          ON fx.currency = o.code
         AND fx.month_end = LAST_DAY(o.effective_date)
        WHERE o.family = 'profile'
          AND o.metric = 'total_net_assets_local'
          AND (o.code = 'USD' OR fx.rate_to_usd IS NOT NULL)
        GROUP BY o.product_id, LAST_DAY(o.effective_date)
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE scalar_obs AS
        SELECT
            product_id,
            family,
            metric,
            family || '_' || metric AS feature_id,
            effective_date,
            fetched_at,
            value
        FROM surviving_observations
        WHERE family IN ('ratios', 'lipper', 'esg', 'mstar', 'profile')
          AND NOT (family = 'profile' AND metric = 'total_net_assets_local')

        UNION ALL

        SELECT
            product_id,
            'profile' AS family,
            'total_net_assets_usd' AS metric,
            'profile_total_net_assets_usd' AS feature_id,
            effective_month AS effective_date,
            effective_month::TIMESTAMP WITH TIME ZONE AS fetched_at,
            value
        FROM aum_usd_monthly
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE scalar_obs_distinct AS
        SELECT
            product_id,
            family,
            metric,
            feature_id,
            effective_date,
            fetched_at,
            value
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY product_id, feature_id, effective_date
                    ORDER BY fetched_at DESC
                ) AS rn
            FROM scalar_obs
        )
        WHERE rn = 1
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE scalar_gaps AS
        SELECT
            product_id,
            feature_id,
            effective_date,
            date_diff('day',
                LAG(effective_date) OVER (PARTITION BY product_id, feature_id ORDER BY effective_date),
                effective_date
            ) AS gap_days
        FROM scalar_obs_distinct
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE scalar_caps AS
        SELECT
            product_id,
            feature_id,
            COUNT(gap_days) AS n_gaps,
            quantile_cont(gap_days, 0.99)::INTEGER AS cap,
            MIN(effective_date) AS min_date
        FROM scalar_gaps
        WHERE gap_days IS NULL OR gap_days > 0
        GROUP BY product_id, feature_id
        """
    )

    # --------------------------------------------------------------------------
    # 8. DIRECT STREAMING APPENDS TO silver.monthly_panel
    # --------------------------------------------------------------------------
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM silver.monthly_panel")

        # 1. Canonical closed universes (strict join on family)
        conn.execute(
            """
            INSERT INTO silver.monthly_panel (product_id, as_of_date, feature_id, value)
            SELECT
                live.product_id,
                live.as_of_date,
                live.family || '_' || u.metric AS feature_id,
                COALESCE(o.value, 0.0) AS value
            FROM default0_live live
            JOIN canonical_universe u
              ON u.family = live.family
            LEFT JOIN default0_obs o
              ON o.product_id = live.product_id
             AND o.family = live.family
             AND o.metric = u.metric
             AND o.effective_date = live.snap
            """
        )

        # 2. Open per-product universes (strict equi-join on product_id AND family)
        conn.execute(
            """
            INSERT INTO silver.monthly_panel (product_id, as_of_date, feature_id, value)
            SELECT
                live.product_id,
                live.as_of_date,
                live.family || '_' || u.metric AS feature_id,
                COALESCE(o.value, 0.0) AS value
            FROM default0_live live
            JOIN product_universe u
              ON u.product_id = live.product_id
             AND u.family = live.family
            LEFT JOIN default0_obs o
              ON o.product_id = live.product_id
             AND o.family = live.family
             AND o.metric = u.metric
             AND o.effective_date = live.snap
            """
        )

        # 3. Scalar observations via LEAD intervals
        conn.execute(
            """
            INSERT INTO silver.monthly_panel (product_id, as_of_date, feature_id, value)
            -- Singleton scalars: full spine coverage
            SELECT
                sp.product_id,
                sp.as_of_date,
                sc.feature_id,
                so.value
            FROM product_spine sp
            JOIN scalar_caps sc
              ON sc.product_id = sp.product_id
             AND sc.n_gaps = 0
            JOIN scalar_obs_distinct so
              ON so.product_id = sc.product_id
             AND so.feature_id = sc.feature_id

            UNION ALL

            -- Multi-obs scalars: valid within [valid_from, valid_to]
            SELECT
                sp.product_id,
                sp.as_of_date,
                inv.feature_id,
                inv.value
            FROM product_spine sp
            JOIN (
                SELECT
                    so.product_id,
                    so.feature_id,
                    so.value,
                    so.effective_date AS valid_from,
                    CASE
                        WHEN LEAD(so.effective_date) OVER w IS NOT NULL
                        THEN LEAST(so.effective_date + sc.cap, LEAD(so.effective_date) OVER w - 1)
                        ELSE so.effective_date + sc.cap
                    END AS valid_to
                FROM scalar_obs_distinct so
                JOIN scalar_caps sc
                  ON sc.product_id = so.product_id
                 AND sc.feature_id = so.feature_id
                 AND sc.n_gaps >= 1
                WINDOW w AS (PARTITION BY so.product_id, so.feature_id ORDER BY so.effective_date)
            ) inv
              ON sp.product_id = inv.product_id
             AND sp.as_of_date >= inv.valid_from
             AND sp.as_of_date <= inv.valid_to
            """
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    n_rows = conn.execute("SELECT COUNT(*) FROM silver.monthly_panel").fetchone()[0]
    elapsed = time.perf_counter() - panel_start
    console.info(f"Wrote {n_rows} rows to silver.monthly_panel.")
    logger.info(
        "Monthly panel rebuild complete in %.2fs: %d rows written",
        elapsed,
        n_rows,
    )
    return int(n_rows)
