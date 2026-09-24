"""Build `silver.monthly_panel` from clean Silver observations (Stage 2).

Aligns observations onto a per-product month-end trading spine. Series-macro only:
last-value interpolation with per-series p99 caps, snapshot replacement with stored
zeros for composition/dummy families, in-month AUM->USD.
"""

from __future__ import annotations

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
    conn.execute("CREATE TEMP TABLE product_spine (product_id INTEGER NOT NULL, as_of_date DATE NOT NULL)")
    spine_rows: list[tuple[int, date]] = []
    for pid, first_p, last_p in product_bounds:
        for m_date in month_end_spine(first_p, last_p):
            spine_rows.append((pid, m_date))

    conn.executemany("INSERT INTO product_spine VALUES (?, ?)", spine_rows)

    # --------------------------------------------------------------------------
    # 1. DEFAULT-0 FAMILIES (Densified snapshot replacement with stored zeros)
    # --------------------------------------------------------------------------
    console.info("Building default-0 family panels with stored zeros…")

    # Style merge: style_box beats style_box_hist on any given (product_id, effective_date)
    conn.execute(
        """
        CREATE TEMP TABLE default0_obs AS
        SELECT
            product_id,
            family,
            metric,
            effective_date,
            date_source_depth,
            fetched_at,
            value
        FROM silver.observations
        WHERE family IN (
            'asset_class', 'country', 'industry', 'credit_rating',
            'maturity', 'theme', 'rank_adj_theme'
        )

        UNION ALL

        SELECT
            s.product_id,
            'style_box' AS family,
            s.metric,
            s.effective_date,
            s.date_source_depth,
            s.fetched_at,
            s.value
        FROM (
            SELECT
                product_id,
                effective_date,
                bool_or(family = 'style_box') AS has_selected
            FROM silver.observations
            WHERE family IN ('style_box', 'style_box_hist')
            GROUP BY product_id, effective_date
        ) p
        JOIN silver.observations s
          ON s.product_id = p.product_id
         AND s.effective_date = p.effective_date
         AND s.family = (CASE WHEN p.has_selected THEN 'style_box' ELSE 'style_box_hist' END)
        """
    )

    # Distinct snapshot dates per (product_id, family)
    conn.execute(
        """
        CREATE TEMP TABLE default0_snaps AS
        SELECT DISTINCT
            product_id,
            family,
            effective_date
        FROM default0_obs
        """
    )

    # Gaps between distinct snapshot dates
    conn.execute(
        """
        CREATE TEMP TABLE default0_gaps AS
        SELECT
            product_id,
            family,
            effective_date,
            date_diff('day',
                LAG(effective_date) OVER (
                    PARTITION BY product_id, family
                    ORDER BY effective_date
                ),
                effective_date
            ) AS gap_days
        FROM default0_snaps
        """
    )

    # Cap per (product_id, family): n_gaps == 0 -> singleton; >= 1 -> p99
    conn.execute(
        """
        CREATE TEMP TABLE default0_caps AS
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

    # Live snapshot per (product_id, as_of_date, family)
    conn.execute(
        """
        CREATE TEMP TABLE default0_live AS
        -- Singleton case: live across full trading spine
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

        -- Multi-obs case: live between first_snap and last_snap + cap
        SELECT
            sp.product_id,
            sp.as_of_date,
            c.family,
            latest.snap
        FROM product_spine sp
        JOIN default0_caps c
          ON c.product_id = sp.product_id
         AND c.n_gaps >= 1
        JOIN LATERAL (
            SELECT MAX(s.effective_date) AS snap
            FROM default0_snaps s
            WHERE s.product_id = sp.product_id
              AND s.family = c.family
              AND s.effective_date <= sp.as_of_date
        ) latest ON latest.snap IS NOT NULL
        WHERE date_diff('day', latest.snap, sp.as_of_date) <= c.cap
        """
    )

    # Register default-0 universes
    conn.execute(
        """
        CREATE TEMP TABLE default0_universe (
            family VARCHAR NOT NULL,
            metric VARCHAR NOT NULL,
            product_id INTEGER
        )
        """
    )

    canonical_universe: list[tuple[str, str, int | None]] = []
    for k, is_res in ASSET_CLASS_METRICS.items():
        if not is_res:
            canonical_universe.append(("asset_class", k, None))
    for k, is_res in INDUSTRY_METRICS.items():
        if not is_res:
            canonical_universe.append(("industry", k, None))
    for k, is_res in CREDIT_RATING_METRICS.items():
        if not is_res:
            canonical_universe.append(("credit_rating", k, None))
    for k, is_res in MATURITY_METRICS.items():
        if not is_res:
            canonical_universe.append(("maturity", k, None))
    for cell in STYLE_CELLS:
        canonical_universe.append(("style_box", cell, None))

    conn.executemany("INSERT INTO default0_universe VALUES (?, ?, ?)", canonical_universe)

    # Open universes (country, theme, rank_adj_theme): distinct per product all time
    conn.execute(
        """
        INSERT INTO default0_universe (family, metric, product_id)
        SELECT DISTINCT family, metric, product_id
        FROM silver.observations
        WHERE family IN ('country', 'theme', 'rank_adj_theme')
        """
    )

    # Densify live snapshots against universe: overlay observed values, missing -> 0.0
    conn.execute(
        """
        CREATE TEMP TABLE panel_default0 AS
        SELECT
            live.product_id,
            live.as_of_date,
            live.family || '_' || u.metric AS feature_id,
            COALESCE(o.value, 0.0) AS value
        FROM default0_live live
        JOIN default0_universe u
          ON u.family = live.family
         AND (u.product_id = live.product_id OR u.product_id IS NULL)
        LEFT JOIN default0_obs o
          ON o.product_id = live.product_id
         AND o.family = live.family
         AND o.metric = u.metric
         AND o.effective_date = live.snap
        """
    )

    # --------------------------------------------------------------------------
    # 2. SCALAR FAMILIES (Interpolation without stored zeros) & AUM -> USD
    # --------------------------------------------------------------------------
    console.info("Interpolating scalar families and computing USD AUM…")

    # AUM -> USD monthly aggregated series
    conn.execute(
        """
        CREATE TEMP TABLE monthly_fx AS
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
        CREATE TEMP TABLE aum_usd_monthly AS
        SELECT
            o.product_id,
            LAST_DAY(o.effective_date) AS effective_month,
            AVG(
                o.value * CASE
                    WHEN o.code = 'USD' THEN 1.0
                    ELSE fx.rate_to_usd
                END
            ) AS value
        FROM silver.observations o
        LEFT JOIN monthly_fx fx
          ON fx.currency = o.code
         AND fx.month_end = LAST_DAY(o.effective_date)
        WHERE o.family = 'profile'
          AND o.metric = 'total_net_assets_local'
          AND (o.code = 'USD' OR fx.rate_to_usd IS NOT NULL)
        GROUP BY o.product_id, LAST_DAY(o.effective_date)
        """
    )

    # Union all scalar observations
    conn.execute(
        """
        CREATE TEMP TABLE scalar_obs AS
        SELECT
            product_id,
            family,
            metric,
            family || '_' || metric AS feature_id,
            effective_date,
            fetched_at,
            value
        FROM silver.observations
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

    # Distinct observations per (product_id, feature_id, effective_date), latest fetched_at wins
    conn.execute(
        """
        CREATE TEMP TABLE scalar_obs_distinct AS
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

    # Gaps between observation dates per (product_id, feature_id)
    conn.execute(
        """
        CREATE TEMP TABLE scalar_gaps AS
        SELECT
            product_id,
            feature_id,
            effective_date,
            date_diff('day',
                LAG(effective_date) OVER (
                    PARTITION BY product_id, feature_id
                    ORDER BY effective_date
                ),
                effective_date
            ) AS gap_days
        FROM scalar_obs_distinct
        """
    )

    # Cap per (product_id, feature_id)
    conn.execute(
        """
        CREATE TEMP TABLE scalar_caps AS
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

    # Scalar interpolation onto trading spine
    conn.execute(
        """
        CREATE TEMP TABLE panel_scalar AS
        -- Singleton: live across whole trading spine
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

        -- Multi-obs: live between first_obs and last_obs + cap
        SELECT
            sp.product_id,
            sp.as_of_date,
            sc.feature_id,
            latest.value
        FROM product_spine sp
        JOIN scalar_caps sc
          ON sc.product_id = sp.product_id
         AND sc.n_gaps >= 1
        JOIN LATERAL (
            SELECT so.value, so.effective_date
            FROM scalar_obs_distinct so
            WHERE so.product_id = sp.product_id
              AND so.feature_id = sc.feature_id
              AND so.effective_date <= sp.as_of_date
            ORDER BY so.effective_date DESC, so.fetched_at DESC
            LIMIT 1
        ) latest ON latest.effective_date IS NOT NULL
        WHERE sp.as_of_date >= sc.min_date
          AND date_diff('day', latest.effective_date, sp.as_of_date) <= sc.cap
        """
    )

    # --------------------------------------------------------------------------
    # 3. WRITE PATH
    # --------------------------------------------------------------------------
    conn.execute(
        """
        CREATE TEMP TABLE panel_final AS
        SELECT product_id, as_of_date, feature_id, value FROM panel_default0
        UNION ALL
        SELECT product_id, as_of_date, feature_id, value FROM panel_scalar
        """
    )

    n_rows = conn.execute("SELECT COUNT(*) FROM panel_final").fetchone()[0]
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM silver.monthly_panel")
        conn.execute(
            """
            INSERT INTO silver.monthly_panel (product_id, as_of_date, feature_id, value)
            SELECT product_id, as_of_date, feature_id, value
            FROM panel_final
            """
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    console.info(f"Wrote {n_rows} rows to silver.monthly_panel.")
    return int(n_rows)
