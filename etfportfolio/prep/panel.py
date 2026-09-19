"""Build `silver.monthly_panel` from Silver observations (Stage 2).

Silver stays long and un-winsorized. LOCF, flattening, clustering, quality
gates, and winsorization happen only here.
"""

from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta

from etfportfolio.core.db import db_connection
from etfportfolio.core.logging import console
from etfportfolio.prep.utils import sanitize_metric_id

# Inclusive LOCF caps (days). None = perpetual (`is_passive`).
ESG_CAP_DAYS = 90
MONTHLY_CAP_DAYS = 180
MEDALIST_FEE_CAP_DAYS = 540
TENURE_STYLE_CAP_DAYS = 365

ASSET_CLASS_FEATURE_IDS = (
    "asset_class_equity",
    "asset_class_fixed_income",
    "asset_class_cash",
    "asset_class_other",
)

ASSET_CLASS_TO_FEATURE = {
    "Equity": "asset_class_equity",
    "Fixed Income": "asset_class_fixed_income",
    "Cash": "asset_class_cash",
    "Other": "asset_class_other",
}

FEATURE_TO_ASSET_CLASS = {v: k for k, v in ASSET_CLASS_TO_FEATURE.items()}

CLOSED_ASSET_CLASSES = frozenset(ASSET_CLASS_TO_FEATURE)

INDUSTRY_FEATURE_OVERRIDES = {
    "Not Classified - Non Equity": "sector_unclassified_non_equity",
    "Non Classified Equity": "sector_unclassified_equity",
}

ESG_FEATURE_MAP = {
    "tresgs": "esg_score",
    "tresgcs": "esg_combined_score",
    "tresgccs": "esg_controversies",
    "tresgens": "esg_environmental",
    "tresgenrrs": "esg_resource_use",
    "tresgeners": "esg_emissions",
    "tresgenpis": "esg_env_innovation",
    "tresgsos": "esg_social",
    "tresgsowos": "esg_workforce",
    "tresgsohrs": "esg_human_rights",
    "tresgsocos": "esg_community",
    "tresgsoprs": "esg_product_responsibility",
    "tresgcgs": "esg_governance",
    "tresgcgbds": "esg_management",
    "tresgcgsrs": "esg_shareholders",
    "tresgcgvss": "esg_csr_strategy",
    "esg_coverage": "esg_coverage",
}

STYLE_SIZES = ("large", "multi", "mid", "small")
STYLE_STYLES = ("value", "core", "growth")
STYLE_FEATURE_IDS = tuple(f"style_{size}_{style}" for size in STYLE_SIZES for style in STYLE_STYLES)

# Cross-sectional 1st/99th winsorization targets (profitability / leverage).
CROSS_SECTION_WINSOR_FEATURES = frozenset(
    {
        "return_on_equity_1yr",
        "return_on_equity_3yr",
        "return_on_assets_1yr",
        "return_on_assets_3yr",
        "return_on_investment_1yr",
        "return_on_investment_3yr",
        "return_on_capital",
        "return_on_capital_3yr",
        "total_assets_total_equity",
        "total_debt_total_capital",
        "total_debt_total_equity",
        "lt_debt_shareholders_equity",
        "ebit_to_interest",
        "sales_to_total_assets",
    }
)

RAW_MSTAR_PILLARS = (
    "mstar_people_analyst",
    "mstar_people_quant",
    "mstar_process_analyst",
    "mstar_process_quant",
    "mstar_parent_analyst",
    "mstar_parent_quant",
)

PARTITION_TOLERANCE = 0.0001
THEME_COVERAGE_GATE = 0.7
EXPENSE_RATIO_WINSOR = (-1.0, 2.0)
MAX_FINITE_CAP_DAYS = MEDALIST_FEE_CAP_DAYS

# Exhaustive 110 → 9 map. Unmapped names are a bug.
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
    "debt_agency_supranational": (
        "Agencies",
        "Small Business Administration",
    ),
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

DEBT_TYPE_TO_CLUSTER: dict[str, str] = {
    raw_name: cluster for cluster, names in DEBT_CLUSTERS.items() for raw_name in names
}


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


def industry_feature_id(name: str) -> str:
    override = INDUSTRY_FEATURE_OVERRIDES.get(name)
    if override is not None:
        return override
    return f"sector_{sanitize_metric_id(name)}"


def country_feature_id(code: str | None) -> str:
    if code is None or not str(code).strip():
        return "country_unidentified"
    return f"country_{str(code).strip().lower()}"


def credit_feature_id(name: str) -> str:
    return f"credit_{sanitize_metric_id(name)}"


def debt_cluster(raw_name: str) -> str:
    cluster = DEBT_TYPE_TO_CLUSTER.get(raw_name)
    if cluster is None:
        raise ValueError(f"Unmapped debt_type name {raw_name!r}")
    return cluster


def run_panel(db_path: str | None = None) -> int:
    """Rebuild `silver.monthly_panel` from current Silver observations.

    Always a full rebuild: LOCF depends on complete history. Returns the number
    of long rows written.
    """
    with db_connection(db_path) as conn:
        return _build_panel(conn)


def _build_panel(conn) -> int:
    n_metrics = conn.execute("SELECT COUNT(*) FROM silver.product_metrics").fetchone()[0]
    n_dims = conn.execute("SELECT COUNT(*) FROM silver.product_dimensions").fetchone()[0]
    if n_metrics == 0 and n_dims == 0:
        conn.execute("DELETE FROM silver.monthly_panel")
        console.info("No Silver observations; monthly_panel cleared.")
        return 0

    _register_maps(conn)
    _assert_closed_sets(conn)
    _load_month_ends(conn)
    console.info("Flattening Silver observations into panel features…")
    _flatten_observations(conn)
    console.info("Applying family LOCF onto the month-end spine…")
    _apply_locf(conn)
    console.info("Synthesizing derived features…")
    _synthesize(conn)
    _apply_theme_gate(conn)
    _winsorize(conn)
    _attach_dominant_asset_class(conn)
    _assert_partition_of_unity(conn)

    n_rows = conn.execute("SELECT COUNT(*) FROM panel_final").fetchone()[0]
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM silver.monthly_panel")
        conn.execute(
            """
            INSERT INTO silver.monthly_panel (product_id, as_of_date, asset_class, feature_id, value)
            SELECT product_id, as_of_date, asset_class, feature_id, value
            FROM panel_final
            """
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    console.info(f"Wrote {n_rows} rows to silver.monthly_panel.")
    return int(n_rows)


def _register_maps(conn) -> None:
    conn.execute("CREATE TEMP TABLE debt_map (raw_name VARCHAR PRIMARY KEY, cluster VARCHAR NOT NULL)")
    conn.executemany("INSERT INTO debt_map VALUES (?, ?)", list(DEBT_TYPE_TO_CLUSTER.items()))

    conn.execute("CREATE TEMP TABLE esg_map (metric_id VARCHAR PRIMARY KEY, feature_id VARCHAR NOT NULL)")
    conn.executemany("INSERT INTO esg_map VALUES (?, ?)", list(ESG_FEATURE_MAP.items()))

    conn.execute(
        "CREATE TEMP TABLE industry_override (dimension_name VARCHAR PRIMARY KEY, feature_id VARCHAR NOT NULL)"
    )
    conn.executemany("INSERT INTO industry_override VALUES (?, ?)", list(INDUSTRY_FEATURE_OVERRIDES.items()))

    conn.execute("CREATE TEMP TABLE asset_class_map (dimension_name VARCHAR PRIMARY KEY, feature_id VARCHAR NOT NULL)")
    conn.executemany("INSERT INTO asset_class_map VALUES (?, ?)", list(ASSET_CLASS_TO_FEATURE.items()))

    conn.execute(
        """
        CREATE TEMP TABLE style_grid (
            size VARCHAR NOT NULL,
            style VARCHAR NOT NULL,
            feature_id VARCHAR NOT NULL,
            code VARCHAR NOT NULL
        )
        """
    )
    conn.executemany(
        "INSERT INTO style_grid VALUES (?, ?, ?, ?)",
        [(size, style, f"style_{size}_{style}", f"{size}_{style}") for size in STYLE_SIZES for style in STYLE_STYLES],
    )

    conn.execute("CREATE TEMP TABLE xs_winsor_features (feature_id VARCHAR PRIMARY KEY)")
    conn.executemany(
        "INSERT INTO xs_winsor_features VALUES (?)",
        [(fid,) for fid in sorted(CROSS_SECTION_WINSOR_FEATURES)],
    )


def _assert_closed_sets(conn) -> None:
    unmapped_debt = conn.execute(
        """
        SELECT DISTINCT dimension_name
        FROM silver.product_dimensions
        WHERE dimension_type = 'debt_type'
          AND dimension_name NOT IN (SELECT raw_name FROM debt_map)
        ORDER BY 1
        """
    ).fetchall()
    if unmapped_debt:
        names = ", ".join(repr(r[0]) for r in unmapped_debt[:20])
        raise ValueError(f"Unmapped debt_type name(s): {names}")

    unknown_ac = conn.execute(
        """
        SELECT DISTINCT dimension_name
        FROM silver.product_dimensions
        WHERE dimension_type = 'asset_class'
          AND dimension_name NOT IN (SELECT dimension_name FROM asset_class_map)
        ORDER BY 1
        """
    ).fetchall()
    if unknown_ac:
        names = ", ".join(repr(r[0]) for r in unknown_ac)
        raise ValueError(f"Unknown asset_class name(s): {names}")


def _load_month_ends(conn) -> None:
    bounds = conn.execute(
        """
        SELECT MIN(d), MAX(d) FROM (
            SELECT effective_date AS d FROM silver.product_metrics
            UNION ALL
            SELECT effective_date FROM silver.product_dimensions
        )
        """
    ).fetchone()
    start, end = bounds
    end_extended = end + timedelta(days=MAX_FINITE_CAP_DAYS)
    months = month_end_spine(start, end_extended)
    conn.execute("CREATE TEMP TABLE month_ends (as_of_date DATE PRIMARY KEY)")
    conn.executemany("INSERT INTO month_ends VALUES (?)", [(d,) for d in months])


def _flatten_observations(conn) -> None:
    conn.execute(
        """
        CREATE TEMP TABLE obs_raw AS
        -- Metrics (ESG aliases applied; average_quality with raw '-' dropped)
        SELECT
            m.product_id,
            COALESCE(e.feature_id, m.metric_id) AS feature_id,
            CASE m.source
                WHEN 'esg' THEN 'esg'
                WHEN 'ratios' THEN 'ratios'
                WHEN 'lipper' THEN 'lipper'
                WHEN 'holdings' THEN 'holdings_scalar'
                WHEN 'theme_weights' THEN 'theme'
                ELSE m.metric_id
            END AS family,
            m.effective_date,
            m.fetched_at,
            m.value,
            m.raw_value,
            CASE
                WHEN m.metric_id = 'is_passive' THEN NULL
                WHEN m.source = 'esg' THEN $esg_cap
                WHEN m.source IN ('ratios', 'holdings', 'lipper', 'theme_weights') THEN $monthly_cap
                WHEN m.metric_id IN (
                    'mstar_morningstar_rating',
                    'mstar_sustainability_rating',
                    'total_net_assets_local'
                ) THEN $monthly_cap
                WHEN m.metric_id = 'manager_tenure_years' THEN $tenure_cap
                WHEN m.metric_id IN (
                    'total_expense_ratio',
                    'management_expense_ratio',
                    'non_management_expense_ratio',
                    'audited_net_expense_ratio',
                    'mstar_medalist_rating',
                    'mstar_analyst_coverage_pct'
                ) THEN $fee_cap
                WHEN m.metric_id LIKE 'mstar_people_%'
                  OR m.metric_id LIKE 'mstar_process_%'
                  OR m.metric_id LIKE 'mstar_parent_%' THEN $fee_cap
                ELSE $monthly_cap
            END AS cap_days,
            CASE
                WHEN m.metric_id = 'manager_tenure_years' THEN COALESCE(
                    try_strptime(m.raw_value, '%Y/%m/%d')::DATE,
                    try_strptime(m.raw_value, '%Y-%m-%d')::DATE,
                    CASE
                        WHEN length(trim(m.raw_value)) = 4
                             AND try_cast(trim(m.raw_value) AS INTEGER) IS NOT NULL
                        THEN make_date(CAST(trim(m.raw_value) AS INTEGER), 1, 1)
                    END
                )
            END AS start_date
        FROM silver.product_metrics m
        LEFT JOIN esg_map e
            ON e.metric_id = m.metric_id AND m.source = 'esg'
        WHERE NOT (m.metric_id = 'average_quality' AND m.raw_value = '-')

        UNION ALL
        -- asset_class weights
        SELECT
            d.product_id,
            am.feature_id,
            'asset_class' AS family,
            d.effective_date,
            d.fetched_at,
            d.value,
            d.raw_value,
            $monthly_cap AS cap_days,
            NULL::DATE AS start_date
        FROM silver.product_dimensions d
        JOIN asset_class_map am ON am.dimension_name = d.dimension_name
        WHERE d.dimension_type = 'asset_class'

        UNION ALL
        -- country
        SELECT
            d.product_id,
            CASE
                WHEN d.dimension_code IS NULL OR trim(CAST(d.dimension_code AS VARCHAR)) = ''
                THEN 'country_unidentified'
                ELSE 'country_' || lower(trim(CAST(d.dimension_code AS VARCHAR)))
            END AS feature_id,
            'country' AS family,
            d.effective_date,
            d.fetched_at,
            d.value,
            d.raw_value,
            $monthly_cap AS cap_days,
            NULL::DATE AS start_date
        FROM silver.product_dimensions d
        WHERE d.dimension_type = 'country'

        UNION ALL
        -- industry / sector
        SELECT
            d.product_id,
            COALESCE(
                io.feature_id,
                'sector_' || trim(regexp_replace(lower(d.dimension_name), '[^a-z0-9]+', '_', 'g'), '_')
            ) AS feature_id,
            'industry' AS family,
            d.effective_date,
            d.fetched_at,
            d.value,
            d.raw_value,
            $monthly_cap AS cap_days,
            NULL::DATE AS start_date
        FROM silver.product_dimensions d
        LEFT JOIN industry_override io ON io.dimension_name = d.dimension_name
        WHERE d.dimension_type = 'industry'

        UNION ALL
        -- credit rating
        SELECT
            d.product_id,
            'credit_' || trim(regexp_replace(lower(d.dimension_name), '[^a-z0-9]+', '_', 'g'), '_')
                AS feature_id,
            'credit_rating' AS family,
            d.effective_date,
            d.fetched_at,
            d.value,
            d.raw_value,
            $monthly_cap AS cap_days,
            NULL::DATE AS start_date
        FROM silver.product_dimensions d
        WHERE d.dimension_type = 'credit_rating'

        UNION ALL
        -- maturity (Silver dimension_code slugs)
        SELECT
            d.product_id,
            d.dimension_code AS feature_id,
            'maturity' AS family,
            d.effective_date,
            d.fetched_at,
            d.value,
            d.raw_value,
            $monthly_cap AS cap_days,
            NULL::DATE AS start_date
        FROM silver.product_dimensions d
        WHERE d.dimension_type = 'maturity'
          AND d.dimension_code IS NOT NULL

        UNION ALL
        -- debt type clusters (sum weights of raw names in each cluster)
        SELECT
            d.product_id,
            dm.cluster AS feature_id,
            'debt_type' AS family,
            d.effective_date,
            MAX(d.fetched_at) AS fetched_at,
            SUM(d.value) AS value,
            CAST(SUM(d.value) AS VARCHAR) AS raw_value,
            $monthly_cap AS cap_days,
            NULL::DATE AS start_date
        FROM silver.product_dimensions d
        JOIN debt_map dm ON dm.raw_name = d.dimension_name
        WHERE d.dimension_type = 'debt_type'
        GROUP BY d.product_id, dm.cluster, d.effective_date

        UNION ALL
        -- theme children
        SELECT
            d.product_id,
            'theme_' || d.dimension_code AS feature_id,
            'theme' AS family,
            d.effective_date,
            d.fetched_at,
            d.value,
            d.raw_value,
            $monthly_cap AS cap_days,
            NULL::DATE AS start_date
        FROM silver.product_dimensions d
        WHERE d.dimension_type = 'theme'
          AND d.dimension_code IS NOT NULL

        UNION ALL
        -- theme parent rollups
        SELECT
            d.product_id,
            'theme_parent_' || t.parent_id AS feature_id,
            'theme' AS family,
            d.effective_date,
            MAX(d.fetched_at) AS fetched_at,
            SUM(d.value) AS value,
            CAST(SUM(d.value) AS VARCHAR) AS raw_value,
            $monthly_cap AS cap_days,
            NULL::DATE AS start_date
        FROM silver.product_dimensions d
        JOIN bronze.themes t ON t.theme_id = d.dimension_code
        WHERE d.dimension_type = 'theme'
          AND t.parent_id IS NOT NULL
          AND trim(t.parent_id) <> ''
        GROUP BY d.product_id, t.parent_id, d.effective_date

        UNION ALL
        -- style-box: always emit all 12 cells; style_box beats style_box_hist
        SELECT
            p.product_id,
            g.feature_id,
            'style_box' AS family,
            p.effective_date,
            p.fetched_at,
            CASE WHEN act.dimension_code IS NOT NULL THEN 1.0 ELSE 0.0 END AS value,
            CASE WHEN act.dimension_code IS NOT NULL THEN '1.0' ELSE '0.0' END AS raw_value,
            $tenure_cap AS cap_days,
            NULL::DATE AS start_date
        FROM (
            SELECT
                d.product_id,
                d.effective_date,
                MAX(d.fetched_at) AS fetched_at,
                CASE
                    WHEN bool_or(d.dimension_type = 'style_box') THEN 'style_box'
                    ELSE 'style_box_hist'
                END AS use_type
            FROM silver.product_dimensions d
            WHERE d.dimension_type IN ('style_box', 'style_box_hist')
            GROUP BY d.product_id, d.effective_date
        ) p
        CROSS JOIN style_grid g
        LEFT JOIN silver.product_dimensions act
            ON act.product_id = p.product_id
           AND act.effective_date = p.effective_date
           AND act.dimension_type = p.use_type
           AND act.dimension_code = g.code
        """,
        {
            "esg_cap": ESG_CAP_DAYS,
            "monthly_cap": MONTHLY_CAP_DAYS,
            "tenure_cap": TENURE_STYLE_CAP_DAYS,
            "fee_cap": MEDALIST_FEE_CAP_DAYS,
        },
    )
    conn.execute(
        """
        CREATE TEMP TABLE obs AS
        SELECT product_id, feature_id, family, effective_date, fetched_at, value, raw_value, cap_days, start_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY product_id, feature_id, effective_date
                    ORDER BY fetched_at DESC
                ) AS rn
            FROM obs_raw
        )
        WHERE rn = 1
        """
    )


def _apply_locf(conn) -> None:
    conn.execute(
        """
        CREATE TEMP TABLE family_dates AS
        SELECT
            product_id,
            family,
            effective_date,
            MAX(fetched_at) AS fetched_at,
            MAX(cap_days) AS cap_days
        FROM obs
        GROUP BY product_id, family, effective_date
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE spine AS
        SELECT p.product_id, m.as_of_date
        FROM (SELECT DISTINCT product_id FROM obs) p
        CROSS JOIN month_ends m
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE locf_family AS
        SELECT
            s.product_id,
            s.as_of_date,
            fd.family,
            fd.effective_date,
            fd.fetched_at
        FROM spine s
        INNER JOIN family_dates fd
            ON fd.product_id = s.product_id
           AND fd.effective_date <= s.as_of_date
           AND (
                fd.cap_days IS NULL
                OR date_diff('day', fd.effective_date, s.as_of_date) <= fd.cap_days
           )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY s.product_id, s.as_of_date, fd.family
            ORDER BY fd.effective_date DESC, fd.fetched_at DESC
        ) = 1
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE panel_locf AS
        SELECT
            lf.product_id,
            lf.as_of_date,
            o.feature_id,
            o.value,
            o.raw_value,
            o.effective_date,
            o.start_date,
            o.family
        FROM locf_family lf
        INNER JOIN obs o
            ON o.product_id = lf.product_id
           AND o.family = lf.family
           AND o.effective_date = lf.effective_date
        """
    )


def _synthesize(conn) -> None:
    conn.execute(
        """
        CREATE TEMP TABLE mstar_raw AS
        SELECT
            product_id,
            as_of_date,
            MAX(CASE WHEN feature_id = 'mstar_people_analyst' THEN value END) AS people_a,
            MAX(CASE WHEN feature_id = 'mstar_people_quant' THEN value END) AS people_q,
            MAX(CASE WHEN feature_id = 'mstar_process_analyst' THEN value END) AS process_a,
            MAX(CASE WHEN feature_id = 'mstar_process_quant' THEN value END) AS process_q,
            MAX(CASE WHEN feature_id = 'mstar_parent_analyst' THEN value END) AS parent_a,
            MAX(CASE WHEN feature_id = 'mstar_parent_quant' THEN value END) AS parent_q
        FROM panel_locf
        WHERE feature_id IN (
            'mstar_people_analyst', 'mstar_people_quant',
            'mstar_process_analyst', 'mstar_process_quant',
            'mstar_parent_analyst', 'mstar_parent_quant'
        )
        GROUP BY product_id, as_of_date
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE panel_synth AS
        -- Carry-through features; recompute tenure along the spine
        SELECT
            product_id,
            as_of_date,
            feature_id,
            CASE
                WHEN feature_id = 'manager_tenure_years' THEN
                    CASE
                        WHEN start_date IS NOT NULL THEN GREATEST(
                            0.0,
                            round(date_diff('day', start_date, as_of_date) / 365.25, 4)
                        )
                        ELSE value + date_diff('day', effective_date, as_of_date) / 365.25
                    END
                ELSE value
            END AS value
        FROM panel_locf
        WHERE feature_id NOT IN (
            'mstar_people_analyst', 'mstar_people_quant',
            'mstar_process_analyst', 'mstar_process_quant',
            'mstar_parent_analyst', 'mstar_parent_quant'
        )

        UNION ALL
        SELECT product_id, as_of_date, 'pillar_people', COALESCE(people_a, people_q)
        FROM mstar_raw
        WHERE people_a IS NOT NULL OR people_q IS NOT NULL

        UNION ALL
        SELECT product_id, as_of_date, 'pillar_people_is_quant',
               CASE WHEN people_a IS NOT NULL THEN 0.0 ELSE 1.0 END
        FROM mstar_raw
        WHERE people_a IS NOT NULL OR people_q IS NOT NULL

        UNION ALL
        SELECT product_id, as_of_date, 'pillar_process', COALESCE(process_a, process_q)
        FROM mstar_raw
        WHERE process_a IS NOT NULL OR process_q IS NOT NULL

        UNION ALL
        SELECT product_id, as_of_date, 'pillar_process_is_quant',
               CASE WHEN process_a IS NOT NULL THEN 0.0 ELSE 1.0 END
        FROM mstar_raw
        WHERE process_a IS NOT NULL OR process_q IS NOT NULL

        UNION ALL
        SELECT product_id, as_of_date, 'pillar_parent', COALESCE(parent_a, parent_q)
        FROM mstar_raw
        WHERE parent_a IS NOT NULL OR parent_q IS NOT NULL

        UNION ALL
        SELECT product_id, as_of_date, 'pillar_parent_is_quant',
               CASE WHEN parent_a IS NOT NULL THEN 0.0 ELSE 1.0 END
        FROM mstar_raw
        WHERE parent_a IS NOT NULL OR parent_q IS NOT NULL

        UNION ALL
        SELECT
            t.product_id,
            t.as_of_date,
            'management_fee_rate',
            t.value * m.value
        FROM panel_locf t
        JOIN panel_locf m
            ON t.product_id = m.product_id
           AND t.as_of_date = m.as_of_date
        WHERE t.feature_id = 'total_expense_ratio'
          AND m.feature_id = 'management_expense_ratio'

        UNION ALL
        SELECT
            product_id,
            as_of_date,
            'is_leveraged',
            CASE WHEN value > 1.0 THEN 1.0 ELSE 0.0 END
        FROM panel_locf
        WHERE feature_id = 'portfolio_top_10_concentration'
        """
    )


def _apply_theme_gate(conn) -> None:
    conn.execute(
        """
        CREATE TEMP TABLE panel_gated AS
        SELECT p.product_id, p.as_of_date, p.feature_id, p.value
        FROM panel_synth p
        LEFT JOIN (
            SELECT product_id, as_of_date, value AS coverage
            FROM panel_synth
            WHERE feature_id = 'theme_coverage'
        ) c
            ON c.product_id = p.product_id
           AND c.as_of_date = p.as_of_date
        WHERE NOT (
            p.feature_id LIKE 'theme_%'
            AND p.feature_id <> 'theme_coverage'
            AND (c.coverage IS NULL OR c.coverage < $gate)
        )
        """,
        {"gate": THEME_COVERAGE_GATE},
    )


def _winsorize(conn) -> None:
    conn.execute(
        """
        CREATE TEMP TABLE xs_bounds AS
        SELECT
            as_of_date,
            feature_id,
            quantile_cont(value, 0.01) AS q01,
            quantile_cont(value, 0.99) AS q99
        FROM panel_gated
        WHERE feature_id IN (SELECT feature_id FROM xs_winsor_features)
        GROUP BY as_of_date, feature_id
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE panel_winsor AS
        SELECT
            p.product_id,
            p.as_of_date,
            p.feature_id,
            CASE
                WHEN p.feature_id IN ('management_expense_ratio', 'non_management_expense_ratio')
                    THEN GREATEST($lo, LEAST($hi, p.value))
                WHEN b.q01 IS NOT NULL AND b.q99 IS NOT NULL
                    THEN GREATEST(b.q01, LEAST(b.q99, p.value))
                ELSE p.value
            END AS value
        FROM panel_gated p
        LEFT JOIN xs_bounds b
            ON b.as_of_date = p.as_of_date
           AND b.feature_id = p.feature_id
        """,
        {"lo": EXPENSE_RATIO_WINSOR[0], "hi": EXPENSE_RATIO_WINSOR[1]},
    )


def _attach_dominant_asset_class(conn) -> None:
    conn.execute(
        """
        CREATE TEMP TABLE dominant_ac AS
        SELECT
            product_id,
            as_of_date,
            CASE feature_id
                WHEN 'asset_class_equity' THEN 'Equity'
                WHEN 'asset_class_fixed_income' THEN 'Fixed Income'
                WHEN 'asset_class_cash' THEN 'Cash'
                WHEN 'asset_class_other' THEN 'Other'
            END AS asset_class
        FROM panel_winsor
        WHERE feature_id IN (
            'asset_class_equity',
            'asset_class_fixed_income',
            'asset_class_cash',
            'asset_class_other'
        )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY product_id, as_of_date
            ORDER BY value DESC, feature_id
        ) = 1
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE panel_final AS
        SELECT
            p.product_id,
            p.as_of_date,
            COALESCE(d.asset_class, 'Unknown') AS asset_class,
            p.feature_id,
            p.value
        FROM panel_winsor p
        LEFT JOIN dominant_ac d
            ON d.product_id = p.product_id
           AND d.as_of_date = p.as_of_date
        """
    )


def _assert_partition_of_unity(conn) -> None:
    violations = conn.execute(
        """
        SELECT product_id, as_of_date, SUM(value) AS weight_sum
        FROM panel_final
        WHERE feature_id IN (
            'asset_class_equity',
            'asset_class_fixed_income',
            'asset_class_cash',
            'asset_class_other'
        )
        GROUP BY product_id, as_of_date
        HAVING abs(SUM(value) - 1.0) > $tol
        ORDER BY product_id, as_of_date
        """,
        {"tol": PARTITION_TOLERANCE},
    ).fetchall()
    if not violations:
        return
    examples = "; ".join(f"(product_id={r[0]}, as_of_date={r[1]}, sum={r[2]})" for r in violations[:10])
    raise ValueError(
        f"asset_class weights do not sum to 1.0 ± {PARTITION_TOLERANCE} "
        f"for {len(violations)} product-date(s). Examples: {examples}"
    )
