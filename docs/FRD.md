# Functional Requirements Document (FRD)

**Title:** Transition to Quantitative Analysis: Silver Observations Data Model & Upstream Snapshot Changelog Refactor  
**Status:** Approved for Implementation  
**Target Modules:** `etfportfolio.observations`, `etfportfolio.core`, `etfportfolio.ingestion`, and migration scripts  

---

## 1. Executive Summary & Context

The ETF Portfolio project is transitioning from bronze raw ingestion into quantitative factor modeling. The complete analytical pipeline is structured as:
$$\text{Ingestion (Bronze)} \longrightarrow \text{Observations (Silver)} \longrightarrow \text{Monthly Panel (Gold)} \longrightarrow \text{Factor Regressions} \longrightarrow \text{Efficient Frontier}$$

To construct the canonical **Silver Observations** layer, raw JSON snapshot payloads stored in `bronze.payload_blobs` must be decompressed, parsed, normalized, and materialized into structured relational tables.

Prior to this initiative:
1. `bronze.snapshots` functioned as an append-only log, appending a new row on every ingestion run even when the underlying payload had not changed. In an embedded DuckDB store, this caused table bloat and mixed snapshot lineage with freshness caching.
2. Ingestion coupled snapshot fetching directly to qualified contracts in `silver.products`. In quantitative analysis, restricting fundamental metric extraction to currently qualified contracts introduces survivorship and look-ahead bias: historical snapshots for funds that traded in past years but were later delisted or failed current contract qualification were excluded.

This document serves as the complete, authoritative specification for:
1. **Upstream Bronze Changelog Refactor**: Converting `bronze.snapshots` into a state-transition changelog (`created_at`, `last_checked_at`), relaxing foreign key constraints, and migrating existing historical records.
2. **Decoupled Architecture**: Allowing `observations` to extract all non-empty historical snapshots in Bronze regardless of current contract qualification, leaving price-metric alignment to the downstream panel layer.
3. **Canonical Silver Observations Model**: Partitioned by data geometry across fund-level scalar metrics (`silver.metric_observations`), portfolio allocation distributions (`silver.portfolio_allocations`), and thematic factor exposures (`silver.theme_exposures`).
4. **Extraction & Normalization Engine**: A synchronous, resilient, batch-chunked extraction package (`etfportfolio.observations`) executed via `main.py observations [--force]`.

---

## 2. Scope & Boundaries

### In Scope
- **Bronze Schema Refactoring**: Relaxing foreign key constraints on Bronze append tables and adding `created_at` / `last_checked_at` to `bronze.snapshots`.
- **Historical Snapshot Migration**: A standalone migration script (`scripts/migrate_snapshots_changelog.py`) that collapses contiguous identical snapshot rows into single interval records.
- **Synchronous Database Connection**: Adding a re-entrant `db_connection()` context manager to `etfportfolio.core.db` for post-ingestion synchronous pipelines.
- **Silver Physical Schemas**: Creating `silver.metric_observations`, `silver.portfolio_allocations`, `silver.theme_exposures`, and `silver.processed_snapshots`.
- **Extraction Engine for 7 Snapshot Endpoints**:
  1. `ratios`: Valuation multiples, growth rates, profitability, fixed-income metrics, and factor Z-scores.
  2. `profile`: Management/non-management expense ratios, total expense ratio (TER), passive/active classification, and Morningstar style box coordinates.
  3. `esg`: Coverage ratio, composite Refinitiv ESG scores, pillar scores, and sub-pillar metrics.
  4. `mstar`: Medalist ratings, analyst/quantitative pillar scores, star ratings, and sustainability globe ratings.
  5. `lipper`: Grouping multi-universe consensus ratings by date, averaging same-date universes, and extracting trailing horizons (`overall`, `3yr`, `5yr`, `10yr`).
  6. `holdings`: Asset class, currency, country, sector/industry, maturity, and credit rating distributions (from `debtor`). Standardizing weights to decimals and summing duplicate categories.
  7. `theme_weights`: Thematic factor weights and rank-adjusted weights linked to `bronze.themes`.
- **CLI Command**: Exposing `main.py observations [--force]` via Python Fire.

### Explicitly Out of Scope
- **Top 10 Company Positions (`holdings.top_10`)**: Omitted to prevent high-cardinality, sparse factor matrices.
- **Debt Instrument Types (`holdings.debt_type`)**: Omitted. Profiling shows over 92 sparse debt types (e.g. `CorporateMediumTermNotes`, `SovereignBond`) that occur in only a fraction of funds and would result almost entirely in zeros in panel features.
- **Granular Prospectus Schedules (`profile.reports`)**: Omitted; summary fees (`Total_Expense_Ratio`, `expenses_allocation`) are captured via `profile.fund_and_profile`.
- **Qualitative Prose & HTML (`mstar.commentary`, `profile.objective`, `profile.themes`)**: Narrative text and raw HTML tags are excluded from numeric factor analysis.
- **Downstream Panel & Factor Modeling**: Monthly LOCF imputation, multicollinearity base-category dropping, return regressions, and portfolio optimization belong to subsequent modules.
- **Granular CLI Flags on Observations**: No `--limit` or `--product-ids` flags (only `--force`). Observations always processes all unparsed snapshots in Bronze.

---

## 3. Architecture & Settled Design Principles

### Decision 1: Bronze Changelog Storage Model
* **Decision**: Refactor `bronze.snapshots` from an append-only log to a state-transition changelog with `created_at` and `last_checked_at`.
* **Rationale**: Payload blobs are already content-addressed and deduplicated in `bronze.payload_blobs`. Recording identical rows on daily crawls creates table bloat and slows down queries. Updating `last_checked_at` in place when content is unchanged preserves exact state intervals (`created_at` = first appearance, `last_checked_at` = last verified unchanged) while cutting metadata growth by >90%.

### Decision 2: Decoupling Observations from `silver.products` (Survivorship Bias Elimination)
* **Decision**: 
  - `ingestion/pipeline.py` (live crawl) continues targeting `silver.products` to avoid web-portal 404 spam for known dead contracts.
  - `observations/pipeline.py` extracts **all** non-empty snapshots in `bronze.snapshots` regardless of whether the contract currently exists in `silver.products`.
* **Rationale**: Survivorship bias must be eliminated from factor construction. If a fund was active and emitted fundamental snapshots in 2023 but delisted in 2024, its historical fundamentals are valid data points. Joining prices and fundamentals is deferred to the Gold Monthly Panel layer.

### Decision 3: Fail-Fast Domain Validation vs. Expected Empty No-Ops
* **Decision**: 
  - Empty `{}` payloads (produced by ingestion for 404s/absent widgets) are valid no-ops: stamp them in `silver.processed_snapshots` and emit zero rows.
  - Unexpected schema deviations in *actively extracted domains* (unknown rating strings, unparseable dates, non-numeric values where floats are expected, or missing required arrays) raise a hard exception to immediately halt the batch transaction.
  - Changes in *unextracted sections* (e.g., vendor benchmark statistics, marketing banners, new commentary subsections) are ignored.
* **Rationale**: IBKR portal schemas change infrequently. Hard exceptions provide immediate developer feedback (DX) to update extractors before bad data corrupts factor models.

### Decision 4: Lossless Vector Retention in Silver vs. Econometric Multicollinearity
* **Decision**: Retain all allocation categories (including placeholders like `'Unassigned'`, `'Other'`, `'Not Rated'`) in `silver.portfolio_allocations`. Sum weights within a snapshot if duplicate category labels exist to satisfy the primary key constraint.
* **Rationale**: In portfolio mathematics, weights sum to 1.0 ($\sum w_i = 1.0$). Dropping a baseline category to prevent exact multicollinearity is essential for OLS regression, but belongs strictly in the **Gold / Factor Modeling** phase when constructing design matrices. Dropping categories in Silver would destroy the integrity of allocation distributions, invalidating Herfindahl concentration indices and data-quality audits.

### Decision 5: Lipper Multi-Universe Consensus & Multi-Date Backfill
* **Decision**:
  - Group Lipper universes by `as_of_date`. For each distinct date, average numeric ratings across universes (e.g. averaging Singapore and Japan share-class ratings).
  - Extract observation rows for **all** distinct `as_of_date`s present in the payload.
* **Rationale**: Averaging same-date universes produces a robust consensus rating across share classes without arbitrary selection. Extracting historical filing dates allows earlier dates to backfill the panel, while downstream LOCF naturally carries values forward until updated.

### Decision 6: Morningstar Per-Pillar Assessment Dates & Directional Alignment
* **Decision**:
  - `effective_date`: Use the pillar's own `publish_date` if present; fall back to top-level `as_of_date`, then snapshot `created_at.date()`.
  - Normalization: Map all ordinal rating scales to a uniform directional float scale ($1.0 \dots 5.0$, where $5.0$ is best):
    - Medalist / Quantitative Rating: Gold=5.0, Silver=4.0, Bronze=3.0, Neutral=2.0, Negative=1.0.
    - Pillars (`people`, `process`, `parent`, etc.): High=5.0, Above_Average=4.0, Average=3.0, Below_Average=2.0, Low=1.0.
    - Star Rating (`morningstar_rating`): Parse `"1"`..`"5"` to `1.0`..`5.0`.
    - Sustainability Rating: 5 globes / High=5.0 down to 1 globe / Low=1.0.
* **Rationale**: Pillars are reviewed on independent schedules by analysts. Capturing per-pillar dates reflects true economic timing, while uniform 1.0–5.0 scaling allows factor models to ingest Morningstar features directly.

### Decision 7: Raw Float Fidelity in `ratios`
* **Decision**: Store all metric values in `ratios` as raw `float(value)` without selective percentage division (e.g. P/E = 38.15, Dividend Yield = 2.5, ROE = 14.2, Z-Score = -0.18).
* **Rationale**: Raw payloads lack explicit unit tags. Whitelisting percentage tags is brittle. Downstream factor modeling passes all features through standard scaling (e.g. `MinMaxScaler()` or Z-scoring), rendering arbitrary 100-divisions redundant.

### Decision 8: Snapshot-Centric Batching (500 Snapshots per Transaction)
* **Decision**: Batch execution in chunks of 500 snapshots per database transaction, tracked by `core.progress.progress_bar(unit="snapshot")`.
* **Rationale**: Committing transactions per-product across 22,500 products would cause 22,500 disk fsync operations, spending minutes waiting on disk I/O. 500-snapshot batches execute in milliseconds, integrate cleanly with `silver.processed_snapshots` (keyed by `snapshot_id`), and allow long runs to be safely interrupted and resumed.

---

## 4. Database Schema Specifications (`etfportfolio/core/schema.sql`)

The entire database schema must be updated to:

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS cold_storage;

-- =====================================================================
-- BRONZE LAYER (Unconstrained Appends & Lookup Tables)
-- =====================================================================

-- Content-addressed store (snapshots only)
CREATE TABLE IF NOT EXISTS bronze.payload_blobs (
    hash    UBIGINT PRIMARY KEY,
    payload BLOB NOT NULL
);

-- Public portal product catalog
CREATE TABLE IF NOT EXISTS bronze.products (
    product_id              INTEGER PRIMARY KEY,
    product_type            VARCHAR,
    symbol                  VARCHAR,
    exchange_id             VARCHAR,
    local_symbol            VARCHAR,
    name                    VARCHAR,
    under_conid             VARCHAR,
    isin                    VARCHAR,
    cusip                   VARCHAR,
    currency                VARCHAR,
    country                 VARCHAR,
    is_primary_exchange_id  BOOLEAN,
    is_new_product          BOOLEAN,
    assoc_entity_id         VARCHAR,
    fc_conid                VARCHAR,
    created_at              TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP NOT NULL,
    last_checked_at         TIMESTAMP
);

-- Official IB Gateway contract details
CREATE TABLE IF NOT EXISTS bronze.contracts (
    product_id              INTEGER PRIMARY KEY,
    sec_type                VARCHAR,
    symbol                  VARCHAR,
    exchange_id             VARCHAR,
    primary_exchange_id     VARCHAR,
    currency                VARCHAR,
    local_symbol            VARCHAR,
    trading_class           VARCHAR,
    market_name             VARCHAR,
    min_tick                DOUBLE,
    order_types             VARCHAR,
    valid_exchanges         VARCHAR,
    price_magnifier         DOUBLE,
    under_conid             INTEGER,
    name                    VARCHAR,
    contract_month          VARCHAR,
    industry                VARCHAR,
    category                VARCHAR,
    subcategory             VARCHAR,
    time_zone_id            VARCHAR,
    trading_hours           VARCHAR,
    liquid_hours            VARCHAR,
    ev_rule                 VARCHAR,
    ev_multiplier           DOUBLE,
    md_size_multiplier      INTEGER,
    agg_group               INTEGER,
    under_symbol            VARCHAR,
    under_sec_type          VARCHAR,
    market_rule_ids         VARCHAR,
    real_expiration_date    VARCHAR,
    last_trade_time         VARCHAR,
    stock_type              VARCHAR,
    min_size                DOUBLE,
    size_increment          DOUBLE,
    suggested_size_increment DOUBLE,
    cusip                   VARCHAR,
    ratings                 VARCHAR,
    desc_append             VARCHAR,
    bond_type               VARCHAR,
    coupon_type             VARCHAR,
    callable                BOOLEAN,
    putable                 BOOLEAN,
    coupon                  DOUBLE,
    convertible             BOOLEAN,
    maturity                VARCHAR,
    issue_date              VARCHAR,
    next_option_date        VARCHAR,
    next_option_type        VARCHAR,
    next_option_partial     BOOLEAN,
    notes                   VARCHAR,
    isin                    VARCHAR,
    created_at              TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP NOT NULL
);

-- Snapshot landing previews (FK constraints relaxed)
CREATE TABLE IF NOT EXISTS bronze.snapshot_previews (
    product_id      INTEGER PRIMARY KEY,
    hash            UBIGINT NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    last_checked_at TIMESTAMP
);

-- Snapshot state changelog (FK constraints relaxed; state-transition intervals)
CREATE SEQUENCE IF NOT EXISTS bronze.snapshots_id_seq;
CREATE TABLE IF NOT EXISTS bronze.snapshots (
    snapshot_id     INTEGER PRIMARY KEY DEFAULT nextval('bronze.snapshots_id_seq'),
    hash            UBIGINT NOT NULL,
    product_id      INTEGER NOT NULL,
    url_prefix      VARCHAR NOT NULL,
    url_slug        VARCHAR,
    created_at      TIMESTAMP NOT NULL,
    last_checked_at TIMESTAMP NOT NULL
);

-- Historical daily prices (FK dropped; compound PK retained for ON CONFLICT upsert)
CREATE TABLE IF NOT EXISTS bronze.prices (
    product_id   INTEGER NOT NULL,
    date         TIMESTAMP NOT NULL,  -- UTC midnight
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE NOT NULL,
    volume       DOUBLE,
    average      DOUBLE,
    bar_count    INTEGER,
    updated_at   TIMESTAMP NOT NULL,
    PRIMARY KEY (product_id, date)
);

-- Cold storage archives
CREATE TABLE IF NOT EXISTS cold_storage.prices (
    product_id   INTEGER NOT NULL,
    run_id       TIMESTAMP NOT NULL,
    date         TIMESTAMP NOT NULL,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE,
    volume       DOUBLE,
    average      DOUBLE,
    bar_count    INTEGER,
    reason       VARCHAR,
    PRIMARY KEY (product_id, run_id, date)
);

-- Global theme taxonomy
CREATE TABLE IF NOT EXISTS bronze.themes (
    theme_id     VARCHAR PRIMARY KEY,
    num_id       INTEGER,
    name         VARCHAR,
    parent_id    VARCHAR,
    created_at   TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL,
    last_checked_at TIMESTAMP
);

-- =====================================================================
-- SILVER LAYER (Structured Observations & Canonical Entities)
-- =====================================================================

-- Verified ETF product universe view
CREATE OR REPLACE VIEW silver.products AS
SELECT
    p.product_id,
    COALESCE(c.name, p.name) AS name,
    COALESCE(c.symbol, p.symbol) AS symbol,
    COALESCE(c.local_symbol, p.local_symbol) AS local_symbol,
    COALESCE(c.sec_type, p.product_type, 'STK') AS sec_type,
    COALESCE(c.exchange_id, 'SMART') AS exchange_id,
    COALESCE(c.primary_exchange_id, p.exchange_id) AS primary_exchange_id,
    COALESCE(c.currency, p.currency) AS currency,
    c.trading_class,
    c.valid_exchanges,
    c.stock_type,
    COALESCE(c.isin, p.isin) AS isin,
    COALESCE(c.cusip, p.cusip) AS cusip,
    c.time_zone_id,
    c.min_tick,
    COALESCE(c.created_at, p.created_at) AS created_at,
    GREATEST(c.updated_at, p.updated_at) AS updated_at
FROM bronze.products p
JOIN bronze.contracts c ON p.product_id = c.product_id;

-- 1. Fund-level scalar metrics (ratios, fees, ESG scores, numeric ratings)
CREATE TABLE IF NOT EXISTS silver.metric_observations (
    product_id     INTEGER NOT NULL,
    source         VARCHAR NOT NULL,  -- 'ratios', 'mstar', 'esg', 'profile', 'lipper'
    metric_id      VARCHAR NOT NULL,  -- e.g. 'price_earnings', 'total_expense_ratio', 'total_return_3yr'
    effective_date DATE NOT NULL,
    fetched_at     TIMESTAMP NOT NULL,
    value          DOUBLE NOT NULL,
    PRIMARY KEY (product_id, source, metric_id, effective_date)
);

-- 2. Portfolio allocation distributions (from holdings)
CREATE TABLE IF NOT EXISTS silver.portfolio_allocations (
    product_id      INTEGER NOT NULL,
    breakdown_type  VARCHAR NOT NULL,  -- 'asset_class', 'currency', 'country', 'industry', 'maturity', 'credit_rating'
    item_name       VARCHAR NOT NULL,  -- e.g. 'United States', 'Real Estate', 'USD', 'AAA', 'Not Rated'
    item_code       VARCHAR,           -- ISO code when available ('US', 'USD'), or same as item_name for credit ratings
    effective_date  DATE NOT NULL,
    fetched_at      TIMESTAMP NOT NULL,
    weight          DOUBLE NOT NULL,   -- Standardized decimal (e.g. 0.9976)
    PRIMARY KEY (product_id, breakdown_type, item_name, effective_date)
);

-- 3. Thematic factor exposures (from theme_weights)
CREATE TABLE IF NOT EXISTS silver.theme_exposures (
    product_id           INTEGER NOT NULL,
    theme_id             VARCHAR NOT NULL,  -- Canonical UUID matching bronze.themes(theme_id)
    effective_date       DATE NOT NULL,
    fetched_at           TIMESTAMP NOT NULL,
    weight               DOUBLE NOT NULL,   -- Standardized decimal (e.g. 0.0841)
    rank_adjusted_weight DOUBLE NOT NULL,   -- Standardized decimal
    PRIMARY KEY (product_id, theme_id, effective_date)
);

-- 4. Observations processing watermark
CREATE TABLE IF NOT EXISTS silver.processed_snapshots (
    snapshot_id  INTEGER PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL
);
```

---

## 5. Upstream Ingestion Changelog Refactor

### 5.1 Refactor `etfportfolio/ingestion/snapshots.py`
Update `store_snapshot` to record state transitions:
```python
def store_snapshot(
    conn: duckdb.DuckDBPyConnection,
    product_id: int,
    url_prefix: str,
    url_slug: str,
    payload: Any,
    fetched_at: datetime | None = None,
) -> int:
    """Content-addresses a snapshot payload, stores the blob, and updates or appends to the changelog."""
    digest, compressed = content_address(payload)
    timestamp = fetched_at or datetime.now(UTC).replace(tzinfo=None)

    conn.execute("BEGIN TRANSACTION")
    try:
        store_blob(conn, digest, compressed)

        # Check latest snapshot for this product and endpoint
        row = conn.execute(
            """
            SELECT snapshot_id, hash
            FROM bronze.snapshots
            WHERE product_id = $1 AND url_prefix = $2
            ORDER BY snapshot_id DESC
            LIMIT 1
            """,
            [product_id, url_prefix],
        ).fetchone()

        if row and row[1] == digest:
            # Hash unchanged: bump last_checked_at
            conn.execute(
                """
                UPDATE bronze.snapshots
                SET last_checked_at = $1
                WHERE snapshot_id = $2
                """,
                [timestamp, row[0]],
            )
        else:
            # New or changed payload: insert new changelog record
            conn.execute(
                """
                INSERT INTO bronze.snapshots (hash, product_id, url_prefix, url_slug, created_at, last_checked_at)
                VALUES ($1, $2, $3, $4, $5, $5)
                """,
                [digest, product_id, url_prefix, url_slug, timestamp],
            )

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return digest
```

### 5.2 Refactor `etfportfolio/ingestion/details.py`
Update `load_endpoint_freshness_cache` to query `MAX(last_checked_at)`:
```python
def load_endpoint_freshness_cache(conn: duckdb.DuckDBPyConnection) -> dict[tuple[int, str], datetime]:
    """Load (product_id, url_prefix) -> MAX(last_checked_at) from bronze.snapshots."""
    rows = conn.execute(
        """
        SELECT product_id, url_prefix, MAX(last_checked_at)
        FROM bronze.snapshots
        GROUP BY product_id, url_prefix
        """
    ).fetchall()
    return {(row[0], row[1]): row[2] for row in rows}
```

---

## 6. Snapshot Migration Script (`scripts/migrate_snapshots_changelog.py`)

Implement the one-time historical migration script. It detects if `last_checked_at` already exists, and if not, collapses contiguous identical blocks into intervals:

```python
"""
One-time migration script:
Converts bronze.snapshots from an append-only log to a state-transition changelog.
Collapses contiguous identical (product_id, url_prefix, hash) rows into single
records with created_at = MIN(fetched_at) and last_checked_at = MAX(fetched_at).
"""
import duckdb
from etfportfolio.core.config import settings
from etfportfolio.core.db import apply_schema

def migrate() -> None:
    conn = duckdb.connect(settings.db_path)
    try:
        cols = [col[1] for col in conn.execute("PRAGMA table_info('bronze.snapshots')").fetchall()]
        if "last_checked_at" in cols:
            print("bronze.snapshots already contains last_checked_at. No migration needed.")
            apply_schema(conn)
            return

        print("Migrating bronze.snapshots to state-transition changelog...")
        conn.execute("BEGIN TRANSACTION")

        # 1. Collapse contiguous blocks of identical hashes using window functions
        conn.execute("""
            CREATE TEMP TABLE snapshots_collapsed AS
            WITH marked AS (
                SELECT
                    snapshot_id,
                    hash,
                    product_id,
                    url_prefix,
                    url_slug,
                    fetched_at,
                    CASE
                        WHEN LAG(hash) OVER (PARTITION BY product_id, url_prefix ORDER BY snapshot_id) = hash THEN 0
                        ELSE 1
                    END AS is_new_block
                FROM bronze.snapshots
            ),
            grouped AS (
                SELECT
                    *,
                    SUM(is_new_block) OVER (PARTITION BY product_id, url_prefix ORDER BY snapshot_id) AS block_id
                FROM marked
            )
            SELECT
                MIN(snapshot_id) AS snapshot_id,
                hash,
                product_id,
                url_prefix,
                FIRST(url_slug) AS url_slug,
                MIN(fetched_at) AS created_at,
                MAX(fetched_at) AS last_checked_at
            FROM grouped
            GROUP BY product_id, url_prefix, block_id, hash
            ORDER BY snapshot_id;
        """)

        old_count = conn.execute("SELECT COUNT(*) FROM bronze.snapshots").fetchone()[0]
        new_count = conn.execute("SELECT COUNT(*) FROM snapshots_collapsed").fetchone()[0]

        # 2. Swap table into place
        conn.execute("DROP TABLE bronze.snapshots")
        conn.execute("""
            CREATE TABLE bronze.snapshots (
                snapshot_id     INTEGER PRIMARY KEY,
                hash            UBIGINT NOT NULL,
                product_id      INTEGER NOT NULL,
                url_prefix      VARCHAR NOT NULL,
                url_slug        VARCHAR,
                created_at      TIMESTAMP NOT NULL,
                last_checked_at TIMESTAMP NOT NULL
            )
        """)
        conn.execute("""
            INSERT INTO bronze.snapshots
            SELECT snapshot_id, hash, product_id, url_prefix, url_slug, created_at, last_checked_at
            FROM snapshots_collapsed
        """)
        conn.execute("DROP TABLE snapshots_collapsed")

        # 3. Synchronize sequence with max snapshot_id
        max_id = conn.execute("SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM bronze.snapshots").fetchone()[0]
        conn.execute("DROP SEQUENCE IF EXISTS bronze.snapshots_id_seq")
        conn.execute(f"CREATE SEQUENCE bronze.snapshots_id_seq START {max_id}")

        # 4. Re-apply schema to create all new silver tables
        apply_schema(conn)

        conn.execute("COMMIT")
        print(f"Migration complete: {old_count} raw snapshot rows collapsed into {new_count} changelog rows.")
    except Exception as e:
        conn.execute("ROLLBACK")
        print(f"Migration failed, rolled back: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
```

---

## 7. Synchronous Database Context Manager (`etfportfolio/core/db.py`)

Add `db_connection()` to `etfportfolio/core/db.py` for synchronous post-ingestion pipelines:

```python
from collections.abc import Iterator
from contextlib import contextmanager

@contextmanager
def db_connection(db_path: str | None = None) -> Iterator[duckdb.DuckDBPyConnection]:
    """Context manager yielding a DuckDB connection with schema applied.

    Used by synchronous post-ingestion pipelines (observations, panel, factors).
    Transactions must be explicitly controlled by the caller.
    """
    target_path = db_path or settings.db_path
    Path(target_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(target_path)
    apply_schema(conn)
    try:
        yield conn
    finally:
        conn.close()
```

---

## 8. Extraction & Normalization Specifications (`etfportfolio/observations/`)

### 8.1 Package Organization
```
etfportfolio/observations/
├── __init__.py
├── cli.py             # Fire CLI exposure: ObservationsCLI
├── extractors.py      # Pure parsing functions for the 7 endpoints
├── pipeline.py        # Batch chunking (500), progress bar, transaction runner
└── utils.py           # Date parsers, string sanitizers, credit rating cleaning
```

### 8.2 Common Helpers (`observations/utils.py`)

1. **Date Normalization (`parse_effective_date`)**:
   - Accepts `int`, `float`, `str`, or `None`, along with `fallback_date: date`.
   - **Epoch ms (`int` / `float`)**: If $>0$, `datetime.fromtimestamp(ms / 1000.0, tz=UTC).date()`. If $\le 0$ (e.g. stub reports with `0`), return `fallback_date`.
   - **`"YYYYMMDD"` (`str`)**: Parse via `datetime.strptime(s, "%Y%m%d").date()`.
   - **`"YYYY-MM-DD"` or `"YYYY/MM/DD"` (`str`)**: Parse standard ISO or slash formats.
   - If parsing fails or input is `None`, return `fallback_date`.

2. **Credit Rating Cleaning (`clean_credit_rating`)**:
   - Strip leading prefixes `"% Quality/"` or `"% Quality "`.
   - Maps raw `name` strings to clean rating tiers:
     - `"% Quality/AAA"` $\rightarrow$ `'AAA'`
     - `"% Quality/BBB"` $\rightarrow$ `'BBB'`
     - `"% Quality/Below B"` $\rightarrow$ `'Below B'`
     - `"% Quality Not Rated"` $\rightarrow$ `'Not Rated'`
     - `"% Quality Not Available"` $\rightarrow$ `'Not Available'`
   - Return clean string for both `item_name` and `item_code`.

3. **String Sanitization**:
   - Lowercase snake_case conversion for metric IDs (e.g. `re.sub(r'[^a-z0-9]+', '_', tag.strip().lower()).strip('_')`).

### 8.3 Extractor Contracts (`observations/extractors.py`)

All extractors are pure functions taking `(product_id: int, payload: dict, snapshot_created_at: datetime)`. Empty `{}` payloads return empty lists immediately.

#### 1. `ratios` (`/tws.proxy/fundamentals/mf_ratios_fundamentals/`)
- **Target Table**: `silver.metric_observations`
- **Source**: `'ratios'`
- **Effective Date**: `parse_effective_date(payload.get("as_of_date"), fallback_date=snapshot_created_at.date())`.
- **Logic**:
  - Iterate arrays: `dividend`, `financials`, `fixed_income`, `ratios`, `zscore`.
  - For each item, read `value = item.get("value")`. Skip if `value is None`.
  - Metric ID: `item["name_tag"].strip().lower()`. (Replace non-alphanumeric chars with `_`).
  - Cast value: `float(value)` (preserved as raw float fidelity).
  - Discard vendor benchmarks (`avg`, `min`, `max`, `percentile`, `vs`).
  - Emits: `(product_id, 'ratios', metric_id, effective_date, snapshot_created_at, value)`.

#### 2. `profile` (`/tws.proxy/fundamentals/mf_profile_and_fees/`)
- **Target Table**: `silver.metric_observations`
- **Source**: `'profile'`
- **Effective Date**: `snapshot_created_at.date()`.
- **Logic**:
  1. `expenses_allocation`:
     - Iterate items: `name = item.get("name")`, `ratio = item.get("ratio")`. If `ratio is not None`:
       - `name == "Management Expenses"` $\rightarrow$ `metric_id = 'management_expense_ratio'`, `value = float(ratio)`.
       - `name == "Non-Management Expenses"` $\rightarrow$ `metric_id = 'non_management_expense_ratio'`, `value = float(ratio)`.
  2. `fund_and_profile`:
     - Iterate key-value items:
       - `Total_Expense_Ratio`: Strip `%`, divide by 100.0 (e.g. `"0.32%"` $\rightarrow$ `0.0032`). Metric ID: `'total_expense_ratio'`.
       - `Management_Approach`: `"Passive"` $\rightarrow$ `1.0`, `"Active"` $\rightarrow$ `0.0`. Metric ID: `'is_passive'`.
  3. `mstar` (Style Box):
     - If `mstar` dict exists and `selected` is a non-empty `[[y, x]]`:
       - `metric_id = 'mstar_style_size'`, `value = float(y)` (Large=3.0, Mid=2.0, Small=1.0, Multi=0.0).
       - `metric_id = 'mstar_style_value'`, `value = float(x)` (Value=1.0, Core=2.0, Growth=3.0).

#### 3. `esg` (`/tws.proxy/impact/esg/`)
- **Target Table**: `silver.metric_observations`
- **Source**: `'esg'`
- **Effective Date**: `parse_effective_date(payload.get("asOfDate"), fallback_date=snapshot_created_at.date())` (note camelCase `asOfDate`).
- **Logic**:
  - If `coverage` is present and not `None`:
    - `metric_id = 'esg_coverage'`, `value = float(payload["coverage"])`.
  - Traverse `content` tree:
    - For each root node:
      - `metric_id = node["name"].strip().lower()`, `value = float(node["value"])`.
      - For each child in `node.get("children", [])`:
        - `metric_id = child["name"].strip().lower()`, `value = float(child["value"])`.

#### 4. `mstar` (`/tws.proxy/mstar/fund/detail?conid=`)
- **Target Table**: `silver.metric_observations`
- **Source**: `'mstar'`
- **Effective Date**: Per-pillar: `parse_effective_date(pillar.get("publish_date"), fallback_date=top_level_date)`, where `top_level_date = parse_effective_date(payload.get("as_of_date"), fallback_date=snapshot_created_at.date())`.
- **Logic**:
  - Ordinal Mappings:
    - Medalist / Quantitative Rating (`medalist_rating`, `quantitative_rating`):
      `{"gold": 5.0, "silver": 4.0, "bronze": 3.0, "neutral": 2.0, "negative": 1.0}`.
    - Pillars (`people`, `process`, `parent`, `q_people`, `q_process`, `q_parent`):
      `{"high": 5.0, "above_average": 4.0, "average": 3.0, "below_average": 2.0, "low": 1.0}`.
    - Star Rating (`morningstar_rating`): Parse `"1"`..`"5"` as `1.0`..`5.0`.
    - Sustainability Rating (`sustainability_rating`): Parse integer `"1"`..`"5"` as `1.0`..`5.0`, or map `"high"` $\rightarrow$ 5.0, `"above_average"` $\rightarrow$ 4.0, `"average"` $\rightarrow$ 3.0, `"below_average"` $\rightarrow$ 2.0, `"low"` $\rightarrow$ 1.0.
  - Process `summary` array:
    - For each pillar item:
      - `pillar_id = pillar.get("id")`. If not present, raise `ValueError` (fail-fast).
      - `raw_val = pillar.get("value")`. If `raw_val is None` or `raw_val in ("", "-")`, skip.
      - Map `raw_val` using the appropriate dictionary above. If an unrecognized non-empty string is encountered on an extracted rating, raise `ValueError` (fail-fast).
      - `metric_id = pillar_id.strip().lower()`.

#### 5. `lipper` (`/tws.proxy/fundamentals/mf_lip_ratings/`)
- **Target Table**: `silver.metric_observations`
- **Source**: `'lipper'`
- **Logic**:
  - Read `universes = payload.get("universes", [])`.
  - Group universes by `as_of_date` (integer epoch ms).
  - For each `as_of_date, group_universes` pair:
    - `eff_date = parse_effective_date(as_of_date, fallback_date=snapshot_created_at.date())`.
    - Horizon suffix mapping: `{"overall": "_overall", "3_year": "_3yr", "5_year": "_5yr", "10_year": "_10yr"}`.
    - Collect scores for each horizon:
      - For each horizon key in `("overall", "3_year", "5_year", "10_year")`:
        - Metric accumulator: `dict[str, list[float]]` (maps `metric_id` $\rightarrow$ list of values across universes in this date group).
        - For `u` in `group_universes`:
          - For item in `u.get(horizon, [])`:
            - `tag = item.get("name_tag")`. If not present, continue.
            - `val = item.get("rating", {}).get("value")`. If `val is None`, continue.
            - `metric_id = f"{tag.strip().lower()}{suffix}"`.
            - Accumulate `float(val)`.
    - Compute the arithmetic mean for each accumulated `metric_id`:
      - `value = sum(values) / len(values)`.
      - Emits: `(product_id, 'lipper', metric_id, eff_date, snapshot_created_at, value)`.

#### 6. `holdings` (`/tws.proxy/fundamentals/mf_holdings/`)
- **Target Table**: `silver.portfolio_allocations`
- **Effective Date**: `parse_effective_date(payload.get("as_of_date"), fallback_date=snapshot_created_at.date())`.
- **Logic**:
  - Breakdown mapping:
    - `allocation_self` $\rightarrow$ `'asset_class'`
    - `currency` $\rightarrow$ `'currency'`
    - `investor_country` $\rightarrow$ `'country'`
    - `industry` $\rightarrow$ `'industry'`
    - `maturity` $\rightarrow$ `'maturity'`
    - `debtor` $\rightarrow$ `'credit_rating'`
  - For each breakdown array:
    - Local aggregator: `dict[tuple[str, str], dict]`: key is `(breakdown_type, item_name)`.
    - For item in array:
      - `raw_name = item.get("name")`. If not `raw_name`, skip.
      - `weight_val = item.get("weight")`. If `weight_val is None`, skip.
      - Standardized weight: `weight = float(weight_val) / 100.0`.
      - Format `item_name` and `item_code`:
        - If `breakdown_type == 'credit_rating'`:
          - `item_name = clean_credit_rating(raw_name)`.
          - `item_code = item_name`.
        - If `breakdown_type == 'currency'`:
          - `item_name = 'Unassigned'` if raw_name.strip() in ("<No Currency>", "<NoCurrency>") else raw_name.strip().
          - `item_code = item.get("code")`.
        - If `breakdown_type == 'country'`:
          - `item_name = raw_name.strip()`.
          - `item_code = item.get("country_code")`.
        - Otherwise:
          - `item_name = raw_name.strip()`.
          - `item_code = None`.
      - **Deduplication Summation**:
        - Key: `(breakdown_type, item_name)`.
        - If key exists: `agg[key]["weight"] += weight`.
        - Else: `agg[key] = {"item_code": item_code, "weight": weight}`.
    - For `(b_type, name), data` in aggregator:
      - Emits: `(product_id, b_type, name, data["item_code"], effective_date, snapshot_created_at, data["weight"])`.

#### 7. `theme_weights` (`/tws.proxy/knowledge-graph/ui/fund?conid=`)
- **Target Table**: `silver.theme_exposures`
- **Effective Date**: `snapshot_created_at.date()`.
- **Logic**:
  - Iterate `themes = payload.get("themes", [])`.
  - For each theme:
    - `theme_id = str(theme.get("key"))`. If not `theme_id`, continue.
    - `weight = float(theme["weight"])`.
    - `rank_adjusted_weight = float(theme["rank_adjusted_weight"])`.
    - Emits: `(product_id, theme_id, effective_date, snapshot_created_at, weight, rank_adjusted_weight)`.

---

## 9. Execution Engine & Watermark Runner (`etfportfolio/observations/pipeline.py`)

### 9.1 Processing Algorithm
1. Open connection via `with db_connection() as conn:`.
2. **If `force=True`**:
   ```sql
   BEGIN TRANSACTION;
   DELETE FROM silver.processed_snapshots;
   DELETE FROM silver.metric_observations;
   DELETE FROM silver.portfolio_allocations;
   DELETE FROM silver.theme_exposures;
   COMMIT;
   ```
3. **Query Unprocessed Snapshots**:
   ```sql
   SELECT
       s.snapshot_id,
       s.product_id,
       s.url_prefix,
       s.created_at,
       b.payload
   FROM bronze.snapshots s
   JOIN bronze.payload_blobs b ON s.hash = b.hash
   LEFT JOIN silver.processed_snapshots p ON s.snapshot_id = p.snapshot_id
   WHERE p.snapshot_id IS NULL
   ORDER BY s.snapshot_id ASC;
   ```
4. If no pending snapshots: log `"All snapshots up to date."` and exit cleanly.
5. **Batch Processing Loop**:
   - Chunk pending records into batches of 500.
   - Initialize `with progress_bar(len(pending_snapshots), desc="Observations", unit="snapshot") as bar:`.
   - For each chunk:
     - Initialize accumulators:
       - `metrics_rows: list[tuple]`
       - `allocations_rows: list[tuple]`
       - `themes_rows: list[tuple]`
       - `processed_ids: list[tuple[int]]`
     - For snapshot in chunk:
       - Decompress payload: `data = decompress_payload(snapshot.payload)`.
       - If `data == {}` (empty stub):
         - Record `processed_ids.append((snapshot.snapshot_id,))` (valid no-op; no metric rows).
         - Continue.
       - Route by `snapshot.url_prefix` to the appropriate extractor in `observations.extractors`.
       - Append resulting rows to respective accumulators.
       - Record `processed_ids.append((snapshot.snapshot_id,))`.
     - **Execute Single Transaction for Chunk**:
       ```sql
       BEGIN TRANSACTION;
       ```
       - Batch upsert metrics:
         ```sql
         INSERT INTO silver.metric_observations (product_id, source, metric_id, effective_date, fetched_at, value)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (product_id, source, metric_id, effective_date) DO UPDATE SET
             value = EXCLUDED.value,
             fetched_at = EXCLUDED.fetched_at;
         ```
       - Batch upsert allocations:
         ```sql
         INSERT INTO silver.portfolio_allocations (product_id, breakdown_type, item_name, item_code, effective_date, fetched_at, weight)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (product_id, breakdown_type, item_name, effective_date) DO UPDATE SET
             weight = EXCLUDED.weight,
             item_code = EXCLUDED.item_code,
             fetched_at = EXCLUDED.fetched_at;
         ```
       - Batch upsert theme exposures:
         ```sql
         INSERT INTO silver.theme_exposures (product_id, theme_id, effective_date, fetched_at, weight, rank_adjusted_weight)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (product_id, theme_id, effective_date) DO UPDATE SET
             weight = EXCLUDED.weight,
             rank_adjusted_weight = EXCLUDED.rank_adjusted_weight,
             fetched_at = EXCLUDED.fetched_at;
         ```
       - Batch insert watermark stamps:
         ```sql
         INSERT INTO silver.processed_snapshots (snapshot_id, processed_at)
         VALUES ($1, CURRENT_TIMESTAMP);
         ```
       - Commit transaction:
         ```sql
         COMMIT;
         ```
     - Update progress:
       - `bar.update(len(chunk))`
       - `bar.set_postfix_str(f"snap {chunk[-1].snapshot_id}")`

---

## 10. CLI Surface (`main.py` & `observations/cli.py`)

### 10.1 `etfportfolio/observations/cli.py`
```python
from etfportfolio.core.logging import console
from etfportfolio.observations.pipeline import run_observations

class ObservationsCLI:
    """CLI surface for the observations phase: `main.py observations [--force]`."""

    def __call__(self, force: bool = False) -> None:
        console.info("=== Starting Silver Observations Extraction ===")
        processed_count = run_observations(force=force)
        console.info(f"=== Observations Complete. Processed {processed_count} snapshots. ===")

cli = ObservationsCLI()
```

### 10.2 Update `main.py`
```python
import sys
import fire
from etfportfolio.core.logging import configure_logging
from etfportfolio.ingestion import pipeline as ingest_pipeline
from etfportfolio.observations import cli as obs_cli

def main() -> None:
    argv = sys.argv[1:]
    verbose = False
    if "-v" in argv or "--verbose" in argv:
        verbose = True
        argv = [a for a in argv if a not in ("-v", "--verbose")]

    configure_logging(verbose=verbose)
    fire.Fire({
        "ingest": ingest_pipeline.cli,
        "observations": obs_cli.cli,
    }, command=argv)

if __name__ == "__main__":
    main()
```

---

## 11. Verification & Acceptance Criteria

1. **Schema & DDL**:
   - `core/schema.sql` applies cleanly to fresh DuckDB databases without foreign key violation errors.
   - `bronze.snapshots` contains `created_at` and `last_checked_at`.
   - `silver.metric_observations`, `silver.portfolio_allocations`, `silver.theme_exposures`, and `silver.processed_snapshots` exist with primary keys as specified.
2. **Upstream Ingestion Changelog Test**:
   - Ingestion of an unchanged snapshot updates `last_checked_at` on the existing row and does not create a new row.
   - Ingestion of a changed snapshot creates a new row with a new `snapshot_id`.
3. **Migration Script**:
   - Running `python scripts/migrate_snapshots_changelog.py` collapses contiguous identical rows and creates `bronze.snapshots_id_seq` cleanly without data loss.
4. **Extraction Accuracy & Data Quality**:
   - `silver.metric_observations.value` contains only valid floats; all Morningstar Medalist and pillar scores map to $1.0 \dots 5.0$.
   - `silver.portfolio_allocations.weight` contains standardized decimals ($0.0 \dots 1.0$), with `'credit_rating'` extracted from `debtor` and stripped of prefixes.
   - Duplicate categories within a single snapshot's breakdown array are summed into a single row without triggering primary key collisions.
   - Lipper same-date universes are averaged into clean consensus scores, and horizon tags use `_overall`, `_3yr`, `_5yr`, `_10yr`.
5. **CLI & Idempotency**:
   - `python main.py observations` processes all unparsed snapshots and updates the watermark.
   - An immediate second run processes 0 snapshots and outputs `"All snapshots up to date."`.
   - `python main.py observations --force` truncates Silver tables and completely repopulates them cleanly.