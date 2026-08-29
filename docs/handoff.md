# Handoff: Ingestion Architecture and Series Pipeline Specification

This document records the architectural decisions, trade-offs, and implementation specifications agreed upon for the ETF portfolio ingestion pipeline. It serves as the single source of truth for implementing the revised ingestion modules.

---

## 1. Problem Context

The ingestion pipeline previously failed on the sentiment series endpoint, returning empty payloads for all products. In addition, the existing series fetching and storage logic needed review:
- The web portal price chart endpoint proved imprecise, supporting only discrete relative periods (1M, 3M, 6M, 9M, 1Y, etc.) that include unfinalized intraday bars.
- Sentiment payloads contained redundant price keys that duplicate the price dataset.
- Session login performed redundant account reconciliation and double-writes to `.env`.
- Several products in the crawled universe failed when queried against official Interactive Brokers APIs.

---

## 2. Settled Decisions and Rationale

### Decision 1: Sentiment Endpoint Query Formatting
- **Endpoint Route**: `/tws.proxy/sma/request?type=search&conid={product_id}&from={from_date}&to={to_date}&bar_size=1D&lang=en`
- **Date Formatting**: `YYYY-MM-DD%2000:00`. The SMA backend requires a time string (even though with `bar_size=1D` it aggregates to daily bars). Passing `YYYY-MM-DD` without a time component causes the endpoint to return empty arrays.
- **Cold Start Lookback**: `from="1990-01-01%2000:00"`.
- **Incremental Fetch Range**: `from=(last_date - 7 days)` and `to=(today_utc - 1 day)`.
- **Query Parameter Preservation**: Keep `&lang=en` on the URL template to match other portal endpoints.

### Decision 2: Dedicated Preprocessing and Cutoff Filter
- **Intraday Bar Cutoff**: Both price and sentiment data must strictly exclude the current day. Any data point with `timestamp >= start_of_today_utc` (00:00:00 UTC) is discarded before hashing or storage.
- **Sentiment Payload Trimming**: Strip all price-related keys (`open`, `high`, `low`, `close`, `price`, `price_change`, `price_change_p`) from each entry in `payload["sentiment"]`.
- **Retained Sentiment Metrics**: `datetime`, `sscore`, `svolatility`, `sdispersion`, `svscore`, `sbuzz`, `svolume`, `sdelta`, `smean`. Numeric precision is preserved as raw numbers without rounding.
- **Order of Operations**: Strip price keys -> Filter timestamps -> Canonicalize -> Overlap check -> Content-address (hash and zstd compress) -> Store in `bronze.payload_blobs` and record lineage in `bronze.series`.

### Decision 3: Overlap Window and Health Check Validation
- **Overlap Span**: 7 calendar days (`last_date - 7 days`). This accounts for weekends, market holidays, and data gaps.
- **Date-for-Date Matching**: Overlapping timestamps between the incoming preprocessed slice and stored history must match date-for-date. If expected dates within the overlap window are missing or shifted, the fetch is flagged as a `date_mismatch`.
- **Value Matching**: For matching timestamps, metric values are compared using `math.isclose(v1, v2, rel_tol=1e-4, abs_tol=1e-4)`. A difference beyond tolerance is flagged as a `value_mismatch`.
- **Handling Mismatches**:
  - Log a warning specifying the product and mismatch type (`date_mismatch` or `value_mismatch`).
  - Discard the incremental slice.
  - Trigger a full cold-start refetch (`1990-01-01%2000:00` for sentiment, `30 Y` for price).
  - For sentiment, insert the full refetch into `bronze.series` with `fetch_type='date_mismatch'` or `'value_mismatch'`. Old rows remain as historical vestiges.
  - For price, delete prior rows for that `product_id` from `bronze.prices` and insert the full series fresh.
- **Lineage Column**: Add `fetch_type VARCHAR NOT NULL` to `bronze.series` with permitted values: `'initial'`, `'incremental'`, `'value_mismatch'`, `'date_mismatch'`, and `'other'`.

### Decision 4: Price Ingestion via `ib_async` and IB Gateway
- **Portal Price Endpoint Dropped**: The web portal endpoint `/tws.proxy/fundamentals/mf_performance_chart/` is replaced with `ib_async` calling the official TWS/Gateway API (`reqHistoricalDataAsync`).
- **Rationale and Trade-offs**:
  - The web portal price endpoint is rigid. It only accepts coarse periods (1M, 3M, 6M, 9M, 1Y, etc.), does not take explicit date bounds, and does not provide split-adjusted prices.
  - `ib_async` supports exact end timestamps (`endDateTime="YYYYMMDD 23:59:59 UTC"`), exact durations (`durationStr=f"{(today - last_date).days + 7} D"`), and split/dividend-adjusted closing prices (`whatToShow="ADJUSTED_LAST"`).
  - All other endpoints (holdings, ratios, profile, lipper, mstar, esg, theme weights, sentiment) remain on the web portal HTTP layer because the official TWS API does not provide equivalent ETF fundamental, ESG, or sentiment datasets.
- **Relational Storage (`bronze.prices`)**:
  - Rather than storing synthetic JSON blobs in `bronze.payload_blobs` and `bronze.series`, price bars are stored directly in a relational DuckDB table (`bronze.prices`).
  - This avoids fake URL slugs (like `url_prefix="ib.reqHistoricalDataAsync()"`), provides columnar compression, and enables SQL queries without JSON unpacking.

### Decision 5: Contract Qualification and `silver.products` View
- **Qualification Step (`reqContractDetailsAsync`)**:
  - Querying the 22.5k products from the public crawl revealed that 45 products are unrecognized or deprecated by the official API.
  - Running `reqContractDetailsAsync` on all `bronze.products` identifies valid contracts and gathers official metadata.
- **Storage in `bronze.contracts`**: Store all returned `ContractDetails` fields flattened into `bronze.contracts` (including `created_at` and `updated_at`).
- **`silver.products` View**:
  - Expose a view that inner-joins `bronze.products p` and `bronze.contracts c`.
  - Coalesce attributes, prioritizing `bronze.contracts` over `bronze.products`:
    - `product_id`: `c.product_id`
    - `name`: `COALESCE(c.long_name, p.name)`
    - `symbol`: `COALESCE(c.symbol, p.symbol)`
    - `local_symbol`: `COALESCE(c.local_symbol, p.local_symbol)`
    - `exchange_id`: `c.exchange_id`
    - `primary_exchange_id`: `c.primary_exchange_id`
    - `currency`: `COALESCE(c.currency, p.currency)`
    - `isin`: `COALESCE(c.isin, p.isin)`
    - `created_at`: `COALESCE(c.created_at, p.created_at)`
    - `updated_at`: `GREATEST(c.updated_at, p.updated_at)`
  - All downstream price, snapshot, and sentiment ingestion stages target `silver.products`.

### Decision 6: Concurrency and Gateway Pacing
- **Batching Point of Contention**: Batching contracts with `reqContractDetailsAsync(*chunk)` in groups of 100 caused memory overflow (exceeding 4GB) and took 90 minutes.
- **Solution**: Processing individual contracts using `asyncio.Semaphore(1)` completed all 22.5k products in 12 minutes without memory pressure.
- **Client IDs**:
  - `clientId=1` for contract qualification (`main.py ingest contracts`).
  - `clientId=2` for historical price series (`main.py ingest prices`).
- **Connection Management**: Wrap `ib_async.IB` in an async context manager that connects to host `127.0.0.1`, port `4001` (configurable in `pyproject.toml`), and disconnects upon completion.
- **Graceful Offline Error**: If IB Gateway is closed or port 4001 is unreachable, catch `ConnectionRefusedError` and print a direct instruction to open IB Gateway and enable API connections.

### Decision 7: Freshness and the `--force` Flag
- **24-Hour Freshness Window**:
  - `contracts`: Skip qualification if `product_id` exists in `bronze.contracts` and was updated within the last 24 hours.
  - `prices`: Skip if `bronze.prices` has data up to yesterday (`MAX(date) >= today - 1 day`).
  - `details`: Skip gated snapshots if the landing preview hash matches stored preview.
- **Override**: Passing `--force` on any command bypasses freshness checks and forces complete re-execution.

### Decision 8: Session Double-Persistence Fix
- `login()` and `ensure_session()` are streamlined so account discovery and credential writing run once, removing duplicate `.env` file writes and redundant log entries.

---

## 3. Target Database Schema (`etfportfolio/core/schema.sql`)

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Content-addressed blob store for raw JSON payloads (snapshots and sentiment)
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
    updated_at              TIMESTAMP NOT NULL
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
    long_name               VARCHAR,
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

-- Verified ETF product universe
CREATE VIEW IF NOT EXISTS silver.products AS
SELECT
    p.product_id,
    COALESCE(c.long_name, p.name) AS name,
    COALESCE(c.symbol, p.symbol) AS symbol,
    COALESCE(c.local_symbol, p.local_symbol) AS local_symbol,
    c.exchange_id,
    c.primary_exchange_id,
    COALESCE(c.currency, p.currency) AS currency,
    COALESCE(c.isin, p.isin) AS isin,
    COALESCE(c.created_at, p.created_at) AS created_at,
    GREATEST(c.updated_at, p.updated_at) AS updated_at
FROM bronze.products p
JOIN bronze.contracts c ON p.product_id = c.product_id;

-- Snapshot landing previews for change detection
CREATE TABLE IF NOT EXISTS bronze.snapshot_previews (
    product_id   INTEGER PRIMARY KEY REFERENCES bronze.products(product_id),
    hash         UBIGINT NOT NULL REFERENCES bronze.payload_blobs(hash),
    updated_at   TIMESTAMP NOT NULL
);

-- Snapshot endpoint lineage
CREATE SEQUENCE IF NOT EXISTS bronze.snapshots_id_seq;
CREATE TABLE IF NOT EXISTS bronze.snapshots (
    snapshot_id  INTEGER PRIMARY KEY DEFAULT nextval('bronze.snapshots_id_seq'),
    hash         UBIGINT NOT NULL REFERENCES bronze.payload_blobs(hash),
    product_id   INTEGER NOT NULL REFERENCES bronze.products(product_id),
    url_prefix   VARCHAR NOT NULL,
    url_slug     VARCHAR,
    fetched_at   TIMESTAMP NOT NULL
);

-- Web portal series lineage (sentiment)
CREATE SEQUENCE IF NOT EXISTS bronze.series_id_seq;
CREATE TABLE IF NOT EXISTS bronze.series (
    series_id    INTEGER PRIMARY KEY DEFAULT nextval('bronze.series_id_seq'),
    hash         UBIGINT NOT NULL REFERENCES bronze.payload_blobs(hash),
    product_id   INTEGER NOT NULL REFERENCES bronze.products(product_id),
    url_prefix   VARCHAR NOT NULL,
    url_slug     VARCHAR,
    first_date   TIMESTAMP NOT NULL,
    last_date    TIMESTAMP NOT NULL,
    fetch_type   VARCHAR NOT NULL, -- 'initial', 'incremental', 'value_mismatch', 'date_mismatch', 'other'
    fetched_at   TIMESTAMP NOT NULL
);

-- Official historical daily prices
CREATE TABLE IF NOT EXISTS bronze.prices (
    product_id   INTEGER NOT NULL REFERENCES bronze.products(product_id),
    date         DATE NOT NULL,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE NOT NULL, -- ADJUSTED_LAST
    volume       DOUBLE,
    average      DOUBLE,
    bar_count    INTEGER,
    updated_at   TIMESTAMP NOT NULL,
    PRIMARY KEY (product_id, date)
);

-- Global theme taxonomy
CREATE TABLE IF NOT EXISTS bronze.themes (
    theme_id     VARCHAR PRIMARY KEY,
    num_id       INTEGER,
    name         VARCHAR,
    parent_id    VARCHAR REFERENCES bronze.themes(theme_id),
    created_at   TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL
);
```

---

## 4. Ingestion Pipeline Workflow and CLI

```
Phase 1: Products (Public web crawl) -> bronze.products
Phase 2: Contracts (IB Gateway clientId=1) -> bronze.contracts & silver.products
Phase 3: Prices (IB Gateway clientId=2) -> bronze.prices
Phase 4: Session (Interactive portal browser login)
Phase 5: Themes (Authenticated portal sync) -> bronze.themes
Phase 6: Details (Snapshots + Sentiment for silver.products) -> bronze.snapshots & bronze.series
```

### CLI Surface
```bash
python -m etfportfolio.main ingest products [--force]
python -m etfportfolio.main ingest contracts [--force]
python -m etfportfolio.main ingest prices [--force]
python -m etfportfolio.main ingest session
python -m etfportfolio.main ingest themes [--force]
python -m etfportfolio.main ingest details [--force] [--product-ids 8335,756733] [--limit 10]
python -m etfportfolio.main ingest [--force]
```

---

## 5. Implementation Checklist for Incoming Agent

1. **Configuration (`config.py` & `pyproject.toml`)**:
   - Add `ib_gateway_host` (default `"127.0.0.1"`), `ib_gateway_port` (default `4001`), `ib_gateway_client_id` (default `1`), `ib_gateway_timeout` (default `60.0`).
2. **Database Schema (`core/schema.sql`)**:
   - Add `bronze.contracts`, `bronze.prices`, and `silver.products` view.
   - Add `fetch_type` column to `bronze.series`.
   - Remove existing database file if present (no migration code required).
3. **Gateway Ingestion Modules**:
   - Create `etfportfolio/ingestion/gateway.py` with the async context manager for `ib_async.IB` connections and error handling for unreachable port 4001.
   - Create `etfportfolio/ingestion/contracts.py` implementing single-semaphore contract qualification and upsert to `bronze.contracts`.
   - Create `etfportfolio/ingestion/prices.py` implementing daily bar fetching (`ADJUSTED_LAST`), overlap health checks (`date_mismatch`, `value_mismatch`), prior-row deletion on mismatch, and upsert to `bronze.prices`.
4. **Web Portal Ingestion Modules**:
   - Update `endpoints.py`: Remove `mf_performance_chart`. Keep `sentiment` with `{from_date}` and `{to_date}`.
   - Update `series.py`: Add `preprocess_sentiment()` to strip price keys and intraday bars. Implement date-for-date overlap checking, `date_mismatch`/`value_mismatch` refetching, and `fetch_type` logging.
   - Update `session.py`: Remove redundant account discovery and duplicate `.env` writes.
   - Update `details.py` and `products.py`: Target `silver.products` for detail resolution.
5. **Pipeline Orchestrator (`pipeline.py`)**:
   - Wire all 6 phases into `Ingest` CLI class with `--force` flag support.
6. **Tests**:
   - Rewrite unit tests in `tests/` to validate new schemas, preprocessing, overlap validation, and gateway ingestion logic.
