CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS cold_storage;


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
    date         TIMESTAMP NOT NULL,
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