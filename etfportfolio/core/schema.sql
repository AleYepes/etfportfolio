CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;   -- reserved

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

-- Snapshot landing previews
CREATE TABLE IF NOT EXISTS bronze.snapshot_previews (
    product_id   INTEGER PRIMARY KEY REFERENCES bronze.products(product_id),
    hash         UBIGINT NOT NULL REFERENCES bronze.payload_blobs(hash),
    updated_at   TIMESTAMP NOT NULL
);

-- Snapshot lineage
CREATE SEQUENCE IF NOT EXISTS bronze.snapshots_id_seq;
CREATE TABLE IF NOT EXISTS bronze.snapshots (
    snapshot_id  INTEGER PRIMARY KEY DEFAULT nextval('bronze.snapshots_id_seq'),
    hash         UBIGINT NOT NULL REFERENCES bronze.payload_blobs(hash),
    product_id   INTEGER NOT NULL REFERENCES bronze.products(product_id),
    url_prefix   VARCHAR NOT NULL,
    url_slug     VARCHAR,
    fetched_at   TIMESTAMP NOT NULL
);

-- Historical daily prices (IB ADJUSTED_LAST)
CREATE TABLE IF NOT EXISTS bronze.prices (
    product_id   INTEGER NOT NULL REFERENCES bronze.products(product_id),
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

-- Sentiment daily metrics
CREATE TABLE IF NOT EXISTS bronze.sentiment (
    product_id   INTEGER NOT NULL REFERENCES bronze.products(product_id),
    date         TIMESTAMP NOT NULL,
    svolatility  DOUBLE,
    sdispersion  DOUBLE,
    svscore      DOUBLE,
    sbuzz        DOUBLE,
    svolume      DOUBLE,
    sdelta       DOUBLE,
    sscore       DOUBLE,
    smean        DOUBLE,
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

-- Verified ETF product universe view
CREATE OR REPLACE VIEW silver.products AS
SELECT
    p.product_id,
    COALESCE(c.name, p.name) AS name,
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