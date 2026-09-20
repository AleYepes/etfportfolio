CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS cold_storage;


CREATE TABLE IF NOT EXISTS bronze.payload_blobs (
    hash    UBIGINT PRIMARY KEY,
    payload BLOB NOT NULL
);

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

CREATE TABLE IF NOT EXISTS bronze.snapshot_previews (
    product_id      INTEGER PRIMARY KEY,
    hash            UBIGINT NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    last_checked_at TIMESTAMP
);

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

CREATE TABLE IF NOT EXISTS bronze.price_status (
    product_id      INTEGER PRIMARY KEY,
    last_checked_at TIMESTAMP NOT NULL,
    status          VARCHAR NOT NULL,
    error_message   VARCHAR
);

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

CREATE TABLE IF NOT EXISTS bronze.themes (
    theme_id        VARCHAR PRIMARY KEY,
    num_id          INTEGER,
    name            VARCHAR,
    parent_id       VARCHAR,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    last_checked_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.product_metrics (
    product_id             INTEGER NOT NULL,
    source                 VARCHAR NOT NULL,
    metric_id              VARCHAR NOT NULL,
    effective_date         DATE NOT NULL,
    effective_date_source  VARCHAR NOT NULL,
    fetched_at             TIMESTAMP WITH TIME ZONE NOT NULL,
    value                  DOUBLE NOT NULL,
    raw_value              VARCHAR NOT NULL,
    currency               VARCHAR,
    PRIMARY KEY (product_id, source, metric_id, effective_date)
);

-- Idempotent add for databases created before the currency column existed.
ALTER TABLE silver.product_metrics ADD COLUMN IF NOT EXISTS currency VARCHAR;

CREATE TABLE IF NOT EXISTS silver.product_dimensions (
    product_id             INTEGER NOT NULL,
    dimension_type         VARCHAR NOT NULL,
    dimension_name         VARCHAR NOT NULL,
    dimension_code         VARCHAR,
    effective_date         DATE NOT NULL,
    effective_date_source  VARCHAR NOT NULL,
    fetched_at             TIMESTAMP WITH TIME ZONE NOT NULL,
    value                  DOUBLE NOT NULL,
    raw_value              VARCHAR NOT NULL,
    PRIMARY KEY (product_id, dimension_type, dimension_name, effective_date)
);

CREATE TABLE IF NOT EXISTS silver.processed_snapshots (
    snapshot_id            BIGINT PRIMARY KEY,
    processed_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.monthly_panel (
    product_id   INTEGER NOT NULL,
    as_of_date   DATE NOT NULL,
    asset_class  VARCHAR NOT NULL,
    feature_id   VARCHAR NOT NULL,
    value        DOUBLE NOT NULL,
    PRIMARY KEY (product_id, as_of_date, feature_id)
);

CREATE OR REPLACE VIEW silver.products AS
SELECT
    c.product_id,
    c.name,
    c.symbol,
    c.local_symbol,
    c.sec_type,
    COALESCE(c.exchange_id, 'SMART') AS exchange_id,
    c.primary_exchange_id,
    c.currency,
    c.trading_class,
    c.valid_exchanges,
    c.stock_type,
    c.isin,
    c.cusip,
    c.time_zone_id,
    c.min_tick,
    c.created_at,
    c.updated_at
FROM bronze.contracts c
WHERE c.product_id IN (SELECT DISTINCT product_id FROM bronze.prices)
  AND (
      c.product_id IN (SELECT DISTINCT product_id FROM silver.product_metrics)
      OR c.product_id IN (SELECT DISTINCT product_id FROM silver.product_dimensions)
  );