CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;   -- reserved, empty for now
CREATE SCHEMA IF NOT EXISTS gold;     -- reserved, empty for now

-- Content-addressed store. Shared by snapshots, series, and snapshot_previews.
CREATE TABLE IF NOT EXISTS bronze.payload_blobs (
    hash    UBIGINT PRIMARY KEY,     -- xxhash.xxh3_64_intdigest(bytes, seed=0) of canonical bytes
    payload BLOB NOT NULL            -- zstd (level 3) compressed canonical bytes
);

-- Upserted, no raw crawl-page preservation. Source of truth for the product universe.
CREATE TABLE IF NOT EXISTS bronze.products (
    product_id              INTEGER PRIMARY KEY,
    product_type            VARCHAR,                -- "ETF" | "FUND" as returned
    symbol                  VARCHAR,
    exchange_id              VARCHAR,
    local_symbol             VARCHAR,
    name                     VARCHAR,
    under_conid              VARCHAR,
    isin                     VARCHAR,
    cusip                    VARCHAR,
    currency                 VARCHAR,
    country                  VARCHAR,
    is_primary_exchange_id   BOOLEAN,                -- from "isPrimeExchId": "T"/"F"
    is_new_product           BOOLEAN,                -- from "isNewPdt": "T"/"F"
    assoc_entity_id           VARCHAR,
    fc_conid                  VARCHAR,
    created_at                TIMESTAMP NOT NULL,     -- set once, never overwritten
    updated_at                TIMESTAMP NOT NULL      -- refreshed on every upsert
);

-- Upserted, one row per product_id. The one hash-referencing table that isn't insert-only.
CREATE TABLE IF NOT EXISTS bronze.snapshot_previews (
    product_id   INTEGER PRIMARY KEY REFERENCES bronze.products(product_id),
    hash         UBIGINT NOT NULL REFERENCES bronze.payload_blobs(hash),
    updated_at   TIMESTAMP NOT NULL
);

-- Insert-only. One row per discrete-date fetch of a snapshot-shaped endpoint.
CREATE SEQUENCE IF NOT EXISTS bronze.snapshots_id_seq;
CREATE TABLE IF NOT EXISTS bronze.snapshots (
    snapshot_id  INTEGER PRIMARY KEY DEFAULT nextval('bronze.snapshots_id_seq'),
    hash         UBIGINT NOT NULL REFERENCES bronze.payload_blobs(hash),
    product_id   INTEGER NOT NULL REFERENCES bronze.products(product_id),
    url_prefix   VARCHAR NOT NULL,   -- literal, invariant portion of the request URL (identifies which endpoint)
    url_slug     VARCHAR,             -- the fully-resolved remainder, exactly as fetched
    fetched_at   TIMESTAMP NOT NULL
);

-- Insert-only. One row per fetch of a series-shaped endpoint (incremental or full).
CREATE SEQUENCE IF NOT EXISTS bronze.series_id_seq;
CREATE TABLE IF NOT EXISTS bronze.series (
    series_id    INTEGER PRIMARY KEY DEFAULT nextval('bronze.series_id_seq'),
    hash         UBIGINT NOT NULL REFERENCES bronze.payload_blobs(hash),
    product_id   INTEGER NOT NULL REFERENCES bronze.products(product_id),
    url_prefix   VARCHAR NOT NULL,      -- All before the first dynamic substring
    url_slug     VARCHAR,               -- All after and including the first dynamic substring
    first_date   TIMESTAMP NOT NULL,
    last_date    TIMESTAMP NOT NULL,
    fetched_at   TIMESTAMP NOT NULL
);

-- Upserted, no raw crawl-page preservation. Global theme taxonomy (definitions/hierarchy)
CREATE TABLE IF NOT EXISTS bronze.themes (
    theme_id     VARCHAR PRIMARY KEY,     -- IBKR's "key" (UUID string)
    num_id       INTEGER,                  -- IBKR's "numId"
    name         VARCHAR,
    parent_id    VARCHAR REFERENCES bronze.themes(theme_id),  -- NULL for root/"parents" entries
    created_at   TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL
);
