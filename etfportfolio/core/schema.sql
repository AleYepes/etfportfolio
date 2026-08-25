CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;   -- reserved, empty for now
CREATE SCHEMA IF NOT EXISTS gold;     -- reserved, empty for now

-- Content-addressed store. Shared by snapshots, series, and summaries.
CREATE TABLE IF NOT EXISTS bronze.payload_blobs (
    hash    VARCHAR PRIMARY KEY,      -- xxhash.xxh128_hexdigest() of canonical bytes
    payload BLOB NOT NULL             -- zstd (level 3) compressed canonical bytes
);

-- Source of truth for the conid universe. Upserted, no raw crawl-page preservation.
CREATE TABLE IF NOT EXISTS bronze.products (
    conid                  INTEGER PRIMARY KEY,
    symbol                 VARCHAR,
    exchange_id             VARCHAR,
    description             VARCHAR,
    isin                    VARCHAR,
    cusip                   VARCHAR,
    currency                VARCHAR,
    country                 VARCHAR,
    is_primary_exchange_id  BOOLEAN,
    first_seen_at            TIMESTAMP NOT NULL,   -- set once, never overwritten
    updated_at               TIMESTAMP NOT NULL    -- refreshed on every upsert
);

-- Insert-only. One row per discrete-date fetch of a snapshot-shaped endpoint.
CREATE SEQUENCE IF NOT EXISTS bronze.snapshots_id_seq;
CREATE TABLE IF NOT EXISTS bronze.snapshots (
    id           INTEGER PRIMARY KEY DEFAULT nextval('bronze.snapshots_id_seq'),
    hash         VARCHAR NOT NULL REFERENCES bronze.payload_blobs(hash),
    conid        INTEGER NOT NULL REFERENCES bronze.products(conid),
    url_path     VARCHAR NOT NULL,
    query_string VARCHAR,
    fetched_at   TIMESTAMP NOT NULL
);

-- Insert-only. One row per fetch of a series-shaped endpoint (incremental or full).
CREATE SEQUENCE IF NOT EXISTS bronze.series_id_seq;
CREATE TABLE IF NOT EXISTS bronze.series (
    id           INTEGER PRIMARY KEY DEFAULT nextval('bronze.series_id_seq'),
    hash         VARCHAR NOT NULL REFERENCES bronze.payload_blobs(hash),
    conid        INTEGER NOT NULL REFERENCES bronze.products(conid),
    url_path     VARCHAR NOT NULL,
    query_string VARCHAR,              -- period=MAX here ⇒ full refetch (inferred, not flagged)
    first_date   TIMESTAMP NOT NULL,
    last_date    TIMESTAMP NOT NULL,
    fetched_at   TIMESTAMP NOT NULL
);

-- Upserted. One row per conid. The one deliberate exception to "bronze is insert-only" —
-- triggers blob GC on change. Never used for downstream analytics.
CREATE TABLE IF NOT EXISTS bronze.summaries (
    conid        INTEGER PRIMARY KEY REFERENCES bronze.products(conid),
    hash         VARCHAR NOT NULL REFERENCES bronze.payload_blobs(hash),
    url_path     VARCHAR NOT NULL,
    query_string VARCHAR,
    updated_at   TIMESTAMP NOT NULL
);