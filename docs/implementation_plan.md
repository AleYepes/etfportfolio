### Round 6 – Final Settlement

The frontier is now empty; all branches of the design tree have been settled. Below is the consolidated design summary for your confirmation before implementation begins.

---

## ✅ Settled Design Summary

### 1. Database Schema (new DB from scratch)

**Keep existing tables:** `bronze.products`, `bronze.snapshot_previews`, `bronze.snapshots`, `bronze.payload_blobs`, `bronze.themes` (blobs remain for snapshots only).

**Add new tables:**

```sql
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
    name                    VARCHAR,      -- was long_name, now name
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

-- Historical daily prices (IB `ADJUSTED_LAST`)
CREATE TABLE IF NOT EXISTS bronze.prices (
    product_id   INTEGER NOT NULL REFERENCES bronze.products(product_id),
    date         TIMESTAMP NOT NULL,   -- UTC midnight
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE NOT NULL,      -- ADJUSTED_LAST
    volume       DOUBLE,
    average      DOUBLE,
    bar_count    INTEGER,
    updated_at   TIMESTAMP NOT NULL,
    PRIMARY KEY (product_id, date)
);

-- Sentiment daily metrics (stripped of price keys)
CREATE TABLE IF NOT EXISTS bronze.sentiment (
    product_id   INTEGER NOT NULL REFERENCES bronze.products(product_id),
    date         TIMESTAMP NOT NULL,   -- UTC midnight
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

-- Cold storage archives (for replaced series)
CREATE SCHEMA IF NOT EXISTS cold_storage;

CREATE TABLE cold_storage.prices (
    product_id   INTEGER NOT NULL,
    run_id       TIMESTAMP NOT NULL,   -- max(updated_at) of archived rows
    date         TIMESTAMP NOT NULL,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE,
    volume       DOUBLE,
    average      DOUBLE,
    bar_count    INTEGER,
    PRIMARY KEY (product_id, run_id, date)
);

CREATE TABLE cold_storage.sentiment (
    product_id   INTEGER NOT NULL,
    run_id       TIMESTAMP NOT NULL,
    date         TIMESTAMP NOT NULL,
    svolatility  DOUBLE,
    sdispersion  DOUBLE,
    svscore      DOUBLE,
    sbuzz        DOUBLE,
    svolume      DOUBLE,
    sdelta       DOUBLE,
    sscore       DOUBLE,
    smean        DOUBLE,
    PRIMARY KEY (product_id, run_id, date)
);
```

**Update `silver.products` view:**

```sql
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
```

---

### 2. Ingestion Modules

- **`gateway.py`** – Async context manager for `IB()` connections, with separate `clientId=1` (contracts) and `clientId=2` (prices). Catches `ConnectionRefusedError` and `TimeoutError`, raising a clear `RuntimeError` with instructions to launch IB Gateway.
- **`contracts.py`** – Single‑semaphore (`Semaphore(1)`) loop over `silver.products` target list (actually all products in `bronze.products`). Uses `reqContractDetailsAsync` per product. Upserts into `bronze.contracts` (with `updated_at` refresh). Skips products that are already fresh (within 24h) unless `--force`.
- **`prices.py`** – Uses IB Gateway for daily bars.  
  - Incremental: `durationStr = f"{(today - last_date).days + 7} D"`, `endDateTime = yesterday.strftime("%Y%m%d") + " 23:59:59 UTC"`.  
  - Full: `durationStr="30Y"`, same `endDateTime`.  
  - Overlap validation: date‑for‑date + value comparison (`math.isclose`).  
  - On mismatch: archive existing rows to `cold_storage.prices` (with `run_id` = max `updated_at` of archived rows), delete existing `bronze.prices` for product, insert full refetch. If full refetch identical, do nothing.  
  - `--force` forces deletion + full refetch (with archiving).
- **`sentiment.py`** – Web portal endpoint.  
  - Incremental: `from_date` = last_date - 7 days, `to_date` = today - 1 day (both as `YYYY-MM-DD`), URL with `%2000:00` appended.  
  - Full: `from` = `1990-01-01`, `to` = today - 1 day.  
  - Strips price keys, stores only sentiment metrics.  
  - Overlap validation identical to prices, with archiving to `cold_storage.sentiment` on mismatch.
- **`endpoints.py`** – Remove `price` endpoint. Sentiment slug template becomes:  
  `"{product_id}&from={from_date}%2000:00&to={to_date}%2000:00&bar_size=1D&lang=en"`.

---

### 3. CLI / Pipeline

- `ingest contracts [--force]` – contract qualification (clientId=1)
- `ingest prices [--force]` – price series (clientId=2)
- `ingest details [--force] [--product-ids ...] [--limit N]` – snapshots + sentiment via web portal
- Full `ingest` runs: **products → contracts → prices → session → themes → details**; aborts hard if any phase fails (including Gateway unreachable).
- `resolve_target_ids` uses `silver.products`; raises `RuntimeError` if empty.

---

### 4. `--force` Semantics

- **contracts**: re‑fetch all products regardless of 24h freshness.
- **prices**: delete all rows for each target product (archiving to cold storage), then full refetch.
- **details**: bypass landing gate (always fetch gated snapshots); force full sentiment refetch (archive + delete + full refetch).

---

### 5. Configuration (`config.py` + `pyproject.toml`)

Add:
- `ib_gateway_host` (default `"127.0.0.1"`)
- `ib_gateway_port` (default `4001`)
- `ib_gateway_client_id` (default `1` for contracts, `2` for prices)
- `ib_gateway_timeout` (default `60.0`)

---

### 6. Cleanup / Migration

- No migration from existing DB; we will create a fresh database (remove current file).
- Remove `series.py` entirely; create `sentiment.py`.
- Remove all references to `bronze.series` and `fetch_type`.

---

### 7. Edge Cases

- **Sentiment payload** may omit price keys on some dates; we only store the 8 sentiment metrics; missing values become `NULL`.
- **Weekend dates** may be absent from sentiment; overlap validation only checks dates that already exist in the DB.
- **Price full refetch** with `durationStr="30Y"` covers all historical data; no newer data exists beyond that.

---

The design tree is now fully explored. If there are no further questions, we can proceed to implementation. Would you like to begin coding?