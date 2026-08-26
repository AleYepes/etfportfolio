# ETF Factor-Series Pipeline — DB & Ingestion Implementation Plan

## 0. Scope

This project runs factor-series analyses on ETF and fund data. At a high level, the
full pipeline will: fetch data from IBKR and supplementary sources; preserve raw
payloads through a medallion architecture; construct monthly LOCF panels for
fundamental metrics; build weighted factor return series from those metrics; select a
subset of factors as independent variables; regress ETF returns on factor returns; and
calculate efficient-frontier portfolios.

This plan covers **only** the database and ingestion layer — fetching data from IBKR,
preserving raw payloads, and landing it in a bronze layer. It does not cover panel
construction, factor weighting/selection, regression, or portfolio construction —
those are separate efforts that will consume this layer's output but aren't designed
here.

---

## 1. Tech stack

**Runtime dependencies:** `duckdb`, `orjson`, `xxhash`, `zstandard`, `httpx`,
`playwright`, `pydantic-settings`, `fire`

**Dev dependencies:** `pytest`, `respx`, `ruff` (lint + format, single tool)

**Python:** `>=3.14`, managed via `uv` (`pyproject.toml` + `uv.lock`)

---

## 2. Directory structure

```
/
├── data/                        # gitignored entirely (except .gitkeep)
│   ├── etf.duckdb                 # the single DuckDB warehouse file
│   ├── session_state.json          # Playwright-captured cookies/storage state
│   └── logs/
│       └── {run_start_datetime}.log
├── docs/
│   └── sample_product_ids.txt      # hand-picked smoke-test product_id list
├── tests/
│   ├── fixtures/                   # raw sample payloads (landing, holdings, ratios, price, themes...)
│   ├── test_utils.py               # canonicalize/hash/compress correctness
│   ├── test_landing.py             # gating decision logic, mocked via respx
│   ├── test_series.py              # incremental fetch + overlap-check logic
│   ├── test_products.py            # pagination termination logic
│   └── test_themes.py              # taxonomy parents/nodes merge logic
├── etfportfolio/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py                # Settings(BaseSettings)
│   │   ├── db.py                     # connection context manager, schema application, blob GC
│   │   ├── schema.sql                # idempotent DDL — bronze fully defined, silver/gold reserved
│   │   └── utils.py                  # canonicalize → hash → compress content-addressing pipeline
│   └── ingestion/
│       ├── __init__.py
│       ├── session.py                # Playwright login, httpx client construction, accountList probe
│       ├── endpoints.py              # static registry: url template, shape, gated
│       ├── landing.py                # fetch landing (pre-filtered), hash, compare against bronze.snapshot_previews
│       ├── snapshots.py              # store function for snapshot-shaped responses
│       ├── series.py                 # fetch+store for series-shaped responses (incremental logic)
│       ├── products.py               # product-discovery crawl → bronze.products
│       ├── themes.py                 # taxonomy sync → bronze.themes (per-product weights go via snapshots.py)
│       └── pipeline.py               # orchestration: phases, semaphore, summary commit
├── main.py                          # Fire dict-based CLI dispatcher — thin, no logic
├── pyproject.toml                    # deps + [tool.etfportfolio] committed config table
├── .env                              # gitignored: IBKR_USERNAME/PASSWORD (reserved), ACCOUNT_ID
└── uv.lock
```

`silver` and `gold` schemas are created (empty) now so the medallion structure exists
end-to-end; their table design is deferred (§10).

---

## 3. Database schema (`etfportfolio/core/schema.sql`)

Applied idempotently every time a connection is opened. No migration framework —
`schema.sql` is the single source of truth.

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;   -- reserved, empty for now
CREATE SCHEMA IF NOT EXISTS gold;     -- reserved, empty for now

-- Content-addressed store. Shared by snapshots, series, and snapshot_previews.
CREATE TABLE IF NOT EXISTS bronze.payload_blobs (
    hash    UBIGINT PRIMARY KEY,     -- xxhash.xxh3_64_intdigest(bytes, seed=0) of canonical bytes
    payload BLOB NOT NULL            -- zstd (level 3) compressed canonical bytes
);
-- NOTE: xxh3_64_intdigest returns an UNSIGNED 64-bit int. Use UBIGINT, not BIGINT
-- (DuckDB's BIGINT is signed and would overflow/reject roughly half of all digests).

-- Upserted, no raw crawl-page preservation. Source of truth for the product universe.
CREATE TABLE IF NOT EXISTS bronze.products (
    product_id              INTEGER PRIMARY KEY,
    type                    VARCHAR,                -- "ETF" | "FUND" as returned
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
-- Landing's URL is fully static aside from product_id substitution, so no url_prefix/url_slug
-- columns are needed here — unlike bronze.snapshots/series, there's no ambiguity to record.
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
    url_slug     VARCHAR,             -- the fully-resolved remainder, exactly as fetched (see note below)
    fetched_at   TIMESTAMP NOT NULL
);

-- Insert-only. One row per fetch of a series-shaped endpoint (incremental or full).
CREATE SEQUENCE IF NOT EXISTS bronze.series_id_seq;
CREATE TABLE IF NOT EXISTS bronze.series (
    series_id    INTEGER PRIMARY KEY DEFAULT nextval('bronze.series_id_seq'),
    hash         UBIGINT NOT NULL REFERENCES bronze.payload_blobs(hash),
    product_id   INTEGER NOT NULL REFERENCES bronze.products(product_id),
    url_prefix   VARCHAR NOT NULL,
    url_slug     VARCHAR,
    first_date   TIMESTAMP NOT NULL,
    last_date    TIMESTAMP NOT NULL,
    fetched_at   TIMESTAMP NOT NULL
);

-- Upserted, no raw crawl-page preservation. Global theme taxonomy (definitions/hierarchy) —
-- NOT per-product theme weights, which are ordinary snapshot rows (see §8.2, "themes" endpoint).
CREATE TABLE IF NOT EXISTS bronze.themes (
    theme_id     VARCHAR PRIMARY KEY,     -- IBKR's "key" (UUID string)
    num_id       INTEGER,                  -- IBKR's "numId" — a cheaper potential future join key
    name         VARCHAR,
    parent_id    VARCHAR REFERENCES bronze.themes(theme_id),  -- NULL for root/"parents" entries
    created_at   TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL
);
```

**On `url_prefix`/`url_slug`:** these store the request **exactly as fetched, fully
resolved** — not an unsubstituted template. `product_id` is already its own column, so
nothing is lost by resolving it into `url_slug` too; the benefit is that the row
reconstructs the literal request that produced it (useful for debugging price/period
selection, ownership's `fields=` list, etc.) without cross-referencing `endpoints.py`.
`url_prefix` is the literal, invariant lead-in up to (not including) the first dynamic
value — constant per endpoint, so `GROUP BY url_prefix` cleanly separates endpoints.
(Named `url_prefix`/`url_slug` rather than `endpoint`/`slug` specifically to avoid
colliding with the `Endpoint` class name in `endpoints.py`.)

---

## 4. `core/config.py` — settings

A single flat `Settings(BaseSettings)`, sourced from `pyproject.toml`'s
`[tool.etfportfolio]` table (committed) layered with `.env` (gitignored —
`IBKR_USERNAME`/`IBKR_PASSWORD`, reserved for future automated login and currently
unused since login is manual-through-browser; `ACCOUNT_ID`, set once after the first
successful probe — a mismatch on a later run raises a fatal error rather than silently
overwriting). Requires overriding `settings_customise_sources` to wire in
`PyprojectTomlConfigSettingsSource` pointed at `("tool", "etfportfolio")` alongside the
dotenv/env sources.

```python
db_path: str = "data/etf.duckdb"
data_dir: str = "data"
session_state_path: str = "data/session_state.json"
log_dir: str = "data/logs"
ibkr_base_url: str = "https://www.interactivebrokers.ie"
endpoint_concurrency: int = 5
ibkr_username: str | None = None
ibkr_password: str | None = None
account_id: str | None = None
```

---

## 5. `core/utils.py` — content-addressing pipeline

Canonicalize by recursively sorting both dict keys and array elements (a thin wrapper,
since `orjson`'s `OPT_SORT_KEYS` alone only sorts dict keys), then hash with
`xxhash.xxh3_64_intdigest`, then compress with `zstd` for storage:

```python
import orjson, xxhash, zstandard as zstd

_TYPE_RANK = {type(None): 0, bool: 1, int: 2, float: 2, str: 3, list: 4, dict: 5}

def _sort_key(value):
    return (_TYPE_RANK.get(type(value), 6),
            orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode())

def _canonicalize(value):
    if isinstance(value, dict):
        return {k: _canonicalize(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return sorted((_canonicalize(v) for v in value), key=_sort_key)
    return value

def canonical_bytes(payload) -> bytes:
    return orjson.dumps(_canonicalize(payload), option=orjson.OPT_SORT_KEYS)

def content_address(payload) -> tuple[int, bytes]:
    """Returns (hash, compressed_bytes) ready for bronze.payload_blobs."""
    canonical = canonical_bytes(payload)
    digest = xxhash.xxh3_64_intdigest(canonical, seed=0)
    compressed = zstd.ZstdCompressor(level=3).compress(canonical)
    return digest, compressed
```

---

## 6. `core/db.py` — connection lifecycle & blob GC

One connection per CLI command, held via a `ContextVar` so nested functions can reach
it without threading `conn` through every signature:

```python
from contextlib import contextmanager
from contextvars import ContextVar
import duckdb

_current: ContextVar[duckdb.DuckDBPyConnection | None] = ContextVar("_current", default=None)

@contextmanager
def connect():
    conn = duckdb.connect(settings.db_path)
    apply_schema(conn)   # executes schema.sql, idempotent
    token = _current.set(conn)
    try:
        yield conn
    finally:
        _current.reset(token)
        conn.close()

def current() -> duckdb.DuckDBPyConnection:
    conn = _current.get()
    if conn is None:
        raise RuntimeError("No active DB connection — call within `with core.db.connect():`")
    return conn
```

**Blob GC**, triggered only by `bronze.snapshot_previews` upserts: before upserting a
product's preview row, read its current `hash`; after the upsert, if the hash changed
and the old hash has zero references left across `snapshots`/`series`/`snapshot_previews`,
delete it from `payload_blobs`. Wrap the check-then-delete in one transaction to avoid a
race against itself. O(1) per ingestion run given the one-hash-per-product invariant on
`snapshot_previews` — no periodic sweep job needed for v1.

---

## 7. `ingestion/session.py` — auth

**`login()`** — opens a real Playwright browser window, blocks waiting for the human to
complete login manually (handles 2FA without special-casing), captures `storage_state`
to `data/session_state.json` on completion.

**`build_client()`** — loads `session_state.json`, constructs an `httpx` client with
those cookies attached.

**`probe(client)`** — calls the session-validation endpoint, which also supplies the
account ID some endpoints need:

```
GET https://www.interactivebrokers.ie/tws.proxy/acesws/accountList
```

Requires exactly one entry in `accessibleAccounts` — zero or more than one raises a
loud error rather than guessing.

### 7.1 Error handling — status-code-based, with one narrow exception

IBKR's web-app API returns all sorts of 4xx/5xx for all sorts of unrelated reasons, and
responses aren't always JSON (sometimes HTML error pages). Examples observed:
- `{"status":404,"message":"Unable to find isin for conid 80553104"}`
- `{"type":"INVALID_REQUEST_PARAMETERS","message":"Unknown contract type"}` (400)

So the general rule is **status-code-based**: any non-2xx is an ordinary failure,
handled by the retry policy below. There is exactly **one** exception — the
session-invalid signature, the only reliable indicator that the session itself (not the
specific request) is the problem:

```json
{"error": "Invalid headers", "statusCode": 400}
```

**Detection algorithm:** on any non-2xx response, attempt to parse the body as JSON
inside a guarded `try/except` (falls through to "ordinary failure" on parse failure,
e.g. an HTML error page). If parsing succeeds, check `body.get("error") == "Invalid headers"`
alone — `statusCode` reliably reads 400 alongside it, so checking it too would be
redundant. A match means: skip the retry policy entirely for this request and route to
the halt-and-reauth path (§8, Phase 2 / §8, Phase 3).

### 7.2 Retry policy

Simple and uniform, deliberately not split by failure type (optimize later if needed):
**any non-2xx response gets up to 3 attempts with exponential backoff (~1s start)**,
except the session-invalid signature above, which bypasses retry entirely. After
exhausting retries on an ordinary failure: log and drop that specific endpoint for this
product, continuing the run.

### 7.3 Where auth-invalid detection leads, by phase

- **Phase 2 probe fails** (start of the session-dependent portion of the run):
  auto-trigger `login()` once, rebuild the client, re-probe once. If that second probe
  also fails: abort `ingest run` with a fatal, clear error message. No further retry
  loop yet — deferred pending observing how the flow behaves against bad credentials.
- **A fetch mid-run hits the signature** (Phase 2.5 or Phase 3): halt the entire run
  immediately with instructions to re-run `auth login`. No auto-login here — an
  unattended run silently blocking on a browser window it can't interact with is worse
  than stopping cleanly.
- **`products.py` (Phase 1) failures** are never auth-related — that phase needs no
  session at all, and its errors are independent (§8, Phase 1).

### 7.4 `account_id` handling

Captured fresh from every successful probe, in memory, for the run; also persisted to
`.env`'s `ACCOUNT_ID`. If `.env` has none yet: write it. If it already has one and the
probe's value differs: raise a fatal error rather than silently overwriting — a
mismatch likely signals something worth investigating (e.g. logged into an unexpected
account).

---

## 8. `ingestion/pipeline.py` — orchestration

### Phase 1 — Product discovery (no session required)

`products.py`'s crawl against `products-by-filters`, requesting **both**
`productType: ["ETF", "FUND"]`. Paginated at `pageSize=500`, starting at `pageNumber=1`
(pagination starts at 1, not 0 — `pageNumber=0` and `=1` return identical results),
terminating when a page returns fewer than `pageSize` products (the response's own
`productCount`/`productTypeCount` fields are unreliable and not used for this). Upserts
into `bronze.products`; `created_at` set once, `updated_at` refreshed every run. No raw
crawl-page preservation — not worth it, since the listing is public and re-crawlable
anytime. Errors here are logged but non-fatal to the run; subsequent phases proceed
using whatever's currently in `bronze.products`.

Also exposed as its own standalone concern, invocable independent of a full `ingest run`
— see §11 for CLI surface.

### Phase 2 — Session validation (blocking, once per `ingest run`)

Build a client from the existing `session_state.json`, call `probe()`. On success:
capture and reconcile `account_id` (§7.4), proceed. On failure: trigger `login()` once,
rebuild the client, re-probe once; a second failure aborts the whole run (§7.3).

### Phase 2.5 — Theme taxonomy sync (requires session; runs immediately after Phase 2)

`themes.py` fetches the full taxonomy:

```
GET https://www.interactivebrokers.ie/tws.proxy/knowledge-graph/meta/themes
```

Response has two arrays: `parents` (root theme categories, no `parentKey` of their
own) and `nodes` (leaf/child themes, each carrying a `parentKey` pointing into
`parents`). Both populate the **same** `bronze.themes` table:
- `parents` entries → `parent_id = NULL`, upserted **first** (so the self-referential
  FK never fails on insert order).
- `nodes` entries → `parent_id = parentKey`, upserted second.
- Each entry's `numId` is captured into `num_id`.

This taxonomy sync re-runs every `ingest run` — the same small reference-data crawl
each time, no change-detection needed given its size. No raw payload preservation, same
rationale as `bronze.products`.

**This is distinct from per-product theme *weights***, which is a per-product endpoint
fetched in Phase 3 like any other detail endpoint, storing raw weight arrays as
ordinary snapshot blobs (§8.2's `themes` registry entry).

### Phase 3 — Per-product fetch loop (sequential across products; concurrent within each)

Scope: every `product_id` in `bronze.products`, or the set passed via
`--product-ids`/`--limit` (mutually exclusive CLI flags, §11). For each product,
sequentially:

1. **Landing fetch** (`landing.py`, not semaphore-bound — must resolve before this
   product's batch can be built). Requests only the fields actually needed, directly:
   ```
   fundamentals/landing/{product_id}?widgets=objective,keyProfile,lipper_ratings,holdings,mf_key_ratios,ownership,mstar&lang=en
   ```
   No trimming step is needed — the naturally minimal response is
   canonicalized/hashed/compressed as-is (same pipeline as everything else) and
   compared against `bronze.snapshot_previews.hash` for this `product_id`. `landing.py`
   returns the gate decision (`changed: bool`) plus the pending
   `(hash, compressed_bytes)` for `pipeline.py` to commit later — it never writes to
   `bronze.snapshot_previews` itself; that commit is conditional on downstream success
   (step 5). On landing-fetch failure after retry: log and skip to the next product
   entirely.
2. **Build the fetch plan**: every `gated=False` endpoint, plus every `gated=True`
   endpoint if landing signaled a change. Gating is binary/all-or-nothing — if any part
   of the landing response changed, every gated endpoint is refetched together,
   regardless of which part moved.
3. **Fetch the plan concurrently** under `asyncio.Semaphore(settings.endpoint_concurrency)`
   (default 5). Every request is formatted with both `product_id` and `account_id`
   regardless of whether a given endpoint's template references `account_id` —
   `str.format()` ignores unused kwargs. **Important:** `product_id` is purely an
   internal/local variable name; IBKR's own literal query parameter names (e.g. `conid=`
   in the `mstar` and `themes` endpoints) are preserved exactly as the API expects —
   only the `{...}` substitution token is named for our own use, never the literal
   parameter name in the URL.
   - Any response matching the session-invalid signature (§7.1) → halt the entire run
     immediately. In-flight sibling requests in this same batch are allowed to finish
     (already issued, bounded by the semaphore) but no further products are processed.
   - Any other failure → retry policy (§7.2) applies.
4. **Store each successful response** by `shape`: `snapshot` → `snapshots.py`, `series`
   → `series.py`. DB writes happen inline/synchronously — no executor/thread pool for
   v1 (local SSD, writes small relative to network I/O).
5. **Commit the pending `bronze.snapshot_previews` upsert** (with blob GC, §6) only if
   **every** gated endpoint in this product's plan succeeded this run. A partial
   failure leaves the preview hash stale, so next run's landing comparison naturally
   re-triggers the full gated set — no separate failure-tracking table needed.

### 8.1 `ingestion/endpoints.py` — static endpoint registry

```python
@dataclass(frozen=True)
class Endpoint:
    name: str
    url: str        # relative template; may reference {product_id} / {account_id}
    shape: str        # "snapshot" | "series"
    gated: bool
```

| name      | url (relative, `lang=en` appended to all)                                                                                                                  | shape    | gated |
|-----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-------|
| holdings  | `fundamentals/mf_holdings/{product_id}?lang=en`                                                                                                                | snapshot | True  |
| ratios    | `fundamentals/mf_ratios_fundamentals/{product_id}?lang=en`                                                                                                     | snapshot | True  |
| ownership | `fundamentals/ownership/{product_id}?fields=owners_types,institutional_owners,insider_owners,institutional_total,insider_total,institutional_summary,insider_summary,others_summary&lang=en` | snapshot | True  |
| profile   | `fundamentals/mf_profile_and_fees/{product_id}?lang=en`                                                                                                        | snapshot | True  |
| lipper    | `fundamentals/mf_lip_ratings/{product_id}?lang=en`                                                                                                             | snapshot | True  |
| mstar     | `mstar/fund/detail?conid={product_id}&lang=en`                                                                                                                 | snapshot | True  |
| esg       | `impact/esg/{product_id}?accounts={account_id}&lang=en`                                                                                                        | snapshot | False |
| themes    | `knowledge-graph/ui/fund?conid={product_id}&max=999999999&lang=en`                                                                                             | snapshot | False |
| sentiment | `sma/request?type=search&conid={product_id}&from={from}&to={to}&bar_size=1D&lang=en`                                                                          | series   | False |
| price     | `fundamentals/mf_performance_chart/{product_id}?chart_period={period}&lang=en`                                                                                 | series   | False |

`gated` and `shape` are independent axes on each entry — nothing enforces that a
gated endpoint must be snapshot-shaped, or that a series-shaped endpoint can't be
gated; this table simply reflects the currently-known set.

**`themes` here is the per-product weights endpoint** (asset-level theme membership +
weight), stored as an ordinary snapshot blob — not to be confused with the taxonomy
sync in Phase 2.5, which populates `bronze.themes` directly and has no registry entry
of its own (it's a one-off global fetch, not a per-product one).

`risk`, `performance`, and `dividends` endpoints, and the `dividends` landing widget,
are out of scope for this pipeline.

### 8.2 Snapshot vs. series storage (`snapshots.py` / `series.py`)

**Snapshots**: `content_address()` the response, insert into `payload_blobs` (no-op on
hash conflict — already stored), insert the lineage row into `bronze.snapshots` with
`url_prefix`/`url_slug` reflecting the actual resolved request (§3).

**Series** — two different incremental strategies depending on the endpoint:
- **`price`** only accepts preset periods (`1M, 3M, 6M, 1Y, 3Y, 5Y, 10Y, MAX`). Period
  selection: look up the max stored `last_date` for this `(product_id, url_prefix)`,
  pick the smallest preset that covers the gap — presets are coarse enough that this
  almost always yields some overlap with existing data.
- **`sentiment`** accepts arbitrary `from`/`to` dates, so its incremental fetch computes
  the exact missing range directly from the last stored date — no preset-rounding
  needed, and by construction always overlaps by exactly the boundary date.

**Overlap validation** (both endpoints): compare the incoming payload's overlapping
dates against previously-stored values for that window.
- Match → store the fetched payload as a new row, as-is. Bronze never merges/trims
  series data; reconciling overlapping ranges into one clean series is a silver-layer
  concern.
- Mismatch → discard the incremental payload (don't store it), re-fetch `price` with
  `chart_period=MAX` (or `sentiment` with its full available range) and store that as a
  new row instead. The old, now-dubious row is left untouched — no updates/deletes on
  `bronze.series`, ever. Which period was actually used is recoverable from `url_slug`
  alone; no separate `fetch_reason` column is needed.

**`sentiment`'s bundled price data is ignored, but the raw response is still stored
as-is.** The endpoint returns both sentiment and price-like data together, but when
`bar_size=1D`, its embedded price series is always aggregated starting at 05:00
regardless of the requested time, inconsistent with `price`'s conventional 13:30 UTC
daily aggregation. Rather than stripping the embedded price sub-object before storage
(which would violate "bronze preserves raw payloads" for a bronze-layer reason), the
full raw `sentiment` response is stored untouched — the decision to source price data
exclusively from the `price` endpoint (and ignore `sentiment`'s bundled copy) is a
silver-layer consumption choice, not a bronze-layer transformation.

---

## 9. `ingestion/themes.py`

Owns exactly one responsibility: the Phase 2.5 taxonomy sync described in §8. Exposes
a `sync(client)` function called internally by `pipeline.py`. A standalone CLI command
for this is not yet defined — see §11.

---

## 10. Explicitly deferred / out of scope

Noted here so these are recognized as deliberate boundaries, not oversights:

- **Silver/gold table design** — schemas created empty; real design happens in a
  separate future module once ingestion is running.
- **Multi-source ingestion** (`wbgapi`, `fredapi`) — no `source` column anywhere in
  bronze yet; will be added once a second source's actual shape is known.
- **Migration framework** — plain idempotent `schema.sql` only.
- **Standalone `vacuum` CLI command** — blob GC is inline/automatic (§6); a periodic
  full-scan sweep as a crash-recovery safety net is a reasonable later addition.
- **Periodic full-series refetch safety net** (to catch retroactive data revisions
  incremental fetching alone can't see) — incremental-only for now; the schema already
  supports adding this later since a full refetch is just another row.
- **Login-flow retry/backoff refinement** — a single retry attempt for now (§7.3);
  revisit once real-world behavior against bad credentials is observed.
- **`risk`, `performance`, `dividends` endpoints, and the `dividends` landing widget**
  — out of scope entirely.
- **A standalone `themes sync` CLI command** — the sync logic exists and runs
  automatically as Phase 2.5 of `ingest run`; a dedicated CLI entry is undecided.
- **`domain`/`productType` parameterization** for product discovery beyond
  `["ETF","FUND"]` — hardcoded; different asset types return different schemas and
  often need different endpoints entirely.
- **Raw preservation of product-discovery and theme-taxonomy crawl pages** — not
  worth it; both are public and re-crawlable anytime.

---

## 11. CLI (`main.py`)

Fire, dict-based dispatch — each ingestion submodule owns its own CLI-facing object;
`main.py` only wires them together:

```python
import fire
from etfportfolio.ingestion import products, session, pipeline

if __name__ == "__main__":
    fire.Fire({
        "products": products.cli,
        "auth": session.cli,
        "ingest": pipeline.cli,
    })
```

- `python main.py products sync` — discovery crawl standalone; also runs automatically
  as Phase 1 of `ingest run`.
- `python main.py auth login` — manual Playwright login standalone.
- `python main.py ingest run [--product-ids=...] [--limit=N]` — full pipeline
  (Phase 1 → 2 → 2.5 → 3). `--product-ids` and `--limit` are mutually exclusive; both
  usable standalone against the full `bronze.products` default when omitted.
  `--product-ids` accepts either a comma-separated list or a path to a
  newline-delimited file (e.g. `docs/sample_product_ids.txt`), auto-detected by
  checking whether the given string is an existing file path.
- No standalone `themes` command yet (§10).

---

## 12. Logging

Standard library `logging`, configured once in `main.py` at startup: a console handler
plus a file handler writing to `data/logs/{run_start_datetime}.log` (one file per
invocation). No structured/JSON logging framework.

---

## 13. Testing

`pytest`, with sample payloads saved under `tests/fixtures/` as golden inputs for:
- Canonicalization/hash correctness (key-reordered and array-reordered variants of the
  same payload must produce identical hashes).
- Landing gating decisions (mocked via `respx`, not a live session).
- Series overlap-check logic (crafted matching and conflicting overlap fixtures).
- Product-discovery pagination termination.
- Theme taxonomy's `parents`/`nodes` merge logic.

`respx` (or `pytest-httpx`) mocks all `httpx` calls so tests never depend on a live
authenticated IBKR session.

---

## 14. Open implementation-time details (flagged, non-blocking)

- Whether a Phase-1 `products.py` crawl failure should be fatal to `ingest run` or only
  logged (currently assumed: logged, non-fatal — see §8, Phase 1).
- DuckDB's exact `INSERT ... ON CONFLICT DO UPDATE` syntax for the upserted tables
  (`products`, `snapshot_previews`, `themes`) — verify against the installed DuckDB
  version at implementation time.
- `url_prefix`/`url_slug` store the fully-resolved request rather than an unsubstituted
  template (§3) — worth double-checking this matches intent before relying on it for
  debugging tooling.
- Exact mechanism for aborting the outer per-product loop after a mid-batch
  session-invalid detection (§8, Phase 3, step 3) — letting already-issued sibling
  requests in the current `asyncio.gather` finish before halting, rather than forcibly
  cancelling them, is the assumed simplest-correct approach.
- Exact query parameters for the `sentiment` and `esg` endpoints beyond what's shown
  above should be double-checked against live responses before implementation, since
  only a subset of fields has been directly verified.