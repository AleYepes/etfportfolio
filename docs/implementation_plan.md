# Ingestion Refactor — FRD

## 0. Purpose & how to use this document

This is the output of an interview-driven design session covering the `etfportfolio` ingestion
subsystem (`etfportfolio/ingestion/`, plus the parts of `etfportfolio/core/` it depends on).
It supersedes the prior `handoff.md` / `refactoring.md`.

Every decision below reflects an explicit choice made during the interview, not a default or an
assumption. Where a decision was informed by production log data, that data is cited so a fresh
agent can judge whether the reasoning still holds if circumstances change. Section 6 lists ideas
that were considered and explicitly rejected.

**Scope**: ingestion only. Panel-building, factor construction, regression, and efficient-frontier
phases are out of scope (Section 8), though a few forward-looking notes are left for whoever builds
the panel phase.

**No migration path is required or wanted.** The database is recreated from scratch after this
refactor lands. Schema changes below are additive/replacing, not migration scripts.

## 1. Guiding principles

- Clear, simple, clean, testable code. No hidden global state. No speculative abstractions.
- Async functions contain no SQL; SQL helpers are pure functions, independently callable/testable.
- Prefer replacing old functionality over accumulating deprecated-but-present alternatives.
- When a design choice is a toss-up, prefer whichever reads as clearer/leaner/more consistent with
  the rest of the codebase, even if it costs a little redundancy.
- Don't build for hypothetical future needs. Several ideas were rejected on this basis alone — not
  because they're bad ideas, but because nothing today needs them and they're cheap to add later if
  that changes.

## 2. Success criteria

- Request efficiency (`new_data_obtained / total_request_count`) and wall-clock runtime for a full
  `ingest details` + `ingest sentiment` pass both improve measurably over the current baseline.
- Baseline: a full run over 22,531 products took **~14h** under the *current*, unmodified code
  (sequential per-product processing, sentiment bundled into `details`, no freshness-window
  skipping). See §3.1 and Appendix A.
- Code is demonstrably cleaner and more testable: no SQL inside async functions; SQL helpers remain
  pure and independently callable.
- All phases continue to produce the same data outputs as today (mismatches aside — see
  confirmed-absence handling in §3.4 and series empty/404 handling in §3.4.1, which are deliberate
  behavior changes agreed on explicitly).
- Unit tests cover the new pure helpers (freshness, 404 path, timeseries overlap/replace/upsert,
  content-addressing, landing stamp rule) and use existing `tests/fixtures/{ep}_{case}.json` where
  applicable.

## 3. Decisions & rationale

### 3.1 Concurrency model for `details` — no change, by design

**Decision: keep per-product processing strictly sequential** (the existing
`for idx, product_id in enumerate(target_ids, 1): await ...` loop in `_run_details_phase`). Do not
introduce a global semaphore, a product-level semaphore, a LIFO primitive, or a two-stage pipeline
for `details`.

**Why:** Once sentiment is split (§3.2), a product's remaining per-iteration cost collapses close to
the sub-second floor. Rough projection: 22,531 products × ~1s ≈ 6–7 hours for `details` alone,
without new concurrency machinery.

**Intra-product semaphore stays.** `details_concurrency` (renamed from `endpoint_concurrency`,
default 10) continues to limit concurrent endpoint fetches *within* one product. After the split
there are ≤7 snapshot endpoints per product, so the knob is already near saturation. It remains
useful as a rate-limit dial (may be lowered to 3–4 later if portal pressure appears). Raising it
above the endpoint count has no effect while products stay sequential.

**DX:** Sequential products keep `product_id` order in logs. An inter-product semaphore would
shuffle them.

**Fallback if the 6–7h projection is still too long:** product-level semaphore for `details`
(documented so a future agent does not rediscover the FIFO priority-inversion problem of a flat
global semaphore). Not implemented in this refactor.

**Sentiment (new phase):** uses a **product-level** semaphore controlled by
`settings.sentiment_concurrency` (default 1). Same pattern as prices/contracts (hardcoded 1 for IB
Gateway); the config exists so real concurrency is a one-line change once standalone behavior is
observed. At default 1, log order stays stable; raising the knob accepts interleaving of
`product_id`s in logs.

### 3.2 Splitting `sentiment` into its own phase

**Decision: full split.** `ingest details` becomes snapshot-only. A new `ingest sentiment` command
is added, structured like `prices.py` (own orchestrator: target selection, `_run_..._ingestion`
loop, `sync()`), with its own `Ingest.sentiment()` CLI method.

**Rationale:** `sentiment` and the snapshot endpoints have different operational profiles — long
daily time series vs. small JSON blobs; different (and stricter) rate-limiting behavior; different
useful refresh cadence. Bundling them forced compromises on both. Splitting lets each phase be
tuned independently.

**Consequences:**
- `delay_before_request` (the field on `Endpoint` and its sleep-before-request logic in
  `session.fetch_with_retry`) is **removed entirely**.
- The `Endpoint.shape` field (`"snapshot"` | `"series"`) is **removed**. Everything `details`
  processes is a snapshot.
- **Session reuse in `_run_full`**: `sentiment` uses the same web-portal `ensure_session()` client
  as `details`. In `_run_full`, reuse the *same* client/`account_id` already established for
  Details — do not open a second session. A standalone `uv run main.py ingest sentiment` still
  calls `ensure_session()` itself.
- **Phase ordering in `_run_full`**: **Phase 7: Sentiment series**, immediately after Phase 6:
  Details, still inside the same `client`/`account_id` scope opened at Phase 4. Numbering stays
  additive; phases sharing a session stay adjacent.
- `sentiment.py` gains the orchestration layer it currently lacks (reuse of
  `products.resolve_target_ids` from `silver.products`, `_run_..._ingestion`, `sync()`), mirroring
  `prices.py`.

### 3.3 `ENDPOINTS` inventory changes

**Decision: fold `landing` and `sentiment` into the single `ENDPOINTS` list.** One inventory of
every IBKR endpoint the codebase talks to.

**No new field to mark them as "special."** `details.py`'s plan-builder excludes
`{"landing", "sentiment"}` by name; `landing.py` and `sentiment.py` look up their entry via
`ENDPOINTS_BY_NAME`.

**`ownership` is dropped entirely** — not added to `ENDPOINTS`, not added to the landing widget
query string. Production data (Appendix A): 404 for 22,079/22,531 products (98.0%). Not worth
calling.

**Final `ENDPOINTS` membership and gating:**

| name | gated | notes |
|---|---|---|
| `landing` | `False` (unused — landing drives the gate) | newly added; consumed via `ENDPOINTS_BY_NAME`; excluded from details plan |
| `holdings` | `True` | unchanged |
| `ratios` | `True` | unchanged |
| `profile` | `True` | unchanged |
| `lipper` | `True` | unchanged |
| `mstar` | `True` | unchanged |
| `esg` | `False` | stays ungated (§3.3.1) |
| `theme_weights` | `False` | stays ungated (§3.3.1) |
| `ownership` | — | **removed** |
| `sentiment` | `False` (unused) | newly added; consumed via `ENDPOINTS_BY_NAME`; excluded from details plan; driven by `ingest sentiment` |

`Endpoint` loses `shape` and `delay_before_request`; `gated` remains.

**Landing widget query string unchanged:**
`objective,keyProfile,lipper_ratings,holdings,mf_key_ratios,mstar&lang=en`

#### 3.3.1 Why `esg` / `theme_weights` stay ungated

Landing hash only reflects widgets in its query string. Gating `esg`/`theme_weights` on a signal
that cannot see them risks false negatives (transient gated failure blocks preview commit) and
false positives (independent changes never refetch). They get the same request savings via the
**per-endpoint freshness cache** (§3.5.3).

### 3.4 Confirmed-absence (404) handling

**Final design:**

1. **`session.fetch_with_retry` treats 404 as a normal, non-exceptional outcome.** Drop
   `NON_RETRYABLE_STATUS_CODES` and any bespoke exception class. On 404, return `(404, None)`
   immediately — no retry, no raise. Every other status code's retry behavior is unchanged.
   Production: 404 was 97.1% of retained error records (47,189 / 48,606).
2. **Callers:**
   - **Snapshots / landing:** `payload or {}` — persist a bare empty dict (no sentinel key).
     Confirmed-absence rows give `fetched_at` / hash so the freshness cache works.
   - **Sentiment (and any series path):** on `status == 404` or empty extract, **do not write**.
     Preserve existing rows. Do not run overlap validation. Do not call replace.
3. **Why persist `{}` for snapshots but not for series:** Snapshot freshness keys off
   `bronze.snapshots.fetched_at`. Without a row, `esg`/`theme_weights` would be re-requested every
   run forever (48–52% 404 rates). Series already use last-date incremental logic; writing empty
   would either no-op or, on the replace path, delete history. Different shapes, different rules.
4. **Landing 404:** content-address `{}`. Repeated 404s hash identically → after the first
   occurrence the product looks unchanged and gated endpoints stop being fetched. If landing later
   returns real data, the hash changes and gated fetching resumes.
5. **Accepted risk (unchanged):** landing 404 while gated endpoints would return real data
   permanently suppresses gated fetches. No clean fix without per-widget gating (rejected).
   Recorded, not deferred work.
6. **Accepted limitation (unchanged):** a product whose sentiment endpoint always 404s has no
   last-date signal and will attempt full historical refetch every run. Unobserved in production
   (0% 4xx on sentiment). Leave unaddressed until observed.

#### 3.4.1 Series empty-body handling (prices & sentiment)

HTTP 200 with empty extract (`{"sentiment":[]}`, or IB returning no bars) is treated the same as
404 for write purposes:

- Do not run overlap validation.
- Do not call replace or upsert.
- Log and return (confirmed absent / nothing to store).
- Existing rows are preserved.

Only a **non-empty** payload may enter overlap validation or replace/upsert.

### 3.5 Freshness-window skip system

Largest structural addition. Generalizes the ad-hoc pattern in `contracts.py` (`_contract_is_fresh`,
hardcoded 24h) into a shared mechanism applied where useful, and explicitly not applied where it
would not help.

**Concrete benefit:** resume after interruption. A crashed or `Ctrl+C`'d run, restarted shortly
after, re-skips what was already successfully processed. For the intended weekly cron, a 24h window
does **not** reduce request volume on a normal uninterrupted run (~168h later). Steady-state weekly
efficiency comes from the sentiment split (§3.2) and confirmed-absence persistence (§3.4), not from
this mechanism.

**Scope:**

| Phase | Mechanism | Column checked |
|---|---|---|
| `contracts` | per-product (existing, generalized) | `bronze.contracts.updated_at` |
| `details` (landing) | per-product (new) | `bronze.snapshot_previews.last_checked_at` (new) |
| `details` (per-endpoint) | per-`(product_id, url_prefix)` via in-memory cache | `bronze.snapshots.fetched_at` (existing) |
| `products` | whole-phase (new) | `bronze.products.last_checked_at` (new, bulk-set) |
| `themes` | whole-phase (new) | `bronze.themes.last_checked_at` (new, bulk-set) |
| `prices` | **not applied** | n/a — incremental last-date already avoids redundant work |
| `sentiment` | **not applied** | n/a — same |

**Shared config:** `settings.freshness_window_hours` (default `24`), one value for every phase above.

**Shared helper:** `is_fresh(last_seen: datetime | None, hours: float) -> bool` lives in
`ingest/utils.py`. Time-math only. No generic SQL-query builder. Each phase writes its own small
query and passes the result through this comparison.

**`--force` bypasses all of this uniformly** (§3.8).

#### 3.5.1 `contracts`

No new column. `_contract_is_fresh` keeps reading `bronze.contracts.updated_at`. Only change: use
`settings.freshness_window_hours` instead of hardcoded 24h.

#### 3.5.2 `details` — landing freshness

**New column: `bronze.snapshot_previews.last_checked_at TIMESTAMP`** (in addition to existing
`updated_at`).

- `updated_at` — bumped only when the landing hash actually **changes**.
- `last_checked_at` — bumped whenever landing was checked and the result reached a **stable**
  state, regardless of whether the hash changed.

**Stamp rule:**

```
stamp last_checked_at  ⟺  NOT (fetch_gated AND NOT gated_success)
```

(`fetch_gated = force or changed`)

Stamp when no gated fetch was attempted, or when a gated fetch was attempted and every gated
endpoint succeeded. Do **not** stamp when a gated fetch was attempted and failed/incomplete — that
product must be re-examined next run.

Key on `fetch_gated`, not `changed`, so `--force` failures are not silently marked stable.

A `KeyboardInterrupt` or crash never reaches the stamp step, so interrupted products are retried in
full — the resume benefit that motivated the mechanism.

**Consequence:** when landing itself is skipped as fresh, `fetch_gated` degrades to just `force`.

#### 3.5.3 `details` — per-endpoint freshness (gated and ungated)

Every snapshot endpoint (`holdings`, `ratios`, `profile`, `lipper`, `mstar`, `esg`, `theme_weights`)
gets a freshness check before attempt. This is what lets permanently-ungated `esg`/`theme_weights`
benefit from skipping.

**No new column.** `MAX(fetched_at)` per `(product_id, url_prefix)` already means "last checked"
once §3.4 ensures every attempt (including 404) writes a row.

**In-memory cache, not per-product queries.** Two queries once at the start of the details run:

```sql
SELECT product_id, last_checked_at FROM bronze.snapshot_previews;

SELECT product_id, url_prefix, MAX(fetched_at)
FROM bronze.snapshots
GROUP BY product_id, url_prefix;
```

Load into `dict[int, datetime]` and `dict[tuple[int, str], datetime]`. Pass as read-only arguments
down through `_run_details_phase → process_product → plan-building`. No module globals. No mid-run
refresh — each product's decision depends only on prior runs.

**Filtering:** inside `process_product`, at plan-build time — partition into `to_fetch` vs
`skipped_fresh` before any tasks. A skipped endpoint never becomes a task. `_fetch_one` stays
unaware of freshness.

**Skips count as satisfied** for gating and overall success. Fold `skipped_fresh` entries in as
trivial successes so a 100%-fresh product succeeds by design, not by an empty-list accident.

#### 3.5.4 `products` / `themes` — whole-phase freshness

**New columns:** `bronze.products.last_checked_at` and `bronze.themes.last_checked_at`. Repeated
identically across every row for a given run. Rejected alternative: dedicated `phase_runs` table
(extra structure for simple bookkeeping).

**Why not `MAX(updated_at)`:** an unchanged run does not bump `updated_at` (conditional updates
only). The phase would look perpetually stale and never skip.

**`themes`:** whole payload already runs in one transaction → stamp `last_checked_at` on all rows
at the end of that same transaction.

**`products`:** multi-page crawl that can `break` early on error without raising. Only bulk-stamp
`UPDATE bronze.products SET last_checked_at = now()` after a confirmed clean complete crawl (page
shorter than `PAGE_SIZE`). One boolean flag at the end.

**CLI:** `Ingest.products()` and `Ingest.themes()` gain `force: bool = False`. `_run_full` threads
its own `force` into every phase that accepts it (`products`, `contracts`, `prices`, `details`,
`sentiment`, `themes`).

#### 3.5.5 Skip reporting

- **`products`/`themes`:** one line before early return, e.g.
  `"Products sync skipped (checked {hours}h ago; use --force to refresh)."`.
- **`details`:** enrich the existing post-phase summary with skip counts, e.g.
  `"details: 18,204/22,531 products skipped (fresh), 4,327 processed; 1,204 endpoint requests
  skipped (fresh) among processed products."` Cheap counters, one line. Do not grow into a
  multi-line report.

### 3.6 Logging & progress (tqdm)

**Decision:** tqdm progress bars on **TTY invocations** as the standard progress indicator for any
per-item loop (`contracts`, `prices`, `sentiment`, `details`). Disabled when stderr is not a TTY
(`disable=not sys.stderr.isatty()`), so cron/redirected runs stay clean. Phase banners and final
summaries remain plain `console.info`.

`--verbose` still controls whether diagnostic log lines are emitted at all; only *where* they
render changes.

**What replaces what:** per-product `console.info(f"[{idx}/{len}] Processing product {id}...")` is
replaced by the tqdm bar (`total` = product count, postfix = current `product_id`). Phase banners
and the §3.5.5 summary stay.

**Coexistence:** small custom handler `TqdmLoggingHandler` (~10 lines) routes log records through
`tqdm.write()` so bars are not corrupted. Installed by `configure_logging` on the stderr path (and
console while a bar is active).

**Where it lives:** `core/progress.py` (new). Lives in `core/` because every future phase directory
will want the same bar + logging coexistence.

### 3.7 Module reorganization

Operating rule:

- Used in only one script → stay declared in that script.
- Shared across multiple scripts **within** one phase directory (`ingest/`, future `panel/`, etc.)
  → that directory's `utils.py`.
- Shared across **multiple** phase directories under `etfportfolio/` → `core/`.

Exceptions that already follow the rule correctly (e.g. `ingest/gateway.py` shared by contracts and
prices) stay as they are. Do not invent extra modules unless something is used in multiple scripts.

Concrete moves:

- **`core/utils.py`:** keep only `decompress_payload` (cross-phase readers of stored payloads).
  Move `content_address`, `canonical_bytes`, and private helpers to **`ingest/utils.py`**.
- **`core/db.py`:** move `store_blob`, `gc_preview_blob` to **`ingest/utils.py`**. Keep
  `apply_schema`, `AsyncDbWorker`.
- **`ingest/utils.py` (new):** content-addressing + blob helpers + consolidated timeseries
  overlap / replace / upsert (including optional cold-storage archive on mismatch-triggered
  replace) + `OVERLAP_CALENDAR_DAYS` + `is_fresh`.
- **No** `core/freshness.py`. `is_fresh` is an ingest concern and lives in `ingest/utils.py`.

### 3.8 `--force` semantics

**One flag, all freshness mechanisms bypassed.** For `details`: ignore freshness windows *and*
ignore the landing gate — fully unconditional refetch. No second force-shaped flag.

Applies to every phase that has a freshness mechanism: `contracts`, `details` (landing +
per-endpoint), `products`, `themes`. `prices`/`sentiment` keep their existing meaning of `force`
(full replace vs incremental); they have no freshness-window interaction.

`_run_full`'s single `--force` threads through to every phase call that accepts `force`, including
`products` and `themes` (new).

### 3.9 Settings / config changes

- **Rename** `endpoint_concurrency` → **`details_concurrency`** (default 10). Intra-product only;
  may be lowered later under portal pressure.
- **New** `sentiment_concurrency`, default **1**. Conservative: 3.1% of products hit at least one
  sentiment 503 even under near-sequential processing. Wired so real concurrency is a one-line
  change once standalone behavior is observed.
- **New** `freshness_window_hours`, default **24**. Single shared setting for every phase in §3.5.
- `[tool.etfportfolio]` in `pyproject.toml`: drop `endpoint_concurrency`; add the three settings
  above.

### 3.10 Series write / destroy policy (prices & sentiment)

Unified rules for both series phases:

1. **404** (or empty extract) → skip write entirely. No overlap validation, no replace, no upsert.
   Existing rows preserved.
2. **Non-empty payload required** before overlap validation or any destructive replace.
3. **Incremental path:** validate overlap on window W; on pass, upsert only `date > last_date`; on
   fail, full refetch then (if non-empty) archive + replace.
4. **Full / force path:** replace only if the extracted points are non-empty; otherwise no-op (do
   not delete existing rows).
5. **Cold-storage archive** only on **mismatch-triggered** replace that is about to write a
   non-empty full-refetch result. Not on `--force`. Not on first fill. Not when the full refetch
   itself is empty (then skip write; live rows stay; no archive).

Archive and replace run in the **same transaction**:
`INSERT cold_storage … SELECT from bronze; DELETE bronze; INSERT new; COMMIT`.

### 3.11 Cold storage (narrow)

**Schema** (added to `schema.sql`; DB recreated from scratch):

```sql
CREATE SCHEMA IF NOT EXISTS cold_storage;

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

CREATE TABLE IF NOT EXISTS cold_storage.sentiment (
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
    reason       VARCHAR,
    PRIMARY KEY (product_id, run_id, date)
);
```

- `run_id` = timestamp of the replace event (per product).
- `reason` = `date_mismatch` | `value_mismatch`.
- No FKs. No retention/GC in this refactor — unbounded; operator may `DROP SCHEMA cold_storage` if
  unused.
- Archive is the full live series for that product at the moment of replace, not only the overlap
  window.

### 3.12 Series overlap window and datetime policy (final)

**Constant:** `OVERLAP_CALENDAR_DAYS = 7` in `ingest/utils.py`.

**Do not floor or rewrite timestamps.** Prices and sentiment keep different native shapes; the
shared overlap helper compares datetime keys **as stored**.

| Item | Prices | Sentiment |
|---|---|---|
| Stored `date` | Naive midnight **calendar label** from IB `YYYYMMDD` (`formatDate=1`) | Server unix instant **as-is** (often 04:00 / 05:00; stable across runs) |
| IB flags | Keep `useRTH=True`, `formatDate=1` — do not switch to `formatDate=2` | n/a |
| `last_date` | `MAX(date)` as stored | same |
| `overlap_start` | `last_date - timedelta(days=7)` (clock arithmetic; time part kept) | same |
| Compare window W | `[overlap_start, last_date]` closed-closed | same |
| Date-set rule | `existing ∩ W == new ∩ W` | same |
| Value rule | `math.isclose(..., rel_tol=1e-4, abs_tol=1e-4)` on value columns; `bar_count` not compared | same on sentiment metrics |
| Insert set | `date > last_date` only | same |
| Incomplete "today" | Fetch through now; extract drops `date > yesterday` (safe: keys are date labels) | Query `to=yesterday` with required dummy `HH:mm` (e.g. `00:00`); **no** post-extract UTC-midnight trim |
| Fetch margin | `duration = f"{max(gap_days + 7 + 2, 10)} D"` — margin sits **before** W so a short IB lookback still covers W; missing margin days must **not** trip equality | `from = (overlap_start - 2 days)` as `YYYY-MM-DD` (date part of the query only); `to = yesterday` as `YYYY-MM-DD`; dummy `00:00` required by the endpoint even though `bar_size=1D` ignores the clock |

**Fetch margin vs W:** the extra lookback days exist so IB/API off-by-one cannot omit the first day
of W. They are outside W. Equality is evaluated only on W. Dates `< overlap_start` are ignored for
compare and for insert.

**Sentiment query string:** the endpoint requires `from`/`to` with an `HH:mm` substring but does
not respect the clock when `bar_size=1D`. Keep dummy `00:00` (or any fixed time). Intra-series
04:00 vs 05:00 variation is server-chosen and stable across runs — store and compare as returned.

**Why not floor to UTC midnight:** flooring would break sentiment key stability (04:00 vs 05:00)
and can shift exchange trading dates for non-UTC markets if prices ever used unix instants.
`formatDate=1` already gives the correct exchange trading date as a calendar label.

**Overlap is a checksum only.** On a pass, do **not** upsert overlapping rows — only `date >
last_date`. Rewriting identical values adds work and bumps `updated_at` for no gain.

## 4. Schema changes (`etfportfolio/core/schema.sql`)

No migration — DB recreated from scratch. Add columns directly in `CREATE TABLE`:

```sql
-- on bronze.products
last_checked_at TIMESTAMP,

-- on bronze.snapshot_previews
last_checked_at TIMESTAMP,

-- on bronze.themes
last_checked_at TIMESTAMP,
```

Plus the full `cold_storage` schema in §3.11.

**Explicitly not added:**
- `bronze.contracts.last_checked_at` — `updated_at` already correct (§3.5.1).
- `bronze.snapshots.last_checked_at` — `fetched_at` already correct (§3.5.3).
- `phase_runs` table — rejected for pattern consistency (§3.5.4).

## 5. File-by-file change summary

| File | Change |
|---|---|
| `core/config.py` | Rename `endpoint_concurrency` → `details_concurrency`; add `sentiment_concurrency` (default 1), `freshness_window_hours` (default 24) |
| `core/utils.py` | Keep only `decompress_payload`; move content-addressing to `ingest/utils.py` |
| `core/db.py` | Move `store_blob`, `gc_preview_blob` to `ingest/utils.py`; keep `apply_schema`, `AsyncDbWorker` |
| `core/logging.py` | Install `TqdmLoggingHandler` so diagnostic logs coexist with tqdm; `--verbose` otherwise unchanged |
| `core/progress.py` | **New.** `TqdmLoggingHandler` + shared bar helper; bars disabled when stderr is not a TTY |
| `core/schema.sql` | Add `last_checked_at` to `products`, `snapshot_previews`, `themes`; add `cold_storage` schema + tables (§3.11) |
| `ingest/utils.py` | **New.** Content-addressing + `store_blob`/`gc_preview_blob` + consolidated timeseries overlap/replace/upsert (cold-storage archive on mismatch replace only) + `OVERLAP_CALENDAR_DAYS` + `is_fresh`. **No** datetime flooring helper |
| `ingest/endpoints.py` | Drop `shape`, `delay_before_request`. Add `landing` and `sentiment`. Remove `ownership`. |
| `ingest/session.py` | `fetch_with_retry`: 404 → `(404, None)` no retry; drop `NON_RETRYABLE_STATUS_CODES` and `delay_before_request` |
| `ingest/landing.py` | Resolve URL via `ENDPOINTS_BY_NAME["landing"]`. Guard `payload or {}`. Stamp `last_checked_at` per §3.5.2 rule |
| `ingest/snapshots.py` | Persist `payload or {}` on 404 so `fetched_at` is always written |
| `ingest/details.py` | Snapshot-only plan from `ENDPOINTS` excluding `{"landing","sentiment"}`; landing + per-endpoint freshness cache; `skipped_fresh` counts as satisfied; skip reporting; `--force` bypasses freshness + gate; sequential products, intra-product semaphore |
| `ingest/prices.py` | Use shared timeseries helper; window/fetch policy §3.12; keep `formatDate=1`, `useRTH=True`; cold-storage archive on mismatch replace only |
| `ingest/sentiment.py` | Full orchestration (`sync()`, product-level `sentiment_concurrency` semaphore); 404/empty skip write; shared timeseries helper; window/fetch policy §3.12; cold-storage on mismatch replace only |
| `ingest/contracts.py` | `_contract_is_fresh` uses `settings.freshness_window_hours` |
| `ingest/products.py` | `force: bool`; clean-vs-partial crawl tracking; bulk-stamp `last_checked_at` only on clean complete crawl; whole-phase skip + report line |
| `ingest/themes.py` | `force: bool`; stamp `last_checked_at` in existing transaction; whole-phase skip + report line |
| `ingest/pipeline.py` | `Ingest.products`/`themes` gain `force`; `_run_full` threads `force` to every phase that accepts it; Phase 7 Sentiment inside existing session scope |
| `pyproject.toml` | Drop `endpoint_concurrency`; add `details_concurrency`, `sentiment_concurrency`, `freshness_window_hours` |
| `tests/` | Unit tests for pure helpers; use `tests/fixtures/` where present |

## 6. Explicitly rejected / deferred

- **Global or LIFO semaphore for `details`** — rejected; sequential + sentiment split is the plan;
  product-level semaphore is the documented fallback only.
- **`EndpointNotFoundError` for 404** — rejected; plain status return.
- **`{"_absent": true}` sentinel** — rejected; bare `{}` for snapshots.
- **Gating `esg`/`theme_weights`** — rejected; per-endpoint freshness instead.
- **Freshness-window for `prices`/`sentiment`** — rejected; incremental last-date already covers it.
- **`phase_runs` table** — rejected; repeated `last_checked_at` column for pattern consistency.
- **Generic SQL-building helper for all freshness checks** — rejected; only time-math is shared.
- **Archiving on `--force`** — rejected; force is an explicit wipe.
- **Empty series body as mismatch** — rejected; treat as 404/skip write.
- **One-way subset date rule** — superseded by set equality on the compare window (§3.12).
- **Always-on tqdm (including non-TTY)** — rejected; disable when stderr is not a TTY.
- **Flooring / rewriting series timestamps to UTC midnight** — rejected; breaks sentiment key
  stability and can shift exchange trading dates (§3.12).
- **`formatDate=2` for prices** — rejected; keep `formatDate=1` + `useRTH=True`.

## 7. Known limitations (accepted risk, not deferred work)

- **Landing 404 masking real gated data** (§3.4): if landing 404s while gated endpoints would
  return data, repeating synthetic `{}` hashes permanently suppress gated fetches. Unlikely, no
  clean fix without per-widget gating. Accepted.
- **Sentiment permanent-404 re-fetch loop** (§3.4): always-404 products have no last-date signal
  and will attempt full historical refetch every run. Unobserved. Accepted; revisit if observed.

## 8. Out of scope

- Panel construction (monthly LOCF panels from bronze snapshots).
- Factor return series, factor selection, regression, efficient frontier.
- Anything in `gold` schema.
- Forward-looking note for panel phase: absent data is stored as `{}` for snapshots; treat "field
  not present in the JSON" as the normal missing-data case — no special unwrapping for
  confirmed-absent rows.

## Appendix A — Production log analysis (unchanged)

Source: full `ingest details` run over 22,531 products on pre-refactor code. Runtime ~14h.
228,934 log lines.

**Status codes among retained errors (48,606):** 404 97.1% · 503 2.8% · 500 0.1% · zero 401/403.

**Per-endpoint 404 rates:** ownership 98.0% (dropped) · theme_weights 51.9% · esg 48.5% ·
mstar 11.1%.

**Sentiment:** 708 products (3.1%) hit ≥1 503; 180 (0.8%) exhausted retries. Basis for
`sentiment_concurrency` default 1.