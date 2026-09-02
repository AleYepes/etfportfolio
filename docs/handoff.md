# Ingestion Refactor — FRD

## 0. Purpose & how to use this document

This is the output of an interview-driven design session covering the `etfportfolio` ingestion
subsystem (`etfportfolio/ingestion/`, plus the parts of `etfportfolio/core/` it depends on).
It supersedes `refactoring.md` — that document's open questions have all been resolved here.

Every decision below reflects an explicit choice made during the interview, not a default or an
assumption. Where a decision was informed by production log data, that data is cited so a fresh
agent can judge whether the reasoning still holds if circumstances change (e.g. request volumes,
error rates). Section 6 lists ideas that were considered and explicitly rejected — implementing
them would be relitigating settled ground, not filling a gap.

**Scope**: ingestion only. Panel-building, factor construction, regression, and efficient-frontier
phases are out of scope (Section 8), though a few forward-looking notes are left for whoever builds
the panel phase, since some ingestion-side choices (e.g. how absence is represented) affect it.

**No migration path is required or wanted.** The database is recreated from scratch after this
refactor lands. Schema changes below are additive/replacing, not migration scripts.

## 1. Guiding principles (carried over from the original brief)

- Clear, simple, clean, testable code. No hidden global state. No speculative abstractions.
- Async functions contain no SQL; SQL helpers are pure functions, independently callable/testable.
- Prefer replacing old functionality over accumulating deprecated-but-present alternatives.
- When a design choice is a toss-up, prefer whichever reads as clearer/leaner/more consistent with
  the rest of the codebase, even if it costs a little redundancy (e.g. some `last_checked_at`
  columns duplicate information for the sake of a uniform pattern — see §3.5).
- Don't build for hypothetical future needs. Several ideas in this document were rejected on this
  basis alone (cold storage revival, per-widget landing gating, sentiment permanent-404 handling)
  — not because they're bad ideas, but because nothing today needs them and they're cheap to add
  later if that changes.

## 2. Success criteria

- Request efficiency (`new_data_obtained / total_request_count`) and wall-clock runtime for a full
  `ingest details` + `ingest sentiment` pass both improve measurably over the current baseline.
- Baseline, for reference: a full run over 22,531 products took **~14h** under the *current*,
  unmodified code (sequential per-product processing, sentiment bundled into `details`, no
  freshness-window skipping at all). See §3.1 and Appendix A for the log analysis this baseline
  came from.
- Code is demonstrably cleaner and more testable: no SQL inside async functions; SQL helpers
  remain pure and independently callable.
- All phases continue to produce the same data outputs as today (mismatches aside — see the
  confirmed-absence handling in §3.4, which is a deliberate behavior change agreed on explicitly).

## 3. Decisions & rationale

### 3.1 Concurrency model for `details` — **no change, by design**

**Decision: keep per-product processing strictly sequential** (the existing `for idx, product_id in
enumerate(target_ids, 1): await ...` loop in `_run_details_phase`). Do not introduce a global
semaphore, a product-level semaphore, a LIFO primitive, or a two-stage pipeline.

**Why this isn't a compromise:** the ~14h baseline was dominated by sentiment, not by insufficient
concurrency. Per the log analysis (Appendix A), snapshot endpoint fetches for a product completed
in well under a second; `sentiment` fetches alone took ~4–5s each and were the long pole in every
per-product iteration. Once sentiment is split into its own phase (§3.2), a product's remaining
per-iteration cost collapses close to that sub-second floor. Rough projection: 22,531 products ×
~1s ≈ 6–7 hours for `details` alone, achieved *without* touching concurrency primitives.

**Why a global/product-level semaphore was rejected anyway:** a flat semaphore shared across every
request in every product creates a FIFO priority-inversion bug — a task that re-acquires the
semaphore mid-flight (e.g. a product's gated endpoints, spawned after its landing call returns)
queues *behind* every task that hasn't made its first acquire yet, not ahead of them. At n=22,531,
this means gated fetches barely progress until landing calls for nearly the whole product universe
are exhausted. A true LIFO semaphore would fix this structurally but isn't in the stdlib, and a
hand-rolled one was judged not worth the complexity once the sentiment split made it unnecessary.
A product-level semaphore (hold one slot for a product's *entire* lifecycle) would have avoided the
FIFO issue by construction, but was set aside in favor of the simpler "just split sentiment" fix,
which gets most of the win with zero new concurrency machinery.

**If this projection turns out wrong** (post-split `details` runtime is still unacceptably long),
the product-level-semaphore design is the fallback — it's documented here specifically so a future
agent doesn't have to rediscover the FIFO problem from scratch.

### 3.2 Splitting `sentiment` into its own phase

**Decision: full split.** `ingest details` becomes snapshot-only. A new `ingest sentiment` command
is added, structured like `prices.py` (own orchestrator: `_select_product_ids`, a `_run_..._ingestion`
loop, `sync()`), with its own `Ingest.sentiment()` CLI method.

**Rationale:** `sentiment` and the snapshot endpoints have different operational profiles — long
daily time series vs. small JSON blobs; different (and stricter) rate-limiting behavior; different
useful refresh cadence. Bundling them forced compromises on both (see `delay_before_request`,
removed below). Splitting lets each phase be tuned independently.

**Consequences:**
- `delay_before_request` (the field on `Endpoint` and its associated sleep-before-request logic in
  `session.fetch_with_retry`) is **removed entirely**. It was a band-aid for sentiment's rate
  limiting under the old bundled design; with dedicated, tunable `sentiment_concurrency` (§3.9) the
  need for it goes away.
- The `Endpoint.shape` field (`"snapshot"` | `"series"`) is **removed**. It existed so `details.py`
  could dispatch between snapshot storage and series-incremental logic; once `sentiment` is its own
  phase with its own dispatch, `details.py` never needs to make that distinction — everything it
  processes is a snapshot.
- **Session reuse in `_run_full`**: `sentiment` goes through the same web-portal `ensure_session()`
  client as `details` (not the IB Gateway client that `contracts`/`prices` use). In `_run_full`,
  reuse the *same* client/`account_id` already established for the Details phase — don't open a
  second session. A standalone `uv run main.py ingest sentiment` invocation still calls
  `ensure_session()` itself, exactly as `Ingest.details()` does today.
- **Phase ordering in `_run_full`**: insert as **Phase 7: Sentiment series**, immediately after
  Phase 6: Details, still inside the same `client`/`account_id` scope opened at Phase 4 (Session
  validation). Numbering stays additive (existing phases 1–6 keep their numbers) and phases sharing
  a session/connection stay adjacent, matching how Contracts/Prices are already grouped by their
  shared IB Gateway dependency.
- `sentiment.py` needs the orchestration layer it currently lacks (today it only exposes
  `fetch_incremental()`, called per-product from `details.py`). Build it out to mirror
  `prices.py`'s shape.

### 3.3 `ENDPOINTS` inventory changes

**Decision: fold `landing` and `sentiment` into the single `ENDPOINTS` list** (they're currently
absent — `landing`'s URL is hardcoded as `LANDING_URL_TEMPLATE` in `landing.py`, and `sentiment`'s
`Endpoint` already exists but is fetched via a bespoke path). One inventory of every IBKR endpoint
the codebase talks to, rather than a partial list plus stragglers.

**No new field to mark them as "special."** `details.py`'s plan-builder excludes `{"landing",
"sentiment"}` by name when building its per-product fetch plan; `landing.py` and `sentiment.py`
each look up their one entry via `ENDPOINTS_BY_NAME`. This avoids adding an enum/role field to the
`Endpoint` dataclass purely to describe two endpoints that don't share the others' generic
fetch-and-store shape.

**`ownership` is dropped entirely** — not added to `ENDPOINTS`, not added to the landing widget
query string. Production data (Appendix A) showed `ownership` returning 404 for 22,079/22,531
products (98.0%) — essentially no product in this universe has this data. This isn't a gating
question (§3.4), it's "this endpoint isn't worth calling." No schema cleanup is needed —
`ownership` was never a dedicated table, only rows in the generic `bronze.snapshots` blob store.

**Final `ENDPOINTS` membership and gating:**

| name | gated | shape (in old terms) | notes |
|---|---|---|---|
| `landing` | `False` (field unused — landing drives the gate, isn't gated by it) | n/a | newly added to the list; consumed only via `ENDPOINTS_BY_NAME`, excluded from `details`'s plan |
| `holdings` | `True` | snapshot | unchanged |
| `ratios` | `True` | snapshot | unchanged |
| `profile` | `True` | snapshot | unchanged |
| `lipper` | `True` | snapshot | unchanged |
| `mstar` | `True` | snapshot | unchanged |
| `esg` | `False` | snapshot | stays ungated (see §3.3.1) |
| `theme_weights` | `False` | snapshot | stays ungated (see §3.3.1) |
| `ownership` | — | — | **removed entirely** |
| `sentiment` | `False` (field unused — dataclass requires a value, but sentiment's own phase never consults it) | n/a | newly added to the list; consumed only via `ENDPOINTS_BY_NAME`, excluded from `details`'s plan, driven by the new `ingest sentiment` phase |

The `Endpoint` dataclass loses both `shape` and `delay_before_request` (§3.2); `gated` remains.

**Landing widget query string is unchanged from today**:
`objective,keyProfile,lipper_ratings,holdings,mf_key_ratios,mstar&lang=en` (an earlier draft of this
plan considered adding `ownership` as a widget and making it gated — that was superseded once the
log data showed `ownership` should be dropped outright, not gated).

#### 3.3.1 Why `esg`/`theme_weights` stay ungated (not widened to `gated=True`)

This was reconsidered explicitly and rejected. The landing hash only reflects the widgets in its
query string (`objective, keyProfile, lipper_ratings, holdings, mf_key_ratios, mstar`) — it has no
visibility into `esg` or `theme_weights` data at all. Gating them on a signal that can't see them
risks two failure modes simultaneously:

- **False negative (already named in `refactoring.md`)**: a transient failure on an unrelated
  gated endpoint would block the whole preview commit, which — after the 404-handling change in
  §3.4 — is largely mitigated for *4xx* failures, but not for genuine transient 5xx failures.
- **False positive**: if `esg`/`theme_weights` data changes independently of the widgets landing
  actually polls, gating them means the pipeline could silently stop refetching real changes,
  because nothing would ever mark them stale.

Instead, `esg`/`theme_weights` get the same benefit gating would have offered — avoiding redundant
requests — through the **per-endpoint freshness cache** (§3.5.3), which doesn't carry either risk.

### 3.4 Confirmed-absence (404) handling

This went through several rounds; the final design is simpler than earlier drafts (an
`EndpointNotFoundError` exception class and a `{"_absent": true}` sentinel payload were both
proposed and then dropped as unnecessary once production volume was known).

**Final design:**

1. **`session.fetch_with_retry` treats 404 as a normal, non-exceptional outcome.** Drop
   `NON_RETRYABLE_STATUS_CODES` and any bespoke exception class. On a 404, return `(404, None)`
   immediately — no retry, no raise. Every other status code's retry behavior is unchanged.
   *Why*: production logs show 404 accounted for 97.1% of all retained error records
   (47,189 of 48,606) across a single run. At that volume, treating it as an exceptional
   control-flow event is the actual anti-pattern; it's a routine, expected outcome of the domain
   (many products genuinely lack `esg`/`theme_weights`/etc. data).
2. **Callers check `status == 404` directly** and treat it as "confirmed absent, satisfied" rather
   than "failed." No special-casing needed in `_fetch_one` — it becomes agnostic to status code
   entirely.
3. **On a 404, still persist a record.** `store_snapshot` (and the equivalent landing/sentiment
   payload handling) is called with `payload or {}` in place of `None` — a bare empty dict, **no
   sentinel key**. This was corrected mid-design: an earlier draft proposed `{"_absent": true}` to
   let a future panel phase distinguish "confirmed absent" from "empty response," but no logic
   anywhere — landing's hash comparison, the freshness cache, or a hypothetical future
   LOCF-imputing panel phase — actually needs that distinction. LOCF imputation is indifferent to
   *why* a data point is missing. A sentinel key would only add work: something reading
   `bronze.snapshots` generically would have to know to strip it out. `payload or {}` requires no
   such special-casing anywhere downstream. This applies uniformly to `landing.fetch_and_gate`,
   `sentiment`'s per-point extraction, and `snapshots.store_snapshot`.
4. **Why persisting on 404 matters (not just returning gracefully)**: `bronze.snapshots.fetched_at`
   is what the per-endpoint freshness cache (§3.5.3) keys off. If a 404'd endpoint never gets a
   `bronze.snapshots` row, the freshness cache has no signal for it, and it will be re-requested
   on *every single future run, forever*. Production data shows this is not a rounding error: `esg`
   404'd for 10,927/22,531 products (48.5%), `theme_weights` for 11,695 (51.9%). Persisting a
   confirmed-absence row is the only way those two endpoints — now permanently ungated — actually
   benefit from the freshness cache.
5. **Landing specifically**: on a 404 landing response, `fetch_and_gate` content-addresses `{}`
   like any other payload. Repeated 404s hash identically to each other, so a product whose landing
   endpoint always 404s (most plausibly: the product was delisted from IBKR, which contract
   qualification should have caught but might not have) will, after the first occurrence, look
   "unchanged" on every subsequent run and correctly stop re-fetching its gated endpoints.
   If the product is ever "resuscitated" (landing starts returning real data again), the hash
   changes and gated fetching resumes automatically — no special handling needed.

**Accepted risk, not a gap to fix**: if a product's `landing` endpoint 404s while its *gated*
endpoints would actually return real data (considered unlikely but not proven impossible), the
repeating synthetic `{}` landing hash will permanently prevent those gated endpoints from ever
being fetched. There's no clean fix for this without reintroducing per-widget gating granularity,
which was deliberately rejected (§3.3.1, and originally in `refactoring.md`'s reasoning about
future panel time-series consistency). This is recorded as an accepted risk, not deferred work.

**Known limitation, also accepted rather than fixed**: `sentiment` has no equivalent freshness
signal (§3.5 deliberately excludes `prices`/`sentiment` from the freshness-window mechanism — they
already have their own incremental last-date logic). If a product's sentiment endpoint always
404s, `_get_last_date` will return `None` forever (nothing ever gets written to `bronze.sentiment`
for it), so every future run will attempt a full `1990-present` refetch, get 404 again, and repeat
— indefinitely. This is currently **unobserved** (0% 4xx on sentiment in the production run
analyzed, Appendix A) but not structurally impossible. Building a fix now would mean giving
`sentiment` its own freshness-checking machinery, which was explicitly kept out of scope for
`prices`/`sentiment` (§3.5). Leave unaddressed; revisit if it's ever actually observed.

**Also considered and rejected**: reviving a `cold_storage` schema (present in an earlier iteration
of the project, since removed) to archive `prices`/`sentiment` data before an overlap-mismatch
triggers `_replace_prices`/`_replace_sentiment`'s destructive delete-and-rewrite. No downstream
logic reads this today, it was already uncertain to be used, and it's unbounded ongoing storage
cost for a forensic capability with no concrete pull. If a mismatch ever turns out to need
investigation, it's a cheap, mechanical addition later (one `INSERT ... SELECT` before the existing
`DELETE`) — nothing about the current design forecloses adding it retroactively.

### 3.5 Freshness-window skip system

This is the largest structural addition. It generalizes the pattern `contracts.py` already
implements ad hoc (`_contract_is_fresh`, hardcoded 24h) into a shared, consistent mechanism applied
where it's actually useful, and explicitly *not* applied where it wouldn't help.

**What this actually buys you, concretely**: for the intended weekly cron cadence, a 24h freshness
window doesn't reduce request volume on a normal, uninterrupted weekly run — each run is naturally
~168h after the last one, well outside the window, so nothing gets skipped on that basis. Its real,
concrete benefit (the one that motivated it) is **resuming after an interruption**: a crashed or
manually `Ctrl+C`'d run, restarted shortly after, correctly re-skips whatever was already
successfully processed minutes or hours earlier instead of redoing it. The steady-state,
once-a-week efficiency gains come from elsewhere in this document — the sentiment split (§3.2) and
confirmed-absence persistence (§3.4) — not from this mechanism. Worth keeping straight so the two
kinds of "efficiency win" in this FRD aren't conflated.

**Scope — where it applies:**

| Phase | Mechanism | Column checked |
|---|---|---|
| `contracts` | per-product freshness check (existing, generalized) | `bronze.contracts.updated_at` |
| `details` (landing) | per-product freshness check (new) | `bronze.snapshot_previews.last_checked_at` (new column) |
| `details` (per-endpoint) | per-`(product_id, url_prefix)` freshness check (new), via in-memory cache | `bronze.snapshots.fetched_at` (existing column, no new one needed) |
| `products` | whole-phase freshness check (new) | `bronze.products.last_checked_at` (new column, bulk-set) |
| `themes` | whole-phase freshness check (new) | `bronze.themes.last_checked_at` (new column, bulk-set) |
| `prices` | **not applied** — existing incremental last-date logic already avoids redundant work | n/a |
| `sentiment` | **not applied** — same reasoning as `prices`; see the accepted limitation in §3.4 | n/a |

**Why `prices`/`sentiment` are excluded**: they already avoid redundant work through a different,
finer-grained mechanism (incremental fetch from last known date, with overlap validation). A
freshness-window skip would be redundant with that, not complementary to it.

**Shared config**: one setting, `settings.freshness_window_hours` (default `24`), used by every
phase above — not a separate setting per phase. Nothing in the current design suggests different
phases need different cadences; if that changes later, splitting it is a small, localized change.

**Shared helper**: a thin time-math function in `core/freshness.py`, e.g.
`is_fresh(last_seen: datetime | None, hours: float) -> bool`. Deliberately **not** a generic
SQL-query-building helper — the four query shapes involved (`WHERE product_id`, `WHERE product_id
AND url_prefix GROUP BY`, `WHERE phase`/bulk table check, IB-Gateway-sourced `updated_at`) are
different enough that forcing them through one SQL abstraction would itself be the kind of
speculative abstraction the guiding principles rule out. Each phase writes its own small, obvious
query and passes the result through the one shared time-comparison.

**`--force` bypasses all of this uniformly** (§3.8).

#### 3.5.1 `contracts` (generalized existing logic)

No new column. `_contract_is_fresh` keeps reading `bronze.contracts.updated_at` — `upsert_contract`
already runs unconditionally on every successful qualification, so `updated_at` already has
"checked" and "changed" semantics collapsed into one signal with no divergence to track. Only
change: swap the hardcoded `timedelta(hours=24)` for `settings.freshness_window_hours`.

#### 3.5.2 `details` — landing freshness

**New column: `bronze.snapshot_previews.last_checked_at TIMESTAMP`** (in addition to the existing
`updated_at`). These two columns serve different purposes and must not be collapsed:

- `updated_at` — bumped only when the landing hash actually **changes**.
- `last_checked_at` — bumped whenever landing was checked and the result reached a **stable**
  state, regardless of whether the hash changed.

This distinction matters because `updated_at` alone is the wrong signal for "was this recently
checked" — on a typical run where nothing changed, `updated_at` goes stale immediately and a
freshness check against it would almost never skip, defeating the purpose.

**Exact stamping rule for `last_checked_at`** (this is the fix for a real bug that was caught mid-design
and is worth stating precisely):

```
stamp last_checked_at  ⟺  NOT (fetch_gated AND NOT gated_success)
```

(recall `fetch_gated = force or changed`, already computed in `process_product` for plan-building —
this reuses it rather than introducing a new condition.)

In words: stamp it whenever no gated fetch was attempted this run (`fetch_gated == False` — landing
was confirmed unchanged and `force` wasn't set), or when a gated fetch *was* attempted and every
endpoint in it succeeded (`gated_success == True`). **Do not stamp it** when a gated fetch was
attempted and failed or was incomplete — that product must be re-examined on the next run, not
skipped for the freshness window.

Why `fetch_gated`, not `changed`, is the right condition to key on: they're equivalent whenever
`force` is `False`, which covers the failure mode named above (a clean, non-crash failure like a
gated endpoint exhausting its retries on a 503 would still mark landing as "recently checked" if
stamping only checked `changed`) — that's exactly the "false negative" failure mode `refactoring.md`
originally raised concern about. But the two diverge on `ingest details --force` when a product's
landing hash happens to be unchanged: `changed` is `False` there, yet `force` still triggers a
gated fetch. If that forced fetch fails, stamping on `changed` alone would mark the product as
stably checked anyway, silently hiding the failure until the freshness window expires on its own.
Keying on `fetch_gated` instead handles both cases — organic and forced — with the same single
condition.

A `KeyboardInterrupt` or crash mid-flight never reaches the stamp step at all (by construction, not
by explicit handling), so a genuinely interrupted run's products are correctly retried in full next
time — this is the resume-after-crash benefit that motivated the freshness-window idea in the first
place.

**Consequence for `fetch_gated`**: once landing itself can be skipped as "recently checked," the
`fetch_gated` decision on a skipped-landing product degrades to just `force` — there's no organic
"landing changed" signal to react to if landing wasn't even checked this run.

#### 3.5.3 `details` — per-endpoint freshness (gated *and* ungated)

Every individual snapshot endpoint fetch (`holdings`, `ratios`, `profile`, `lipper`, `mstar`, and
now permanently-ungated `esg`/`theme_weights`) gets a freshness check before being attempted —
this is what lets `esg`/`theme_weights` benefit from freshness skipping without needing to be
gated (§3.3.1).

**No new column needed** — `bronze.snapshots.fetched_at` already has exactly the right semantics:
it's only ever written via `store_snapshot`, which (post-§3.4) is called on every attempt whether
it succeeds or confirmed-404s. So `MAX(fetched_at)` per `(product_id, url_prefix)` already means
"last time this was checked," with no divergence-tracking needed. (A `last_checked_at` column was
considered here purely for naming consistency with the other tables and explicitly rejected — it
would be a guaranteed-always-equal duplicate of existing data, and `bronze.snapshots` is the table
most likely to accumulate a large number of rows over time, making that duplication the most
expensive place to add it for a purely cosmetic gain.)

**In-memory cache, not per-product queries**: run two queries once, before any product tasks are
dispatched for the `details` run — not once per product:

```sql
-- 1. Landing freshness, keyed by product_id
SELECT product_id, last_checked_at FROM bronze.snapshot_previews;

-- 2. Per-endpoint freshness, keyed by (product_id, url_prefix)
SELECT product_id, url_prefix, MAX(fetched_at)
FROM bronze.snapshots
GROUP BY product_id, url_prefix;
```

Load both into plain dicts (`dict[int, datetime]` and `dict[tuple[int, str], datetime]`) and pass
them as read-only arguments down through `_run_details_phase → process_product → plan-building`.
**Not module globals** — passed explicitly, consistent with "no hidden global state." No mid-run
refresh: each product's freshness decision only depends on data from *prior runs*, never on other
products processed earlier in the same run, so one snapshot at the start is correct, not just an
optimization.

**Where filtering happens**: inside `process_product`, at plan-build time — partition the endpoint
plan into `to_fetch` vs. `skipped_fresh` *before* any tasks are created. A skipped endpoint never
becomes a task at all (simpler than a live check inside `_fetch_one`, which stays unaware of
freshness entirely).

**How skips count toward success/gating**: `skipped_fresh` endpoints count as **satisfied**, on par
with a real success — "checked and confirmed fresh" and "checked and it worked" are equivalent
outcomes for gating purposes. Concretely: the `results` list from `asyncio.gather` only covers
`to_fetch`; both `gated_success` and the final `all(r is True for r in results)` computation need
`skipped_fresh` entries folded in as trivial successes, not merely ignored — otherwise a
100%-fresh product would look like it fetched zero things and only vacuously "succeed" by accident
of an empty list, rather than by explicit design.

#### 3.5.4 `products` / `themes` — whole-phase freshness

Neither `sync()` is per-product — both crawl/replace the whole table — so there's no natural row
to check per invocation. **New column on both tables**: `bronze.products.last_checked_at` and
`bronze.themes.last_checked_at TIMESTAMP`, repeated identically across every row in the table for a
given run. This does waste some space (rejected an alternative dedicated `phase_runs` state table
for the same reason cited throughout: added structural complexity — an extra table — isn't worth it
for what's fundamentally simple "did we run this recently" bookkeeping; a repeated column value is
the simpler, more consistent choice given the pattern already established elsewhere).

**Why not infer staleness from `MAX(updated_at)` instead of adding a column**: this was considered
and is actually wrong, not just less clean. An unchanged run doesn't bump any row's `updated_at` at
all — `themes.sync()`'s update path is conditional on the row's data differing, and `products.sync()`
similarly only touches `updated_at` on `ON CONFLICT ... DO UPDATE`. Using `updated_at` as a
staleness proxy would make the phase look perpetually stale on a quiet run and never skip — the
opposite of the intended behavior. This is the same class of mistake §3.5.2 explicitly avoided for
landing.

**`themes`**: `sync()` already runs its whole payload inside one `BEGIN/COMMIT/ROLLBACK`
transaction, so stamping `last_checked_at` on all rows at the end of that same transaction is
already safe — no new failure mode to guard against.

**`products`**: different — `sync()` is an unenclosed multi-page crawl loop that can `break` early
on a page error and return a partial count *without raising*. Bulk-stamping `last_checked_at` per
page as it goes would leave earlier pages looking "freshly checked" even after a later page failed,
so a rerun within the freshness window would skip the crawl and never pick up the missing pages.
**Fix**: track whether the loop reached its natural termination condition (a page shorter than
`PAGE_SIZE`, meaning pagination is genuinely exhausted) versus broke early on an error. Only run
`UPDATE bronze.products SET last_checked_at = now()` (a single bulk statement, not per-page) after
a confirmed clean, complete crawl. This is one added boolean flag checked once at the end — not a
per-page cost.

**`Ingest.products()` / `Ingest.themes()` need a `force: bool = False` parameter** (neither has one
today) so `--force` can bypass the whole-phase skip, consistent with §3.8. `_run_full` must thread
its own `force` argument into both calls — today it calls `products.sync()` / `themes.sync()` with
no `force` argument at all (a pre-existing gap, unrelated to this refactor, that becomes a real
inconsistency once the parameter exists and other phases receive it). Per explicit clarification:
`uv run main.py ingest --force` should pass `force` to every phase/function in the full run that
accepts it — `products`, `contracts`, `prices`, `details`, and `sentiment` alike.

#### 3.5.5 Skip reporting

Two related, deliberately terse additions — matching the existing minimal style, not expanding it:

- **`products`/`themes`, whole-phase skip**: one line before returning early, e.g.
  `"Products sync skipped (checked {hours}h ago; use --force to refresh)."` — same pattern for
  themes. Gives an immediate answer to "why did this run so fast" without adding a real report.
- **`details`, per-product/per-endpoint skip counts**: enrich the existing terse post-phase summary
  in `_run_details_phase` (today: `"Done. {n}/{total} products fully succeeded..."`) with skip
  counts, e.g. `"details: 18,204/22,531 products skipped (fresh), 4,327 processed; 1,204 endpoint
  requests skipped (fresh) among processed products."` Cheap counters accumulated during the
  existing loop, printed once. This directly answers the request-efficiency question the whole
  effort is aimed at, and gives a concrete number to sanity-check the design against once it's
  running for real. Keep it to roughly this length — it should not grow into a multi-line report.

### 3.6 Logging & progress (tqdm)

**Decision**: tqdm progress bars run on **every** invocation (not gated behind `--verbose`) as the
standard progress indicator for any per-item loop (currently: `contracts`, `prices`, `sentiment`,
`details`; likely extending to future `panels`/`regression` phases). `--verbose` continues to
control whether diagnostic log lines are emitted at all — that existing behavior is unchanged, only
*where* those lines render changes.

**What replaces what**: the current per-product `console.info(f"[{idx}/{len}] Processing product
{id}...")` line is replaced by the tqdm bar itself (`total` = product count) with a postfix showing
the current product_id. Phase banners (`console.info("=== Phase N: ... ===")`) and the final
post-phase summary (§3.5.5) remain plain `console.info` prints, untouched — they bracket a phase,
they don't compete with its bar for the same line.

**The coexistence problem**: tqdm doesn't automatically play nicely with `logging`'s
`StreamHandler`s — a log line printed while a bar is active corrupts the bar's rendering unless
routed through `tqdm.write()`. This needs a small custom logging handler (roughly ~10 lines):
`TqdmLoggingHandler`, installed by `configure_logging` on the stderr handler (and on the `console`
channel specifically while a bar is active), routing every emitted record through `tqdm.write()`
instead of a raw stream write.

**Where it lives**: `core/progress.py` (new file) — `core/` is a package, not a single file, so this
is a new module alongside `db.py`, `config.py`, `logging.py`, not a monolithic `core.py`. Lives in
`core/` specifically because every future phase directory (panels, regression, etc.), not just
`ingest/`, will want the same bar+logging coexistence.

### 3.7 Module reorganization

Two moves, both justified by "used across multiple scripts within `ingest/`" — not a general
reshuffle. The operating rule agreed on: something used in only one script stays declared in that
script; something shared across multiple scripts within a phase-directory gets its own shared
module. The real inter-module boundary is the *phase directory* (`ingest/`, future `panels/`, etc.)
— `core/` is reserved for what every phase directory needs, not for `ingest`-internal sharing.

- **`core/utils.py`** keeps only `decompress_payload` (used outside `ingest/`, by future
  cross-phase consumers reading stored payloads back). `content_address`, `canonical_bytes`, and
  their private helpers (`_TYPE_RANK`, `_sort_key`, `_canonicalize`) move to **`ingest/utils.py`** —
  they're used by both `landing.py` and `snapshots.py`, so they're ingestion-shared, not
  cross-phase-shared.
- **`core/db.py`**: `store_blob` and `gc_preview_blob` move to **`ingest/utils.py`** as well — same
  reasoning, used only by `landing.py`/`snapshots.py`. `core/db.py` keeps `apply_schema` and
  `AsyncDbWorker`, which every phase directory genuinely needs.
- **New**: the `prices.py`/`sentiment.py` overlap-validate-and-upsert duplication
  (`_validate_overlap`, `_replace_*`, `_upsert_*` — structurally identical between the two modules,
  differing only in table/column names) gets consolidated into one generic, table/column-parameterized
  helper, also placed in **`ingest/utils.py`** (shared by exactly those two scripts). Both existing
  implementations are already pure, independently-testable SQL helper functions — this is a DRY
  consolidation of existing duplication, not a new abstraction layer, and the success-criteria bar
  ("SQL helpers independently callable") is already met either way. `prices.py` and `sentiment.py`
  drop their local copies and call the shared helper instead — see §5 for the file-by-file impact
  on `prices.py` specifically, which is easy to miss since only `sentiment.py`'s duplication was
  discussed at length during the interview.

Net result: `ingest/utils.py` ends up holding three related-but-distinct things — content-addressing,
blob-store helpers, and the timeseries overlap/upsert helper. This was weighed against splitting
into `ingest/payloads.py` + `ingest/timeseries.py`, but per the explicit low-priority preference
stated during the interview ("avoid creating more scripts unless something is used in multiple
scripts... this is a very low-priority DX preference, we don't want to turn this into a
script/function reshuffling job"), keep it to the one file unless it becomes unwieldy in practice.

`ingest/gateway.py` is unaffected and already follows this same pattern correctly (shared by
`contracts.py` and `prices.py` for their IB Gateway connection) — noted for consistency, not as a
change.

### 3.8 `--force` semantics

**One flag, all freshness mechanisms bypassed.** For `details` specifically: `--force` means
"ignore the freshness window(s) *and* ignore the landing gate — do a fully unconditional refetch,"
not two separately-triggerable behaviors. This was considered and rejected in favor of the simpler
model: two different "force-shaped" flags on one command would be confusing, and "force means don't
skip anything" is the simplest mental model, matching how `contracts --force` already behaves
today.

This applies uniformly across every phase that gets a freshness mechanism in this refactor:
`contracts`, `details` (both landing and per-endpoint), `products`, `themes`. `prices`/`sentiment`
don't have a freshness-window `--force` interaction since they were excluded from that mechanism
(§3.5) — their existing `force` parameter continues to mean what it already means (full replace vs.
incremental).

`_run_full`'s single `--force` flag threads through to every phase call that accepts a `force`
parameter, including the two that gain one for the first time in this refactor (`products`,
`themes` — see §3.5.4).

### 3.9 Settings / config changes

- **Rename** `endpoint_concurrency` → **`details_concurrency`**. Now that `details` is
  snapshot-only, this can likely go higher than the current default — the snapshot endpoints showed
  no meaningful 5xx rate even under today's effectively-low concurrency (Appendix A).
- **New** `sentiment_concurrency`, default **`1`**. Deliberately conservative: production data shows
  708/22,531 products (3.1%) hit at least one `sentiment` 503 *even under today's near-sequential
  processing* (at most one sentiment call in flight system-wide, competing only with that same
  product's own snapshot calls) — meaning the rate limiting looks endpoint-side, not client-side,
  and shouldn't be assumed to improve just because concurrency is nominally raised. Defaulting to 1
  makes the phase practically sequential (with some orchestration overhead) but gets the config
  wired up so real concurrency is a one-line change once `ingest sentiment` has run standalone and
  its real 503 behavior can be observed in isolation, decoupled from the snapshot endpoints'
  behavior for the first time.
- **New** `freshness_window_hours`, default **`24`** — single shared setting used by every phase in
  §3.5's scope (not a per-phase setting).
- `[tool.etfportfolio]` in `pyproject.toml` needs updating to match: drop `endpoint_concurrency`,
  add `details_concurrency`, `sentiment_concurrency`, `freshness_window_hours`.

## 4. Schema changes (`etfportfolio/core/schema.sql`)

No migration needed — DB is recreated from scratch.

**Additive columns:**
```sql
ALTER TABLE bronze.products           ADD COLUMN last_checked_at TIMESTAMP;
ALTER TABLE bronze.snapshot_previews  ADD COLUMN last_checked_at TIMESTAMP;
ALTER TABLE bronze.themes             ADD COLUMN last_checked_at TIMESTAMP;
```
(Expressed as `ALTER` for clarity; since the DB is rebuilt from scratch, these should actually be
added directly into each table's `CREATE TABLE` statement in `schema.sql`, not applied as a
migration.)

**Explicitly not added**, with reasoning cross-referenced above:
- `bronze.contracts.last_checked_at` — `updated_at` already has the right semantics (§3.5.1).
- `bronze.snapshots.last_checked_at` — `fetched_at` already has the right semantics (§3.5.3); this
  is the table most likely to accumulate many rows, making it the worst place to add a purely
  redundant column.
- A `cold_storage` schema for `prices`/`sentiment` — rejected, no current consumer (§3.4).
- A dedicated `phase_runs` state table for `products`/`themes` — rejected in favor of a column on
  each table, for consistency with the row-level pattern used elsewhere (§3.5.4).

**No structural change** for the `ownership` endpoint's removal — it was never a dedicated table,
so there's nothing to drop.

## 5. File-by-file change summary

| File | Change |
|---|---|
| `core/config.py` | Rename `endpoint_concurrency`→`details_concurrency`; add `sentiment_concurrency` (default 1), `freshness_window_hours` (default 24) |
| `core/utils.py` | Keep only `decompress_payload`; move content-addressing helpers to `ingest/utils.py` |
| `core/db.py` | Move `store_blob`, `gc_preview_blob` to `ingest/utils.py`; keep `apply_schema`, `AsyncDbWorker` |
| `core/logging.py` | Install `TqdmLoggingHandler` on stderr (and console-while-bar-active) so diagnostic logs coexist with tqdm bars; `--verbose` behavior otherwise unchanged |
| `core/freshness.py` | **New.** `is_fresh(last_seen, hours) -> bool` — thin time-math helper only, no query building |
| `core/progress.py` | **New.** `TqdmLoggingHandler` + shared bar-creation helper for any per-item phase loop |
| `core/schema.sql` | Add `last_checked_at` to `products`, `snapshot_previews`, `themes` (§4) |
| `ingest/utils.py` | **New.** Content-addressing (`content_address`, `canonical_bytes`, private helpers) + `store_blob`/`gc_preview_blob` (moved) + consolidated `prices`/`sentiment` overlap-validate-upsert helper (new consolidation) |
| `ingest/endpoints.py` | `Endpoint` loses `shape`, `delay_before_request`. `landing` and `sentiment` added to `ENDPOINTS`. `ownership` removed entirely. |
| `ingest/session.py` | `fetch_with_retry`: drop `NON_RETRYABLE_STATUS_CODES` and the bespoke exception path; return `(404, None)` directly, no retry, on a 404; drop the `delay_before_request` parameter and its associated sleep-before-request logic entirely (§3.2) |
| `ingest/landing.py` | Widget query string unchanged (no `ownership`). `fetch_and_gate` guards `payload or {}`. `last_checked_at` stamping per the §3.5.2 stability rule (keyed on `fetch_gated`, not `changed`). |
| `ingest/snapshots.py` | `store_snapshot`/`fetch_snapshot` persist `payload or {}` on a 404 rather than skipping storage, so `fetched_at` is always written |
| `ingest/details.py` | Drop shape-based endpoint split; plan built from `ENDPOINTS` excluding `{"landing","sentiment"}` by name; per-endpoint + landing freshness cache passed in and applied at plan-build time; `skipped_fresh` counted as satisfied for gating/success; skip-count reporting (§3.5.5); `--force` bypasses freshness + gate |
| `ingest/prices.py` | Remove local `_validate_overlap`, `_replace_prices`, `_upsert_prices`; call the consolidated timeseries helper in `ingest/utils.py` instead (§3.7) — no behavior change, pure deduplication |
| `ingest/sentiment.py` | Remove `delay_before_request` usage; add full orchestration layer (`_select_product_ids`, `_run_..._ingestion`, `sync()`) mirroring `prices.py`; own `sentiment_concurrency` semaphore; `payload or {}` guard before point extraction; call the consolidated timeseries helper in `ingest/utils.py` for its overlap/upsert logic (§3.7) |
| `ingest/contracts.py` | Fix pre-existing Python-2-syntax bug (`except AttributeError, KeyError:` → `except (AttributeError, KeyError):`), unrelated to this refactor but currently prevents the module from importing at all under Python 3.14; `_contract_is_fresh` reads `settings.freshness_window_hours` instead of hardcoded 24h |
| `ingest/products.py` | Add `force: bool` param to whatever wraps `sync()` for CLI purposes; track clean-vs-partial crawl completion; bulk-stamp `last_checked_at` only on a clean complete crawl; whole-phase skip check + skip-report line |
| `ingest/themes.py` | Add `force: bool` param; stamp `last_checked_at` on all rows within the existing single transaction; whole-phase skip check + skip-report line |
| `ingest/pipeline.py` | `Ingest.products()`/`Ingest.themes()` gain `force` params; `_run_full` threads its own `force` into every phase call that accepts one (`products`, `contracts`, `prices`, `details`, `sentiment`); new "Phase 7: Sentiment series" inserted after Phase 6, inside the existing session scope |
| `pyproject.toml` | `[tool.etfportfolio]`: drop `endpoint_concurrency`; add `details_concurrency`, `sentiment_concurrency` (default 1), `freshness_window_hours` (default 24) (§3.9) |

**Prerequisite note**: the `contracts.py` syntax fix isn't optional or schedulable alongside the
rest — the module currently cannot be imported at all under Python 3.14 (`except X, Y:` is Python 2
syntax and raises `SyntaxError` on import). Nothing involving `contracts.py`, and nothing that
transitively imports it, can be tested until this is fixed first.

## 6. Explicitly rejected / deferred ideas

Recorded so a future agent doesn't reopen settled ground without new information:

- **Global or LIFO semaphore for `details`** — rejected in favor of the sentiment split alone
  solving the throughput problem; documented as a fallback if the projection in §3.1 proves wrong.
- **`EndpointNotFoundError` exception class for 404 handling** — rejected once volume data showed
  404 is a routine, high-frequency outcome (97.1% of all errors in one run), not an exceptional one;
  replaced by a plain status-code check.
- **`{"_absent": true}` sentinel payload for confirmed-absence** — rejected in favor of bare
  `payload or {}`; no logic anywhere (present or reasonably foreseeable) needs to distinguish
  "confirmed absent" from "empty response," and a sentinel key would require special-casing on the
  read side for no corresponding benefit.
- **Gating `esg`/`theme_weights`/(the now-removed) `ownership`** — rejected; the per-endpoint
  freshness cache gets equivalent request savings without the false-positive/false-negative risk of
  gating on a landing hash that can't see this data (§3.3.1).
- **Reviving `cold_storage` for `prices`/`sentiment` mismatch archiving** — rejected for now, no
  current consumer; cheap to add later if a real need appears (§3.4).
- **Freshness-window mechanism for `prices`/`sentiment`** — rejected; they already have equivalent
  protection via incremental last-date fetching, and there's no signal that would help beyond that
  (the sentiment permanent-404 limitation in §3.4 is a known gap in that existing incremental
  logic, not something the freshness-window mechanism would fix without its own new machinery).
- **A dedicated `phase_runs` state table for whole-phase freshness** — rejected in favor of a
  repeated `last_checked_at` column on `bronze.products`/`bronze.themes`, for pattern consistency
  over marginal storage efficiency (§3.5.4).
- **One generic SQL-building helper for all freshness checks** — rejected; the query shapes differ
  enough (per-product, per-product-and-prefix-grouped, whole-table) that forcing them through one
  abstraction would itself be premature; only the time-comparison math is shared (§3.5).

## 7. Known limitations (accepted risk, not deferred work)

- **Landing 404 masking real gated data** (§3.4): if a product's landing endpoint 404s while its
  gated endpoints would return real data, repeating synthetic landing hashes permanently prevent
  those gated endpoints from ever being fetched. Considered unlikely, not proven impossible, no
  clean fix without reintroducing per-widget gating (rejected for good reason elsewhere). Accepted.
- **Sentiment permanent-404 re-fetch loop** (§3.4): a product whose sentiment endpoint always 404s
  will trigger a full historical refetch attempt on every single run, forever, since sentiment has
  no "confirmed checked" signal of its own. Currently unobserved in production. Accepted; revisit
  if it starts occurring.

## 8. Out of scope

- Panel construction (monthly LOCF panels from the accumulated bronze snapshots).
- Factor return series construction, factor selection, regression, efficient-frontier calculation.
- Anything in `gold` schema (currently reserved/empty).
- One forward-looking note for whoever *does* build the panel phase: because absent data is now
  stored as `{}` rather than omitted or specially marked, panel-building logic that flattens
  `bronze.snapshots` payloads into columns should treat "field not present in the JSON" as its
  normal missing-data case — no special unwrapping or filtering step is needed for confirmed-absent
  rows, they behave identically to a payload that's merely missing a few fields.

## Appendix A — Production log analysis this FRD is based on

Source: a full `ingest details` run over 22,531 products on the pre-refactor codebase (sequential
per-product, sentiment bundled into `details`, no freshness-window skipping, `delay_before_request`
active for sentiment). Runtime: **~14 hours**. 228,934 log lines analyzed.

**Status codes among retained error records (48,606 total):**
- 404: 47,189 (97.1%)
- 503: 1,355 (2.8%)
- 500: 62 (0.1%)
- Zero 401/403 — session handling stayed valid for the entire run, no session-related churn.

**Distinct products affected:**
- 404: 22,167 products (~98% of the universe hit at least one 404 somewhere)
- 503: 718 products (3.2%)

**Per-endpoint 404 rates (products affected / 22,531 total):**
- `ownership`: 22,079 (98.0%) → **dropped from `ENDPOINTS` entirely** (§3.3)
- `theme_weights`: 11,695 (51.9%) → stays ungated, gains freshness caching (§3.3.1, §3.4)
- `esg`: 10,927 (48.5%) → stays ungated, gains freshness caching (§3.3.1, §3.4)
- `mstar`: 2,499 (11.1%) → stays gated; this is exactly the case §3.4's confirmed-absence
  handling protects — a *gated* endpoint 404ing this often would have poisoned the landing-preview
  commit on every affected product, every run, prior to this fix.

**Sentiment retry/failure profile**: 708 products (3.1%) hit at least one 503; 206 needed a second
retry; 180 (0.8% of the universe) exhausted all 3 retries and failed completely for that run
(non-fatal — sentiment's incremental design picks failed products up automatically next run). This
occurred under effectively-sequential concurrency (at most one sentiment call in flight system-wide
at any time), which is the basis for treating sentiment's rate limiting as endpoint-side rather than
concurrency-driven, and for defaulting `sentiment_concurrency` conservatively (§3.9).

**Error concentration by hour** climbed from ~200/hr at midnight to a peak of ~7,000/hr around
noon — consistent with the vast majority of "errors" being routine 404s hit at whatever rate
products were being processed, not a time-of-day-correlated failure mode.