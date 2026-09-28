# Functional Design Record (FDR): Ingestion Architecture Simplification & Universal Rate Limiting

* **Status:** Approved
* **Subsystem:** Ingestion Layer (`etfportfolio.ingest`, `etfportfolio.core`)
* **Layer:** Medallion Bronze Layer

---

## 1. Executive Summary & Design Rationale ("Why")

The ETF ingestion subsystem acquires ETF fundamentals, holdings, taxonomy weights, and ratings across seven granular IBKR Web REST endpoints (`holdings`, `ratios`, `profile`, `lipper`, `mstar`, `esg`, and `theme_weights`), alongside daily prices and contracts qualified via IB Gateway.

Historically, snapshot ingestion in `details.py` utilized a two-tier "landing gate" canary pattern:
1. It fetched a composite `/tws.proxy/fundamentals/landing/{product_id}` widget payload.
2. It content-addressed and compared the digest against `bronze.snapshot_previews`.
3. If the preview digest was unchanged and fresh, it skipped the five "gated" endpoints (`holdings`, `ratios`, `profile`, `lipper`, `mstar`).

### 1.1 Why Decommission the Landing Gate Outright?
* **Stale Canary Desynchronization:** IBKR's edge servers cache and serve the composite landing widget independently from underlying granular endpoints. A fund's holdings, Morningstar pillar ratings, or Lipper scores frequently update on IBKR's backend while the landing endpoint remains static. Treating the composite payload as an authoritative gatekeeper indefinitely blocked fresh fundamental data from ever being ingested.
* **Dead Code & Redundant Storage:** The composite landing endpoint was never parsed into downstream silver models (`silver.observations`); it existed exclusively to drive the gating check.
* **Architectural Simplification:** Eliminating the canary allows the complete removal of `landing.py`, `tests/ingest/test_landing.py`, the `bronze.snapshot_previews` database table, dedicated preview blob garbage collection (`gc_preview_blob()`), dual-tier freshness caches, and bifurcated endpoint definitions (`GATED_ENDPOINTS` vs. `UNGATED_ENDPOINTS`).

### 1.2 Why Pure Per-Endpoint Freshness?
Rather than maintaining a synthetic product-level gating table, `bronze.snapshots` already stores `(product_id, url_prefix, last_checked_at)`.
* Freshness is determined solely by checking whether `(product_id, url_prefix)` has a `last_checked_at` within `settings.freshness_window_hours`.
* A product is skipped with zero network requests if and only if **all** configured endpoints are fresh.
* If a subset of endpoints is stale, only the stale endpoints are fetched.

### 1.3 Why Preserve Sentinel Payloads (HTTP 200 & HTTP 404)?
* **Empirical Invariance:** HTTP 404 and empty HTTP 200 responses represent valid, confirmed absences on IBKR's platform (e.g. an ETF lacking Morningstar ratings or ESG disclosures). Repeatedly refetching them yields identical empty responses.
* **Sentinel Persistence:** HTTP 404 and empty/non-JSON 200 responses are stored as empty dictionaries `{}` in `bronze.snapshots` and stamp `last_checked_at`. This prevents constant, wasteful refetching of missing endpoints across runs.
* **No Premature Ingestion Filtering:** Ingestion must not presume what constitutes a "valid" payload. Ingestion remains maximally permissive; parsing, shape validation, and domain-specific interpretation belong strictly to post-hoc silver extraction in `etfportfolio.prep`.

### 1.4 Why Windowed / Epoch-Based 429 Rate Limiting?
Under concurrency, multiple asynchronous tasks encountering HTTP 429 simultaneously would naively double their backoff independently, resulting in compounding delays ($2^N$ seconds for $N$ workers).
* **Epoch-Based Cooldown:** When task A encounters 429 at $t_0$, it opens a pause window (`pause_until = now + delay`) and doubles the delay for the *next* wave. Other concurrent tasks encountering 429 within the same wave observe `now < pause_until` and do not escalate backoff further.
* **Jittered Resume:** On resume, workers add a random jitter ($\in [0.1, 0.5]\text{s}$) to avoid waking in lockstep and triggering immediate subsequent 429s (thundering herd).
* **Quiet-Period Reset:** Delay resets to baseline (`initial_delay`) only after a quiet period of successful traffic (`cooldown_seconds`) elapses without any 429s.
* **Respecting `Retry-After`:** If IBKR provides a `Retry-After` response header, the throttler enforces `max(retry_after, current_delay)`.
* **Explicit Lifecycle:** The `RateLimiter` is instantiated once per pipeline run and passed explicitly to HTTP clients/workers. No hidden global state or module-level singletons.

### 1.5 Why Sequential Product Iteration with Intra-Product Concurrency?
* `details.py` iterates through target products **sequentially**, executing only the stale endpoints of the active product concurrently (up to 7 parallel tasks per product, bounded by a semaphore).
* This provides significant speedup over purely serial execution while keeping network load gentle on IBKR servers, preventing thundering-herd 429 cascades, ensuring clean sequential writes to DuckDB via `AsyncDbWorker`, and maintaining deterministic progress bar updates (`bar.set_postfix_str(str(product_id))`).

### 1.6 Why Granular Partial Checkpointing & Continuation?
If an endpoint exhausts its retries (e.g. persistent 500 error or hard 429):
* `details.py` logs the failure, marks that endpoint failed, and continues processing remaining endpoints and products.
* Successfully fetched endpoints commit their snapshots and advance `last_checked_at` in DuckDB.
* Failed endpoints leave `bronze.snapshots` unwritten (or stale), ensuring they are automatically retried on the subsequent run without discarding already-acquired data.

### 1.7 Why Decouple Product Discovery from Session State?
* The product discovery endpoint (`/webrest/search/products-by-filters`) is public and unauthenticated; it does not require portal session cookies.
* `products.py` must not load session cookies from disk or invoke `session.ensure_session()`. It constructs a dedicated, clean HTTP client with browser headers, sharing only the run's `RateLimiter`.
* In `pipeline._run_full()`, IB Gateway phases (Phase 2 Contracts and Phase 4 Prices) can take several hours. Session validation is intentionally deferred to Phase 5 (immediately prior to themes and details) so the authenticated browser session does not expire while IB Gateway is running. Neither `_run_full()` nor standalone `main.py ingest products` calls `ensure_session()`.

### 1.8 Why Keep Production Schema Pure (No In-Code Migration Shims)?
* In accordance with repository guidelines (*"Delete obsolete logic outright; do not add compatibility shims or fallback layers"*), `etfportfolio/core/schema.sql` represents only the clean, desired medallion state. No `DROP TABLE` statements or migration shims belong in production application schema files.
* A standalone one-off migration script is placed in `scripts/migrate_drop_snapshot_previews.py` to drop the obsolete table, clean orphaned blobs, and checkpoint DuckDB for existing local databases.

---

## 2. Pipeline Execution Sequence

In `etfportfolio/ingest/pipeline.py`, the sequence of operations in `_run_full()` is:

```
[Phase 1: Product Discovery] ──> Unauthenticated POST crawl; uses dedicated client & RateLimiter.
             │
[Phase 2: Contract Qualification] ──> IB Gateway (clientId=1). Takes significant time.
             │
[Phase 3: FX Series] ───────────────> yfinance foreign exchange series.
             │
[Phase 4: Price Series] ────────────> IB Gateway (clientId=2). Takes significant time.
             │
[Phase 5: Session Validation] ──────> Validates portal cookies; launches Playwright login if expired.
             │
[Phase 6: Theme Taxonomy] ──────────> Authenticated GET request using validated session client.
             │
[Phase 7: Product Details] ─────────> Pure per-endpoint freshness; fetches stale endpoints concurrently.
             │
[Phase 8: Storage Cleanup] ─────────> Purges cold storage runs & unreferenced bronze payload blobs.
```

---

## 3. Response Classification & Outbound Routing Matrix

All outbound HTTP calls in `products.py`, `themes.py`, `details.py`, and `snapshots.py` route through `fetch_with_retry`. The table below defines how each response status and payload condition is handled:

| HTTP Status / Condition | Classification | Transport & RateLimiter Action | Bronze Snapshot Storage | Freshness Impact |
| :--- | :--- | :--- | :--- | :--- |
| **200 OK (Valid JSON)** | Success | Return `(200, parsed_json)` immediately. | Stored in `bronze.payload_blobs` and `bronze.snapshots`. | Advances `last_checked_at` (fresh for 24h). |
| **200 OK (Empty / Non-JSON body)** | Success (Empty Sentinel) | Return `(200, {})` immediately. | Stored as `{}` in blobs and `bronze.snapshots`. | Advances `last_checked_at` (fresh for 24h). |
| **404 Not Found** | Expected Absence | Return `(404, None)` immediately (no retry). | Caller stores `{}` in blobs and `bronze.snapshots`. | Advances `last_checked_at` (fresh for 24h). |
| **401 Unauthorized / 403 Forbidden** | Session Invalid | Raise `SessionInvalidError` immediately (no retry). | Nothing stored. | Remains stale; prompts re-login in authenticated phases. |
| **400 Bad Request with `{"error": "Invalid headers", ...}`** | Session Invalid | Raise `SessionInvalidError` immediately (no retry). | Nothing stored. | Remains stale; prompts re-login in authenticated phases. |
| **429 Too Many Requests** | Throttled | Call `RateLimiter.report_429(retry_after)`. Await `RateLimiter.wait_if_paused()`. Retry up to `http_max_retries`. | If retry succeeds: stored. If retries exhaust: raise `RuntimeError`, nothing stored. | If exhausted: remains stale for next run. |
| **500, 502, 503, 504 & Network Timeouts** | Infrastructure Flake | Exponential backoff for calling task only (does **not** pause global `RateLimiter`). Retry up to `http_max_retries`. | If retry succeeds: stored. If retries exhaust: raise `RuntimeError`, nothing stored. | If exhausted: remains stale for next run. |
| **Other 4XX (e.g. 400 without invalid headers, 408)** | Client/Request Error | Retry with exponential backoff up to `http_max_retries`. | If retries exhaust: raise `RuntimeError`, nothing stored. | If exhausted: remains stale for next run. |

---

## 4. Component Technical Specifications

### 4.1 Configuration (`etfportfolio/core/config.py`)
Add rate-limiting and HTTP retry settings to `Settings`:

```python
class Settings(BaseSettings):
    # Existing settings...
    db_path: str = "data/etf.duckdb"
    details_concurrency: int = 10
    freshness_window_hours: float = 24.0
    
    # HTTP Rate Limiting and Retries
    rate_limit_initial_delay: float = 1.0
    rate_limit_max_delay: float = 60.0
    rate_limit_cooldown_seconds: float = 15.0
    http_max_retries: int = 5
```

### 4.2 Endpoints Declaration (`etfportfolio/core/endpoints.py`)
1. Remove `gated: bool` from `Endpoint`.
2. Remove the `landing` endpoint entry.
3. Remove `_DETAILS_EXCLUDED`, `DETAILS_ENDPOINTS`, `GATED_ENDPOINTS`, and `UNGATED_ENDPOINTS`.
4. `ENDPOINTS` directly lists the 7 fundamental snapshot endpoints:

```python
@dataclass(frozen=True)
class Endpoint:
    name: str
    url_prefix: str
    slug_template: str

    @property
    def url_template(self) -> str:
        return f"{self.url_prefix}{self.slug_template}"

    def resolve(self, **kwargs: Any) -> tuple[str, str, str]:
        slug = self.slug_template.format(**kwargs)
        full_url = f"{self.url_prefix}{slug}"
        return self.url_prefix, slug, full_url


ENDPOINTS: list[Endpoint] = [
    Endpoint(
        name="holdings",
        url_prefix="/tws.proxy/fundamentals/mf_holdings/",
        slug_template="{product_id}?lang=en",
    ),
    Endpoint(
        name="ratios",
        url_prefix="/tws.proxy/fundamentals/mf_ratios_fundamentals/",
        slug_template="{product_id}?lang=en",
    ),
    Endpoint(
        name="profile",
        url_prefix="/tws.proxy/fundamentals/mf_profile_and_fees/",
        slug_template="{product_id}?lang=en",
    ),
    Endpoint(
        name="lipper",
        url_prefix="/tws.proxy/fundamentals/mf_lip_ratings/",
        slug_template="{product_id}?lang=en",
    ),
    Endpoint(
        name="mstar",
        url_prefix="/tws.proxy/mstar/fund/detail?conid=",
        slug_template="{product_id}&lang=en",
    ),
    Endpoint(
        name="esg",
        url_prefix="/tws.proxy/impact/esg/",
        slug_template="{product_id}?accounts={account_id}&lang=en",
    ),
    Endpoint(
        name="theme_weights",
        url_prefix="/tws.proxy/knowledge-graph/ui/fund?conid=",
        slug_template="{product_id}&max=999999999&lang=en",
    ),
]

ENDPOINTS_BY_NAME: dict[str, Endpoint] = {ep.name: ep for ep in ENDPOINTS}
```

### 4.3 Rate Limiter & Outbound Engine (`etfportfolio/ingest/session.py`)

#### 4.3.1 `RateLimiter`
Coordinates throttling across concurrent coroutines:

```python
import asyncio
import logging
import random
import time
from typing import Any

from etfportfolio.core.config import settings

logger = logging.getLogger(__name__)


class RateLimiter:
    """Coordinates windowed 429 throttling and jittered backoff across coroutines."""

    def __init__(
        self,
        initial_delay: float | None = None,
        max_delay: float | None = None,
        cooldown_window: float | None = None,
    ) -> None:
        self.initial_delay = initial_delay if initial_delay is not None else settings.rate_limit_initial_delay
        self.max_delay = max_delay if max_delay is not None else settings.rate_limit_max_delay
        self.cooldown_window = cooldown_window if cooldown_window is not None else settings.rate_limit_cooldown_seconds

        self._current_delay: float = self.initial_delay
        self._pause_until: float = 0.0
        self._last_429_time: float = 0.0
        self._lock = asyncio.Lock()

    async def wait_if_paused(self) -> None:
        """Suspends coroutine execution while a rate-limiting pause window is active."""
        while True:
            now = time.monotonic()
            if now >= self._pause_until:
                break
            jitter = random.uniform(0.1, 0.5)
            sleep_duration = (self._pause_until - now) + jitter
            await asyncio.sleep(sleep_duration)

    async def report_429(self, retry_after: float | None = None) -> None:
        """Registers a 429 response, escalating backoff once per wave."""
        async with self._lock:
            now = time.monotonic()

            # 1. Reset delay to baseline if cooldown period passed without 429s
            if now - self._last_429_time > self.cooldown_window:
                self._current_delay = self.initial_delay

            # 2. Check if this is a new wave or continuation of an active wave
            if now >= self._pause_until:
                delay = self._current_delay
                if retry_after is not None and retry_after > 0:
                    delay = max(retry_after, delay)

                self._pause_until = now + delay
                self._current_delay = min(self._current_delay * 2.0, self.max_delay)
                logger.warning(
                    "HTTP 429 hit. Pausing outbound requests for %.2fs (next backoff: %.2fs)",
                    delay,
                    self._current_delay,
                )
            else:
                # Extend deadline if explicit retry_after requires a longer wait
                if retry_after is not None and retry_after > 0:
                    self._pause_until = max(self._pause_until, now + retry_after)
                logger.debug("HTTP 429 received within active wave. Awaiting existing deadline.")

            self._last_429_time = now
```

#### 4.3.2 Session Invalidation Detection (`is_session_invalid`)
Catch 401, 403, and 400 responses with the `"Invalid headers"` error body:

```python
def is_session_invalid(response: httpx.Response) -> bool:
    """Detects IBKR's specific session-invalid signature or unauthorized response."""
    if response.is_success:
        return False
    if response.status_code in (401, 403):
        return True
    try:
        data = response.json()
        if isinstance(data, dict):
            if data.get("error") == "Invalid headers":
                return True
            if data.get("statusCode") == 400 and data.get("error") == "Invalid headers":
                return True
    except Exception:
        if "Invalid headers" in response.text:
            return True
    return False
```

#### 4.3.3 Client Builder & Outbound Transport (`fetch_with_retry`)
Update `build_async_client` to accept and attach an optional `RateLimiter`:

```python
def build_async_client(
    timeout: float = 30.0,
    cookies: dict[str, str] | None = None,
    rate_limiter: RateLimiter | None = None,
) -> httpx.AsyncClient:
    """Builds an httpx.AsyncClient preloaded with session cookies, headers, and rate limiter."""
    if cookies is None:
        session_path = Path(settings.session_state_path)
        cookies = _load_cookies_from_storage_state(session_path)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Referer": f"{settings.ibkr_base_url}/portal/",
        "X-Requested-With": "XMLHttpRequest",
    }

    client = httpx.AsyncClient(
        base_url=settings.ibkr_base_url.rstrip("/"),
        headers=headers,
        cookies=cookies,
        timeout=timeout,
        follow_redirects=True,
    )
    client.rate_limiter = rate_limiter or RateLimiter()  # type: ignore[attr-defined]
    return client
```

Refactor `fetch_with_retry` to support `GET`/`POST`, rate-limiting backoffs, and sentinel parsing:

```python
def _parse_retry_after(response: httpx.Response) -> float | None:
    header = response.headers.get("Retry-After")
    if not header:
        return None
    try:
        return float(header)
    except ValueError:
        return None


async def fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    method: str = "GET",
    json: Any = None,
    max_retries: int | None = None,
    initial_backoff: float | None = None,
    rate_limiter: RateLimiter | None = None,
) -> tuple[int, Any]:
    """Sends HTTP request with rate-limiting backoff, sentinel handling, and retries.

    - 200: Returns (200, parsed_json) or (200, {}) on empty/non-JSON body.
    - 404: Returns immediately as (404, None).
    - Session invalid errors (401, 403, 400 Invalid Headers): Raises SessionInvalidError immediately.
    - 429: Notifies RateLimiter, pauses, and retries.
    - 5XX and network errors: Retries with exponential backoff up to max_retries.
    """
    retries_limit = max_retries if max_retries is not None else settings.http_max_retries
    backoff = initial_backoff if initial_backoff is not None else settings.rate_limit_initial_delay
    limiter = rate_limiter or getattr(client, "rate_limiter", None)

    attempt = 0
    while attempt < retries_limit:
        attempt += 1
        if limiter is not None:
            await limiter.wait_if_paused()

        try:
            if method.upper() == "POST":
                resp = await client.post(url, json=json)
            else:
                resp = await client.get(url)

            if resp.is_success:
                try:
                    data = resp.json()
                    return resp.status_code, data if data is not None else {}
                except Exception:
                    # Permissive sentinel: treat empty or non-JSON 200 as empty payload {}
                    return resp.status_code, {}

            if resp.status_code == 404:
                return 404, None

            if is_session_invalid(resp):
                logger.error("Session invalid signature hit on %s", url)
                raise SessionInvalidError("Session is invalid ('Invalid headers').")

            if resp.status_code == 429:
                retry_after = _parse_retry_after(resp)
                if limiter is not None:
                    await limiter.report_429(retry_after)
                else:
                    delay = retry_after if (retry_after is not None and retry_after > 0) else backoff
                    await asyncio.sleep(delay)
                    backoff *= 2.0
                continue

            logger.warning(
                "Request to %s failed (status %d), attempt %d/%d",
                url,
                resp.status_code,
                attempt,
                retries_limit,
            )
        except (httpx.RequestError, httpx.TimeoutException) as e:
            logger.warning("Network error on %s: %s (attempt %d/%d)", url, e, attempt, retries_limit)

        if attempt < retries_limit:
            await asyncio.sleep(backoff)
            backoff *= 2.0

    raise RuntimeError(f"Request to {url} failed after {retries_limit} attempts.")
```

Update `session.ensure_session` to accept and propagate the pipeline's `RateLimiter`:

```python
async def ensure_session(rate_limiter: RateLimiter | None = None) -> tuple[httpx.AsyncClient, str]:
    """Ensures a valid, authenticated client and resolves the active account_id."""
    client = build_async_client(rate_limiter=rate_limiter)
    try:
        account_id = await probe(client)
        return client, account_id
    except Exception as e:
        logger.warning("Session probe failed (%s). Launching interactive login...", e)
        await client.aclose()
        await login()
        client = build_async_client(rate_limiter=rate_limiter)
        try:
            account_id = await probe(client)
            return client, account_id
        except Exception as login_err:
            raise RuntimeError(f"Session authentication failed after login: {login_err}") from login_err
```

### 4.4 Unauthenticated Product Discovery (`etfportfolio/ingest/products.py`)
1. Remove all imports of `session.build_async_client` and cookie state.
2. In `products.py`, provide a dedicated helper that instantiates a clean `httpx.AsyncClient` pointing to `settings.ibkr_base_url` with browser headers and attached `RateLimiter`.
3. Accept optional `rate_limiter: RateLimiter | None = None`.
4. Route pagination POST requests through `fetch_with_retry`.
5. Neither `_run_full()` nor standalone `main.py ingest products` calls `ensure_session()`.

```python
def _build_unauthenticated_client(rate_limiter: Any | None = None) -> httpx.AsyncClient:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
    }
    client = httpx.AsyncClient(
        base_url=settings.ibkr_base_url.rstrip("/"),
        headers=headers,
        timeout=30.0,
        follow_redirects=True,
    )
    from etfportfolio.ingest.session import RateLimiter
    client.rate_limiter = rate_limiter or RateLimiter()  # type: ignore[attr-defined]
    return client
```

In `products.sync()`:
```python
async def sync(
    client: httpx.AsyncClient | None = None,
    rate_limiter: Any | None = None,
    force: bool = False,
) -> int:
    """Crawls webrest/search/products-by-filters endpoint and populates bronze.products."""
    page_number = 1
    total_synced = 0
    close_client = False
    clean_complete = False

    if client is None:
        client = _build_unauthenticated_client(rate_limiter=rate_limiter)
        close_client = True
    assert client is not None

    try:
        async with AsyncDbWorker(settings.db_path) as worker:
            if not force:
                last_checked = await worker.submit(_check_products_freshness)
                if last_checked and is_fresh(last_checked, settings.freshness_window_hours):
                    now = datetime.now(UTC)
                    if last_checked.tzinfo is None:
                        last_checked = last_checked.replace(tzinfo=UTC)
                    seconds = max(0.0, (now - last_checked).total_seconds())
                    hours = round(seconds / 3600.0, 1)
                    console.info(f"Products sync skipped (checked {hours}h ago; use --force to refresh).")
                    return await worker.submit(_count_products)

            existing_count = await worker.submit(_count_products)
            initial_total = existing_count if existing_count > 0 else None

            with progress_bar(initial_total, desc="Products", unit="product") as bar:
                while True:
                    bar.set_postfix_str(f"page {page_number}")
                    logger.debug("Fetching products page %d (pageSize=%d)...", page_number, PAGE_SIZE)
                    url = "/webrest/search/products-by-filters"
                    payload = {
                        "domain": "ie",
                        "newProduct": "all",
                        "pageNumber": page_number,
                        "pageSize": PAGE_SIZE,
                        "productCountry": [],
                        "productSymbol": "",
                        "productType": ["ETF", "FUND"],
                        "sortDirection": "asc",
                        "sortField": "conid",
                    }
                    try:
                        status_code, data = await fetch_with_retry(
                            client,
                            url,
                            method="POST",
                            json=payload,
                            rate_limiter=rate_limiter,
                        )
                        if status_code != 200 or not data:
                            logger.error("Products crawl failed at page %d with status %d", page_number, status_code)
                            break
                    except Exception as e:
                        logger.error("Products crawl error at page %d: %s", page_number, e)
                        break

                    products_list = data.get("products", [])
                    logger.debug("Received %d products on page %d", len(products_list), page_number)

                    if products_list:
                        await worker.submit(upsert_products, products_list)
                        total_synced += len(products_list)
                        bar.update(len(products_list))

                    if len(products_list) < PAGE_SIZE:
                        logger.debug(
                            "Pagination complete after %d pages. Total products: %d",
                            page_number,
                            total_synced,
                        )
                        clean_complete = True
                        if bar.total and total_synced != bar.total:
                            bar.total = total_synced
                            bar.refresh()
                        break

                    page_number += 1

            if clean_complete:
                await worker.submit(_stamp_products_last_checked)
    finally:
        if close_client:
            await client.aclose()

    return total_synced
```

### 4.5 Snapshot Storage & Sentinel Handling (`etfportfolio/ingest/snapshots.py`)
`fetch_snapshot` fetches the endpoint via `fetch_with_retry`. Both 404 (`payload is None`) and empty responses are passed as `{}` to `store_snapshot`, ensuring `bronze.snapshots` records the sentinel and bumps `last_checked_at`:

```python
async def fetch_snapshot(
    client: httpx.AsyncClient,
    worker: AsyncDbWorker,
    ep: endpoints.Endpoint,
    product_id: int,
    account_id: str,
) -> None:
    """Fetches a single snapshot endpoint for a product and stores it.

    404 and empty responses are persisted as {} so fetched_at / last_checked_at
    is updated and the freshness cache skips the endpoint on subsequent runs.
    """
    url_prefix, url_slug, full_url = ep.resolve(product_id=product_id, account_id=account_id)
    _, payload = await session.fetch_with_retry(client, full_url)
    await worker.submit(store_snapshot, product_id, url_prefix, url_slug, payload or {})
```

### 4.6 Pure Per-Endpoint Details Ingestion (`etfportfolio/ingest/details.py`)
1. Remove all imports from `etfportfolio.ingest.landing`.
2. Remove `load_landing_freshness_cache`. Only `load_endpoint_freshness_cache` is retained:
```python
def load_endpoint_freshness_cache(conn: duckdb.DuckDBPyConnection) -> dict[tuple[int, str], datetime]:
    """Load (product_id, url_prefix) -> MAX(last_checked_at) from bronze.snapshots."""
    rows = conn.execute(
        """
        SELECT product_id, url_prefix, MAX(last_checked_at)
        FROM bronze.snapshots
        GROUP BY product_id, url_prefix
        """
    ).fetchall()
    return {(row[0], row[1]): row[2] for row in rows}
```
3. Rewrite `process_product`:
   - Iterate over `endpoints.ENDPOINTS`.
   - Check `(product_id, ep.url_prefix)` against `endpoint_cache`. If not `force` and fresh, increment `skipped_fresh_eps`.
   - Otherwise, append to `to_fetch`.
   - If `to_fetch` is empty: return `ProductDetailsResult(ok=True, product_skipped_fresh=True, endpoints_skipped_fresh=skipped_fresh_eps)`.
   - Concurrently fetch stale endpoints via `asyncio.gather(*tasks, return_exceptions=True)`.
   - If `SessionInvalidError` is raised by any task, immediately re-raise it.
   - For all other errors: record task as failed (`fetch_success[ep] = False`), log the error, and allow other endpoints to complete.
   - Return `ProductDetailsResult(ok=all(fetch_success.values()), product_skipped_fresh=False, endpoints_skipped_fresh=skipped_fresh_eps)`.

```python
async def process_product(
    client: httpx.AsyncClient,
    worker: AsyncDbWorker,
    product_id: int,
    account_id: str,
    semaphore: asyncio.Semaphore,
    endpoint_cache: dict[tuple[int, str], datetime],
    force: bool = False,
) -> ProductDetailsResult:
    """Runs snapshot ingestion for one product across all endpoints.

    Evaluates freshness per endpoint. Fetches only stale endpoints concurrently.
    Employs partial continuation on individual endpoint errors.
    """
    to_fetch: list[endpoints.Endpoint] = []
    skipped_fresh_eps = 0

    for ep in endpoints.ENDPOINTS:
        last_checked = endpoint_cache.get((product_id, ep.url_prefix))
        if not force and is_fresh(last_checked, settings.freshness_window_hours):
            skipped_fresh_eps += 1
        else:
            to_fetch.append(ep)

    if not to_fetch:
        return ProductDetailsResult(
            ok=True,
            product_skipped_fresh=True,
            endpoints_skipped_fresh=skipped_fresh_eps,
        )

    tasks = [_fetch_one(client, worker, ep, product_id, account_id, semaphore) for ep in to_fetch]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, session.SessionInvalidError):
            raise r

    fetch_success = {ep: (res is True) for ep, res in zip(to_fetch, results, strict=False)}
    all_ok = all(fetch_success.values()) if fetch_success else True

    return ProductDetailsResult(
        ok=all_ok,
        product_skipped_fresh=False,
        endpoints_skipped_fresh=skipped_fresh_eps,
    )
```

### 4.7 Pipeline Details Runner & Standalone Execution (`etfportfolio/ingest/pipeline.py`)

1. Rewrite `_is_product_fully_fresh`:
```python
def _is_product_fully_fresh(
    product_id: int,
    endpoint_cache: dict[tuple[int, str], datetime],
) -> bool:
    """True iff every endpoint in endpoints.ENDPOINTS is fresh for this product."""
    from etfportfolio.core import endpoints as ep_mod
    from etfportfolio.ingest.utils import is_fresh

    for ep in ep_mod.ENDPOINTS:
        last_checked = endpoint_cache.get((product_id, ep.url_prefix))
        if not is_fresh(last_checked, settings.freshness_window_hours):
            return False
    return True
```

2. Update `_run_details_phase`:
   - Sequential outer product loop with intra-product endpoint concurrency.
   - Filter `to_process` using `_is_product_fully_fresh`.
   - Pass `endpoint_cache` to `process_product`.

```python
async def _run_details_phase(
    worker: AsyncDbWorker,
    client: httpx.AsyncClient,
    account_id: str,
    target_ids: list[int],
    force: bool,
) -> None:
    """Runs the details phase across a resolved list of target products."""
    semaphore = asyncio.Semaphore(settings.details_concurrency)
    endpoint_cache = await worker.submit(details.load_endpoint_freshness_cache)

    if force:
        to_process = target_ids
    else:
        to_process = [pid for pid in target_ids if not _is_product_fully_fresh(pid, endpoint_cache)]

    skipped_prods = len(target_ids) - len(to_process)
    if skipped_prods:
        console.info(f"{skipped_prods}/{len(target_ids)} products fresh, {len(to_process)} to process.")
    else:
        console.info(f"Processing {len(target_ids)} product(s)...")

    if not to_process:
        console.info("Done. All products are fresh.")
        return

    failures = 0
    skipped_eps = 0

    with progress_bar(len(to_process), desc="Details") as bar:
        for product_id in to_process:
            bar.set_postfix_str(str(product_id))
            try:
                res = await details.process_product(
                    client,
                    worker,
                    product_id,
                    account_id,
                    semaphore,
                    endpoint_cache,
                    force=force,
                )
            except session.SessionInvalidError:
                logger.error("Session became invalid while processing product %d. Aborting.", product_id)
                raise
            finally:
                bar.update(1)

            if not res.ok:
                failures += 1
            skipped_eps += res.endpoints_skipped_fresh

    console.info(
        f"details: {len(to_process)} products processed; {skipped_eps} endpoint requests skipped (fresh) among them."
    )
    if failures:
        console.info(
            f"Done. {len(to_process) - failures}/{len(to_process)} products fully succeeded, "
            f"{failures} had failures — see log for detail."
        )
    else:
        console.info(f"Done. All {len(to_process)} products fully succeeded.")
```

3. Pipeline-Scoped `RateLimiter` & Standalone CLI Methods:
   - In `_run_full()`, instantiate a single `rate_limiter = RateLimiter()`.
   - Pass `rate_limiter` to `products.sync(rate_limiter=rate_limiter)`.
   - In Phase 5, call `client, account_id = await session.ensure_session(rate_limiter=rate_limiter)`.
   - Use that client for Phase 6 (`themes.sync`) and Phase 7 (`_run_details_phase`).
   - Standalone CLI methods in `Ingest`:
     - `products(force=False)`: Calls `asyncio.run(products.sync(force=force))` with no session validation.
     - `details(force=False)`: Calls `_run_details_only(force=force)`, which calls `ensure_session()` and creates a dedicated `RateLimiter`.
     - `themes(force=False)`: Calls `_run_themes(force=force)`, which calls `ensure_session()`.

### 4.8 Storage Cleanup & Helpers (`clean.py`, `utils.py`)
1. **`etfportfolio/ingest/utils.py`:** Delete the `gc_preview_blob` function completely.
2. **`etfportfolio/ingest/clean.py`:** In `clean_payload_blobs()`, remove the union with `bronze.snapshot_previews`:

```python
def clean_payload_blobs(conn: duckdb.DuckDBPyConnection) -> int:
    """Deletes unreferenced payload blobs from bronze.payload_blobs.

    Returns the number of deleted blobs.
    """
    res = conn.execute(
        """
        DELETE FROM bronze.payload_blobs
        WHERE hash NOT IN (
            SELECT hash FROM bronze.snapshots
        )
        """
    ).fetchone()
    return res[0] if res else 0
```

### 4.9 Target Database Schema (`etfportfolio/core/schema.sql`)
1. Remove `CREATE TABLE IF NOT EXISTS bronze.snapshot_previews ...`.
2. Do **not** place `DROP TABLE` statements in `schema.sql`. The file defines the clean, desired medallion state.

### 4.10 Migration Script (`scripts/migrate_drop_snapshot_previews.py`)
Create a one-off migration script for existing local DuckDB databases:

```python
"""One-off database migration: drops bronze.snapshot_previews and purges unreferenced blobs."""

from etfportfolio.core.config import settings
from etfportfolio.core.db import db_connection
from etfportfolio.core.logging import console
from etfportfolio.ingest.clean import checkpoint, clean_payload_blobs


def run_migration() -> None:
    console.info("Starting migration: dropping bronze.snapshot_previews...")
    with db_connection(settings.db_path) as conn:
        conn.execute("DROP TABLE IF EXISTS bronze.snapshot_previews")
        console.info("Table bronze.snapshot_previews dropped.")

        deleted_blobs = clean_payload_blobs(conn)
        console.info(f"Purged {deleted_blobs:,} unreferenced blobs.")

        checkpoint(conn)
        console.info("DuckDB storage checkpointed.")
    console.info("Migration complete.")


if __name__ == "__main__":
    run_migration()
```

---

## 5. Deletion & Modification Checklist

| Target Path | Operation | Description |
| :--- | :---: | :--- |
| `etfportfolio/ingest/landing.py` | **DELETE** | Remove obsolete landing gate file outright. |
| `tests/ingest/test_landing.py` | **DELETE** | Remove obsolete landing test file outright. |
| `etfportfolio/core/schema.sql` | **MODIFY** | Remove `bronze.snapshot_previews` table definition. |
| `scripts/migrate_drop_snapshot_previews.py` | **CREATE** | Create one-off migration script. |
| `etfportfolio/core/config.py` | **MODIFY** | Add rate limiter and retry settings (`rate_limit_*`, `http_max_retries`). |
| `etfportfolio/core/endpoints.py` | **MODIFY** | Remove `landing` and `gated: bool`; simplify to 7 endpoints. |
| `etfportfolio/ingest/session.py` | **MODIFY** | Implement `RateLimiter`, update `build_async_client`, upgrade `fetch_with_retry` and `is_session_invalid`. |
| `etfportfolio/ingest/products.py` | **MODIFY** | Use dedicated unauthenticated client; route pagination POST through `fetch_with_retry`. |
| `etfportfolio/ingest/snapshots.py` | **MODIFY** | Ensure sentinel payloads are stored on 404 or empty responses. |
| `etfportfolio/ingest/details.py` | **MODIFY** | Remove landing references; implement pure per-endpoint freshness & partial continuation. |
| `etfportfolio/ingest/pipeline.py` | **MODIFY** | Update `_is_product_fully_fresh`; instantiate and pass run-scoped `RateLimiter`. |
| `etfportfolio/ingest/utils.py` | **MODIFY** | Delete `gc_preview_blob()`. |
| `etfportfolio/ingest/clean.py` | **MODIFY** | Check blob references solely against `bronze.snapshots`. |

---

## 6. Test Suite & Verification Specifications

### 6.1 `RateLimiter` & Transport Tests (`tests/ingest/test_session.py`)
1. **Wave Deduplication:** Simulate 5 concurrent coroutines calling `report_429()` simultaneously. Verify that `_pause_until` is updated once, and `_current_delay` doubles only once (e.g. from 1.0 to 2.0).
2. **Cooldown Reset:** Advance monotonic clock past `cooldown_window` (15s). Call `report_429()`. Verify `_current_delay` resets to `initial_delay`.
3. **`Retry-After` Header:** Mock response with `Retry-After: 45`. Verify pause window is extended to at least 45 seconds.
4. **Jittered Wait:** Verify that `wait_if_paused()` sleeps for `(pause_until - now) + jitter` where `0.1 <= jitter <= 0.5`.
5. **Sentinel 200 & 404 in `fetch_with_retry`:**
   - 200 with empty body returns `(200, {})`.
   - 200 with non-JSON text returns `(200, {})`.
   - 404 returns `(404, None)` on attempt 1 without retrying.
6. **Session Invalidation in `fetch_with_retry`:**
   - 401, 403, and 400 with `{"error": "Invalid headers"}` raise `SessionInvalidError` on attempt 1 without retrying.
7. **5XX Exponential Backoff:**
   - 500/503 errors retry 5 times with exponential backoff before raising `RuntimeError`. Global `RateLimiter` pause window is not triggered.

### 6.2 Details Freshness & Continuation Tests (`tests/ingest/test_details.py`)
1. **All Fresh Product Skip:** Configure cache with all 7 endpoints fresh. Verify product is skipped (`product_skipped_fresh=True`), zero network calls occur, and `endpoints_skipped_fresh=7`.
2. **Selective Stale Fetch:** Configure cache with 5 fresh endpoints and 2 stale endpoints. Verify only the 2 stale endpoints are requested, and `endpoints_skipped_fresh=5`.
3. **Partial Failure Continuation:** One endpoint raises `RuntimeError` after exhausting retries; the remaining 6 succeed. Verify:
   - `ProductDetailsResult(ok=False, product_skipped_fresh=False, endpoints_skipped_fresh=0)`.
   - The 6 successful endpoints are written to `bronze.snapshots`.
   - The failed endpoint has no snapshot row written and remains stale.

### 6.3 Cleanup & Schema Tests (`tests/ingest/test_clean.py`, `tests/ingest/test_utils.py`)
1. `tests/ingest/test_utils.py`: Remove tests for `gc_preview_blob`.
2. `tests/ingest/test_clean.py`: Insert unreferenced blobs and blobs referenced only by `bronze.snapshots`. Call `clean_payload_blobs(conn)` and verify only blobs absent from `bronze.snapshots` are deleted. Verify no queries attempt to access `bronze.snapshot_previews`.

### 6.4 Products & Pipeline Tests (`tests/ingest/test_products.py`, `tests/ingest/test_pipeline.py`, `tests/core/test_endpoints.py`)
1. `tests/core/test_endpoints.py`: Verify `ENDPOINTS` contains exactly 7 items, has no `landing` entry, and `Endpoint` has no `gated` attribute.
2. `tests/ingest/test_products.py`: Verify product pagination crawl uses the unauthenticated client and routes requests through `fetch_with_retry`.
3. `tests/ingest/test_pipeline.py`: Verify `_run_details_phase` and `_is_product_fully_fresh` operate with pure per-endpoint freshness cache signatures.