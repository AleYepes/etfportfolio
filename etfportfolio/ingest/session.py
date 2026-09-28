import asyncio
import contextlib
import json
import logging
import random
import re
import time
from pathlib import Path
from typing import Any

import httpx
import questionary
from playwright.async_api import async_playwright

from etfportfolio.core.config import settings

logger = logging.getLogger(__name__)


class SessionInvalidError(Exception):
    """Raised when IBKR returns the session-invalid signature."""


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


DEFAULT_ENV_PATH = Path(".env")
_ACCOUNTS_ACESWS_PATH_RE = re.compile(r"/portal\.proxy/v1/portal/acesws/([^/?]+)")
_ACCOUNTS_PORTFOLIO2_PATH_RE = re.compile(r"/portal\.proxy/v1/portal/portfolio2/([^/?]+)")
_ACCOUNT_ID_RE = re.compile(r"^(?:U|DU|DF|F)?\d+$")
_ONE_BAR_AUTH_ENDPOINT = "/AccountManagement/OneBarAuthentication?json=1"
_AUTH_CHECK_ENDPOINTS = [
    "/tws.proxy/fundamentals/landing/756733?widgets=objective&lang=en",
    "/tws.proxy/acesws/accountList",
    "/AccountManagement/OneBarAuthentication?json=1",
]
_LOGIN_URL_FRAGMENTS = ("/sso/Login", "/portal/", "/portal/#/")
_2FA_PANEL_SELECTORS = [
    "div.xyzblock-notification",
    "div.xyzblock-gold",
    "div.xyzblock-silver",
    "div.xyzblock-bronze",
    "div.xyzblock-temp",
    "div.xyzblock-fido",
    "div.xyzblock-qrcode",
    "div.xyzblock-multiplesf",
    "div.xyzblock-otpselector",
]


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


def _choose_account_id(candidates: list[Any]) -> str | None:
    """Selects the best account ID from candidate list (prioritizes 'U...' format)."""
    valid = []
    for candidate in candidates:
        text = str(candidate or "").strip()
        if _ACCOUNT_ID_RE.match(text):
            valid.append(text)
    if not valid:
        return None
    unique = sorted(set(valid))
    unique.sort(key=lambda x: (0 if x.startswith("U") else 1, -len(x), x))
    return unique[0]


async def validate_client_session(client: httpx.AsyncClient) -> bool:
    """Validates session by calling lightweight authenticated endpoints."""
    for endpoint in _AUTH_CHECK_ENDPOINTS:
        try:
            resp = await client.get(endpoint)
            if is_session_invalid(resp):
                return False
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    if data:
                        return True
                except Exception:
                    pass
        except Exception:
            continue
    return False


async def discover_account_id(
    client: httpx.AsyncClient,
    state_dict: dict[str, Any] | None = None,
) -> str | None:
    """Discovers the active account ID in priority order across live session,

    state dict, cookies paths, and configuration.
    """
    if state_dict:
        state_candidates: list[Any] = []
        if state_dict.get("primary_account_id"):
            state_candidates.append(state_dict.get("primary_account_id"))
        for cookie in state_dict.get("cookies", []):
            path = cookie.get("path") or ""
            m1 = _ACCOUNTS_ACESWS_PATH_RE.search(path)
            if m1:
                state_candidates.append(m1.group(1))
            m2 = _ACCOUNTS_PORTFOLIO2_PATH_RE.search(path)
            if m2:
                state_candidates.append(m2.group(1))
        acct = _choose_account_id(state_candidates)
        if acct:
            return acct

    try:
        resp = await client.get("/tws.proxy/acesws/accountList")
        if resp.is_success:
            data = resp.json()
            if isinstance(data, dict):
                api_candidates: list[Any] = []
                for key in ("accessibleAccounts", "accounts", "acctList", "selectedAccount", "acctId"):
                    val = data.get(key)
                    if isinstance(val, list):
                        for item in val:
                            if isinstance(item, str):
                                api_candidates.append(item)
                            elif isinstance(item, dict):
                                api_candidates.extend(
                                    [item.get("accountId"), item.get("acctId"), item.get("accountVan")]
                                )
                    elif isinstance(val, str):
                        api_candidates.append(val)
                acct = _choose_account_id(api_candidates)
                if acct:
                    return acct
    except Exception as e:
        logger.debug("accountList probe exception: %s", e)

    try:
        resp = await client.get(_ONE_BAR_AUTH_ENDPOINT)
        if resp.is_success:
            data = resp.json()
            if isinstance(data, dict):
                onebar_candidates: list[Any] = []
                if data.get("mostRelevantAccount"):
                    onebar_candidates.append(data.get("mostRelevantAccount"))
                for acct_item in data.get("portfolioAccounts", []):
                    if isinstance(acct_item, dict):
                        onebar_candidates.extend([acct_item.get("accountId"), acct_item.get("accountVan")])
                acct = _choose_account_id(onebar_candidates)
                if acct:
                    return acct
    except Exception as e:
        logger.debug("OneBarAuthentication probe exception: %s", e)

    session_path = Path(settings.session_state_path)
    if session_path.exists():
        try:
            with session_path.open("r", encoding="utf-8") as f:
                disk_state = json.load(f)
                disk_candidates: list[Any] = []
                if disk_state.get("primary_account_id"):
                    disk_candidates.append(disk_state.get("primary_account_id"))
                for cookie in disk_state.get("cookies", []):
                    path = cookie.get("path") or ""
                    m1 = _ACCOUNTS_ACESWS_PATH_RE.search(path)
                    if m1:
                        disk_candidates.append(m1.group(1))
                    m2 = _ACCOUNTS_PORTFOLIO2_PATH_RE.search(path)
                    if m2:
                        disk_candidates.append(m2.group(1))
                acct = _choose_account_id(disk_candidates)
                if acct:
                    return acct
        except Exception:
            pass

    if settings.account_id:
        return _choose_account_id([settings.account_id])

    return None


def _load_cookies_from_storage_state(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            cookies = {}
            for cookie in data.get("cookies", []):
                name = cookie.get("name")
                value = cookie.get("value")
                if name and value is not None:
                    cookies[name] = value
            return cookies
    except Exception as e:
        logger.warning("Failed to load cookies from %s: %s", path, e)
        return {}


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


def reconcile_account_id(account_id: str, env_path: Path | None = None) -> bool:
    """Reconciles probe account ID with .env and settings.

    Returns True to proceed, False to restart the login flow.
    """
    target_env = env_path or DEFAULT_ENV_PATH
    env_account_id = (settings.account_id or "").strip()

    if env_account_id and env_account_id != account_id:
        answer = (
            input(f"Account mismatch: '{account_id}' (new) vs '{env_account_id}' (expected). Continue? [y/N] ")
            .strip()
            .lower()
        )
        if answer != "y":
            return False
        # User confirmed — fall through to persist the new ID.

    _write_account_id(account_id, target_env)
    return True


def _write_account_id(account_id: str, target_env: Path) -> None:
    """Writes ACCOUNT_ID to .env and updates settings in-process."""
    lines: list[str] = []
    if target_env.exists():
        with contextlib.suppress(Exception):
            lines = target_env.read_text(encoding="utf-8").splitlines()

    if settings.account_id == account_id and f"ACCOUNT_ID={account_id}" in lines:
        return

    found = False
    new_lines: list[str] = []
    for line in lines:
        if line.startswith("ACCOUNT_ID="):
            new_lines.append(f"ACCOUNT_ID={account_id}")
            found = True
        else:
            new_lines.append(line)
    if not found:
        new_lines.append(f"ACCOUNT_ID={account_id}")
    with contextlib.suppress(Exception):
        target_env.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    settings.account_id = account_id
    logger.info("Persisted ACCOUNT_ID=%s to %s", account_id, target_env)


def _write_credentials(username: str, password: str, env_path: Path | None = None) -> None:
    """Writes IBKR_USERNAME and IBKR_PASSWORD to .env and updates settings in-process."""
    target_env = env_path or DEFAULT_ENV_PATH
    lines: list[str] = []
    if target_env.exists():
        with contextlib.suppress(Exception):
            lines = target_env.read_text(encoding="utf-8").splitlines()

    replacements = {"IBKR_USERNAME": username, "IBKR_PASSWORD": password}
    found: dict[str, bool] = {k: False for k in replacements}
    new_lines: list[str] = []
    for line in lines:
        matched = False
        for key, value in replacements.items():
            if line.startswith(f"{key}="):
                new_lines.append(f"{key}={value}")
                found[key] = True
                matched = True
                break
        if not matched:
            new_lines.append(line)
    for key, value in replacements.items():
        if not found[key]:
            new_lines.append(f"{key}={value}")

    with contextlib.suppress(Exception):
        target_env.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    settings.ibkr_username = username
    settings.ibkr_password = password
    logger.info("Persisted IBKR_USERNAME and IBKR_PASSWORD to %s", target_env)


async def probe(client: httpx.AsyncClient) -> str:
    """Probes session validity and resolves active account_id."""
    is_valid = await validate_client_session(client)
    if not is_valid:
        raise RuntimeError("Session invalid: 'Invalid headers' detected.")

    account_id = await discover_account_id(client)
    if not account_id:
        raise RuntimeError("Probe error: no accessible accounts returned in accountList.")

    reconcile_account_id(account_id)
    return account_id


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


async def get_credentials() -> tuple[str, str]:
    """Retrieves IBKR username and password from settings/.env or prompts via questionary."""
    username = (settings.ibkr_username or "").strip()
    password = (settings.ibkr_password or "").strip()

    if not username:
        entered_user = await questionary.text("IBKR Username:").ask_async()
        username = (entered_user or "").strip()

    if not password:
        entered_pass = await questionary.password("IBKR Password:").ask_async()
        password = (entered_pass or "").strip()

    return username, password


async def _prompt_credentials() -> tuple[str, str]:
    """Always prompts for credentials interactively, ignoring any stored values."""
    username = await questionary.text("IBKR Username:").ask_async()
    password = await questionary.password("IBKR Password:").ask_async()
    return (username or "").strip(), (password or "").strip()


async def _dismiss_cookie_modal(page: Any) -> None:
    """Dismisses the IBKR cookie-consent modal.

    Uses JS evaluation to bypass the Bootstrap modal backdrop, which blocks
    Playwright's normal pointer-event-based clicks.
    """
    dismissed = await page.evaluate("""() => {
        const ids = ['gdpr-reject-all', 'gdpr-save-settings', 'btn_accept_cookies'];
        for (const id of ids) {
            const el = document.getElementById(id);
            if (el) { el.click(); return true; }
        }
        const modal = document.getElementById('cookie-modal');
        if (modal && typeof bootstrap !== 'undefined') {
            try { bootstrap.Modal.getInstance(modal)?.hide(); return true; } catch(_) {}
        }
        if (modal) { modal.style.display = 'none'; return true; }
        return false;
    }""")
    if dismissed:
        await page.evaluate("""() => {
            document.querySelectorAll('.modal-backdrop').forEach(el => el.remove());
            document.body.classList.remove('modal-open');
            document.body.style.overflow = '';
            document.body.style.paddingRight = '';
        }""")
        await asyncio.sleep(0.5)
        return

    for selector in (
        "#gdpr-reject-all",
        "#btn_accept_cookies",
        "button#onetrust-reject-all-handler",
        "button#onetrust-accept-btn-handler",
    ):
        try:
            elem = await page.wait_for_selector(selector, state="visible", timeout=2000)
            if elem:
                await elem.click()
                await asyncio.sleep(0.5)
                return
        except Exception:
            continue


async def _fill_and_submit(page: Any, username: str, password: str) -> bool:
    """Dismisses the cookie modal then fills and submits the login form."""
    try:
        await _dismiss_cookie_modal(page)
        with contextlib.suppress(Exception):
            await page.wait_for_function(
                "() => !document.getElementById('cookie-modal') || "
                "getComputedStyle(document.getElementById('cookie-modal')).display === 'none'",
                timeout=5000,
            )

        await page.wait_for_selector("input#xyz-field-username, input[name='username']", state="visible", timeout=15000)
        await page.fill("input#xyz-field-username, input[name='username']", username)
        await page.fill("input#xyz-field-password, input[name='password']", password)

        submit_selector = (
            "form.xyzform-username button[type='submit'], form[name='xyzform-username'] button[type='submit']"
        )
        submit_btn = await page.wait_for_selector(submit_selector, state="visible", timeout=5000)
        if submit_btn:
            await submit_btn.click()
            logger.info("Submitted login credentials.")
            return True
    except Exception as e:
        logger.warning("Auto-fill skipped (%s). Please complete login manually.", e)
    return False


def _is_login_url(url: str) -> bool:
    # Post-login portal uses SPA hash routing (#/dashboard, #/account-management, etc.)
    # The login page itself never has a #/ fragment.
    if "#/" in url:
        return False
    return any(frag in url for frag in _LOGIN_URL_FRAGMENTS)


async def _watch_login_page(page: Any, timeout_s: float) -> str:
    """Polls the IBKR login page DOM to determine login outcome.

    Returns one of:
      'success'         — authenticated portal content detected (DOM or URL)
      'error:<message>' — xyzblock-error panel appeared (e.g. bad credentials)
      'timeout'         — timeout_s elapsed with no conclusive signal
    """
    tfa_hint_shown = False
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s

    while loop.time() < deadline:
        await asyncio.sleep(1.0)

        try:
            error_msg = await page.evaluate("""() => {
                const el = document.querySelector('div.xyzblock-error');
                if (!el || el.style.display === 'none' || !el.offsetParent) return null;
                const msg = el.querySelector('.xyz-errormessage');
                return msg ? msg.innerText.trim() : 'Authentication failed';
            }""")
            if error_msg:
                return f"error:{error_msg}"
        except Exception:
            pass

        try:
            finished = await page.evaluate(
                "() => {"
                "  const finished = document.querySelector('div.xyzblock-finished');"
                "  if (finished && finished.style.display !== 'none' && finished.offsetParent) return true;"
                "  return !!document.querySelector('main#cp-ib-app-main-content');"
                "}"
            )
            if finished:
                return "success"
        except Exception:
            pass

        try:
            if page.url and not _is_login_url(page.url):
                return "success"
        except Exception:
            pass

        if not tfa_hint_shown:
            try:
                selectors_json = json.dumps(_2FA_PANEL_SELECTORS)
                panel_visible = await page.evaluate(f"""() => {{
                    const selectors = {selectors_json};
                    return selectors.some(sel => {{
                        const el = document.querySelector(sel);
                        return !!el && el.style.display !== 'none' && !!el.offsetParent;
                    }});
                }}""")
                if panel_visible:
                    logger.info("2FA prompt detected in browser — please complete authentication there.")
                    tfa_hint_shown = True
            except Exception:
                pass

    return "timeout"


async def login(timeout_s: float = 300.0) -> None:
    """Launches an interactive browser login, retrying on bad credentials,

    then saves the authenticated session state and probes the account endpoint.
    """
    while True:
        username, password = await get_credentials()

        session_file = Path(settings.session_state_path)
        session_file.parent.mkdir(parents=True, exist_ok=True)

        login_url = f"{settings.ibkr_base_url}/portal/"
        logger.info("Launching browser for interactive IBKR login at %s...", login_url)

        restart = False
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            try:
                context = await browser.new_context()
                try:
                    page = await context.new_page()
                    await page.goto(login_url, wait_until="domcontentloaded")

                    while True:
                        if username and password:
                            await _fill_and_submit(page, username, password)

                        logger.info("Waiting for login and 2FA authentication to complete in browser...")
                        outcome = await _watch_login_page(page, timeout_s)

                        if outcome == "success":
                            break
                        if outcome == "timeout":
                            raise RuntimeError("Login timed out.")

                        # outcome == "error:…" — IBKR reset the form; re-prompt and retry
                        msg = outcome[len("error:") :]
                        logger.warning("Login failed: %s", msg)
                        username, password = await _prompt_credentials()

                    raw_state = await context.storage_state()
                    state: dict[str, Any] = dict(raw_state)
                    cookies = {
                        str(c["name"]): str(c["value"])
                        for c in state.get("cookies", [])
                        if "name" in c and "value" in c and c["value"] is not None
                    }

                    async with build_async_client(timeout=10.0, cookies=cookies) as test_client:
                        if not await validate_client_session(test_client):
                            raise RuntimeError(
                                "Browser indicated login success, but session probe returned unauthenticated."
                            )
                        account_id = await discover_account_id(test_client, state)
                        if account_id:
                            state["primary_account_id"] = account_id
                            if not reconcile_account_id(account_id):
                                restart = True

                    if not restart:
                        session_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
                        _write_credentials(username, password)
                        logger.info("Login successful. Session state saved to %s", session_file)

                finally:
                    with contextlib.suppress(Exception):
                        await context.close()
            finally:
                with contextlib.suppress(Exception):
                    await browser.close()

        if not restart:
            break
