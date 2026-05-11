"""JoyAgent customer-facing usage dashboard.

Single page, no sidebar, no menus. Shows the customer's own JoyAgent token
usage and an estimated cost (RMB) per row. NO balance, NO recharge, NO admin
features. Auto-paginates the JoyAgent billing-statistics API for any month
the customer selects (default: current month).

Setup
-----
    python -m pip install -r requirements.txt
    python -m playwright install chromium

First-time login (or after session expires):
    python client_dashboard.py --login

Run the dashboard:
    python client_dashboard.py
    # then open http://127.0.0.1:8766/
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

try:
    from playwright.sync_api import BrowserContext, Page, sync_playwright
except ImportError:
    print("Missing dependency: playwright")
    print("    pip install -r requirements.txt")
    print("    python -m playwright install chromium")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WORKSPACE_DIR = Path(__file__).resolve().parent
USER_DATA_DIR = WORKSPACE_DIR / "joyagent_profile"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8766

# Cache-busting build tag derived from this file's mtime + a hash of its bytes.
# Surfaced both in the HTML topbar (so the user can see at a glance whether the
# browser is showing the latest version) and in HTTP ETag/Last-Modified headers
# (so any intermediate cache returns a fresh copy).
def _compute_build_tag() -> tuple[str, int]:
    src = Path(__file__).resolve()
    try:
        mtime = int(src.stat().st_mtime)
        digest = hashlib.sha1(src.read_bytes()).hexdigest()[:8]
    except OSError:
        mtime = int(time.time())
        digest = "unknown"
    stamp = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
    return f"{stamp} #{digest}", mtime


BUILD_TAG, BUILD_MTIME = _compute_build_tag()

PROFILE_URL = "https://joyagent.jd.com/pl/profile?tab=usageStatistics"
USERINFO_API = "https://agentrs.jd.com/api/saas/user/v2/userInfo"
USAGE_API = "https://agentrs.jd.com/api/saas/billing-statistics/page"
RESOURCES_API = "https://agentrs.jd.com/api/saas/tenant-resource/v1/model-by-tenant?pageNo=1&pageSize=100"
TENANT_LIST_API = "https://agentrs.jd.com/api/saas/tenant/v1/list-by-user"

EXCHANGE_RATE = Decimal("7")

# Public list price per 1M tokens (USD). Calibrated against JD Cloud actual
# bills to ¥0.01 across two months on the reference tenant.
PRICING_USD_PER_M: dict[str, dict[str, Decimal]] = {
    "claude-opus-4.6": {
        "input":          Decimal("5"),
        "output":         Decimal("25"),
        "cache_write_5m": Decimal("6.25"),
        "cache_write_1h": Decimal("10"),
        "cache_read":     Decimal("0.50"),
    },
    "claude-sonnet-4.6": {
        "input":          Decimal("3"),
        "output":         Decimal("15"),
        "cache_write_5m": Decimal("3.75"),
        "cache_write_1h": Decimal("6"),
        "cache_read":     Decimal("0.30"),
    },
    "gemini-3.1-pro-preview": {
        "input":  Decimal("2"),
        "output": Decimal("12"),
    },
    "gpt-5.4": {
        "input":      Decimal("2.5"),
        "output":     Decimal("15"),
        "cache_read": Decimal("0.25"),
    },
}

TOKEN_TYPE_ALIASES: dict[str, str] = {
    "input": "input",
    "input_tokens": "input",
    "prompt_tokens": "input",
    "\u8f93\u5165": "input",                                 # 输入
    "output": "output",
    "output_tokens": "output",
    "completion_tokens": "output",
    "\u8f93\u51fa": "output",                                # 输出
    "prompt_cached_create_tokens_5m": "cache_write_5m",
    "cache_creation_input_tokens": "cache_write_5m",
    "cache_creation_5m_input_tokens": "cache_write_5m",
    "prompt_cached_create_tokens_1h": "cache_write_1h",
    "cache_creation_1h_input_tokens": "cache_write_1h",
    "prompt_cached_tokens": "cache_read",
    "cache_read_input_tokens": "cache_read",
}


# ---------------------------------------------------------------------------
# Cost helpers
# ---------------------------------------------------------------------------

def canonical_token_type(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return TOKEN_TYPE_ALIASES.get(text.lower()) or TOKEN_TYPE_ALIASES.get(text)


def compute_cost(model: str, token_type: Any, tokens: int) -> tuple[float, float, str | None]:
    canon = canonical_token_type(token_type)
    if not model or canon is None or tokens is None:
        return 0.0, 0.0, canon
    usd = PRICING_USD_PER_M.get(model, {}).get(canon)
    if usd is None:
        return 0.0, 0.0, canon
    raw = Decimal(int(tokens)) / Decimal("1000000") * usd * EXCHANGE_RATE
    raw_q = raw.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    floor_q = raw.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return float(raw_q), float(floor_q), canon


# ---------------------------------------------------------------------------
# Single-thread browser worker (Playwright sync API is greenlet-bound)
# ---------------------------------------------------------------------------

_BROWSER_QUEUE: "queue.Queue[tuple]" = queue.Queue()
_BROWSER_THREAD: threading.Thread | None = None
_BROWSER_LOCK = threading.Lock()
# Sentinel job: when received by the worker loop it tears down its own
# Playwright context cleanly so a fresh one can be created on the next request.
_RESTART_SENTINEL = object()


def _new_context(pw, headless: bool) -> BrowserContext:
    USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
    return pw.chromium.launch_persistent_context(
        user_data_dir=str(USER_DATA_DIR),
        headless=headless,
        viewport={"width": 1280, "height": 900},
        args=["--disable-blink-features=AutomationControlled"],
    )


def _browser_worker_loop() -> None:
    pw = sync_playwright().start()
    try:
        while True:
            # (Re)create a fresh persistent context. Recreating happens after
            # `_RESTART_SENTINEL` so freshly-saved login cookies (`--login`)
            # are picked up without restarting the whole server.
            context = _new_context(pw, headless=True)
            page = context.new_page()
            try:
                page.goto(PROFILE_URL, wait_until="domcontentloaded", timeout=60_000)
            except Exception as exc:
                print(f"  [client] initial goto warning: {exc}")

            stop = False
            restart = False
            while not stop and not restart:
                job, holder = _BROWSER_QUEUE.get()
                if job is None:
                    stop = True
                    break
                if job is _RESTART_SENTINEL:
                    restart = True
                    if holder is not None:
                        holder["result"] = "restarted"
                        holder["done"].set()
                    break
                try:
                    holder["result"] = job(page)
                except Exception as exc:
                    holder["error"] = exc
                finally:
                    holder["done"].set()

            try:
                context.close()
            except Exception:
                pass
            if stop:
                return
    finally:
        try:
            pw.stop()
        except Exception:
            pass


def _ensure_browser_worker() -> None:
    global _BROWSER_THREAD
    with _BROWSER_LOCK:
        if _BROWSER_THREAD is not None and _BROWSER_THREAD.is_alive():
            return
        t = threading.Thread(target=_browser_worker_loop, name="client-browser", daemon=True)
        t.start()
        _BROWSER_THREAD = t


def with_remote_page(job, timeout: float = 120.0):
    _ensure_browser_worker()
    holder: dict = {"done": threading.Event(), "result": None, "error": None}
    _BROWSER_QUEUE.put((job, holder))
    if not holder["done"].wait(timeout=timeout):
        raise TimeoutError(f"Browser job timed out after {timeout:.1f}s")
    if holder["error"] is not None:
        raise holder["error"]
    return holder["result"]


def restart_browser_worker(timeout: float = 30.0) -> tuple[bool, str]:
    """Drain the queue with a sentinel so the worker rebuilds its Playwright
    context. Safe to call when no worker exists (becomes a no-op)."""
    if _BROWSER_THREAD is None or not _BROWSER_THREAD.is_alive():
        return True, "browser worker not running (will start fresh on next request)"
    holder: dict = {"done": threading.Event(), "result": None, "error": None}
    _BROWSER_QUEUE.put((_RESTART_SENTINEL, holder))
    if not holder["done"].wait(timeout=timeout):
        return False, "restart sentinel timed out"
    return True, "browser worker restarted; new request will re-read profile"


# ---------------------------------------------------------------------------
# Subprocess-driven login: the web UI's "登录 JoyAgent" button spawns a
# `python client_dashboard.py --login` child so a real, headed browser can pop
# up for the user. We capture the child's stdout so the page can show live
# progress without making the user open another terminal.
# ---------------------------------------------------------------------------

_LOGIN_LOCK = threading.Lock()
_LOGIN_PROC: subprocess.Popen | None = None
_LOGIN_LOG: collections.deque[str] = collections.deque(maxlen=120)
_LOGIN_MODE: str = ""  # "login" or "switch-tenant"


def _login_reader(proc: subprocess.Popen) -> None:
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            _LOGIN_LOG.append(line.rstrip("\r\n"))
    except Exception as exc:
        _LOGIN_LOG.append(f"[reader error] {exc}")
    finally:
        try:
            proc.stdout.close()  # type: ignore[union-attr]
        except Exception:
            pass


def start_login_subprocess(mode: str, target: str | None = None) -> tuple[bool, str]:
    """Launch `python client_dashboard.py --login` (or --switch-tenant) as a
    child process and stream its output into _LOGIN_LOG. Only one login can
    run at a time; calling again while a login is in flight is a no-op."""
    global _LOGIN_PROC, _LOGIN_MODE
    with _LOGIN_LOCK:
        if _LOGIN_PROC is not None and _LOGIN_PROC.poll() is None:
            return False, "another login is already running"
        if mode not in ("login", "switch-tenant"):
            return False, f"unknown login mode: {mode}"

        _LOGIN_LOG.clear()
        _LOGIN_MODE = mode
        cmd = [sys.executable, str(Path(__file__).resolve())]
        if mode == "login":
            cmd.append("--login")
            if target:
                # --login forwards target_tenant via --switch-tenant value
                cmd += ["--switch-tenant", target]
        else:
            cmd.append("--switch-tenant")
            if target:
                cmd.append(target)

        creationflags = 0
        if os.name == "nt":
            # Hide the python console window of the child; Chromium's own
            # window is still visible because it's a separate process.
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

        # Make sure the child's stdout is line-buffered UTF-8 even when stdout
        # is a pipe and the parent is on Windows (default cp936 mojibake).
        env = dict(os.environ)
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUNBUFFERED"] = "1"

        try:
            _LOGIN_PROC = subprocess.Popen(
                cmd,
                cwd=str(WORKSPACE_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env=env,
                creationflags=creationflags,
            )
        except Exception as exc:
            return False, f"spawn failed: {exc}"

        threading.Thread(
            target=_login_reader, args=(_LOGIN_PROC,), daemon=True, name="login-reader"
        ).start()
        return True, f"login subprocess started (pid={_LOGIN_PROC.pid})"


def login_subprocess_status() -> dict:
    """Snapshot of the current/last login subprocess for the web UI."""
    proc = _LOGIN_PROC
    log = list(_LOGIN_LOG)
    if proc is None:
        return {"state": "idle", "mode": "", "exit_code": None, "log": log}
    rc = proc.poll()
    if rc is None:
        return {"state": "running", "mode": _LOGIN_MODE, "pid": proc.pid, "exit_code": None, "log": log}
    state = "succeeded" if rc == 0 else "failed"
    return {"state": state, "mode": _LOGIN_MODE, "pid": proc.pid, "exit_code": rc, "log": log}


def cancel_login_subprocess() -> tuple[bool, str]:
    proc = _LOGIN_PROC
    if proc is None or proc.poll() is not None:
        return True, "no login running"
    try:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        return True, f"terminated pid {proc.pid}"
    except Exception as exc:
        return False, f"terminate failed: {exc}"


def reset_local_state(*, drop_profile: bool = True) -> list[str]:
    """Wipe local-only state. Currently the customer dashboard has no SQLite
    cache, only the persistent login profile. Stops the worker first so the
    profile directory isn't held by a running browser process on Windows."""
    notes: list[str] = []
    try:
        restart_browser_worker(timeout=10.0)
        notes.append("stopped in-memory browser worker")
    except Exception as exc:
        notes.append(f"worker stop warning: {exc}")
    if drop_profile and USER_DATA_DIR.exists():
        try:
            shutil.rmtree(USER_DATA_DIR, ignore_errors=False)
            notes.append(f"deleted login profile: {USER_DATA_DIR}")
        except OSError as exc:
            notes.append(f"profile delete failed: {exc}")
    else:
        notes.append("no profile to delete")
    return notes


# ---------------------------------------------------------------------------
# JoyAgent API (via page-context fetch so cookies/Referer/CSRF match)
# ---------------------------------------------------------------------------

_FETCH_JS = """
async (url) => {
  const resp = await fetch(url, {
    method: 'GET',
    credentials: 'include',
    headers: { 'Accept': 'application/json' }
  });
  const text = await resp.text();
  try { return { status: resp.status, json: JSON.parse(text) }; }
  catch (e) { return { status: resp.status, json: null, text: text }; }
}
"""


def _page_fetch(page: Page, url: str) -> dict | None:
    try:
        result = page.evaluate(_FETCH_JS, url)
    except Exception as exc:
        print(f"  [client] fetch failed for {url}: {exc}")
        return None
    if not isinstance(result, dict) or result.get("status") != 200:
        return None
    return result.get("json") if isinstance(result.get("json"), dict) else None


def fetch_userinfo(page: Page) -> dict | None:
    data = _page_fetch(page, USERINFO_API)
    if not data or data.get("code") != 0:
        return None
    return data.get("data") or {}


def fetch_resources(page: Page) -> list[dict]:
    data = _page_fetch(page, RESOURCES_API)
    if not data or data.get("code") != 0:
        return []
    return ((data.get("data") or {}).get("list") or [])


def fetch_billing_page(page: Page, dt_month: str, page_no: int, page_size: int) -> dict | None:
    url = f"{USAGE_API}?pageNo={page_no}&pageSize={page_size}&dtMonth={dt_month}"
    data = _page_fetch(page, url)
    if not data or data.get("code") != 0:
        return None
    return data.get("data") or {}


def fetch_billing_all(page: Page, dt_month: str, page_size: int = 100) -> tuple[list[dict], dict]:
    """Paginate through every record for the given month. Returns (rows, info)."""
    rows: list[dict] = []
    page_no = 1
    total = None
    while True:
        d = fetch_billing_page(page, dt_month, page_no, page_size)
        if d is None:
            return rows, {"month": dt_month, "total": total, "fetched": len(rows), "error": "fetch failed (session expired?)"}
        items = d.get("list") or []
        rows.extend(items)
        if total is None:
            total = d.get("total")
        try:
            total_int = int(total) if total is not None else None
        except (TypeError, ValueError):
            total_int = None
        if total_int is not None and len(rows) >= total_int:
            break
        if len(items) < page_size:
            break
        page_no += 1
        if page_no > 500:
            return rows, {"month": dt_month, "total": total, "fetched": len(rows), "error": "stopped at 500 pages"}
    return rows, {"month": dt_month, "total": total, "fetched": len(rows), "error": None}


# ---------------------------------------------------------------------------
# Login flow
# ---------------------------------------------------------------------------

def _probe_logged_in(page: Page) -> tuple[bool, dict | None]:
    """Real login check: call userInfo API in the page context, return (ok, info).

    JoyAgent's profile page does NOT redirect to a login URL when you are not
    signed in - it just renders empty data. So we cannot rely on URL inspection;
    we must call the API and look at code/userId.
    """
    try:
        info = fetch_userinfo(page)
    except Exception:
        return False, None
    if not info or not info.get("userId"):
        return False, info
    # userInfo can briefly return a userId while other SaaS APIs still return
    # 401 during SSO/tenant switching. Treat that as "not ready yet".
    if str(info.get("statusCode") or "") == "401":
        return False, info

    # Require at least one business API to be usable before closing the login
    # browser. This prevents persisting a half-complete login state.
    for url in (TENANT_LIST_API, RESOURCES_API, f"{USAGE_API}?pageNo=1&pageSize=1&dtMonth={datetime.now().strftime('%Y-%m')}"):
        data = _page_fetch(page, url)
        if isinstance(data, dict) and data.get("code") == 0:
            return True, info
    return False, info


def fetch_tenant_list(page: Page) -> list[dict]:
    data = _page_fetch(page, TENANT_LIST_API)
    if not isinstance(data, dict) or data.get("code") != 0:
        return []
    payload = data.get("data")
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("list") or []
    return []


def switch_to_tenant_space(page: Page, target: str | None = None) -> tuple[bool, str]:
    """Click the avatar dropdown -> '切换空间' -> select the tenant entry.

    target: tenant name OR jdAccount OR id. If None, the only-non-personal
    tenant is auto-selected (typical case for an invited member).
    Returns (ok, message).
    """
    tenants = fetch_tenant_list(page)
    if not tenants:
        return False, "tenant list empty (not logged in?)"

    candidates: list[str] = []
    if target:
        candidates.append(str(target))
    else:
        for t in tenants:
            for key in ("name", "jdAccount", "id"):
                v = t.get(key)
                if v:
                    candidates.append(str(v))

    try:
        page.goto(PROFILE_URL, wait_until="networkidle", timeout=60_000)
    except Exception:
        pass
    time.sleep(2)

    try:
        avatar = page.locator(".ant-dropdown-trigger").first
        avatar.scroll_into_view_if_needed()
        avatar.hover()
        time.sleep(0.5)
        avatar.click()
        time.sleep(1.5)
    except Exception as exc:
        return False, f"open avatar dropdown failed: {exc}"

    try:
        switch_entry = page.get_by_text("\u5207\u6362\u7a7a\u95f4", exact=False)  # 切换空间
        if switch_entry.count() == 0:
            return False, "'切换空间' entry not found in dropdown"
        switch_entry.first.scroll_into_view_if_needed()
        switch_entry.first.hover()
        time.sleep(1.5)
    except Exception as exc:
        return False, f"hover '切换空间' failed: {exc}"

    matched: str | None = None
    for cand in candidates:
        try:
            loc = page.get_by_text(cand, exact=False)
            if loc.count() == 0:
                continue
            loc.first.scroll_into_view_if_needed()
            loc.first.click()
            matched = cand
            break
        except Exception:
            continue
    if not matched:
        return False, f"none of the candidates matched in sub-menu: {candidates}"

    time.sleep(4)
    try:
        page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass
    time.sleep(2)

    info = _page_fetch(page, USERINFO_API) or {}
    data = info.get("data") if isinstance(info, dict) else None
    new_tenant = (data or {}).get("tenantName")
    if not new_tenant:
        return False, f"clicked '{matched}' but tenantName is still empty"
    return True, f"switched to tenant '{new_tenant}' (clicked '{matched}')"


def auto_switch_if_needed(page: Page, target: str | None = None) -> str | None:
    """If the current login has no tenant context but exactly one non-personal
    tenant is available, auto-switch into it. Returns a status message."""
    info = _page_fetch(page, USERINFO_API) or {}
    data = info.get("data") if isinstance(info, dict) else None
    if data and data.get("tenantName"):
        return None
    tenants = fetch_tenant_list(page)
    if not tenants:
        return "tenant list empty - cannot auto switch"
    if target is None and len(tenants) > 1:
        names = ", ".join(str(t.get("name") or t.get("jdAccount") or t.get("id")) for t in tenants)
        return f"multiple tenants available ({names}); use --switch-tenant <name>"
    ok, msg = switch_to_tenant_space(page, target)
    return msg


def login_only(timeout_seconds: int = 600, target_tenant: str | None = None) -> None:
    print("Opening browser for JoyAgent customer login.")
    print("Sign in inside the browser window. The script polls the userInfo API")
    print("every 2 seconds and exits as soon as your account is detected.")
    print(f"Profile dir: {USER_DATA_DIR}")
    print(f"(timeout: {timeout_seconds}s, press Ctrl+C to abort)\n")

    with sync_playwright() as pw:
        context = _new_context(pw, headless=False)
        page = context.new_page()
        try:
            page.goto(PROFILE_URL, wait_until="domcontentloaded", timeout=60_000)
        except Exception as exc:
            print(f"goto failed (continue anyway): {exc}")

        time.sleep(2)
        deadline = time.time() + timeout_seconds
        last_print = 0.0
        success = False
        info: dict | None = None
        while time.time() < deadline:
            ok, info = _probe_logged_in(page)
            if ok:
                print(f"Login detected: userId={info.get('userId')}  realName={info.get('realName')}  tenant={info.get('tenantName')}")
                print("Waiting 3s for cookies to settle...")
                time.sleep(3)
                success = True
                break
            now = time.time()
            if now - last_print > 10:
                try:
                    cur_url = page.url or ""
                except Exception:
                    cur_url = ""
                print(f"  not logged in yet... current URL: {cur_url}")
                last_print = now
            time.sleep(2)

        if success and not (info or {}).get("tenantName"):
            print("No tenant context set. Attempting auto-switch to enterprise space...")
            msg = auto_switch_if_needed(page, target_tenant)
            if msg:
                print(f"  -> {msg}")

        try:
            context.close()
        except Exception:
            pass

    if success:
        print(f"\nLogin profile saved at: {USER_DATA_DIR}")
        print("Now run:  python client_dashboard.py")
    else:
        print("\nWARNING: timed out before userInfo returned a valid userId.")
        print("Re-run `python client_dashboard.py --login` to try again.")


def switch_tenant_cli(target: str | None = None, headless: bool = True) -> None:
    """Standalone CLI: open browser with the saved profile and switch tenant."""
    print(f"Switching tenant space (target={target!r}, headless={headless})")
    with sync_playwright() as pw:
        ctx = _new_context(pw, headless=headless)
        page = ctx.new_page()
        try:
            page.goto(PROFILE_URL, wait_until="networkidle", timeout=60_000)
        except Exception as exc:
            print(f"goto failed: {exc}")
        time.sleep(2)
        ok, msg = switch_to_tenant_space(page, target)
        print(("OK: " if ok else "FAIL: ") + msg)
        try:
            ctx.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# HTTP payloads
# ---------------------------------------------------------------------------

def normalize_billing_row(raw: dict) -> dict | None:
    model = raw.get("resourceName")
    token_type = raw.get("billingTokenType")
    qty = raw.get("billingQuantity")
    if not model or not token_type or qty is None:
        return None
    try:
        tokens = int(float(qty))
    except (TypeError, ValueError):
        return None
    if tokens < 0:
        return None
    raw_cny, floor_cny, canon = compute_cost(model, token_type, tokens)
    return {
        "call_time": raw.get("statTime") or raw.get("dt") or "",
        "day": (raw.get("statTime") or raw.get("dt") or "")[:10],
        "account_pin": str(raw.get("invokeUserId") or ""),
        "username": str(raw.get("realName") or ""),
        "model": str(model),
        "token_type_raw": str(token_type),
        "token_type": canon or str(token_type),
        "tokens": tokens,
        "cny_raw": raw_cny,
        "cny_floor": floor_cny,
    }


def build_user_payload() -> dict:
    def _job(page):
        ok, info = _probe_logged_in(page)
        tenants = fetch_tenant_list(page) if ok else []
        return ok, info, tenants
    try:
        ok, info, tenants = with_remote_page(_job)
    except Exception as exc:
        return {"logged_in": False, "error": str(exc)}
    if not ok or not info or not info.get("userId"):
        return {"logged_in": False, "error": "session expired"}
    tenant_name = info.get("tenantName")
    return {
        "logged_in": True,
        "userId": info.get("userId"),
        "realName": info.get("realName"),
        "tenantName": tenant_name,
        "tenant_missing": not tenant_name,
        "available_tenants": [
            {"name": t.get("name"), "jdAccount": t.get("jdAccount"), "id": t.get("id")}
            for t in tenants
        ],
    }


def build_usage_payload(query: dict[str, list[str]]) -> dict:
    months_param = (query.get("months") or query.get("month") or [datetime.now().strftime("%Y-%m")])[0]
    months = [m.strip() for m in months_param.split(",") if m.strip()]
    if not months:
        months = [datetime.now().strftime("%Y-%m")]

    def _job(page):
        ok, info = _probe_logged_in(page)
        if not ok or not info or not info.get("userId"):
            tenants = fetch_tenant_list(page)
            return {
                "logged_in": False,
                "error": "session expired",
                "available_tenants": [
                    {"name": t.get("name"), "jdAccount": t.get("jdAccount"), "id": t.get("id")}
                    for t in tenants
                ],
            }
        tenants = fetch_tenant_list(page)
        resources = fetch_resources(page)
        all_rows: list[dict] = []
        month_info: list[dict] = []
        for m in months:
            rows, meta = fetch_billing_all(page, m)
            month_info.append(meta)
            for r in rows:
                norm = normalize_billing_row(r)
                if norm:
                    all_rows.append(norm)
        return {
            "logged_in": True,
            "userinfo": {
                "userId": info.get("userId"),
                "realName": info.get("realName"),
                "tenantName": info.get("tenantName"),
                "tenant_missing": not info.get("tenantName"),
            },
            "available_tenants": [
                {"name": t.get("name"), "jdAccount": t.get("jdAccount"), "id": t.get("id")}
                for t in tenants
            ],
            "resources": [
                {"label": r.get("label") or r.get("resourceName") or r.get("modelName"),
                 "modelId": r.get("modelId")}
                for r in resources
                if (r.get("label") or r.get("resourceName") or r.get("modelName"))
            ],
            "rows": all_rows,
            "month_info": month_info,
        }

    try:
        payload = with_remote_page(_job)
    except Exception as exc:
        return {"logged_in": False, "error": str(exc), "months": months}

    if not payload.get("logged_in"):
        return {"logged_in": False, "months": months}

    payload["months"] = months
    payload["generated_at"] = datetime.now().isoformat(timespec="seconds")
    return payload


def build_debug_payload() -> dict:
    """Return raw API probes from the same browser worker used by the app.

    This is intentionally read-only and helps diagnose customer-account tenant
    context issues: a personal login may be valid while no JoyAgent tenant is
    selected, yielding zero usage.
    """
    def _job(page):
        urls = {
            "userInfo": USERINFO_API,
            "tenantList": TENANT_LIST_API,
            "resources": RESOURCES_API,
            "usageCurrent": f"{USAGE_API}?pageNo=1&pageSize=5&dtMonth={datetime.now().strftime('%Y-%m')}",
            "usage2026_05": f"{USAGE_API}?pageNo=1&pageSize=5&dtMonth=2026-05",
            "usage2026_04": f"{USAGE_API}?pageNo=1&pageSize=5&dtMonth=2026-04",
        }
        out = {"page_url": page.url, "responses": {}}
        for name, url in urls.items():
            out["responses"][name] = _page_fetch(page, url)
        return out

    try:
        payload = with_remote_page(_job)
        payload["generated_at"] = datetime.now().isoformat(timespec="seconds")
        return payload
    except Exception as exc:
        return {"error": str(exc), "generated_at": datetime.now().isoformat(timespec="seconds")}


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

_CLOSED = (ConnectionAbortedError, ConnectionResetError, BrokenPipeError, TimeoutError)


class ClientHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        return

    def log_error(self, fmt: str, *args) -> None:
        return

    def handle_one_request(self) -> None:
        try:
            super().handle_one_request()
        except _CLOSED:
            self.close_connection = True

    def _send(self, status: int, ctype: str, body: bytes, *, etag: str | None = None) -> None:
        try:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            # Triple-belt cache busting: no-store covers modern browsers, the
            # legacy Pragma+Expires pair handles older proxies, and an ETag lets
            # us return 304 only when the bytes literally have not changed.
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            if etag:
                self.send_header("ETag", etag)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
        except _CLOSED:
            self.close_connection = True

    def send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send(status, "application/json; charset=utf-8", body)

    def send_html(self, html: str) -> None:
        # Inject the build tag at send time so the user can see a per-deploy
        # cache-bust marker in the topbar without us editing the raw HTML
        # template.
        rendered = html.replace("__BUILD_TAG__", BUILD_TAG)
        body = rendered.encode("utf-8")
        # ETag derives from the actual bytes so the browser refetches whenever
        # the python source file changes. We still set no-store so clients
        # that ignore ETags still get fresh HTML.
        etag = '"' + hashlib.sha1(body).hexdigest()[:16] + '"'
        if_none_match = self.headers.get("If-None-Match")
        if if_none_match == etag:
            self._send(304, "text/html; charset=utf-8", b"", etag=etag)
            return
        self._send(200, "text/html; charset=utf-8", body, etag=etag)

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            if path == "/":
                self.send_html(INDEX_HTML)
                return
            if path == "/api/userinfo":
                self.send_json(build_user_payload())
                return
            if path == "/api/usage":
                self.send_json(build_usage_payload(parse_qs(parsed.query)))
                return
            if path == "/api/debug":
                self.send_json(build_debug_payload())
                return
            if path == "/api/restart-worker":
                # Customer-facing "clear in-process session cache" button:
                # tears down the running headless Playwright context so the
                # next request re-reads the saved profile from disk. Use this
                # right after `--login` so the new cookies take effect without
                # restarting the whole server.
                ok, msg = restart_browser_worker()
                self.send_json({"ok": ok, "message": msg})
                return
            if path == "/api/login-status":
                self.send_json(login_subprocess_status())
                return
            self.send_json({"error": "not found"}, status=404)
        except _CLOSED:
            self.close_connection = True
        except Exception as exc:
            try:
                self.send_json({"error": str(exc)}, status=500)
            except _CLOSED:
                self.close_connection = True

    def do_POST(self) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            if path in ("/api/login-start", "/api/login-cancel"):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length > 0 else b""
                try:
                    payload = json.loads(raw.decode("utf-8")) if raw else {}
                except Exception:
                    payload = {}
                if path == "/api/login-start":
                    mode = str(payload.get("mode") or "login")
                    target = payload.get("target") or None
                    ok, msg = start_login_subprocess(mode, target=target)
                    self.send_json({"ok": ok, "message": msg, "status": login_subprocess_status()})
                    return
                if path == "/api/login-cancel":
                    ok, msg = cancel_login_subprocess()
                    self.send_json({"ok": ok, "message": msg})
                    return
            self.send_json({"error": "not found"}, status=404)
        except _CLOSED:
            self.close_connection = True
        except Exception as exc:
            try:
                self.send_json({"error": str(exc)}, status=500)
            except _CLOSED:
                self.close_connection = True

    do_HEAD = do_GET


# ---------------------------------------------------------------------------
# HTML page
# ---------------------------------------------------------------------------

INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>JoyAgent 用量明细</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #F8F9FA;
      --card: #FFFFFF;
      --text: #202122;
      --mid: #525357;
      --muted: #83858B;
      --line: #EAEAEB;
      --line-2: #D3D7DD;
      --primary: #3568FF;
      --primary-tint: rgba(53, 104, 255, 0.08);
      --warn: #B45309;
      --shadow: 0 2px 6px rgba(15, 23, 42, 0.05);
    }
    * { box-sizing: border-box; }
    html, body { margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Microsoft YaHei",
                   "Segoe UI", system-ui, sans-serif;
      color: var(--text);
      background: var(--bg);
      font-size: 14px;
      line-height: 1.5;
    }
    a { color: inherit; }
    button { font: inherit; cursor: pointer; }

    .topbar {
      position: sticky;
      top: 0;
      z-index: 50;
      background: var(--card);
      border-bottom: 1px solid var(--line);
      padding: 0 24px;
      height: 60px;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .brand { font-weight: 700; letter-spacing: -0.02em; font-size: 18px; display: flex; align-items: baseline; gap: 10px; }
    .build-tag {
      font-size: 11px; font-weight: 400; letter-spacing: 0;
      color: var(--muted); background: #F1F5F9;
      padding: 2px 8px; border-radius: 999px;
      font-family: SFMono-Regular, Menlo, Consolas, monospace;
    }
    .topbar-meta { color: var(--muted); font-size: 13px; display: flex; gap: 12px; align-items: center; }
    .topbar-meta strong { color: var(--text); font-weight: 600; }

    .wrap { max-width: 1400px; margin: 0 auto; padding: 20px 24px 40px; }

    .toolbar {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px 16px;
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      align-items: center;
      margin-bottom: 14px;
      box-shadow: var(--shadow);
    }
    .field {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      height: 34px;
      padding: 0 10px;
      background: #fff;
      border: 1px solid var(--line-2);
      border-radius: 8px;
    }
    .field label { color: var(--muted); }
    .field input, .field select {
      border: none; outline: none; background: transparent; font: inherit;
      color: var(--text); min-width: 120px;
    }
    .field select { appearance: none; cursor: pointer; padding-right: 16px; }
    .btn {
      height: 34px; padding: 0 14px;
      background: #fff; color: var(--text);
      border: 1px solid var(--line-2);
      border-radius: 8px;
      display: inline-flex; align-items: center; gap: 4px;
    }
    .btn:hover:not([disabled]) { border-color: var(--text); }
    .btn-primary { background: var(--primary); color: #fff; border-color: var(--primary); }
    .btn-primary:hover:not([disabled]) { background: #2a55d8; border-color: #2a55d8; }
    .btn[disabled] { opacity: 0.45; cursor: not-allowed; }
    .grow { flex: 1; }

    .month-tag {
      display: inline-flex; align-items: center; gap: 6px;
      padding: 4px 10px; border-radius: 999px;
      background: var(--primary-tint); color: var(--primary);
      font-size: 13px;
    }
    .month-tag button {
      background: none; border: none; color: var(--primary); cursor: pointer;
      font-size: 14px; line-height: 1; padding: 0;
    }

    .metric-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(180px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }
    .metric {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px 16px;
      box-shadow: var(--shadow);
    }
    .metric-label { font-size: 12px; color: var(--muted); }
    .metric-value { font-size: 24px; font-weight: 700; margin-top: 4px; }
    .metric-foot { font-size: 12px; color: var(--muted); margin-top: 6px; }

    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px;
      box-shadow: var(--shadow);
      margin-bottom: 14px;
      min-width: 0;
      overflow: hidden;
    }
    .card h2 { font-size: 15px; margin: 0 0 10px; font-weight: 600; }
    .card .sub { color: var(--muted); font-size: 12px; margin-left: 8px; font-weight: 400; }

    .table-wrap { overflow: auto; max-width: 100%; max-height: 65vh; border: 1px solid var(--line); border-radius: 8px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; min-width: 900px; }
    th, td {
      padding: 8px 10px; text-align: left; white-space: nowrap;
      border-bottom: 1px solid var(--line);
    }
    th {
      background: #FAFAFA; color: var(--mid); font-weight: 500;
      position: sticky; top: 0; z-index: 1;
    }
    tbody tr:hover { background: #FAFBFC; }
    tbody tr:last-child td { border-bottom: none; }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
    .empty { padding: 36px 0; text-align: center; color: var(--muted); }

    .pager {
      display: flex; align-items: center; justify-content: flex-end;
      gap: 10px; padding: 10px 4px 0; font-size: 13px; color: var(--mid);
    }
    .pager select {
      height: 28px; border: 1px solid var(--line-2); border-radius: 6px;
      background: #fff; padding: 0 6px; font: inherit;
    }
    .pager .muted { color: var(--muted); }

    .login-screen {
      max-width: 480px; margin: 96px auto 40px;
      background: var(--card); border: 1px solid var(--line);
      border-radius: 16px; padding: 36px 36px 28px; text-align: center;
      box-shadow: 0 6px 30px rgba(15, 23, 42, 0.08);
    }
    .login-icon {
      width: 64px; height: 64px; border-radius: 50%;
      background: var(--primary-tint);
      color: var(--primary); font-size: 30px;
      display: inline-flex; align-items: center; justify-content: center;
      margin: 0 auto 16px;
    }
    .login-screen h1 { font-size: 20px; margin: 0 0 8px; font-weight: 600; }
    .login-screen p.login-lead { color: var(--mid); font-size: 14px; line-height: 1.6; margin: 0 0 4px; }
    .login-screen p.login-note {
      color: var(--warn); font-size: 13px; line-height: 1.5;
      background: #FFFBEB; border: 1px solid #FDE68A;
      border-radius: 8px; padding: 8px 12px; margin: 14px 0 0;
    }
    .login-screen p.login-hint {
      color: var(--muted); font-size: 12px; line-height: 1.5;
      margin: 14px 0 0;
    }

    .tenant-picker { margin: 18px 0 4px; text-align: left; }
    .tenant-picker label {
      display: block; font-size: 12px; color: var(--muted);
      margin-bottom: 4px;
    }
    .tenant-picker select {
      width: 100%; height: 38px; padding: 0 10px;
      border: 1px solid var(--line-2); border-radius: 8px;
      background: #fff; font: inherit; color: var(--text);
    }

    .login-actions {
      margin-top: 22px;
      display: flex; gap: 10px; justify-content: center; flex-wrap: wrap;
    }
    /* Inside the login card every button shares the same height/padding so
       primary and secondary actions read as a balanced pair, regardless of
       whether the markup tagged them as `.big`. */
    .login-actions .btn {
      height: 44px;
      padding: 0 22px;
      font-size: 14px;
      font-weight: 500;
      min-width: 168px;
      justify-content: center;
    }
    .btn.big { height: 44px; padding: 0 22px; font-size: 14px; font-weight: 500; }
    @media (max-width: 480px) {
      .login-actions { flex-direction: column; align-items: stretch; }
      .login-actions .btn { width: 100%; min-width: 0; }
    }

    .login-status {
      margin-top: 18px; padding: 14px;
      border-radius: 10px; text-align: left;
      border: 1px solid var(--line);
      background: #FAFBFC;
    }
    .login-status.running { border-color: var(--primary); background: var(--primary-tint); }
    .login-status.success { border-color: #34D399; background: #ECFDF5; }
    .login-status.failed  { border-color: #F87171; background: #FEF2F2; }
    .login-status-title { font-weight: 600; font-size: 13.5px; color: var(--text); }
    .login-status-hint  { color: var(--mid); font-size: 12px; margin-top: 4px; line-height: 1.5; }
    .login-status-line  { color: var(--mid); font-size: 13px; }
    .login-log {
      margin: 10px 0 0; padding: 10px 12px;
      background: #0F172A; color: #CBD5E1; border-radius: 8px;
      font-family: SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 11.5px; line-height: 1.55;
      max-height: 180px; overflow: auto; white-space: pre-wrap;
    }

    .pill {
      display: inline-flex; align-items: center;
      padding: 2px 8px; border-radius: 999px;
      font-size: 12px;
    }
    .pill-info { background: var(--primary-tint); color: var(--primary); }
    .pill-warn { background: #FFFBEB; color: var(--warn); }

    .summary-grid {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      gap: 14px;
      margin-bottom: 14px;
    }
    .summary-grid .card { min-width: 0; }
    .summary-grid .table-wrap { max-height: none; overflow-x: hidden; }
    .summary-grid table { min-width: 0; table-layout: fixed; }
    .summary-grid th, .summary-grid td { white-space: normal; vertical-align: top; }
    .summary-grid th.num, .summary-grid td.num { white-space: nowrap; width: 86px; }
    .summary-grid td:first-child { overflow-wrap: anywhere; }
    .summary-grid .token-table th:nth-child(2),
    .summary-grid .token-table td:nth-child(2) { width: 120px; }
    .summary-grid .token-table th:nth-child(3),
    .summary-grid .token-table td:nth-child(3) { width: 64px; }
    .summary-grid .token-table th:nth-child(4),
    .summary-grid .token-table td:nth-child(4) { width: 96px; }
    .summary-grid .model-table th:nth-child(2),
    .summary-grid .model-table td:nth-child(2) { width: 62px; }
    .summary-grid .model-table th:nth-child(3),
    .summary-grid .model-table td:nth-child(3) { width: 120px; }
    .summary-grid .model-table th:nth-child(4),
    .summary-grid .model-table td:nth-child(4) { width: 96px; }
    @media (max-width: 1100px) {
      .metric-grid { grid-template-columns: repeat(2, 1fr); }
      .summary-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 720px) {
      .metric-grid { grid-template-columns: 1fr; }
      .toolbar { gap: 8px; padding: 12px; }
      .field { width: 100%; justify-content: space-between; }
      .field input, .field select { min-width: 0; flex: 1; }
      .summary-grid .table-wrap { overflow-x: auto; }
      .summary-grid table { min-width: 560px; }
    }

    /* New components for the customer view rewrite */
    .row-bar { display: flex; align-items: center; gap: 8px; }
    .row-bar .bar-track {
      flex: 1; height: 6px; background: #F1F5F9; border-radius: 999px; overflow: hidden;
    }
    .row-bar .bar-fill {
      height: 100%; background: linear-gradient(90deg, var(--primary), #87b3ff);
      border-radius: 999px;
    }
    .row-bar .bar-pct { color: var(--muted); font-size: 12px; min-width: 44px; text-align: right; }

    .trend-chart { display: flex; align-items: flex-end; gap: 3px; height: 96px; padding: 0 4px; border-bottom: 1px solid var(--line); }
    .trend-bar { flex: 1; min-width: 6px; display: flex; align-items: flex-end; height: 100%; cursor: default; }
    .trend-bar .fill {
      width: 100%; min-height: 2px;
      background: linear-gradient(180deg, var(--primary), #87b3ff);
      border-radius: 3px 3px 0 0;
      transition: height 200ms ease;
    }
    .trend-bar:hover .fill { background: linear-gradient(180deg, #1f4ed8, #6ea1ff); }
    .trend-axis { display: flex; justify-content: space-between; font-size: 11px; color: var(--muted); padding: 4px 4px 0; }
    .trend-foot { color: var(--muted); font-size: 12px; margin-top: 6px; }

    .token-type-pill {
      display: inline-flex; gap: 6px; align-items: center;
      padding: 2px 8px; border-radius: 999px;
      background: var(--primary-tint); color: var(--primary); font-size: 12px;
      max-width: 100%;
      white-space: normal;
      overflow-wrap: anywhere;
    }

    /* "按 Token 类型汇总" uses a compact stat-row list instead of a table
       because there are only 4-5 categories and a fixed-column table makes
       the cells look jagged when pill widths and raw type names vary widely. */
    .ttype-list { display: flex; flex-direction: column; gap: 12px; padding: 4px 2px 2px; }
    .ttype-row {
      display: grid;
      grid-template-columns: minmax(96px, 140px) minmax(60px, 1fr) minmax(120px, 180px);
      gap: 12px; align-items: center;
    }
    .ttype-label { display: flex; align-items: center; gap: 8px; font-size: 13px; min-width: 0; }
    .ttype-name { font-weight: 500; color: var(--text); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .ttype-dot { width: 10px; height: 10px; border-radius: 999px; background: var(--primary); flex-shrink: 0; }
    .ttype-dot.dot-input          { background: #2563EB; }
    .ttype-dot.dot-output         { background: #10B981; }
    .ttype-dot.dot-cache_read     { background: #F59E0B; }
    .ttype-dot.dot-cache_write_5m { background: #8B5CF6; }
    .ttype-dot.dot-cache_write_1h { background: #EC4899; }
    .ttype-bar { height: 8px; background: #F1F5F9; border-radius: 999px; overflow: hidden; min-width: 0; }
    .ttype-fill {
      height: 100%;
      background: linear-gradient(90deg, var(--primary), #87b3ff);
      border-radius: 999px;
      transition: width 200ms ease;
    }
    .ttype-meta { text-align: right; font-variant-numeric: tabular-nums; min-width: 0; }
    .ttype-pct { font-size: 13px; color: var(--text); font-weight: 600; }
    .ttype-amt { font-size: 11px; color: var(--muted); margin-top: 2px; white-space: nowrap; }
    .ttype-amt strong { color: var(--text); font-weight: 500; }
    @media (max-width: 480px) {
      .ttype-row {
        grid-template-columns: minmax(0, 1fr) minmax(110px, auto);
        grid-template-areas: "label meta" "bar bar";
        row-gap: 6px;
      }
      .ttype-label { grid-area: label; }
      .ttype-meta  { grid-area: meta; }
      .ttype-bar   { grid-area: bar; }
    }
    .empty-with-action { padding: 28px 20px; text-align: center; color: var(--muted); }
    .empty-with-action .why { color: var(--warn); margin-bottom: 10px; }
    .clear-filter-btn { display: none; }
    .clear-filter-btn.active { display: inline-flex; }
  </style>
</head>
<body>
  <header class="topbar">
    <div class="brand">
      JoyAgent 用量明细
      <span class="build-tag" title="构建版本 / build version">build __BUILD_TAG__</span>
    </div>
    <div class="topbar-meta" id="topbarMeta">加载中...</div>
  </header>

  <main class="wrap" id="mainWrap">
    <div class="card"><div class="empty">加载中...</div></div>
  </main>

  <script>
    const state = {
      months: [ym(new Date())],
      rows: [],
      info: null,
      monthInfo: [],
      resources: [],
      filterModel: "",
      filterTokenType: "",
      sortKey: "time-desc",
      pageNo: 1,
      pageSize: 50,
      timer: null,
      refreshInterval: 30000,
    };

    // Format Date -> "YYYY-MM" using local time. We must NOT use
    // `d.toISOString().slice(0, 7)` because toISOString converts to UTC,
    // which shifts a local "month-start" date back into the previous month
    // for any timezone east of UTC. That caused 上月 to query the wrong month.
    function ym(d) {
      const y = d.getFullYear();
      const m = d.getMonth() + 1;
      return y + "-" + (m < 10 ? "0" + m : "" + m);
    }

    // Friendly Chinese labels for the canonical token-type buckets the
    // pricing formula recognizes. Anything else falls through to the raw API name.
    const TOKEN_TYPE_LABELS = {
      input: "输入",
      output: "输出",
      cache_write_5m: "缓存写入 (5m)",
      cache_write_1h: "缓存写入 (1h)",
      cache_read: "缓存读取",
    };
    function tokenTypeLabel(canon, raw) {
      if (canon && TOKEN_TYPE_LABELS[canon]) return TOKEN_TYPE_LABELS[canon];
      return raw || canon || "-";
    }

    const $ = (id) => document.getElementById(id);
    const fmtMoney = (v, d = 2) => "¥" + Number(v || 0).toLocaleString("zh-CN", { minimumFractionDigits: d, maximumFractionDigits: d });
    const fmtInt = (v) => Number(v || 0).toLocaleString("zh-CN");
    const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

    /* ========================================================================
       Login screen
       ======================================================================== */
    function renderLoginScreen(reason, opts) {
      opts = opts || {};
      const tenants = opts.availableTenants || [];
      const isTenantMissing = !!opts.tenantMissing;
      $("topbarMeta").textContent = isTenantMissing ? "已登录 · 未进入企业空间" : "未登录";

      let body;
      if (isTenantMissing) {
        const tenantOptions = tenants.length
          ? '<select id="tenantPicker">' +
            tenants.map(t => `<option value="${esc(t.name || t.jdAccount || t.id || "")}">${esc(t.name || t.jdAccount || ("id=" + t.id))}</option>`).join("") +
            '</select>'
          : '<p class="login-note">当前账号没有可用的企业空间。请联系管理员把你加入企业。</p>';
        body = `
          <div class="login-icon" aria-hidden="true">⚙</div>
          <h1>切换到企业空间</h1>
          <p class="login-lead">JoyAgent 默认进入个人空间，需要切换到企业空间才能看到团队的 token 用量。</p>
          ${tenants.length ? `<div class="tenant-picker"><label>选择企业空间</label>${tenantOptions}</div>` : tenantOptions}
          <div class="login-actions">
            ${tenants.length ? '<button class="btn btn-primary big" id="startSwitch">切换</button>' : ''}
            <button class="btn" id="recheckLogin">我已切换，重新检查</button>
          </div>
          <div class="login-status" id="loginStatusBox" hidden></div>`;
      } else {
        // Combine the server reason and any recheck-timeout hint into one
        // visible warning so the user always sees feedback after clicking
        // "我已登录过，重新检查".
        const noteText =
          (_recheckHint || (reason && reason !== "session expired" && reason !== "未登录" ? reason : ""));
        const noteHtml = noteText ? `<p class="login-note">${esc(noteText)}</p>` : "";
        // Consume the one-shot hint so it doesn't stick around for unrelated
        // future renders.
        _recheckHint = "";
        body = `
          <div class="login-icon" aria-hidden="true">🔐</div>
          <h1>请登录 JoyAgent</h1>
          <p class="login-lead">登录后即可查看你所在企业的 token 用量明细。</p>
          ${noteHtml}
          <div class="login-actions">
            <button class="btn btn-primary big" id="startLogin">登录 JoyAgent</button>
            <button class="btn" id="recheckLogin">我已登录过，重新检查</button>
          </div>
          <p class="login-hint">点击后会弹出 JoyAgent 登录窗口；扫码或输入账号密码完成登录后窗口会自动关闭，本页会自动加载数据。</p>
          <div class="login-status" id="loginStatusBox" hidden></div>`;
      }
      $("mainWrap").innerHTML = `<div class="login-screen">${body}</div>`;

      $("recheckLogin").addEventListener("click", () => {
        // recheck flow: drop in-memory worker session first so the freshly
        // saved cookies (from a prior --login) take effect immediately.
        recheckAfterLogin();
      });

      const startBtn = $("startLogin");
      if (startBtn) startBtn.addEventListener("click", () => triggerLoginFlow("login"));

      const switchBtn = $("startSwitch");
      if (switchBtn) switchBtn.addEventListener("click", () => {
        const picked = ($("tenantPicker") || {}).value || "";
        triggerLoginFlow("switch-tenant", picked);
      });
    }

    // Module-level flag the next renderLoginScreen reads to surface a one-shot
    // warning when the user clicked "我已登录过" but no new login was found.
    let _recheckHint = "";

    async function recheckAfterLogin() {
      const box = $("loginStatusBox");
      const allBtns = document.querySelectorAll(".login-actions .btn");
      allBtns.forEach(b => { b.disabled = true; });
      if (box) {
        box.hidden = false;
        box.className = "login-status running";
        box.innerHTML = `
          <div class="login-status-title">正在重新检测登录态...</div>
          <div class="login-status-hint" id="recheckHint">正在重启浏览器引擎并重新读取登录 cookie。</div>`;
      }

      // Step 1: drop the in-process Playwright context so it re-reads the
      // freshly saved profile from disk. Server returns once the worker has
      // acknowledged the sentinel; the actual context recreation continues
      // in the background.
      try { await fetch("/api/restart-worker", { cache: "no-store" }); } catch (_) {}

      // Step 2: poll /api/userinfo every 1.5s. Successful detection -> hand
      // off to initialize() which will render the dashboard. Otherwise show
      // a clear timeout message instead of silently re-rendering the same
      // login screen (which used to look like nothing happened).
      const deadline = Date.now() + 30 * 1000;
      let lastInfo = null;
      while (Date.now() < deadline) {
        try {
          const u = await (await fetch("/api/userinfo", { cache: "no-store" })).json();
          lastInfo = u;
          if (u.logged_in) {
            if (box) {
              box.className = "login-status success";
              box.querySelector(".login-status-title").textContent = "已检测到登录，正在加载数据...";
              const hint = $("recheckHint");
              if (hint) hint.textContent = "";
            }
            _recheckHint = "";
            setTimeout(initialize, 400);
            return;
          }
        } catch (_) {
          // network blip or server still warming up; keep polling
        }
        const remaining = Math.max(0, Math.round((deadline - Date.now()) / 1000));
        const hint = $("recheckHint");
        if (hint) hint.textContent = `仍未检测到登录态，继续重试中（剩余 ${remaining}s）...`;
        await new Promise(r => setTimeout(r, 1500));
      }

      // Timed out. Surface a clear message so the user understands the
      // recheck actually ran. Set a hint that the next renderLoginScreen
      // will display, then re-enable the buttons.
      if (box) {
        box.className = "login-status failed";
        box.querySelector(".login-status-title").textContent = "未检测到新的登录态";
        const hint = $("recheckHint");
        if (hint) {
          hint.textContent = (lastInfo && lastInfo.error)
            ? `服务器返回：${lastInfo.error}。请确认 --login 已经成功完成，或直接点击「登录 JoyAgent」。`
            : "请确认登录已完成；如果反复失败，可以再点一次「登录 JoyAgent」。";
        }
      }
      _recheckHint = "未检测到新的登录态。请确认登录窗口里的扫码 / 输入流程已经完成。";
      allBtns.forEach(b => { b.disabled = false; });
    }

    async function triggerLoginFlow(mode, target) {
      const box = $("loginStatusBox");
      const allBtns = document.querySelectorAll(".login-actions .btn");
      allBtns.forEach(b => { b.disabled = true; });
      if (box) {
        box.hidden = false;
        box.className = "login-status running";
        box.innerHTML = `
          <div class="login-status-title">正在打开 JoyAgent 登录窗口...</div>
          <div class="login-status-hint">请在弹出的浏览器窗口中完成登录；登录成功后窗口将自动关闭。</div>
          <pre class="login-log" id="loginLog"></pre>`;
      }
      try {
        const r = await fetch("/api/login-start", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          cache: "no-store",
          body: JSON.stringify({ mode: mode, target: target || null }),
        });
        const j = await r.json();
        if (!j.ok) throw new Error(j.message || "无法启动登录");
        await pollLoginStatus(mode);
      } catch (err) {
        if (box) {
          box.className = "login-status failed";
          box.innerHTML = `<div class="login-status-title">登录启动失败</div>
            <div class="login-status-hint">${esc(err.message || String(err))}</div>`;
        }
        allBtns.forEach(b => { b.disabled = false; });
      }
    }

    async function pollLoginStatus(mode) {
      const box = $("loginStatusBox");
      const logEl = () => $("loginLog");
      const updateLog = (lines) => {
        const el = logEl();
        if (el) {
          el.textContent = (lines || []).slice(-12).join("\n");
          el.scrollTop = el.scrollHeight;
        }
      };
      // Poll every 1.5s until the subprocess is no longer "running".
      const deadline = Date.now() + 10 * 60 * 1000; // 10 minutes
      while (Date.now() < deadline) {
        await new Promise(r => setTimeout(r, 1500));
        let s;
        try {
          s = await (await fetch("/api/login-status", { cache: "no-store" })).json();
        } catch (_) { continue; }
        updateLog(s.log);
        if (s.state === "running") continue;
        if (s.state === "succeeded") {
          if (box) {
            box.className = "login-status success";
            box.querySelector(".login-status-title").textContent =
              mode === "switch-tenant" ? "切换成功，正在加载数据..." : "登录成功，正在加载数据...";
          }
          // Drop the in-process worker so it picks up the freshly saved
          // cookies, then fall back to the normal initialization flow.
          try { await fetch("/api/restart-worker", { cache: "no-store" }); } catch (_) {}
          setTimeout(initialize, 800);
          return;
        }
        if (box) {
          box.className = "login-status failed";
          box.querySelector(".login-status-title").textContent =
            mode === "switch-tenant" ? "切换失败" : "登录失败或被取消";
          box.insertAdjacentHTML("beforeend",
            `<div class="login-status-hint">退出码 ${s.exit_code}。可以再次点击「${mode === "switch-tenant" ? "切换" : "登录 JoyAgent"}」重试。</div>`);
        }
        document.querySelectorAll(".login-actions .btn").forEach(b => { b.disabled = false; });
        return;
      }
      if (box) {
        box.className = "login-status failed";
        box.querySelector(".login-status-title").textContent = "登录超时";
      }
      document.querySelectorAll(".login-actions .btn").forEach(b => { b.disabled = false; });
    }

    /* ========================================================================
       Main shell
       ======================================================================== */
    function renderShell() {
      $("mainWrap").innerHTML = `
        <div class="toolbar">
          <button class="btn" data-preset="current">本月</button>
          <button class="btn" data-preset="prev">上月</button>
          <button class="btn" data-preset="3">最近 3 个月</button>
          <button class="btn" data-preset="6">最近 6 个月</button>
          <span style="width:1px;height:20px;background:var(--line);margin:0 4px"></span>
          <div class="field">
            <label>追加月份</label>
            <input type="month" id="monthPicker" />
          </div>
          <button class="btn" id="addMonth">+ 追加</button>
          <span class="grow"></span>
          <button class="btn btn-primary" id="reload">立即刷新</button>
          <button class="btn" id="exportCsv">导出 CSV</button>
        </div>

        <div id="monthTags" style="margin-bottom:14px;display:flex;gap:6px;flex-wrap:wrap;align-items:center"></div>

        <div class="toolbar" style="margin-bottom:14px">
          <div class="field">
            <label>资源</label>
            <select id="filterModel"><option value="">全部资源</option></select>
          </div>
          <div class="field">
            <label>Token 类型</label>
            <select id="filterTokenType"><option value="">全部类型</option></select>
          </div>
          <div class="field">
            <label>排序</label>
            <select id="sortKey">
              <option value="time-desc" selected>调用时间 ↓</option>
              <option value="time-asc">调用时间 ↑</option>
              <option value="cost-desc">费用 ↓</option>
              <option value="tokens-desc">tokens ↓</option>
            </select>
          </div>
          <button class="btn clear-filter-btn" id="clearFilters">清空筛选</button>
          <span class="grow"></span>
          <div class="field">
            <label>自动刷新</label>
            <select id="refreshInterval">
              <option value="0">关闭</option>
              <option value="30000" selected>30s</option>
              <option value="60000">60s</option>
              <option value="300000">5min</option>
            </select>
          </div>
        </div>

        <div class="metric-grid">
          <div class="metric"><div class="metric-label">查询范围</div><div class="metric-value" id="mMonths">-</div><div class="metric-foot" id="mMonthsFoot">-</div></div>
          <div class="metric"><div class="metric-label">明细笔数</div><div class="metric-value" id="mRows">-</div><div class="metric-foot" id="mRowsFoot">-</div></div>
          <div class="metric"><div class="metric-label">总 tokens</div><div class="metric-value" id="mTokens">-</div><div class="metric-foot" id="mTokensFoot">-</div></div>
          <div class="metric"><div class="metric-label">费用</div><div class="metric-value" id="mCost">-</div><div class="metric-foot" id="mCostFoot">公开单价 × 汇率 7 · 每条向下抹零到分</div></div>
        </div>

        <div class="summary-grid">
          <div class="card">
            <h2>按模型汇总 <span class="sub">基于当前筛选</span></h2>
            <div id="byModelTable"></div>
          </div>
          <div class="card">
            <h2>按 Token 类型汇总 <span class="sub">输入/输出/缓存读/缓存写 5m/1h</span></h2>
            <div id="byTokenTypeTable"></div>
          </div>
        </div>

        <div class="card" style="margin-bottom:14px">
          <h2>近 30 天扣费走势 <span class="sub">按公开单价折算的费用 · 每条悬停显示金额</span></h2>
          <div id="trendChart"></div>
        </div>

        <div class="card" style="margin-bottom:14px">
          <h2>按天小计 <span class="sub">每天的 tokens 与费用</span></h2>
          <div id="byDayTable"></div>
        </div>

        <div class="card">
          <h2>用量明细 <span class="sub">每条记录附带按公开单价折算的费用</span></h2>
          <div id="detailTable"></div>
          <div class="pager">
            <span class="muted">每页</span>
            <select id="pageSize">
              <option>20</option><option selected>50</option><option>100</option><option>200</option>
            </select>
            <span class="muted">条</span>
            <span class="muted">共 <span id="totalRows">0</span> 条</span>
            <button class="btn" id="pagePrev" title="上一页">←</button>
            <span><span id="pageNo">1</span> / <span id="pageMax">1</span></span>
            <button class="btn" id="pageNext" title="下一页">→</button>
          </div>
        </div>

        <div class="card" id="monthInfoCard" style="display:none;margin-top:14px">
          <h2>本次拉取明细 <span class="sub">每月分页进度，仅供排查</span></h2>
          <div id="monthInfoTable"></div>
        </div>
      `;
      $("monthPicker").value = ym(new Date());
      $("addMonth").addEventListener("click", () => {
        const v = $("monthPicker").value;
        if (v && !state.months.includes(v)) {
          state.months = [...state.months, v].sort().reverse();
          loadData();
        }
      });
      document.querySelectorAll("[data-preset]").forEach(b => {
        b.addEventListener("click", () => applyPreset(b.dataset.preset));
      });
      $("filterModel").addEventListener("change", e => { state.filterModel = e.target.value; state.pageNo = 1; render(); });
      $("filterTokenType").addEventListener("change", e => { state.filterTokenType = e.target.value; state.pageNo = 1; render(); });
      $("sortKey").addEventListener("change", e => { state.sortKey = e.target.value; state.pageNo = 1; render(); });
      $("clearFilters").addEventListener("click", () => {
        state.filterModel = ""; state.filterTokenType = "";
        $("filterModel").value = ""; $("filterTokenType").value = "";
        state.pageNo = 1; render();
      });
      $("refreshInterval").addEventListener("change", e => { state.refreshInterval = Number(e.target.value); resetTimer(); });
      $("reload").addEventListener("click", loadData);
      $("exportCsv").addEventListener("click", exportCsv);
      $("pageSize").addEventListener("change", e => { state.pageSize = Number(e.target.value) || 50; state.pageNo = 1; render(); });
      $("pagePrev").addEventListener("click", () => { if (state.pageNo > 1) { state.pageNo -= 1; render(); } });
      $("pageNext").addEventListener("click", () => { state.pageNo += 1; render(); });
    }

    function applyPreset(name) {
      const today = new Date();
      const monthsBack = (n) => Array.from({length: n}, (_, i) =>
        ym(new Date(today.getFullYear(), today.getMonth() - i, 1))
      );
      if (name === "current") state.months = [ym(today)];
      else if (name === "prev") state.months = [ym(new Date(today.getFullYear(), today.getMonth() - 1, 1))];
      else state.months = monthsBack(Number(name));
      state.pageNo = 1;
      // Switching the months range invalidates filters that may not match the
      // new dataset. Clear them up-front so the user always sees the full
      // result of the new range, not a silent empty list.
      state.filterModel = "";
      state.filterTokenType = "";
      loadData();
    }

    function renderMonthTags() {
      const sorted = state.months.slice().sort();
      // Pre-compute per-month counts from already loaded rows so users get
      // immediate visual feedback on whether each tag actually returned data.
      const counts = new Map();
      state.rows.forEach(r => {
        const m = (r.call_time || "").slice(0, 7);
        if (m) counts.set(m, (counts.get(m) || 0) + 1);
      });
      const tags = sorted.map(m => {
        const c = counts.get(m) || 0;
        const cls = c === 0 ? "month-tag" : "month-tag";
        const cnt = `<span class="muted" style="font-size:11px;margin:0 4px">${c} 条</span>`;
        return `<span class="${cls}">${esc(m)} ${cnt}<button data-rm="${esc(m)}" title="移除">×</button></span>`;
      }).join("");
      const label = '<span class="muted" style="font-size:12px;margin-right:6px">已选月份:</span>';
      $("monthTags").innerHTML = sorted.length
        ? label + tags
        : '<span class="muted" style="font-size:12px">未选择月份</span>';
      $("monthTags").querySelectorAll("[data-rm]").forEach(b => {
        b.addEventListener("click", () => {
          state.months = state.months.filter(m => m !== b.dataset.rm);
          if (!state.months.length) state.months = [ym(new Date())];
          loadData();
        });
      });
    }

    /* ========================================================================
       Data flow
       ======================================================================== */
    async function loadData() {
      $("mRows").textContent = "加载中...";
      try {
        const url = "/api/usage?months=" + encodeURIComponent(state.months.join(","));
        const resp = await fetch(url, { cache: "no-store" });
        if (!resp.ok) throw new Error("HTTP " + resp.status);
        const data = await resp.json();
        if (!data.logged_in) {
          renderLoginScreen(data.error || "登录已过期", { availableTenants: data.available_tenants || [] });
          return;
        }
        if (data.userinfo && data.userinfo.tenant_missing) {
          renderLoginScreen("当前默认在个人空间，无法看到团队用量。", {
            tenantMissing: true,
            availableTenants: data.available_tenants || [],
          });
          return;
        }
        state.info = data.userinfo || null;
        state.rows = data.rows || [];
        state.monthInfo = data.month_info || [];
        state.resources = data.resources || [];
        $("topbarMeta").innerHTML = `欢迎 <strong>${esc(state.info?.realName || "-")}</strong> · 团队 ${esc(state.info?.tenantName || "-")} · 生成于 ${esc(data.generated_at || "")}`;
        populateFilters();
        renderMonthTags();
        render();
      } catch (err) {
        $("mainWrap").innerHTML = `<div class="card"><div class="empty">加载失败: ${esc(err.message)} <button class="btn btn-primary" id="retry" style="margin-top:14px">重试</button></div></div>`;
        $("retry").addEventListener("click", loadData);
      }
    }

    function populateFilters() {
      const fm = $("filterModel");
      const ft = $("filterTokenType");
      if (!fm || !ft) return;

      const models = Array.from(new Set(state.rows.map(r => r.model).filter(Boolean))).sort();
      fm.innerHTML = '<option value="">全部资源</option>' +
        models.map(m => `<option value="${esc(m)}">${esc(m)}</option>`).join("");
      // Validate the saved filter against the new option set; reset if stale,
      // otherwise the dropdown silently shows '全部资源' but the state still
      // filters by an old value, hiding all rows.
      if (state.filterModel && !models.includes(state.filterModel)) state.filterModel = "";
      fm.value = state.filterModel;

      const tokenTypes = Array.from(new Set(state.rows.map(r => r.token_type_raw).filter(Boolean))).sort();
      ft.innerHTML = '<option value="">全部类型</option>' + tokenTypes.map(t => {
        const row = state.rows.find(r => r.token_type_raw === t);
        const lbl = tokenTypeLabel(row && row.token_type, t);
        const display = lbl === t ? esc(t) : `${esc(lbl)} (${esc(t)})`;
        return `<option value="${esc(t)}">${display}</option>`;
      }).join("");
      if (state.filterTokenType && !tokenTypes.includes(state.filterTokenType)) state.filterTokenType = "";
      ft.value = state.filterTokenType;

      const sk = $("sortKey");
      if (sk) sk.value = state.sortKey;

      const cb = $("clearFilters");
      if (cb) cb.classList.toggle("active", !!(state.filterModel || state.filterTokenType));
    }

    function filteredRows() {
      return state.rows.filter(r => {
        if (state.filterModel && r.model !== state.filterModel) return false;
        if (state.filterTokenType && r.token_type_raw !== state.filterTokenType) return false;
        return true;
      });
    }

    function sortedRows(rows) {
      const arr = rows.slice();
      switch (state.sortKey) {
        case "time-asc":
          arr.sort((a, b) => String(a.call_time || "").localeCompare(String(b.call_time || ""))); break;
        case "cost-desc":
          arr.sort((a, b) => Number(b.cny_raw || 0) - Number(a.cny_raw || 0)); break;
        case "tokens-desc":
          arr.sort((a, b) => Number(b.tokens || 0) - Number(a.tokens || 0)); break;
        case "time-desc":
        default:
          arr.sort((a, b) => String(b.call_time || "").localeCompare(String(a.call_time || ""))); break;
      }
      return arr;
    }

    function render() {
      const rows = filteredRows();
      const tokens = rows.reduce((s, r) => s + Number(r.tokens || 0), 0);
      const costRaw = rows.reduce((s, r) => s + Number(r.cny_raw || 0), 0);
      const costFloor = rows.reduce((s, r) => s + Number(r.cny_floor || 0), 0);

      const sortedMonths = state.months.slice().sort();
      const monthsLabel = sortedMonths.length === 0 ? "-"
        : sortedMonths.length === 1 ? sortedMonths[0]
        : sortedMonths[0] + " ~ " + sortedMonths[sortedMonths.length - 1];

      const dayBuckets = new Map();
      rows.forEach(r => {
        const d = (r.call_time || "").slice(0, 10);
        if (!d) return;
        dayBuckets.set(d, (dayBuckets.get(d) || 0) + Number(r.cny_raw || 0));
      });
      const activeDays = dayBuckets.size;

      $("mMonths").textContent = monthsLabel;
      $("mMonthsFoot").textContent = `${state.months.length} 个月份 · ${activeDays} 天有调用`;
      $("mRows").textContent = fmtInt(rows.length);
      $("mRowsFoot").textContent = (state.filterModel || state.filterTokenType)
        ? `已筛选自 ${fmtInt(state.rows.length)} 条原始记录` : `全部 ${fmtInt(state.rows.length)} 条`;
      $("mTokens").textContent = fmtInt(tokens);
      $("mTokensFoot").textContent = "金额 ≈ tokens × 单价 × 7";
      $("mCost").textContent = fmtMoney(costFloor);
      $("mCostFoot").textContent = `未抹零 ${fmtMoney(costRaw, 4)} · 京东云实际扣费按其计费表为准`;

      const cb = $("clearFilters");
      if (cb) cb.classList.toggle("active", !!(state.filterModel || state.filterTokenType));

      renderByModel(rows);
      renderByTokenType(rows);
      renderTrend(rows);
      renderByDay(rows);
      renderDetail(rows);
      renderMonthInfo();
    }

    function table(headers, body, opts) {
      opts = opts || {};
      const cls = opts.className ? ` class="${esc(opts.className)}"` : "";
      const head = '<thead><tr>' + headers.map(h => `<th class="${h.num ? "num" : ""}">${esc(h.label)}</th>`).join("") + '</tr></thead>';
      const empty = opts.emptyHTML || `<tr><td colspan="${headers.length}" class="empty">暂无数据</td></tr>`;
      const rows = body.length ? body.join("") : empty;
      return `<div class="table-wrap"><table${cls}>${head}<tbody>${rows}</tbody></table></div>`;
    }

    function emptyMessage(rows) {
      if (rows.length) return null;
      if (state.filterModel || state.filterTokenType) {
        return `<div class="empty-with-action">
          <div class="why">当前筛选条件下没有匹配记录</div>
          <button class="btn btn-primary" onclick="document.getElementById('clearFilters').click()">清空筛选</button>
        </div>`;
      }
      return `<div class="empty-with-action">
        <div>所选月份内没有任何调用记录</div>
        <div style="margin-top:6px;font-size:12px">月份: ${esc(state.months.join(", ") || "-")}</div>
      </div>`;
    }

    function renderByModel(rows) {
      const map = new Map();
      rows.forEach(r => {
        const k = r.model || "-";
        const cur = map.get(k) || {model: k, tokens: 0, raw: 0, floor: 0, calls: 0};
        cur.tokens += Number(r.tokens || 0);
        cur.raw += Number(r.cny_raw || 0);
        cur.floor += Number(r.cny_floor || 0);
        cur.calls += 1;
        map.set(k, cur);
      });
      const list = Array.from(map.values()).sort((a, b) => b.floor - a.floor);
      const totalCost = list.reduce((s, r) => s + r.floor, 0) || 1;
      const body = list.map(r => {
        const pct = (r.floor / totalCost) * 100;
        return `<tr>
          <td><div style="font-weight:500">${esc(r.model)}</div>
              <div class="row-bar" style="margin-top:6px">
                <div class="bar-track"><div class="bar-fill" style="width:${pct.toFixed(1)}%"></div></div>
                <span class="bar-pct">${pct.toFixed(1)}%</span>
              </div>
          </td>
          <td class="num">${fmtInt(r.calls)}</td>
          <td class="num">${fmtInt(r.tokens)}</td>
          <td class="num">${fmtMoney(r.floor)}</td>
        </tr>`;
      });
      const empty = emptyMessage(rows);
      $("byModelTable").innerHTML = table(
        [{label:"模型 (按费用占比)"}, {label:"调用", num:true}, {label:"tokens", num:true}, {label:"费用", num:true}],
        body,
        empty ? { emptyHTML: `<tr><td colspan="4">${empty}</td></tr>`, className: "model-table" } : { className: "model-table" }
      );
    }

    function renderByTokenType(rows) {
      const map = new Map();
      rows.forEach(r => {
        const canon = r.token_type || r.token_type_raw || "-";
        const cur = map.get(canon) || { canon, tokens: 0, raw_amt: 0, floor: 0, rawSet: new Set() };
        cur.tokens += Number(r.tokens || 0);
        cur.raw_amt += Number(r.cny_raw || 0);
        cur.floor += Number(r.cny_floor || 0);
        if (r.token_type_raw) cur.rawSet.add(r.token_type_raw);
        map.set(canon, cur);
      });
      const list = Array.from(map.values()).sort((a, b) => b.tokens - a.tokens);
      const totalTokens = list.reduce((s, r) => s + r.tokens, 0) || 1;
      const empty = emptyMessage(rows);
      if (!list.length) {
        $("byTokenTypeTable").innerHTML = empty || '<div class="empty">暂无数据</div>';
        return;
      }
      // Sanitize the canonical key for use as a CSS class suffix; anything
      // unknown collapses to a neutral dot color.
      const dotClassFor = (canon) => "dot-" + (canon || "other").toString().replace(/[^a-z0-9_-]/gi, "");
      const html = list.map(r => {
        const firstRaw = r.rawSet.values().next().value;
        const lbl = tokenTypeLabel(r.canon, firstRaw);
        const pct = (r.tokens / totalTokens) * 100;
        const rawList = Array.from(r.rawSet);
        // Tooltip shows the raw API type names (helpful for debugging without
        // cluttering the visual layout with secondary text on every row).
        const tip = rawList.length ? `${lbl} · ${rawList.join(", ")}` : lbl;
        return `
          <div class="ttype-row" title="${esc(tip)}">
            <div class="ttype-label">
              <span class="ttype-dot ${dotClassFor(r.canon)}"></span>
              <span class="ttype-name">${esc(lbl)}</span>
            </div>
            <div class="ttype-bar"><div class="ttype-fill" style="width:${pct.toFixed(1)}%"></div></div>
            <div class="ttype-meta">
              <div class="ttype-pct">${pct.toFixed(1)}%</div>
              <div class="ttype-amt"><strong>${fmtInt(r.tokens)}</strong> tokens · ${fmtMoney(r.floor)}</div>
            </div>
          </div>`;
      }).join("");
      $("byTokenTypeTable").innerHTML = `<div class="ttype-list">${html}</div>`;
    }

    function renderTrend(rows) {
      // Build the last 30 days bucket from the latest call_time we have, so
      // when looking at a past month the chart still focuses on the data we
      // actually fetched instead of always showing "today minus 30".
      const dayMap = new Map();
      rows.forEach(r => {
        const d = (r.call_time || "").slice(0, 10);
        if (!d) return;
        dayMap.set(d, (dayMap.get(d) || 0) + Number(r.cny_raw || 0));
      });
      if (!dayMap.size) {
        $("trendChart").innerHTML = `<div class="empty" style="padding:20px">所选范围无消费记录</div>`;
        return;
      }
      const days = Array.from(dayMap.keys()).sort();
      const last = days[days.length - 1];
      const lastDate = new Date(last + "T00:00:00");
      const window = [];
      for (let i = 29; i >= 0; i--) {
        const d = new Date(lastDate.getFullYear(), lastDate.getMonth(), lastDate.getDate() - i);
        const k = d.toISOString().slice(0, 10);
        window.push({ d: k, v: dayMap.get(k) || 0 });
      }
      const max = Math.max(...window.map(w => w.v), 0.0001);
      const bars = window.map(w => {
        const h = (w.v / max) * 100;
        const tip = `${w.d}\\n费用 ${fmtMoney(w.v, 4)}`;
        return `<div class="trend-bar" title="${esc(tip)}"><div class="fill" style="height:${h.toFixed(2)}%"></div></div>`;
      }).join("");
      const total = window.reduce((s, w) => s + w.v, 0);
      $("trendChart").innerHTML = `
        <div class="trend-chart">${bars}</div>
        <div class="trend-axis"><span>${esc(window[0].d)}</span><span>${esc(window[window.length - 1].d)}</span></div>
        <div class="trend-foot">窗口合计 (raw): ${fmtMoney(total, 4)} · 单日峰值: ${fmtMoney(max, 4)}</div>
      `;
    }

    function renderByDay(rows) {
      const map = new Map();
      rows.forEach(r => {
        const d = (r.call_time || "").slice(0, 10) || "-";
        const cur = map.get(d) || {date: d, tokens: 0, raw: 0, floor: 0, calls: 0, models: new Set()};
        cur.tokens += Number(r.tokens || 0);
        cur.raw += Number(r.cny_raw || 0);
        cur.floor += Number(r.cny_floor || 0);
        cur.calls += 1;
        if (r.model) cur.models.add(r.model);
        map.set(d, cur);
      });
      const list = Array.from(map.values()).sort((a, b) => b.date.localeCompare(a.date));
      const body = list.map(r => `<tr>
        <td>${esc(r.date)}</td>
        <td class="num">${fmtInt(r.calls)}</td>
        <td>${Array.from(r.models).map(m => `<span class="pill pill-info" style="margin-right:4px">${esc(m)}</span>`).join("")}</td>
        <td class="num">${fmtInt(r.tokens)}</td>
        <td class="num">${fmtMoney(r.raw, 4)}</td>
        <td class="num">${fmtMoney(r.floor)}</td>
      </tr>`);
      const empty = emptyMessage(rows);
      $("byDayTable").innerHTML = table(
        [{label:"日期"}, {label:"调用", num:true}, {label:"涉及模型"}, {label:"tokens", num:true}, {label:"raw 费用", num:true}, {label:"费用", num:true}],
        body,
        empty ? { emptyHTML: `<tr><td colspan="6">${empty}</td></tr>` } : undefined
      );
    }

    function renderDetail(rows) {
      const sorted = sortedRows(rows);
      const total = sorted.length;
      const maxPage = Math.max(1, Math.ceil(total / state.pageSize));
      if (state.pageNo > maxPage) state.pageNo = maxPage;
      const start = (state.pageNo - 1) * state.pageSize;
      const slice = sorted.slice(start, start + state.pageSize);

      const body = slice.map(r => {
        const lbl = tokenTypeLabel(r.token_type, r.token_type_raw);
        const showRaw = r.token_type_raw && r.token_type_raw !== lbl
          ? `<div class="muted" style="font-size:11px">${esc(r.token_type_raw)}</div>` : "";
        return `<tr>
          <td style="white-space:nowrap">${esc(r.call_time || "-")}</td>
          <td>${esc(r.model || "-")}</td>
          <td><span class="token-type-pill">${esc(lbl)}</span>${showRaw}</td>
          <td class="num">${fmtInt(r.tokens)}</td>
          <td class="num">${fmtMoney(r.cny_raw, 4)}</td>
          <td class="num">${fmtMoney(r.cny_floor)}</td>
        </tr>`;
      });
      const empty = emptyMessage(rows);
      $("detailTable").innerHTML = table([
        {label:"调用时间"}, {label:"模型"}, {label:"Token 类型"},
        {label:"使用量 (tokens)", num:true},
        {label:"raw 费用", num:true}, {label:"费用", num:true}
      ], body, empty ? { emptyHTML: `<tr><td colspan="6">${empty}</td></tr>` } : undefined);

      $("totalRows").textContent = fmtInt(total);
      $("pageNo").textContent = state.pageNo;
      $("pageMax").textContent = maxPage;
      $("pagePrev").disabled = state.pageNo <= 1;
      $("pageNext").disabled = state.pageNo >= maxPage;
    }

    function renderMonthInfo() {
      if (!(state.monthInfo && state.monthInfo.length)) {
        $("monthInfoCard").style.display = "none";
        return;
      }
      const hasErr = state.monthInfo.some(m => m.error);
      $("monthInfoCard").style.display = hasErr ? "block" : "none";
      const body = state.monthInfo.map(m => `<tr>
        <td>${esc(m.month)}</td>
        <td class="num">${m.total == null ? "-" : fmtInt(m.total)}</td>
        <td class="num">${fmtInt(m.fetched)}</td>
        <td>${m.error ? `<span class="pill pill-warn">${esc(m.error)}</span>` : `<span class="pill pill-info">OK</span>`}</td>
      </tr>`);
      $("monthInfoTable").innerHTML = table(
        [{label:"月份"}, {label:"接口 total", num:true}, {label:"已拉取", num:true}, {label:"状态"}],
        body
      );
    }

    function exportCsv() {
      const rows = sortedRows(filteredRows());
      const head = ["调用时间","账户(PIN)","用户名","模型","Token类型(原始)","Token类型(归一)","使用量(Tokens)","费用raw","费用floor"];
      const csv = [head].concat(rows.map(r => [
        r.call_time || "", r.account_pin || "", r.username || "",
        r.model || "",
        r.token_type_raw || "",
        tokenTypeLabel(r.token_type, r.token_type_raw),
        String(r.tokens || 0), String(r.cny_raw || 0), String(r.cny_floor || 0),
      ])).map(cols => cols.map(v => '"' + String(v).replaceAll('"', '""') + '"').join(",")).join("\n");
      const blob = new Blob(["\ufeff" + csv], { type: "text/csv;charset=utf-8" });
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      const sorted = state.months.slice().sort();
      a.download = "joyagent_usage_" + sorted.join("_") + ".csv";
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
      URL.revokeObjectURL(a.href);
    }

    function resetTimer() {
      if (state.timer) clearInterval(state.timer);
      if (state.refreshInterval > 0) {
        state.timer = setInterval(loadData, state.refreshInterval);
      }
    }

    /* ========================================================================
       Bootstrap
       ======================================================================== */
    async function initialize() {
      try {
        const u = await (await fetch("/api/userinfo", { cache: "no-store" })).json();
        if (!u.logged_in) {
          renderLoginScreen(u.error || "未登录", { availableTenants: u.available_tenants || [] });
          return;
        }
        if (u.tenant_missing) {
          renderLoginScreen("当前默认在个人空间，无法看到团队用量。", {
            tenantMissing: true,
            availableTenants: u.available_tenants || [],
          });
          return;
        }
        renderShell();
        renderMonthTags();
        await loadData();
        resetTimer();
      } catch (err) {
        renderLoginScreen("无法连接到本地服务: " + err.message);
      }
    }

    initialize();
  </script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def run_server(host: str, port: int, open_browser: bool) -> None:
    server = ThreadingHTTPServer((host, port), ClientHandler)
    url = f"http://{host}:{port}/"
    print("=" * 60)
    print(f"Customer dashboard:  {url}")
    print(f"Build:               {BUILD_TAG}")
    print(f"Profile directory:   {USER_DATA_DIR}")
    if not USER_DATA_DIR.exists() or not any(USER_DATA_DIR.iterdir()):
        print()
        print("WARNING: login profile is empty.")
        print("First-time setup: run this in another shell, then refresh the page:")
        print("    python client_dashboard.py --login")
    print("=" * 60)
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="JoyAgent customer-facing usage dashboard")
    parser.add_argument("--login", action="store_true", help="One-off login flow; persists cookies into ./joyagent_profile")
    parser.add_argument(
        "--switch-tenant",
        nargs="?",
        const="",
        default=None,
        metavar="NAME_OR_ID",
        help=(
            "Click the avatar -> '切换空间' and enter the named enterprise tenant. "
            "Pass without value to auto-pick when only one is available, or pass "
            "the tenant name / jdAccount / id to force a specific one."
        ),
    )
    parser.add_argument("--show-browser", action="store_true", help="Show the browser window during --switch-tenant")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--no-open", action="store_true", help="Don't auto-open the browser")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Wipe local cached state (login profile). Customer dashboard does NOT keep a billing DB; this only removes ./joyagent_profile.",
    )
    args = parser.parse_args()

    if args.reset:
        print(f"Build: {BUILD_TAG}")
        print(f"Resetting local state under: {WORKSPACE_DIR}")
        for note in reset_local_state():
            print(f"  - {note}")
        print("Done. Run `python client_dashboard.py --login` to sign in again.")
        return

    if args.login:
        login_only(target_tenant=(args.switch_tenant or None))
        return

    if args.switch_tenant is not None:
        target = args.switch_tenant or None
        switch_tenant_cli(target=target, headless=not args.show_browser)
        return

    try:
        run_server(args.host, args.port, open_browser=not args.no_open)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
