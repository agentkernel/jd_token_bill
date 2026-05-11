from __future__ import annotations

import argparse
import json
import queue
import sqlite3
import threading
import time
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import joyagent_monitor as monitor


APP_DIR = Path(__file__).resolve().parent
DB_PATH = monitor.DB_PATH
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
DEFAULT_INTERVAL = 10

STATE_LOCK = threading.Lock()
APP_STATE = {
    "started_at": None,
    "last_poll_at": None,
    "last_poll_ok": False,
    "last_poll_error": None,
    "poll_count": 0,
    "months": [],
    "polling_enabled": False,
}

# Playwright's sync API is bound to the greenlet that started it. To avoid
# `greenlet.error: cannot switch to a different thread` we confine ALL browser
# operations to a single dedicated worker thread. Other threads (HTTP request
# handlers + the polling loop) submit callables via `with_remote_page` and
# block on a per-task event for the result.
_BROWSER_QUEUE: "queue.Queue[tuple]" = queue.Queue()
_BROWSER_THREAD: threading.Thread | None = None
_BROWSER_THREAD_LOCK = threading.Lock()
_BROWSER_INIT_ERROR: str | None = None


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def money(value: float | int | None) -> str:
    if value is None:
        return "¥0.00"
    return f"¥{float(value):,.2f}"


def round4(value: float | int | None) -> str:
    if value is None:
        return "¥0.0000"
    return f"¥{float(value):,.4f}"


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def fetch_all(conn: sqlite3.Connection, sql: str, args: tuple = ()) -> list[dict]:
    return [dict(row) for row in conn.execute(sql, args).fetchall()]


def fetch_one(conn: sqlite3.Connection, sql: str, args: tuple = ()) -> dict | None:
    row = conn.execute(sql, args).fetchone()
    return None if row is None else dict(row)


def get_state() -> dict:
    with STATE_LOCK:
        return dict(APP_STATE)


def update_state(**kwargs) -> None:
    with STATE_LOCK:
        APP_STATE.update(kwargs)


def _browser_worker_loop() -> None:
    """Own a single Playwright session and run submitted jobs on its page."""
    global _BROWSER_INIT_ERROR
    try:
        pw = monitor.sync_playwright().start()
    except Exception as exc:
        _BROWSER_INIT_ERROR = f"playwright start failed: {exc}"
        # Drain pending jobs with the init error so callers don't hang forever.
        while True:
            job, holder = _BROWSER_QUEUE.get()
            if job is None:
                return
            holder["error"] = RuntimeError(_BROWSER_INIT_ERROR)
            holder["done"].set()

    try:
        context = monitor._new_context(pw, headless=True)
        page = context.new_page()
        try:
            page.goto(monitor.PROFILE_URL, wait_until="domcontentloaded", timeout=60_000)
        except Exception as exc:
            print(f"  browser worker: initial goto failed: {exc}")

        while True:
            job, holder = _BROWSER_QUEUE.get()
            if job is None:
                return
            try:
                holder["result"] = job(page)
            except Exception as exc:
                holder["error"] = exc
            finally:
                holder["done"].set()
    finally:
        try:
            pw.stop()
        except Exception:
            pass


def _ensure_browser_worker() -> None:
    global _BROWSER_THREAD
    with _BROWSER_THREAD_LOCK:
        if _BROWSER_THREAD is not None and _BROWSER_THREAD.is_alive():
            return
        t = threading.Thread(target=_browser_worker_loop, name="joyagent-browser", daemon=True)
        t.start()
        _BROWSER_THREAD = t


def with_remote_page(job, timeout: float = 90.0):
    """Run `job(page)` on the dedicated browser thread and return its result."""
    _ensure_browser_worker()
    holder: dict = {"done": threading.Event(), "result": None, "error": None}
    _BROWSER_QUEUE.put((job, holder))
    if not holder["done"].wait(timeout=timeout):
        raise TimeoutError(f"Browser job timed out after {timeout:.1f}s")
    if holder["error"] is not None:
        raise holder["error"]
    return holder["result"]


def import_default_bills() -> None:
    conn = monitor.open_db()
    patterns = [
        str(APP_DIR.parent / "*.csv"),
        str(APP_DIR / "bills_paste_*.tsv"),
    ]
    seen: set[Path] = set()
    for pattern in patterns:
        for path in sorted(Path().glob(pattern) if not any(ch in pattern for ch in "*?[]") else []):
            full = path.resolve()
            if full in seen:
                continue
            seen.add(full)
            monitor.import_bills_auto(conn, full)


def import_default_bills_glob() -> None:
    import glob

    conn = monitor.open_db()
    seen: set[Path] = set()
    for pattern in (str(APP_DIR.parent / "*.csv"), str(APP_DIR / "bills_paste_*.tsv")):
        for raw in glob.glob(pattern):
            full = Path(raw).resolve()
            if full in seen or not full.exists():
                continue
            seen.add(full)
            monitor.import_bills_auto(conn, full)


def normalize_months(months: list[str] | None) -> list[str]:
    if months:
        return months
    return [datetime.now().strftime("%Y-%m")]


def poll_loop(months: list[str], interval: int) -> None:
    update_state(polling_enabled=True, months=months)
    while True:
        now = datetime.now().isoformat(timespec="seconds")
        try:
            userinfo = with_remote_page(monitor.fetch_userinfo)
            if not userinfo or not userinfo.get("userId"):
                update_state(
                    last_poll_at=now,
                    last_poll_ok=False,
                    last_poll_error="Not logged in. Run: python joyagent_monitor.py --login",
                )
            else:
                def _job(page, _months=months, _ts=now):
                    conn = monitor.open_db()
                    try:
                        monitor._poll_once(conn, page, _months, _ts)
                    finally:
                        conn.close()

                with_remote_page(_job)
                state = get_state()
                update_state(
                    last_poll_at=datetime.now().isoformat(timespec="seconds"),
                    last_poll_ok=True,
                    last_poll_error=None,
                    poll_count=int(state.get("poll_count") or 0) + 1,
                )
        except Exception as exc:
            update_state(
                last_poll_at=datetime.now().isoformat(timespec="seconds"),
                last_poll_ok=False,
                last_poll_error=str(exc),
            )
        time.sleep(interval)


def build_dashboard_payload() -> dict:
    monitor.open_db().close()
    conn = connect_db()
    try:
        balance = fetch_one(
            conn,
            """
            SELECT captured_at, balance_cny, delta_cny, cash_cny, voucher_cny, credits
              FROM balance_snapshots
             ORDER BY id DESC
             LIMIT 1
            """,
        )
        usage_total = fetch_one(
            conn,
            """
            SELECT COUNT(*) AS rows,
                   COALESCE(SUM(tokens), 0) AS tokens,
                   COALESCE(SUM(cny_amount), 0) AS cny_raw,
                   COALESCE(SUM(cny_amount_floor), 0) AS cny_floor
              FROM usage_records
            """,
        )
        usage_by_model = fetch_all(
            conn,
            """
            SELECT model,
                   COALESCE(token_type_canonical, token_type_raw) AS token_type,
                   SUM(tokens) AS tokens,
                   SUM(cny_amount) AS cny_raw,
                   SUM(cny_amount_floor) AS cny_floor
              FROM usage_records
             GROUP BY model, token_type
             ORDER BY cny_raw DESC
            """,
        )
        users = fetch_all(
            conn,
            """
            SELECT account_pin,
                   username,
                   SUM(tokens) AS tokens,
                   SUM(cny_amount) AS cny_raw,
                   SUM(cny_amount_floor) AS cny_floor
              FROM usage_records
             GROUP BY account_pin, username
             ORDER BY cny_raw DESC
            """,
        )
        user_model = fetch_all(
            conn,
            """
            SELECT account_pin,
                   username,
                   model,
                   SUM(tokens) AS tokens,
                   SUM(cny_amount) AS cny_raw,
                   SUM(cny_amount_floor) AS cny_floor
              FROM usage_records
             GROUP BY account_pin, username, model
             ORDER BY account_pin, username, cny_raw DESC
            """,
        )
        user_daily = fetch_all(
            conn,
            """
            SELECT account_pin,
                   username,
                   SUBSTR(call_time, 1, 10) AS day,
                   model,
                   SUM(tokens) AS tokens,
                   SUM(cny_amount) AS cny_raw,
                   SUM(cny_amount_floor) AS cny_floor
              FROM usage_records
             WHERE call_time IS NOT NULL AND call_time != ''
             GROUP BY account_pin, username, day, model
             ORDER BY day DESC, account_pin, username, cny_raw DESC
            """,
        )
        latest_usage = fetch_all(
            conn,
            """
            SELECT call_time, account_pin, username, model, token_type_raw,
                   tokens, cny_amount, cny_amount_floor
              FROM usage_records
             ORDER BY call_time DESC, model, token_type_raw
             LIMIT 300
            """,
        )

        bill_total = {"rows": 0, "amount": 0}
        bills_daily: list[dict] = []
        bills_by_month: list[dict] = []
        bill_records: list[dict] = []
        if table_exists(conn, "historical_bills"):
            bill_total = fetch_one(
                conn,
                """
                SELECT COUNT(*) AS rows, COALESCE(SUM(cny_amount), 0) AS amount
                  FROM historical_bills
                """,
            ) or bill_total
            bills_daily = fetch_all(
                conn,
                """
                SELECT charge_date AS day,
                       COALESCE(resource, '未知模型') AS resource,
                       COUNT(*) AS rows,
                       SUM(cny_amount) AS amount
                  FROM historical_bills
                 GROUP BY charge_date, resource
                 ORDER BY charge_date DESC, amount DESC
                """,
            )
            bills_by_month = fetch_all(
                conn,
                """
                SELECT SUBSTR(charge_date, 1, 7) AS month,
                       COUNT(*) AS rows,
                       SUM(cny_amount) AS amount
                  FROM historical_bills
                 GROUP BY month
                 ORDER BY month DESC
                """,
            )
            bill_records = fetch_all(
                conn,
                """
                SELECT charge_time,
                       charge_date AS day,
                       resource_type,
                       COALESCE(resource, '未知模型') AS resource,
                       cny_amount AS amount,
                       source
                  FROM historical_bills
                 ORDER BY charge_time DESC
                 LIMIT 1000
                """,
            )

        jdcloud_total = {"rows": 0, "actual_fee": 0, "bill_fee": 0, "erase_fee": 0}
        jdcloud_by_month: list[dict] = []
        jdcloud_by_resource: list[dict] = []
        jdcloud_by_day: list[dict] = []
        jdcloud_records: list[dict] = []
        if table_exists(conn, "jdcloud_bills"):
            jdcloud_total = fetch_one(
                conn,
                """
                SELECT COUNT(*) AS rows,
                       COALESCE(SUM(actual_fee), 0) AS actual_fee,
                       COALESCE(SUM(bill_fee), 0) AS bill_fee,
                       COALESCE(SUM(erase_fee), 0) AS erase_fee
                  FROM jdcloud_bills
                """,
            ) or jdcloud_total
            jdcloud_by_month = fetch_all(
                conn,
                """
                SELECT bill_date AS month,
                       COUNT(*) AS rows,
                       SUM(bill_fee) AS bill_fee,
                       SUM(actual_fee) AS actual_fee,
                       SUM(erase_fee) AS erase_fee
                  FROM jdcloud_bills
                 GROUP BY bill_date
                 ORDER BY bill_date DESC
                """,
            )
            jdcloud_by_resource = fetch_all(
                conn,
                """
                SELECT COALESCE(service_code_name, app_code_name, '-') AS resource,
                       COALESCE(service_code, app_code) AS service_code,
                       COUNT(*) AS rows,
                       SUM(actual_fee) AS actual_fee
                  FROM jdcloud_bills
                 GROUP BY resource, service_code
                 ORDER BY actual_fee DESC
                """,
            )
            jdcloud_by_day = fetch_all(
                conn,
                """
                SELECT SUBSTR(bill_time, 1, 10) AS day,
                       COUNT(*) AS rows,
                       SUM(actual_fee) AS actual_fee
                  FROM jdcloud_bills
                 WHERE bill_time IS NOT NULL AND bill_time != ''
                 GROUP BY day
                 ORDER BY day DESC
                """,
            )
            jdcloud_records = fetch_all(
                conn,
                """
                SELECT bill_id, bill_date, bill_time, time_range,
                       resource_id, resource_name, service_code_name,
                       billing_type_name, region_name,
                       bill_fee, actual_fee, cash_coupon_fee, erase_fee
                  FROM jdcloud_bills
                 ORDER BY bill_time DESC
                 LIMIT 500
                """,
            )

        return {
            "state": get_state(),
            "balance": balance,
            "usage_total": usage_total,
            "usage_by_model": usage_by_model,
            "users": users,
            "user_model": user_model,
            "user_daily": user_daily,
            "latest_usage": latest_usage,
            "bill_total": bill_total,
            "bills_daily": bills_daily,
            "bills_by_month": bills_by_month,
            "bill_records": bill_records,
            "jdcloud_total": jdcloud_total,
            "jdcloud_by_month": jdcloud_by_month,
            "jdcloud_by_resource": jdcloud_by_resource,
            "jdcloud_by_day": jdcloud_by_day,
            "jdcloud_records": jdcloud_records,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }
    finally:
        conn.close()


def build_profile_payload(query: dict[str, list[str]]) -> dict:
    month = (query.get("month") or [datetime.now().strftime("%Y-%m")])[0]
    page_no = int((query.get("pageNo") or ["1"])[0] or "1")
    page_size = int((query.get("pageSize") or ["10"])[0] or "10")
    resource_filter = (query.get("resource") or [""])[0]

    def _job(page, _month=month):
        info = monitor.fetch_userinfo(page)
        amt = monitor.fetch_balance_amounts(page)
        rj = monitor._page_fetch(
            page,
            "https://agentrs.jd.com/api/saas/tenant-resource/v1/model-by-tenant?pageNo=1&pageSize=100",
        )
        res_list: list[dict] = []
        if rj and rj.get("code") == 0:
            res_list = (rj.get("data") or {}).get("list") or []
        billing_rows = monitor.fetch_all_billing_rows(page, _month, page_size=100)
        return info, amt, res_list, billing_rows

    fetch_error: str | None = None
    try:
        userinfo, amount, resources, rows = with_remote_page(_job)
    except Exception as exc:
        fetch_error = str(exc)
        userinfo = None
        amount = None
        resources = []
        rows = []

    if resource_filter:
        rows = [r for r in rows if str(r.get("resourceName") or "") == resource_filter]
    total = len(rows)
    start = max(0, (page_no - 1) * page_size)
    end = start + page_size
    billing_data = {
        "list": rows[start:end],
        "total": total,
        "pageNo": page_no,
        "pageSize": page_size,
        "dtMonth": month,
        "resource": resource_filter,
    }

    return {
        "userinfo": userinfo,
        "amount": {
            "total": str(amount.get("total", "0")) if amount else "0",
            "cash": str(amount.get("cash", "0")) if amount else "0",
            "voucher": str(amount.get("voucher", "0")) if amount else "0",
            "credits": amount.get("credits", 0) if amount else 0,
            "arrear": str(amount.get("arrear", "0")) if amount else "0",
        },
        "resources": [
            {
                "label": r.get("label") or r.get("resourceName") or r.get("modelName"),
                "modelId": r.get("modelId"),
                "description": r.get("description"),
            }
            for r in resources
            if (r.get("label") or r.get("resourceName") or r.get("modelName"))
        ],
        "billing": billing_data,
        "fetch_error": fetch_error,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


TEAM_API = "https://agentrs.jd.com/api/saas/tenant-member/v1/page?pageNo=1&pageSize=100"
RESOURCES_API = "https://agentrs.jd.com/api/saas/tenant-resource/v1/model-by-tenant?pageNo=1&pageSize=100"
TENANT_LIST_API = "https://agentrs.jd.com/api/saas/tenant/v1/list-by-user"

# Real /pl/profile?tab=resource page exposes three sub-tabs: 模型/插件/MCP.
# We know the model URL above. Plugin/MCP endpoints are probed because the
# JoyAgent web bundle loads them lazily and we can't see the exact path until
# we have a live session that opens those tabs. Add more candidates here when
# new ones turn up in the captured XHR log.
PLUGIN_API_CANDIDATES = [
    "https://agentrs.jd.com/api/saas/tenant-resource/v1/plugin-by-tenant?pageNo=1&pageSize=100",
    "https://agentrs.jd.com/api/saas/tenant-resource/v1/tool-by-tenant?pageNo=1&pageSize=100",
    "https://agentrs.jd.com/api/saas/tenant-plugin/v1/page?pageNo=1&pageSize=100",
    "https://agentrs.jd.com/api/saas/tenant-tool/v1/page?pageNo=1&pageSize=100",
    "https://agentrs.jd.com/api/saas/plugin/v1/page?pageNo=1&pageSize=100",
]
MCP_API_CANDIDATES = [
    "https://agentrs.jd.com/api/saas/tenant-resource/v1/mcp-by-tenant?pageNo=1&pageSize=100",
    "https://agentrs.jd.com/api/saas/tenant-mcp/v1/page?pageNo=1&pageSize=100",
    "https://agentrs.jd.com/api/saas/mcp/v1/page?pageNo=1&pageSize=100",
    "https://agentrs.jd.com/api/saas/mcp-server/v1/page?pageNo=1&pageSize=100",
]
API_KEY_CANDIDATES = [
    "https://agentrs.jd.com/api/saas/api-key/v1/list",
    "https://agentrs.jd.com/api/saas/api-key/v1/page?pageNo=1&pageSize=100",
    "https://agentrs.jd.com/api/saas/apikey/v1/page?pageNo=1&pageSize=100",
    "https://agentrs.jd.com/api/saas/apikey/v1/list",
    "https://agentrs.jd.com/api/saas/tenant-api-key/v1/page?pageNo=1&pageSize=100",
    "https://agentrs.jd.com/api/saas/tenant/v1/api-key/page?pageNo=1&pageSize=100",
    "https://agentrs.jd.com/api/saas/access-key/v1/page?pageNo=1&pageSize=100",
    "https://agentrs.jd.com/api/saas/user/v1/api-key",
    "https://agentrs.jd.com/api/saas/user/v1/api-key/page?pageNo=1&pageSize=100",
]

ROLE_LABELS = {1: "\u62e5\u6709\u8005", 2: "\u6210\u5458", 3: "\u7ba1\u7406\u5458"}  # 拥有者 / 成员 / 管理员
STATUS_LABELS = {0: "\u6b63\u5e38", 1: "\u51bb\u7ed3", 2: "\u5df2\u9000\u51fa"}  # 正常 / 冻结 / 已退出


def _ts_to_iso(value) -> str:
    """Convert millisecond timestamps coming from JoyAgent APIs to ISO strings."""
    if value is None or value == "":
        return ""
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return str(value)
    if ms <= 0:
        return ""
    seconds = ms / 1000 if ms > 10_000_000_000 else float(ms)
    try:
        return datetime.fromtimestamp(seconds).isoformat(timespec="seconds")
    except (OverflowError, OSError, ValueError):
        return str(value)


def _classify_response(data) -> tuple[str | None, dict | None]:
    """Return (error_label, payload) for a JoyAgent API response."""
    if data is None:
        return ("\u8bf7\u6c42\u5931\u8d25\uff08\u672a\u8fd4\u56de JSON\uff09", None)  # 请求失败（未返回 JSON）
    if not isinstance(data, dict):
        return ("\u54cd\u5e94\u683c\u5f0f\u5f02\u5e38", None)  # 响应格式异常
    code = data.get("code")
    if code == 0:
        return (None, data.get("data"))
    if code == 401:
        return ("\u8d26\u53f7\u672a\u767b\u5f55\uff1a\u8bf7\u8fd0\u884c python joyagent_monitor.py --login", None)
    return (f"\u63a5\u53e3\u8fd4\u56de code={code}, msg={data.get('msg')}", None)


def build_team_payload() -> dict:
    def _job(page):
        return monitor._page_fetch(page, TEAM_API), monitor._page_fetch(page, TENANT_LIST_API)

    fetch_error: str | None = None
    members_raw: list[dict] = []
    tenants_raw: list[dict] = []
    try:
        team_resp, tenant_resp = with_remote_page(_job)
    except Exception as exc:
        fetch_error = str(exc)
        team_resp, tenant_resp = None, None

    if not fetch_error:
        err, data = _classify_response(team_resp)
        if err:
            fetch_error = err
        elif isinstance(data, dict):
            members_raw = data.get("list") or []
        err2, data2 = _classify_response(tenant_resp)
        if not err and not err2 and isinstance(data2, list):
            tenants_raw = data2
        elif err2 and not fetch_error:
            fetch_error = err2

    members = [
        {
            "id": m.get("id"),
            "userId": m.get("userId"),
            "nickname": m.get("nickname"),
            "role": m.get("role"),
            "roleLabel": ROLE_LABELS.get(m.get("role"), str(m.get("role") or "-")),
            "status": m.get("status"),
            "statusLabel": STATUS_LABELS.get(m.get("status"), str(m.get("status") or "-")),
            "joinTime": _ts_to_iso(m.get("joinTime")),
            "applyTime": _ts_to_iso(m.get("applyTime")),
            "remark": m.get("remark"),
        }
        for m in members_raw
    ]
    tenants = [
        {
            "id": t.get("id"),
            "name": t.get("name"),
            "jdAccount": t.get("jdAccount"),
            "ownerName": t.get("ownerName"),
            "createTime": _ts_to_iso(t.get("createTime")),
            "expireTime": _ts_to_iso(t.get("expireTime")),
            "inviteKey": t.get("inviteKey"),
        }
        for t in tenants_raw
    ]
    return {
        "members": members,
        "tenants": tenants,
        "fetch_error": fetch_error,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _probe_first_ok(page, candidates: list[str]) -> tuple[dict | None, list[dict], str | None]:
    """Try each URL in order, return the first code:0 payload + attempt log."""
    attempts: list[dict] = []
    payload_data = None
    matched_url = None
    for url in candidates:
        data = monitor._page_fetch(page, url)
        if data is None:
            attempts.append({"url": url, "code": None, "msg": "no JSON / network"})
            continue
        if not isinstance(data, dict):
            attempts.append({"url": url, "code": "?", "msg": "unexpected shape"})
            continue
        attempts.append({"url": url, "code": data.get("code"), "msg": data.get("msg")})
        if data.get("code") == 0 and matched_url is None:
            matched_url = url
            payload_data = data.get("data")
    return payload_data, attempts, matched_url


def _normalize_resource_row(raw: dict, kind: str) -> dict:
    """Map a raw JoyAgent resource record into the shape the table needs.

    The real /pl/profile?tab=resource page renders 5 columns:
      资源名称 / 类型 / 资源池使用情况 (千Tokens) / 已分配成员数 / 操作
    For each kind we pick the best available source field; missing values
    fall through as None and the UI shows '-' for them.
    """
    raw = raw or {}
    label = (
        raw.get("label") or raw.get("resourceName") or raw.get("modelName")
        or raw.get("name") or raw.get("pluginName") or raw.get("mcpName") or "-"
    )
    type_text = (
        raw.get("chatApiModel") or raw.get("type") or raw.get("category")
        or raw.get("provider") or raw.get("vendor") or kind
    )
    max_tokens = raw.get("maxTotalTokens") or 0
    used_tokens = (
        raw.get("usedTokens") or raw.get("usedTotalTokens")
        or raw.get("consumedTokens") or 0
    )
    member_count = (
        raw.get("memberCount") or raw.get("assignedMemberCount")
        or raw.get("allocatedMembers") or raw.get("memberNum")
    )
    return {
        "kind": kind,
        "id": raw.get("id") or raw.get("modelId") or raw.get("pluginId") or raw.get("mcpId"),
        "label": str(label),
        "description": raw.get("description") or raw.get("intro") or "",
        "avatar": raw.get("avatar"),
        "type": str(type_text) if type_text is not None else "-",
        "maxTotalTokens": int(max_tokens) if str(max_tokens).isdigit() else max_tokens,
        "usedTotalTokens": int(used_tokens) if str(used_tokens).isdigit() else used_tokens,
        "memberCount": member_count,
        "respMaxTokens": raw.get("respMaxTokens"),
        "temperature": raw.get("temperature"),
        "features": raw.get("features"),
        "status": raw.get("status"),
    }


def build_resources_payload() -> dict:
    def _job(page):
        model_resp = monitor._page_fetch(page, RESOURCES_API)
        plugin_data, plugin_attempts, plugin_url = _probe_first_ok(page, PLUGIN_API_CANDIDATES)
        mcp_data, mcp_attempts, mcp_url = _probe_first_ok(page, MCP_API_CANDIDATES)
        return model_resp, (plugin_data, plugin_attempts, plugin_url), (mcp_data, mcp_attempts, mcp_url)

    fetch_error: str | None = None
    model_resp = None
    plugin_data = mcp_data = None
    plugin_attempts: list[dict] = []
    mcp_attempts: list[dict] = []
    plugin_url = mcp_url = None
    try:
        model_resp, (plugin_data, plugin_attempts, plugin_url), (mcp_data, mcp_attempts, mcp_url) = with_remote_page(_job)
    except Exception as exc:
        fetch_error = str(exc)

    model_items_raw: list[dict] = []
    if model_resp is not None:
        err, data = _classify_response(model_resp)
        if err and not fetch_error:
            fetch_error = err
        elif isinstance(data, dict):
            model_items_raw = data.get("list") or []

    def _list_from(payload) -> list[dict]:
        if isinstance(payload, dict):
            return payload.get("list") or payload.get("items") or []
        if isinstance(payload, list):
            return payload
        return []

    items_model = [_normalize_resource_row(r, "model") for r in model_items_raw]
    items_plugin = [_normalize_resource_row(r, "plugin") for r in _list_from(plugin_data)]
    items_mcp = [_normalize_resource_row(r, "mcp") for r in _list_from(mcp_data)]

    return {
        "items": items_model,  # backwards-compatible: old UI used data.items
        "groups": {
            "model": {
                "label": "\u6a21\u578b",  # 模型
                "items": items_model,
                "matched_url": RESOURCES_API,
                "attempts": [],
                "available": True,
            },
            "plugin": {
                "label": "\u63d2\u4ef6",  # 插件
                "items": items_plugin,
                "matched_url": plugin_url,
                "attempts": plugin_attempts,
                "available": plugin_url is not None,
            },
            "mcp": {
                "label": "MCP",
                "items": items_mcp,
                "matched_url": mcp_url,
                "attempts": mcp_attempts,
                "available": mcp_url is not None,
            },
        },
        "fetch_error": fetch_error,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def build_keys_payload() -> dict:
    def _job(page):
        results = []
        for url in API_KEY_CANDIDATES:
            data = monitor._page_fetch(page, url)
            results.append((url, data))
            if isinstance(data, dict) and data.get("code") == 0:
                break
        return results

    fetch_error: str | None = None
    attempts: list[dict] = []
    payload_data = None
    matched_url: str | None = None
    try:
        results = with_remote_page(_job)
    except Exception as exc:
        fetch_error = str(exc)
        results = []

    for url, data in results:
        if data is None:
            attempts.append({"url": url, "code": None, "msg": "no JSON / network error"})
            continue
        if not isinstance(data, dict):
            attempts.append({"url": url, "code": "?", "msg": "unexpected shape"})
            continue
        code = data.get("code")
        attempts.append({"url": url, "code": code, "msg": data.get("msg")})
        if code == 0 and matched_url is None:
            matched_url = url
            payload_data = data.get("data")

    if matched_url is None and not fetch_error:
        # If everything returned 401, prefer that as the error message.
        if attempts and all(a.get("code") == 401 for a in attempts):
            fetch_error = "\u8d26\u53f7\u672a\u767b\u5f55\uff1a\u8bf7\u8fd0\u884c python joyagent_monitor.py --login"
        else:
            fetch_error = "\u672a\u627e\u5230\u53ef\u7528\u7684 API-KEY \u63a5\u53e3\uff08\u5e73\u53f0\u672a\u5bf9\u672c\u8d26\u53f7\u5f00\u653e\uff09"

    keys = []
    if isinstance(payload_data, dict):
        keys = payload_data.get("list") or payload_data.get("items") or []
    elif isinstance(payload_data, list):
        keys = payload_data

    normalized_keys = []
    for k in keys or []:
        if not isinstance(k, dict):
            continue
        normalized_keys.append({
            "id": k.get("id"),
            "name": k.get("name") or k.get("keyName") or k.get("alias") or "-",
            "key": k.get("apiKey") or k.get("accessKey") or k.get("key") or k.get("token") or "-",
            "createdAt": _ts_to_iso(k.get("createTime") or k.get("createdAt")),
            "expireAt": _ts_to_iso(k.get("expireTime") or k.get("expireAt")),
            "status": k.get("status"),
            "statusLabel": STATUS_LABELS.get(k.get("status"), str(k.get("status") or "-")),
        })

    return {
        "keys": normalized_keys,
        "matched_url": matched_url,
        "attempts": attempts,
        "fetch_error": fetch_error,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>个人中心 - JoyAgent 本地复刻</title>
  <style>
    :root {
      color-scheme: light;
      --bg-shell: #F8F9FA;
      --bg-side: #FAFAFA;
      --bg-card: #FFFFFF;
      --text-strong: #202122;
      --text-mid: #525357;
      --text-light: #83858B;
      --text-faint: #9E9FA3;
      --line-soft: #EAEAEB;
      --line-strong: #D3D7DD;
      --primary: #3568FF;
      --primary-tint: rgba(53, 104, 255, 0.08);
      --primary-tint-strong: rgba(53, 104, 255, 0.16);
      --warn: #FF640A;
      --ok: #00A86B;
      --danger: #DC2626;
      --shadow-sm: 0 2px 6px rgba(15, 23, 42, 0.04);
      --shadow-md: 0 8px 24px rgba(15, 23, 42, 0.08);
    }
    * { box-sizing: border-box; }
    html, body { margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Microsoft YaHei",
                   "Segoe UI", system-ui, sans-serif;
      color: var(--text-strong);
      background: var(--bg-shell);
      font-size: 14px;
      line-height: 1.5;
      min-width: 1240px;
    }
    button { font: inherit; cursor: pointer; }
    a { color: inherit; text-decoration: none; }

    /* Top bar */
    .j-topbar {
      position: sticky;
      top: 0;
      z-index: 100;
      height: 64px;
      background: var(--bg-card);
      border-bottom: 1px solid var(--line-soft);
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 24px;
    }
    .j-topbar-left { display: flex; align-items: center; gap: 16px; }
    .j-brand {
      font-size: 18px;
      font-weight: 700;
      letter-spacing: -0.02em;
      background: linear-gradient(135deg, var(--text-strong), #3a3b3f);
      -webkit-background-clip: text;
      background-clip: text;
      color: transparent;
    }
    .j-divider {
      width: 1px;
      height: 12px;
      background: #B8B9BC;
    }
    .j-tenant {
      font-size: 16px;
      color: var(--text-strong);
    }
    .j-topbar-right { display: flex; align-items: center; gap: 12px; }
    .j-poll-status {
      font-size: 12px;
      color: var(--text-light);
      max-width: 360px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .j-poll-status.ok { color: var(--ok); }
    .j-poll-status.bad { color: var(--danger); }

    /* Body shell */
    .j-shell { min-height: 100vh; display: flex; flex-direction: column; }
    .j-body {
      display: flex;
      min-height: calc(100vh - 64px);
    }

    /* Sidebar */
    .j-sidebar {
      width: 210px;
      min-width: 210px;
      background: var(--bg-side);
      border-right: 1px solid var(--line-soft);
      display: flex;
      flex-direction: column;
      position: relative;
    }
    .j-side-nav {
      padding: 12px;
      display: flex;
      flex-direction: column;
      gap: 6px;
      flex: 1;
    }
    .j-nav {
      width: 100%;
      text-align: left;
      padding: 10px 14px;
      border: none;
      background: transparent;
      border-radius: 6px;
      font-size: 14px;
      color: var(--text-mid);
      display: flex;
      align-items: center;
      gap: 8px;
      transition: background 120ms ease, color 120ms ease;
    }
    .j-nav:hover:not(.disabled):not(.active) { background: rgba(0, 0, 0, 0.04); }
    .j-nav.active {
      background: var(--primary-tint);
      color: var(--primary);
      font-weight: 600;
    }
    .j-nav.disabled {
      color: var(--text-faint);
      cursor: not-allowed;
    }
    .j-nav-icon {
      width: 16px;
      height: 16px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }

    .j-account-card {
      margin: 12px;
      padding: 12px 16px;
      background: var(--bg-card);
      border: 1px solid #F1F1F2;
      border-radius: 8px;
      box-shadow: var(--shadow-sm);
      position: relative;
      overflow: hidden;
    }
    .j-account-card::before {
      content: "";
      position: absolute;
      inset: -40px -60px auto auto;
      width: 200px;
      height: 100px;
      background: radial-gradient(120px 80px at 60% 40%, rgba(255, 100, 10, 0.15), transparent 70%);
      pointer-events: none;
    }
    .j-acc-row {
      position: relative;
      padding-bottom: 7px;
      border-bottom: 1px solid #ededed;
    }
    .j-acc-row + .j-acc-row { margin-top: 8px; border-bottom: none; padding-bottom: 0; }
    .j-acc-label { font-size: 12px; color: var(--text-strong); margin-bottom: 4px; }
    .j-acc-value {
      display: flex;
      align-items: flex-end;
      font-size: 18px;
      font-weight: 600;
      color: var(--text-strong);
      line-height: 24px;
    }
    .j-acc-yuan { font-size: 12px; line-height: 20px; }
    .j-acc-mini-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      margin-top: 8px;
      font-size: 12px;
    }
    .j-acc-mini-grid .j-acc-label-mini { color: var(--text-faint); }
    .j-acc-mini-grid .j-acc-value-mini {
      color: var(--text-strong);
      font-weight: 500;
      margin-top: 2px;
    }
    .j-acc-yuan-mini { font-size: 10px; }

    /* Main content */
    .j-main {
      flex: 1;
      min-width: 0;
      background: var(--bg-card);
      padding: 12px 24px 24px;
      overflow: auto;
    }
    .j-view { display: none; }
    .j-view.active { display: block; }

    .j-page-title {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 8px 12px;
      font-size: 16px;
      font-weight: 600;
      color: var(--text-strong);
    }
    .j-pill {
      display: inline-flex;
      align-items: center;
      height: 20px;
      padding: 0 6px;
      border-radius: 4px;
      font-size: 12px;
      font-weight: 400;
    }
    .j-pill-primary { background: var(--primary-tint); color: var(--primary); }
    .j-pill-soft { background: rgba(0, 0, 0, 0.04); color: var(--text-mid); }

    /* Toolbar (filters) */
    .j-toolbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 0 8px;
      margin-bottom: 16px;
      gap: 12px;
      flex-wrap: wrap;
    }
    .j-toolbar-group { display: inline-flex; align-items: center; gap: 12px; flex-wrap: wrap; }

    .j-field {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      height: 32px;
      padding: 0 8px 0 12px;
      background: var(--bg-card);
      border: 1px solid var(--line-strong);
      border-radius: 6px;
      font-size: 14px;
      transition: border-color 120ms ease;
      min-width: 155px;
    }
    .j-field:focus-within { border-color: var(--text-strong); }
    .j-field-prefix { color: var(--text-faint); font-weight: 400; }
    .j-field input,
    .j-field select {
      border: none;
      outline: none;
      background: transparent;
      font: inherit;
      color: var(--text-strong);
      flex: 1;
      min-width: 0;
    }
    .j-field select { appearance: none; padding-right: 16px; cursor: pointer; }

    .j-btn {
      height: 32px;
      padding: 0 12px;
      border: 1px solid var(--line-strong);
      background: var(--bg-card);
      color: var(--text-strong);
      border-radius: 6px;
      font-size: 14px;
      display: inline-flex;
      align-items: center;
      gap: 4px;
      transition: border-color 120ms ease, background 120ms ease;
    }
    .j-btn:hover:not([disabled]) { border-color: var(--text-strong); }
    .j-btn[disabled] { opacity: 0.5; cursor: not-allowed; }
    .j-btn-primary {
      background: var(--primary);
      color: #fff;
      border-color: var(--primary);
    }
    .j-btn-primary:hover:not([disabled]) { background: #2a55d8; border-color: #2a55d8; }
    .j-btn-icon {
      height: 28px;
      min-width: 28px;
      padding: 0 6px;
      border: 1px solid var(--line-strong);
      background: var(--bg-card);
      border-radius: 6px;
      color: var(--text-mid);
    }
    .j-btn-icon[disabled] { opacity: 0.4; cursor: not-allowed; }

    /* Table */
    .j-table-wrap {
      border: 1px solid var(--line-soft);
      border-radius: 12px;
      overflow: hidden;
      background: var(--bg-card);
    }
    .j-scroll {
      overflow: auto;
      max-height: 60vh;
    }
    .j-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    .j-table th, .j-table td {
      padding: 10px 14px;
      text-align: left;
      border-bottom: 1px solid var(--line-soft);
      white-space: nowrap;
    }
    .j-table thead th {
      background: #FAFAFA;
      color: var(--text-mid);
      font-weight: 500;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    .j-table tbody tr:last-child td { border-bottom: none; }
    .j-table tbody tr:hover { background: #FAFBFC; }
    .j-table .num { text-align: right; font-variant-numeric: tabular-nums; }

    .j-empty { padding: 48px 0; text-align: center; color: var(--text-light); }

    /* Pagination */
    .j-pager {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 12px;
      padding: 12px 8px 0;
      font-size: 13px;
      color: var(--text-mid);
    }
    .j-pager .muted { color: var(--text-light); }
    .j-pager select {
      height: 28px;
      border: 1px solid var(--line-strong);
      border-radius: 6px;
      background: var(--bg-card);
      padding: 0 6px;
      font: inherit;
    }

    /* Local dashboard - metric cards */
    .j-metric-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(180px, 1fr));
      gap: 12px;
      padding: 0 8px 16px;
    }
    .j-metric {
      background: var(--bg-card);
      border: 1px solid var(--line-soft);
      border-radius: 12px;
      padding: 14px 16px;
      box-shadow: var(--shadow-sm);
    }
    .j-metric-label { font-size: 12px; color: var(--text-light); }
    .j-metric-value {
      font-size: 24px;
      font-weight: 700;
      margin-top: 4px;
      color: var(--text-strong);
    }
    .j-metric-foot { color: var(--text-light); font-size: 12px; margin-top: 6px; }

    /* Tabs */
    .j-tabs {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      padding: 0 8px;
      margin-bottom: 12px;
    }
    .j-tab {
      height: 30px;
      padding: 0 14px;
      border-radius: 999px;
      border: 1px solid var(--line-strong);
      background: var(--bg-card);
      color: var(--text-mid);
      font-size: 13px;
    }
    .j-tab.active { background: var(--text-strong); color: #fff; border-color: var(--text-strong); }

    .j-card {
      background: var(--bg-card);
      border: 1px solid var(--line-soft);
      border-radius: 12px;
      padding: 14px;
      margin: 0 8px;
    }
    .j-tab-panel { display: none; }
    .j-tab-panel.active { display: block; }

    /* User detail block (local dashboard) */
    .j-user-block {
      border: 1px solid var(--line-soft);
      border-radius: 12px;
      padding: 14px;
      margin-bottom: 14px;
      background: var(--bg-card);
    }
    .j-user-title {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 10px;
      flex-wrap: wrap;
    }
    .j-user-title strong { color: var(--text-strong); }
    .muted { color: var(--text-light); }
    .j-tag-row { display: flex; gap: 6px; flex-wrap: wrap; }
    .j-tag {
      display: inline-flex;
      padding: 2px 8px;
      border-radius: 999px;
      background: var(--primary-tint);
      color: var(--primary);
      font-size: 12px;
    }

    .j-detail-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(140px, 1fr));
      gap: 10px;
      margin: 12px 0 14px;
    }
    .j-mini-card {
      border: 1px solid var(--line-soft);
      border-radius: 10px;
      padding: 10px 12px;
      background: #FAFBFC;
    }
    .j-mini-label { color: var(--text-light); font-size: 12px; }
    .j-mini-value { font-size: 18px; font-weight: 700; margin-top: 2px; color: var(--text-strong); }

    .j-detail-sections {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 14px;
    }
    .j-detail-sections .j-table-wrap .j-scroll { max-height: 360px; }

    .j-clickable-row { cursor: pointer; }
    .j-clickable-row:hover td { background: #F2F4FF; }
    .j-clickable-row.selected td { background: var(--primary-tint); }

    .j-section-title {
      font-size: 14px;
      font-weight: 600;
      margin: 16px 0 8px;
      color: var(--text-strong);
    }

    @media (max-width: 1280px) {
      body { min-width: 1024px; }
      .j-detail-sections { grid-template-columns: 1fr; }
      .j-metric-grid { grid-template-columns: repeat(2, minmax(180px, 1fr)); }
    }

    /* Resource grid (resource management view) */
    .j-res-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
      gap: 12px;
      padding: 0 8px;
    }
    .j-res-card {
      border: 1px solid var(--line-soft);
      border-radius: 12px;
      padding: 14px;
      background: var(--bg-card);
      box-shadow: var(--shadow-sm);
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .j-res-head { display: flex; gap: 10px; align-items: center; }
    .j-res-avatar { width: 36px; height: 36px; border-radius: 8px; object-fit: cover; }
    .j-res-name { font-size: 14px; font-weight: 600; color: var(--text-strong); }
    .j-res-meta { font-size: 12px; }
    .j-res-desc {
      font-size: 12px;
      color: var(--text-mid);
      line-height: 1.55;
      max-height: 4.5em;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .j-res-stats { display: flex; flex-wrap: wrap; gap: 6px; }

    /* Resource table cell - icon + name + truncated description */
    .j-res-cell { display: flex; gap: 10px; align-items: center; }
    .j-res-avatar-sm { width: 28px; height: 28px; border-radius: 6px; object-fit: cover; flex-shrink: 0; }

    /* Usage meter for the 资源池使用情况 column */
    .j-res-meter {
      width: 100%;
      max-width: 320px;
      height: 8px;
      background: #f1f5f9;
      border-radius: 999px;
      overflow: hidden;
    }
    .j-res-meter-fill {
      height: 100%;
      background: linear-gradient(90deg, var(--primary), #4f8bff);
      transition: width 200ms ease;
    }

    /* Mini bar chart for the 费用统计 trend */
    .j-bar-chart {
      display: flex;
      align-items: flex-end;
      gap: 3px;
      height: 120px;
      padding: 0 4px;
      border-bottom: 1px solid var(--line-soft);
    }
    .j-bar {
      flex: 1;
      min-width: 6px;
      display: flex;
      align-items: flex-end;
      height: 100%;
      cursor: default;
    }
    .j-bar-fill {
      width: 100%;
      background: linear-gradient(180deg, var(--primary), #87b3ff);
      border-radius: 4px 4px 0 0;
      min-height: 2px;
      transition: height 200ms ease;
    }
    .j-bar:hover .j-bar-fill { background: linear-gradient(180deg, #1f4ed8, #6ea1ff); }
    .j-bar-axis {
      display: flex;
      justify-content: space-between;
      font-size: 11px;
      color: var(--text-light);
      padding: 4px 4px 0;
    }
  </style>
</head>
<body>
  <div class="j-shell">
    <header class="j-topbar">
      <div class="j-topbar-left">
        <div class="j-brand">JoyAgent</div>
        <div class="j-divider"></div>
        <div class="j-tenant" id="tenantName">-</div>
      </div>
      <div class="j-topbar-right">
        <span class="j-poll-status" id="pollStatus">本地复刻 · 操作通过持久化登录态调用真实 JoyAgent API</span>
      </div>
    </header>

    <div class="j-body">
      <aside class="j-sidebar">
        <nav class="j-side-nav">
          <button class="j-nav" data-view="keys"><span class="j-nav-icon">◆</span>API-KEY</button>
          <button class="j-nav active" data-view="usage"><span class="j-nav-icon">◆</span>用量统计</button>
          <button class="j-nav" data-view="cost"><span class="j-nav-icon">◆</span>费用统计</button>
          <button class="j-nav" data-view="team"><span class="j-nav-icon">◆</span>团队管理</button>
          <button class="j-nav" data-view="resources"><span class="j-nav-icon">◆</span>资源管理</button>
          <button class="j-nav" data-view="local"><span class="j-nav-icon">◆</span>本地账单看板</button>
        </nav>
        <div class="j-account-card">
          <div class="j-acc-row">
            <div class="j-acc-label">账户总额</div>
            <div class="j-acc-value">
              <span class="j-acc-yuan">¥</span>
              <span id="amtTotal">0.00</span>
            </div>
          </div>
          <div class="j-acc-row">
            <div class="j-acc-label">剩余积分</div>
            <div class="j-acc-value" id="amtCredits">0</div>
          </div>
          <div class="j-acc-mini-grid">
            <div>
              <div class="j-acc-label-mini">余额</div>
              <div class="j-acc-value-mini">
                <span class="j-acc-yuan-mini">¥</span><span id="amtCash">0.00</span>
              </div>
            </div>
            <div>
              <div class="j-acc-label-mini">代金券</div>
              <div class="j-acc-value-mini">
                <span class="j-acc-yuan-mini">¥</span><span id="amtVoucher">0.00</span>
              </div>
            </div>
          </div>
        </div>
      </aside>

      <main class="j-main">
        <!-- View 1: cloned profile usage statistics -->
        <section class="j-view active" id="view-usage">
          <div class="j-page-title">
            <span>用量统计</span>
            <span class="j-pill j-pill-primary">T+1 更新</span>
            <span class="j-pill j-pill-soft" id="usageGenAt">-</span>
          </div>
          <div class="j-toolbar">
            <div class="j-toolbar-group">
              <label class="j-field">
                <span class="j-field-prefix">月份</span>
                <input type="month" id="usageMonth" />
              </label>
              <label class="j-field">
                <span class="j-field-prefix">资源</span>
                <select id="usageResource"><option value="">全部资源</option></select>
              </label>
              <button class="j-btn" id="usageClear">清空筛选</button>
            </div>
            <div class="j-toolbar-group">
              <button class="j-btn" id="usageExport">↓ 导出调用明细</button>
              <button class="j-btn j-btn-primary" id="usageRefresh">立即刷新</button>
            </div>
          </div>
          <div class="j-table-wrap">
            <div class="j-scroll">
              <table class="j-table">
                <thead>
                  <tr>
                    <th>调用时间</th>
                    <th>账户信息 (PIN)</th>
                    <th>用户名</th>
                    <th>模型</th>
                    <th>Token类型</th>
                    <th class="num">使用量 (Tokens)</th>
                  </tr>
                </thead>
                <tbody id="usageBody">
                  <tr><td colspan="6" class="j-empty">加载中...</td></tr>
                </tbody>
              </table>
            </div>
          </div>
          <div class="j-pager">
            <span class="muted">每页</span>
            <select id="usagePageSize">
              <option>10</option>
              <option>20</option>
              <option>50</option>
              <option>100</option>
            </select>
            <span class="muted">条</span>
            <span class="muted">共 <span id="usageTotal">0</span> 条</span>
            <button class="j-btn-icon" id="usagePrev" title="上一页">←</button>
            <span><span id="usagePageNo">1</span> / <span id="usageMaxPage">1</span></span>
            <button class="j-btn-icon" id="usageNext" title="下一页">→</button>
          </div>
        </section>

        <!-- View 2: existing local dashboard -->
        <section class="j-view" id="view-local">
          <div class="j-page-title">
            <span>本地账单看板</span>
            <span class="j-pill j-pill-soft" id="dashGenAt">-</span>
          </div>

          <div class="j-toolbar">
            <div class="j-toolbar-group">
              <label class="j-field">
                <span class="j-field-prefix">用户</span>
                <select id="userSelect"><option value="">全部用户</option></select>
              </label>
              <label class="j-field">
                <span class="j-field-prefix">搜索</span>
                <input id="search" placeholder="筛选用户/模型/日期" />
              </label>
            </div>
            <div class="j-toolbar-group">
              <label class="j-field">
                <span class="j-field-prefix">自动刷新</span>
                <select id="refreshInterval">
                  <option value="5000">5s</option>
                  <option value="10000" selected>10s</option>
                  <option value="30000">30s</option>
                  <option value="0">关闭</option>
                </select>
              </label>
              <button class="j-btn j-btn-primary" id="reload">立即刷新</button>
            </div>
          </div>

          <section class="j-metric-grid">
            <div class="j-metric"><div class="j-metric-label">账户总额</div><div class="j-metric-value" id="balanceTotal">-</div><div class="j-metric-foot" id="balanceDetail">-</div></div>
            <div class="j-metric"><div class="j-metric-label">用量费用 (raw)</div><div class="j-metric-value" id="usageRaw">-</div><div class="j-metric-foot" id="usageFoot">-</div></div>
            <div class="j-metric"><div class="j-metric-label">用量费用 (floor)</div><div class="j-metric-value" id="usageFloor">-</div><div class="j-metric-foot">每条记录向下抹零到分</div></div>
            <div class="j-metric"><div class="j-metric-label">历史实际扣费</div><div class="j-metric-value" id="billTotal">-</div><div class="j-metric-foot" id="billFoot">-</div></div>
          </section>

          <nav class="j-tabs">
            <button class="j-tab active" data-tab="users">各用户累计</button>
            <button class="j-tab" data-tab="daily">用户按天</button>
            <button class="j-tab" data-tab="bills">实际扣费按天</button>
            <button class="j-tab" data-tab="models">模型 / Token 类型</button>
            <button class="j-tab" data-tab="latest">明细</button>
          </nav>

          <div class="j-card">
            <div class="j-tab-panel active" id="tab-users">
              <div id="usersTable" class="muted">加载中...</div>
              <div id="userDetail" style="margin-top:16px"></div>
            </div>
            <div class="j-tab-panel" id="tab-daily">
              <div id="dailyTable" class="muted">加载中...</div>
            </div>
            <div class="j-tab-panel" id="tab-bills">
              <div id="billsTable" class="muted">加载中...</div>
            </div>
            <div class="j-tab-panel" id="tab-models">
              <div id="modelsTable" class="muted">加载中...</div>
            </div>
            <div class="j-tab-panel" id="tab-latest">
              <div id="latestTable" class="muted">加载中...</div>
            </div>
          </div>
        </section>

        <!-- View 3: API-KEY -->
        <section class="j-view" id="view-keys">
          <div class="j-page-title">
            <span>API-KEY</span>
            <span class="j-pill j-pill-soft" id="keysGenAt">-</span>
          </div>
          <div class="j-toolbar">
            <div class="j-toolbar-group">
              <span class="muted">本地复刻只读：从已登录态探测可用接口</span>
            </div>
            <div class="j-toolbar-group">
              <button class="j-btn j-btn-primary" id="keysRefresh">立即刷新</button>
            </div>
          </div>
          <div class="j-card" id="keysContainer">
            <div class="j-empty">加载中...</div>
          </div>
        </section>

        <!-- View 4: team management -->
        <section class="j-view" id="view-team">
          <div class="j-page-title">
            <span>团队管理</span>
            <span class="j-pill j-pill-soft" id="teamGenAt">-</span>
          </div>
          <div class="j-toolbar">
            <div class="j-toolbar-group">
              <label class="j-field">
                <span class="j-field-prefix">搜索</span>
                <input id="teamSearch" placeholder="按账户/昵称/角色筛选" />
              </label>
            </div>
            <div class="j-toolbar-group">
              <button class="j-btn j-btn-primary" id="teamRefresh">立即刷新</button>
            </div>
          </div>
          <div class="j-card" id="teamTenantCard"></div>
          <div style="margin-top:12px"></div>
          <div class="j-card" id="teamMembersCard">
            <div class="j-empty">加载中...</div>
          </div>
        </section>

        <!-- View 5: resource management -->
        <section class="j-view" id="view-resources">
          <div class="j-page-title">
            <span>资源管理</span>
            <span class="j-pill j-pill-soft" id="resourcesGenAt">-</span>
          </div>
          <nav class="j-tabs" id="resourcesTabs">
            <button class="j-tab active" data-restab="model">模型</button>
            <button class="j-tab" data-restab="plugin">插件</button>
            <button class="j-tab" data-restab="mcp">MCP</button>
          </nav>
          <div class="j-toolbar">
            <div class="j-toolbar-group">
              <label class="j-field">
                <span class="j-field-prefix">搜索</span>
                <input id="resSearch" placeholder="按资源名/类型/描述筛选" />
              </label>
            </div>
            <div class="j-toolbar-group">
              <button class="j-btn j-btn-primary" id="resourcesRefresh">立即刷新</button>
            </div>
          </div>
          <div id="resourcesContent">
            <div class="j-empty">加载中...</div>
          </div>
          <div id="resourceDetail" style="margin-top:12px"></div>
        </section>

        <!-- View 6: cost statistics -->
        <section class="j-view" id="view-cost">
          <div class="j-page-title">
            <span>费用统计</span>
            <span class="j-pill j-pill-primary" id="costSourceBadge">三方对账</span>
            <span class="j-pill j-pill-soft" id="costGenAt">-</span>
          </div>

          <div class="j-toolbar">
            <div class="j-toolbar-group">
              <span class="muted" id="costSourceHint">数据来源：JD Cloud 账单明细 (jdcloud_bills) + JoyAgent 扣费 (historical_bills) + 本地按 token 计算 (usage_records)</span>
            </div>
            <div class="j-toolbar-group">
              <button class="j-btn j-btn-primary" id="costRefresh">立即刷新</button>
            </div>
          </div>

          <div class="j-metric-grid">
            <div class="j-metric"><div class="j-metric-label">累计实际扣费</div><div class="j-metric-value" id="costTotal">-</div><div class="j-metric-foot" id="costTotalFoot">-</div></div>
            <div class="j-metric"><div class="j-metric-label">本月实际扣费</div><div class="j-metric-value" id="costThisMonth">-</div><div class="j-metric-foot" id="costThisMonthFoot">-</div></div>
            <div class="j-metric"><div class="j-metric-label">上月实际扣费</div><div class="j-metric-value" id="costLastMonth">-</div><div class="j-metric-foot" id="costLastMonthFoot">-</div></div>
            <div class="j-metric"><div class="j-metric-label">日均扣费 (近 30 天)</div><div class="j-metric-value" id="costDailyAvg">-</div><div class="j-metric-foot" id="costDailyAvgFoot">-</div></div>
          </div>

          <div class="j-card" style="margin-bottom:14px">
            <div class="j-section-title">按月对账（实际 vs 本地计算）</div>
            <div id="costMonthlyTable" class="muted">加载中...</div>
          </div>

          <div class="j-card" style="margin-bottom:14px">
            <div class="j-section-title">按资源 / 模型 扣费排行</div>
            <div id="costResourceTable" class="muted">加载中...</div>
          </div>

          <div class="j-card" style="margin-bottom:14px">
            <div class="j-section-title">最近 30 天扣费走势</div>
            <div id="costTrend" class="muted">加载中...</div>
          </div>

          <div class="j-card">
            <div class="j-section-title">最近扣费明细（最多 100 条）</div>
            <div id="costRecordsTable" class="muted">加载中...</div>
          </div>
        </section>
      </main>
    </div>
  </div>

  <script>
    /* ============================================================
       State + helpers
       ============================================================ */
    const state = {
      currentView: "usage",
      currentTab: "users",
      selectedUserKey: "",
      profile: { month: new Date().toISOString().slice(0, 7), resource: "", pageNo: 1, pageSize: 10 },
      timer: null,
      dashPayload: null,
      profilePayload: null,
      teamPayload: null,
      resPayload: null,
    };

    const $ = (id) => document.getElementById(id);

    /* In-flight guard: skip a fetch if the same URL is still pending so
       auto-refresh + manual refresh + view switching don't stack up. */
    const _inflight = new Map();
    async function safeFetchJson(url, opts) {
      const existing = _inflight.get(url);
      if (existing) return existing;
      const promise = (async () => {
        try {
          const resp = await fetch(url, Object.assign({ cache: "no-store" }, opts || {}));
          if (!resp.ok) throw new Error("HTTP " + resp.status);
          return await resp.json();
        } finally {
          _inflight.delete(url);
        }
      })();
      _inflight.set(url, promise);
      return promise;
    }
    const fmtMoney = (v, digits = 2) => {
      const n = Number(v || 0);
      return "¥" + n.toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
    };
    const fmtInt = (v) => Number(v || 0).toLocaleString("zh-CN");
    const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

    function userKey(row) { return (row.account_pin || "") + "\u0001" + (row.username || ""); }
    function encUser(key) { return encodeURIComponent(key || ""); }
    function decUser(value) { try { return decodeURIComponent(value || ""); } catch (e) { return value || ""; } }
    function splitUserKey(key) {
      const parts = String(key || "").split("\u0001");
      return { pin: parts[0] || "", username: parts[1] || "" };
    }

    function searchText() {
      const el = $("search");
      return (el && el.value || "").trim().toLowerCase();
    }
    function includesSearch(obj) {
      const q = searchText();
      if (!q) return true;
      try { return JSON.stringify(obj).toLowerCase().includes(q); }
      catch { return true; }
    }

    function table(headers, rows) {
      return '<div class="j-table-wrap"><div class="j-scroll"><table class="j-table"><thead><tr>'
        + headers.map(h => '<th class="' + (h.num ? "num" : "") + '">' + esc(h.label) + '</th>').join("")
        + '</tr></thead><tbody>' + (rows.length ? rows.join("") : '<tr><td colspan="' + headers.length + '" class="j-empty">暂无数据</td></tr>')
        + '</tbody></table></div></div>';
    }

    /* ============================================================
       Sidebar / view routing
       ============================================================ */
    function selectView(name) {
      state.currentView = name;
      document.querySelectorAll(".j-nav").forEach(btn => {
        if (btn.dataset.view) btn.classList.toggle("active", btn.dataset.view === name);
      });
      document.querySelectorAll(".j-view").forEach(v => v.classList.toggle("active", v.id === "view-" + name));
      const loaders = {
        usage: loadProfile,
        local: loadDashboard,
        keys: loadKeys,
        team: loadTeam,
        resources: loadResources,
        cost: loadCost,
      };
      const fn = loaders[name];
      if (fn) fn();
    }

    function selectTab(name) {
      state.currentTab = name;
      document.querySelectorAll(".j-tab").forEach(b => b.classList.toggle("active", b.dataset.tab === name));
      document.querySelectorAll(".j-tab-panel").forEach(p => p.classList.toggle("active", p.id === "tab-" + name));
    }

    /* ============================================================
       Sidebar account card update (shared across views)
       ============================================================ */
    function renderAccount(amount, userinfo) {
      const a = amount || {};
      $("amtTotal").textContent = Number(a.total || 0).toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      $("amtCash").textContent = Number(a.cash || 0).toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      $("amtVoucher").textContent = Number(a.voucher || 0).toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      $("amtCredits").textContent = fmtInt(a.credits || 0);
      if (userinfo && userinfo.tenantName) $("tenantName").textContent = userinfo.tenantName;
    }

    /* ============================================================
       View 1: usage statistics (cloned profile)
       ============================================================ */
    async function loadProfile() {
      const params = new URLSearchParams({
        month: state.profile.month,
        resource: state.profile.resource,
        pageNo: String(state.profile.pageNo),
        pageSize: String(state.profile.pageSize),
      });
      try {
        renderProfile(await safeFetchJson("/api/profile?" + params.toString()));
        return;
      } catch (err) {
        $("usageBody").innerHTML = '<tr><td colspan="6" class="j-empty">加载失败：' + esc(err.message) + '</td></tr>';
      }
    }

    function renderProfile(data) {
      state.profilePayload = data;
      renderAccount(data.amount, data.userinfo);
      $("usageGenAt").textContent = "更新于 " + (data.generated_at || "-");

      const sel = $("usageResource");
      const cur = state.profile.resource || "";
      sel.innerHTML = '<option value="">全部资源</option>' + (data.resources || [])
        .map(r => '<option value="' + esc(r.label || "") + '">' + esc(r.label || "") + '</option>')
        .join("");
      sel.value = cur;

      const billing = data.billing || {};
      const rows = billing.list || [];
      const total = Number(billing.total || 0);
      const pageSize = Number(billing.pageSize || state.profile.pageSize) || 10;
      const pageNo = Number(billing.pageNo || state.profile.pageNo) || 1;
      const maxPage = Math.max(1, Math.ceil(total / pageSize));
      $("usageTotal").textContent = fmtInt(total);
      $("usagePageNo").textContent = pageNo;
      $("usageMaxPage").textContent = maxPage;
      $("usagePrev").disabled = pageNo <= 1;
      $("usageNext").disabled = pageNo >= maxPage;

      $("usageBody").innerHTML = rows.length
        ? rows.map(r => '<tr>'
            + '<td>' + esc(r.statTime || r.dt || "") + '</td>'
            + '<td>' + esc(r.invokeUserId || "") + '</td>'
            + '<td>' + esc(r.realName || "") + '</td>'
            + '<td>' + esc(r.resourceName || "") + '</td>'
            + '<td>' + esc(r.billingTokenType || "") + '</td>'
            + '<td class="num">' + fmtInt(r.billingQuantity || 0) + '</td>'
          + '</tr>').join("")
        : '<tr><td colspan="6" class="j-empty">No data</td></tr>';
    }

    function exportUsageCsv() {
      const billing = (state.profilePayload && state.profilePayload.billing) || {};
      const rows = billing.list || [];
      const head = ["调用时间", "账户信息(PIN)", "用户名", "模型", "Token类型", "使用量(Tokens)"];
      const csv = [head].concat(rows.map(r => [
        r.statTime || r.dt || "", r.invokeUserId || "", r.realName || "",
        r.resourceName || "", r.billingTokenType || "", String(r.billingQuantity || 0),
      ])).map(cols => cols.map(v => '"' + String(v).replaceAll('"', '""') + '"').join(",")).join("\n");
      const blob = new Blob(["\ufeff" + csv], { type: "text/csv;charset=utf-8" });
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = "joyagent_usage_" + state.profile.month + ".csv";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(a.href);
    }

    /* ============================================================
       View 2: local dashboard
       ============================================================ */
    async function loadDashboard() {
      try {
        renderDashboard(await safeFetchJson("/api/dashboard"));
      } catch (err) {
        $("usersTable").innerHTML = '<div class="j-empty">加载失败：' + esc(err.message) + '</div>';
      }
    }

    function renderDashboard(data) {
      state.dashPayload = data;
      const b = data.balance || {};
      const u = data.usage_total || {};
      const bt = data.bill_total || {};
      $("balanceTotal").textContent = fmtMoney(b.balance_cny || 0);
      $("balanceDetail").textContent = "余额 " + fmtMoney(b.cash_cny || 0)
        + " · 代金券 " + fmtMoney(b.voucher_cny || 0)
        + " · 积分 " + fmtInt(b.credits || 0);
      $("usageRaw").textContent = fmtMoney(u.cny_raw || 0, 4);
      $("usageFoot").textContent = fmtInt(u.tokens || 0) + " tokens / " + fmtInt(u.rows || 0) + " 行";
      $("usageFloor").textContent = fmtMoney(u.cny_floor || 0);
      $("billTotal").textContent = fmtMoney(bt.amount || 0);
      $("billFoot").textContent = fmtInt(bt.rows || 0) + " 笔历史扣费";

      const st = data.state || {};
      const cls = st.last_poll_ok ? "ok" : (st.last_poll_error ? "bad" : "");
      $("dashGenAt").textContent = "更新于 " + (data.generated_at || "-");
      $("pollStatus").className = "j-poll-status " + cls;
      $("pollStatus").textContent = "轮询 "
        + (st.polling_enabled ? "已开启" : "未开启")
        + " · 最近 " + (st.last_poll_at || "-")
        + (st.last_poll_error ? " · " + st.last_poll_error : "");

      renderUsers(data);
      renderDaily(data);
      renderBills(data);
      renderModels(data);
      renderLatest(data);
    }

    function renderUsers(data) {
      const select = $("userSelect");
      const old = state.selectedUserKey;
      select.innerHTML = '<option value="">全部用户</option>' + (data.users || []).map(r => {
        const key = userKey(r);
        return '<option value="' + esc(encUser(key)) + '">'
          + esc((r.account_pin || "-") + " / " + (r.username || "-") + " / " + fmtMoney(r.cny_raw || 0, 4))
          + '</option>';
      }).join("");
      if (old && (data.users || []).some(r => userKey(r) === old)) select.value = encUser(old);
      else if (old) state.selectedUserKey = "";

      const modelMap = new Map();
      (data.user_model || []).forEach(r => {
        const k = userKey(r);
        if (!modelMap.has(k)) modelMap.set(k, []);
        modelMap.get(k).push(r);
      });

      const rows = (data.users || []).filter(includesSearch).map(r => {
        const key = userKey(r);
        const models = (modelMap.get(key) || []).map(m =>
          '<span class="j-tag">' + esc(m.model) + ' ' + fmtMoney(m.cny_raw || 0) + '</span>').join(" ");
        return '<tr class="j-clickable-row ' + (key === state.selectedUserKey ? 'selected' : '') + '" '
          + 'data-user-key="' + esc(encUser(key)) + '" title="点击查看该用户细分">'
          + '<td>' + esc(r.account_pin) + '</td>'
          + '<td>' + esc(r.username) + '</td>'
          + '<td class="num">' + fmtInt(r.tokens) + '</td>'
          + '<td class="num">' + fmtMoney(r.cny_raw, 4) + '</td>'
          + '<td class="num">' + fmtMoney(r.cny_floor) + '</td>'
          + '<td><div class="j-tag-row">' + models + '</div></td>'
        + '</tr>';
      });
      $("usersTable").innerHTML = table([
        { label: "账户信息 (PIN)" }, { label: "用户名" }, { label: "总 tokens", num: true },
        { label: "费用 (raw)", num: true }, { label: "费用 (floor)", num: true }, { label: "各模型费用" }
      ], rows);

      document.querySelectorAll("#usersTable .j-clickable-row").forEach(row => {
        row.addEventListener("click", () => {
          state.selectedUserKey = decUser(row.dataset.userKey || "");
          select.value = encUser(state.selectedUserKey);
          renderDashboard(data);
          $("userDetail").scrollIntoView({ behavior: "smooth", block: "start" });
        });
      });
      renderUserDetail(data);
    }

    function renderUserDetail(data) {
      const box = $("userDetail");
      if (!state.selectedUserKey) {
        box.innerHTML = '<div class="j-empty">点击上方用户行，或在“用户”下拉框中选择，可查看模型 / Token 类型 / 按天用量 / 关联账单。</div>';
        return;
      }
      const user = (data.users || []).find(r => userKey(r) === state.selectedUserKey);
      if (!user) { box.innerHTML = '<div class="j-empty">该用户暂无数据。</div>'; return; }

      const { pin, username } = splitUserKey(state.selectedUserKey);
      const sameUser = (r) => userKey(r) === state.selectedUserKey;
      const modelRows = (data.user_model || []).filter(sameUser);
      const dailyRows = (data.user_daily || []).filter(sameUser);
      const usageRows = (data.latest_usage || []).filter(sameUser);
      const tokenAgg = new Map();
      usageRows.forEach(r => {
        const k = (r.model || "") + "\u0001" + (r.token_type_raw || "");
        const cur = tokenAgg.get(k) || { model: r.model, token_type: r.token_type_raw, tokens: 0, cny_raw: 0, cny_floor: 0 };
        cur.tokens += Number(r.tokens || 0);
        cur.cny_raw += Number(r.cny_amount || 0);
        cur.cny_floor += Number(r.cny_amount_floor || 0);
        tokenAgg.set(k, cur);
      });
      const tokenRows = Array.from(tokenAgg.values()).sort((a, b) => b.cny_raw - a.cny_raw);
      const usedDays = new Set(dailyRows.map(r => r.day));
      const usedModels = new Set(dailyRows.map(r => r.model));
      const relatedBills = (data.bill_records || []).filter(r =>
        usedDays.has(r.day) && (usedModels.has(r.resource) || r.resource === "未知模型")
      );

      const modelTable = table(
        [{label:"模型"}, {label:"tokens", num:true}, {label:"费用 (raw)", num:true}, {label:"费用 (floor)", num:true}],
        modelRows.map(r => '<tr>'
          + '<td>' + esc(r.model) + '</td>'
          + '<td class="num">' + fmtInt(r.tokens) + '</td>'
          + '<td class="num">' + fmtMoney(r.cny_raw, 4) + '</td>'
          + '<td class="num">' + fmtMoney(r.cny_floor) + '</td>'
        + '</tr>')
      );
      const tokenTable = table(
        [{label:"模型"}, {label:"Token 类型"}, {label:"tokens", num:true}, {label:"费用 (raw)", num:true}, {label:"费用 (floor)", num:true}],
        tokenRows.map(r => '<tr>'
          + '<td>' + esc(r.model) + '</td>'
          + '<td>' + esc(r.token_type) + '</td>'
          + '<td class="num">' + fmtInt(r.tokens) + '</td>'
          + '<td class="num">' + fmtMoney(r.cny_raw, 4) + '</td>'
          + '<td class="num">' + fmtMoney(r.cny_floor) + '</td>'
        + '</tr>')
      );
      const dailyTable = table(
        [{label:"日期"}, {label:"模型"}, {label:"tokens", num:true}, {label:"费用 (raw)", num:true}, {label:"费用 (floor)", num:true}],
        dailyRows.map(r => '<tr>'
          + '<td>' + esc(r.day) + '</td>'
          + '<td>' + esc(r.model) + '</td>'
          + '<td class="num">' + fmtInt(r.tokens) + '</td>'
          + '<td class="num">' + fmtMoney(r.cny_raw, 4) + '</td>'
          + '<td class="num">' + fmtMoney(r.cny_floor) + '</td>'
        + '</tr>')
      );
      const usageTable = table(
        [{label:"调用时间"}, {label:"模型"}, {label:"Token 类型"}, {label:"tokens", num:true}, {label:"费用 (raw)", num:true}],
        usageRows.map(r => '<tr>'
          + '<td>' + esc(r.call_time) + '</td>'
          + '<td>' + esc(r.model) + '</td>'
          + '<td>' + esc(r.token_type_raw) + '</td>'
          + '<td class="num">' + fmtInt(r.tokens) + '</td>'
          + '<td class="num">' + fmtMoney(r.cny_amount, 4) + '</td>'
        + '</tr>')
      );
      const billTable = table(
        [{label:"扣费时间"}, {label:"资源模型"}, {label:"实际扣费", num:true}, {label:"来源"}],
        relatedBills.map(r => '<tr>'
          + '<td>' + esc(r.charge_time) + '</td>'
          + '<td>' + esc(r.resource) + '</td>'
          + '<td class="num">' + fmtMoney(r.amount) + '</td>'
          + '<td>' + esc(r.source) + '</td>'
        + '</tr>')
      );

      box.innerHTML =
        '<div class="j-user-block">'
        + '<div class="j-user-title">'
        +   '<strong>用户详情：' + esc(pin || "-") + ' / ' + esc(username || "-") + '</strong>'
        +   '<span class="muted">关联账单按 用户出现日期 ∩ 用户使用模型 匹配（账单源不携带用户字段）</span>'
        + '</div>'
        + '<div class="j-detail-grid">'
        +   '<div class="j-mini-card"><div class="j-mini-label">总 tokens</div><div class="j-mini-value">' + fmtInt(user.tokens) + '</div></div>'
        +   '<div class="j-mini-card"><div class="j-mini-label">费用 (raw)</div><div class="j-mini-value">' + fmtMoney(user.cny_raw, 4) + '</div></div>'
        +   '<div class="j-mini-card"><div class="j-mini-label">费用 (floor)</div><div class="j-mini-value">' + fmtMoney(user.cny_floor) + '</div></div>'
        +   '<div class="j-mini-card"><div class="j-mini-label">关联账单笔数</div><div class="j-mini-value">' + fmtInt(relatedBills.length) + '</div></div>'
        + '</div>'
        + '<div class="j-detail-sections">'
        +   '<div><div class="j-section-title">模型拆分</div>' + modelTable + '</div>'
        +   '<div><div class="j-section-title">Token 类型拆分</div>' + tokenTable + '</div>'
        +   '<div><div class="j-section-title">按天用量 / 费用</div>' + dailyTable + '</div>'
        +   '<div><div class="j-section-title">关联历史账单</div>' + billTable + '</div>'
        + '</div>'
        + '<div class="j-section-title">该用户调用明细</div>' + usageTable
        + '</div>';
    }

    function renderDaily(data) {
      const grouped = new Map();
      (data.user_daily || []).filter(includesSearch).forEach(r => {
        const k = userKey(r);
        if (!grouped.has(k)) grouped.set(k, []);
        grouped.get(k).push(r);
      });
      const blocks = [];
      grouped.forEach((rows, key) => {
        const { pin, username } = splitUserKey(key);
        const totalRaw = rows.reduce((s, r) => s + Number(r.cny_raw || 0), 0);
        const totalFloor = rows.reduce((s, r) => s + Number(r.cny_floor || 0), 0);
        const totalTokens = rows.reduce((s, r) => s + Number(r.tokens || 0), 0);
        const body = rows.map(r => '<tr>'
          + '<td>' + esc(r.day) + '</td>'
          + '<td>' + esc(r.model) + '</td>'
          + '<td class="num">' + fmtInt(r.tokens) + '</td>'
          + '<td class="num">' + fmtMoney(r.cny_raw, 4) + '</td>'
          + '<td class="num">' + fmtMoney(r.cny_floor) + '</td>'
        + '</tr>').join("");
        blocks.push('<div class="j-user-block">'
          + '<div class="j-user-title">'
          +   '<strong>' + esc(pin) + ' / ' + esc(username || "-") + '</strong>'
          +   '<span class="muted">' + fmtInt(totalTokens) + ' tokens · raw ' + fmtMoney(totalRaw, 4) + ' · floor ' + fmtMoney(totalFloor) + '</span>'
          + '</div>'
          + table([{label:"日期"}, {label:"模型"}, {label:"tokens", num:true}, {label:"费用 (raw)", num:true}, {label:"费用 (floor)", num:true}], [body])
        + '</div>');
      });
      $("dailyTable").innerHTML = blocks.length ? blocks.join("") : '<div class="j-empty">暂无数据</div>';
    }

    function renderBills(data) {
      const months = (data.bills_by_month || []).map(r =>
        '<span class="j-tag">' + esc(r.month) + ' ' + fmtMoney(r.amount) + ' / ' + fmtInt(r.rows) + ' 笔</span>').join(" ");
      const rows = (data.bills_daily || []).filter(includesSearch).map(r => '<tr>'
        + '<td>' + esc(r.day) + '</td>'
        + '<td>' + esc(r.resource) + '</td>'
        + '<td class="num">' + fmtInt(r.rows) + '</td>'
        + '<td class="num">' + fmtMoney(r.amount) + '</td>'
      + '</tr>');
      $("billsTable").innerHTML = '<div class="j-tag-row" style="margin-bottom:12px">' + months + '</div>'
        + table([{label:"日期"}, {label:"资源模型"}, {label:"计费笔数", num:true}, {label:"扣费", num:true}], rows);
    }

    function renderModels(data) {
      const rows = (data.usage_by_model || []).filter(includesSearch).map(r => '<tr>'
        + '<td>' + esc(r.model) + '</td>'
        + '<td>' + esc(r.token_type) + '</td>'
        + '<td class="num">' + fmtInt(r.tokens) + '</td>'
        + '<td class="num">' + fmtMoney(r.cny_raw, 4) + '</td>'
        + '<td class="num">' + fmtMoney(r.cny_floor) + '</td>'
      + '</tr>');
      $("modelsTable").innerHTML = table(
        [{label:"模型"}, {label:"Token 类型"}, {label:"tokens", num:true}, {label:"费用 (raw)", num:true}, {label:"费用 (floor)", num:true}],
        rows
      );
    }

    function renderLatest(data) {
      const rows = (data.latest_usage || []).filter(includesSearch).map(r => '<tr>'
        + '<td>' + esc(r.call_time) + '</td>'
        + '<td>' + esc(r.account_pin) + '</td>'
        + '<td>' + esc(r.username) + '</td>'
        + '<td>' + esc(r.model) + '</td>'
        + '<td>' + esc(r.token_type_raw) + '</td>'
        + '<td class="num">' + fmtInt(r.tokens) + '</td>'
        + '<td class="num">' + fmtMoney(r.cny_amount, 4) + '</td>'
        + '<td class="num">' + fmtMoney(r.cny_amount_floor) + '</td>'
      + '</tr>');
      $("latestTable").innerHTML = table([
        {label:"调用时间"}, {label:"账户信息 (PIN)"}, {label:"用户名"}, {label:"模型"}, {label:"Token 类型"},
        {label:"使用量 (Tokens)", num:true}, {label:"费用 (raw)", num:true}, {label:"费用 (floor)", num:true}
      ], rows);
    }

    /* ============================================================
       View 3: API-KEY
       ============================================================ */
    function renderErrorBanner(msg) {
      return '<div class="j-empty" style="color:#b45309;background:#fffbeb;border-radius:8px;padding:14px;">' + esc(msg) + '</div>';
    }

    async function loadKeys() {
      try {
        renderKeys(await safeFetchJson("/api/keys"));
      } catch (err) {
        $("keysContainer").innerHTML = renderErrorBanner("加载失败：" + err.message);
      }
    }

    function renderKeys(data) {
      $("keysGenAt").textContent = "更新于 " + (data.generated_at || "-");
      const blocks = [];
      if (data.fetch_error) {
        blocks.push(renderErrorBanner(data.fetch_error));
      }
      if (data.matched_url) {
        blocks.push('<div class="muted" style="margin-bottom:10px;font-size:12px;">命中接口：' + esc(data.matched_url) + '</div>');
      }
      const keys = data.keys || [];
      if (keys.length) {
        blocks.push(table(
          [{label:"名称"}, {label:"Key"}, {label:"状态"}, {label:"创建时间"}, {label:"过期时间"}],
          keys.map(k => '<tr>'
            + '<td>' + esc(k.name) + '</td>'
            + '<td><code style="font-size:12px">' + esc(k.key) + '</code> '
            +   '<button class="j-btn-icon" data-copy="' + esc(k.key) + '" title="复制">⧉</button></td>'
            + '<td>' + esc(k.statusLabel || k.status || "-") + '</td>'
            + '<td>' + esc(k.createdAt || "-") + '</td>'
            + '<td>' + esc(k.expireAt || "-") + '</td>'
          + '</tr>')
        ));
      } else if (!data.fetch_error) {
        blocks.push('<div class="j-empty">该账号下没有 API-KEY 记录。</div>');
      }
      if ((data.attempts || []).length) {
        blocks.push('<details style="margin-top:14px"><summary class="muted" style="cursor:pointer">探测明细（' + data.attempts.length + ' 个候选）</summary>'
          + table([{label:"接口"}, {label:"code", num:true}, {label:"msg"}],
            data.attempts.map(a => '<tr>'
              + '<td><code style="font-size:12px">' + esc(a.url) + '</code></td>'
              + '<td class="num">' + esc(a.code == null ? "-" : a.code) + '</td>'
              + '<td>' + esc(a.msg || "") + '</td>'
            + '</tr>'))
          + '</details>');
      }
      $("keysContainer").innerHTML = blocks.join("") || '<div class="j-empty">暂无数据</div>';
      $("keysContainer").querySelectorAll("[data-copy]").forEach(btn => {
        btn.addEventListener("click", async () => {
          try { await navigator.clipboard.writeText(btn.dataset.copy || ""); btn.textContent = "✓"; setTimeout(() => { btn.textContent = "⧉"; }, 1200); }
          catch { /* clipboard unavailable */ }
        });
      });
    }

    /* ============================================================
       View 4: team management
       ============================================================ */
    async function loadTeam() {
      try {
        state.teamPayload = await safeFetchJson("/api/team");
        renderTeam();
      } catch (err) {
        $("teamMembersCard").innerHTML = renderErrorBanner("加载失败：" + err.message);
      }
    }

    function renderTeam() {
      const data = state.teamPayload || {};
      $("teamGenAt").textContent = "更新于 " + (data.generated_at || "-");

      const tenants = data.tenants || [];
      $("teamTenantCard").innerHTML = tenants.length
        ? '<div class="j-section-title">所属团队</div>' + table(
            [{label:"团队名称"}, {label:"JD 账号"}, {label:"拥有者"}, {label:"创建时间"}, {label:"邀请码"}],
            tenants.map(t => '<tr>'
              + '<td>' + esc(t.name) + '</td>'
              + '<td>' + esc(t.jdAccount) + '</td>'
              + '<td>' + esc(t.ownerName) + '</td>'
              + '<td>' + esc(t.createTime || "-") + '</td>'
              + '<td><code style="font-size:12px">' + esc(t.inviteKey || "-") + '</code></td>'
            + '</tr>'))
        : "";

      if (data.fetch_error && !(data.members || []).length) {
        $("teamMembersCard").innerHTML = renderErrorBanner(data.fetch_error);
        return;
      }

      const q = ($("teamSearch").value || "").trim().toLowerCase();
      const members = (data.members || []).filter(m => {
        if (!q) return true;
        return (
          (m.userId || "").toLowerCase().includes(q)
          || (m.nickname || "").toLowerCase().includes(q)
          || (m.roleLabel || "").toLowerCase().includes(q)
          || (m.statusLabel || "").toLowerCase().includes(q)
        );
      });

      const rows = members.map(m => '<tr>'
        + '<td>' + esc(m.userId) + '</td>'
        + '<td>' + esc(m.nickname) + '</td>'
        + '<td>' + esc(m.roleLabel) + '</td>'
        + '<td>' + esc(m.statusLabel) + '</td>'
        + '<td>' + esc(m.joinTime || "-") + '</td>'
        + '<td>' + esc(m.applyTime || "-") + '</td>'
        + '<td>' + esc(m.remark || "") + '</td>'
      + '</tr>');

      $("teamMembersCard").innerHTML =
        '<div class="j-section-title">成员（' + members.length + '/' + (data.members || []).length + '）</div>' +
        table([
          {label:"账号 (PIN)"}, {label:"昵称"}, {label:"角色"}, {label:"状态"},
          {label:"加入时间"}, {label:"申请时间"}, {label:"备注"}
        ], rows);
    }

    /* ============================================================
       View 5: resource management (model / plugin / MCP)
       ============================================================ */
    state.resTab = "model";
    state.resSelected = null;

    async function loadResources() {
      try {
        state.resPayload = await safeFetchJson("/api/resources");
        renderResources();
      } catch (err) {
        $("resourcesContent").innerHTML = renderErrorBanner("加载失败：" + err.message);
      }
    }

    function selectResourceTab(name) {
      state.resTab = name;
      state.resSelected = null;
      document.querySelectorAll("#resourcesTabs .j-tab").forEach(b =>
        b.classList.toggle("active", b.dataset.restab === name));
      renderResources();
    }

    function _humanTokens(v) {
      if (v == null || v === "") return "-";
      const n = Number(v);
      if (!isFinite(n)) return esc(v);
      if (n >= 1000) return (n / 1000).toLocaleString("zh-CN", { maximumFractionDigits: 1 }) + " 千";
      return n.toLocaleString("zh-CN");
    }

    function renderResources() {
      const data = state.resPayload || {};
      $("resourcesGenAt").textContent = "更新于 " + (data.generated_at || "-");
      const groups = data.groups || {};
      const tab = state.resTab || "model";
      const group = groups[tab] || { items: [], available: false, attempts: [] };

      if (data.fetch_error && !group.items.length) {
        $("resourcesContent").innerHTML = renderErrorBanner(data.fetch_error);
        $("resourceDetail").innerHTML = "";
        return;
      }

      if (tab !== "model" && !group.available && !group.items.length) {
        const tried = (group.attempts || []).map(a =>
          '<tr><td><code style="font-size:12px">' + esc(a.url) + '</code></td>'
          + '<td class="num">' + esc(a.code == null ? "-" : a.code) + '</td>'
          + '<td>' + esc(a.msg || "") + '</td></tr>'
        ).join("");
        $("resourcesContent").innerHTML =
          '<div class="j-empty" style="background:#fffbeb;color:#b45309;border-radius:8px;padding:14px">'
          + '当前账号未在 JoyAgent 平台开放该子分类的接口。下面是后端按候选顺序探测的明细：'
          + '</div>'
          + (tried
              ? '<details open style="margin-top:10px"><summary class="muted" style="cursor:pointer">探测明细（' + (group.attempts || []).length + ' 个候选）</summary>'
                + table([{label:"接口"}, {label:"code", num:true}, {label:"msg"}], (group.attempts || []).map(a =>
                  '<tr><td><code style="font-size:12px">' + esc(a.url) + '</code></td>'
                  + '<td class="num">' + esc(a.code == null ? "-" : a.code) + '</td>'
                  + '<td>' + esc(a.msg || "") + '</td></tr>'))
                + '</details>'
              : "");
        $("resourceDetail").innerHTML = "";
        return;
      }

      const q = ($("resSearch").value || "").trim().toLowerCase();
      const items = (group.items || []).filter(it => {
        if (!q) return true;
        return ((it.label || "") + " " + (it.type || "") + " " + (it.description || ""))
          .toLowerCase().includes(q);
      });

      const headers = [
        {label: "资源名称", width: "25%"},
        {label: "类型", width: "8%"},
        {label: "资源池使用情况 (千Tokens)", width: "45%"},
        {label: "已分配成员数", width: "14%", num: true},
        {label: "操作", width: "10%"},
      ];

      const rows = items.map((it, i) => {
        const used = Number(it.usedTotalTokens || 0);
        const max = Number(it.maxTotalTokens || 0);
        const pct = (max > 0 && used >= 0) ? Math.min(100, Math.round(used / max * 1000) / 10) : null;
        const meterFill = pct == null ? 0 : pct;
        let meterText;
        if (max > 0) {
          const usedThousands = (used / 1000).toLocaleString("zh-CN", {maximumFractionDigits: 1});
          const maxThousands = (max / 1000).toLocaleString("zh-CN", {maximumFractionDigits: 0}) + " 千";
          meterText = esc(usedThousands) + " / " + esc(maxThousands);
          if (pct != null) meterText += " (" + pct.toFixed(1) + "%)";
        } else {
          meterText = "上限 -";
        }
        const rowId = esc(String(it.id == null ? i : it.id));
        return '<tr class="j-clickable-row ' + (state.resSelected == it.id ? "selected" : "") + '" data-res-id="' + rowId + '">'
          + '<td><div class="j-res-cell">'
          +   (it.avatar ? '<img class="j-res-avatar-sm" src="' + esc(it.avatar) + '" alt="" onerror="this.remove()" />' : '')
          +   '<div><div class="j-res-name">' + esc(it.label || "-") + '</div>'
          +   '<div class="muted" style="font-size:12px">' + esc((it.description || "").slice(0, 60)) + '</div></div></div></td>'
          + '<td><span class="j-tag">' + esc(it.type || "-") + '</span></td>'
          + '<td>'
          +   '<div class="j-res-meter"><div class="j-res-meter-fill" style="width:' + meterFill + '%"></div></div>'
          +   '<div class="muted" style="font-size:12px;margin-top:4px">' + meterText + '</div>'
          + '</td>'
          + '<td class="num">' + (it.memberCount != null ? fmtInt(it.memberCount) : "-") + '</td>'
          + '<td><button class="j-btn" data-res-detail="' + rowId + '">查看</button></td>'
        + '</tr>';
      });

      const colgroup = '<colgroup>' + headers.map(h => '<col style="width:' + h.width + '" />').join("") + '</colgroup>';
      const head = '<thead><tr>' + headers.map(h =>
        '<th class="' + (h.num ? "num" : "") + '">' + esc(h.label) + '</th>').join("") + '</tr></thead>';
      const body = rows.length
        ? '<tbody>' + rows.join("") + '</tbody>'
        : '<tbody><tr><td colspan="' + headers.length + '" class="j-empty">暂无数据</td></tr></tbody>';
      $("resourcesContent").innerHTML = '<div class="j-table-wrap"><div class="j-scroll">'
        + '<table class="j-table">' + colgroup + head + body + '</table></div></div>';

      $("resourcesContent").querySelectorAll("[data-res-detail]").forEach(btn => {
        btn.addEventListener("click", e => {
          e.stopPropagation();
          state.resSelected = btn.dataset.resDetail;
          renderResourceDetail(items.find(x => String(x.id || "") === btn.dataset.resDetail) || items[Number(btn.dataset.resDetail)]);
        });
      });
      $("resourcesContent").querySelectorAll(".j-clickable-row").forEach(row => {
        row.addEventListener("click", () => {
          state.resSelected = row.dataset.resId;
          renderResourceDetail(items.find(x => String(x.id || "") === row.dataset.resId) || items[Number(row.dataset.resId)]);
        });
      });
      if (!state.resSelected) $("resourceDetail").innerHTML = "";
      else {
        const it = items.find(x => String(x.id || "") === String(state.resSelected));
        if (it) renderResourceDetail(it);
      }
    }

    function renderResourceDetail(it) {
      if (!it) { $("resourceDetail").innerHTML = ""; return; }
      const stats = [];
      if (it.maxTotalTokens) stats.push('<span class="j-tag">最大 token: ' + fmtInt(it.maxTotalTokens) + '</span>');
      if (it.respMaxTokens) stats.push('<span class="j-tag">回复上限: ' + fmtInt(it.respMaxTokens) + '</span>');
      if (it.temperature != null) stats.push('<span class="j-tag">temperature: ' + esc(it.temperature) + '</span>');
      if (it.usedTotalTokens != null) stats.push('<span class="j-tag">已用: ' + _humanTokens(it.usedTotalTokens) + ' Tokens</span>');
      if (it.memberCount != null) stats.push('<span class="j-tag">已分配成员: ' + fmtInt(it.memberCount) + '</span>');
      $("resourceDetail").innerHTML = '<div class="j-user-block">'
        + '<div class="j-user-title">'
        +   '<strong>' + esc(it.label || "-") + '</strong>'
        +   '<span class="muted">类型 ' + esc(it.type || "-") + '</span>'
        + '</div>'
        + (it.description ? '<div class="muted" style="font-size:13px;margin-bottom:8px">' + esc(it.description) + '</div>' : '')
        + '<div class="j-tag-row">' + (stats.length ? stats.join("") : '<span class="muted">无更多元数据</span>') + '</div>'
      + '</div>';
    }

    /* ============================================================
       View 6: cost statistics
       ============================================================ */
    async function loadCost() {
      try {
        state.dashPayload = await safeFetchJson("/api/dashboard");
        renderCost();
      } catch (err) {
        $("costMonthlyTable").innerHTML = renderErrorBanner("加载失败：" + err.message);
      }
    }

    function _todayStr() { return new Date().toISOString().slice(0, 10); }
    function _monthStr(d) { return d.toISOString().slice(0, 7); }
    function _addDays(d, n) { const x = new Date(d.getTime()); x.setDate(x.getDate() + n); return x; }

    function renderCost() {
      const data = state.dashPayload || {};
      $("costGenAt").textContent = "更新于 " + (data.generated_at || "-");

      const billRecords = data.bill_records || [];
      const billsDaily = data.bills_daily || [];
      const billsByMonth = data.bills_by_month || [];
      const billTotal = data.bill_total || {rows: 0, amount: 0};

      const jdTotal = data.jdcloud_total || {rows: 0, actual_fee: 0, bill_fee: 0, erase_fee: 0};
      const jdByMonth = data.jdcloud_by_month || [];
      const jdByDay = data.jdcloud_by_day || [];
      const jdByRes = data.jdcloud_by_resource || [];
      const jdRecords = data.jdcloud_records || [];
      const hasJD = (jdTotal.rows || 0) > 0;

      const today = new Date();
      const thisMonth = _monthStr(today);
      const lastMonthDate = new Date(today.getFullYear(), today.getMonth() - 1, 1);
      const lastMonth = _monthStr(lastMonthDate);

      // historical_bills aggregations
      const billMonthMap = new Map();
      billsByMonth.forEach(m => billMonthMap.set(m.month, m));
      const billDayMap = new Map();
      billsDaily.forEach(r => {
        billDayMap.set(r.day, (billDayMap.get(r.day) || 0) + Number(r.amount || 0));
      });

      // jdcloud aggregations
      const jdMonthMap = new Map();
      jdByMonth.forEach(m => jdMonthMap.set(m.month, m));
      const jdDayMap = new Map();
      jdByDay.forEach(r => jdDayMap.set(r.day, Number(r.actual_fee || 0)));

      // primary metric source: prefer JD Cloud (authoritative) when available
      const usingJD = hasJD;
      const dayMap = usingJD ? jdDayMap : billDayMap;

      const curJD = jdMonthMap.get(thisMonth) || {actual_fee: 0, rows: 0, bill_fee: 0, erase_fee: 0};
      const prevJD = jdMonthMap.get(lastMonth) || {actual_fee: 0, rows: 0, bill_fee: 0, erase_fee: 0};
      const curBill = billMonthMap.get(thisMonth) || {amount: 0, rows: 0};
      const prevBill = billMonthMap.get(lastMonth) || {amount: 0, rows: 0};

      // daily average
      const since = _addDays(today, -29);
      const sinceStr = since.toISOString().slice(0, 10);
      let activeDays = 0, activeSum = 0;
      dayMap.forEach((amt, day) => {
        if (day >= sinceStr && day <= _todayStr()) { activeDays += 1; activeSum += Number(amt || 0); }
      });
      const dailyAvg = activeDays > 0 ? activeSum / activeDays : 0;

      // top metric cards
      if (usingJD) {
        $("costTotal").textContent = fmtMoney(jdTotal.actual_fee || 0);
        $("costTotalFoot").textContent = "JD Cloud · " + fmtInt(jdTotal.rows) + " 笔 · 抹零 " + fmtMoney(jdTotal.erase_fee || 0);
        $("costThisMonth").textContent = fmtMoney(curJD.actual_fee || 0);
        $("costThisMonthFoot").textContent = thisMonth + " · " + fmtInt(curJD.rows) + " 笔 · 原价 " + fmtMoney(curJD.bill_fee || 0);
        $("costLastMonth").textContent = fmtMoney(prevJD.actual_fee || 0);
        $("costLastMonthFoot").textContent = lastMonth + " · " + fmtInt(prevJD.rows) + " 笔 · 原价 " + fmtMoney(prevJD.bill_fee || 0);
      } else {
        $("costTotal").textContent = fmtMoney(billTotal.amount || 0);
        $("costTotalFoot").textContent = "本地账单 · " + fmtInt(billTotal.rows) + " 笔（无 JD Cloud 数据）";
        $("costThisMonth").textContent = fmtMoney(curBill.amount || 0);
        $("costThisMonthFoot").textContent = thisMonth + " · " + fmtInt(curBill.rows) + " 笔";
        $("costLastMonth").textContent = fmtMoney(prevBill.amount || 0);
        $("costLastMonthFoot").textContent = lastMonth + " · " + fmtInt(prevBill.rows) + " 笔";
      }
      $("costDailyAvg").textContent = fmtMoney(dailyAvg);
      $("costDailyAvgFoot").textContent = activeDays + " 天有扣费 · 共 " + fmtMoney(activeSum)
        + " · 数据源: " + (usingJD ? "JD Cloud" : "本地账单");

      const badge = $("costSourceBadge");
      const hint = $("costSourceHint");
      if (badge && hint) {
        if (usingJD) {
          badge.textContent = "三方对账 · JD Cloud 已接入 (" + fmtInt(jdTotal.rows) + " 条明细)";
          hint.textContent = "数据源：JD Cloud describeBillDetails (优先) · 本地 historical_bills · 本地按 token 计算 usage_records";
        } else {
          badge.textContent = "二方对账 · JD Cloud 未接入";
          hint.textContent = "JD Cloud 数据缺失，先运行 `python joyagent_monitor.py --login-jdcloud` 然后 `--import-jdcloud --month YYYY-MM`";
        }
      }

      // ---- 3-way monthly reconciliation: JD Cloud / 本地账单 / 计算 floor ----
      const calcMonthMap = new Map();
      (data.user_daily || []).forEach(r => {
        const m = String(r.day || "").slice(0, 7);
        if (!m) return;
        const acc = calcMonthMap.get(m) || {raw: 0, floor: 0, tokens: 0};
        acc.raw += Number(r.cny_raw || 0);
        acc.floor += Number(r.cny_floor || 0);
        acc.tokens += Number(r.tokens || 0);
        calcMonthMap.set(m, acc);
      });
      const months = new Set([
        ...jdMonthMap.keys(),
        ...billMonthMap.keys(),
        ...calcMonthMap.keys(),
      ]);
      const monthRows = Array.from(months).filter(Boolean).sort().reverse().map(m => {
        const jd = jdMonthMap.get(m) || {actual_fee: 0, rows: 0, bill_fee: 0, erase_fee: 0};
        const bill = billMonthMap.get(m) || {amount: 0, rows: 0};
        const calc = calcMonthMap.get(m) || {raw: 0, floor: 0, tokens: 0};
        const diffJDvsBill = Number(jd.actual_fee || 0) - Number(bill.amount || 0);
        const diffJDvsCalc = Number(jd.actual_fee || 0) - Number(calc.floor || 0);
        const cls1 = Math.abs(diffJDvsBill) < 0.01 ? "ok" : (diffJDvsBill > 0.01 ? "warn" : "bad");
        const cls2 = Math.abs(diffJDvsCalc) < 0.5 ? "ok" : (diffJDvsCalc > 0.5 ? "warn" : "bad");
        return '<tr>'
          + '<td>' + esc(m) + '</td>'
          + '<td class="num">' + fmtMoney(jd.actual_fee || 0) + '</td>'
          + '<td class="num"><span class="muted" style="font-size:12px">原价 ' + fmtMoney(jd.bill_fee || 0) + ' · 抹零 ' + fmtMoney(jd.erase_fee || 0) + '</span><br/>' + fmtInt(jd.rows) + ' 笔</td>'
          + '<td class="num">' + fmtMoney(bill.amount || 0) + '</td>'
          + '<td class="num">' + fmtInt(bill.rows) + '</td>'
          + '<td class="num">' + fmtMoney(calc.floor || 0) + '</td>'
          + '<td class="num"><span class="' + cls1 + '">' + (diffJDvsBill >= 0 ? '+' : '') + fmtMoney(diffJDvsBill, 4) + '</span></td>'
          + '<td class="num"><span class="' + cls2 + '">' + (diffJDvsCalc >= 0 ? '+' : '') + fmtMoney(diffJDvsCalc, 4) + '</span></td>'
        + '</tr>';
      });
      $("costMonthlyTable").innerHTML = table([
        {label: "月份"},
        {label: "JD Cloud 实付", num: true},
        {label: "JD Cloud 详情", num: true},
        {label: "本地账单 应付", num: true},
        {label: "本地账单 笔数", num: true},
        {label: "本地计算 floor", num: true},
        {label: "JD - 本地账单", num: true},
        {label: "JD - 计算 floor", num: true},
      ], monthRows);

      // ---- per-resource ranking (prefer JD Cloud) ----
      let resTitle, resHeaders, resRows;
      if (usingJD) {
        const tot = jdByRes.reduce((s, r) => s + Number(r.actual_fee || 0), 0);
        resRows = jdByRes.map(r => {
          const pct = tot > 0 ? (Number(r.actual_fee || 0) / tot * 100) : 0;
          return '<tr>'
            + '<td>' + esc(r.resource || "-") + '<div class="muted" style="font-size:12px">' + esc(r.service_code || '') + '</div></td>'
            + '<td class="num">' + fmtInt(r.rows) + '</td>'
            + '<td class="num">' + fmtMoney(r.actual_fee || 0) + '</td>'
            + '<td><div class="j-res-meter"><div class="j-res-meter-fill" style="width:' + pct.toFixed(1) + '%"></div></div>'
            +   '<div class="muted" style="font-size:12px;margin-top:4px">' + pct.toFixed(1) + '%</div></td>'
          + '</tr>';
        });
        resHeaders = [{label:"JD Cloud 资源/服务"}, {label:"笔数", num:true}, {label:"实付", num:true}, {label:"占比"}];
      } else {
        const resMap = new Map();
        billsDaily.forEach(r => {
          const k = r.resource || "-";
          const acc = resMap.get(k) || {amount: 0, rows: 0};
          acc.amount += Number(r.amount || 0);
          acc.rows += Number(r.rows || 0);
          resMap.set(k, acc);
        });
        const tot = Array.from(resMap.values()).reduce((s, x) => s + x.amount, 0);
        resRows = Array.from(resMap.entries()).sort((a, b) => b[1].amount - a[1].amount).map(([k, v]) => {
          const pct = tot > 0 ? (v.amount / tot * 100) : 0;
          return '<tr>'
            + '<td>' + esc(k) + '</td>'
            + '<td class="num">' + fmtInt(v.rows) + '</td>'
            + '<td class="num">' + fmtMoney(v.amount) + '</td>'
            + '<td><div class="j-res-meter"><div class="j-res-meter-fill" style="width:' + pct.toFixed(1) + '%"></div></div>'
            +   '<div class="muted" style="font-size:12px;margin-top:4px">' + pct.toFixed(1) + '%</div></td>'
          + '</tr>';
        });
        resHeaders = [{label:"资源 / 模型"}, {label:"笔数", num:true}, {label:"扣费", num:true}, {label:"占比"}];
      }
      $("costResourceTable").innerHTML = table(resHeaders, resRows);

      // ---- last 30 day trend bar chart ----
      const trendDays = [];
      for (let i = 29; i >= 0; i -= 1) trendDays.push(_addDays(today, -i).toISOString().slice(0, 10));
      const trendValues = trendDays.map(d => Number(dayMap.get(d) || 0));
      const trendMax = Math.max(0.01, ...trendValues);
      const bars = trendDays.map((day, i) => {
        const v = trendValues[i];
        const h = Math.round((v / trendMax) * 100);
        return '<div class="j-bar" title="' + esc(day) + ' · ' + fmtMoney(v) + '">'
          + '<div class="j-bar-fill" style="height:' + h + '%"></div>'
        + '</div>';
      }).join("");
      const labels = '<div class="j-bar-axis"><span>' + esc(trendDays[0])
                  + '</span><span>' + esc(trendDays[14])
                  + '</span><span>' + esc(trendDays[29]) + '</span></div>';
      $("costTrend").innerHTML = '<div class="j-bar-chart">' + bars + '</div>' + labels
        + '<div class="muted" style="font-size:12px;margin-top:6px">'
        + '近 30 天合计 ' + fmtMoney(trendValues.reduce((s, x) => s + x, 0))
        + ' · 最高单日 ' + fmtMoney(trendMax === 0.01 ? 0 : trendMax)
        + ' · 数据源 ' + (usingJD ? "JD Cloud actualFee" : "本地账单 cny_amount")
        + '</div>';

      // ---- recent records: prefer JD Cloud detail records ----
      let recHeaders, recRows;
      if (usingJD) {
        recRows = jdRecords.slice(0, 100).map(r => '<tr>'
          + '<td>' + esc(r.bill_time || "-") + '</td>'
          + '<td>' + esc(r.bill_date || "-") + '</td>'
          + '<td>' + esc(r.service_code_name || r.resource_name || "-") + '</td>'
          + '<td class="num">' + fmtMoney(r.bill_fee || 0, 4) + '</td>'
          + '<td class="num">' + fmtMoney(r.actual_fee || 0) + '</td>'
          + '<td class="num">' + fmtMoney(r.erase_fee || 0, 4) + '</td>'
          + '<td>' + esc(r.region_name || "") + '</td>'
        + '</tr>');
        recHeaders = [
          {label:"扣费时间"}, {label:"账期"}, {label:"服务/资源"},
          {label:"原价 billFee", num:true}, {label:"实付 actualFee", num:true}, {label:"抹零 eraseFee", num:true},
          {label:"地域"}
        ];
      } else {
        const records = (billRecords || []).slice(0, 100);
        recRows = records.map(r => '<tr>'
          + '<td>' + esc(r.charge_time) + '</td>'
          + '<td>' + esc(r.day) + '</td>'
          + '<td>' + esc(r.resource || "-") + '</td>'
          + '<td class="num">' + fmtMoney(r.amount) + '</td>'
          + '<td>' + esc(r.source || "") + '</td>'
        + '</tr>');
        recHeaders = [{label:"扣费时间"}, {label:"日期"}, {label:"资源 / 模型"}, {label:"金额", num:true}, {label:"来源"}];
      }
      $("costRecordsTable").innerHTML = table(recHeaders, recRows);
    }

    /* ============================================================
       Auto refresh
       ============================================================ */
    function resetTimer() {
      if (state.timer) clearInterval(state.timer);
      const v = Number($("refreshInterval").value);
      if (v > 0) {
        state.timer = setInterval(() => {
          const loaders = {
            usage: loadProfile,
            local: loadDashboard,
            keys: loadKeys,
            team: loadTeam,
            resources: loadResources,
            cost: loadCost,
          };
          const fn = loaders[state.currentView] || loadDashboard;
          fn();
        }, v);
      }
    }

    /* ============================================================
       Wiring
       ============================================================ */
    document.querySelectorAll(".j-nav[data-view]").forEach(btn => {
      btn.addEventListener("click", () => selectView(btn.dataset.view));
    });
    document.querySelectorAll(".j-tab").forEach(btn => {
      btn.addEventListener("click", () => selectTab(btn.dataset.tab));
    });

    $("usageMonth").value = state.profile.month;
    $("usageMonth").addEventListener("change", e => {
      state.profile.month = e.target.value || new Date().toISOString().slice(0, 7);
      state.profile.pageNo = 1;
      loadProfile();
    });
    $("usageResource").addEventListener("change", e => {
      state.profile.resource = e.target.value || "";
      state.profile.pageNo = 1;
      loadProfile();
    });
    $("usagePageSize").addEventListener("change", e => {
      state.profile.pageSize = Number(e.target.value) || 10;
      state.profile.pageNo = 1;
      loadProfile();
    });
    $("usagePrev").addEventListener("click", () => {
      if (state.profile.pageNo > 1) { state.profile.pageNo -= 1; loadProfile(); }
    });
    $("usageNext").addEventListener("click", () => {
      state.profile.pageNo += 1;
      loadProfile();
    });
    $("usageClear").addEventListener("click", () => {
      state.profile.month = new Date().toISOString().slice(0, 7);
      state.profile.resource = "";
      state.profile.pageNo = 1;
      $("usageMonth").value = state.profile.month;
      $("usageResource").value = "";
      loadProfile();
    });
    $("usageRefresh").addEventListener("click", loadProfile);
    $("usageExport").addEventListener("click", exportUsageCsv);

    $("reload").addEventListener("click", loadDashboard);
    $("keysRefresh").addEventListener("click", loadKeys);
    $("teamRefresh").addEventListener("click", loadTeam);
    $("teamSearch").addEventListener("input", () => state.teamPayload && renderTeam());
    $("resourcesRefresh").addEventListener("click", loadResources);
    $("resSearch").addEventListener("input", () => state.resPayload && renderResources());
    document.querySelectorAll("#resourcesTabs .j-tab").forEach(btn =>
      btn.addEventListener("click", () => selectResourceTab(btn.dataset.restab)));
    $("costRefresh").addEventListener("click", loadCost);
    $("refreshInterval").addEventListener("change", resetTimer);
    $("search").addEventListener("input", () => state.dashPayload && renderDashboard(state.dashPayload));
    $("userSelect").addEventListener("change", e => {
      state.selectedUserKey = decUser(e.target.value || "");
      if (state.dashPayload) renderDashboard(state.dashPayload);
      const detail = $("userDetail");
      if (detail) detail.scrollIntoView({ behavior: "smooth", block: "start" });
    });

    /* Initial load: usage view + a background dashboard fetch so the
       sidebar metric reflects local DB even before we open it. */
    loadProfile();
    loadDashboard();
    resetTimer();
  </script>
</body>
</html>
"""


_CONNECTION_CLOSED_ERRORS = (
    ConnectionAbortedError,
    ConnectionResetError,
    BrokenPipeError,
    TimeoutError,
)


class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        return

    def log_error(self, fmt: str, *args) -> None:
        return

    def handle_one_request(self) -> None:
        # The default implementation logs noisy tracebacks when the client
        # closes the socket in the middle of a write. Just swallow those.
        try:
            super().handle_one_request()
        except _CONNECTION_CLOSED_ERRORS:
            self.close_connection = True

    def _try_send(self, status: int, headers: list[tuple[str, str]], body: bytes) -> bool:
        try:
            self.send_response(status)
            for name, value in headers:
                self.send_header(name, value)
            self.end_headers()
            if body:
                self.wfile.write(body)
            return True
        except _CONNECTION_CLOSED_ERRORS:
            # Client went away while we were writing. Drop silently so we
            # don't try to push a 500 response on the same dead socket.
            self.close_connection = True
            return False

    def send_json(self, payload: dict, status: int = 200) -> bool:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        return self._try_send(status, [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(body))),
            ("Cache-Control", "no-store"),
        ], body)

    def send_html(self, html: str) -> bool:
        body = html.encode("utf-8")
        return self._try_send(200, [
            ("Content-Type", "text/html; charset=utf-8"),
            ("Content-Length", str(len(body))),
            ("Cache-Control", "no-store"),
        ], body)

    def _serve(self, builder) -> None:
        try:
            payload = builder()
        except _CONNECTION_CLOSED_ERRORS:
            self.close_connection = True
            return
        except Exception as exc:
            # Best-effort 500 response; ignore further connection errors.
            self.send_json({"error": str(exc)}, status=500)
            return
        self.send_json(payload)

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            if path == "/":
                self.send_html(INDEX_HTML)
                return
            if path == "/api/dashboard":
                self._serve(build_dashboard_payload)
                return
            if path == "/api/team":
                self._serve(build_team_payload)
                return
            if path == "/api/resources":
                self._serve(build_resources_payload)
                return
            if path == "/api/keys":
                self._serve(build_keys_payload)
                return
            if path == "/api/profile":
                self._serve(lambda: build_profile_payload(parse_qs(parsed.query)))
                return
            if path == "/api/health":
                self.send_json({"ok": True, "state": get_state()})
                return
            self.send_json({"error": "not found"}, status=404)
        except _CONNECTION_CLOSED_ERRORS:
            # Final catch-all: client closed the socket. Swallow.
            self.close_connection = True


def run_server(host: str, port: int, months: list[str], poll: bool, interval: int, open_browser: bool) -> None:
    update_state(
        started_at=datetime.now().isoformat(timespec="seconds"),
        months=months,
        polling_enabled=poll,
    )
    if poll:
        thread = threading.Thread(target=poll_loop, args=(months, interval), daemon=True)
        thread.start()

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    url = f"http://{host}:{port}/"
    print(f"Dashboard: {url}")
    print(f"Database:  {DB_PATH}")
    print(f"Polling:   {'enabled' if poll else 'disabled'} / interval={interval}s / months={', '.join(months)}")
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="JoyAgent billing web dashboard")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL)
    parser.add_argument("--month", action="append", default=None)
    parser.add_argument("--no-poll", action="store_true", help="Only read existing SQLite data.")
    parser.add_argument("--no-open", action="store_true", help="Do not open the browser automatically.")
    parser.add_argument("--skip-import", action="store_true", help="Do not import local CSV/TSV bill files on startup.")
    args = parser.parse_args()

    if not args.skip_import:
        import_default_bills_glob()

    months = normalize_months(args.month)
    run_server(
        host=args.host,
        port=args.port,
        months=months,
        poll=not args.no_poll,
        interval=args.interval,
        open_browser=not args.no_open,
    )


if __name__ == "__main__":
    main()
