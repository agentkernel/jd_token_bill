"""
JoyAgent usage monitor.

Headless background watcher for
    https://joyagent.jd.com/pl/profile?tab=usageStatistics

For every poll we capture:
  - usage rows: call_time, account PIN, username, model, token type, tokens
  - account balance (best-effort scan of XHR responses + DOM text)

All raw rows go into a SQLite database. Whenever a row is new or its
token count grew, we print an itemized RMB cost delta on the console.

Setup
-----
    python -m pip install -r requirements_monitor.txt
    python -m playwright install chromium

First run (interactive login, persists cookies into ./joyagent_profile):
    python joyagent_monitor.py --login

Background monitor (default 10s polling, headless):
    python joyagent_monitor.py
    python joyagent_monitor.py --interval 30 --show-browser
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import csv
import urllib.parse

try:
    from playwright.sync_api import BrowserContext, Page, Response, sync_playwright
except ImportError:
    print("Missing dependency: playwright")
    print("    pip install -r requirements_monitor.txt")
    print("    python -m playwright install chromium")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROFILE_URL = "https://joyagent.jd.com/pl/profile?tab=usageStatistics"
USERINFO_API = "https://agentrs.jd.com/api/saas/user/v2/userInfo"
USAGE_API = "https://agentrs.jd.com/api/saas/billing-statistics/page"
BALANCE_API = "https://agentrs.jd.com/api/saas/coupon/v1/getUserAmount"

WORKSPACE_DIR = Path(__file__).resolve().parent
DB_PATH = WORKSPACE_DIR / "joyagent_usage.db"
USER_DATA_DIR = WORKSPACE_DIR / "joyagent_profile"
DEFAULT_INTERVAL = 10

EXCHANGE_RATE = Decimal("7")

# USD per 1M tokens. Extend this map when new models appear in the bill.
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

# Map every token-type label (Chinese page text or English API field) to a
# canonical key used in PRICING_USD_PER_M.
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
# Pricing helpers
# ---------------------------------------------------------------------------

def canonical_token_type(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return TOKEN_TYPE_ALIASES.get(text.lower()) or TOKEN_TYPE_ALIASES.get(text)


def cny_amount(model: str, token_type: Any, tokens: int) -> Decimal:
    """tokens / 1,000,000 * USD * exchange_rate, rounded to 4 decimals."""
    if not model or token_type is None or tokens is None:
        return Decimal("0")
    canon = canonical_token_type(token_type)
    if canon is None:
        return Decimal("0")
    usd = PRICING_USD_PER_M.get(model, {}).get(canon)
    if usd is None:
        return Decimal("0")
    raw = Decimal(int(tokens)) / Decimal("1000000") * usd * EXCHANGE_RATE
    return raw.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def floor_to_cent(amount: Decimal) -> Decimal:
    """Match the JoyAgent bill behaviour: floor every record to one cent."""
    return amount.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS usage_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call_time TEXT NOT NULL,
    account_pin TEXT,
    username TEXT,
    model TEXT NOT NULL,
    token_type_raw TEXT NOT NULL,
    token_type_canonical TEXT,
    tokens INTEGER NOT NULL,
    cny_amount REAL NOT NULL,
    cny_amount_floor REAL NOT NULL,
    row_key TEXT NOT NULL UNIQUE,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS usage_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    detected_at TEXT NOT NULL,
    usage_record_id INTEGER NOT NULL,
    prev_tokens INTEGER NOT NULL,
    new_tokens INTEGER NOT NULL,
    delta_tokens INTEGER NOT NULL,
    delta_cny REAL NOT NULL,
    FOREIGN KEY (usage_record_id) REFERENCES usage_records(id)
);

CREATE TABLE IF NOT EXISTS balance_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    balance_cny REAL NOT NULL,
    delta_cny REAL NOT NULL,
    cash_cny REAL,
    voucher_cny REAL,
    credits INTEGER,
    breakdown_json TEXT,
    source TEXT
);

CREATE TABLE IF NOT EXISTS raw_responses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    url TEXT NOT NULL,
    body_hash TEXT NOT NULL UNIQUE,
    body TEXT
);

CREATE TABLE IF NOT EXISTS historical_bills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    charge_time TEXT NOT NULL,
    charge_date TEXT NOT NULL,
    resource_type TEXT,
    resource TEXT,
    cny_amount REAL NOT NULL,
    source TEXT,
    row_hash TEXT NOT NULL UNIQUE,
    imported_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_historical_bills_date ON historical_bills(charge_date);
CREATE INDEX IF NOT EXISTS idx_historical_bills_resource ON historical_bills(resource);

-- Per-record billing details pulled from
-- https://billing-console.jdcloud.com/openApi/billingbills/describeBillDetails
-- These are the authoritative JD Cloud-side bills and are joined against
-- historical_bills (CSV/paste imports) and usage_records (token-derived costs)
-- for triangulated reconciliation.
CREATE TABLE IF NOT EXISTS jdcloud_bills (
    bill_id TEXT PRIMARY KEY,
    bill_date TEXT NOT NULL,         -- YYYY-MM
    bill_time TEXT,                  -- YYYY-MM-DD HH:MM:SS
    time_range TEXT,
    resource_id TEXT,
    resource_name TEXT,
    app_code TEXT,
    app_code_name TEXT,
    service_code TEXT,
    service_code_name TEXT,
    billing_type INTEGER,
    billing_type_name TEXT,
    bill_type INTEGER,
    bill_type_name TEXT,
    consume_type INTEGER,
    consume_type_name TEXT,
    region TEXT,
    region_name TEXT,
    bill_fee REAL,
    discount_fee REAL,
    actual_fee REAL,
    cash_pay_fee REAL,
    cash_coupon_fee REAL,
    arrear_fee REAL,
    erase_fee REAL,
    formula TEXT,
    pin TEXT,
    payer_id TEXT,
    user_id TEXT,
    raw_json TEXT,
    imported_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jdcloud_bills_date ON jdcloud_bills(bill_date);
CREATE INDEX IF NOT EXISTS idx_jdcloud_bills_resource ON jdcloud_bills(resource_id);
CREATE INDEX IF NOT EXISTS idx_jdcloud_bills_service ON jdcloud_bills(service_code);
"""


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(DB_SCHEMA)
    # Forward-compatible migration for older DBs created before the breakdown columns existed.
    cur = conn.execute("PRAGMA table_info(balance_snapshots)")
    cols = {row["name"] for row in cur.fetchall()}
    for column, decl in (
        ("cash_cny", "REAL"),
        ("voucher_cny", "REAL"),
        ("credits", "INTEGER"),
        ("breakdown_json", "TEXT"),
    ):
        if column not in cols:
            conn.execute(f"ALTER TABLE balance_snapshots ADD COLUMN {column} {decl}")
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Extraction from network JSON
# ---------------------------------------------------------------------------

def _extract_usage_dicts(node: Any, out: list[dict]) -> None:
    if isinstance(node, dict):
        keys = {k.lower() for k in node.keys() if isinstance(k, str)}
        looks_like_row = (
            any(k in keys for k in ("model", "modelname", "modelid"))
            and any(k in keys for k in ("tokens", "tokencount", "usage", "amount"))
            and any(k in keys for k in ("tokentype", "type", "token_type"))
        )
        if looks_like_row:
            out.append(node)
        for v in node.values():
            _extract_usage_dicts(v, out)
    elif isinstance(node, list):
        for v in node:
            _extract_usage_dicts(v, out)


def normalize_row(raw: dict) -> dict | None:
    def pick(*names: str) -> Any:
        for name in names:
            for key, value in raw.items():
                if isinstance(key, str) and key.lower() == name.lower():
                    return value
        return None

    model = pick("model", "modelName", "modelId")
    token_type = pick("tokenType", "type", "token_type")
    tokens = pick("tokens", "tokenCount", "usage", "amount", "value", "count")
    call_time = pick("callTime", "time", "createdAt", "datetime", "date", "callDate")
    account_pin = pick("accountInfo", "accountPin", "pin", "accountId")
    username = pick("username", "userName", "user", "nickName")

    if model is None or token_type is None or tokens is None:
        return None

    try:
        tokens_int = int(str(tokens).replace(",", "").strip())
    except (ValueError, TypeError):
        return None

    return {
        "call_time": str(call_time) if call_time is not None else "",
        "account_pin": str(account_pin) if account_pin is not None else "",
        "username": str(username) if username is not None else "",
        "model": str(model),
        "token_type_raw": str(token_type),
        "tokens": tokens_int,
    }


_BALANCE_KEYS = {
    "balance",
    "accountbalance",
    "remaining",
    "remainingamount",
    "totalamount",
    "totalbalance",
    "amount",
    "yue",
}


def extract_balance(node: Any) -> Decimal | None:
    """Walk a JSON tree and pick the first numeric balance-looking value."""
    candidates: list[Decimal] = []

    def visit(n: Any, path: tuple[str, ...]) -> None:
        if isinstance(n, dict):
            for k, v in n.items():
                if isinstance(k, str) and k.lower() in _BALANCE_KEYS and isinstance(v, (int, float, str)):
                    try:
                        candidates.append(Decimal(str(v).replace(",", "")))
                    except Exception:
                        pass
                visit(v, path + (str(k),))
        elif isinstance(n, list):
            for v in n:
                visit(v, path)

    visit(node, ())
    return candidates[0] if candidates else None


# ---------------------------------------------------------------------------
# Extraction from rendered DOM
# ---------------------------------------------------------------------------

DOM_USAGE_JS = r"""
() => {
  const tables = Array.from(document.querySelectorAll('table'));
  const result = [];
  for (const table of tables) {
    const headerCells = Array.from(
      table.querySelectorAll('thead th, thead td, tr:first-child th, tr:first-child td')
    ).map(c => c.innerText.trim());
    if (!headerCells.length) continue;
    const idx = {
      time:   headerCells.findIndex(h => h.includes('\u8c03\u7528\u65f6\u95f4') || /time/i.test(h)),
      pin:    headerCells.findIndex(h => h.includes('\u8d26\u6237\u4fe1\u606f') || /pin/i.test(h)),
      user:   headerCells.findIndex(h => h.includes('\u7528\u6237\u540d') || /user/i.test(h)),
      model:  headerCells.findIndex(h => h.includes('\u6a21\u578b') || /model/i.test(h)),
      type:   headerCells.findIndex(h => h.includes('Token\u7c7b\u578b') || /type/i.test(h)),
      tokens: headerCells.findIndex(h => h.includes('\u4f7f\u7528\u91cf') || /token/i.test(h)),
    };
    if (idx.model < 0 || idx.type < 0 || idx.tokens < 0) continue;
    const bodyRows = table.querySelectorAll('tbody tr');
    for (const row of bodyRows) {
      const cells = Array.from(row.querySelectorAll('td')).map(c => c.innerText.trim());
      if (!cells.length) continue;
      result.push({
        call_time:      idx.time   >= 0 ? cells[idx.time]   : '',
        account_pin:    idx.pin    >= 0 ? cells[idx.pin]    : '',
        username:       idx.user   >= 0 ? cells[idx.user]   : '',
        model:          cells[idx.model] || '',
        token_type_raw: cells[idx.type] || '',
        tokens:         cells[idx.tokens] || '',
      });
    }
  }
  return result;
}
"""

# Each entry: canonical key -> (regex pattern, value parser)
# Patterns target labels exactly as they appear on
# https://joyagent.jd.com/pl/profile?tab=usageStatistics
ACCOUNT_AMOUNT_PATTERNS: list[tuple[str, re.Pattern[str], str]] = [
    (
        "total",
        re.compile(r"\u8d26\u6237\u603b\u989d\s*[:\uff1a]?\s*[\u00a5\uffe5]\s*([0-9,]+(?:\.[0-9]+)?)"),
        "cny",
    ),
    (
        "cash",
        re.compile(r"(?<![\u8d26\u6237])\u4f59\u989d\s*[:\uff1a]?\s*[\u00a5\uffe5]\s*([0-9,]+(?:\.[0-9]+)?)"),
        "cny",
    ),
    (
        "voucher",
        re.compile(r"\u4ee3\u91d1\u5238\s*[:\uff1a]?\s*[\u00a5\uffe5]\s*([0-9,]+(?:\.[0-9]+)?)"),
        "cny",
    ),
    (
        "credits",
        re.compile(r"\u5269\u4f59\u79ef\u5206\s*[:\uff1a]?\s*([0-9,]+)"),
        "int",
    ),
]


def extract_account_amounts_from_text(text: str) -> dict[str, Any]:
    """Return {'total': Decimal, 'cash': Decimal, 'voucher': Decimal, 'credits': int} where matched."""
    out: dict[str, Any] = {}
    if not text:
        return out
    for key, pattern, kind in ACCOUNT_AMOUNT_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        raw = match.group(1).replace(",", "")
        try:
            if kind == "cny":
                out[key] = Decimal(raw)
            else:
                out[key] = int(raw)
        except Exception:
            continue
    return out


def extract_usage_from_dom(page: Page) -> list[dict]:
    rows = page.evaluate(DOM_USAGE_JS) or []
    out: list[dict] = []
    for r in rows:
        if not r.get("model"):
            continue
        try:
            r["tokens"] = int(str(r["tokens"]).replace(",", "").replace("\uff0c", "").strip())
        except (ValueError, TypeError):
            continue
        out.append(r)
    return out


def extract_account_amounts_from_dom(page: Page) -> dict[str, Any]:
    text = page.evaluate("() => document.body ? document.body.innerText : ''") or ""
    return extract_account_amounts_from_text(text)


def extract_balance_from_dom(page: Page) -> Decimal | None:
    amounts = extract_account_amounts_from_dom(page)
    return amounts.get("total") if isinstance(amounts.get("total"), Decimal) else None


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def _row_key(raw: dict) -> str:
    src = "|".join([
        raw.get("call_time", ""),
        raw.get("account_pin", ""),
        raw.get("username", ""),
        raw["model"],
        raw["token_type_raw"],
    ])
    return hashlib.sha1(src.encode("utf-8")).hexdigest()


def upsert_usage(conn: sqlite3.Connection, rows: list[dict], detected_at: str) -> list[dict]:
    changes: list[dict] = []
    for raw in rows:
        canonical = canonical_token_type(raw["token_type_raw"])
        cny = cny_amount(raw["model"], raw["token_type_raw"], raw["tokens"])
        cny_floor = floor_to_cent(cny)
        row_key = _row_key(raw)

        cur = conn.execute(
            "SELECT id, tokens, cny_amount FROM usage_records WHERE row_key = ?",
            (row_key,),
        )
        existing = cur.fetchone()

        if existing is None:
            cur = conn.execute(
                """
                INSERT INTO usage_records (
                    call_time, account_pin, username, model,
                    token_type_raw, token_type_canonical, tokens,
                    cny_amount, cny_amount_floor, row_key,
                    first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    raw.get("call_time", ""),
                    raw.get("account_pin", ""),
                    raw.get("username", ""),
                    raw["model"],
                    raw["token_type_raw"],
                    canonical,
                    raw["tokens"],
                    float(cny),
                    float(cny_floor),
                    row_key,
                    detected_at,
                    detected_at,
                ),
            )
            usage_id = cur.lastrowid
            conn.execute(
                """INSERT INTO usage_changes (
                    detected_at, usage_record_id,
                    prev_tokens, new_tokens, delta_tokens, delta_cny
                ) VALUES (?, ?, ?, ?, ?, ?)""",
                (detected_at, usage_id, 0, raw["tokens"], raw["tokens"], float(cny)),
            )
            changes.append({
                **raw,
                "kind": "new",
                "delta_tokens": raw["tokens"],
                "delta_cny": float(cny),
                "delta_cny_floor": float(cny_floor),
                "canonical": canonical,
            })
        else:
            prev_tokens = existing["tokens"]
            new_tokens = raw["tokens"]
            if new_tokens != prev_tokens:
                delta_tokens = new_tokens - prev_tokens
                if delta_tokens > 0:
                    delta_cny_dec = cny_amount(raw["model"], raw["token_type_raw"], delta_tokens)
                else:
                    delta_cny_dec = Decimal(str(float(cny) - existing["cny_amount"]))
                conn.execute(
                    """UPDATE usage_records
                          SET tokens = ?, cny_amount = ?, cny_amount_floor = ?, last_seen_at = ?
                        WHERE id = ?""",
                    (new_tokens, float(cny), float(cny_floor), detected_at, existing["id"]),
                )
                conn.execute(
                    """INSERT INTO usage_changes (
                        detected_at, usage_record_id,
                        prev_tokens, new_tokens, delta_tokens, delta_cny
                    ) VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        detected_at,
                        existing["id"],
                        prev_tokens,
                        new_tokens,
                        delta_tokens,
                        float(delta_cny_dec),
                    ),
                )
                changes.append({
                    **raw,
                    "kind": "updated",
                    "delta_tokens": delta_tokens,
                    "delta_cny": float(delta_cny_dec),
                    "delta_cny_floor": float(floor_to_cent(delta_cny_dec)),
                    "canonical": canonical,
                })
            else:
                conn.execute(
                    "UPDATE usage_records SET last_seen_at = ? WHERE id = ?",
                    (detected_at, existing["id"]),
                )
    conn.commit()
    return changes


def upsert_balance(
    conn: sqlite3.Connection,
    amounts: dict[str, Any] | Decimal | None,
    detected_at: str,
    source: str,
) -> dict | None:
    """Persist account amounts. `amounts` may be the breakdown dict from
    extract_account_amounts_from_dom, or a single Decimal (legacy: total only)."""
    if amounts is None:
        return None
    if isinstance(amounts, Decimal):
        amounts = {"total": amounts}
    if not isinstance(amounts, dict):
        return None

    total = amounts.get("total")
    cash = amounts.get("cash")
    voucher = amounts.get("voucher")
    credits = amounts.get("credits")
    if total is None and cash is not None and voucher is not None:
        try:
            total = Decimal(str(cash)) + Decimal(str(voucher))
        except Exception:
            total = None
    if total is None:
        return None

    cur = conn.execute(
        "SELECT balance_cny, cash_cny, voucher_cny, credits "
        "FROM balance_snapshots ORDER BY id DESC LIMIT 1"
    )
    last = cur.fetchone()
    new_total = float(total)
    delta = new_total - (last["balance_cny"] if last else new_total)
    cash_f = float(cash) if cash is not None else None
    voucher_f = float(voucher) if voucher is not None else None
    credits_i = int(credits) if credits is not None else None

    same_as_last = (
        last is not None
        and abs(delta) < 1e-6
        and (last["cash_cny"] is None or cash_f is None or abs(last["cash_cny"] - cash_f) < 1e-6)
        and (last["voucher_cny"] is None or voucher_f is None or abs(last["voucher_cny"] - voucher_f) < 1e-6)
        and (last["credits"] is None or credits_i is None or last["credits"] == credits_i)
    )
    if same_as_last:
        return None

    breakdown = {k: (str(v) if isinstance(v, Decimal) else v) for k, v in amounts.items()}
    conn.execute(
        "INSERT INTO balance_snapshots (captured_at, balance_cny, delta_cny, "
        "cash_cny, voucher_cny, credits, breakdown_json, source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            detected_at,
            new_total,
            delta,
            cash_f,
            voucher_f,
            credits_i,
            json.dumps(breakdown, ensure_ascii=False),
            source,
        ),
    )
    conn.commit()
    return {
        "balance": new_total,
        "delta": delta,
        "cash": cash_f,
        "voucher": voucher_f,
        "credits": credits_i,
    }


def _bill_row_hash(charge_time: str, amount: Decimal | float | str) -> str:
    amt = Decimal(str(amount)).quantize(Decimal("0.000001"))
    return hashlib.sha1(f"{charge_time}|{amt}".encode("utf-8")).hexdigest()


def import_bills_text(conn: sqlite3.Connection, path: Path) -> tuple[int, int]:
    """Import a TSV/text bill listing with columns: 调用时间, 资源类型, 资源, 消费金额(¥)."""
    inserted = 0
    seen = 0
    text = path.read_text(encoding="utf-8")
    detected_at = datetime.now().isoformat(timespec="seconds")
    src_name = f"text:{path.name}"
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("\u8c03\u7528\u65f6\u95f4"):  # 调用时间 header
            continue
        parts = [p.strip() for p in line.split("\t") if p.strip() != ""]
        if len(parts) < 4:
            parts = [p.strip() for p in line.split() if p.strip() != ""]
        if len(parts) < 4:
            continue
        charge_time = parts[0]
        if len(parts[0].split()) == 2:
            charge_time = parts[0]
        else:
            charge_time = parts[0] + " " + parts[1]
        if len(charge_time.split(" ")) != 2:
            continue
        resource_type = parts[-3]
        resource = parts[-2]
        amount_str = parts[-1].lstrip("\u00a5\uffe5$").replace(",", "").strip()
        try:
            amount = Decimal(amount_str)
        except Exception:
            continue
        seen += 1
        if amount < 0:
            continue
        try:
            charge_date = charge_time.split(" ")[0]
        except Exception:
            charge_date = charge_time
        row_hash = _bill_row_hash(charge_time, amount)
        cur = conn.execute(
            """INSERT OR REPLACE INTO historical_bills
               (charge_time, charge_date, resource_type, resource,
                cny_amount, source, row_hash, imported_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                charge_time,
                charge_date,
                resource_type if resource_type != "-" else None,
                resource if resource != "-" else None,
                float(amount),
                src_name,
                row_hash,
                detected_at,
            ),
        )
        if cur.rowcount > 0:
            inserted += 1
    conn.commit()
    return inserted, seen


def import_bills_csv(conn: sqlite3.Connection, path: Path) -> tuple[int, int]:
    """Import 京东 JoyAgent 明细账单 CSV. Model name is not in the CSV; rows imported
    with model=NULL but will be back-filled by a later text import (matched by
    charge_time + amount via row_hash)."""
    inserted = 0
    seen = 0
    detected_at = datetime.now().isoformat(timespec="seconds")
    src_name = f"csv:{path.name}"
    with path.open("r", encoding="gb18030", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)
        except StopIteration:
            return 0, 0
        for row in reader:
            if not row or len(row) < 25:
                continue
            charge_time = (row[12] or "").strip().strip("\t").strip()
            if not charge_time:
                continue
            try:
                payable = Decimal((row[19] or "0").strip() or "0")
            except Exception:
                continue
            seen += 1
            if payable < 0:
                continue
            charge_date = charge_time.split(" ")[0] if " " in charge_time else charge_time
            resource_id = (row[2] or "").strip()
            resource_type = "\u6a21\u578b" if "joyagentrs" in resource_id.lower() else None
            row_hash = _bill_row_hash(charge_time, payable)
            cur = conn.execute(
                """INSERT OR IGNORE INTO historical_bills
                   (charge_time, charge_date, resource_type, resource,
                    cny_amount, source, row_hash, imported_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    charge_time,
                    charge_date,
                    resource_type,
                    None,
                    float(payable),
                    src_name,
                    row_hash,
                    detected_at,
                ),
            )
            if cur.rowcount > 0:
                inserted += 1
    conn.commit()
    return inserted, seen


def import_bills_auto(conn: sqlite3.Connection, path: Path) -> tuple[int, int]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return import_bills_csv(conn, path)
    return import_bills_text(conn, path)


# ---------------------------------------------------------------------------
# JD Cloud billing console importer
# ---------------------------------------------------------------------------

JDCLOUD_BILL_DETAILS_URL = "https://billing-console.jdcloud.com/openApi/billingbills/describeBillDetails"
JDCLOUD_BILL_SUMMARY_URL = "https://billing-console.jdcloud.com/openApi/billingbills/describeBillSummary"


def _build_jdcloud_url(base: str, params: dict[str, Any]) -> str:
    payload = json.dumps(params, ensure_ascii=False, separators=(",", ":"))
    ts = int(time.time() * 1000)
    return f"{base}?_t={ts}&params={urllib.parse.quote(payload, safe='')}"


def fetch_jdcloud_bill_summary(page: Page, dt_month: str) -> dict | None:
    params = {
        "currency": "CNY",
        "ignoreZero": 0,
        "queryTimeType": "month",
        "currencySymbol": "\u00a5",
        "billStartDate": dt_month,
        "billEndDate": dt_month,
        "memberPin": "",
    }
    url = _build_jdcloud_url(JDCLOUD_BILL_SUMMARY_URL, params)
    data = _page_fetch(page, url)
    if not isinstance(data, dict):
        return None
    return (data.get("result") or {}).get("result") if "result" in (data.get("result") or {}) else data.get("result")


def fetch_jdcloud_bill_details(page: Page, dt_month: str, page_size: int = 100) -> tuple[list[dict], str | None]:
    """Paginate through every bill detail row for one month. Returns (rows, error)."""
    rows: list[dict] = []
    page_no = 1
    while True:
        params = {
            "pageSize": page_size,
            "pageIndex": page_no,
            "currency": "CNY",
            "ignoreZero": 0,
            "queryTimeType": "month",
            "currencySymbol": "\u00a5",
            "billStartDate": dt_month,
            "billEndDate": dt_month,
            "statisticalItem": 1,
            "statisticalPeriod": 1,
            "memberPin": "",
        }
        url = _build_jdcloud_url(JDCLOUD_BILL_DETAILS_URL, params)
        data = _page_fetch(page, url)
        if not isinstance(data, dict):
            return rows, "no JSON response"
        if data.get("error"):
            return rows, str(data.get("error"))
        result = data.get("result") or {}
        records = result.get("records") or result.get("data") or result.get("list") or []
        if not isinstance(records, list):
            return rows, "unexpected result shape"
        rows.extend(records)
        total = result.get("totalCount") or result.get("total")
        try:
            total_int = int(total) if total is not None else None
        except (TypeError, ValueError):
            total_int = None
        if total_int is not None and len(rows) >= total_int:
            break
        if len(records) < page_size:
            break
        page_no += 1
        if page_no > 500:
            return rows, "stopped at 500 pages (safety)"
    return rows, None


def _ensure_jdcloud_session(page: Page) -> tuple[bool, str | None]:
    """Navigate the page to the billing console so subsequent fetch() calls
    are same-origin. Detect if we got bounced back to the login page."""
    try:
        page.goto(JDCLOUD_BILLING_URL, wait_until="domcontentloaded", timeout=60_000)
    except Exception as exc:
        return False, f"goto failed: {exc}"
    try:
        page.wait_for_load_state("networkidle", timeout=10_000)
    except Exception:
        pass
    cur = page.url or ""
    if "login.jdcloud.com" in cur or "passport.jd.com" in cur:
        return False, f"redirected to login: {cur}"
    return True, None


def _insert_jdcloud_record(conn: sqlite3.Connection, raw: dict, detected_at: str) -> int:
    bill_id = raw.get("billId")
    if not bill_id:
        return 0
    cur = conn.execute(
        """INSERT OR REPLACE INTO jdcloud_bills (
            bill_id, bill_date, bill_time, time_range,
            resource_id, resource_name, app_code, app_code_name,
            service_code, service_code_name,
            billing_type, billing_type_name, bill_type, bill_type_name,
            consume_type, consume_type_name, region, region_name,
            bill_fee, discount_fee, actual_fee, cash_pay_fee,
            cash_coupon_fee, arrear_fee, erase_fee,
            formula, pin, payer_id, user_id, raw_json, imported_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            str(bill_id),
            str(raw.get("billDate") or ""),
            str(raw.get("billTime") or ""),
            str(raw.get("timeRange") or ""),
            raw.get("resourceId"),
            raw.get("resourceName"),
            raw.get("appCode"),
            raw.get("appCodeName"),
            raw.get("serviceCode"),
            raw.get("serviceCodeName"),
            raw.get("billingType"),
            raw.get("billingTypeName"),
            raw.get("billType"),
            raw.get("billTypeName"),
            raw.get("consumeType"),
            raw.get("consumeTypeName"),
            raw.get("region"),
            raw.get("regionName"),
            float(raw.get("billFee") or 0),
            float(raw.get("discountFee") or 0),
            float(raw.get("actualFee") or 0),
            float(raw.get("cashPayFee") or 0),
            float(raw.get("cashCouponFee") or 0),
            float(raw.get("arrearFee") or 0),
            float(raw.get("eraseFee") or 0),
            raw.get("formula"),
            raw.get("pin"),
            str(raw.get("payerId") or ""),
            str(raw.get("userId") or ""),
            json.dumps(raw, ensure_ascii=False),
            detected_at,
        ),
    )
    return cur.rowcount


def import_jdcloud_bills(conn: sqlite3.Connection, page: Page, months: list[str], page_size: int = 100) -> dict:
    ok, err = _ensure_jdcloud_session(page)
    if not ok:
        return {"ok": False, "error": err, "months": {}}
    results: dict[str, dict] = {}
    detected_at = datetime.now().isoformat(timespec="seconds")
    for month in months:
        summary = fetch_jdcloud_bill_summary(page, month)
        rows, err = fetch_jdcloud_bill_details(page, month, page_size=page_size)
        inserted = 0
        for r in rows:
            try:
                inserted += _insert_jdcloud_record(conn, r, detected_at)
            except sqlite3.Error:
                pass
        conn.commit()
        results[month] = {
            "fetched": len(rows),
            "inserted": inserted,
            "summary": summary,
            "error": err,
        }
    return {"ok": True, "error": None, "months": results}


def store_raw_response(conn: sqlite3.Connection, url: str, body: str, detected_at: str) -> None:
    digest = hashlib.sha1(body.encode("utf-8", errors="ignore")).hexdigest()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO raw_responses (captured_at, url, body_hash, body) "
            "VALUES (?, ?, ?, ?)",
            (detected_at, url, digest, body),
        )
        conn.commit()
    except sqlite3.Error:
        pass


# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------

def print_change(change: dict) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    sign = "+" if change["delta_cny"] >= 0 else ""
    print(
        f"[{ts}] {change['kind'].upper():7} "
        f"{change['model']:24} "
        f"{change['token_type_raw']:35} "
        f"\u0394tokens={change['delta_tokens']:>10}  "
        f"\u0394cost={sign}\u00a5{change['delta_cny']:.4f}  "
        f"floor={sign}\u00a5{change['delta_cny_floor']:.2f}  "
        f"call_time={change['call_time']} pin={change['account_pin']}"
    )


def print_balance_change(change: dict) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    sign = "+" if change["delta"] >= 0 else ""
    parts = [
        f"\u8d26\u6237\u603b\u989d=\u00a5{change['balance']:.2f}",
        f"delta={sign}\u00a5{change['delta']:.2f}",
    ]
    if change.get("cash") is not None:
        parts.append(f"\u4f59\u989d=\u00a5{change['cash']:.2f}")
    if change.get("voucher") is not None:
        parts.append(f"\u4ee3\u91d1\u5238=\u00a5{change['voucher']:.2f}")
    if change.get("credits") is not None:
        parts.append(f"\u5269\u4f59\u79ef\u5206={change['credits']}")
    print(f"[{ts}] BALANCE  " + "  ".join(parts))


def _visual_width(text: str) -> int:
    """Treat CJK / fullwidth glyphs as width 2 for terminal alignment."""
    width = 0
    for ch in text:
        code = ord(ch)
        if (
            0x1100 <= code <= 0x115F
            or 0x2E80 <= code <= 0x303E
            or 0x3041 <= code <= 0x33FF
            or 0x3400 <= code <= 0x4DBF
            or 0x4E00 <= code <= 0x9FFF
            or 0xA000 <= code <= 0xA4CF
            or 0xAC00 <= code <= 0xD7A3
            or 0xF900 <= code <= 0xFAFF
            or 0xFE30 <= code <= 0xFE4F
            or 0xFF00 <= code <= 0xFF60
            or 0xFFE0 <= code <= 0xFFE6
        ):
            width += 2
        else:
            width += 1
    return width


def _pad(text: str, width: int, align: str = "left", max_visual: int | None = 240) -> str:
    text = "" if text is None else str(text)
    if max_visual is not None and _visual_width(text) > max_visual:
        # Defensive truncation only for absurd values; normal columns are fine.
        text = text[: max_visual - 3] + "..."
    pad = max(0, width - _visual_width(text))
    if align == "right":
        return " " * pad + text
    if align == "center":
        left = pad // 2
        return " " * left + text + " " * (pad - left)
    return text + " " * pad


def current_balance(conn: sqlite3.Connection) -> float | None:
    snap = current_balance_snapshot(conn)
    return None if snap is None else snap.get("total")


def current_balance_snapshot(conn: sqlite3.Connection) -> dict | None:
    cur = conn.execute(
        "SELECT balance_cny, cash_cny, voucher_cny, credits, captured_at "
        "FROM balance_snapshots ORDER BY id DESC LIMIT 1"
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "total": float(row["balance_cny"]) if row["balance_cny"] is not None else None,
        "cash": float(row["cash_cny"]) if row["cash_cny"] is not None else None,
        "voucher": float(row["voucher_cny"]) if row["voucher_cny"] is not None else None,
        "credits": int(row["credits"]) if row["credits"] is not None else None,
        "captured_at": row["captured_at"],
    }


SNAPSHOT_COLUMNS = [
    ("\u8c03\u7528\u65f6\u95f4",       19, "left"),   # 调用时间
    ("\u8d26\u6237\u4fe1\u606f(PIN)",  20, "left"),   # 账户信息(PIN)
    ("\u7528\u6237\u540d",             12, "left"),   # 用户名
    ("\u6a21\u578b",                   24, "left"),   # 模型
    ("Token\u7c7b\u578b",              35, "left"),   # Token类型
    ("\u4f7f\u7528\u91cf(Tokens)",     14, "right"),  # 使用量(Tokens)
    ("\u8d39\u7528",                   12, "right"),  # 费用
    ("\u5b9e\u65f6\u4f59\u989d",       12, "right"),  # 实时余额
]


def print_full_snapshot(conn: sqlite3.Connection, title: str = "JoyAgent bill snapshot") -> None:
    cur = conn.execute(
        """SELECT call_time, account_pin, username, model,
                  token_type_raw, tokens, cny_amount, cny_amount_floor
             FROM usage_records
            ORDER BY call_time DESC, model, token_type_raw"""
    )
    rows = cur.fetchall()
    snap = current_balance_snapshot(conn) or {}
    total = snap.get("total")
    bal_text = f"\u00a5{total:.2f}" if total is not None else "N/A"

    breakdown_bits = []
    if total is not None:
        breakdown_bits.append(f"\u8d26\u6237\u603b\u989d=\u00a5{total:.2f}")
    if snap.get("cash") is not None:
        breakdown_bits.append(f"\u4f59\u989d=\u00a5{snap['cash']:.2f}")
    if snap.get("voucher") is not None:
        breakdown_bits.append(f"\u4ee3\u91d1\u5238=\u00a5{snap['voucher']:.2f}")
    if snap.get("credits") is not None:
        breakdown_bits.append(f"\u5269\u4f59\u79ef\u5206={snap['credits']}")
    breakdown_line = "   ".join(breakdown_bits) if breakdown_bits else "\u8d26\u6237\u4fe1\u606f\u672a\u83b7\u53d6"

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    total_width = sum(w for _, w, _ in SNAPSHOT_COLUMNS) + 3 * (len(SNAPSHOT_COLUMNS) - 1)
    bar = "=" * total_width
    print(f"\n{bar}")
    print(f"{title}  @ {ts}   rows={len(rows)}")
    print(f"\u8d26\u6237: {breakdown_line}")
    print(bar)

    header = "   ".join(_pad(name, w, align) for name, w, align in SNAPSHOT_COLUMNS)
    print(header)
    print("-" * total_width)

    if not rows:
        print("(no usage records yet)")
        print(bar + "\n")
        return

    grand_raw = 0.0
    grand_floor = 0.0
    for r in rows:
        cny = float(r["cny_amount"])
        grand_raw += cny
        grand_floor += float(r["cny_amount_floor"])
        cells = [
            (_pad(r["call_time"] or "",                 SNAPSHOT_COLUMNS[0][1], SNAPSHOT_COLUMNS[0][2])),
            (_pad(r["account_pin"] or "",               SNAPSHOT_COLUMNS[1][1], SNAPSHOT_COLUMNS[1][2])),
            (_pad(r["username"] or "",                  SNAPSHOT_COLUMNS[2][1], SNAPSHOT_COLUMNS[2][2])),
            (_pad(r["model"] or "",                     SNAPSHOT_COLUMNS[3][1], SNAPSHOT_COLUMNS[3][2])),
            (_pad(r["token_type_raw"] or "",            SNAPSHOT_COLUMNS[4][1], SNAPSHOT_COLUMNS[4][2])),
            (_pad(f"{int(r['tokens']):,}",              SNAPSHOT_COLUMNS[5][1], SNAPSHOT_COLUMNS[5][2])),
            (_pad(f"\u00a5{cny:.4f}",                   SNAPSHOT_COLUMNS[6][1], SNAPSHOT_COLUMNS[6][2])),
            (_pad(bal_text,                             SNAPSHOT_COLUMNS[7][1], SNAPSHOT_COLUMNS[7][2])),
        ]
        print("   ".join(cells))

    print("-" * total_width)
    print(
        f"\u5408\u8ba1\u8d39\u7528: \u00a5{grand_raw:.4f}  "
        f"(\u6309 floor: \u00a5{grand_floor:.2f})   "
        f"\u5b9e\u65f6\u4f59\u989d: {bal_text}"
    )
    print(f"{breakdown_line}")

    print_per_user_summary(conn, total_width)
    print_per_user_daily(conn, total_width)
    print_bills_daily(conn, total_width)

    print(bar + "\n")


PER_USER_COLUMNS = [
    ("\u8d26\u6237\u4fe1\u606f(PIN)",  20, "left"),   # 账户信息(PIN)
    ("\u7528\u6237\u540d",             12, "left"),   # 用户名
    ("\u603b tokens",                  16, "right"),  # 总 tokens
    ("\u8d39\u7528(raw)",              12, "right"),  # 费用(raw)
    ("\u8d39\u7528(floor)",            12, "right"),  # 费用(floor)
    ("\u5404\u6a21\u578b\u8d39\u7528", 60, "left"),   # 各模型费用
]


def print_per_user_summary(conn: sqlite3.Connection, hint_width: int = 0) -> None:
    cur = conn.execute(
        """SELECT account_pin,
                  username,
                  model,
                  SUM(tokens)            AS tokens,
                  SUM(cny_amount)        AS cny,
                  SUM(cny_amount_floor)  AS cny_floor
             FROM usage_records
            GROUP BY account_pin, username, model
            ORDER BY account_pin, username, model"""
    )
    rows = cur.fetchall()
    if not rows:
        return

    by_user: dict[tuple[str, str], list[sqlite3.Row]] = {}
    for r in rows:
        key = (r["account_pin"] or "", r["username"] or "")
        by_user.setdefault(key, []).append(r)

    sub_width = sum(w for _, w, _ in PER_USER_COLUMNS) + 3 * (len(PER_USER_COLUMNS) - 1)
    width = max(sub_width, hint_width)

    print()
    print("-" * width)
    print("\u5404\u7528\u6237\u7d2f\u8ba1\u6d88\u8017")  # 各用户累计消耗
    print("-" * width)
    print("   ".join(_pad(name, w, align) for name, w, align in PER_USER_COLUMNS))
    print("-" * width)

    grand_tokens = 0
    grand_raw = 0.0
    grand_floor = 0.0
    sorted_users = sorted(
        by_user.items(),
        key=lambda kv: -sum(float(r["cny"]) for r in kv[1]),
    )
    for (pin, user), items in sorted_users:
        total_tokens = sum(int(r["tokens"]) for r in items)
        total_raw = sum(float(r["cny"]) for r in items)
        total_floor = sum(float(r["cny_floor"]) for r in items)
        grand_tokens += total_tokens
        grand_raw += total_raw
        grand_floor += total_floor
        breakdown = "  ".join(
            f"{r['model']}=\u00a5{float(r['cny']):.2f}" for r in items
        )
        cells = [
            _pad(pin,                                PER_USER_COLUMNS[0][1], PER_USER_COLUMNS[0][2]),
            _pad(user,                               PER_USER_COLUMNS[1][1], PER_USER_COLUMNS[1][2]),
            _pad(f"{total_tokens:,}",                PER_USER_COLUMNS[2][1], PER_USER_COLUMNS[2][2]),
            _pad(f"\u00a5{total_raw:.4f}",           PER_USER_COLUMNS[3][1], PER_USER_COLUMNS[3][2]),
            _pad(f"\u00a5{total_floor:.2f}",         PER_USER_COLUMNS[4][1], PER_USER_COLUMNS[4][2]),
            breakdown,  # last column: full text, no padding/truncation
        ]
        print("   ".join(cells))

    print("-" * width)
    summary_cells = [
        _pad("\u5408\u8ba1",                         PER_USER_COLUMNS[0][1], PER_USER_COLUMNS[0][2]),  # 合计
        _pad("",                                     PER_USER_COLUMNS[1][1], PER_USER_COLUMNS[1][2]),
        _pad(f"{grand_tokens:,}",                    PER_USER_COLUMNS[2][1], PER_USER_COLUMNS[2][2]),
        _pad(f"\u00a5{grand_raw:.4f}",               PER_USER_COLUMNS[3][1], PER_USER_COLUMNS[3][2]),
        _pad(f"\u00a5{grand_floor:.2f}",             PER_USER_COLUMNS[4][1], PER_USER_COLUMNS[4][2]),
        "",
    ]
    print("   ".join(summary_cells))


def print_per_user_daily(conn: sqlite3.Connection, hint_width: int = 0) -> None:
    """Per (account_pin, username): for each day, show per-model 费用 plus daily total."""
    cur = conn.execute(
        """SELECT account_pin,
                  username,
                  SUBSTR(call_time, 1, 10) AS dt,
                  model,
                  SUM(tokens) AS tokens,
                  SUM(cny_amount) AS cny,
                  SUM(cny_amount_floor) AS cny_floor
             FROM usage_records
            WHERE call_time IS NOT NULL AND call_time != ''
            GROUP BY account_pin, username, dt, model
            ORDER BY account_pin, username, dt DESC, model"""
    )
    rows = cur.fetchall()
    if not rows:
        return

    by_user: dict[tuple[str, str], dict[str, dict[str, dict[str, float]]]] = {}
    user_totals: dict[tuple[str, str], dict[str, float]] = {}
    all_models: list[str] = []
    seen_models: set[str] = set()
    for r in rows:
        key = (r["account_pin"] or "", r["username"] or "")
        dt = r["dt"]
        model = r["model"]
        if model not in seen_models:
            seen_models.add(model)
            all_models.append(model)
        by_user.setdefault(key, {}).setdefault(dt, {})[model] = {
            "tokens": int(r["tokens"]),
            "cny": float(r["cny"]),
            "cny_floor": float(r["cny_floor"]),
        }
        ut = user_totals.setdefault(key, {"raw": 0.0, "floor": 0.0, "tokens": 0})
        ut["raw"] += float(r["cny"])
        ut["floor"] += float(r["cny_floor"])
        ut["tokens"] += int(r["tokens"])

    all_models.sort()

    width = hint_width if hint_width else 0
    sub_width = 13 + 18 * len(all_models) + 14 + 14
    width = max(width, sub_width)

    print()
    print("=" * width)
    print("\u5404\u7528\u6237\u6309\u5929\u660e\u7ec6\uff08usage_records\uff0c\u6309 raw \u8d39\u7528\u6392\u5e8f\uff09")  # 各用户按天明细
    print("=" * width)

    sorted_users = sorted(user_totals.items(), key=lambda kv: -kv[1]["raw"])
    for (pin, user), totals in sorted_users:
        days_map = by_user[(pin, user)]
        sorted_days = sorted(days_map.keys(), reverse=True)
        header_cells = [_pad("\u65e5\u671f", 13, "left")]  # 日期
        for m in all_models:
            header_cells.append(_pad(m, 18, "right"))
        header_cells.append(_pad("\u65e5\u8d39\u7528(raw)", 14, "right"))   # 日费用(raw)
        header_cells.append(_pad("\u65e5\u8d39\u7528(floor)", 14, "right")) # 日费用(floor)
        print()
        print(
            f"\u8d26\u6237: {pin or '-':20} \u7528\u6237: {user or '-':12} "
            f"\u603b raw=\u00a5{totals['raw']:.4f} floor=\u00a5{totals['floor']:.2f} "
            f"tokens={totals['tokens']:,}"
        )
        print("-" * width)
        print(" ".join(header_cells))
        print("-" * width)
        for dt in sorted_days:
            row_cells = [_pad(dt, 13, "left")]
            day_raw = 0.0
            day_floor = 0.0
            for m in all_models:
                v = days_map[dt].get(m)
                if v is None:
                    row_cells.append(_pad("-", 18, "right"))
                else:
                    row_cells.append(_pad(f"\u00a5{v['cny']:.4f}", 18, "right"))
                    day_raw += v["cny"]
                    day_floor += v["cny_floor"]
            row_cells.append(_pad(f"\u00a5{day_raw:.4f}", 14, "right"))
            row_cells.append(_pad(f"\u00a5{day_floor:.2f}", 14, "right"))
            print(" ".join(row_cells))


def print_bills_daily(conn: sqlite3.Connection, hint_width: int = 0) -> None:
    """Show daily totals from historical_bills, broken down by resource (model)."""
    has_data = conn.execute("SELECT 1 FROM historical_bills LIMIT 1").fetchone()
    if not has_data:
        return
    cur = conn.execute(
        """SELECT charge_date,
                  COALESCE(resource, '\u672a\u77e5\u6a21\u578b') AS resource,
                  SUM(cny_amount) AS amount,
                  COUNT(*) AS n
             FROM historical_bills
            GROUP BY charge_date, resource
            ORDER BY charge_date DESC, resource"""
    )
    rows = cur.fetchall()
    if not rows:
        return

    by_date: dict[str, list[sqlite3.Row]] = {}
    for r in rows:
        by_date.setdefault(r["charge_date"], []).append(r)

    width = max(hint_width, 90)
    print()
    print("=" * width)
    print("\u5b9e\u9645\u6263\u8d39\u6309\u5929\uff08historical_bills\uff09")  # 实际扣费按天（historical_bills）
    print("=" * width)
    cols = [
        ("\u65e5\u671f",      12, "left"),    # 日期
        ("\u8d44\u6e90\u6a21\u578b",     30, "left"),    # 资源模型
        ("\u8ba1\u8d39\u7b14\u6570",     8,  "right"),   # 计费笔数
        ("\u6263\u8d39(\u00a5)",         12, "right"),   # 扣费(¥)
    ]
    print(" ".join(_pad(name, w, a) for name, w, a in cols))
    print("-" * width)
    grand_total = 0.0
    by_month: dict[str, float] = {}
    for date in sorted(by_date.keys(), reverse=True):
        items = by_date[date]
        date_total = 0.0
        for it in items:
            amount = float(it["amount"])
            date_total += amount
            grand_total += amount
            month = (date or "")[:7]
            by_month[month] = by_month.get(month, 0.0) + amount
            cells = [
                _pad(date or "",            cols[0][1], cols[0][2]),
                _pad(it["resource"] or "",  cols[1][1], cols[1][2]),
                _pad(str(int(it["n"])),     cols[2][1], cols[2][2]),
                _pad(f"\u00a5{amount:.2f}", cols[3][1], cols[3][2]),
            ]
            print(" ".join(cells))
        if len(items) > 1:
            print(
                f"  {_pad('', cols[0][1] - 2, 'left')} {_pad('\u5c0f\u8ba1', cols[1][1], 'right')} "  # 小计
                f"{_pad('', cols[2][1], 'right')} {_pad(f'\u00a5{date_total:.2f}', cols[3][1], 'right')}"
            )
    print("-" * width)
    for month in sorted(by_month.keys(), reverse=True):
        print(
            f"\u6708\u5408\u8ba1 {month}: \u00a5{by_month[month]:.2f}"  # 月合计 YYYY-MM: ¥X.XX
        )
    print(
        f"\u603b\u5408\u8ba1: \u00a5{grand_total:.2f}"  # 总合计: ¥X.XX
    )


# ---------------------------------------------------------------------------
# Browser session helpers
# ---------------------------------------------------------------------------

def _new_context(pw, headless: bool) -> BrowserContext:
    USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
    return pw.chromium.launch_persistent_context(
        user_data_dir=str(USER_DATA_DIR),
        headless=headless,
        viewport={"width": 1280, "height": 900},
        args=["--disable-blink-features=AutomationControlled"],
    )


def is_logged_in(page: Page) -> bool:
    url = page.url or ""
    if any(token in url for token in ("login", "passport", "signin")):
        return False
    return True


def run_inspect(headless: bool, months: list[str] | None) -> None:
    """One-shot diagnostic: call APIs directly + capture XHR + dump everything."""
    print(f"DB: {DB_PATH}")
    print(f"Profile dir: {USER_DATA_DIR}")
    months_resolved = _months_to_query(months)
    print(f"Inspect mode (headless={headless}), months={months_resolved}\n")

    captured: list[tuple[str, int, dict, str]] = []

    with sync_playwright() as pw:
        context = _new_context(pw, headless=headless)
        page = context.new_page()

        def on_response(resp: Response) -> None:
            ctype = ""
            try:
                ctype = (resp.headers or {}).get("content-type", "")
            except Exception:
                pass
            if "json" not in ctype.lower():
                return
            try:
                body = resp.text()
            except Exception:
                return
            try:
                data = json.loads(body)
            except Exception:
                return
            captured.append((resp.url, resp.status, data, body))

        page.on("response", on_response)

        try:
            page.goto(PROFILE_URL, wait_until="domcontentloaded", timeout=60_000)
        except Exception as exc:
            print(f"Initial page open failed: {exc}")

        if not is_logged_in(page):
            print("Not logged in. Sign in inside the open window now and press ENTER.")
            input()
            try:
                page.goto(PROFILE_URL, wait_until="networkidle", timeout=60_000)
            except Exception as exc:
                print(f"Reopen failed: {exc}")
        else:
            try:
                page.wait_for_load_state("networkidle", timeout=30_000)
            except Exception:
                pass

        time.sleep(3)

        print(f"\n=== Final URL: {page.url}")
        print(f"=== Captured JSON responses: {len(captured)}")
        for url, status, data, body in captured:
            buckets: list[dict] = []
            _extract_usage_dicts(data, buckets)
            bal = extract_balance(data)
            top_keys = list(data.keys()) if isinstance(data, dict) else f"<{type(data).__name__}>"
            print(f"  - [{status}] {url}")
            print(f"      top_keys={top_keys}")
            print(f"      usage-like rows in JSON: {len(buckets)}")
            if buckets:
                first = buckets[0]
                print(f"      sample row keys: {list(first.keys())}")
                print(f"      sample row: {json.dumps(first, ensure_ascii=False)[:400]}")
            if bal is not None:
                print(f"      balance candidate from JSON: {bal}")
            preview = body[:500].replace("\n", " ")
            print(f"      body[:500]={preview}")

        print("\n=== DOM tables")
        try:
            tables = page.evaluate(
                "() => Array.from(document.querySelectorAll('table')).map(t => ({"
                "headers: Array.from(t.querySelectorAll('thead th, thead td, tr:first-child th, tr:first-child td')).map(c => c.innerText.trim()),"
                "rows: Array.from(t.querySelectorAll('tbody tr')).slice(0, 5).map(r => Array.from(r.querySelectorAll('td')).map(c => c.innerText.trim())),"
                "row_count: t.querySelectorAll('tbody tr').length"
                "}))"
            )
        except Exception as exc:
            tables = []
            print(f"DOM eval failed: {exc}")
        for i, t in enumerate(tables):
            print(f"  Table #{i}: headers={t.get('headers')}  total_rows={t.get('row_count')}")
            for r in t.get("rows", []):
                print(f"    row: {r}")

        dom_rows = extract_usage_from_dom(page)
        print(f"\n=== Rows extracted via DOM helper: {len(dom_rows)}")
        for r in dom_rows[:10]:
            print(f"  {r}")

        json_rows: list[dict] = []
        for _url, _status, data, _body in captured:
            buckets: list[dict] = []
            _extract_usage_dicts(data, buckets)
            for raw in buckets:
                norm = normalize_row(raw)
                if norm:
                    json_rows.append(norm)
        print(f"\n=== Rows extracted via JSON helper: {len(json_rows)}")
        for r in json_rows[:10]:
            print(f"  {r}")

        chosen = json_rows or dom_rows
        print(f"\n=== Cost preview (using {'JSON' if json_rows else 'DOM'} rows)")
        total = Decimal("0")
        for r in chosen:
            cny = cny_amount(r["model"], r["token_type_raw"], r["tokens"])
            canon = canonical_token_type(r["token_type_raw"]) or "?"
            total += cny
            print(
                f"  {r.get('call_time',''):20} {r.get('account_pin',''):20} "
                f"{r.get('username',''):12} {r['model']:24} {r['token_type_raw']:35}"
                f" tokens={r['tokens']:>10}  canon={canon:14}  \u00a5{cny}"
            )
        print(f"  TOTAL raw cost: \u00a5{total}")

        amounts_dom = extract_account_amounts_from_dom(page)
        bal_json: Decimal | None = None
        for _url, _status, data, _body in captured:
            if bal_json is None:
                bal_json = extract_balance(data)
        print(f"\n=== Account amounts (DOM): {amounts_dom}")
        print(f"=== Balance candidate from generic JSON heuristic: {bal_json}")

        userinfo = fetch_userinfo(page)
        print(f"\n=== Direct userInfo API: {userinfo}")
        amounts_api = fetch_balance_amounts(page)
        print(f"=== Direct balance API:  {amounts_api}")
        print(f"=== Querying months via API: {months_resolved}")
        for month in months_resolved:
            raws = fetch_all_billing_rows(page, month)
            print(f"  {month}: {len(raws)} raw billing rows")
            for r in raws[:3]:
                print(f"    raw: {r}")
            normalized = [normalize_billing_row(r) for r in raws]
            normalized = [r for r in normalized if r]
            print(f"  {month}: {len(normalized)} normalized rows")
            total = Decimal(0)
            for r in normalized[:8]:
                cny = cny_amount(r["model"], r["token_type_raw"], r["tokens"])
                total += cny
                canon = canonical_token_type(r["token_type_raw"]) or "?"
                print(
                    f"    {r['call_time']:20} {r['account_pin']:18} "
                    f"{r['username']:10} {r['model']:24} {r['token_type_raw']:35} "
                    f"tokens={r['tokens']:>10} canon={canon:14} \u00a5{cny}"
                )
            full_total = sum(
                cny_amount(r["model"], r["token_type_raw"], r["tokens"])
                for r in normalized
            )
            print(f"  {month}: full total cost = \u00a5{full_total}")

        try:
            html_path = WORKSPACE_DIR / "inspect_page.html"
            content = page.content()
            html_path.write_text(content, encoding="utf-8")
            print(f"\n=== Full HTML saved to {html_path}")
        except Exception as exc:
            print(f"Failed to save HTML: {exc}")

        try:
            shot_path = WORKSPACE_DIR / "inspect_page.png"
            page.screenshot(path=str(shot_path), full_page=True)
            print(f"=== Screenshot saved to {shot_path}")
        except Exception as exc:
            print(f"Failed to take screenshot: {exc}")

        try:
            context.close()
        except Exception:
            pass


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
    """Run fetch() inside the page so that cookies, referer, CSRF headers all
    match what the JoyAgent web app would send natively."""
    try:
        result = page.evaluate(_FETCH_JS, url)
    except Exception as exc:
        print(f"  page.evaluate fetch failed for {url}: {exc}")
        return None
    if not isinstance(result, dict):
        return None
    if result.get("status") != 200:
        return None
    return result.get("json") if isinstance(result.get("json"), dict) else None


def fetch_userinfo(page: Page) -> dict | None:
    data = _page_fetch(page, USERINFO_API)
    if not data or data.get("code") != 0:
        return None
    return data.get("data") or {}


def fetch_billing_page(page: Page, dt_month: str, page_no: int, page_size: int) -> dict | None:
    url = f"{USAGE_API}?pageNo={page_no}&pageSize={page_size}&dtMonth={dt_month}"
    data = _page_fetch(page, url)
    if not data or data.get("code") != 0:
        return None
    return data.get("data") or {}


def fetch_all_billing_rows(page: Page, dt_month: str, page_size: int = 100) -> list[dict]:
    all_rows: list[dict] = []
    page_no = 1
    while True:
        data = fetch_billing_page(page, dt_month, page_no, page_size)
        if data is None:
            break
        items = data.get("list") or []
        all_rows.extend(items)
        total = data.get("total")
        if total is not None:
            try:
                if len(all_rows) >= int(total):
                    break
            except (TypeError, ValueError):
                pass
        if len(items) < page_size:
            break
        page_no += 1
        if page_no > 100:
            break
    return all_rows


def fetch_balance_amounts(page: Page) -> dict | None:
    data = _page_fetch(page, BALANCE_API)
    if not data or data.get("code") != 0:
        return None
    inner = data.get("data") or {}
    try:
        wallet = Decimal(str(inner.get("walletAmount") or "0"))
        coupon = Decimal(str(inner.get("couponAmount") or "0"))
        arrear = Decimal(str(inner.get("arrearAmount") or "0"))
        points_raw = str(inner.get("availablePoints") or "0")
        points = int(float(points_raw))
    except Exception:
        return None
    return {
        "total": wallet + coupon,
        "cash": wallet,
        "voucher": coupon,
        "credits": points,
        "arrear": arrear,
    }


def normalize_billing_row(raw: dict) -> dict | None:
    if not isinstance(raw, dict):
        return None
    model = raw.get("resourceName")
    token_type = raw.get("billingTokenType")
    if not model or not token_type:
        return None
    qty = raw.get("billingQuantity")
    if qty is None:
        return None
    try:
        tokens = int(float(qty))
    except (TypeError, ValueError):
        return None
    if tokens < 0:
        return None
    return {
        "call_time": raw.get("statTime") or raw.get("dt") or "",
        "account_pin": str(raw.get("invokeUserId") or ""),
        "username": str(raw.get("realName") or ""),
        "model": str(model),
        "token_type_raw": str(token_type),
        "tokens": tokens,
    }


def _looks_like_logged_in_url(url: str) -> bool:
    """True only when we are inside JoyAgent and not on a login page."""
    if not url:
        return False
    if "joyagent.jd.com" not in url:
        return False
    lowered = url.lower()
    bad = ("login", "passport", "signin", "sso")
    return not any(token in lowered for token in bad)


JDCLOUD_BILLING_URL = "https://billing-console.jdcloud.com/cost/consume/consume-history/v2"


def _looks_like_logged_in_jdcloud(url: str) -> bool:
    """Login is considered done as soon as we leave the JD passport / login page
    and land on any *.jdcloud.com page. We then navigate to the billing URL
    explicitly to capture XHR."""
    if not url:
        return False
    lowered = url.lower()
    if "login.jdcloud.com" in lowered:
        return False
    if "passport.jd.com" in lowered:
        return False
    return ".jdcloud.com" in lowered or "billing-console.jdcloud.com" in lowered


def login_jdcloud_only(timeout_seconds: int = 600) -> None:
    """Open the JD Cloud billing console, let user log in, then capture the
    actual XHR endpoints fired by the page so the importer can use them."""
    print("Opening browser for JD Cloud billing console login.")
    print("URL:", JDCLOUD_BILLING_URL)
    print("Sign in inside the browser window. The script auto-detects when the")
    print("billing-console.jdcloud.com page loads and then captures XHR endpoints.")
    print(f"(timeout: {timeout_seconds}s, press Ctrl+C to abort)\n")

    captured: list[dict] = []

    with sync_playwright() as pw:
        context = _new_context(pw, headless=False)
        page = context.new_page()

        def on_response(resp):
            try:
                ctype = (resp.headers or {}).get("content-type", "")
            except Exception:
                ctype = ""
            if "json" not in ctype.lower():
                return
            try:
                body = resp.text()
            except Exception:
                return
            try:
                data = json.loads(body)
            except Exception:
                return
            captured.append({
                "url": resp.url,
                "status": resp.status,
                "method": resp.request.method,
                "post_data": resp.request.post_data,
                "body_preview": json.dumps(data, ensure_ascii=False)[:2500],
            })

        page.on("response", on_response)

        try:
            page.goto(JDCLOUD_BILLING_URL, wait_until="domcontentloaded", timeout=60_000)
        except Exception as exc:
            print(f"Initial open failed (continue anyway): {exc}")

        time.sleep(3)
        deadline = time.time() + timeout_seconds
        last_url = ""
        last_print = 0.0
        success = False
        while time.time() < deadline:
            try:
                url = page.url or ""
            except Exception:
                url = ""
            if url != last_url:
                print(f"  current URL: {url}")
                last_url = url
                last_print = time.time()
            elif time.time() - last_print > 15:
                print(f"  still waiting... URL: {url}")
                last_print = time.time()
            if _looks_like_logged_in_jdcloud(url):
                print("Login detected. Reloading billing page to capture endpoints...")
                captured.clear()
                try:
                    page.goto(JDCLOUD_BILLING_URL, wait_until="networkidle", timeout=60_000)
                except Exception as exc:
                    print(f"  reload warning: {exc}")
                time.sleep(5)
                success = True
                break
            time.sleep(2)

        try:
            context.close()
        except Exception:
            pass

    if not success:
        print("WARNING: timed out before reaching billing-console.jdcloud.com.")
        print("You can re-run `python joyagent_monitor.py --login-jdcloud` to try again.")
        return

    out_path = WORKSPACE_DIR / "_jdcloud_endpoints.json"
    interesting = [
        c for c in captured
        if "billing-console.jdcloud.com" in c["url"]
        or "consume" in c["url"].lower()
        or "/cost/" in c["url"].lower()
        or "/bill" in c["url"].lower()
    ]
    out_path.write_text(
        json.dumps({"all": captured, "billing_candidates": interesting}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Login profile saved at: {USER_DATA_DIR}")
    print(f"Captured {len(captured)} JSON responses ({len(interesting)} look billing-related).")
    print(f"Saved to: {out_path}")
    if interesting:
        print("\nBilling-related endpoints:")
        for c in interesting[:20]:
            print(f"  [{c['status']}] {c['method']} {c['url']}")
            if c['post_data']:
                print(f"    post: {c['post_data'][:240]}")
            preview = (c['body_preview'] or '').replace('\n', ' ')[:160]
            print(f"    resp: {preview}")


def login_only(timeout_seconds: int = 600) -> None:
    print("Opening browser for interactive login.")
    print("Sign in to JD inside the browser window. The script will auto-detect")
    print("when the JoyAgent profile page loads and exit on its own.")
    print(f"(timeout: {timeout_seconds}s, press Ctrl+C to abort)\n")

    with sync_playwright() as pw:
        context = _new_context(pw, headless=False)
        page = context.new_page()
        try:
            page.goto(PROFILE_URL, wait_until="domcontentloaded", timeout=60_000)
        except Exception as exc:
            print(f"Failed to open page (continue anyway): {exc}")

        time.sleep(3)
        deadline = time.time() + timeout_seconds
        last_url = ""
        last_print = 0.0
        success = False
        while time.time() < deadline:
            try:
                url = page.url or ""
            except Exception:
                url = ""
            if url != last_url:
                print(f"  current URL: {url}")
                last_url = url
                last_print = time.time()
            elif time.time() - last_print > 15:
                print(f"  still waiting... URL: {url}")
                last_print = time.time()
            if _looks_like_logged_in_url(url):
                print("Login detected. Waiting 5s for cookies to settle...")
                time.sleep(5)
                success = True
                break
            time.sleep(2)
        try:
            context.close()
        except Exception:
            pass

    if success:
        print(f"Login profile saved at: {USER_DATA_DIR}")
    else:
        print("WARNING: timed out before reaching the JoyAgent profile page.")
        print("You can re-run `python joyagent_monitor.py --login` to try again.")


# ---------------------------------------------------------------------------
# Main monitor loop
# ---------------------------------------------------------------------------

def _months_to_query(arg_months: list[str] | None) -> list[str]:
    if arg_months:
        return arg_months
    return [datetime.now().strftime("%Y-%m")]


def _poll_once(conn: sqlite3.Connection, page: Page, months: list[str], detected_at: str) -> tuple[list[dict], dict | None]:
    api_rows: list[dict] = []
    for month in months:
        raws = fetch_all_billing_rows(page, month)
        for raw in raws:
            norm = normalize_billing_row(raw)
            if norm:
                api_rows.append(norm)
    amounts = fetch_balance_amounts(page)
    changes = upsert_usage(conn, api_rows, detected_at)
    balance_change = upsert_balance(conn, amounts, detected_at, source="api") if amounts else None
    return changes, balance_change


def run_monitor(
    interval: int,
    headless: bool,
    store_raw: bool,
    snapshot_mode: str,
    clear_screen: bool,
    months: list[str] | None,
) -> None:
    print(f"DB: {DB_PATH}")
    print(f"Profile dir: {USER_DATA_DIR}")
    print(f"Polling every {interval}s, headless={headless}")
    months_resolved = _months_to_query(months)
    print(f"Months: {', '.join(months_resolved)}\n")

    conn = open_db()

    with sync_playwright() as pw:
        context = _new_context(pw, headless=headless)
        page = context.new_page()

        try:
            page.goto(PROFILE_URL, wait_until="domcontentloaded", timeout=60_000)
        except Exception as exc:
            print(f"Initial page open failed: {exc}")

        userinfo = fetch_userinfo(page)
        if not userinfo or not userinfo.get("userId"):
            print("Not logged in (userInfo API returned no data).")
            print("Run `python joyagent_monitor.py --login` first.")
            try:
                context.close()
            except Exception:
                pass
            return

        print(
            f"Logged in: userId={userinfo.get('userId')}  "
            f"tenant={userinfo.get('tenantName')}  "
            f"realName={userinfo.get('realName')}\n"
        )

        detected_at = datetime.now().isoformat(timespec="seconds")
        changes, balance_change = _poll_once(conn, page, months_resolved, detected_at)
        if balance_change:
            print_balance_change(balance_change)
        if snapshot_mode != "never":
            print_full_snapshot(conn, title="JoyAgent bill snapshot (startup)")

        while True:
            time.sleep(interval)
            detected_at = datetime.now().isoformat(timespec="seconds")
            try:
                changes, balance_change = _poll_once(conn, page, months_resolved, detected_at)
            except Exception as exc:
                print(f"poll failed: {exc}")
                continue

            ts = datetime.now().strftime("%H:%M:%S")
            should_snapshot = snapshot_mode == "always" or (
                snapshot_mode == "change" and (changes or balance_change)
            )
            if not changes and not balance_change:
                print(f"[{ts}] no change")
            else:
                for c in changes:
                    print_change(c)
                if balance_change:
                    print_balance_change(balance_change)

            if should_snapshot:
                if clear_screen:
                    print("\033[2J\033[H", end="")
                print_full_snapshot(conn)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="JoyAgent usage monitor")
    parser.add_argument(
        "--login",
        action="store_true",
        help="Open a visible browser so you can sign in once; persists to ./joyagent_profile.",
    )
    parser.add_argument(
        "--login-jdcloud",
        action="store_true",
        help=(
            "Open the JD Cloud billing console (https://billing-console.jdcloud.com/) "
            "so you can sign in. Cookies are persisted into the same profile dir, "
            "and any billing-related XHR endpoints are written to "
            "./_jdcloud_endpoints.json for the importer to consume."
        ),
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL,
        help=f"Polling interval in seconds (default: {DEFAULT_INTERVAL}).",
    )
    parser.add_argument(
        "--show-browser",
        action="store_true",
        help="Run with a visible browser window (helps if JD blocks headless sessions).",
    )
    parser.add_argument(
        "--store-raw",
        action="store_true",
        help="Also persist every captured JSON response into the raw_responses table.",
    )
    parser.add_argument(
        "--snapshot",
        choices=["never", "change", "always"],
        default="change",
        help=(
            "When to print the full bill table: "
            "'never' (silent), 'change' (default, on first poll and on changes), "
            "'always' (every poll)."
        ),
    )
    parser.add_argument(
        "--clear-screen",
        action="store_true",
        help="Clear the terminal before each snapshot for a dashboard-like feel.",
    )
    parser.add_argument(
        "--inspect",
        action="store_true",
        help=(
            "Run a single poll in verbose diagnostic mode: dump captured JSON URLs, "
            "DOM tables, balance detection, and the parsed/computed rows. "
            "Useful to verify selectors against the real page."
        ),
    )
    parser.add_argument(
        "--month",
        action="append",
        default=None,
        help=(
            "Month(s) to query, format YYYY-MM. May be passed multiple times. "
            "Default: current month."
        ),
    )
    parser.add_argument(
        "--import-bills",
        action="append",
        default=None,
        help=(
            "Import historical bill data into the historical_bills table. "
            "Accepts both JD bill CSV files (.csv, gb18030) and tab-separated "
            "text files exported from the JoyAgent 调用明细 view "
            "(columns: 调用时间, 资源类型, 资源, 消费金额(¥)). "
            "May be passed multiple times. Then exits."
        ),
    )
    parser.add_argument(
        "--import-jdcloud",
        action="store_true",
        help=(
            "Pull paginated billing detail records from the JD Cloud billing "
            "console (describeBillDetails) into the jdcloud_bills table. "
            "Requires `--login-jdcloud` to have been run at least once. "
            "Defaults to current month; pass --month YYYY-MM (repeatable) for others."
        ),
    )
    args = parser.parse_args()

    if args.login:
        login_only()
        return

    if getattr(args, "login_jdcloud", False):
        login_jdcloud_only()
        return

    if getattr(args, "import_jdcloud", False):
        months = args.month or [datetime.now().strftime("%Y-%m")]
        print(f"Importing JD Cloud bills for: {', '.join(months)}")
        conn = open_db()
        with sync_playwright() as pw:
            context = _new_context(pw, headless=True)
            page = context.new_page()
            try:
                result = import_jdcloud_bills(conn, page, months)
            finally:
                try:
                    context.close()
                except Exception:
                    pass
        if not result.get("ok"):
            print(f"  FAIL: {result.get('error')}")
            print("  Tip: run `python joyagent_monitor.py --login-jdcloud` first.")
            return
        for month, info in (result.get("months") or {}).items():
            line = f"  {month}: fetched {info['fetched']} rows, inserted/replaced {info['inserted']}"
            if info.get("error"):
                line += f"  (warn: {info['error']})"
            print(line)
            s = info.get("summary") or {}
            if s:
                print(
                    f"    summary: billFee=\u00a5{s.get('billFee', 0):.4f}  "
                    f"actualFee=\u00a5{s.get('actualFee', 0):.4f}  "
                    f"cashCouponFee=\u00a5{s.get('cashCouponFee', 0):.4f}  "
                    f"eraseFee=\u00a5{s.get('eraseFee', 0):.4f}"
                )
        cur = conn.execute("SELECT COUNT(*) AS n, SUM(actual_fee) AS pay FROM jdcloud_bills")
        row = cur.fetchone()
        print(f"  jdcloud_bills total: {int(row['n'])} rows / actual_fee \u00a5{float(row['pay'] or 0):.2f}")
        return

    if args.import_bills:
        import glob as _glob
        conn = open_db()
        resolved: list[Path] = []
        for raw in args.import_bills:
            matches = [Path(m) for m in _glob.glob(raw)]
            if not matches:
                p = Path(raw)
                if p.exists():
                    matches = [p]
            if not matches:
                print(f"  SKIP (no match): {raw}")
                continue
            resolved.extend(matches)
        seen_paths: set[Path] = set()
        for path in resolved:
            full = path.resolve()
            if full in seen_paths:
                continue
            seen_paths.add(full)
            if not full.exists():
                print(f"  SKIP (not found): {full}")
                continue
            try:
                inserted, seen = import_bills_auto(conn, full)
            except Exception as exc:
                print(f"  FAIL {full.name}: {exc}")
                continue
            print(f"  {full.name}: inserted/updated {inserted} rows (parsed {seen})")
        cur = conn.execute(
            "SELECT COUNT(*) AS n, SUM(cny_amount) AS total FROM historical_bills"
        )
        row = cur.fetchone()
        print(f"  historical_bills total: {int(row['n'])} rows / \u00a5{float(row['total'] or 0):.2f}")
        return

    if args.inspect:
        try:
            run_inspect(headless=not args.show_browser, months=args.month)
        except KeyboardInterrupt:
            print("\nStopped.")
        return

    try:
        run_monitor(
            interval=args.interval,
            headless=not args.show_browser,
            store_raw=args.store_raw,
            snapshot_mode=args.snapshot,
            clear_screen=args.clear_screen,
            months=args.month,
        )
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
