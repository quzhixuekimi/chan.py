# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


BASE_DIR = Path(__file__).resolve().parent

STRATEGY_DIGEST_REGISTRY = {
  "v6_bspzs": {
    "results_dir": BASE_DIR / "user_strategy_v6_bspzs" / "results",
    # "digest_file": "market_signal_digest_last_per_symbol_v6_bspzs.csv",
    "digest_file": "market_trading_signal_digest_last_per_symbol_v6_bspzs.csv",
  },
  "v7_bi": {
    "results_dir": BASE_DIR / "user_strategy_v7_bi" / "results",
    "digest_file": "market_signal_digest_last_per_symbol_v7_bi.csv",
  },
  "v8_byma": {
    "results_dir": BASE_DIR / "user_strategy_v8_byma" / "results",
    "digest_file": "market_signal_digest_last_per_symbol_v8_byma.csv",
  },
  "v9_mr": {
    "results_dir": BASE_DIR / "user_strategy_v9_mr" / "results",
    "digest_file": "market_signal_digest_last_per_symbol_v9_mr.csv",
  },
}

DEFAULT_STRATEGY_ID = "v7_bi"

TELEGRAM_EVENT_WHITELIST = {
  "LONG_ENTRY_READY",
  "LONG_WEAKEN_ALERT",
  "LONG_EXIT_TREND",
  "LONG_STOP_LOSS",
  "BUY_SIGNAL",
  "SELL_SIGNAL",
  "STOP_LOSS_TRIGGERED",
  "POSITION_OPEN",
  "TRADE_CLOSED",
}

SENT_STATE_DIR = BASE_DIR / "_telegram_sent_state"
SENT_STATE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Telegram Notify API", version="0.6.0")


class TelegramTestRequest(BaseModel):
  text: str = Field(..., description="要发送的消息内容")
  chat_id: Optional[str] = Field(
    default=None, description="可选，若不传则使用环境变量 TELEGRAM_CHAT_ID"
  )
  parse_mode: Optional[str] = Field(
    default=None, description="可选，支持 Markdown / HTML"
  )
  disable_web_page_preview: bool = Field(default=True)


class TelegramDigestRequest(BaseModel):
  strategy_id: str = Field(
    default=DEFAULT_STRATEGY_ID,
    description="策略ID，例如 v7_bi / v8_byma",
  )
  digest_file: Optional[str] = Field(
    default=None,
    description="摘要CSV文件路径；若不传则按 strategy_id 使用默认 digest 文件",
  )
  chat_id: Optional[str] = Field(
    default=None,
    description="可选，若不传则使用环境变量 TELEGRAM_CHAT_ID",
  )
  only_has_signal: bool = Field(
    default=True,
    description="是否只发送 has_signal=True 的记录",
  )
  only_whitelist_event_types: bool = Field(
    default=True,
    description="是否只发送白名单事件类型，默认过滤掉 BULL_ENV_READY 等观察类事件",
  )
  include_empty_summary: bool = Field(
    default=False,
    description="是否包含全周期无信号的空摘要，默认不包含",
  )
  limit: Optional[int] = Field(
    default=None,
    description="最多发送多少条，调试时可用",
  )
  dry_run: bool = Field(
    default=False,
    description="若为 true，只返回将要发送的内容，不真正调用 Telegram",
  )
  parse_mode: Optional[str] = Field(
    default=None,
    description="可选，支持 Markdown / HTML",
  )
  disable_web_page_preview: bool = Field(default=True)
  deduplicate: bool = Field(
    default=True,
    description="是否对已发送过的 digest 消息做去重",
  )
  resend_existing: bool = Field(
    default=False,
    description="若为 true，则忽略去重状态，重新发送当前命中的消息",
  )


def get_telegram_token() -> str:
  token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
  if not token:
    raise HTTPException(status_code=500, detail="TELEGRAM_BOT_TOKEN is empty")
  return token


def get_default_chat_id() -> str:
  chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
  if not chat_id:
    raise HTTPException(status_code=500, detail="TELEGRAM_CHAT_ID is empty")
  return chat_id


def telegram_api_url(method: str) -> str:
  token = get_telegram_token()
  return f"https://api.telegram.org/bot{token}/{method}"


def telegram_get_me() -> dict:
  url = telegram_api_url("getMe")
  resp = requests.get(url, timeout=20)
  if not resp.ok:
    raise HTTPException(
      status_code=502,
      detail=f"Telegram getMe failed: status={resp.status_code}, body={resp.text[:1000]}",
    )
  data = resp.json()
  if not data.get("ok"):
    raise HTTPException(status_code=502, detail=f"Telegram getMe failed: {data}")
  return data


def telegram_send_message(
  chat_id: str,
  text: str,
  parse_mode: Optional[str] = None,
  disable_web_page_preview: bool = True,
) -> dict:
  url = telegram_api_url("sendMessage")
  payload = {
    "chat_id": chat_id,
    "text": text,
    "disable_web_page_preview": disable_web_page_preview,
  }
  if parse_mode:
    payload["parse_mode"] = parse_mode

  resp = requests.post(url, json=payload, timeout=20)
  if not resp.ok:
    raise HTTPException(
      status_code=502,
      detail=f"Telegram sendMessage failed: status={resp.status_code}, body={resp.text[:1000]}",
    )
  data = resp.json()
  if not data.get("ok"):
    raise HTTPException(status_code=502, detail=f"Telegram sendMessage failed: {data}")
  return data


def resolve_digest_file(strategy_id: str, digest_file: Optional[str]) -> Path:
  if digest_file and str(digest_file).strip():
    p = Path(digest_file).expanduser()
    if not p.is_absolute():
      p = BASE_DIR / p
    return p

  item = STRATEGY_DIGEST_REGISTRY.get(strategy_id)
  if not item:
    raise HTTPException(
      status_code=404,
      detail=f"unknown strategy_id: {strategy_id}",
    )

  return item["results_dir"] / item["digest_file"]


def normalize_bool(v) -> bool:
  if isinstance(v, bool):
    return v
  if v is None:
    return False
  s = str(v).strip().lower()
  return s in {"1", "true", "yes", "y"}


def is_empty_summary_row(row: dict) -> bool:
  has_signal = normalize_bool(row.get("has_signal", False))
  if has_signal:
    return False

  summary_text = str(row.get("summary_text", "") or "").strip()
  if not summary_text:
    return True

  if "无信号" in summary_text:
    return True

  summary_json_raw = row.get("summary_json", "")
  if summary_json_raw is None:
    return True

  try:
    payload = json.loads(summary_json_raw) if str(summary_json_raw).strip() else {}
  except Exception:
    payload = {}

  if not isinstance(payload, dict) or not payload:
    return True

  for tf_data in payload.values():
    if not isinstance(tf_data, dict):
      continue
    event_type = str(tf_data.get("event_type", "") or "").strip()
    is_fresh = normalize_bool(tf_data.get("is_fresh", False))
    telegram_allowed = normalize_bool(tf_data.get("telegram_allowed", False))
    if event_type and is_fresh and telegram_allowed:
      return False

  return True


def load_digest_rows(
  digest_path: Path,
  only_has_signal: bool = True,
  only_whitelist_event_types: bool = True,
  include_empty_summary: bool = False,
  limit: Optional[int] = None,
) -> list[dict]:
  if not digest_path.exists():
    raise HTTPException(
      status_code=404,
      detail=f"digest file not found: {digest_path}",
    )

  try:
    df = pd.read_csv(digest_path)
  except Exception as e:
    raise HTTPException(
      status_code=500,
      detail=f"failed to read digest csv: {e}",
    )

  if df.empty:
    return []

  if "summary_text" not in df.columns:
    raise HTTPException(
      status_code=500,
      detail="digest csv missing required column: summary_text",
    )

  rows = df.to_dict(orient="records")

  filtered_rows: list[dict] = []
  for row in rows:
    if only_has_signal and not normalize_bool(row.get("has_signal", False)):
      continue

    if not include_empty_summary and is_empty_summary_row(row):
      continue

    if only_whitelist_event_types and "event_type" in row:
      event_type = str(row.get("event_type", "") or "").strip()
      if event_type and event_type not in TELEGRAM_EVENT_WHITELIST:
        continue

    summary_text = str(row.get("summary_text", "") or "").strip()
    if not summary_text:
      continue

    filtered_rows.append(row)

  if filtered_rows and "symbol" in filtered_rows[0]:
    filtered_rows = sorted(filtered_rows, key=lambda x: str(x.get("symbol", "")))

  if limit is not None and limit > 0:
    filtered_rows = filtered_rows[:limit]

  return filtered_rows


def get_sent_state_path(strategy_id: str) -> Path:
  safe_id = str(strategy_id).strip().lower().replace("/", "_")
  return SENT_STATE_DIR / f"sent_digest_keys_{safe_id}.json"


def load_sent_state(strategy_id: str) -> dict:
  path = get_sent_state_path(strategy_id)
  if not path.exists():
    return {"strategy_id": strategy_id, "items": {}}
  try:
    return json.loads(path.read_text(encoding="utf-8"))
  except Exception:
    return {"strategy_id": strategy_id, "items": {}}


def save_sent_state(strategy_id: str, state: dict):
  path = get_sent_state_path(strategy_id)
  path.write_text(
    json.dumps(state, ensure_ascii=False, indent=2),
    encoding="utf-8",
  )


def fingerprint_text(text: str) -> str:
  return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_row_dedup_key(row: dict) -> str:
  summary_text = str(row.get("summary_text", "")).strip()
  symbol = str(row.get("symbol", "")).strip()
  summary_json = str(row.get("summary_json", "")).strip()
  fingerprint = fingerprint_text(summary_text + "\n" + summary_json)
  return f"{symbol}::{fingerprint}"


@app.get("/api/notify/telegram/health")
def health():
  default_item = STRATEGY_DIGEST_REGISTRY.get(DEFAULT_STRATEGY_ID, {})
  return {
    "ok": True,
    "service": "telegram-notify-api",
    "default_strategy_id": DEFAULT_STRATEGY_ID,
    "supported_strategies": sorted(STRATEGY_DIGEST_REGISTRY.keys()),
    "default_digest_file": str(
      default_item.get("results_dir", BASE_DIR) / default_item.get("digest_file", "")
    ).rstrip("/"),
    "telegram_event_whitelist": sorted(TELEGRAM_EVENT_WHITELIST),
    "sent_state_dir": str(SENT_STATE_DIR),
  }


@app.get("/api/notify/telegram/me")
def get_me():
  data = telegram_get_me()
  return {
    "ok": True,
    "telegram": data.get("result", {}),
  }


@app.post("/api/notify/telegram/test")
def send_test_message(req: TelegramTestRequest):
  chat_id = (req.chat_id or get_default_chat_id()).strip()
  if not chat_id:
    raise HTTPException(
      status_code=400,
      detail="chat_id is required, either in request body or TELEGRAM_CHAT_ID env",
    )

  data = telegram_send_message(
    chat_id=chat_id,
    text=req.text,
    parse_mode=req.parse_mode,
    disable_web_page_preview=req.disable_web_page_preview,
  )

  result = data.get("result", {})
  return {
    "ok": True,
    "message_id": result.get("message_id"),
    "chat": result.get("chat", {}),
    "date": result.get("date"),
    "text": result.get("text"),
  }


@app.post("/api/notify/telegram/send-digest")
def send_digest(req: TelegramDigestRequest):
  chat_id = (req.chat_id or get_default_chat_id()).strip()
  if not chat_id:
    raise HTTPException(
      status_code=400,
      detail="chat_id is required, either in request body or TELEGRAM_CHAT_ID env",
    )

  digest_path = resolve_digest_file(req.strategy_id, req.digest_file)
  rows = load_digest_rows(
    digest_path=digest_path,
    only_has_signal=req.only_has_signal,
    only_whitelist_event_types=req.only_whitelist_event_types,
    include_empty_summary=req.include_empty_summary,
    limit=req.limit,
  )

  sent_state = load_sent_state(req.strategy_id) if req.deduplicate else {"items": {}}
  sent_items = sent_state.get("items", {})

  messages: list[str] = []
  filtered_rows: list[dict] = []
  new_state_items: dict = {}

  for row in rows:
    # text = str(row.get("summary_text", "")).strip()
    text = str(row.get("summary_text", "")).strip()
    if text:
      text = f"[{req.strategy_id}]\n{text}"
    if not text:
      continue

    dedup_key = build_row_dedup_key(row)
    already_sent = dedup_key in sent_items

    if req.deduplicate and (not req.resend_existing) and already_sent:
      new_state_items[dedup_key] = sent_items[dedup_key]
      continue

    filtered_rows.append(
      {
        "symbol": row.get("symbol", ""),
        "summary_text": text,
        "dedup_key": dedup_key,
        "already_sent": already_sent,
      }
    )
    messages.append(text)

    new_state_items[dedup_key] = {
      "symbol": row.get("symbol", ""),
      "summary_text": text,
      "first_sent_at": sent_items.get(dedup_key, {}).get("first_sent_at"),
      "last_sent_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
      "digest_file": str(digest_path),
    }

  if not messages:
    return {
      "ok": True,
      "dry_run": req.dry_run,
      "strategy_id": req.strategy_id,
      "digest_file": str(digest_path),
      "message_count": 0,
      "messages": [],
      "rows": [],
      "deduplicate": req.deduplicate,
      "include_empty_summary": req.include_empty_summary,
    }

  if req.dry_run:
    return {
      "ok": True,
      "dry_run": True,
      "strategy_id": req.strategy_id,
      "digest_file": str(digest_path),
      "message_count": len(messages),
      "messages": messages,
      "rows": filtered_rows,
      "deduplicate": req.deduplicate,
      "include_empty_summary": req.include_empty_summary,
    }

  sent = []
  now_utc = datetime.utcnow().isoformat(timespec="seconds") + "Z"

  for row, msg in zip(filtered_rows, messages):
    data = telegram_send_message(
      chat_id=chat_id,
      text=msg,
      parse_mode=req.parse_mode,
      disable_web_page_preview=req.disable_web_page_preview,
    )
    result = data.get("result", {})
    sent.append(
      {
        "message_id": result.get("message_id"),
        "text": result.get("text"),
        "chat": result.get("chat", {}),
        "date": result.get("date"),
        "dedup_key": row.get("dedup_key"),
      }
    )

    if req.deduplicate:
      key = row.get("dedup_key")
      prev = sent_items.get(key, {})
      new_state_items[key] = {
        "symbol": row.get("symbol", ""),
        "summary_text": row.get("summary_text", ""),
        "first_sent_at": prev.get("first_sent_at") or now_utc,
        "last_sent_at": now_utc,
        "digest_file": str(digest_path),
      }

  if req.deduplicate:
    merged_items = dict(sent_items)
    merged_items.update(new_state_items)
    save_sent_state(
      req.strategy_id,
      {
        "strategy_id": req.strategy_id,
        "updated_at": now_utc,
        "items": merged_items,
      },
    )

  return {
    "ok": True,
    "dry_run": False,
    "strategy_id": req.strategy_id,
    "digest_file": str(digest_path),
    "message_count": len(sent),
    "sent": sent,
    "event_type_whitelist": sorted(TELEGRAM_EVENT_WHITELIST),
    "deduplicate": req.deduplicate,
    "include_empty_summary": req.include_empty_summary,
  }


if __name__ == "__main__":
  import uvicorn

  uvicorn.run("telegram_notify_api:app", host="127.0.0.1", port=8010, reload=True)
