from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Literal

import pandas as pd
import requests
from zoneinfo import ZoneInfo

from trade_system.notifiers.telegram import telegram_send_message

try:
  from apscheduler.schedulers.blocking import BlockingScheduler
  from apscheduler.triggers.cron import CronTrigger
except Exception:
  BlockingScheduler = None
  CronTrigger = None

LevelType = Literal["1D", "1H", "2H", "4H", "30M", "15M"]

BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = "/tmp/daily_workflow_scheduler.log"

STRATEGY_DIGEST_REGISTRY = {
  "v5_macdtd": {
    "results_dir": BASE_DIR / "user_strategy_v5_macdtd" / "results",
    "digest_file": "market_signal_digest_last_per_symbol_v5_macdtd.csv",
  },
  "v6_bspzs": {
    "results_dir": BASE_DIR / "user_strategy_v6_bspzs" / "results",
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
}

LEGACY_TELEGRAM_EVENT_WHITELIST = {
  "LONG_ENTRY_READY",
  "LONG_WEAKEN_ALERT",
  "LONG_EXIT_TREND",
  "LONG_STOP_LOSS",
}

CURRENT_TELEGRAM_EVENT_WHITELIST = {
  "BUY_SIGNAL",
  "SELL_SIGNAL",
  "STOP_LOSS_TRIGGERED",
  "POSITION_OPEN",
  "TRADE_CLOSED",
}

TELEGRAM_EVENT_WHITELIST = (
  LEGACY_TELEGRAM_EVENT_WHITELIST | CURRENT_TELEGRAM_EVENT_WHITELIST
)

SENT_STATE_DIR = BASE_DIR / "_telegram_sent_state"
SENT_STATE_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_SYMBOLS = [
  "AAPL",
  "TSLA",
  "NVDA",
  "COIN",
  "PLTR",
  "HOOD",
  "OKLO",
  "SOXL",
  "AMD",
  "MU",
  "AVGO",
  "ORCL",
  "BTC-USD",
  "ETH-USD",
]
DEFAULT_LEVELS: list[LevelType] = ["1D", "1H", "2H", "4H", "30M", "15M"]

scheduler = None


@dataclass
class WorkflowConfig:
  base_url: str
  symbols: list[str]
  levels: list[LevelType]
  backtest_strategy_id: str
  timezone: str
  cron_hour: int
  cron_minute: int
  request_timeout: int = 300
  pause_seconds: float = 0.2
  continue_on_analyze_error: bool = True

  notify_enabled: bool = True
  notify_dry_run: bool = False
  notify_only_has_signal: bool = True
  notify_only_whitelist_event_types: bool = True
  notify_include_empty_summary: bool = False
  notify_deduplicate: bool = True
  notify_resend_existing: bool = False
  notify_limit: int | None = None
  notify_digest_file: str | None = None
  continue_on_notify_error: bool = True

  retry_delay_hours: int = 1
  retry_max_attempts: int = 3


def normalize_bool(v) -> bool:
  if isinstance(v, bool):
    return v
  if v is None:
    return False
  return str(v).strip().lower() in {"1", "true", "yes", "y"}


def is_empty_summary_row(row: dict) -> bool:
  if "aggregated_summary_text" in row:
    agg = str(row.get("aggregated_summary_text", "") or "").strip()
    return not bool(agg)
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
  ignore_event_type_whitelist: bool = False,
  include_empty_summary: bool = False,
  limit: int | None = None,
) -> list[dict]:
  if not digest_path.exists():
    raise FileNotFoundError(f"digest file not found: {digest_path}")
  try:
    df = pd.read_csv(digest_path)
  except Exception as e:
    raise RuntimeError(f"failed to read digest csv: {e}")
  if df.empty:
    return []
  if "summary_text" not in df.columns and "aggregated_summary_text" not in df.columns:
    raise RuntimeError(
      "digest csv missing required column: summary_text or aggregated_summary_text"
    )

  rows = df.to_dict(orient="records")
  if "aggregated_summary_text" in df.columns and "summary_text" not in df.columns:
    for r in rows:
      r["summary_text"] = str(r.get("aggregated_summary_text", "") or "").strip()
  filtered_rows: list[dict] = []
  whitelist_enabled = only_whitelist_event_types and not ignore_event_type_whitelist

  for row in rows:
    if only_has_signal and not normalize_bool(row.get("has_signal", False)):
      continue
    if not include_empty_summary and is_empty_summary_row(row):
      continue
    if whitelist_enabled and "event_type" in row:
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
  path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def clear_sent_state(strategy_id: str):
  path = get_sent_state_path(strategy_id)
  if path.exists():
    path.unlink()


def fingerprint_text(text: str) -> str:
  return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_row_dedup_key(row: dict) -> str:
  summary_text = str(row.get("summary_text", "")).strip()
  symbol = str(row.get("symbol", "")).strip()
  summary_json = str(row.get("summary_json", "")).strip()
  event_type = str(row.get("event_type", "")).strip()
  event_time = str(row.get("event_time", "")).strip()
  fingerprint = fingerprint_text(
    summary_text + "\n" + summary_json + "\n" + event_type + "\n" + event_time
  )
  return f"{symbol}::{fingerprint}"


def resolve_digest_file(strategy_id: str, digest_file: str | None) -> Path:
  if digest_file and str(digest_file).strip():
    p = Path(digest_file).expanduser()
    if not p.is_absolute():
      p = BASE_DIR / p
    return p
  item = STRATEGY_DIGEST_REGISTRY.get(strategy_id)
  if not item:
    raise ValueError(f"unknown strategy_id: {strategy_id}")
  return item["results_dir"] / item["digest_file"]


def send_digest_telegram(
  strategy_id: str,
  digest_path: Path | None = None,
  only_has_signal: bool = True,
  only_whitelist_event_types: bool = True,
  include_empty_summary: bool = False,
  deduplicate: bool = True,
  resend_existing: bool = False,
  limit: int | None = None,
  dry_run: bool = False,
  reset_sent_state_before_send: bool = False,
) -> dict:
  if digest_path is None:
    digest_path = resolve_digest_file(strategy_id, None)

  if reset_sent_state_before_send:
    clear_sent_state(strategy_id)

  rows = load_digest_rows(
    digest_path=digest_path,
    only_has_signal=only_has_signal,
    only_whitelist_event_types=only_whitelist_event_types,
    ignore_event_type_whitelist=False,
    include_empty_summary=include_empty_summary,
    limit=limit,
  )

  sent_state = load_sent_state(strategy_id) if deduplicate else {"items": {}}
  sent_items = sent_state.get("items", {})

  messages: list[str] = []
  filtered_rows: list[dict] = []
  new_state_items: dict = {}

  for row in rows:
    if "aggregated_summary_text" in row:
      text = str(row.get("aggregated_summary_text", "")).strip()
      dedup_fingerprint = str(row.get("aggregated_fingerprint", "")).strip()
      dedup_key = f"{row.get('symbol', '')}::{dedup_fingerprint}"
    else:
      text = str(row.get("summary_text", "")).strip()
      dedup_key = build_row_dedup_key(row)

    if "aggregated_summary_text" not in row and text:
      text = f"[{strategy_id}]\n{text}"

    if not text:
      continue

    already_sent = dedup_key in sent_items

    if deduplicate and (not resend_existing) and already_sent:
      new_state_items[dedup_key] = sent_items[dedup_key]
      continue

    filtered_rows.append(
      {
        "symbol": row.get("symbol", ""),
        "event_type": row.get("event_type", ""),
        "event_time": row.get("event_time", ""),
        "summary_text": text,
        "dedup_key": dedup_key,
        "already_sent": already_sent,
      }
    )
    messages.append(text)

    new_state_items[dedup_key] = {
      "symbol": row.get("symbol", ""),
      "event_type": row.get("event_type", ""),
      "event_time": row.get("event_time", ""),
      "summary_text": text,
      "first_sent_at": sent_items.get(dedup_key, {}).get("first_sent_at"),
      "last_sent_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
      "digest_file": str(digest_path),
    }

  if not messages:
    return {
      "ok": True,
      "dry_run": dry_run,
      "strategy_id": strategy_id,
      "digest_file": str(digest_path),
      "message_count": 0,
      "messages": [],
      "rows": [],
      "deduplicate": deduplicate,
      "include_empty_summary": include_empty_summary,
    }

  if dry_run:
    return {
      "ok": True,
      "dry_run": True,
      "strategy_id": strategy_id,
      "digest_file": str(digest_path),
      "message_count": len(messages),
      "messages": messages,
      "rows": filtered_rows,
      "deduplicate": deduplicate,
      "include_empty_summary": include_empty_summary,
    }

  sent = []
  now_utc = datetime.utcnow().isoformat(timespec="seconds") + "Z"
  for row, msg in zip(filtered_rows, messages):
    data = telegram_send_message(
      chat_id=None,
      text=msg,
      parse_mode=None,
      disable_web_page_preview=True,
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
    if deduplicate:
      key = row.get("dedup_key")
      prev = sent_items.get(key, {})
      new_state_items[key] = {
        "symbol": row.get("symbol", ""),
        "event_type": row.get("event_type", ""),
        "event_time": row.get("event_time", ""),
        "summary_text": row.get("summary_text", ""),
        "first_sent_at": prev.get("first_sent_at") or now_utc,
        "last_sent_at": now_utc,
        "digest_file": str(digest_path),
      }

  if deduplicate:
    merged_items = dict(sent_items)
    merged_items.update(new_state_items)
    save_sent_state(
      strategy_id,
      {
        "strategy_id": strategy_id,
        "updated_at": now_utc,
        "items": merged_items,
      },
    )

  return {
    "ok": True,
    "dry_run": False,
    "strategy_id": strategy_id,
    "digest_file": str(digest_path),
    "message_count": len(sent),
    "sent": sent,
    "deduplicate": deduplicate,
    "include_empty_summary": include_empty_summary,
  }


logger = logging.getLogger("daily_workflow_scheduler")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
if not logger.handlers:
  file_handler = TimedRotatingFileHandler(
    str(LOG_FILE), when="D", interval=7, backupCount=12, encoding="utf-8"
  )
  file_handler.setFormatter(formatter)
  stream_handler = logging.StreamHandler(sys.stdout)
  stream_handler.setFormatter(formatter)
  logger.addHandler(file_handler)
  logger.addHandler(stream_handler)
  logger.propagate = False


def _now_str(tz_name: str) -> str:
  return datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S %Z")


def _post_json(url: str, payload: dict, timeout: int) -> requests.Response:
  return requests.post(url, json=payload, timeout=timeout)


def run_analyze_step(config: WorkflowConfig) -> dict:
  analyze_url = f"{config.base_url.rstrip('/')}/api/chan/analyze"
  total = 0
  success = 0
  failed = 0
  failures: list[dict] = []

  logger.info(
    "[ANALYZE] start symbols=%s levels=%s", len(config.symbols), config.levels
  )

  for symbol in config.symbols:
    for level in config.levels:
      total += 1
      payload = {"code": symbol, "level": level}
      try:
        logger.info("[ANALYZE] request symbol=%s level=%s", symbol, level)
        resp = _post_json(analyze_url, payload, config.request_timeout)
        if resp.ok:
          try:
            body = resp.json()
          except Exception:
            body = {"raw": resp.text[:1000]}
          success += 1
          logger.info(
            "[ANALYZE] ok symbol=%s level=%s status=%s", symbol, level, resp.status_code
          )
          logger.debug(
            "[ANALYZE] response symbol=%s level=%s body=%s",
            symbol,
            level,
            json.dumps(body, ensure_ascii=False)[:2000],
          )
        else:
          failed += 1
          info = {
            "symbol": symbol,
            "level": level,
            "status_code": resp.status_code,
            "response": resp.text[:2000],
          }
          failures.append(info)
          logger.error(
            "[ANALYZE] fail symbol=%s level=%s status=%s response=%s",
            symbol,
            level,
            resp.status_code,
            resp.text[:1000],
          )
          if not config.continue_on_analyze_error:
            raise RuntimeError(f"analyze failed: {info}")
      except Exception as e:
        failed += 1
        info = {
          "symbol": symbol,
          "level": level,
          "status_code": None,
          "response": str(e),
        }
        failures.append(info)
        logger.exception(
          "[ANALYZE] exception symbol=%s level=%s err=%s", symbol, level, e
        )
        if not config.continue_on_analyze_error:
          raise
      finally:
        if config.pause_seconds > 0:
          time.sleep(config.pause_seconds)

  logger.info(
    "[ANALYZE] finished total=%s success=%s failed=%s", total, success, failed
  )
  return {
    "total": total,
    "success": success,
    "failed": failed,
    "failures": failures,
  }


def run_indicators_step(config: WorkflowConfig) -> dict:
  indicators_url = f"{config.base_url.rstrip('/')}/api/chan/indicators"
  total = 0
  success = 0
  failed = 0
  failures: list[dict] = []

  logger.info(
    "[INDICATORS] start symbols=%s levels=%s", len(config.symbols), config.levels
  )

  for symbol in config.symbols:
    for level in config.levels:
      total += 1
      payload = {"code": symbol, "level": level}
      try:
        logger.info("[INDICATORS] request symbol=%s level=%s", symbol, level)
        resp = _post_json(indicators_url, payload, config.request_timeout)
        if resp.ok:
          try:
            body = resp.json()
          except Exception:
            body = {"raw": resp.text[:1000]}
          success += 1
          logger.info(
            "[INDICATORS] ok symbol=%s level=%s status=%s",
            symbol,
            level,
            resp.status_code,
          )
          logger.debug(
            "[INDICATORS] response symbol=%s level=%s body=%s",
            symbol,
            level,
            json.dumps(body, ensure_ascii=False)[:2000],
          )
        else:
          failed += 1
          info = {
            "symbol": symbol,
            "level": level,
            "status_code": resp.status_code,
            "response": resp.text[:2000],
          }
          failures.append(info)
          logger.error(
            "[INDICATORS] fail symbol=%s level=%s status=%s response=%s",
            symbol,
            level,
            resp.status_code,
            resp.text[:1000],
          )
      except Exception as e:
        failed += 1
        info = {
          "symbol": symbol,
          "level": level,
          "status_code": None,
          "response": str(e),
        }
        failures.append(info)
        logger.exception(
          "[INDICATORS] exception symbol=%s level=%s err=%s", symbol, level, e
        )
      finally:
        if config.pause_seconds > 0:
          time.sleep(config.pause_seconds)

  logger.info(
    "[INDICATORS] finished total=%s success=%s failed=%s", total, success, failed
  )
  return {
    "total": total,
    "success": success,
    "failed": failed,
    "failures": failures,
  }


def run_pivot_sr_step(config: WorkflowConfig) -> dict:
  pivot_sr_url = f"{config.base_url.rstrip('/')}/api/chan/pivot_sr"
  total = 0
  success = 0
  failed = 0
  failures: list[dict] = []

  logger.info(
    "[PIVOT_SR] start symbols=%s levels=%s", len(config.symbols), config.levels
  )

  for symbol in config.symbols:
    for level in config.levels:
      total += 1
      payload = {"code": symbol, "level": level}
      try:
        logger.info("[PIVOT_SR] request symbol=%s level=%s", symbol, level)
        resp = _post_json(pivot_sr_url, payload, config.request_timeout)
        if resp.ok:
          try:
            body = resp.json()
          except Exception:
            body = {"raw": resp.text[:1000]}
          success += 1
          logger.info(
            "[PIVOT_SR] ok symbol=%s level=%s status=%s",
            symbol,
            level,
            resp.status_code,
          )
          logger.debug(
            "[PIVOT_SR] response symbol=%s level=%s body=%s",
            symbol,
            level,
            json.dumps(body, ensure_ascii=False)[:2000],
          )
        else:
          failed += 1
          info = {
            "symbol": symbol,
            "level": level,
            "status_code": resp.status_code,
            "response": resp.text[:2000],
          }
          failures.append(info)
          logger.error(
            "[PIVOT_SR] fail symbol=%s level=%s status=%s response=%s",
            symbol,
            level,
            resp.status_code,
            resp.text[:1000],
          )
      except Exception as e:
        failed += 1
        info = {
          "symbol": symbol,
          "level": level,
          "status_code": None,
          "response": str(e),
        }
        failures.append(info)
        logger.exception(
          "[PIVOT_SR] exception symbol=%s level=%s err=%s", symbol, level, e
        )
      finally:
        if config.pause_seconds > 0:
          time.sleep(config.pause_seconds)

  logger.info(
    "[PIVOT_SR] finished total=%s success=%s failed=%s", total, success, failed
  )
  return {
    "total": total,
    "success": success,
    "failed": failed,
    "failures": failures,
  }


def _push_queue_to_telegram(config: WorkflowConfig):
  try:
    import db
    from trade_system.notifiers.telegram import telegram_send_message

    queue = db.load_queue_today_from_db()
    signals = queue.get("signals", [])
    if not signals:
      return
    buy_count = sum(1 for s in signals if s.get("action") == "buy")
    sell_count = sum(1 for s in signals if s.get("action") == "sell")
    manual_count = sum(1 for s in signals if s.get("action") == "") + sum(
      1 for s in signals if s.get("action") == "manual_review"
    )
    # Send signals in chunks of up to 10. Keep header/counts in each message.
    total = len(signals)
    chunk_size = 10
    for start in range(0, total, chunk_size):
      chunk = signals[start : start + chunk_size]
      lines = [
        f"📋 今日交易队列 ({queue.get('generated_at', '')[:10]})",
        f"共 {total} 个信号",
        f"买入: {buy_count} | 卖出: {sell_count} | 待人工: {manual_count}",
        "",
      ]
      for sig in chunk:
        action_emoji = {"buy": "✅", "sell": "🔴", "manual_review": "⚠️"}.get(
          sig.get("action"), "❓"
        )
        lines.append(
          f"{action_emoji} {sig.get('symbol', 'N/A')} {sig.get('action', 'N/A')} {sig.get('strategy', '')} {sig.get('period', '')} {sig.get('event_time', '')}"
        )
      msg = "📋 交易队列生成完成\n" + "\n".join(lines)
      telegram_send_message(msg)
    logger.info("[QUEUE-PUSH] telegram推送成功")
  except Exception as e:
    logger.exception("[QUEUE-PUSH] telegram推送异常: %s", e)


def run_backtest_step(config: WorkflowConfig, note: str | None = None) -> dict:
  backtest_url = f"{config.base_url.rstrip('/')}/api/chan/backtest"
  payload = {
    "run_mode": "all",
    "note": note or f"scheduled workflow {_now_str(config.timezone)}",
  }
  logger.info("[BACKTEST] request run_mode=all")
  resp = _post_json(backtest_url, payload, timeout=max(config.request_timeout, 1800))
  if not resp.ok:
    logger.error(
      "[BACKTEST] fail status=%s response=%s", resp.status_code, resp.text[:2000]
    )
    raise RuntimeError(
      f"backtest failed: status={resp.status_code}, response={resp.text[:2000]}"
    )
  data = resp.json()
  requested = data.get("data", {}).get("requested_strategies", []) or []
  logger.info("[BACKTEST] ok run_mode=all requested_strategies=%s", requested)
  logger.debug("[BACKTEST] response=%s", json.dumps(data, ensure_ascii=False)[:4000])

  return data


def generate_queue_step(config: WorkflowConfig):
  try:
    from pathlib import Path
    from trade_system.queue.writer import write_queue_from_multiple_digests

    base_dir = Path(__file__).resolve().parent
    # 搜索各策略目录下的results文件夹
    strategy_dirs = [
      base_dir / "user_strategy_v5_macdtd" / "results",
      base_dir / "user_strategy_v7_bi" / "results",
      base_dir / "user_strategy_v8_byma" / "results",
    ]
    digest_files = []
    for d in strategy_dirs:
      if d.exists():
        digest_files.extend(d.glob("market_signal_digest_last_per_symbol_*.csv"))

    if digest_files:
      out = write_queue_from_multiple_digests(digest_files)
      logger.info(
        "[QUEUE] wrote queue from %s digest files output=%s", len(digest_files), out
      )
      # Push queue summary to telegram
      if config.notify_enabled:
        _push_queue_to_telegram(config)
    else:
      logger.warning("[QUEUE] no digest files found in strategy directories")
  except Exception as e:
    logger.exception("[QUEUE] failed to write queue: %s", e)
    if not config.continue_on_analyze_error:
      raise


def run_notify_step(config: WorkflowConfig, strategy_ids: list[str]) -> dict:
  if not config.notify_enabled:
    logger.info("[NOTIFY] skipped: notify_enabled=False")
    return {
      "enabled": False,
      "skipped": True,
      "reason": "notify disabled",
    }

  if not strategy_ids:
    logger.info("[NOTIFY] skipped: no strategy_ids from backtest")
    return {
      "enabled": True,
      "skipped": True,
      "reason": "no strategy_ids",
      "requested_strategies": [],
      "results": [],
    }

  digest_path = Path(config.notify_digest_file) if config.notify_digest_file else None
  results: list[dict] = []

  for strategy_id in strategy_ids:
    logger.info(
      "[NOTIFY] strategy_id=%s dry_run=%s only_has_signal=%s only_whitelist_event_types=%s include_empty_summary=%s deduplicate=%s resend_existing=%s limit=%s digest_file=%s",
      strategy_id,
      config.notify_dry_run,
      config.notify_only_has_signal,
      config.notify_only_whitelist_event_types,
      config.notify_include_empty_summary,
      config.notify_deduplicate,
      config.notify_resend_existing,
      config.notify_limit,
      config.notify_digest_file,
    )

    try:
      data = send_digest_telegram(
        strategy_id=strategy_id,
        digest_path=digest_path,
        only_has_signal=config.notify_only_has_signal,
        only_whitelist_event_types=config.notify_only_whitelist_event_types,
        include_empty_summary=config.notify_include_empty_summary,
        deduplicate=config.notify_deduplicate,
        resend_existing=config.notify_resend_existing,
        limit=config.notify_limit,
        dry_run=config.notify_dry_run,
      )
    except Exception as e:
      logger.exception("[NOTIFY] exception strategy_id=%s err=%s", strategy_id, e)
      if not config.continue_on_notify_error:
        raise
      results.append(
        {
          "strategy_id": strategy_id,
          "ok": False,
          "error": str(e),
        }
      )
      continue

    logger.info(
      "[NOTIFY] ok strategy_id=%s message_count=%s",
      strategy_id,
      data.get("message_count"),
    )
    logger.debug(
      "[NOTIFY] response strategy_id=%s body=%s",
      strategy_id,
      json.dumps(data, ensure_ascii=False)[:4000],
    )

    results.append(
      {
        "strategy_id": strategy_id,
        "ok": data.get("ok", False),
        "data": data,
      }
    )

  success_count = sum(1 for x in results if x.get("ok"))
  return {
    "enabled": True,
    "ok": success_count == len(results) if results else True,
    "requested_strategies": strategy_ids,
    "success_count": success_count,
    "failed_count": len(results) - success_count,
    "results": results,
  }


def run_daily_workflow(config: WorkflowConfig) -> dict:
  started_at = _now_str(config.timezone)
  logger.info("[WORKFLOW] start at=%s tz=%s", started_at, config.timezone)

  analyze_result = run_analyze_step(config)

  if analyze_result["success"] <= 0:
    logger.error("[WORKFLOW] no analyze success, skip backtest")
    return {
      "started_at": started_at,
      "finished_at": _now_str(config.timezone),
      "analyze": analyze_result,
      "indicators": None,
      "backtest": None,
      "notify": None,
      "status": "failed",
      "reason": "no analyze success",
    }

  indicators_result = run_indicators_step(config)

  pivot_sr_result = run_pivot_sr_step(config)

  backtest_result = run_backtest_step(config)
  requested_strategies = (
    backtest_result.get("data", {}).get("requested_strategies", []) or []
  )

  if getattr(config, "notify_aggregate", "byrule") == "bystock":
    try:
      import subprocess

      script = Path(__file__).resolve().parent / "scripts" / "aggregate_signals.py"
      today = datetime.utcnow().strftime("%Y/%m/%d")
      out_dir = Path(__file__).resolve().parent / "results" / "aggregated"
      out_dir.mkdir(parents=True, exist_ok=True)
      cmd = ["python3", str(script), "--date", today, "--out", str(out_dir)]
      logger.info("[AGGREGATE] running aggregation script: %s", " ".join(cmd))
      subprocess.run(cmd, check=True)
      agg_file = (
        out_dir
        / f"market_aggregated_signal_digest_bystock_{today.replace('/', '-')}.csv"
      )
      if agg_file.exists():
        config.notify_digest_file = str(agg_file)
        requested_strategies = ["bystock"]
        logger.info("[AGGREGATE] aggregated digest generated: %s", agg_file)
      else:
        logger.warning(
          "[AGGREGATE] aggregated digest not found after script run: %s", agg_file
        )
    except Exception as e:
      logger.exception("[AGGREGATE] aggregation failed: %s", e)

  notify_result = run_notify_step(config, requested_strategies)

  generate_queue_step(config)

  finished_at = _now_str(config.timezone)
  logger.info("[WORKFLOW] finished at=%s", finished_at)
  return {
    "started_at": started_at,
    "finished_at": finished_at,
    "analyze": analyze_result,
    "indicators": indicators_result,
    "pivot_sr": pivot_sr_result,
    "backtest": backtest_result,
    "notify": notify_result,
    "status": "ok",
  }


def is_workflow_success(result: dict, config: WorkflowConfig) -> bool:
  if not result:
    return False

  if result.get("status") != "ok":
    return False

  analyze = result.get("analyze") or {}
  if analyze.get("success", 0) <= 0:
    return False

  if result.get("backtest") is None:
    return False

  notify = result.get("notify")
  if config.notify_enabled and notify is not None and notify.get("ok") is False:
    return False

  return True


def schedule_retry_job(config: WorkflowConfig, retry_count: int, reason: str) -> None:
  global scheduler

  if scheduler is None:
    logger.error("[RETRY] scheduler is None, cannot schedule retry")
    return

  if retry_count >= config.retry_max_attempts:
    logger.error(
      "[RETRY] reached max retry attempts=%s, no more retries, reason=%s",
      config.retry_max_attempts,
      reason,
    )
    return

  tz = ZoneInfo(config.timezone)
  now = datetime.now(tz)
  next_run = now + timedelta(hours=config.retry_delay_hours)

  day_key = now.strftime("%Y%m%d")
  next_retry = retry_count + 1
  retry_job_id = f"chan_daily_workflow_retry_{day_key}_{next_retry}"

  existing_job = scheduler.get_job(retry_job_id)
  if existing_job is not None:
    logger.warning(
      "[RETRY] retry job already exists id=%s next_retry=%s reason=%s",
      retry_job_id,
      next_retry,
      reason,
    )
    return

  scheduler.add_job(
    run_daily_workflow_with_retry,
    trigger="date",
    run_date=next_run,
    args=[config, next_retry],
    id=retry_job_id,
    replace_existing=False,
    coalesce=True,
    max_instances=1,
    misfire_grace_time=3600,
  )

  logger.warning(
    "[RETRY] scheduled retry #%s at=%s reason=%s job_id=%s",
    next_retry,
    next_run.strftime("%Y-%m-%d %H:%M:%S %Z"),
    reason,
    retry_job_id,
  )


def run_daily_workflow_with_retry(config: WorkflowConfig, retry_count: int = 0) -> None:
  logger.info("[RETRY] workflow entry retry_count=%s", retry_count)

  try:
    result = run_daily_workflow(config)
  except Exception as e:
    logger.exception("[RETRY] workflow exception retry_count=%s err=%s", retry_count, e)
    schedule_retry_job(config, retry_count, f"exception: {e}")
    return

  if is_workflow_success(result, config):
    logger.info("[RETRY] workflow success retry_count=%s, stop retry", retry_count)
    return

  reason = result.get("reason", "workflow result not successful")
  logger.warning(
    "[RETRY] workflow failed retry_count=%s reason=%s",
    retry_count,
    reason,
  )
  schedule_retry_job(config, retry_count, str(reason))


def parse_symbols(value: str | None) -> list[str]:
  if not value:
    return DEFAULT_SYMBOLS.copy()
  return [x.strip().upper() for x in value.split(",") if x.strip()]


def parse_levels(value: str | None) -> list[LevelType]:
  if not value:
    return DEFAULT_LEVELS.copy()
  levels = [x.strip().upper() for x in value.split(",") if x.strip()]
  allowed = set(DEFAULT_LEVELS)
  invalid = [x for x in levels if x not in allowed]
  if invalid:
    raise ValueError(f"invalid levels: {invalid}, allowed={sorted(allowed)}")
  return levels  # type: ignore[return-value]


def build_config(args: argparse.Namespace) -> WorkflowConfig:
  return WorkflowConfig(
    base_url=args.base_url,
    symbols=parse_symbols(args.symbols),
    levels=parse_levels(args.levels),
    backtest_strategy_id=args.strategy_id,
    timezone=args.timezone,
    cron_hour=args.hour,
    cron_minute=args.minute,
    request_timeout=args.timeout,
    pause_seconds=args.pause_seconds,
    continue_on_analyze_error=not args.stop_on_analyze_error,
    notify_enabled=not args.disable_notify,
    notify_dry_run=args.notify_dry_run,
    notify_only_has_signal=not args.notify_send_all,
    notify_only_whitelist_event_types=not args.notify_send_non_whitelist,
    notify_include_empty_summary=args.notify_include_empty_summary,
    notify_deduplicate=not args.notify_disable_deduplicate,
    notify_resend_existing=args.notify_resend_existing,
    notify_limit=args.notify_limit,
    notify_digest_file=args.notify_digest_file,
    continue_on_notify_error=not args.stop_on_notify_error,
    retry_delay_hours=args.retry_delay_hours,
    retry_max_attempts=args.retry_max_attempts,
  )


def main() -> None:
  global scheduler

  parser = argparse.ArgumentParser(
    description="Daily workflow scheduler: analyze -> backtest -> notify"
  )
  parser.add_argument(
    "--base-url", default="http://localhost:8000", help="Chan API base URL"
  )
  parser.add_argument(
    "--symbols", default=",".join(DEFAULT_SYMBOLS), help="Comma-separated symbols"
  )
  parser.add_argument(
    "--levels",
    default=",".join(DEFAULT_LEVELS),
    help="Comma-separated levels, e.g. 1D,4H,2H,1H",
  )
  parser.add_argument("--strategy-id", default="v7_bi", help="Backtest strategy id")
  parser.add_argument("--timezone", default="Asia/Shanghai", help="Scheduler timezone")
  parser.add_argument("--hour", type=int, default=9, help="Daily schedule hour")
  parser.add_argument("--minute", type=int, default=30, help="Daily schedule minute")
  parser.add_argument(
    "--timeout", type=int, default=300, help="Analyze request timeout seconds"
  )
  parser.add_argument(
    "--pause-seconds", type=float, default=0.2, help="Pause between analyze requests"
  )
  parser.add_argument(
    "--stop-on-analyze-error",
    action="store_true",
    help="Stop immediately when any analyze call fails",
  )

  parser.add_argument(
    "--disable-notify",
    action="store_true",
    help="Disable telegram notify step",
  )
  parser.add_argument(
    "--notify-dry-run",
    action="store_true",
    help="Notify dry run only, do not actually send telegram messages",
  )
  parser.add_argument(
    "--notify-send-all",
    action="store_true",
    help="Send all digest rows, not only has_signal=True",
  )
  parser.add_argument(
    "--notify-send-non-whitelist",
    action="store_true",
    help="Allow non-whitelist event rows",
  )
  parser.add_argument(
    "--notify-include-empty-summary",
    action="store_true",
    help="Include empty summaries like 全周期无信号",
  )
  parser.add_argument(
    "--notify-disable-deduplicate",
    action="store_true",
    help="Disable deduplication when calling telegram notify api",
  )
  parser.add_argument(
    "--notify-resend-existing",
    action="store_true",
    help="Resend existing deduplicated messages",
  )
  parser.add_argument(
    "--notify-limit",
    type=int,
    default=None,
    help="Limit notify message count for debugging",
  )
  parser.add_argument(
    "--notify-digest-file",
    default=None,
    help="Optional digest csv file path for telegram notify api",
  )
  parser.add_argument(
    "--stop-on-notify-error",
    action="store_true",
    help="Stop workflow immediately when notify step fails",
  )

  parser.add_argument(
    "--run-once", action="store_true", help="Run workflow immediately once and exit"
  )
  parser.add_argument(
    "--notify-aggregate",
    choices=["byrule", "bystock"],
    default="bystock",
    help="Notify aggregation mode: byrule (per-strategy messages) or bystock (per-symbol aggregated messages). Default: bystock",
  )
  parser.add_argument(
    "--retry-delay-hours",
    type=int,
    default=1,
    help="Retry delay hours after workflow failure. Default: 2",
  )
  parser.add_argument(
    "--retry-max-attempts",
    type=int,
    default=3,
    help="Max retry attempts after initial scheduled run failure. Default: 3",
  )

  args = parser.parse_args()

  config = build_config(args)
  config.notify_aggregate = args.notify_aggregate
  if config.notify_aggregate == "bystock":
    config.notify_only_has_signal = False

  logger.info(
    "[BOOT] config=%s notify_aggregate=%s retry_delay_hours=%s retry_max_attempts=%s",
    config,
    config.notify_aggregate,
    config.retry_delay_hours,
    config.retry_max_attempts,
  )

  if args.run_once:
    result = run_daily_workflow(config)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return

  if BlockingScheduler is None or CronTrigger is None:
    raise RuntimeError(
      "apscheduler is required for scheduler mode. Please install APScheduler."
    )

  scheduler = BlockingScheduler(timezone=config.timezone)
  scheduler.add_job(
    run_daily_workflow_with_retry,
    trigger=CronTrigger(
      day_of_week="mon-sat",
      hour=config.cron_hour,
      minute=config.cron_minute,
      timezone=config.timezone,
    ),
    args=[config, 0],
    id="chan_daily_workflow",
    replace_existing=True,
    coalesce=True,
    max_instances=1,
    misfire_grace_time=3600,
  )

  logger.info(
    "[BOOT] scheduler started base_url=%s time=%02d:%02d timezone=%s symbols=%s levels=%s strategy_id=%s notify_enabled=%s retry_delay_hours=%s retry_max_attempts=%s",
    config.base_url,
    config.cron_hour,
    config.cron_minute,
    config.timezone,
    len(config.symbols),
    config.levels,
    config.backtest_strategy_id,
    config.notify_enabled,
    config.retry_delay_hours,
    config.retry_max_attempts,
  )
  scheduler.start()


if __name__ == "__main__":
  main()
