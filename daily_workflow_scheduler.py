from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Literal

import requests
from zoneinfo import ZoneInfo

try:
  from apscheduler.schedulers.blocking import BlockingScheduler
  from apscheduler.triggers.cron import CronTrigger
except Exception:
  BlockingScheduler = None
  CronTrigger = None

LevelType = Literal["1D", "4H", "2H", "1H"]

BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = "/tmp/daily_workflow_scheduler.log"

DEFAULT_SYMBOLS = ["AAPL", "TSLA", "NVDA"]
DEFAULT_LEVELS: list[LevelType] = ["1D", "4H", "2H", "1H"]


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
  notify_base_url: str = "http://127.0.0.1:8010"
  notify_timeout: int = 120
  notify_dry_run: bool = False
  notify_only_has_signal: bool = True
  notify_limit: int | None = None
  notify_digest_file: str | None = None
  continue_on_notify_error: bool = True


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

  notify_url = f"{config.notify_base_url.rstrip('/')}/api/notify/telegram/send-digest"
  results: list[dict] = []

  for strategy_id in strategy_ids:
    payload = {
      "strategy_id": strategy_id,
      "digest_file": config.notify_digest_file,
      "only_has_signal": config.notify_only_has_signal,
      "limit": config.notify_limit,
      "dry_run": config.notify_dry_run,
    }

    logger.info(
      "[NOTIFY] request url=%s strategy_id=%s dry_run=%s only_has_signal=%s limit=%s digest_file=%s",
      notify_url,
      strategy_id,
      config.notify_dry_run,
      config.notify_only_has_signal,
      config.notify_limit,
      config.notify_digest_file,
    )

    try:
      resp = _post_json(notify_url, payload, timeout=config.notify_timeout)
    except Exception as e:
      logger.exception("[NOTIFY] exception strategy_id=%s err=%s", strategy_id, e)
      if not config.continue_on_notify_error:
        raise
      results.append(
        {
          "strategy_id": strategy_id,
          "ok": False,
          "status_code": None,
          "error": str(e),
        }
      )
      continue

    if not resp.ok:
      logger.error(
        "[NOTIFY] fail strategy_id=%s status=%s response=%s",
        strategy_id,
        resp.status_code,
        resp.text[:2000],
      )
      if not config.continue_on_notify_error:
        raise RuntimeError(
          f"notify failed: strategy_id={strategy_id}, status={resp.status_code}, response={resp.text[:2000]}"
        )
      results.append(
        {
          "strategy_id": strategy_id,
          "ok": False,
          "status_code": resp.status_code,
          "response": resp.text[:2000],
        }
      )
      continue

    try:
      data = resp.json()
    except Exception:
      data = {"raw": resp.text[:2000]}

    logger.info("[NOTIFY] ok strategy_id=%s status=%s", strategy_id, resp.status_code)
    logger.debug(
      "[NOTIFY] response strategy_id=%s body=%s",
      strategy_id,
      json.dumps(data, ensure_ascii=False)[:4000],
    )

    results.append(
      {
        "strategy_id": strategy_id,
        "ok": True,
        "status_code": resp.status_code,
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
      "backtest": None,
      "notify": None,
      "status": "failed",
      "reason": "no analyze success",
    }

  backtest_result = run_backtest_step(config)
  requested_strategies = (
    backtest_result.get("data", {}).get("requested_strategies", []) or []
  )
  notify_result = run_notify_step(config, requested_strategies)

  finished_at = _now_str(config.timezone)
  logger.info("[WORKFLOW] finished at=%s", finished_at)
  return {
    "started_at": started_at,
    "finished_at": finished_at,
    "analyze": analyze_result,
    "backtest": backtest_result,
    "notify": notify_result,
    "status": "ok",
  }


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
    notify_base_url=args.notify_base_url,
    notify_timeout=args.notify_timeout,
    notify_dry_run=args.notify_dry_run,
    notify_only_has_signal=not args.notify_send_all,
    notify_limit=args.notify_limit,
    notify_digest_file=args.notify_digest_file,
    continue_on_notify_error=not args.stop_on_notify_error,
  )


def main() -> None:
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
    "--notify-base-url",
    default="http://127.0.0.1:8010",
    help="Telegram notify API base URL",
  )
  parser.add_argument(
    "--notify-timeout",
    type=int,
    default=120,
    help="Notify request timeout seconds",
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
  args = parser.parse_args()

  config = build_config(args)
  logger.info("[BOOT] config=%s", config)

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
    run_daily_workflow,
    trigger=CronTrigger(
      hour=config.cron_hour, minute=config.cron_minute, timezone=config.timezone
    ),
    args=[config],
    id="chan_daily_workflow",
    replace_existing=True,
    coalesce=True,
    max_instances=1,
  )

  logger.info(
    "[BOOT] scheduler started base_url=%s time=%02d:%02d timezone=%s symbols=%s levels=%s strategy_id=%s notify_enabled=%s notify_base_url=%s",
    config.base_url,
    config.cron_hour,
    config.cron_minute,
    config.timezone,
    len(config.symbols),
    config.levels,
    config.backtest_strategy_id,
    config.notify_enabled,
    config.notify_base_url,
  )
  scheduler.start()


if __name__ == "__main__":
  main()
