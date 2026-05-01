from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
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

DEFAULT_SYMBOLS = [
  "AAPL",
  "TSLA",
  "NVDA",
  "COIN",
  "PLTR",
]
DEFAULT_LEVELS: list[LevelType] = ["1D", "4H", "2H", "1H"]

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
  notify_base_url: str = "http://127.0.0.1:8010"
  notify_timeout: int = 120
  notify_dry_run: bool = False
  notify_only_has_signal: bool = True
  notify_only_whitelist_event_types: bool = True
  notify_include_empty_summary: bool = False
  notify_deduplicate: bool = True
  notify_resend_existing: bool = False
  notify_limit: int | None = None
  notify_digest_file: str | None = None
  continue_on_notify_error: bool = True

  retry_delay_hours: int = 2
  retry_max_attempts: int = 3


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


def _push_queue_to_telegram(config: WorkflowConfig):
  try:
    from trade_system.queue.writer import load_queue_today
    from trade_system.notifiers.telegram import telegram_send_message

    queue = load_queue_today()
    signals = queue.get("signals", [])
    if not signals:
      return
    buy_count = sum(1 for s in signals if s.get("action") == "buy")
    sell_count = sum(1 for s in signals if s.get("action") == "sell")
    manual_count = sum(1 for s in signals if s.get("action") == "") + sum(
      1 for s in signals if s.get("action") == "manual_review"
    )
    lines = [
      f"📋 今日交易队列 ({queue.get('generated_at', '')[:10]})",
      f"共 {len(signals)} 个信号",
      f"买入: {buy_count} | 卖出: {sell_count} | 待人工: {manual_count}",
      "",
    ]
    for sig in signals[:10]:
      action_emoji = {"buy": "✅", "sell": "🔴", "manual_review": "⚠️"}.get(
        sig.get("action"), "❓"
      )
      lines.append(
        f"{action_emoji} {sig.get('symbol', 'N/A')} {sig.get('action', 'N/A')} {sig.get('strategy', '')} {sig.get('period', '')}"
      )
    if len(signals) > 10:
      lines.append(f"...还有 {len(signals) - 10} 个信号")
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

  notify_url = f"{config.notify_base_url.rstrip('/')}/api/notify/telegram/send-digest"
  results: list[dict] = []

  for strategy_id in strategy_ids:
    payload = {
      "strategy_id": strategy_id,
      "digest_file": config.notify_digest_file,
      "only_has_signal": config.notify_only_has_signal,
      "only_whitelist_event_types": config.notify_only_whitelist_event_types,
      "include_empty_summary": config.notify_include_empty_summary,
      "deduplicate": config.notify_deduplicate,
      "resend_existing": config.notify_resend_existing,
      "limit": config.notify_limit,
      "dry_run": config.notify_dry_run,
      "aggregate_mode": getattr(config, "notify_aggregate", None),
    }

    logger.info(
      "[NOTIFY] request url=%s strategy_id=%s dry_run=%s only_has_signal=%s only_whitelist_event_types=%s include_empty_summary=%s deduplicate=%s resend_existing=%s limit=%s digest_file=%s",
      notify_url,
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

    logger.info(
      "[NOTIFY] ok strategy_id=%s status=%s message_count=%s",
      strategy_id,
      resp.status_code,
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

  generate_queue_step(config)

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
    notify_base_url=args.notify_base_url,
    notify_timeout=args.notify_timeout,
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
    default=2,
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
    "[BOOT] scheduler started base_url=%s time=%02d:%02d timezone=%s symbols=%s levels=%s strategy_id=%s notify_enabled=%s notify_base_url=%s retry_delay_hours=%s retry_max_attempts=%s",
    config.base_url,
    config.cron_hour,
    config.cron_minute,
    config.timezone,
    len(config.symbols),
    config.levels,
    config.backtest_strategy_id,
    config.notify_enabled,
    config.notify_base_url,
    config.retry_delay_hours,
    config.retry_max_attempts,
  )
  scheduler.start()


if __name__ == "__main__":
  main()
