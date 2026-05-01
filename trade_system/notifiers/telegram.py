import os
import logging
from typing import Optional

import requests

from trade_system.config import get_config

logger = logging.getLogger(__name__)


def _get_telegram_token() -> str:
  config = get_config()
  token = config.telegram_bot_token
  if not token:
    raise ValueError("TELEGRAM_BOT_TOKEN 环境变量未设置")
  return token


def _get_chat_id() -> str:
  config = get_config()
  chat_id = config.telegram_chat_id
  if not chat_id:
    raise ValueError("TELEGRAM_CHAT_ID 环境变量未设置")
  return chat_id


def _telegram_api_url(method: str) -> str:
  token = _get_telegram_token()
  return f"https://api.telegram.org/bot{token}/{method}"


def telegram_send_message(
  text: str,
  chat_id: Optional[str] = None,
  parse_mode: Optional[str] = None,
  disable_web_page_preview: bool = True,
) -> dict:
  url = _telegram_api_url("sendMessage")
  if chat_id is None:
    chat_id = _get_chat_id()
  payload = {
    "chat_id": chat_id,
    "text": text,
    "disable_web_page_preview": disable_web_page_preview,
  }
  if parse_mode:
    payload["parse_mode"] = parse_mode

  try:
    resp = requests.post(url, json=payload, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
      logger.error(f"Telegram API 返回错误: {data}")
      raise RuntimeError(f"Telegram API error: {data}")
    return data
  except requests.RequestException as e:
    logger.error(f"Telegram 发送失败: {e}")
    raise


def telegram_test() -> dict:
  url = _telegram_api_url("getMe")
  resp = requests.get(url, timeout=20)
  resp.raise_for_status()
  data = resp.json()
  if not data.get("ok"):
    raise RuntimeError(f"Telegram getMe failed: {data}")
  return data


def format_signal_message(symbol: str, action: str, signal: dict) -> str:
  emoji = "✅" if action == "buy" else "🔴"
  action_text = "买入" if action == "buy" else "卖出"

  lines = [
    f"{emoji} {action_text} {symbol}",
    f"策略: {signal.get('strategy', 'N/A')}",
    f"周期: {signal.get('period', 'N/A')}",
    f"状态: {signal.get('status', 'N/A')}",
  ]

  if signal.get("target_price"):
    lines.append(f"目标价: {signal.get('target_price')}")
  if signal.get("stop_price"):
    lines.append(f"止损价: {signal.get('stop_price')}")

  return "\n".join(lines)


def format_queue_summary(queue_data: dict) -> str:
  signals = queue_data.get("signals", [])
  if not signals:
    return "📭 今日无交易信号"

  buy_signals = [s for s in signals if s.get("action") == "buy"]
  sell_signals = [s for s in signals if s.get("action") == "sell"]
  manual_review = [s for s in signals if s.get("status") == "manual_review"]

  lines = [
    f"📊 今日交易信号 ({len(signals)} 条)",
    f"  买入: {len(buy_signals)}",
    f"  卖出: {len(sell_signals)}",
    f"  待人工: {len(manual_review)}",
    "",
  ]

  if buy_signals:
    lines.append("【买入信号】")
    for s in buy_signals:
      lines.append(f"  • {s.get('symbol')} [{s.get('strategy')}] {s.get('period')}")

  if sell_signals:
    lines.append("")
    lines.append("【卖出信号】")
    for s in sell_signals:
      lines.append(f"  • {s.get('symbol')} [{s.get('strategy')}] {s.get('period')}")

  if manual_review:
    lines.append("")
    lines.append("【待人工审核】")
    for s in manual_review:
      lines.append(f"  • {s.get('symbol')} [{s.get('strategy')}] {s.get('period')}")

  return "\n".join(lines)


def notify_queue(queue_data: dict) -> dict:
  text = format_queue_summary(queue_data)
  result = telegram_send_message(text)
  return result


def notify_signals(signals: list[dict]) -> list[dict]:
  results = []
  for sig in signals:
    symbol = sig.get("symbol", "")
    action = sig.get("action", "")
    if not symbol or not action:
      continue

    text = format_signal_message(symbol, action, sig)
    try:
      result = telegram_send_message(text)
      results.append({"symbol": symbol, "action": action, "result": result})
      logger.info(f"已推送信号: {symbol} {action}")
    except Exception as e:
      logger.error(f"推送信号失败: {symbol} {action}, 错误: {e}")
      results.append({"symbol": symbol, "action": action, "error": str(e)})

  return results

