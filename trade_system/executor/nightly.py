import json
import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import NamedTuple, Literal
import uuid
import asyncio
import requests

from futu import OpenSecTradeContext, TrdMarket, TrdEnv, TrdSide, RET_OK

from trade_system.config import get_config
from trade_system.queue.writer import load_queue_today
from trade_system.engine.position_tracker import PositionTracker


logger = logging.getLogger("nightly_executor")


class MarketQuote(NamedTuple):
  symbol: str
  price: float
  bid: float
  ask: float


def is_us_market_hours() -> bool:
  now = datetime.now(ZoneInfo("America/New_York"))
  market_open = time(9, 30)
  market_close = time(16, 0)
  current_time = now.time()
  if now.weekday() >= 5:
    return False
  return market_open <= current_time < market_close


async def get_realtime_quote(symbol: str, trd_ctx) -> MarketQuote | None:
  code = f"US.{symbol}"
  ret, data = await asyncio.to_thread(trd_ctx.get_stock_quote, code)
  if ret != RET_OK:
    logger.error(f"获取行情失败: {symbol}, {data}")
    return None
  return MarketQuote(
    symbol=symbol,
    price=data.iloc[0]["latest_price"],
    bid=data.iloc[0]["bid_price"],
    ask=data.iloc[0]["ask_price"],
  )


async def execute_order(
  symbol: str,
  action: Literal["buy", "sell"],
  qty: int,
  trd_ctx,
) -> dict:
  code = f"US.{symbol}"
  trd_side = TrdSide.BUY if action == "buy" else TrdSide.SELL
  ret, data = await asyncio.to_thread(
    trd_ctx.place_order,
    price=0,
    qty=qty,
    code=code,
    trd_side=trd_side,
    trd_env=TrdEnv.SIMULATE,
  )
  if ret == RET_OK:
    return {
      "success": True,
      "order_id": data.iloc[0]["order_id"],
      "filled_price": data.iloc[0].get("price", 0),
      "filled_qty": data.iloc[0].get("qty", 0),
    }
  else:
    return {
      "success": False,
      "error": str(data),
    }


class NightlyExecutor:
  def __init__(self, trd_ctx=None, positions_dir: Path | None = None):
    self.trd_ctx = trd_ctx
    self.config = get_config()
    self.positions_dir = positions_dir or Path("trade_system/data/positions")
    self.position_tracker = PositionTracker(self.positions_dir)

  def _push_telegram(self, symbol: str, action: str, result: dict, signal: dict):
    from trade_system.notifiers.telegram import telegram_send_message

    try:
      title = f"{'✅ 买入' if action == 'buy' else '🔴 卖出'} {symbol}"
      msg = (
        f"{title}\n"
        f"策略: {signal.get('strategy', 'N/A')}\n"
        f"周期: {signal.get('period', 'N/A')}\n"
        f"价格: {result.get('filled_price', 'N/A')}\n"
        f"数量: {result.get('filled_qty', 'N/A')}\n"
        f"订单ID: {result.get('order_id', 'N/A')}"
      )
      telegram_send_message(msg)
      logger.info(f"Telegram推送成功: {symbol} {action}")
    except Exception as e:
      logger.error(f"Telegram推送异常: {e}")

    async def run(self):
      if not is_us_market_hours():
        logger.warning("美股市场未开放，跳过执行")
        return {"skipped": True, "reason": "us_market_closed"}
      queue = load_queue_today()
      if not queue.get("signals"):
        logger.info("队列为空")
        return {"skipped": True, "reason": "empty_queue"}
      if self.trd_ctx is None:
        self.trd_ctx = OpenSecTradeContext(
          filter_trdmarket=TrdMarket.US,
          host=self.config.futu_host,
          port=self.config.futu_port,
        )
      results = []
      for signal in queue["signals"]:
        if signal.get("status") != "queued":
          continue
        symbol = signal["symbol"]
        action = signal["action"]
        if action == "manual_review":
          results.append(
            {
              "symbol": symbol,
              "status": "manual_review",
              "reason": "conflict detected",
            }
          )
          continue

        if action == "sell":
          open_positions = self.position_tracker.get_open_positions(symbol=symbol)
          if not open_positions:
            logger.warning(f"{symbol} 卖出信号但无未平仓买入记录，跳过")
            results.append(
              {
                "symbol": symbol,
                "status": "skipped",
                "reason": "no open position for sell",
              }
            )
            continue

        result = await execute_order(
          symbol=symbol,
          action=action,
          qty=self.config.order_qty,
          trd_ctx=self.trd_ctx,
        )
        results.append(
          {
            "symbol": symbol,
            "signal": signal,
            "result": result,
          }
        )
        logger.info(f"订单结果: {symbol} {action} -> {result}")

        # 推送 telegram + 追踪持仓
        if result.get("success"):
          self._push_telegram(symbol, action, result, signal)

          order_req = type(
            "OrderReq",
            (),
            {
              "symbol": symbol,
              "strategy": signal.get("strategy", ""),
              "period": signal.get("period", ""),
            },
          )()
          order_res = type(
            "OrderRes",
            (),
            {
              "order_id": result.get("order_id", ""),
              "filled_price": result.get("filled_price", 0),
              "filled_qty": result.get("filled_qty", 0),
            },
          )()

          if action == "buy":
            self.position_tracker.on_buy_filled(
              order_req, order_res, queue_id=signal.get("id", "")
            )
          elif action == "sell":
            self.position_tracker.on_sell_filled(
              order_req, order_res, reason=signal.get("strategy", "manual")
            )

      if self.trd_ctx:
        self.trd_ctx.close()
      return {"executed": results}
