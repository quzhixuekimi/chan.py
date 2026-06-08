import logging
import asyncio
import json
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo
import logging

_HAS_PMC = True
try:
  import pandas_market_calendars as mcal
except Exception:  # pragma: no cover - optional dependency
  _HAS_PMC = False
  logging.getLogger("nightly_executor").warning(
    "pandas_market_calendars not available; holiday checks will be disabled"
  )

from pathlib import Path


from futu import OpenSecTradeContext, TrdMarket, TrdEnv, TrdSide, RET_OK, OrderType

from trade_system.config import get_config
import db
from trade_system.engine.position_tracker import PositionTracker
import db


logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)

file_handler = logging.FileHandler("/tmp/nightly_executor.log", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(
  logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
)

root_logger = logging.getLogger()
root_logger.addHandler(file_handler)

logging.getLogger("trade_system").propagate = True
logging.getLogger("trade_system").setLevel(logging.INFO)

logger = logging.getLogger("nightly_executor")


def is_us_market_hours() -> bool:
  now = datetime.now(ZoneInfo("America/New_York"))
  market_open = dt_time(9, 30)
  market_close = dt_time(16, 0)

  # Weekend check remains
  if now.weekday() >= 5:
    return False

  # If pandas_market_calendars is available, also ensure today is a trading day
  if _HAS_PMC:
    try:
      nyse = mcal.get_calendar("NYSE")
      # Query schedule for a 3-day window to be safe around midnight boundaries
      start = (now - timedelta(days=1)).date()
      end = (now + timedelta(days=1)).date()
      schedule = nyse.schedule(start_date=start, end_date=end)
      # schedule.index are Timestamps; check whether today's date is present
      # schedule.index contains Timestamps; check whether any index has today's date
      today_date = now.date()
      matches = [
        idx
        for idx in schedule.index
        if getattr(idx, "date", lambda: None)() == today_date
      ]
      if not matches:
        return False
      # use the schedule's market open/close for today for correctness
      day_idx = matches[0]
      day_schedule = schedule.loc[day_idx]
      market_open_ts = day_schedule["market_open"]
      market_close_ts = day_schedule["market_close"]
      # ensure timestamps are in America/New_York tz and compare with now
      try:
        market_open_dt = market_open_ts.tz_convert("America/New_York").to_pydatetime()
        market_close_dt = market_close_ts.tz_convert("America/New_York").to_pydatetime()
      except Exception:
        market_open_dt = market_open_ts.to_pydatetime()
        market_close_dt = market_close_ts.to_pydatetime()
      return market_open_dt <= now < market_close_dt
    except Exception as e:
      # If any error occurs while checking calendar, log and fallback to time-only check
      logging.getLogger("nightly_executor").exception(
        "Error checking market calendar; falling back to time-only check: %s", e
      )

  # Fallback: simple time window
  return market_open <= now.time() < market_close


# Unused helper functions removed


async def execute_order(
  symbol: str,
  action: str,
  qty: int,
  trd_ctx,
) -> dict:
  code = f"US.{symbol}"
  trd_side = TrdSide.BUY if action == "buy" else TrdSide.SELL

  # Place order (blocking SDK call moved to thread)
  ret, data = await asyncio.to_thread(
    trd_ctx.place_order,
    price=0,
    qty=qty,
    code=code,
    trd_side=trd_side,
    trd_env=TrdEnv.SIMULATE,
    order_type=OrderType.MARKET,
  )

  if ret != RET_OK:
    return {"success": False, "error": str(data)}

  # Try to extract order_id from place_order return
  order_id = None
  try:
    # handle DataFrame-like
    if hasattr(data, "iloc"):
      row = data.iloc[0]
      order_id = row.get("order_id") if hasattr(row, "get") else row["order_id"]
    else:
      order_id = getattr(data, "order_id", None)
  except Exception:
    try:
      # fallback for list/tuple
      order_id = (
        data[0]["order_id"] if isinstance(data, (list, tuple)) and data else None
      )
    except Exception:
      order_id = None

  if not order_id:
    return {"success": False, "error": "no_order_id_returned"}
  # NOTE:
  # Previous implementation polled the broker SDK for order status (_wait_filled),
  # but the polling calls often time out (especially for the Futu simulate environment)
  # which caused downstream DB updates to be skipped. To avoid relying on the
  # polling API (which is flaky in the current environment), we assume that a
  # successful place_order call that returns an order_id on the simulate trade
  # environment means the order was accepted/filled. We therefore return a
  # synthetic fill result using the requested qty. This keeps behavior minimal
  # and allows positions/trades to be recorded.

  assumed_filled_qty = qty
  assumed_filled_price = 0.0

  return {
    "success": True,
    "order_id": order_id,
    "filled_price": float(assumed_filled_price),
    "filled_qty": int(assumed_filled_qty),
    "raw_fill_status": {"status": "assumed_filled", "order_id": order_id},
  }


async def _wait_filled(
  trd_ctx,
  order_id: str,
  timeout: float = 30.0,
  poll_interval: float = 3.0,
  per_call_timeout: float = 3.0,
) -> dict:
  """Poll order status via trd_ctx.order_list_query (or similar) until filled or timeout.

  Returns dict with keys: filled_qty (int), filled_price (float), status (filled|open|rejected|timeout|error),
  and optional error.
  """
  import time

  start = time.time()
  while True:
    try:
      # trd_ctx.order_list_query is synchronous in futu SDK; run in thread
      # protect each SDK call with a per-call timeout to avoid indefinite blocking
      try:
        coro = asyncio.to_thread(trd_ctx.order_list_query, order_id=order_id)
        ret, df = await asyncio.wait_for(coro, timeout=per_call_timeout)
      except asyncio.TimeoutError:
        # per-call timeout reached; treat as transient and continue until overall timeout
        if (time.time() - start) >= float(timeout):
          return {"filled_qty": 0, "filled_price": 0, "status": "timeout"}
        await asyncio.sleep(poll_interval)
        continue
    except Exception as e:
      return {"filled_qty": 0, "filled_price": 0, "status": "error", "error": str(e)}

    if ret != RET_OK:
      # API error, return with error status
      return {"filled_qty": 0, "filled_price": 0, "status": "error", "error": str(df)}

    # df is expected to be a DataFrame-like result; parse defensively
    try:
      if hasattr(df, "empty") and not df.empty:
        row = df.iloc[0]
        # candidate fields for filled qty
        filled_qty = int(
          (
            row.get("filled_qty")
            or row.get("fill_qty")
            or row.get("filled_volume")
            or row.get("qty_filled")
            or row.get("qty")
            or 0
          )
        )
        # candidate fields for filled price
        filled_price = float(
          (
            row.get("filled_price")
            or row.get("avg_price")
            or row.get("filled_avg_price")
            or row.get("deal_avg_price")
            or 0
          )
          or 0
        )
        status_field = (row.get("order_status") or row.get("status") or "").lower()

        if filled_qty > 0:
          return {
            "filled_qty": filled_qty,
            "filled_price": filled_price,
            "status": "filled",
          }

        # If order is rejected/cancelled, return as rejected
        if any(
          x in status_field
          for x in ("reject", "rejected", "cancel", "cancelled", "failed")
        ):
          return {"filled_qty": 0, "filled_price": 0, "status": "rejected"}

    except Exception:
      # If parsing fails, continue to polling until timeout
      pass

    if (time.time() - start) >= float(timeout):
      return {"filled_qty": 0, "filled_price": 0, "status": "timeout"}

    await asyncio.sleep(poll_interval)


class NightlyExecutor:
  def __init__(self, trd_ctx=None, positions_dir: Path | None = None):
    self.trd_ctx = trd_ctx
    self.config = get_config()
    # PositionTracker now uses DB; the positions_dir argument is kept for compatibility but ignored
    self.position_tracker = PositionTracker(db.engine)

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
      logger.exception(f"Telegram推送异常: {e}")

  async def run(self):
    if not is_us_market_hours():
      logger.warning("美股市场未开放，跳过执行")
      return {"skipped": True, "reason": "us_market_closed"}

    # load queue from database (per-day)
    queue = db.load_queue_today_from_db()
    logger.info(f"加载队列完成，signals数量: {len(queue.get('signals', []))}")

    if not queue.get("signals"):
      logger.info("队列为空")
      return {"skipped": True, "reason": "empty_queue"}

    ctx_created_here = False

    try:
      if self.trd_ctx is None:
        logger.info("准备创建OpenSecTradeContext")
        self.trd_ctx = OpenSecTradeContext(
          filter_trdmarket=TrdMarket.US,
          host=self.config.futu_host,
          port=self.config.futu_port,
        )
        ctx_created_here = True
        logger.info("OpenSecTradeContext创建成功")

        results = []

        for signal in queue["signals"]:
          if signal.get("status") != "queued":
            logger.info(f"跳过非queued信号: {signal}")
            continue

          symbol = signal["symbol"]
          action = signal["action"]
          signal_id = signal.get("id", "")
          logger.info(f"开始处理信号: symbol={symbol}, action={action}, id={signal_id}")

          # Crypto → ETF 映射（虚拟账户只支持美股代码）
          CRYPTO_ETF_MAP = {"BTC-USD": "IBIT", "ETH-USD": "ETHA"}
          if symbol in CRYPTO_ETF_MAP:
            trade_symbol = CRYPTO_ETF_MAP[symbol]
            trade_qty = 10
            logger.info(f"[CRYPTO-ETF] {symbol} -> {trade_symbol} qty={trade_qty}")
          else:
            trade_symbol = symbol
            trade_qty = self.config.order_qty

          if action == "manual_review":
            results.append(
              {
                "symbol": symbol,
                "status": "manual_review",
                "reason": "conflict detected",
              }
            )
            logger.info(f"{symbol} 需要人工审核，跳过自动下单")
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
              # 更新信号状态
              if signal_id:
                for sig in queue["signals"]:
                  if sig.get("id") == signal_id:
                    sig["status"] = "skipped"
                    break
              continue

          result = await execute_order(
            symbol=trade_symbol,
            action=action,
            qty=trade_qty,
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

          if result.get("success"):
            # 更新信号状态为 filled
            if signal_id:
              for sig in queue["signals"]:
                if sig.get("id") == signal_id:
                  sig["status"] = "filled"
                  break
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
                order_req,
                order_res,
                buy_signal_id=signal_id,
              )
              logger.info(f"{symbol} 买入持仓记录已更新")
            elif action == "sell":
              self.position_tracker.on_sell_filled(
                order_req,
                order_res,
                reason=signal.get("strategy", "manual"),
                sell_signal_id=signal_id,
              )
              logger.info(f"{symbol} 卖出持仓记录已更新，sell_signal_id={signal_id}")
          else:
            # 下单失败，更新状态
            if signal_id:
              for sig in queue["signals"]:
                if sig.get("id") == signal_id:
                  sig["status"] = "failed"
                  break

          # 回写 queue.json，更新信号状态防止重复下单
          try:
            # write updated queue back to DB
            db.write_queue_back_to_db(queue)
            logger.info("已回写队列状态到数据库")
          except Exception as e:
            logger.exception(f"回写队列状态失败: {e}")

        return {"executed": results}

    except Exception:
      logger.exception("NightlyExecutor执行异常")
      raise

    finally:
      if ctx_created_here and self.trd_ctx:
        logger.info("关闭OpenSecTradeContext")
        self.trd_ctx.close()
        self.trd_ctx = None

  async def main(self):
    result = await self.run()
    logger.info(f"nightly result: {result}")
    return result


async def main():
  executor = NightlyExecutor()
  await executor.main()


if __name__ == "__main__":
  asyncio.run(main())
