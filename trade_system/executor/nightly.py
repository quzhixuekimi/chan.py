import logging
import asyncio
import json
from datetime import datetime, time
from pathlib import Path
from typing import NamedTuple, Literal
from zoneinfo import ZoneInfo

from futu import OpenSecTradeContext, TrdMarket, TrdEnv, TrdSide, RET_OK, OrderType

from trade_system.config import get_config
from trade_system.queue.writer import load_queue_today
from trade_system.engine.position_tracker import PositionTracker


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
    order_type=OrderType.MARKET,
  )

  if ret != RET_OK:
    return {
      "success": False,
      "error": str(data),
    }

  row = data.iloc[0]
  order_id = row["order_id"]

  return {
    "success": True,
    "order_id": order_id,
    "filled_price": row.get("dealt_avg_price", 0) or row.get("price", 0),
    "filled_qty": row.get("dealt_qty", 0),
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
      logger.exception(f"Telegram推送异常: {e}")

  async def run(self):
    if not is_us_market_hours():
      logger.warning("美股市场未开放，跳过执行")
      return {"skipped": True, "reason": "us_market_closed"}

    queue = load_queue_today()
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
              logger.info(
                f"{symbol} 卖出持仓记录已更新，sell_signal_id={signal_id}"
              )
          else:
            # 下单失败，更新状态
            if signal_id:
              for sig in queue["signals"]:
                if sig.get("id") == signal_id:
                  sig["status"] = "failed"
                  break

        # 回写 queue.json，更新信号状态防止重复下单
        try:
          from trade_system.queue.writer import _get_queue_dir

          today = datetime.now().strftime("%Y%m%d")
          queue_path = _get_queue_dir() / f"{today}-queue.json"
          queue_path.parent.mkdir(parents=True, exist_ok=True)
          queue_path.write_text(
            json.dumps(queue, ensure_ascii=False, indent=2), encoding="utf-8"
          )
          logger.info(f"已回写队列状态: {queue_path}")
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
