"""Paper trading 适配器（占位）

此适配器模拟下单、持仓、资金等行为，用于 paper trading 模式。
当前为占位文件。
"""

from .exchange_interface import ExchangeAdapter
from .order_types import OrderRequest, OrderResponse, Fill
import uuid, json, os, datetime
from typing import Dict, Any


class PaperAdapter(ExchangeAdapter):
  def __init__(
    self,
    data_dir: str = "trade_system_data",
    initial_cash: float = 100000.0,
    config: Dict[str, Any] = None,
  ):
    self.data_dir = data_dir
    os.makedirs(self.data_dir, exist_ok=True)
    self.initial_cash = float(initial_cash)
    self.cash = float(initial_cash)
    self.positions = {}  # symbol -> {qty, avg_cost}
    self.orders = {}
    self.next_trade_id = 1
    # defaults
    cfg = config or {}
    self.immediate_filled = cfg.get("immediate_filled", True)
    self.slippage_pct = cfg.get("slippage_pct", 0.0)

  def get_balance(self):
    return self.cash

  def get_positions(self):
    return self.positions

  def _append_trade_log(self, trade: Dict[str, Any]):
    path = os.path.join(self.data_dir, "trades.log")
    with open(path, "a", encoding="utf-8") as f:
      f.write(json.dumps(trade, ensure_ascii=False) + "\n")

  def place_order(self, order_request: OrderRequest) -> OrderResponse:
    # simple validation
    est_cost = (order_request.price or 0.0) * order_request.qty
    if order_request.side == "BUY" and est_cost > self.cash:
      return OrderResponse(
        order_id=str(uuid.uuid4()),
        request_id=order_request.request_id,
        status="rejected",
        raw_response={"reason": "insufficient_funds"},
      )
    order_id = str(uuid.uuid4())
    if self.immediate_filled:
      price = order_request.price or 0.0
      filled_qty = order_request.qty
      total = price * filled_qty
      self.cash -= total
      pos = self.positions.get(order_request.symbol, {"qty": 0, "avg_cost": 0.0})
      # update avg cost
      prev_qty = pos["qty"]
      prev_cost = pos["avg_cost"]
      new_qty = prev_qty + filled_qty
      new_avg = ((prev_qty * prev_cost) + total) / new_qty if new_qty else 0.0
      self.positions[order_request.symbol] = {"qty": new_qty, "avg_cost": new_avg}
      trade = {
        "trade_id": str(self.next_trade_id),
        "order_id": order_id,
        "request_id": order_request.request_id,
        "symbol": order_request.symbol,
        "side": order_request.side,
        "qty": filled_qty,
        "price": price,
        "fee": 0.0,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "adapter": "paper",
      }
      self._append_trade_log(trade)
      self.next_trade_id += 1
      resp = OrderResponse(
        order_id=order_id,
        request_id=order_request.request_id,
        status="filled",
        filled_qty=filled_qty,
        avg_price=price,
        fills=[Fill(qty=filled_qty, price=price, ts=trade["timestamp"])],
      )
      return resp
    else:
      # open order
      self.orders[order_id] = {"request": order_request, "status": "open"}
      return OrderResponse(
        order_id=order_id, request_id=order_request.request_id, status="open"
      )

  def cancel_order(self, order_id: str):
    if order_id in self.orders and self.orders[order_id]["status"] == "open":
      self.orders[order_id]["status"] = "cancelled"
      return {"success": True}
    return {"success": False}
