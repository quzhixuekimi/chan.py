"""执行器

负责把由 OrderManager 接收的订单发送给 ExchangeAdapter（futu 或 paper adapter）。
"""

from typing import Optional, Dict, Any
from datetime import datetime


class OrderResult:
  def __init__(
    self,
    success: bool,
    order_id: str = "",
    filled_price: float = 0.0,
    filled_qty: int = 0,
    status: str = "",
    message: str = "",
    raw: Optional[Dict[str, Any]] = None,
  ):
    self.success = success
    self.order_id = order_id
    self.filled_price = filled_price
    self.filled_qty = filled_qty
    self.status = status
    self.message = message
    self.raw = raw or {}


class Executor:
  def __init__(self, adapter):
    self.adapter = adapter

  def execute(self, order) -> OrderResult:
    """Execute an OrderRequest via the configured adapter and return OrderResult."""
    if not self.adapter:
      return OrderResult(
        success=False,
        status="NO_ADAPTER",
        message="No adapter configured",
      )
    try:
      result = self.adapter.place_order(order)
      return OrderResult(
        success=result.get("success", False),
        order_id=str(result.get("order_id", "")),
        filled_price=float(result.get("filled_price", 0)),
        filled_qty=int(result.get("filled_qty", 0)),
        status="FILLED" if result.get("success") else "FAILED",
        message=str(result.get("error", "")),
        raw=result,
      )
    except Exception as e:
      return OrderResult(
        success=False,
        status="ERROR",
        message=str(e),
      )
