"""订单管理

负责接收订单请求、分配ID、变更订单状态，并发送到 executor/adapter。
"""

from typing import Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime
from uuid import uuid4


@dataclass
class OrderRequest:
  id: str = field(default_factory=lambda: str(uuid4()))
  symbol: str = ""
  side: str = ""  # "BUY" or "SELL"
  quantity: int = 0
  price: Optional[float] = None
  order_type: str = "MARKET"  # MARKET or LIMIT
  status: str = "PENDING"  # PENDING, SENT, FILLED, CANCELLED, FAILED
  created_at: str = field(default_factory=lambda: datetime.now().isoformat())
  updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
  queue_id: Optional[str] = None
  strategy: str = ""
  period: str = ""


@dataclass
class OrderResponse:
  order_id: str
  success: bool
  filled_price: Optional[float] = None
  filled_quantity: int = 0
  status: str = ""
  message: str = ""
  raw: Optional[dict] = None


class OrderManager:
  def __init__(self, executor=None, risk_manager=None):
    self.executor = executor
    self.risk_manager = risk_manager
    self.orders: Dict[str, OrderRequest] = {}
    self.responses: Dict[str, OrderResponse] = {}

  def submit_order(self, order: OrderRequest) -> OrderResponse:
    """Submit order after risk check."""
    self.orders[order.id] = order
    order.status = "PENDING"
    order.updated_at = datetime.now().isoformat()

    # Risk check
    if self.risk_manager:
      if not self.risk_manager.validate_order(order):
        order.status = "REJECTED"
        return OrderResponse(
          order_id=order.id,
          success=False,
          status="REJECTED",
          message="Risk check failed",
        )

    # Send to executor
    if self.executor:
      order.status = "SENT"
      order.updated_at = datetime.now().isoformat()
      response = self.executor.execute(order)
      self.responses[order.id] = response
      return response

    return OrderResponse(
      order_id=order.id,
      success=False,
      status="FAILED",
      message="No executor configured",
    )

  def cancel_order(self, order_id: str) -> bool:
    """Cancel a pending order."""
    if order_id not in self.orders:
      return False
    order = self.orders[order_id]
    if order.status not in ("PENDING", "SENT"):
      return False
    order.status = "CANCELLED"
    order.updated_at = datetime.now().isoformat()
    return True

  def get_order(self, order_id: str) -> Optional[OrderRequest]:
    return self.orders.get(order_id)

  def update_order_status(self, order_id: str, status: str) -> None:
    if order_id in self.orders:
      self.orders[order_id].status = status
      self.orders[order_id].updated_at = datetime.now().isoformat()
