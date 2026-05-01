"""风控管理

用于订单前风控检查、仓位限制、总持仓暴露等。
"""

from typing import Optional, Dict
from datetime import datetime


class RiskManager:
  def __init__(self, config=None):
    self.config = config
    self.daily_limits: Dict[str, int] = {}
    self.position_limits: Dict[str, int] = {}
    self.max_order_value: float = (
      getattr(config, "max_order_value", 100000.0) if config else 100000.0
    )
    self.max_daily_orders: int = (
      getattr(config, "max_daily_orders", 10) if config else 10
    )

  def validate_order(self, order) -> bool:
    if order.price and order.quantity:
      order_value = order.price * order.quantity
      if order_value > self.max_order_value:
        return False

    today = datetime.now().strftime("%Y-%m-%d")
    key = f"{today}:{order.symbol}"
    if key in self.daily_limits:
      if self.daily_limits[key] >= self.max_daily_orders:
        return False

    return True

  def record_order(self, order) -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    key = f"{today}:{order.symbol}"
    self.daily_limits[key] = self.daily_limits.get(key, 0) + 1

  def check_position_limit(self, symbol: str, current_qty: int, new_qty: int) -> bool:
    limit = self.position_limits.get(symbol, float("inf"))
    return (current_qty + new_qty) <= limit
