"""执行器（占位）

负责把由 OrderManager 接收的订单发送给 ExchangeAdapter（futu 或 paper adapter）。
"""


class Executor:
  def __init__(self, adapter):
    self.adapter = adapter

  def execute(self, order):
    """Execute an OrderRequest via the configured adapter and return OrderResponse."""
    return self.adapter.place_order(order)
