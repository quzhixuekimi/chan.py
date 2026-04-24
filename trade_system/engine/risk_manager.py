"""风控管理（占位）

用于订单前风控检查、仓位限制、总持仓暴露等。
"""


class RiskManager:
  def __init__(self, config=None):
    self.config = config

  def validate_order(self, order):
    raise NotImplementedError
