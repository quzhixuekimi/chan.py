"""撮合引擎（占位）

可选组件：用于高仿真度的订单簿穿透与撮合模拟。如果仅做简单 paper trading，可以暂时保持空实现。
"""


class MatchingEngine:
  def __init__(self):
    pass

  def on_new_order(self, order):
    raise NotImplementedError
