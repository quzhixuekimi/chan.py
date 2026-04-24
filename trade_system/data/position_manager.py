"""持仓管理（占位）"""


class PositionManager:
  def __init__(self):
    self.positions = {}

  def update_from_fill(self, fill):
    raise NotImplementedError
