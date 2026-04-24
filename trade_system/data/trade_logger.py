"""交易日志（占位）"""


class TradeLogger:
  def __init__(self, path=None):
    self.path = path

  def log(self, record):
    raise NotImplementedError
