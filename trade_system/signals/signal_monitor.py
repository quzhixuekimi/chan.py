"""信号监控（占位）

此模块负责从 CChan 或回测结果中读取信号，转化为订单请求并提交给 OrderManager。
"""


class SignalMonitor:
  def __init__(self, chan=None):
    self.chan = chan

  def scan(self):
    """扫描并产生订单请求（占位）"""
    raise NotImplementedError
