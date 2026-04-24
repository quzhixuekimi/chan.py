"""统一交易所适配器接口（占位）

该文件仅为接口占位，后续实现应在此处定义 ExchangeAdapter 抽象类，
并在 futu_adapter.py / paper_adapter.py 中实现具体适配器。
"""

from abc import ABC, abstractmethod


class ExchangeAdapter(ABC):
  @abstractmethod
  def get_balance(self):
    raise NotImplementedError

  @abstractmethod
  def get_positions(self):
    raise NotImplementedError

  @abstractmethod
  def place_order(self, *args, **kwargs):
    raise NotImplementedError

  @abstractmethod
  def cancel_order(self, order_id: str):
    raise NotImplementedError
