"""Paper trading 适配器（占位）

此适配器模拟下单、持仓、资金等行为，用于 paper trading 模式。
当前为占位文件。
"""

from .exchange_interface import ExchangeAdapter


class PaperAdapter(ExchangeAdapter):
    def __init__(self, initial_cash=100000):
        self.initial_cash = initial_cash

    def get_balance(self):
        raise NotImplementedError

    def get_positions(self):
        raise NotImplementedError

    def place_order(self, *args, **kwargs):
        raise NotImplementedError

    def cancel_order(self, order_id: str):
        raise NotImplementedError
