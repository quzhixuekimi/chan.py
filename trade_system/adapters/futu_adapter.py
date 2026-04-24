"""富途适配器（占位）

在此文件中实现对 futu-api 的封装，保证遵循 exchange_interface.ExchangeAdapter 的方法签名。
当前为占位文件，不包含具体实现。
"""

from .exchange_interface import ExchangeAdapter


class FutuAdapter(ExchangeAdapter):
    def __init__(self, *args, **kwargs):
        # 延迟导入 futu 库，避免导入时出错
        pass

    def get_balance(self):
        raise NotImplementedError

    def get_positions(self):
        raise NotImplementedError

    def place_order(self, *args, **kwargs):
        raise NotImplementedError

    def cancel_order(self, order_id: str):
        raise NotImplementedError
