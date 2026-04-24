"""订单管理（占位）

负责接收订单请求、分配ID、变更订单状态，并发送到 executor/adapter。
"""

class OrderManager:
    def __init__(self):
        pass

    def submit_order(self, order):
        raise NotImplementedError

    def cancel_order(self, order_id: str):
        raise NotImplementedError
