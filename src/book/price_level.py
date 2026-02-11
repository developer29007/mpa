from book.order import Order

'''
Represents one price level of a stock order book.

'''


class PriceLevel:

    def __init__(self, price):
        self.price = price
        self.size = 0
        self.timestamp_ns: int = 0
        self.orders: dict[int, Order] = {}

    def add_order(self, order: Order):
        self.orders[order.id] = order
        self.size += order.shares
        self.timestamp_ns = max(self.timestamp_ns, order.timestamp_ns)

    def delete_order(self, id, timestamp_ns):
        order = self.orders.pop(id)
        if order:
            self.size -= order.shares
            self.timestamp_ns = max(self.timestamp_ns, timestamp_ns)

    def order_executed(self, id, exec_qty, timestamp_ns):
        order = self.orders.get(id)
        if order:
            order.shares -= exec_qty
            self.size -= exec_qty
            self.timestamp_ns = max(self.timestamp_ns, timestamp_ns)
            if order.shares <= 0:
                del self.orders[id]

    def order_cancelled(self, id, cancelled_shares, timestamp_ns):
        order = self.orders.get(id)
        if order:
            order.shares -= cancelled_shares
            self.size -= cancelled_shares
            self.timestamp_ns = max(self.timestamp_ns, timestamp_ns)
            if order.shares <= 0:
                del self.orders[id]

    def is_empty(self) -> bool:
        return len(self.orders) == 0
