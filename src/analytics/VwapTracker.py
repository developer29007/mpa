'''
VwapCalculator tracks VwapBuckets for different intervals

say
last 250 ms
last 1000 ms
last 2000 ms
last 5000 ms
last 10_000 ms
last 20_000 ms
'''
from analytics.VwapBucket import VwapBucket
from book.trade import Trade
from sortedcontainers import SortedDict


class VwapTracker:

    # several buckets
    # easy to create new buckets (say pass tuples which creates the buckets)

    def __init__(self, stock, *bucket_intervals):
        self.stock = stock
        self.buckets = SortedDict()
        for bucket_interval in bucket_intervals:
            self.buckets[bucket_interval] = VwapBucket(bucket_interval)

    # trade is added to all buckets
    def add_trade(self, trade: Trade):
        for bucket in self.buckets.values():
            bucket.add_trade(trade)
