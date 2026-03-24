import heapq
from typing import Optional, Self

from util import TimerListener


class Timer:

    def __init__(self, interval: int, scheduled_time: int, timer_listener: TimerListener):
        self.time_interval: int = interval
        self.scheduled_time: int = scheduled_time
        self.expired_time: int = interval + scheduled_time
        self.timer_listener: TimerListener = timer_listener
        self.is_removed: bool = False

    # since Timer lives in a heap, we need to define __lt__
    def __lt__(self, other: Self) -> bool:
        return self.expired_time < other.expired_time


class TimerService:

    _instance: Optional['TimerService'] = None

    def __new__(cls) -> 'TimerService':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.timers = []
            cls._instance.timer_map = {}
        return cls._instance

    @classmethod
    def instance(cls) -> 'TimerService':
        if cls._instance is None:
            cls()
        return cls._instance

    def add_timer(self, time_interval: int, current_time: int, timer_listener: TimerListener):
        """
        add_timer adds the timer for the timer_listener. if a Timer already exists
        for the given timer_listener, previous timer is marked removed and a new timer is added.
        """
        prev_timer = self.timer_map.get(timer_listener)
        if prev_timer:
            prev_timer.is_removed = True
        new_timer = Timer(time_interval, current_time, timer_listener)
        self.timer_map[timer_listener] = new_timer
        heapq.heappush(self.timers, new_timer)

    def check_timers(self, current_time: int) -> bool:
        """Fire all expired timers and return True if at least one timer fired."""
        fired = False
        while self.timers:

            if self.timers[0].is_removed:
                heapq.heappop(self.timers)
                continue

            if self.timers[0].expired_time > current_time:
                return fired

            head: Timer = heapq.heappop(self.timers)
            del self.timer_map[head.timer_listener]
            head.timer_listener.on_timer_expired(head.time_interval, head.scheduled_time, current_time)
            fired = True

        return fired
