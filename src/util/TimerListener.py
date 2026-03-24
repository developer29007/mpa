from abc import ABC, abstractmethod


class TimerListener(ABC):

    @abstractmethod
    def on_timer_expired(self, time_interval: int, scheduled_time: int, time_now: int):
        pass