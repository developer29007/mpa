from unittest.mock import MagicMock

from util.TimerListener import TimerListener
from util.TimerService import TimerService


def test_time_service():
    mock_timer_listener: TimerListener = MagicMock(spec=TimerListener)
    timer_service = TimerService()

    timer_service.add_timer(10, 100, timer_listener=mock_timer_listener)
    timer_service.check_timers(current_time=120)
    mock_timer_listener.on_timer_expired.assert_called_once_with(10, 100, 120)

def test_update_timer():
    mock_timer_listener: TimerListener = MagicMock(spec=TimerListener)
    timer_service = TimerService()

    timer_service.add_timer(10, 100, timer_listener=mock_timer_listener)
    timer_service.add_timer(30, 110, timer_listener=mock_timer_listener)
    timer_service.check_timers(current_time=120)
    mock_timer_listener.on_timer_expired.assert_not_called()
    timer_service.check_timers(current_time=130)
    mock_timer_listener.on_timer_expired.assert_not_called()
    timer_service.check_timers(current_time=140)
    mock_timer_listener.on_timer_expired.assert_called_once_with(30, 110, 140)


def test_timer_not_fired_before_expiry():
    # timer should not fire if current_time has not reached expired_time
    mock_timer_listener: TimerListener = MagicMock(spec=TimerListener)
    timer_service = TimerService()

    timer_service.add_timer(10, 100, timer_listener=mock_timer_listener)  # expired_time = 110
    timer_service.check_timers(current_time=109)
    mock_timer_listener.on_timer_expired.assert_not_called()


def test_timer_fired_at_exact_expiry():
    # boundary: expired_time == current_time should fire (condition is expired_time > current_time)
    mock_timer_listener: TimerListener = MagicMock(spec=TimerListener)
    timer_service = TimerService()

    timer_service.add_timer(10, 100, timer_listener=mock_timer_listener)  # expired_time = 110
    timer_service.check_timers(current_time=110)
    mock_timer_listener.on_timer_expired.assert_called_once_with(10, 100, 110)


def test_multiple_listeners_fired_in_order():
    # multiple listeners with different expiry times should fire in expiry order
    mock_listener_1: TimerListener = MagicMock(spec=TimerListener)
    mock_listener_2: TimerListener = MagicMock(spec=TimerListener)
    timer_service = TimerService()

    timer_service.add_timer(30, 100, timer_listener=mock_listener_1)  # expired_time = 130
    timer_service.add_timer(10, 100, timer_listener=mock_listener_2)  # expired_time = 110

    timer_service.check_timers(current_time=120)
    mock_listener_2.on_timer_expired.assert_called_once_with(10, 100, 120)  # fires first
    mock_listener_1.on_timer_expired.assert_not_called()                    # not yet expired

    timer_service.check_timers(current_time=140)
    mock_listener_1.on_timer_expired.assert_called_once_with(30, 100, 140)  # now fires


def test_timer_removed_from_map_after_firing():
    # after a timer fires, it should be cleaned up from timer_map
    mock_timer_listener: TimerListener = MagicMock(spec=TimerListener)
    timer_service = TimerService()

    timer_service.add_timer(10, 100, timer_listener=mock_timer_listener)  # expired_time = 110
    timer_service.check_timers(current_time=120)

    assert mock_timer_listener not in timer_service.timer_map


def test_no_timers_does_not_crash():
    # check_timers on empty service should be a no-op
    timer_service = TimerService()
    timer_service.check_timers(current_time=100)  # should not raise
