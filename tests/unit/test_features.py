from collections import deque
from decimal import Decimal

from pdp.stream.features import AccountWindow, from_state, to_state, update

D1 = Decimal("100000")


def test_windows_count_and_prune():
    w = AccountWindow(deque(), {})
    for i in range(5):
        f = update(w, 1_000 + i * 60_000, D1, "d1")
    assert f.txn_count_10m == 5 and f.txn_count_1h == 5 and f.txn_amount_10m == 5 * D1
    f = update(w, 1_000 + 11 * 60_000, Decimal("1"), "d1")
    # the first event (t=1_000) left the 10-minute window; the one at t=61_000 is exactly 10 min old
    assert f.txn_count_10m == 5 and f.txn_count_1h == 6


def test_one_hour_window_prunes():
    w = AccountWindow(deque(), {})
    update(w, 0, D1, "d1")
    f = update(w, 3_600_001, D1, "d1")
    assert f.txn_count_1h == 1 and len(w.events) == 1


def test_new_device_flag_once():
    w = AccountWindow(deque(), {})
    assert update(w, 0, D1, "x").is_new_device
    assert not update(w, 1000, D1, "x").is_new_device
    assert update(w, 2000, D1, "y").is_new_device


def test_devices_forgotten_after_30_days():
    w = AccountWindow(deque(), {})
    update(w, 0, D1, "x")
    assert update(w, 31 * 86_400_000, D1, "x").is_new_device


def test_state_roundtrip():
    w = AccountWindow(deque(), {})
    update(w, 5, Decimal("12.50"), "x")
    back = from_state(to_state(w))
    assert back == w and isinstance(back.events[0][1], Decimal)
