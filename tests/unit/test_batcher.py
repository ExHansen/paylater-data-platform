import time

from pdp.loader.batcher import Batcher


def test_batcher_flushes_on_size():
    b = Batcher(max_size=3, max_wait_s=5)
    assert [b.add(i) for i in range(3)] == [False, False, True]
    assert b.drain() == [0, 1, 2] and b.drain() == []


def test_batcher_flushes_on_time():
    b = Batcher(max_size=100, max_wait_s=5)
    assert not b.due(time.monotonic() + 60)  # empty batch is never due
    b.add("x")
    assert not b.due(time.monotonic()) and b.due(time.monotonic() + 6)


def test_peek_keeps_items_until_drained():
    b = Batcher(max_size=10, max_wait_s=5)
    b.add(1)
    b.add(2)
    assert b.peek() == [1, 2] and len(b) == 2
