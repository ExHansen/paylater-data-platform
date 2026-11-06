"""Collect messages until the batch is big enough or old enough to flush."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any


class Batcher:
    def __init__(
        self,
        max_size: int = 500,
        max_wait_s: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.max_size = max_size
        self.max_wait_s = max_wait_s
        self._clock = clock
        self._items: list[Any] = []
        self._first_at: float | None = None

    def __len__(self) -> int:
        return len(self._items)

    def add(self, item: Any) -> bool:
        """Add an item; True when the batch is full and should be flushed."""
        if not self._items:
            self._first_at = self._clock()
        self._items.append(item)
        return len(self._items) >= self.max_size

    def due(self, now: float | None = None) -> bool:
        if not self._items or self._first_at is None:
            return False
        return (now if now is not None else self._clock()) - self._first_at >= self.max_wait_s

    def peek(self) -> list[Any]:
        return list(self._items)

    def drain(self) -> list[Any]:
        items, self._items, self._first_at = self._items, [], None
        return items
