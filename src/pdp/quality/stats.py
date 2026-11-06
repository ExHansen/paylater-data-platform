"""Small statistics helpers for the data-quality checks."""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence


def zscore(value: float, baseline: Sequence[float]) -> float | None:
    """How many standard deviations `value` is from the baseline; None if undefined."""
    if len(baseline) < 3:
        return None
    stdev = statistics.pstdev(baseline)
    if stdev == 0:
        return None
    return (value - statistics.fmean(baseline)) / stdev


def pct_diff(expected: float, observed: float) -> float:
    """Relative difference |observed - expected| / expected (1.0 when expected is 0)."""
    if expected == 0:
        return 0.0 if observed == 0 else 1.0
    return abs(observed - expected) / abs(expected)


def volume_zscore(
    counts: Mapping[int, int], current_window: int, baseline_windows: int
) -> float | None:
    """z-score of the current window's event count against the previous windows.

    Windows before the first window with traffic are ignored, so a pipeline that just
    started is not compared against a stretch of empty windows.
    """
    previous = range(current_window - baseline_windows, current_window)
    active = [w for w in previous if counts.get(w, 0) > 0]
    if not active:
        return None
    baseline = [counts.get(w, 0) for w in previous if w >= active[0]]
    return zscore(counts.get(current_window, 0), baseline)
