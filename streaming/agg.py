"""
streaming.agg — Aggregation helpers re-exported from Deephaven.

Usage::

    from streaming import agg

    summary = live.agg_by([
        agg.sum(["TotalMV=MarketValue"]),
        agg.avg(["AvgGamma=Gamma"]),
        agg.count("NumPositions"),
    ])

All functions are thin wrappers that defer the ``deephaven`` import
until first call (the JVM must be running).
"""

from __future__ import annotations

from typing import Any


def _dh_agg() -> Any:
    import platform
    if platform.system() == "Linux" and platform.machine() in ("aarch64", "arm64"):
        from pydeephaven import agg as _agg
    else:
        from deephaven import agg as _agg
    return _agg


# -- primary aggregations used in the codebase ----------------------------

def sum(cols: list[str]) -> Any:
    """Sum columns.  Wraps ``deephaven.agg.sum_``."""
    return _dh_agg().sum_(cols)


def avg(cols: list[str]) -> Any:
    """Average columns.  Wraps ``deephaven.agg.avg``."""
    return _dh_agg().avg(cols)


def count(col: str) -> Any:
    """Count rows.  Wraps ``deephaven.agg.count_``."""
    return _dh_agg().count_(col)


def min(cols: list[str]) -> Any:
    """Min columns.  Wraps ``deephaven.agg.min_``."""
    return _dh_agg().min_(cols)


def max(cols: list[str]) -> Any:
    """Max columns.  Wraps ``deephaven.agg.max_``."""
    return _dh_agg().max_(cols)


def first(cols: list[str]) -> Any:
    """First value per group.  Wraps ``deephaven.agg.first``."""
    return _dh_agg().first(cols)


def last(cols: list[str]) -> Any:
    """Last value per group.  Wraps ``deephaven.agg.last``."""
    return _dh_agg().last(cols)


def std(cols: list[str]) -> Any:
    """Standard deviation.  Wraps ``deephaven.agg.std``."""
    return _dh_agg().std(cols)


def var(cols: list[str]) -> Any:
    """Variance.  Wraps ``deephaven.agg.var``."""
    return _dh_agg().var(cols)


def median(cols: list[str]) -> Any:
    """Median.  Wraps ``deephaven.agg.median``."""
    return _dh_agg().median(cols)
