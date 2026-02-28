"""
timeseries — Backend-Agnostic Time-Series Store
================================================
User API::

    from timeseries import Timeseries

    ts = Timeseries("demo")
    await ts.start()
    bars = ts.get_bars("equity", "AAPL", interval="1m")
    await ts.stop()

Platform API lives in ``timeseries.admin``.
Internal: TSDBBackend ABC, models, factory, consumer.
"""

from timeseries.base import TSDBBackend
from timeseries.models import Bar, HistoryQuery, BarQuery
from timeseries.factory import create_backend
from timeseries.consumer import TSDBConsumer
from timeseries.client import Timeseries

__all__ = [
    "Timeseries",
    "TSDBBackend",
    "Bar",
    "HistoryQuery",
    "BarQuery",
    "create_backend",
    "TSDBConsumer",
]
