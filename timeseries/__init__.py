"""
timeseries — Backend-Agnostic Time-Series Store
================================================
Public API: TSDBBackend ABC, models, factory, and consumer.
Backend-specific code lives in timeseries/backends/<name>/.
"""

from timeseries.base import TSDBBackend
from timeseries.models import Bar, HistoryQuery, BarQuery
from timeseries.factory import create_backend
from timeseries.consumer import TSDBConsumer

__all__ = [
    "TSDBBackend",
    "Bar",
    "HistoryQuery",
    "BarQuery",
    "create_backend",
    "TSDBConsumer",
]
