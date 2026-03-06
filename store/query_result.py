"""
QueryResult — paginated query result container.

Used by ActiveRecordMixin.query() and Storable.query() for return types.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Generic, TypeVar

_T = TypeVar("_T")


class QueryResult(Generic[_T]):
    """Result of a paginated query. Contains items and an optional next_cursor."""

    def __init__(self, items: list[_T], next_cursor: Any = None) -> None:
        self.items = items
        self.next_cursor = next_cursor

    def __iter__(self) -> Iterator[_T]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, index: int) -> _T:
        return self.items[index]
