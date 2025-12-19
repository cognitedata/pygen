from __future__ import annotations

import datetime
from abc import ABC
from typing import Any, ClassVar, Generic, Sequence, TypeVar

from cognite.client import data_modeling as dm

from wind_turbine.data_classes._core.helpers import as_instance_dict_id


T_QueryCore = TypeVar("T_QueryCore")


class Filtering(Generic[T_QueryCore], ABC):
    counter: ClassVar[int] = 0

    def __init__(self, query: T_QueryCore, prop_path: list[str] | tuple[str, ...]) -> None:
        self._query = query
        self._prop_path = prop_path
        self._filter: dm.Filter | None = None
        self._sort: dm.InstanceSort | None = None
        self._sort_priority: int | None = None
        # Used for earliest/latest
        self._limit: int | None = None

    def _raise_if_filter_set(self):
        if self._filter is not None:
            raise ValueError("Filter has already been set")

    def _raise_if_sort_set(self):
        if self._sort is not None:
            raise ValueError("Sort has already been set")

    @classmethod
    def _get_sort_priority(cls) -> int:
        # This is used in case of multiple sorts, to ensure that the order is correct
        Filtering.counter += 1
        return Filtering.counter

    def _as_filter(self) -> dm.Filter | None:
        return self._filter

    def _as_sort(self) -> tuple[dm.InstanceSort | None, int]:
        return self._sort, self._sort_priority or 0

    @property
    def _has_limit_1(self) -> bool:
        return self._limit == 1

    def sort_ascending(self) -> T_QueryCore:
        self._raise_if_sort_set()
        self._sort = dm.InstanceSort(self._prop_path, "ascending")
        self._sort_priority = self._get_sort_priority()
        return self._query

    def sort_descending(self) -> T_QueryCore:
        self._raise_if_sort_set()
        self._sort = dm.InstanceSort(self._prop_path, "descending")
        self._sort_priority = self._get_sort_priority()
        return self._query


class StringFilter(Filtering[T_QueryCore]):
    def equals(self, value: str) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Equals(self._prop_path, value)
        return self._query

    def prefix(self, prefix: str) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Prefix(self._prop_path, prefix)
        return self._query

    def in_(self, values: list[str]) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.In(self._prop_path, values)
        return self._query


class BooleanFilter(Filtering[T_QueryCore]):
    def equals(self, value: bool) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Equals(self._prop_path, value)
        return self._query


class IntFilter(Filtering[T_QueryCore]):
    def range(self, gte: int | None, lte: int | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(self._prop_path, gte=gte, lte=lte)
        return self._query


class FloatFilter(Filtering[T_QueryCore]):
    def range(self, gte: float | None, lte: float | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(self._prop_path, gte=gte, lte=lte)
        return self._query


class TimestampFilter(Filtering[T_QueryCore]):
    def range(self, gte: datetime.datetime | None, lte: datetime.datetime | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(
            self._prop_path,
            gte=gte.isoformat(timespec="milliseconds") if gte else None,
            lte=lte.isoformat(timespec="milliseconds") if lte else None,
        )
        return self._query

    def earliest(self) -> T_QueryCore:
        self._raise_if_sort_set()
        self._sort = dm.InstanceSort(self._prop_path, "ascending")
        self._sort_priority = self._get_sort_priority()
        self._limit = 1
        return self._query

    def latest(self) -> T_QueryCore:
        self._raise_if_sort_set()
        self._sort = dm.InstanceSort(self._prop_path, "descending")
        self._sort_priority = self._get_sort_priority()
        self._limit = 1
        return self._query


class DateFilter(Filtering[T_QueryCore]):
    def range(self, gte: datetime.date | None, lte: datetime.date | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(
            self._prop_path,
            gte=gte.isoformat() if gte else None,
            lte=lte.isoformat() if lte else None,
        )
        return self._query

    def earliest(self) -> T_QueryCore:
        self._raise_if_sort_set()
        self._sort = dm.InstanceSort(self._prop_path, "ascending")
        self._sort_priority = self._get_sort_priority()
        self._limit = 1
        return self._query

    def latest(self) -> T_QueryCore:
        self._raise_if_sort_set()
        self._sort = dm.InstanceSort(self._prop_path, "descending")
        self._sort_priority = self._get_sort_priority()
        self._limit = 1
        return self._query


class DirectRelationFilter(Filtering[T_QueryCore]):
    def equals(self, value: str | dm.NodeId | tuple[str, str] | dm.DirectRelationReference | Any) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Equals(self._prop_path, as_instance_dict_id(value))
        return self._query

    def in_(
        self, values: Sequence[str | dm.NodeId | tuple[str, str] | dm.DirectRelationReference | Any]
    ) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.In(self._prop_path, [as_instance_dict_id(value) for value in values])
        return self._query
