import sys
from datetime import date, datetime
from typing import Any, Literal

from ._references import ViewReference
from .filters import (
    Filter,
    FilterAdapter,
    FilterData,
    FilterTypes,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class DataTypeFilter:
    """Base class for all data type filters."""

    def __init__(self, view_ref: ViewReference, property_id: str, operator: Literal["and", "or"]) -> None:
        self._view_ref = view_ref
        self._property_id = property_id
        self._operator = operator
        self._filters: dict[FilterTypes, FilterData] = {}

    def _add_filter(self, filter_type: FilterTypes, key: str, value: Any) -> Self:
        """Add a filter condition and return self for chaining."""
        if filter_type in self._filters:
            filter_data = self._filters[filter_type]
            self._filters[filter_type] = filter_data.model_copy(update={key: value})
        else:
            filter_ = FilterAdapter.validate_python({filter_type: {"property": self._property_path, key: value}})
            self._filters.update(filter_)
        return self

    @property
    def _property_path(self) -> list[str]:
        """Get the property path for the filter."""
        return [self._view_ref.space, f"{self._view_ref.external_id}/{self._view_ref.version}", self._property_id]

    def as_filter(self) -> Filter | None:
        """Convert the accumulated conditions to a Filter."""
        if not self._filters:
            return None

        if len(self._filters) == 1:
            return self._filters

        leaf_filters = [{filter_type: value} for filter_type, value in self._filters.items()]
        return {self._operator: leaf_filters}  # type: ignore[dict-item]

    def dump(self, exclude_none: bool = False) -> dict[str, Any]:
        """Dump the filter to a dictionary."""
        filter_obj = self.as_filter()
        if filter_obj is None:
            return {}
        return FilterAdapter.dump_python(filter_obj, exclude_none=exclude_none)


class FilterContainer:
    def __init__(self, data_type_filters: list[DataTypeFilter], operator: Literal["and", "or"]) -> None:
        self._data_type_filters = data_type_filters
        self._operator = operator

    def as_filter(self) -> Filter | None:
        """Convert the accumulated conditions to a Filter."""
        if not self._data_type_filters:
            raise ValueError("No data type filters have been added.")

        leaf_filters = [leaf_filter for dtf in self._data_type_filters if (leaf_filter := dtf.as_filter())]
        if len(leaf_filters) == 0:
            return None
        elif len(leaf_filters) == 1:
            return leaf_filters[0]
        else:
            return {self._operator: leaf_filters}  # type: ignore[dict-item]


class FloatFilter(DataTypeFilter):
    """Filter for float/numeric properties."""

    def _validate_value(self, value: float | int) -> float:
        return float(value)

    def equals(self, value: float | int) -> Self:
        """Filter for values equal to the given value."""
        return self._add_filter("equals", "value", self._validate_value(value))

    def less_than(self, value: float | int) -> Self:
        """Filter for values less than the given value."""
        return self._add_filter("range", "lt", self._validate_value(value))

    def less_than_or_equals(self, value: float | int) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_filter("range", "lte", self._validate_value(value))

    def greater_than(self, value: float | int) -> Self:
        """Filter for values greater than the given value."""
        return self._add_filter("range", "gt", self._validate_value(value))

    def greater_than_or_equals(self, value: float | int) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_filter("range", "gte", self._validate_value(value))


class IntegerFilter(DataTypeFilter):
    """Filter for integer properties."""

    def _validate_value(self, value: int) -> int:
        return int(value)

    def equals(self, value: int) -> Self:
        """Filter for values equal to the given value."""
        return self._add_filter("equals", "value", self._validate_value(value))

    def less_than(self, value: int) -> Self:
        """Filter for values less than the given value."""
        return self._add_filter("range", "lt", self._validate_value(value))

    def less_than_or_equals(self, value: int) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_filter("range", "lte", self._validate_value(value))

    def greater_than(self, value: int) -> Self:
        """Filter for values greater than the given value."""
        return self._add_filter("range", "gt", self._validate_value(value))

    def greater_than_or_equals(self, value: int) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_filter("range", "gte", self._validate_value(value))


class DateTimeFilter(DataTypeFilter):
    """Filter for datetime properties."""

    def _validate_value(self, value: datetime | str) -> str:
        if isinstance(value, datetime):
            return value.isoformat(timespec="milliseconds")
        if isinstance(value, str):
            # Validate it's a valid ISO format by parsing it
            datetime.fromisoformat(value)
            return value
        raise TypeError(f"Expected datetime or ISO format string, got {type(value)}")

    def equals(self, value: datetime | str) -> Self:
        """Filter for values equal to the given value."""
        return self._add_filter("equals", "value", self._validate_value(value))

    def less_than(self, value: datetime | str) -> Self:
        """Filter for values less than the given value."""
        return self._add_filter("range", "lt", self._validate_value(value))

    def less_than_or_equals(self, value: datetime | str) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_filter("range", "lte", self._validate_value(value))

    def greater_than(self, value: datetime | str) -> Self:
        """Filter for values greater than the given value."""
        return self._add_filter("range", "gt", self._validate_value(value))

    def greater_than_or_equals(self, value: datetime | str) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_filter("range", "gte", self._validate_value(value))


class DateFilter(DataTypeFilter):
    """Filter for date properties."""

    def _validate_value(self, value: date | str) -> str:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, str):
            # Validate it's a valid ISO format by parsing it
            date.fromisoformat(value)
            return value
        raise TypeError(f"Expected date or ISO format string, got {type(value)}")

    def equals(self, value: date | str) -> Self:
        """Filter for values equal to the given value."""
        return self._add_filter("equals", "value", self._validate_value(value))

    def less_than(self, value: date | str) -> Self:
        """Filter for values less than the given value."""
        return self._add_filter("range", "lt", self._validate_value(value))

    def less_than_or_equals(self, value: date | str) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_filter("range", "lte", self._validate_value(value))

    def greater_than(self, value: date | str) -> Self:
        """Filter for values greater than the given value."""
        return self._add_filter("range", "gt", self._validate_value(value))

    def greater_than_or_equals(self, value: date | str) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_filter("range", "gte", self._validate_value(value))


class TextFilter(DataTypeFilter):
    """Filter for text/string properties."""

    def equals(self, value: str) -> Self:
        """Filter for values equal to the given string."""
        return self._add_filter("equals", "value", str(value))

    def prefix(self, value: str) -> Self:
        """Filter for values starting with the given prefix."""
        return self._add_filter("prefix", "value", str(value))

    def in_(self, values: list[str]) -> Self:
        """Filter for values that are in the given list."""
        return self._add_filter("in", "values", [str(v) for v in values])


class BooleanFilter(DataTypeFilter):
    """Filter for boolean properties."""

    def equals(self, value: bool) -> Self:
        """Filter for values equal to the given boolean."""
        return self._add_filter("equals", "value", bool(value))
