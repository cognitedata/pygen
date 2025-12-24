import sys
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Literal

from ._references import ViewReference

if TYPE_CHECKING:
    from .filters import Filter

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class DataTypeFilter(ABC):
    """Base class for all data type filters."""

    def __init__(self, view_ref: ViewReference, property_id: str, operator: Literal["and", "or"]) -> None:
        self._view_ref = view_ref
        self._property_id = property_id
        self._operator = operator
        self._conditions: list[tuple[str, Any]] = []

    def _add_condition(self, operator: str, value: Any) -> Self:
        """Add a filter condition and return self for chaining."""
        self._conditions.append((operator, value))
        return self

    @property
    def _property_path(self) -> list[str]:
        """Get the property path for the filter."""
        return [self._view_ref.space, f"{self._view_ref.external_id}/{self._view_ref.version}", self._property_id]

    @abstractmethod
    def as_filter(self) -> Filter:
        """Convert the accumulated conditions to a Filter."""
        ...

    def _build_leaf_filter(self, operator: str, value: Any) -> dict[str, Any]:
        """Build a single leaf filter dict."""
        if operator == "eq":
            return {"equals": {"property": self._property_path, "value": value}}
        elif operator == "ne":
            return {"not": {"equals": {"property": self._property_path, "value": value}}}
        elif operator == "lt":
            return {"range": {"property": self._property_path, "lt": value}}
        elif operator == "le":
            return {"range": {"property": self._property_path, "lte": value}}
        elif operator == "gt":
            return {"range": {"property": self._property_path, "gt": value}}
        elif operator == "ge":
            return {"range": {"property": self._property_path, "gte": value}}
        elif operator == "prefix":
            return {"prefix": {"property": self._property_path, "value": value}}
        elif operator == "in":
            return {"in": {"property": self._property_path, "values": value}}
        else:
            raise ValueError(f"Unknown operator: {operator}")

    def _build_filter(self) -> Filter:
        """Build the complete filter from accumulated conditions."""
        from .filters import FilterAdapter

        if not self._conditions:
            raise ValueError("No filter conditions have been added.")

        if len(self._conditions) == 1:
            operator, value = self._conditions[0]
            filter_dict = self._build_leaf_filter(operator, value)
        else:
            leaf_filters = [self._build_leaf_filter(op, val) for op, val in self._conditions]
            filter_dict = {self._operator: leaf_filters}

        return FilterAdapter.validate_python(filter_dict)


class ComparableFilter(DataTypeFilter, ABC):
    """Base class for filters that support comparison operators."""

    @abstractmethod
    def _validate_value(self, value: Any) -> Any:
        """Validate and potentially convert the input value."""
        ...

    def equals(self, value: Any) -> Self:
        """Filter for values equal to the given value."""
        return self._add_condition("eq", self._validate_value(value))

    def not_equals(self, value: Any) -> Self:
        """Filter for values not equal to the given value."""
        return self._add_condition("ne", self._validate_value(value))

    def less_than(self, value: Any) -> Self:
        """Filter for values less than the given value."""
        return self._add_condition("lt", self._validate_value(value))

    def less_than_or_equals(self, value: Any) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_condition("le", self._validate_value(value))

    def greater_than(self, value: Any) -> Self:
        """Filter for values greater than the given value."""
        return self._add_condition("gt", self._validate_value(value))

    def greater_than_or_equals(self, value: Any) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_condition("ge", self._validate_value(value))


class FloatFilter(ComparableFilter):
    """Filter for float/numeric properties."""

    def _validate_value(self, value: float | int) -> float:
        return float(value)

    def equals(self, value: float | int) -> Self:
        """Filter for values equal to the given value."""
        return self._add_condition("eq", self._validate_value(value))

    def not_equals(self, value: float | int) -> Self:
        """Filter for values not equal to the given value."""
        return self._add_condition("ne", self._validate_value(value))

    def less_than(self, value: float | int) -> Self:
        """Filter for values less than the given value."""
        return self._add_condition("lt", self._validate_value(value))

    def less_than_or_equals(self, value: float | int) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_condition("le", self._validate_value(value))

    def greater_than(self, value: float | int) -> Self:
        """Filter for values greater than the given value."""
        return self._add_condition("gt", self._validate_value(value))

    def greater_than_or_equals(self, value: float | int) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_condition("ge", self._validate_value(value))

    def as_filter(self) -> Filter:
        """Convert the accumulated conditions to a Filter."""
        return self._build_filter()


class IntegerFilter(ComparableFilter):
    """Filter for integer properties."""

    def _validate_value(self, value: int) -> int:
        return int(value)

    def equals(self, value: int) -> Self:
        """Filter for values equal to the given value."""
        return self._add_condition("eq", self._validate_value(value))

    def not_equals(self, value: int) -> Self:
        """Filter for values not equal to the given value."""
        return self._add_condition("ne", self._validate_value(value))

    def less_than(self, value: int) -> Self:
        """Filter for values less than the given value."""
        return self._add_condition("lt", self._validate_value(value))

    def less_than_or_equals(self, value: int) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_condition("le", self._validate_value(value))

    def greater_than(self, value: int) -> Self:
        """Filter for values greater than the given value."""
        return self._add_condition("gt", self._validate_value(value))

    def greater_than_or_equals(self, value: int) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_condition("ge", self._validate_value(value))

    def as_filter(self) -> Filter:
        """Convert the accumulated conditions to a Filter."""
        return self._build_filter()


class DateTimeFilter(ComparableFilter):
    """Filter for datetime properties."""

    def _validate_value(self, value: datetime | str) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, str):
            # Validate it's a valid ISO format by parsing it
            datetime.fromisoformat(value)
            return value
        raise TypeError(f"Expected datetime or ISO format string, got {type(value)}")

    def equals(self, value: datetime | str) -> Self:
        """Filter for values equal to the given value."""
        return self._add_condition("eq", self._validate_value(value))

    def not_equals(self, value: datetime | str) -> Self:
        """Filter for values not equal to the given value."""
        return self._add_condition("ne", self._validate_value(value))

    def less_than(self, value: datetime | str) -> Self:
        """Filter for values less than the given value."""
        return self._add_condition("lt", self._validate_value(value))

    def less_than_or_equals(self, value: datetime | str) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_condition("le", self._validate_value(value))

    def greater_than(self, value: datetime | str) -> Self:
        """Filter for values greater than the given value."""
        return self._add_condition("gt", self._validate_value(value))

    def greater_than_or_equals(self, value: datetime | str) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_condition("ge", self._validate_value(value))

    def as_filter(self) -> Filter:
        """Convert the accumulated conditions to a Filter."""
        return self._build_filter()


class DateFilter(ComparableFilter):
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
        return self._add_condition("eq", self._validate_value(value))

    def not_equals(self, value: date | str) -> Self:
        """Filter for values not equal to the given value."""
        return self._add_condition("ne", self._validate_value(value))

    def less_than(self, value: date | str) -> Self:
        """Filter for values less than the given value."""
        return self._add_condition("lt", self._validate_value(value))

    def less_than_or_equals(self, value: date | str) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_condition("le", self._validate_value(value))

    def greater_than(self, value: date | str) -> Self:
        """Filter for values greater than the given value."""
        return self._add_condition("gt", self._validate_value(value))

    def greater_than_or_equals(self, value: date | str) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_condition("ge", self._validate_value(value))

    def as_filter(self) -> Filter:
        """Convert the accumulated conditions to a Filter."""
        return self._build_filter()


class TextFilter(DataTypeFilter):
    """Filter for text/string properties."""

    def equals(self, value: str) -> Self:
        """Filter for values equal to the given string."""
        return self._add_condition("eq", str(value))

    def prefix(self, value: str) -> Self:
        """Filter for values starting with the given prefix."""
        return self._add_condition("prefix", str(value))

    def in_(self, values: list[str]) -> Self:
        """Filter for values that are in the given list."""
        return self._add_condition("in", [str(v) for v in values])

    def as_filter(self) -> Filter:
        """Convert the accumulated conditions to a Filter."""
        return self._build_filter()


class BooleanFilter(DataTypeFilter):
    """Filter for boolean properties."""

    def equals(self, value: bool) -> Self:
        """Filter for values equal to the given boolean."""
        return self._add_condition("eq", bool(value))

    def as_filter(self) -> Filter:
        """Convert the accumulated conditions to a Filter."""
        return self._build_filter()
