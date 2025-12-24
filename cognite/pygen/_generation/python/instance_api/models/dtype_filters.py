from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from ._references import ViewReference


@dataclass(frozen=True)
class FilterCondition:
    """Represents a filter condition that can be applied to a query."""

    view_ref: ViewReference
    property_id: str
    operator: str
    value: Any


class DataTypeFilter:
    def __init__(self, view_ref: ViewReference, property_id: str) -> None:
        self.view_ref = view_ref
        self.property_id = property_id

    def _create_condition(self, operator: str, value: Any) -> FilterCondition:
        return FilterCondition(
            view_ref=self.view_ref,
            property_id=self.property_id,
            operator=operator,
            value=value,
        )


class ComparableFilter(DataTypeFilter, ABC):
    """Base class for filters that support comparison operators."""

    @abstractmethod
    def _validate_value(self, value: Any) -> Any:
        """Validate and potentially convert the input value."""
        ...

    def eq(self, value: Any) -> FilterCondition:
        """Filter for values equal to the given value."""
        return self._create_condition("eq", self._validate_value(value))

    def ne(self, value: Any) -> FilterCondition:
        """Filter for values not equal to the given value."""
        return self._create_condition("ne", self._validate_value(value))

    def lt(self, value: Any) -> FilterCondition:
        """Filter for values less than the given value."""
        return self._create_condition("lt", self._validate_value(value))

    def le(self, value: Any) -> FilterCondition:
        """Filter for values less than or equal to the given value."""
        return self._create_condition("le", self._validate_value(value))

    def gt(self, value: Any) -> FilterCondition:
        """Filter for values greater than the given value."""
        return self._create_condition("gt", self._validate_value(value))

    def ge(self, value: Any) -> FilterCondition:
        """Filter for values greater than or equal to the given value."""
        return self._create_condition("ge", self._validate_value(value))


class FloatFilter(ComparableFilter):
    """Filter for float/numeric properties."""

    def _validate_value(self, value: Any) -> float:
        return float(value)


class IntegerFilter(ComparableFilter):
    """Filter for integer properties."""

    def _validate_value(self, value: Any) -> int:
        return int(value)


class DateTimeFilter(ComparableFilter):
    """Filter for datetime properties."""

    def _validate_value(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        raise TypeError(f"Expected datetime or ISO format string, got {type(value)}")


class DateFilter(ComparableFilter):
    """Filter for date properties."""

    def _validate_value(self, value: Any) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value)
        raise TypeError(f"Expected date or ISO format string, got {type(value)}")


class TextFilter(DataTypeFilter):
    """Filter for text/string properties."""

    def eq(self, value: str) -> FilterCondition:
        """Filter for values equal to the given string."""
        return self._create_condition("eq", str(value))

    def prefix(self, value: str) -> FilterCondition:
        """Filter for values starting with the given prefix."""
        return self._create_condition("prefix", str(value))

    def in_(self, values: list[str]) -> FilterCondition:
        """Filter for values that are in the given list."""
        return self._create_condition("in", [str(v) for v in values])


class BooleanFilter(DataTypeFilter):
    """Filter for boolean properties."""

    def eq(self, value: bool) -> FilterCondition:
        """Filter for values equal to the given boolean."""
        return self._create_condition("eq", bool(value))
