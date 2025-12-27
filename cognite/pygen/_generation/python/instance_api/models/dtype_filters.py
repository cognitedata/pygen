import sys
from datetime import date, datetime
from typing import Any, Literal

from cognite.pygen._generation.python.instance_api.models.instance import InstanceId

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

    def __init__(
        self, prop_ref: ViewReference | Literal["node", "edge"], property_id: str, operator: Literal["and", "or"]
    ) -> None:
        self._prop_ref = prop_ref
        self._property_id = property_id
        self._operator = operator
        self._filters: dict[FilterTypes, FilterData] = {}

    def _add_filter(self, filter_type: FilterTypes, key: str, value: Any | None) -> Self:
        """Add a filter condition and return self for chaining."""
        if value is None:
            return self
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
        if isinstance(self._prop_ref, ViewReference):
            return [self._prop_ref.space, f"{self._prop_ref.external_id}/{self._prop_ref.version}", self._property_id]
        else:
            return [self._prop_ref, self._property_id]

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
    def __init__(
        self,
        data_type_filters: list[DataTypeFilter],
        operator: Literal["and", "or"],
        instance_type: Literal["node", "edge"],
    ) -> None:
        self._data_type_filters = data_type_filters
        self._operator = operator
        self.space = TextFilter(instance_type, "space", operator)
        self.external_id = TextFilter(instance_type, "externalId", operator)
        self.version = IntegerFilter(instance_type, "version", operator)
        self.type = DirectRelationFilter(instance_type, "type", operator)
        self.created_time = DateTimeFilter(instance_type, "createdTime", operator)
        self.last_updated_time = DateTimeFilter(instance_type, "lastUpdatedTime", operator)
        self.deleted_time = DateTimeFilter(instance_type, "deletedTime", operator)
        self._data_type_filters.extend(
            [
                self.space,
                self.external_id,
                self.version,
                self.type,
                self.created_time,
                self.last_updated_time,
                self.deleted_time,
            ]
        )
        if instance_type == "edge":
            self.start_node = DirectRelationFilter(instance_type, "startNode", operator)
            self.end_node = DirectRelationFilter(instance_type, "endNode", operator)
            self._data_type_filters.extend([self.start_node, self.end_node])

    def as_filter(self) -> Filter | None:
        """Convert the accumulated conditions to a Filter."""
        if not self._data_type_filters:
            return None

        leaf_filters = [leaf_filter for dtf in self._data_type_filters if (leaf_filter := dtf.as_filter())]
        if len(leaf_filters) == 0:
            return None
        elif len(leaf_filters) == 1:
            return leaf_filters[0]
        else:
            return {self._operator: leaf_filters}  # type: ignore[dict-item]


class FloatFilter(DataTypeFilter):
    """Filter for float/numeric properties."""

    def _validate_value(self, value: float | int | None) -> float | None:
        return float(value) if value is not None else None

    def equals(self, value: float | int | None) -> Self:
        """Filter for values equal to the given value."""
        return self._add_filter("equals", "value", self._validate_value(value))

    def less_than(self, value: float | int | None) -> Self:
        """Filter for values less than the given value."""
        return self._add_filter("range", "lt", self._validate_value(value))

    def less_than_or_equals(self, value: float | int | None) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_filter("range", "lte", self._validate_value(value))

    def greater_than(self, value: float | int | None) -> Self:
        """Filter for values greater than the given value."""
        return self._add_filter("range", "gt", self._validate_value(value))

    def greater_than_or_equals(self, value: float | int | None) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_filter("range", "gte", self._validate_value(value))


class IntegerFilter(DataTypeFilter):
    """Filter for integer properties."""

    def _validate_value(self, value: int | None) -> int | None:
        return int(value) if value is not None else None

    def equals(self, value: int | None) -> Self:
        """Filter for values equal to the given value."""
        return self._add_filter("equals", "value", self._validate_value(value))

    def less_than(self, value: int | None) -> Self:
        """Filter for values less than the given value."""
        return self._add_filter("range", "lt", self._validate_value(value))

    def less_than_or_equals(self, value: int | None) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_filter("range", "lte", self._validate_value(value))

    def greater_than(self, value: int | None) -> Self:
        """Filter for values greater than the given value."""
        return self._add_filter("range", "gt", self._validate_value(value))

    def greater_than_or_equals(self, value: int | None) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_filter("range", "gte", self._validate_value(value))


class DateTimeFilter(DataTypeFilter):
    """Filter for datetime properties."""

    def _validate_value(self, value: datetime | str | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat(timespec="milliseconds")
        if isinstance(value, str):
            # Validate it's a valid ISO format by parsing it
            for format in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S.%f%Z"]:
                try:
                    return datetime.strptime(value, format).isoformat(timespec="milliseconds")
                except ValueError:
                    continue
            raise ValueError(f"String '{value}' is not a valid ISO format datetime.")
        raise TypeError(f"Expected datetime or ISO format string, got {type(value)}")

    def equals(self, value: datetime | str | None) -> Self:
        """Filter for values equal to the given value."""
        return self._add_filter("equals", "value", self._validate_value(value))

    def less_than(self, value: datetime | str | None) -> Self:
        """Filter for values less than the given value."""
        return self._add_filter("range", "lt", self._validate_value(value))

    def less_than_or_equals(self, value: datetime | str | None) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_filter("range", "lte", self._validate_value(value))

    def greater_than(self, value: datetime | str | None) -> Self:
        """Filter for values greater than the given value."""
        return self._add_filter("range", "gt", self._validate_value(value))

    def greater_than_or_equals(self, value: datetime | str | None) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_filter("range", "gte", self._validate_value(value))


class DateFilter(DataTypeFilter):
    """Filter for date properties."""

    def _validate_value(self, value: date | str | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, str):
            # Validate it's a valid ISO format by parsing it
            date.fromisoformat(value)
            return value
        raise TypeError(f"Expected date or ISO format string, got {type(value)}")

    def equals(self, value: date | str | None) -> Self:
        """Filter for values equal to the given value."""
        return self._add_filter("equals", "value", self._validate_value(value))

    def less_than(self, value: date | str | None) -> Self:
        """Filter for values less than the given value."""
        return self._add_filter("range", "lt", self._validate_value(value))

    def less_than_or_equals(self, value: date | str | None) -> Self:
        """Filter for values less than or equal to the given value."""
        return self._add_filter("range", "lte", self._validate_value(value))

    def greater_than(self, value: date | str | None) -> Self:
        """Filter for values greater than the given value."""
        return self._add_filter("range", "gt", self._validate_value(value))

    def greater_than_or_equals(self, value: date | str | None) -> Self:
        """Filter for values greater than or equal to the given value."""
        return self._add_filter("range", "gte", self._validate_value(value))


class TextFilter(DataTypeFilter):
    """Filter for text/string properties."""

    def _validate_value(self, value: str | list[str] | None) -> str | list[str] | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return [str(v) for v in value]

    def equals(self, value: str | None) -> Self:
        """Filter for values equal to the given string."""
        return self._add_filter("equals", "value", self._validate_value(value))

    def prefix(self, value: str | None) -> Self:
        """Filter for values starting with the given prefix."""
        return self._add_filter("prefix", "value", self._validate_value(value))

    def in_(self, values: list[str] | None) -> Self:
        """Filter for values that are in the given list."""
        return self._add_filter("in", "values", self._validate_value(values))

    def equals_or_in(self, value: str | list[str] | None) -> Self:
        """Filter for values equal to the given string or in the given list."""
        if isinstance(value, list):
            return self.in_(value)
        else:
            return self.equals(value)


class BooleanFilter(DataTypeFilter):
    """Filter for boolean properties."""

    def equals(self, value: bool | None) -> Self:
        """Filter for values equal to the given boolean."""
        return self._add_filter("equals", "value", value if value is None else bool(value))


class DirectRelationFilter(DataTypeFilter):
    """Filter for direct relation properties."""

    def _validate_value(
        self,
        value: str | InstanceId | tuple[str, str] | None,
        space: str | None = None,
    ) -> dict[str, str] | None:
        if value is None:
            return None
        elif isinstance(value, tuple) and len(value) == 2:
            return {"space": value[0], "externalId": value[1]}
        elif isinstance(value, InstanceId):
            return value.model_dump(exclude={"instance_type"}, by_alias=True)
        elif isinstance(value, str):
            if space is None:
                raise ValueError("Space must be provided when value is a string.")
            return {"space": space, "externalId": value}
        else:
            raise TypeError(f"Expected str, InstanceId, tuple[str, str], or list thereof, got {type(value)}")

    def equals(self, value: str | InstanceId | tuple[str, str] | None, space: str | None = None) -> Self:
        """Filter for values equal to the given relation ID."""
        return self._add_filter("equals", "value", self._validate_value(value, space))

    def in_(self, values: list[str | InstanceId | tuple[str, str]] | None, space: str | None = None) -> Self:
        """Filter for values that are in the given list of relation IDs."""
        return self._add_filter(
            "in", "values", [self._validate_value(value, space) for value in values] if values is not None else None
        )

    def equals_or_in(
        self,
        value: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]] | None,
        space: str | None = None,
    ) -> Self:
        """Filter for values equal to the given relation ID or in the given list."""
        if isinstance(value, list):
            return self.in_(value, space)
        else:
            return self.equals(value, space)
