"""Query-related data structures for sorting, units, and debug information.

This module contains data classes used for configuring instance queries including:
- Sorting: Order query results by property values
- Units: Convert property values to target units
- Debug: Include additional debug information in responses
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class PropertySort(BaseModel):
    """Sort configuration for a property.

    This corresponds to the CDF API InstanceSort structure for sorting query results
    by property values.

    Args:
        property: The property path to sort by. Can be:
            - A list of strings for view properties: [<space>, <view/version>, <property>]
            - A list of strings for node/edge properties: ["node", "externalId"] or ["edge", "type"]
        direction: Sort direction - "ascending" or "descending". Defaults to "ascending".
        nulls_first: Whether null values should appear first. If not specified, null values
            are sorted last for ascending and first for descending.

    Examples:
        Sort by a view property in ascending order:

        >>> sort = PropertySort(
        ...     property=["my_space", "MyView/v1", "name"],
        ...     direction="ascending"
        ... )

        Sort by external_id with nulls first:

        >>> sort = PropertySort(
        ...     property=["node", "externalId"],
        ...     direction="descending",
        ...     nulls_first=True
        ... )
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
    )

    property: list[str] = Field(..., min_length=2, max_length=3)
    direction: Literal["ascending", "descending"] = "ascending"
    nulls_first: bool | None = Field(default=None, alias="nullsFirst")


class UnitConversion(BaseModel):
    """Unit conversion configuration for a property.

    Use this to specify a target unit for numeric properties that have units defined
    in the container. The API will automatically convert values to the target unit.

    Args:
        property: The property path to convert. Must be a 3-element list:
            [<space>, <view_external_id/version>, <property>]
        target_unit: The target unit to convert to. Must be a valid unit from the
            same unit catalog as the source property's unit.

    Examples:
        Convert temperature from Celsius to Fahrenheit:

        >>> unit = UnitConversion(
        ...     property=["my_space", "Sensor/v1", "temperature"],
        ...     target_unit="temperature:fah"
        ... )
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
    )

    property: list[str] = Field(..., min_length=3, max_length=3)
    target_unit: str = Field(..., alias="targetUnit")


class PropertyWithUnits(BaseModel):
    """A property value along with its unit information.

    This is returned when debug mode is enabled and includes the unit information
    for properties that have units defined.

    Args:
        value: The property value (converted if unit conversion was requested).
        unit: The unit information containing the external_id of the unit.
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )

    value: float | int | list[float] | list[int] | None = None
    unit: dict[str, str] | None = None  # Contains {"externalId": "unit:identifier"}


class DebugInfo(BaseModel):
    """Debug information for a query response.

    When debug mode is enabled in list/iterate/search operations, the response includes
    additional metadata about the query execution.

    Args:
        request_items_limit: The maximum number of items that could be returned.
        query_time_ms: Time spent processing the query in milliseconds.
        parse_time_ms: Time spent parsing the request in milliseconds.
        serialize_time_ms: Time spent serializing the response in milliseconds.
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )

    request_items_limit: int | None = Field(default=None, alias="requestItemsLimit")
    query_time_ms: float | None = Field(default=None, alias="queryTimeMs")
    parse_time_ms: float | None = Field(default=None, alias="parseTimeMs")
    serialize_time_ms: float | None = Field(default=None, alias="serializeTimeMs")
