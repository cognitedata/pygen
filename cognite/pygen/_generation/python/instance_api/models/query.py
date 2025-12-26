"""Query-related data structures for sorting, units, and debug information.

This module contains data classes used for configuring instance queries including:
- Sorting: Order query results by property values
- Units: Convert property values to target units
- Debug: Include additional debug information in responses
- Aggregation: Aggregate data across instances
"""

from typing import Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class QueryParameters(BaseModel):
    """Base class for query parameters.

    This class can be extended to include common query parameters
    used across different instance queries.
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
    )


# =============================================================================
# Aggregation Types
# =============================================================================


class MetricAggregation(QueryParameters):
    """Base class for metric aggregations.

    Metric aggregations compute numeric metrics over property values.
    """

    property: str


class Count(MetricAggregation):
    """Count aggregation.

    Counts the number of instances (or non-null values for a property).

    Args:
        property: The property to count. Use "externalId" for counting all instances.

    Examples:
        Count all instances:

        >>> agg = Count(property="externalId")

        Count non-null values of a specific property:

        >>> agg = Count(property="name")
    """

    aggregate: Literal["count"] = "count"


class Sum(MetricAggregation):
    """Sum aggregation.

    Computes the sum of numeric property values.

    Args:
        property: The numeric property to sum.

    Examples:
        >>> agg = Sum(property="price")
    """

    aggregate: Literal["sum"] = "sum"


class Avg(MetricAggregation):
    """Average aggregation.

    Computes the average of numeric property values.

    Args:
        property: The numeric property to average.

    Examples:
        >>> agg = Avg(property="temperature")
    """

    aggregate: Literal["avg"] = "avg"


class Min(MetricAggregation):
    """Minimum aggregation.

    Finds the minimum value of a property.

    Args:
        property: The property to find the minimum of.

    Examples:
        >>> agg = Min(property="startDate")
    """

    aggregate: Literal["min"] = "min"


class Max(MetricAggregation):
    """Maximum aggregation.

    Finds the maximum value of a property.

    Args:
        property: The property to find the maximum of.

    Examples:
        >>> agg = Max(property="endDate")
    """

    aggregate: Literal["max"] = "max"


# Type alias for all aggregation types
Aggregation: TypeAlias = Count | Sum | Avg | Min | Max


class PropertySort(QueryParameters):
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

    property: list[str] = Field(..., min_length=2, max_length=3)
    direction: Literal["ascending", "descending"] = "ascending"
    nulls_first: bool | None = Field(default=None)


class UnitReference(QueryParameters):
    """Reference to a specific unit by its external ID.

    Args:
        external_id: The external ID of the unit.
    """

    external_id: str


class UnitSystemReference(QueryParameters):
    """Reference to a unit system by its external ID.

    Args:
        unit_system_name: The name of the unit system.
    """

    unit_system_name: str


class UnitConversion(QueryParameters):
    """Unit conversion configuration for a property.

    Use this to specify a target unit for numeric properties that have units defined
    in the container. The API will automatically convert values to the target unit.

    Args:
        property: The property path to convert. Must be a 3-element list:
            [<space>, <view_external_id/version>, <property>]
        unit: The target unit to convert to. Must be a valid unit from the
            same unit catalog as the source property's unit.

    """

    property: str
    unit: UnitReference | UnitSystemReference


class DebugParameters(QueryParameters):
    """Debug parameters for a query request.

    When debug mode is enabled in list/iterate/search operations, the response includes
    additional metadata about the query execution.

    Args:
        emit_results: Include the query result in the response. Set to False for
            advanced query analysis features.
        timeout: Query timeout in milliseconds. Can be used to override the default
            timeout when analysing queries. Requires emit_results=False.
        profile: Most thorough level of query analysis. Requires emit_results=False.
    """

    emit_results: bool = Field(
        True,
        description="Include the query result in the response. emitResults=false is required for "
        "advanced query analysis features.",
    )
    timeout: int | None = Field(
        default=None,
        description="Query timeout in milliseconds. Can be used to override the default timeout"
        " when analysing queries. Requires emitResults=false.",
    )
    profile: bool = Field(
        default=False, description="Most thorough level of query analysis. " "Requires emitResults=false."
    )
