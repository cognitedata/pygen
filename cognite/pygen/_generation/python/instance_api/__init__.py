"""Generic Instance API for CDF Data Modeling.

This module provides a generic client and base classes for working with
CDF Data Modeling instances.
"""

from cognite.pygen._generation.python.instance_api.models.instance import (
    DataRecord,
    DataRecordWrite,
    Instance,
    InstanceId,
    InstanceList,
    InstanceModel,
    InstanceWrite,
    ViewReference,
)
from cognite.pygen._generation.python.instance_api.models.query import (
    AggregatedValue,
    AggregateResult,
    Aggregation,
    AggregationGroup,
    AggregationLiteral,
    Avg,
    Count,
    DebugParameters,
    Max,
    MetricAggregation,
    Min,
    PropertySort,
    Sum,
    UnitConversion,
)
from cognite.pygen._generation.python.instance_api.models.responses import (
    InstanceResultItem,
    ListResponse,
    Page,
    RetrieveResponse,
    UpsertResult,
)

from ._api import InstanceAPI
from ._client import InstanceClient

__all__ = [
    "AggregatedValue",
    "AggregateResult",
    "Aggregation",
    "AggregationGroup",
    "AggregationLiteral",
    "Avg",
    "Count",
    "DataRecord",
    "DataRecordWrite",
    "DebugParameters",
    "Instance",
    "InstanceAPI",
    "InstanceClient",
    "InstanceId",
    "InstanceList",
    "InstanceModel",
    "InstanceResultItem",
    "InstanceWrite",
    "ListResponse",
    "Max",
    "MetricAggregation",
    "Min",
    "Page",
    "PropertySort",
    "RetrieveResponse",
    "Sum",
    "UnitConversion",
    "UpsertResult",
    "ViewReference",
]
