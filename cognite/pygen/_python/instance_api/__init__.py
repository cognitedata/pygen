"""Generic Instance API for CDF Data Modeling.

This module provides a generic client and base classes for working with
CDF Data Modeling instances.
"""

from cognite.pygen._python.instance_api.models.instance import (
    DataRecord,
    DataRecordWrite,
    Instance,
    InstanceId,
    InstanceList,
    InstanceModel,
    InstanceWrite,
    ViewReference,
)
from cognite.pygen._python.instance_api.models.query import (
    Aggregation,
    Avg,
    Count,
    DebugParameters,
    Max,
    Min,
    PropertySort,
    Sum,
    UnitConversion,
)
from cognite.pygen._python.instance_api.models.responses import (
    AggregatedNumberValue,
    AggregatedValue,
    AggregateResponse,
    InstanceResultItem,
    ListResponse,
    Page,
    UpsertResult,
)

from ._api import InstanceAPI
from ._client import InstanceClient

__all__ = [
    "AggregatedNumberValue",
    "AggregatedValue",
    "AggregateResponse",
    "Aggregation",
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
    "Aggregation",
    "Min",
    "Page",
    "PropertySort",
    "Sum",
    "UnitConversion",
    "UpsertResult",
    "ViewReference",
]
