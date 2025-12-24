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
    DebugInfo,
    PropertySort,
    PropertyWithUnits,
    UnitConversion,
)
from cognite.pygen._generation.python.instance_api.models.responses import (
    InstanceResultItem,
    ListResponse,
    Page,
    UpsertResult,
)

from ._api import InstanceAPI
from ._client import InstanceClient

__all__ = [
    "DataRecord",
    "DataRecordWrite",
    "DebugInfo",
    "Instance",
    "InstanceAPI",
    "InstanceClient",
    "InstanceId",
    "InstanceList",
    "InstanceModel",
    "InstanceResultItem",
    "InstanceWrite",
    "ListResponse",
    "Page",
    "PropertySort",
    "PropertyWithUnits",
    "UnitConversion",
    "UpsertResult",
    "ViewReference",
]
