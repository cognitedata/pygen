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
from cognite.pygen._generation.python.instance_api.models.responses import (
    ApplyResponse,
    InstanceResult,
    InstanceResultItem,
    Page,
)

from ._client import InstanceClient

__all__ = [
    "ApplyResponse",
    "DataRecord",
    "DataRecordWrite",
    "Instance",
    "InstanceClient",
    "InstanceId",
    "InstanceList",
    "InstanceModel",
    "InstanceResult",
    "InstanceResultItem",
    "InstanceWrite",
    "Page",
    "ViewReference",
]
