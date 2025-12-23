"""Generic Instance API for CDF Data Modeling.

This module provides a generic client and base classes for working with
CDF Data Modeling instances.
"""

from ._client import InstanceClient
from ._instance import (
    DataRecord,
    DataRecordWrite,
    Instance,
    InstanceId,
    InstanceList,
    InstanceModel,
    InstanceResult,
    InstanceResultItem,
    InstanceWrite,
    Page,
    ViewRef,
)

__all__ = [
    "InstanceClient",
    "InstanceModel",
    "Instance",
    "InstanceWrite",
    "InstanceList",
    "InstanceId",
    "InstanceResult",
    "InstanceResultItem",
    "ViewRef",
    "DataRecord",
    "DataRecordWrite",
    "Page",
]
