from ._asset import Asset, AssetApply, AssetList
from ._cdf_3_d_connection_properties import (
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
    CdfConnectionPropertiesList,
)
from ._cdf_3_d_entity import CdfEntity, CdfEntityApply, CdfEntityList
from ._cdf_3_d_model import CdfModel, CdfModelApply, CdfModelList
from ._work_item import WorkItem, WorkItemApply, WorkItemList
from ._work_order import WorkOrder, WorkOrderApply, WorkOrderList

AssetApply.update_forward_refs(
    AssetApply=AssetApply,
    CdfModelApply=CdfModelApply,
)
CdfEntityApply.update_forward_refs(
    CdfModelApply=CdfModelApply,
)
CdfModelApply.update_forward_refs(
    CdfEntityApply=CdfEntityApply,
)
WorkItemApply.update_forward_refs(
    AssetApply=AssetApply,
    WorkOrderApply=WorkOrderApply,
)
WorkOrderApply.update_forward_refs(
    AssetApply=AssetApply,
    WorkItemApply=WorkItemApply,
)

__all__ = [
    "Asset",
    "AssetApply",
    "AssetList",
    "CdfConnectionProperties",
    "CdfConnectionPropertiesApply",
    "CdfConnectionPropertiesList",
    "CdfEntity",
    "CdfEntityApply",
    "CdfEntityList",
    "CdfModel",
    "CdfModelApply",
    "CdfModelList",
    "WorkItem",
    "WorkItemApply",
    "WorkItemList",
    "WorkOrder",
    "WorkOrderApply",
    "WorkOrderList",
]
