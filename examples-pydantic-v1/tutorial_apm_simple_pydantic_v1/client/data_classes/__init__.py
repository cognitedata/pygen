from ._asset import Asset, AssetApply, AssetList, AssetApplyList
from ._cdf_3_d_connection_properties import (
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
    CdfConnectionPropertiesList,
    CdfConnectionPropertiesApplyList,
)
from ._cdf_3_d_entity import CdfEntity, CdfEntityApply, CdfEntityList, CdfEntityApplyList
from ._cdf_3_d_model import CdfModel, CdfModelApply, CdfModelList, CdfModelApplyList
from ._work_item import WorkItem, WorkItemApply, WorkItemList, WorkItemApplyList
from ._work_order import WorkOrder, WorkOrderApply, WorkOrderList, WorkOrderApplyList

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
    "AssetApplyList",
    "CdfConnectionProperties",
    "CdfConnectionPropertiesApply",
    "CdfConnectionPropertiesList",
    "CdfConnectionPropertiesApplyList",
    "CdfEntity",
    "CdfEntityApply",
    "CdfEntityList",
    "CdfEntityApplyList",
    "CdfModel",
    "CdfModelApply",
    "CdfModelList",
    "CdfModelApplyList",
    "WorkItem",
    "WorkItemApply",
    "WorkItemList",
    "WorkItemApplyList",
    "WorkOrder",
    "WorkOrderApply",
    "WorkOrderList",
    "WorkOrderApplyList",
]
