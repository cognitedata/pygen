from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    ResourcesApplyResult,
)
from ._asset import Asset, AssetApply, AssetApplyList, AssetFields, AssetList, AssetTextFields
from ._cdf_3_d_connection_properties import (
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
    CdfConnectionPropertiesApplyList,
    CdfConnectionPropertiesFields,
    CdfConnectionPropertiesList,
)
from ._cdf_3_d_entity import CdfEntity, CdfEntityApply, CdfEntityApplyList, CdfEntityList
from ._cdf_3_d_model import CdfModel, CdfModelApply, CdfModelApplyList, CdfModelFields, CdfModelList, CdfModelTextFields
from ._work_item import WorkItem, WorkItemApply, WorkItemApplyList, WorkItemFields, WorkItemList, WorkItemTextFields
from ._work_order import (
    WorkOrder,
    WorkOrderApply,
    WorkOrderApplyList,
    WorkOrderFields,
    WorkOrderList,
    WorkOrderTextFields,
)

Asset.update_forward_refs(
    Asset=Asset,
    CdfConnectionProperties=CdfConnectionProperties,
)
CdfConnectionProperties.update_forward_refs(
    CdfEntity=CdfEntity,
)
CdfEntity.update_forward_refs(
    CdfConnectionProperties=CdfConnectionProperties,
)
CdfModel.update_forward_refs(
    CdfConnectionProperties=CdfConnectionProperties,
)
WorkItem.update_forward_refs(
    Asset=Asset,
    WorkOrder=WorkOrder,
)
WorkOrder.update_forward_refs(
    Asset=Asset,
    WorkItem=WorkItem,
)

AssetApply.update_forward_refs(
    AssetApply=AssetApply,
    CdfConnectionPropertiesApply=CdfConnectionPropertiesApply,
)
CdfConnectionPropertiesApply.update_forward_refs(
    CdfEntityApply=CdfEntityApply,
)
CdfEntityApply.update_forward_refs(
    CdfConnectionPropertiesApply=CdfConnectionPropertiesApply,
)
CdfModelApply.update_forward_refs(
    CdfConnectionPropertiesApply=CdfConnectionPropertiesApply,
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
    "ResourcesApply",
    "DomainModel",
    "DomainModelApply",
    "DomainModelList",
    "DomainRelationApply",
    "ResourcesApplyResult",
    "Asset",
    "AssetApply",
    "AssetList",
    "AssetApplyList",
    "AssetFields",
    "AssetTextFields",
    "CdfConnectionProperties",
    "CdfConnectionPropertiesApply",
    "CdfConnectionPropertiesList",
    "CdfConnectionPropertiesApplyList",
    "CdfConnectionPropertiesFields",
    "CdfEntity",
    "CdfEntityApply",
    "CdfEntityList",
    "CdfEntityApplyList",
    "CdfModel",
    "CdfModelApply",
    "CdfModelList",
    "CdfModelApplyList",
    "CdfModelFields",
    "CdfModelTextFields",
    "WorkItem",
    "WorkItemApply",
    "WorkItemList",
    "WorkItemApplyList",
    "WorkItemFields",
    "WorkItemTextFields",
    "WorkOrder",
    "WorkOrderApply",
    "WorkOrderList",
    "WorkOrderApplyList",
    "WorkOrderFields",
    "WorkOrderTextFields",
]
