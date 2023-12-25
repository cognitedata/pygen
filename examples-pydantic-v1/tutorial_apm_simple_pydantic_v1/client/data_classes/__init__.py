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
    Cdf3dConnectionProperties,
    Cdf3dConnectionPropertiesApply,
    Cdf3dConnectionPropertiesApplyList,
    Cdf3dConnectionPropertiesFields,
    Cdf3dConnectionPropertiesList,
)
from ._cdf_3_d_entity import Cdf3dEntity, Cdf3dEntityApply, Cdf3dEntityApplyList, Cdf3dEntityList
from ._cdf_3_d_model import (
    Cdf3dModel,
    Cdf3dModelApply,
    Cdf3dModelApplyList,
    Cdf3dModelFields,
    Cdf3dModelList,
    Cdf3dModelTextFields,
)
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
    Cdf3dConnectionProperties=Cdf3dConnectionProperties,
)
AssetApply.update_forward_refs(
    AssetApply=AssetApply,
    Cdf3dConnectionPropertiesApply=Cdf3dConnectionPropertiesApply,
)

Cdf3dConnectionProperties.update_forward_refs(
    Cdf3dEntity=Cdf3dEntity,
    Cdf3dModel=Cdf3dModel,
)
Cdf3dConnectionPropertiesApply.update_forward_refs(
    Cdf3dEntityApply=Cdf3dEntityApply,
    Cdf3dModelApply=Cdf3dModelApply,
)

Cdf3dEntity.update_forward_refs(
    Cdf3dConnectionProperties=Cdf3dConnectionProperties,
)
Cdf3dEntityApply.update_forward_refs(
    Cdf3dConnectionPropertiesApply=Cdf3dConnectionPropertiesApply,
)

Cdf3dModel.update_forward_refs(
    Cdf3dConnectionProperties=Cdf3dConnectionProperties,
)
Cdf3dModelApply.update_forward_refs(
    Cdf3dConnectionPropertiesApply=Cdf3dConnectionPropertiesApply,
)

WorkItem.update_forward_refs(
    Asset=Asset,
    WorkOrder=WorkOrder,
)
WorkItemApply.update_forward_refs(
    AssetApply=AssetApply,
    WorkOrderApply=WorkOrderApply,
)

WorkOrder.update_forward_refs(
    Asset=Asset,
    WorkItem=WorkItem,
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
    "Cdf3dConnectionProperties",
    "Cdf3dConnectionPropertiesApply",
    "Cdf3dConnectionPropertiesList",
    "Cdf3dConnectionPropertiesApplyList",
    "Cdf3dConnectionPropertiesFields",
    "Cdf3dEntity",
    "Cdf3dEntityApply",
    "Cdf3dEntityList",
    "Cdf3dEntityApplyList",
    "Cdf3dModel",
    "Cdf3dModelApply",
    "Cdf3dModelList",
    "Cdf3dModelApplyList",
    "Cdf3dModelFields",
    "Cdf3dModelTextFields",
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
