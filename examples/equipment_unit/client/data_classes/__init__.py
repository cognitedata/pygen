from ._core import (
    DataRecord,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    ResourcesWriteResult,
)
from ._equipment_module import (
    EquipmentModule,
    EquipmentModuleApply,
    EquipmentModuleApplyList,
    EquipmentModuleFields,
    EquipmentModuleGraphQL,
    EquipmentModuleList,
    EquipmentModuleTextFields,
    EquipmentModuleWrite,
    EquipmentModuleWriteList,
)
from ._start_end_time import (
    StartEndTime,
    StartEndTimeApply,
    StartEndTimeApplyList,
    StartEndTimeFields,
    StartEndTimeGraphQL,
    StartEndTimeList,
    StartEndTimeWrite,
    StartEndTimeWriteList,
)
from ._unit_procedure import (
    UnitProcedure,
    UnitProcedureApply,
    UnitProcedureApplyList,
    UnitProcedureFields,
    UnitProcedureGraphQL,
    UnitProcedureList,
    UnitProcedureTextFields,
    UnitProcedureWrite,
    UnitProcedureWriteList,
)
from ._work_order import (
    WorkOrder,
    WorkOrderApply,
    WorkOrderApplyList,
    WorkOrderFields,
    WorkOrderGraphQL,
    WorkOrderList,
    WorkOrderTextFields,
    WorkOrderWrite,
    WorkOrderWriteList,
)

StartEndTime.model_rebuild()
StartEndTimeWrite.model_rebuild()
StartEndTimeApply.model_rebuild()
UnitProcedure.model_rebuild()
UnitProcedureWrite.model_rebuild()
UnitProcedureApply.model_rebuild()


_GRAPHQL_DATA_CLASS_BY_VIEW_ID = {_cls.view_id: _cls for _cls in GraphQLCore.__subclasses__()}

__all__ = [
    "DataRecord",
    "DataRecordWrite",
    "ResourcesWrite",
    "DomainModel",
    "DomainModelCore",
    "DomainModelWrite",
    "DomainModelList",
    "DomainRelationWrite",
    "ResourcesWriteResult",
    "EquipmentModule",
    "EquipmentModuleGraphQL",
    "EquipmentModuleWrite",
    "EquipmentModuleApply",
    "EquipmentModuleList",
    "EquipmentModuleWriteList",
    "EquipmentModuleApplyList",
    "EquipmentModuleFields",
    "EquipmentModuleTextFields",
    "StartEndTime",
    "StartEndTimeGraphQL",
    "StartEndTimeWrite",
    "StartEndTimeApply",
    "StartEndTimeList",
    "StartEndTimeWriteList",
    "StartEndTimeApplyList",
    "StartEndTimeFields",
    "UnitProcedure",
    "UnitProcedureGraphQL",
    "UnitProcedureWrite",
    "UnitProcedureApply",
    "UnitProcedureList",
    "UnitProcedureWriteList",
    "UnitProcedureApplyList",
    "UnitProcedureFields",
    "UnitProcedureTextFields",
    "WorkOrder",
    "WorkOrderGraphQL",
    "WorkOrderWrite",
    "WorkOrderApply",
    "WorkOrderList",
    "WorkOrderWriteList",
    "WorkOrderApplyList",
    "WorkOrderFields",
    "WorkOrderTextFields",
]
