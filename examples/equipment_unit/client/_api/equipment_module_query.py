from cognite.client import data_modeling as dm
from equipment_unit.client.data_classes import (
    DomainModelList,
    StartEndTimeApply,
    StartEndTime,
    EquipmentModuleApply,
    EquipmentModule,
)
from ._core import QueryStep, QueryBuilder, QueryAPI
from cognite.client import CogniteClient
from equipment_unit.client.data_classes._start_end_time import _STARTENDTIME_PROPERTIES_BY_FIELD
from equipment_unit.client.data_classes._equipment_module import _EQUIPMENTMODULE_PROPERTIES_BY_FIELD


class EquipmentModuleQueryAPI(QueryAPI):
    def query(self, retrieve_equipment_module: bool = True) -> DomainModelList:
        if retrieve_equipment_module:
            self._builder.append(
                QueryStep(
                    name="equipment_module",
                    filter=None,
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[EquipmentModuleApply],
                                list(_EQUIPMENTMODULE_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    expression_cls=dm.query.NodeResultSetExpression,
                    result_cls=EquipmentModule,
                    from_=None,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
