from cognite.client import data_modeling as dm

from equipment_unit.client.data_classes import (
    EquipmentModuleApply,
    EquipmentModule,
)
from equipment_unit.client.data_classes._equipment_module import _EQUIPMENTMODULE_PROPERTIES_BY_FIELD
from ._core import QueryStep, QueryAPI, T_DomainModelList


class EquipmentModuleQueryAPI(QueryAPI[T_DomainModelList]):
    def query(self, retrieve_equipment_module: bool = True) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_equipment_module: Whether to retrieve the equipment module or not.

        Returns:
            The list of the source nodes of the query.

        """
        # If the last step is equipment_module, we already have it in the query.
        if retrieve_equipment_module and self._builder[-1].name != "equipment_module":
            self._builder.append(
                QueryStep(
                    name="equipment_module",
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=self._builder[-1].name,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[EquipmentModuleApply],
                                list(_EQUIPMENTMODULE_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=EquipmentModule,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
