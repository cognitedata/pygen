from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from equipment_unit_pydantic_v1.client.data_classes import (
    EquipmentModule,
    EquipmentModuleApply,
)
from equipment_unit_pydantic_v1.client.data_classes._equipment_module import (
    _EQUIPMENTMODULE_PROPERTIES_BY_FIELD,
)


class EquipmentModuleQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_equipment_module: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_equipment_module: Whether to retrieve the equipment module or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_equipment_module and not self._builder[-1].name.startswith("equipment_module"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("equipment_module"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
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