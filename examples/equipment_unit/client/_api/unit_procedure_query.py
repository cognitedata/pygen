from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from equipment_unit.client.data_classes import (
    UnitProcedure,
    UnitProcedureApply,
    StartEndTime,
    StartEndTimeApply,
)
from equipment_unit.client.data_classes._unit_procedure import (
    _UNITPROCEDURE_PROPERTIES_BY_FIELD,
)
from equipment_unit.client.data_classes._start_end_time import (
    _STARTENDTIME_PROPERTIES_BY_FIELD,
    _create_start_end_time_filter,
)

if TYPE_CHECKING:
    from .equipment_module_query import EquipmentModuleQueryAPI


class UnitProcedureQueryAPI(QueryAPI[T_DomainModelList]):
    def work_units(
        self,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = None,
    ) -> EquipmentModuleQueryAPI[T_DomainModelList]:
        """Query along the work unit edges of the unit procedure.

        Args:
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            EquipmentModuleQueryAPI: The query API for the equipment module.
        """
        from .equipment_module_query import EquipmentModuleQueryAPI

        f = dm.filters
        edge_view = self._view_by_write_class[StartEndTimeApply]
        edge_filter = _create_start_end_time_filter(
            edge_view,
            None,
            None,
            None,
            None,
            min_end_time,
            max_end_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "UnitProcedure.equipment_module"},
            ),
        )
        self._builder.append(
            QueryStep(
                name="work_units",
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(edge_view, list(_STARTENDTIME_PROPERTIES_BY_FIELD.values()))]
                ),
                result_cls=StartEndTime,
                max_retrieve_limit=limit,
            )
        )
        return EquipmentModuleQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_unit_procedure: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_unit_procedure: Whether to retrieve the unit procedure or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_unit_procedure and not self._builder[-1].name.startswith("unit_procedure"):
            self._builder.append(
                QueryStep(
                    name="unit_procedure",
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[UnitProcedureApply],
                                list(_UNITPROCEDURE_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=UnitProcedure,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
