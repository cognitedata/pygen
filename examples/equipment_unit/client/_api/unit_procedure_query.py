from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryBuilder, QueryStep, QueryAPI, T_DomainModelList
from equipment_unit.client.data_classes._start_end_time import (
    _STARTENDTIME_PROPERTIES_BY_FIELD,
)
from equipment_unit.client.data_classes import (
    UnitProcedure,
    StartEndTimeApply,
    StartEndTime,
    UnitProcedureApply,
)
from equipment_unit.client.data_classes._unit_procedure import (
    _UNITPROCEDURE_PROPERTIES_BY_FIELD,
)


if TYPE_CHECKING:
    from .equipment_module_query import EquipmentModuleQueryAPI


class UnitProcedureQueryAPI(QueryAPI[T_DomainModelList]):
    def work_units(
        self,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        space: str = "IntegrationTestsImmutable",
        limit: int | None = None,
    ) -> EquipmentModuleQueryAPI[T_DomainModelList]:
        """Query along the work_units edges of the unit procedure.

        Args:
            min_start_time: The minimum start time of the work unit edges.
            max_start_time: The maximum start time of the work unit edges.
            min_end_time: The minimum end time of the work unit edges.
            max_end_time: The maximum end time of the work unit edges.
            space: The space where all the work unit edges are located.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            EquipmentModuleQueryAPI: The query API for the equipment module.
        """
        from .equipment_module_query import EquipmentModuleQueryAPI

        f = dm.filters
        edge_view = self._view_by_write_class[StartEndTimeApply]
        edge_filters = _create_filter_work_units(
            edge_view,
            None,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
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
                    filter=f.And(*edge_filters),
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

    def query(self, retrieve_unit_procedure: bool = True) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_unit_procedure: Whether to retrieve the unit procedure or not.

        Returns:
            The list of the source nodes of the query.

        """
        if retrieve_unit_procedure:
            self._builder.append(
                QueryStep(
                    name="unit_procedure",
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=self._builder[-1].name,
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


def _create_filter_work_units(
    view_id: dm.ViewId,
    equipment_module: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    space: str = "IntegrationTestsImmutable",
    filter: dm.Filter | None = None,
) -> list[dm.Filter]:
    filters: list[dm.Filter] = []
    if equipment_module and isinstance(equipment_module, str):
        filters.append(dm.filters.Equals(["edge", "endNode"], value={"space": space, "externalId": equipment_module}))
    if equipment_module and isinstance(equipment_module, list):
        filters.append(
            dm.filters.In(
                ["edge", "endNode"],
                values=[
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in equipment_module
                ],
            )
        )
    if min_start_time or max_start_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("start_time"),
                gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None,
                lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None,
            )
        )
    if min_end_time or max_end_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("end_time"),
                gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None,
                lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None,
            )
        )
    if filter:
        filters.append(filter)
    return filters
