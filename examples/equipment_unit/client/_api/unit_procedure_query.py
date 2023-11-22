from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from equipment_unit.client.data_classes import UnitProcedureList, DomainModelList
from ._core import QueryBuilder, QueryExpression, QueryAPI
from equipment_unit.client.data_classes._start_end_time import _STARTENDTIME_PROPERTIES_BY_FIELD
from cognite.client import CogniteClient

if TYPE_CHECKING:
    from .equipment_module_query import EquipmentModuleQueryAPI


class UnitProcedureQueryAPI(QueryAPI):
    def __init__(self, client: CogniteClient, builder: QueryBuilder, from_: str, edge_view: dm.ViewId):
        super().__init__(client, builder, from_)

        self._edge_view = edge_view

    def work_units(
        self,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        space: str = "IntegrationTestsImmutable",
        limit: int | None = None,
    ) -> EquipmentModuleQueryAPI:
        from .equipment_module_query import EquipmentModuleQueryAPI

        f = dm.filters
        edge_filters = _create_filter_work_units(
            self._edge_view,
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
            QueryExpression(
                name="work_units",
                filter=f.And(*edge_filters),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._edge_view, list(_STARTENDTIME_PROPERTIES_BY_FIELD.values()))]
                ),
                expression_cls=dm.query.EdgeResultSetExpression,
                from_=self._from,
                max_retrieve_limit=limit,
            )
        )
        return EquipmentModuleQueryAPI(self._client, self._builder, "work_units", self._edge_view)

    def query(self, retrieve_unit_procedure: bool = True) -> DomainModelList:
        # Todo add unit procedure to builder if retrieve_unit_procedure
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
