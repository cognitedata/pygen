from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from equipment_unit.data_classes import (
    DomainModelCore,
    UnitProcedure,
    StartEndTime,
)
from equipment_unit.data_classes._work_order import (
    WorkOrder,
    _create_work_order_filter,
)
from equipment_unit.data_classes._equipment_module import (
    EquipmentModule,
    _create_equipment_module_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

from equipment_unit.data_classes._start_end_time import (
    _create_start_end_time_filter,
)

if TYPE_CHECKING:
    from .work_order_query import WorkOrderQueryAPI
    from .equipment_module_query import EquipmentModuleQueryAPI


class UnitProcedureQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("IntegrationTestsImmutable", "UnitProcedure", "a6e2fea1e1c664")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("unit_procedure"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])]),
                result_cls=UnitProcedure,
                max_retrieve_limit=limit,
            )
        )

    def work_orders(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        performed_by: str | list[str] | None = None,
        performed_by_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        min_end_time_edge: datetime.datetime | None = None,
        max_end_time_edge: datetime.datetime | None = None,
        min_start_time_edge: datetime.datetime | None = None,
        max_start_time_edge: datetime.datetime | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> WorkOrderQueryAPI[T_DomainModelList]:
        """Query along the work order edges of the unit procedure.

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            performed_by: The performed by to filter on.
            performed_by_prefix: The prefix of the performed by to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            min_end_time_edge: The minimum value of the end time to filter on.
            max_end_time_edge: The maximum value of the end time to filter on.
            min_start_time_edge: The minimum value of the start time to filter on.
            max_start_time_edge: The maximum value of the start time to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of work order edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.

        Returns:
            WorkOrderQueryAPI: The query API for the work order.
        """
        from .work_order_query import WorkOrderQueryAPI

        from_ = self._builder[-1].name
        edge_view = self._view_by_read_class[StartEndTime]
        edge_filter = _create_start_end_time_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.work_order"),
            edge_view,
            min_end_time=min_end_time_edge,
            max_end_time=max_end_time_edge,
            min_start_time=min_start_time_edge,
            max_start_time=max_start_time_edge,
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("work_orders"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(edge_view, ["*"])],
                ),
                result_cls=StartEndTime,
                max_retrieve_limit=limit,
            )
        )

        view_id = WorkOrderQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_work_order_filter(
            view_id,
            description,
            description_prefix,
            performed_by,
            performed_by_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return WorkOrderQueryAPI(self._client, self._builder, node_filer, limit)

    def work_units(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        min_end_time_edge: datetime.datetime | None = None,
        max_end_time_edge: datetime.datetime | None = None,
        min_start_time_edge: datetime.datetime | None = None,
        max_start_time_edge: datetime.datetime | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> EquipmentModuleQueryAPI[T_DomainModelList]:
        """Query along the work unit edges of the unit procedure.

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            min_end_time_edge: The minimum value of the end time to filter on.
            max_end_time_edge: The maximum value of the end time to filter on.
            min_start_time_edge: The minimum value of the start time to filter on.
            max_start_time_edge: The maximum value of the start time to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of work unit edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.

        Returns:
            EquipmentModuleQueryAPI: The query API for the equipment module.
        """
        from .equipment_module_query import EquipmentModuleQueryAPI

        from_ = self._builder[-1].name
        edge_view = self._view_by_read_class[StartEndTime]
        edge_filter = _create_start_end_time_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.equipment_module"),
            edge_view,
            min_end_time=min_end_time_edge,
            max_end_time=max_end_time_edge,
            min_start_time=min_start_time_edge,
            max_start_time=max_start_time_edge,
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("work_units"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(edge_view, ["*"])],
                ),
                result_cls=StartEndTime,
                max_retrieve_limit=limit,
            )
        )

        view_id = EquipmentModuleQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_equipment_module_filter(
            view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return EquipmentModuleQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
