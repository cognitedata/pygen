from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from equipment_unit.client.data_classes import (
    DomainModelApply,
    UnitProcedure,
    UnitProcedureApply,
    StartEndTimeApply,
    StartEndTime,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList

from equipment_unit.client.data_classes._start_end_time import (
    _create_start_end_time_filter,
)

if TYPE_CHECKING:
    from .work_order_query import WorkOrderQueryAPI
    from .equipment_module_query import EquipmentModuleQueryAPI


class UnitProcedureQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_write_class: dict[type[DomainModelApply], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_write_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("unit_procedure"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[UnitProcedureApply], ["*"])]),
                result_cls=UnitProcedure,
                max_retrieve_limit=limit,
            )
        )

    def work_orders(
        self,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> WorkOrderQueryAPI[T_DomainModelList]:
        """Query along the work order edges of the unit procedure.

        Args:
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work order edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            WorkOrderQueryAPI: The query API for the work order.
        """
        from .work_order_query import WorkOrderQueryAPI

        from_ = self._builder[-1].name

        edge_view = self._view_by_write_class[StartEndTimeApply]
        edge_filter = _create_start_end_time_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.work_order"),
            edge_view,
            min_end_time=min_end_time,
            max_end_time=max_end_time,
            min_start_time=min_start_time,
            max_start_time=max_start_time,
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("work_orders"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(edge_view, ["*"])],
                ),
                result_cls=StartEndTime,
                max_retrieve_limit=limit,
            )
        )
        return WorkOrderQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def work_units(
        self,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
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
            WorkOrderQueryAPI: The query API for the work order.
        """
        from .equipment_module_query import EquipmentModuleQueryAPI

        from_ = self._builder[-1].name

        edge_view = self._view_by_write_class[StartEndTimeApply]
        edge_filter = _create_start_end_time_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.equipment_module"),
            edge_view,
            min_end_time=min_end_time,
            max_end_time=max_end_time,
            min_start_time=min_start_time,
            max_start_time=max_start_time,
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("work_units"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(edge_view, ["*"])]),
                result_cls=StartEndTime,
                max_retrieve_limit=limit,
            )
        )

        return EquipmentModuleQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
