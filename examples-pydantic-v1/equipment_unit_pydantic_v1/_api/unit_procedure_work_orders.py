from __future__ import annotations

import datetime

from cognite.client import data_modeling as dm

from equipment_unit_pydantic_v1.data_classes import (
    StartEndTimeList,
)
from equipment_unit_pydantic_v1.data_classes._start_end_time import _create_start_end_time_filter

from ._core import DEFAULT_LIMIT_READ, EdgePropertyAPI
from equipment_unit_pydantic_v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class UnitProcedureWorkOrdersAPI(EdgePropertyAPI):
    def list(
        self,
        from_unit_procedure: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_unit_procedure_space: str = DEFAULT_INSTANCE_SPACE,
        to_work_order: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_work_order_space: str = DEFAULT_INSTANCE_SPACE,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> StartEndTimeList:
        """List work order edges of a unit procedure.

        Args:
            from_unit_procedure: ID of the source unit procedure.
            from_unit_procedure_space: Location of the unit procedures.
            to_work_order: ID of the target work order.
            to_work_order_space: Location of the work orders.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work order edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested work order edges.

        Examples:

            List 5 work order edges connected to "my_unit_procedure":

                >>> from equipment_unit_pydantic_v1 import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> unit_procedure = client.unit_procedure.work_orders_edge.list("my_unit_procedure", limit=5)

        """
        filter_ = _create_start_end_time_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.work_order"),
            self._view_id,
            from_unit_procedure,
            from_unit_procedure_space,
            to_work_order,
            to_work_order_space,
            min_end_time,
            max_end_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
