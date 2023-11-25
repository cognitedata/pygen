from __future__ import annotations

import datetime

from cognite.client import data_modeling as dm

from equipment_unit.client.data_classes import (
    StartEndTimeList,
)

from ._core import DEFAULT_LIMIT_READ, EdgeAPI


class UnitProcedureWorkUnitsAPI(EdgeAPI):
    def list(
        self,
        unit_procedure: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        equipment_module: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        space: str = "IntegrationTestsImmutable",
        limit=DEFAULT_LIMIT_READ,
    ) -> StartEndTimeList:
        """List work_units edges of a unit procedure.

        Args:
            unit_procedure: ID of the source unit procedure.
            equipment_module: ID of the target equipment module.
            min_start_time: The minimum start time of the work unit edges.
            max_start_time: The maximum start time of the work unit edges.
            min_end_time: The minimum end time of the work unit edges.
            max_end_time: The maximum end time of the work unit edges.
            space: The space where all the work unit edges are located.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested work unit edges.

        Examples:

            List 5 work_units edges connected to "my_unit_procedure":

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> unit_procedure = client.unit_procedure.work_units.list("my_unit_procedure", limit=5)

        """
        f = dm.filters
        filters = _create_filter_work_units(
            self._view_id,
            equipment_module,
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
        return self._list(unit_procedure, filters=filters, limit=limit, space=space)
