from __future__ import annotations

import datetime
from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList
from equipment_unit.client.data_classes import (
    DomainModelApply,
    DomainRelationApply,
    ResourcesApplyResult,
    EquipmentModule,
    EquipmentModuleApply,
    StartEndTime,
    StartEndTimeApply,
    StartEndTimeList,
    UnitProcedure,
    UnitProcedureApply,
    UnitProcedureFields,
    UnitProcedureList,
    UnitProcedureTextFields,
)
from equipment_unit.client.data_classes._equipment_module import (
    _EQUIPMENTMODULE_PROPERTIES_BY_FIELD,
    _create_equipment_module_filter,
)
from equipment_unit.client.data_classes._start_end_time import _STARTENDTIME_PROPERTIES_BY_FIELD
from equipment_unit.client.data_classes._unit_procedure import _UNITPROCEDURE_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, INSTANCE_QUERY_LIMIT, Aggregations, EdgeAPI


class UnitProcedureWorkUnitsQuery:
    def __init__(
        self,
        client: CogniteClient,
        node_view: dm.ViewId,
        edge_view: dm.ViewId,
        end_node_view: dm.ViewId,
        node_limit: int = DEFAULT_LIMIT_READ,
        node_filter: dm.Filter | None = None,
    ):
        self._client = client
        self._node_view = node_view
        self._edge_view = edge_view
        self._end_node_view = end_node_view
        self._node_limit = node_limit
        self._node_filter = node_filter

    def list(
        self,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        space: str = "IntegrationTestsImmutable",
        limit: int | None = None,
        retrieve_equipment_module: bool = True,
    ) -> UnitProcedureList:
        """List unit procedures with work units.

        Args:
            min_start_time: The minimum start time of the work unit edges.
            max_start_time: The maximum start time of the work unit edges.
            min_end_time: The minimum end time of the work unit edges.
            max_end_time: The maximum end time of the work unit edges.
            space: The space where all the work units are located.
            limit: Maximum number of edges to return per unit procedure. Defaults to -1. Set to -1, float("inf") or None to return all items.
            retrieve_equipment_module: Whether to retrieve `equipment_module` for each work unit. Defaults to True.

        Returns:
            List of unit procedures with work units and optionally equipment module.

        Examples:

            List 5 unit procedures with work units:

                    >>> from equipment_unit.client import EquipmentUnitClient
                    >>> client = EquipmentUnitClient()
                    >>> unit_procedures = client.unit_procedure.work_units(limit=5).list()

        """
        f = dm.filters
        edge_filter = _create_filter_work_units(
            self._edge_view,
            None,
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
        limit = float("inf") if limit is None or limit == -1 else limit
        cursors = {"nodes": None, "edges": None}
        results = {"nodes": [], "edges": []}
        total_retrieved = {"nodes": 0, "edges": 0}
        limits = {"nodes": self._node_limit, "edges": limit}
        if retrieve_equipment_module:
            cursors["end_nodes"] = None
            results["end_nodes"] = []
            total_retrieved["end_nodes"] = 0
            limits["end_nodes"] = float("nan")

        while True:
            query_limits = {k: min(INSTANCE_QUERY_LIMIT, v - total_retrieved[k]) for k, v in limits.items()}

            selected_nodes = dm.query.NodeResultSetExpression(filter=self._node_filter, limit=query_limits["nodes"])
            selected_edges = dm.query.EdgeResultSetExpression(
                from_="nodes", filter=edge_filter, limit=query_limits["edges"]
            )
            with_ = {
                "nodes": selected_nodes,
                "edges": selected_edges,
            }
            if retrieve_equipment_module:
                with_["end_nodes"] = dm.query.NodeResultSetExpression(from_="edges", limit=query_limits["end_nodes"])

            select = {
                "nodes": dm.query.Select(
                    [dm.query.SourceSelector(self._node_view, list(_UNITPROCEDURE_PROPERTIES_BY_FIELD.values()))],
                ),
                "edges": dm.query.Select(
                    [dm.query.SourceSelector(self._edge_view, list(_STARTENDTIME_PROPERTIES_BY_FIELD.values()))],
                ),
            }
            if retrieve_equipment_module:
                select["end_nodes"] = dm.query.Select(
                    [dm.query.SourceSelector(self._end_node_view, list(_EQUIPMENTMODULE_PROPERTIES_BY_FIELD.values()))]
                )

            query = dm.query.Query(with_=with_, select=select, cursors=cursors)
            batch = self._client.data_modeling.instances.query(query)
            for key in total_retrieved:
                total_retrieved[key] += len(batch[key])
                results[key].extend(batch[key])
                cursors[key] = batch.cursors[key]

            if all(
                total_retrieved[k] >= limits[k] or cursors[k] is None or len(batch[k]) == 0 for k in total_retrieved
            ):
                break

        if retrieve_equipment_module:
            end_node_by_id = {
                (node.space, node.external_id): EquipmentModule.from_node(node) for node in results["end_nodes"]
            }
        else:
            end_node_by_id = {}

        edge_by_start_node = defaultdict(list)
        for edge in results["edges"]:
            edge = StartEndTime.from_edge(edge)
            edge.equipment_module = end_node_by_id.get((edge.end_node.space, edge.end_node.external_id))
            edge_by_start_node[(edge.start_node.space, edge.start_node.external_id)].append(edge)

        nodes = []
        for node in results["nodes"]:
            node = UnitProcedure.from_node(node)
            node.work_units = edge_by_start_node.get((node.space, node.external_id), [])
            nodes.append(node)

        return UnitProcedureList(nodes)


class UnitProcedureWorkUnitsAPI:
    def __init__(
        self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId]
    ):
        self._client = client
        self._view_by_write_class = view_by_write_class
        self._view_id = view_by_write_class[StartEndTimeApply]

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> UnitProcedureWorkUnitsQuery:
        """Query timeseries `equipment_module.sensor_value`

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of unit procedures to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query object that can be used to retrieve datapoins for the equipment_module.sensor_value timeseries
            selected in this method.

        Examples:

            Retrieve all data for 5 equipment_module.sensor_value timeseries:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> equipment_modules = client.equipment_module.sensor_value(limit=5).retrieve()

        """
        node_view = self._view_by_write_class[UnitProcedureApply]
        filter_ = _create_equipment_module_filter(
            node_view,
            name,
            name_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
        )

        return UnitProcedureWorkUnitsQuery(
            client=self._client,
            node_view=node_view,
            edge_view=self._view_id,
            end_node_view=self._view_by_write_class[EquipmentModuleApply],
            node_limit=limit,
            node_filter=filter_,
        )


class UnitProcedureWorkUnitsEdgeAPI(EdgeAPI):
    def __init__(
        self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId]
    ):
        self._client = client
        self._view_by_write_class = view_by_write_class
        self._view_id = view_by_write_class[StartEndTimeApply]

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
        filter = _create_filter_work_units(
            self._view_id,
            unit_procedure,
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

        edges = self._client.data_modeling.instances.list("edge", limit=limit, filter=filter, sources=[self._view_id])
        return StartEndTimeList([StartEndTime.from_edge(edge) for edge in edges])


def _create_filter_work_units(
    view_id: dm.ViewId,
    unit_procedure: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    equipment_module: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    space: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if unit_procedure and isinstance(unit_procedure, str):
        filters.append(dm.filters.Equals(["edge", "startNode"], value={"space": space, "externalId": unit_procedure}))
    if unit_procedure and isinstance(unit_procedure, list):
        filters.append(
            dm.filters.In(
                ["edge", "startNode"],
                values=[
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in unit_procedure
                ],
            )
        )
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
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
