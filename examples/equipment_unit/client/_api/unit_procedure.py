from __future__ import annotations

import datetime
from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from equipment_unit.client.data_classes import (
    UnitProcedure,
    UnitProcedureApply,
    UnitProcedureList,
    UnitProcedureApplyList,
    UnitProcedureFields,
    UnitProcedureTextFields,
    DomainModelApply,
    StartEndTimeList,
    StartEndTime,
    StartEndTimeApply,
)
from equipment_unit.client.data_classes._unit_procedure import _UNITPROCEDURE_PROPERTIES_BY_FIELD


class UnitProcedureWorkUnitsAPI:
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        self._client = client
        self._view_id = view_id

    def retrieve(
        self,
        unit_procedure: str | Sequence[str] | dm.NodeId | list[dm.NodeId],
        space: str = "IntegrationTestsImmutable",
    ) -> StartEndTimeList:
        """Retrieve one or more work_units edges by id(s) of a unit procedure.

        Args:
            unit_procedure: External id or list of external ids source unit procedure.
            space: The space where all the work unit edges are located.

        Returns:
            The requested work unit edges.

        Examples:

            Retrieve work_units edge by id:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> unit_procedure = client.unit_procedure.work_units.retrieve("my_work_units")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "UnitProcedure.equipment_module"},
        )
        if isinstance(unit_procedure, (str, dm.NodeId)):
            is_unit_procedures = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": unit_procedure}
                if isinstance(unit_procedure, str)
                else unit_procedure.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_unit_procedures = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in unit_procedure
                ],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_unit_procedures), sources=[self._view_id]
        )

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
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the work unit edges are located.

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


class UnitProcedureAPI(TypeAPI[UnitProcedure, UnitProcedureApply, UnitProcedureList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[UnitProcedureApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=UnitProcedure,
            class_apply_type=UnitProcedureApply,
            class_list=UnitProcedureList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.work_units = UnitProcedureWorkUnitsAPI(client, view_by_write_class[StartEndTimeApply])

    def apply(
        self, unit_procedure: UnitProcedureApply | Sequence[UnitProcedureApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) unit procedures.

        Note: This method iterates through all nodes linked to unit_procedure and create them including the edges
        between the nodes. For example, if any of `work_units` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            unit_procedure: Unit procedure or sequence of unit procedures to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new unit_procedure:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> from equipment_unit.client.data_classes import UnitProcedureApply
                >>> client = EquipmentUnitClient()
                >>> unit_procedure = UnitProcedureApply(external_id="my_unit_procedure", ...)
                >>> result = client.unit_procedure.apply(unit_procedure)

        """
        if isinstance(unit_procedure, UnitProcedureApply):
            instances = unit_procedure.to_instances_apply(self._view_by_write_class)
        else:
            instances = UnitProcedureApplyList(unit_procedure).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more unit procedure.

        Args:
            external_id: External id of the unit procedure to delete.
            space: The space where all the unit procedure are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete unit_procedure by id:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> client.unit_procedure.delete("my_unit_procedure")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> UnitProcedure:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> UnitProcedureList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> UnitProcedure | UnitProcedureList:
        """Retrieve one or more unit procedures by id(s).

        Args:
            external_id: External id or list of external ids of the unit procedures.
            space: The space where all the unit procedures are located.

        Returns:
            The requested unit procedures.

        Examples:

            Retrieve unit_procedure by id:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> unit_procedure = client.unit_procedure.retrieve("my_unit_procedure")

        """
        if isinstance(external_id, str):
            unit_procedure = self._retrieve((space, external_id))

            work_unit_edges = self.work_units.retrieve(external_id, space=space)
            unit_procedure.work_units = [edge.end_node.external_id for edge in work_unit_edges]

            return unit_procedure
        else:
            unit_procedures = self._retrieve([(space, ext_id) for ext_id in external_id])

            work_unit_edges = self.work_units.retrieve(unit_procedures.as_node_ids())
            self._set_work_units(unit_procedures, work_unit_edges)

            return unit_procedures

    def search(
        self,
        query: str,
        properties: UnitProcedureTextFields | Sequence[UnitProcedureTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> UnitProcedureList:
        """Search unit procedures

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of unit procedures to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `work_units` external ids for the unit procedures. Defaults to True.

        Returns:
            Search results unit procedures matching the query.

        Examples:

           Search for 'my_unit_procedure' in all text properties:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> unit_procedures = client.unit_procedure.search('my_unit_procedure')

        """
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _UNITPROCEDURE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: UnitProcedureFields | Sequence[UnitProcedureFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: UnitProcedureTextFields | Sequence[UnitProcedureTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: UnitProcedureFields | Sequence[UnitProcedureFields] | None = None,
        group_by: UnitProcedureFields | Sequence[UnitProcedureFields] = None,
        query: str | None = None,
        search_properties: UnitProcedureTextFields | Sequence[UnitProcedureTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: UnitProcedureFields | Sequence[UnitProcedureFields] | None = None,
        group_by: UnitProcedureFields | Sequence[UnitProcedureFields] | None = None,
        query: str | None = None,
        search_property: UnitProcedureTextFields | Sequence[UnitProcedureTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across unit procedures

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of unit procedures to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `work_units` external ids for the unit procedures. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count unit procedures in space `my_space`:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> result = client.unit_procedure.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _UNITPROCEDURE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: UnitProcedureFields,
        interval: float,
        query: str | None = None,
        search_property: UnitProcedureTextFields | Sequence[UnitProcedureTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for unit procedures

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of unit procedures to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `work_units` external ids for the unit procedures. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _UNITPROCEDURE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> UnitProcedureList:
        """List/filter unit procedures

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of unit procedures to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `work_units` external ids for the unit procedures. Defaults to True.

        Returns:
            List of requested unit procedures

        Examples:

            List unit procedures and limit to 5:

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> unit_procedures = client.unit_procedure.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )

        unit_procedures = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            space_arg = {"space": space} if space else {}
            if len(ids := unit_procedures.as_node_ids()) > IN_FILTER_LIMIT:
                work_unit_edges = self.work_units.list(limit=-1, **space_arg)
            else:
                work_unit_edges = self.work_units.list(ids, limit=-1)
            self._set_work_units(unit_procedures, work_unit_edges)

        return unit_procedures

    @staticmethod
    def _set_work_units(unit_procedures: Sequence[UnitProcedure], work_unit_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in work_unit_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for unit_procedure in unit_procedures:
            node_id = unit_procedure.id_tuple()
            if node_id in edges_by_start_node:
                unit_procedure.work_units = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    type_: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if type_ and isinstance(type_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type_))
    if type_ and isinstance(type_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type_))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
