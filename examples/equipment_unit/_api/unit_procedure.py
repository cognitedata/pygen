from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from equipment_unit.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
from equipment_unit.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    UnitProcedure,
    UnitProcedureWrite,
    UnitProcedureFields,
    UnitProcedureList,
    UnitProcedureWriteList,
    UnitProcedureTextFields,
    StartEndTime,
    StartEndTimeWrite,
    StartEndTimeList,
    EquipmentModule,
    StartEndTime,
    WorkOrder,
)
from equipment_unit.data_classes._unit_procedure import (
    UnitProcedureQuery,
    _UNITPROCEDURE_PROPERTIES_BY_FIELD,
    _create_unit_procedure_filter,
)
from equipment_unit._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from equipment_unit._api.unit_procedure_work_orders import UnitProcedureWorkOrdersAPI
from equipment_unit._api.unit_procedure_work_units import UnitProcedureWorkUnitsAPI
from equipment_unit._api.unit_procedure_query import UnitProcedureQueryAPI


class UnitProcedureAPI(NodeAPI[UnitProcedure, UnitProcedureWrite, UnitProcedureList, UnitProcedureWriteList]):
    _view_id = dm.ViewId("IntegrationTestsImmutable", "UnitProcedure", "a6e2fea1e1c664")
    _properties_by_field = _UNITPROCEDURE_PROPERTIES_BY_FIELD
    _class_type = UnitProcedure
    _class_list = UnitProcedureList
    _class_write_list = UnitProcedureWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.work_orders_edge = UnitProcedureWorkOrdersAPI(client)
        self.work_units_edge = UnitProcedureWorkUnitsAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> UnitProcedureQueryAPI[UnitProcedureList]:
        """Query starting at unit procedures.

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
            A query API for unit procedures.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_unit_procedure_filter(
            self._view_id,
            name,
            name_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(UnitProcedureList)
        return UnitProcedureQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        unit_procedure: UnitProcedureWrite | Sequence[UnitProcedureWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) unit procedures.

        Note: This method iterates through all nodes and timeseries linked to unit_procedure and creates them including the edges
        between the nodes. For example, if any of `work_orders` or `work_units` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            unit_procedure: Unit procedure or sequence of unit procedures to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new unit_procedure:

                >>> from equipment_unit import EquipmentUnitClient
                >>> from equipment_unit.data_classes import UnitProcedureWrite
                >>> client = EquipmentUnitClient()
                >>> unit_procedure = UnitProcedureWrite(external_id="my_unit_procedure", ...)
                >>> result = client.unit_procedure.apply(unit_procedure)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.unit_procedure.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(unit_procedure, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more unit procedure.

        Args:
            external_id: External id of the unit procedure to delete.
            space: The space where all the unit procedure are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete unit_procedure by id:

                >>> from equipment_unit import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> client.unit_procedure.delete("my_unit_procedure")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.unit_procedure.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> UnitProcedure | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> UnitProcedureList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> UnitProcedure | UnitProcedureList | None:
        """Retrieve one or more unit procedures by id(s).

        Args:
            external_id: External id or list of external ids of the unit procedures.
            space: The space where all the unit procedures are located.

        Returns:
            The requested unit procedures.

        Examples:

            Retrieve unit_procedure by id:

                >>> from equipment_unit import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> unit_procedure = client.unit_procedure.retrieve("my_unit_procedure")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.work_orders_edge,
                    "work_orders",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.work_order"),
                    "outwards",
                    dm.ViewId("IntegrationTestsImmutable", "WorkOrder", "c5543fb2b1bc81"),
                ),
                (
                    self.work_units_edge,
                    "work_units",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.equipment_module"),
                    "outwards",
                    dm.ViewId("IntegrationTestsImmutable", "EquipmentModule", "b1cd4bf14a7a33"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: UnitProcedureTextFields | SequenceNotStr[UnitProcedureTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: UnitProcedureFields | SequenceNotStr[UnitProcedureFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
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
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results unit procedures matching the query.

        Examples:

           Search for 'my_unit_procedure' in all text properties:

                >>> from equipment_unit import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> unit_procedures = client.unit_procedure.search('my_unit_procedure')

        """
        filter_ = _create_unit_procedure_filter(
            self._view_id,
            name,
            name_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: UnitProcedureFields | SequenceNotStr[UnitProcedureFields] | None = None,
        query: str | None = None,
        search_property: UnitProcedureTextFields | SequenceNotStr[UnitProcedureTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue: ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: UnitProcedureFields | SequenceNotStr[UnitProcedureFields] | None = None,
        query: str | None = None,
        search_property: UnitProcedureTextFields | SequenceNotStr[UnitProcedureTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: UnitProcedureFields | SequenceNotStr[UnitProcedureFields],
        property: UnitProcedureFields | SequenceNotStr[UnitProcedureFields] | None = None,
        query: str | None = None,
        search_property: UnitProcedureTextFields | SequenceNotStr[UnitProcedureTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: UnitProcedureFields | SequenceNotStr[UnitProcedureFields] | None = None,
        property: UnitProcedureFields | SequenceNotStr[UnitProcedureFields] | None = None,
        query: str | None = None,
        search_property: UnitProcedureTextFields | SequenceNotStr[UnitProcedureTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across unit procedures

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
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

        Returns:
            Aggregation results.

        Examples:

            Count unit procedures in space `my_space`:

                >>> from equipment_unit import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> result = client.unit_procedure.aggregate("count", space="my_space")

        """

        filter_ = _create_unit_procedure_filter(
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
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: UnitProcedureFields,
        interval: float,
        query: str | None = None,
        search_property: UnitProcedureTextFields | SequenceNotStr[UnitProcedureTextFields] | None = None,
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

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_unit_procedure_filter(
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
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    def query(self) -> UnitProcedureQuery:
        """Start a query for unit procedures."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return UnitProcedureQuery(self._client)

    def select(self) -> UnitProcedureQuery:
        """Start selecting from unit procedures."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return UnitProcedureQuery(self._client)

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
        sort_by: UnitProcedureFields | Sequence[UnitProcedureFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
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
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `work_orders` and `work_units` for the unit procedures. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested unit procedures

        Examples:

            List unit procedures and limit to 5:

                >>> from equipment_unit import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> unit_procedures = client.unit_procedure.list(limit=5)

        """
        filter_ = _create_unit_procedure_filter(
            self._view_id,
            name,
            name_prefix,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )

        if retrieve_connections == "skip":
            return self._list(
                limit=limit,
                filter=filter_,
                sort_by=sort_by,  # type: ignore[arg-type]
                direction=direction,
                sort=sort,
            )

        builder = DataClassQueryBuilder(UnitProcedureList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                UnitProcedure,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        edge_work_orders = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_work_orders,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
                StartEndTime,
            )
        )
        edge_work_units = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_work_units,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
                StartEndTime,
            )
        )
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name(edge_work_orders),
                    dm.query.NodeResultSetExpression(
                        from_=edge_work_orders,
                        filter=dm.filters.HasData(views=[WorkOrder._view_id]),
                    ),
                    WorkOrder,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(edge_work_units),
                    dm.query.NodeResultSetExpression(
                        from_=edge_work_units,
                        filter=dm.filters.HasData(views=[EquipmentModule._view_id]),
                    ),
                    EquipmentModule,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
