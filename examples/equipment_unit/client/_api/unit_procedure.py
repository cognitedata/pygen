from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from equipment_unit.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from equipment_unit.client.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    UnitProcedure,
    UnitProcedureApply,
    UnitProcedureFields,
    UnitProcedureList,
    UnitProcedureApplyList,
    UnitProcedureTextFields,
    StartEndTime,
    StartEndTimeApply,
    StartEndTimeList,
)
from equipment_unit.client.data_classes._unit_procedure import (
    _UNITPROCEDURE_PROPERTIES_BY_FIELD,
    _create_unit_procedure_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .unit_procedure_work_orders import UnitProcedureWorkOrdersAPI
from .unit_procedure_work_units import UnitProcedureWorkUnitsAPI
from .unit_procedure_query import UnitProcedureQueryAPI


class UnitProcedureAPI(NodeAPI[UnitProcedure, UnitProcedureApply, UnitProcedureList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[UnitProcedure]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=UnitProcedure,
            class_list=UnitProcedureList,
            class_apply_list=UnitProcedureApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.work_orders_edge = UnitProcedureWorkOrdersAPI(
            client, view_by_read_class, StartEndTime, StartEndTimeApply, StartEndTimeList
        )
        self.work_units_edge = UnitProcedureWorkUnitsAPI(
            client, view_by_read_class, StartEndTime, StartEndTimeApply, StartEndTimeList
        )

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
        builder = QueryBuilder(UnitProcedureList)
        return UnitProcedureQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        unit_procedure: UnitProcedureApply | Sequence[UnitProcedureApply],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesApplyResult:
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

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> from equipment_unit.client.data_classes import UnitProcedureApply
                >>> client = EquipmentUnitClient()
                >>> unit_procedure = UnitProcedureApply(external_id="my_unit_procedure", ...)
                >>> result = client.unit_procedure.apply(unit_procedure)

        """
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

                >>> from equipment_unit.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> client.unit_procedure.delete("my_unit_procedure")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> UnitProcedure | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> UnitProcedureList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> UnitProcedure | UnitProcedureList | None:
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
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_quad=[
                (
                    self.work_orders_edge,
                    "work_orders",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.work_order"),
                    "outwards",
                ),
                (
                    self.work_units_edge,
                    "work_units",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.equipment_module"),
                    "outwards",
                ),
            ],
        )

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

        Returns:
            Search results unit procedures matching the query.

        Examples:

           Search for 'my_unit_procedure' in all text properties:

                >>> from equipment_unit.client import EquipmentUnitClient
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

        Returns:
            Aggregation results.

        Examples:

            Count unit procedures in space `my_space`:

                >>> from equipment_unit.client import EquipmentUnitClient
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
            retrieve_edges: Whether to retrieve `work_orders` or `work_units` external ids for the unit procedures. Defaults to True.

        Returns:
            List of requested unit procedures

        Examples:

            List unit procedures and limit to 5:

                >>> from equipment_unit.client import EquipmentUnitClient
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

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_quad=[
                (
                    self.work_orders_edge,
                    "work_orders",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.work_order"),
                    "outwards",
                ),
                (
                    self.work_units_edge,
                    "work_units",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.equipment_module"),
                    "outwards",
                ),
            ],
        )
