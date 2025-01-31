from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from wind_turbine._api._core import (
    DEFAULT_LIMIT_READ,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from wind_turbine.data_classes._generating_unit import (
    GeneratingUnitQuery,
    _GENERATINGUNIT_PROPERTIES_BY_FIELD,
    _create_generating_unit_filter,
)
from wind_turbine.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    GeneratingUnit,
    GeneratingUnitWrite,
    GeneratingUnitFields,
    GeneratingUnitList,
    GeneratingUnitWriteList,
    GeneratingUnitTextFields,
    SolarPanel,
    WindTurbine,
)


class GeneratingUnitAPI(NodeAPI[GeneratingUnit, GeneratingUnitWrite, GeneratingUnitList, GeneratingUnitWriteList]):
    _view_id = dm.ViewId("sp_pygen_power", "GeneratingUnit", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _GENERATINGUNIT_PROPERTIES_BY_FIELD
    _direct_children_by_external_id: ClassVar[dict[str, type[DomainModel]]] = {
        "SolarPanel": SolarPanel,
        "WindTurbine": WindTurbine,
    }
    _class_type = GeneratingUnit
    _class_list = GeneratingUnitList
    _class_write_list = GeneratingUnitWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["SolarPanel", "WindTurbine"]] | None = None,
    ) -> GeneratingUnit | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["SolarPanel", "WindTurbine"]] | None = None,
    ) -> GeneratingUnitList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["SolarPanel", "WindTurbine"]] | None = None,
    ) -> GeneratingUnit | GeneratingUnitList | None:
        """Retrieve one or more generating units by id(s).

        Args:
            external_id: External id or list of external ids of the generating units.
            space: The space where all the generating units are located.
            as_child_class: If you want to retrieve the generating units as a child class,
                you can specify the child class here. Note that if one node has properties in
                multiple child classes, you will get duplicate nodes in the result.

        Returns:
            The requested generating units.

        Examples:

            Retrieve generating_unit by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> generating_unit = client.generating_unit.retrieve(
                ...     "my_generating_unit"
                ... )

        """
        return self._retrieve(external_id, space, as_child_class=as_child_class)

    def search(
        self,
        query: str,
        properties: GeneratingUnitTextFields | SequenceNotStr[GeneratingUnitTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: GeneratingUnitFields | SequenceNotStr[GeneratingUnitFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> GeneratingUnitList:
        """Search generating units

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generating units to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results generating units matching the query.

        Examples:

           Search for 'my_generating_unit' in all text properties:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> generating_units = client.generating_unit.search(
                ...     'my_generating_unit'
                ... )

        """
        filter_ = _create_generating_unit_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            description,
            description_prefix,
            name,
            name_prefix,
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
        property: GeneratingUnitFields | SequenceNotStr[GeneratingUnitFields] | None = None,
        query: str | None = None,
        search_property: GeneratingUnitTextFields | SequenceNotStr[GeneratingUnitTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: GeneratingUnitFields | SequenceNotStr[GeneratingUnitFields] | None = None,
        query: str | None = None,
        search_property: GeneratingUnitTextFields | SequenceNotStr[GeneratingUnitTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        group_by: GeneratingUnitFields | SequenceNotStr[GeneratingUnitFields],
        property: GeneratingUnitFields | SequenceNotStr[GeneratingUnitFields] | None = None,
        query: str | None = None,
        search_property: GeneratingUnitTextFields | SequenceNotStr[GeneratingUnitTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        group_by: GeneratingUnitFields | SequenceNotStr[GeneratingUnitFields] | None = None,
        property: GeneratingUnitFields | SequenceNotStr[GeneratingUnitFields] | None = None,
        query: str | None = None,
        search_property: GeneratingUnitTextFields | SequenceNotStr[GeneratingUnitTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across generating units

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generating units to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count generating units in space `my_space`:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> result = client.generating_unit.aggregate("count", space="my_space")

        """

        filter_ = _create_generating_unit_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            description,
            description_prefix,
            name,
            name_prefix,
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
        property: GeneratingUnitFields,
        interval: float,
        query: str | None = None,
        search_property: GeneratingUnitTextFields | SequenceNotStr[GeneratingUnitTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for generating units

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generating units to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_generating_unit_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            description,
            description_prefix,
            name,
            name_prefix,
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

    def select(self) -> GeneratingUnitQuery:
        """Start selecting from generating units."""
        return GeneratingUnitQuery(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> list[dict[str, Any]]:
        builder = QueryBuilder()
        factory = QueryStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                sort=sort,
                limit=limit,
                has_container_fields=True,
            )
        )
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        return QueryUnpacker(builder, edges=unpack_edges).unpack()

    def list(
        self,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: GeneratingUnitFields | Sequence[GeneratingUnitFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> GeneratingUnitList:
        """List/filter generating units

        Args:
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generating units to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested generating units

        Examples:

            List generating units and limit to 5:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> generating_units = client.generating_unit.list(limit=5)

        """
        filter_ = _create_generating_unit_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            description,
            description_prefix,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
