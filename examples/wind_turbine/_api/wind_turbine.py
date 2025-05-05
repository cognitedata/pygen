from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from wind_turbine._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from wind_turbine.data_classes._wind_turbine import (
    WindTurbineQuery,
    _WINDTURBINE_PROPERTIES_BY_FIELD,
    _create_wind_turbine_filter,
)
from wind_turbine.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    WindTurbine,
    WindTurbineWrite,
    WindTurbineFields,
    WindTurbineList,
    WindTurbineWriteList,
    WindTurbineTextFields,
    Distance,
    DistanceWrite,
    DistanceList,
    Blade,
    DataSheet,
    Distance,
    Metmast,
    Nacelle,
    Rotor,
)
from wind_turbine._api.wind_turbine_metmast import WindTurbineMetmastAPI


class WindTurbineAPI(NodeAPI[WindTurbine, WindTurbineWrite, WindTurbineList, WindTurbineWriteList]):
    _view_id = dm.ViewId("sp_pygen_power", "WindTurbine", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _WINDTURBINE_PROPERTIES_BY_FIELD
    _class_type = WindTurbine
    _class_list = WindTurbineList
    _class_write_list = WindTurbineWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.metmast_edge = WindTurbineMetmastAPI(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> WindTurbine | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> WindTurbineList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> WindTurbine | WindTurbineList | None:
        """Retrieve one or more wind turbines by id(s).

        Args:
            external_id: External id or list of external ids of the wind turbines.
            space: The space where all the wind turbines are located.
            retrieve_connections: Whether to retrieve `blades`, `datasheets`, `metmast`, `nacelle` and `rotor` for the
            wind turbines. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve
            the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested wind turbines.

        Examples:

            Retrieve wind_turbine by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> wind_turbine = client.wind_turbine.retrieve(
                ...     "my_wind_turbine"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
        )

    def search(
        self,
        query: str,
        properties: WindTurbineTextFields | SequenceNotStr[WindTurbineTextFields] | None = None,
        blades: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        datasheets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: WindTurbineFields | SequenceNotStr[WindTurbineFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> WindTurbineList:
        """Search wind turbines

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            blades: The blade to filter on.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            datasheets: The datasheet to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            nacelle: The nacelle to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            rotor: The rotor to filter on.
            windfarm: The windfarm to filter on.
            windfarm_prefix: The prefix of the windfarm to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wind turbines to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results wind turbines matching the query.

        Examples:

           Search for 'my_wind_turbine' in all text properties:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> wind_turbines = client.wind_turbine.search(
                ...     'my_wind_turbine'
                ... )

        """
        filter_ = _create_wind_turbine_filter(
            self._view_id,
            blades,
            min_capacity,
            max_capacity,
            datasheets,
            description,
            description_prefix,
            nacelle,
            name,
            name_prefix,
            rotor,
            windfarm,
            windfarm_prefix,
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
        property: WindTurbineFields | SequenceNotStr[WindTurbineFields] | None = None,
        query: str | None = None,
        search_property: WindTurbineTextFields | SequenceNotStr[WindTurbineTextFields] | None = None,
        blades: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        datasheets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
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
        property: WindTurbineFields | SequenceNotStr[WindTurbineFields] | None = None,
        query: str | None = None,
        search_property: WindTurbineTextFields | SequenceNotStr[WindTurbineTextFields] | None = None,
        blades: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        datasheets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
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
        group_by: WindTurbineFields | SequenceNotStr[WindTurbineFields],
        property: WindTurbineFields | SequenceNotStr[WindTurbineFields] | None = None,
        query: str | None = None,
        search_property: WindTurbineTextFields | SequenceNotStr[WindTurbineTextFields] | None = None,
        blades: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        datasheets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
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
        group_by: WindTurbineFields | SequenceNotStr[WindTurbineFields] | None = None,
        property: WindTurbineFields | SequenceNotStr[WindTurbineFields] | None = None,
        query: str | None = None,
        search_property: WindTurbineTextFields | SequenceNotStr[WindTurbineTextFields] | None = None,
        blades: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        datasheets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across wind turbines

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            blades: The blade to filter on.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            datasheets: The datasheet to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            nacelle: The nacelle to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            rotor: The rotor to filter on.
            windfarm: The windfarm to filter on.
            windfarm_prefix: The prefix of the windfarm to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wind turbines to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count wind turbines in space `my_space`:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> result = client.wind_turbine.aggregate("count", space="my_space")

        """

        filter_ = _create_wind_turbine_filter(
            self._view_id,
            blades,
            min_capacity,
            max_capacity,
            datasheets,
            description,
            description_prefix,
            nacelle,
            name,
            name_prefix,
            rotor,
            windfarm,
            windfarm_prefix,
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
        property: WindTurbineFields,
        interval: float,
        query: str | None = None,
        search_property: WindTurbineTextFields | SequenceNotStr[WindTurbineTextFields] | None = None,
        blades: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        datasheets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for wind turbines

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            blades: The blade to filter on.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            datasheets: The datasheet to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            nacelle: The nacelle to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            rotor: The rotor to filter on.
            windfarm: The windfarm to filter on.
            windfarm_prefix: The prefix of the windfarm to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wind turbines to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_wind_turbine_filter(
            self._view_id,
            blades,
            min_capacity,
            max_capacity,
            datasheets,
            description,
            description_prefix,
            nacelle,
            name,
            name_prefix,
            rotor,
            windfarm,
            windfarm_prefix,
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

    def select(self) -> WindTurbineQuery:
        """Start selecting from wind turbines."""
        return WindTurbineQuery(self._client)

    def _build(
        self,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        chunk_size: int | None = None,
    ) -> QueryExecutor:
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                sort=sort,
                limit=limit,
                max_retrieve_batch_limit=chunk_size,
                has_container_fields=True,
            )
        )
        if retrieve_connections == "identifier" or retrieve_connections == "full":
            builder.extend(
                factory.from_edge(
                    Metmast._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "metmast"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                    edge_view=Distance._view_id,
                )
            )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    Blade._view_id,
                    ViewPropertyId(self._view_id, "blades"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    DataSheet._view_id,
                    ViewPropertyId(self._view_id, "datasheets"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    Nacelle._view_id,
                    ViewPropertyId(self._view_id, "nacelle"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    Rotor._view_id,
                    ViewPropertyId(self._view_id, "rotor"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        blades: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        datasheets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[WindTurbineList]:
        """Iterate over wind turbines

        Args:
            chunk_size: The number of wind turbines to return in each iteration. Defaults to 100.
            blades: The blade to filter on.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            datasheets: The datasheet to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            nacelle: The nacelle to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            rotor: The rotor to filter on.
            windfarm: The windfarm to filter on.
            windfarm_prefix: The prefix of the windfarm to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `blades`, `datasheets`, `metmast`, `nacelle` and `rotor` for the
            wind turbines. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve
            the identifier of the connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of wind turbines to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of wind turbines

        Examples:

            Iterate wind turbines in chunks of 100 up to 2000 items:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> for wind_turbines in client.wind_turbine.iterate(chunk_size=100, limit=2000):
                ...     for wind_turbine in wind_turbines:
                ...         print(wind_turbine.external_id)

            Iterate wind turbines in chunks of 100 sorted by external_id in descending order:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> for wind_turbines in client.wind_turbine.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for wind_turbine in wind_turbines:
                ...         print(wind_turbine.external_id)

            Iterate wind turbines in chunks of 100 and use cursors to resume the iteration:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> for first_iteration in client.wind_turbine.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for wind_turbines in client.wind_turbine.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for wind_turbine in wind_turbines:
                ...         print(wind_turbine.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_wind_turbine_filter(
            self._view_id,
            blades,
            min_capacity,
            max_capacity,
            datasheets,
            description,
            description_prefix,
            nacelle,
            name,
            name_prefix,
            rotor,
            windfarm,
            windfarm_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        blades: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        datasheets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: WindTurbineFields | Sequence[WindTurbineFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> WindTurbineList:
        """List/filter wind turbines

        Args:
            blades: The blade to filter on.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            datasheets: The datasheet to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            nacelle: The nacelle to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            rotor: The rotor to filter on.
            windfarm: The windfarm to filter on.
            windfarm_prefix: The prefix of the windfarm to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wind turbines to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `blades`, `datasheets`, `metmast`, `nacelle` and `rotor` for the
            wind turbines. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve
            the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested wind turbines

        Examples:

            List wind turbines and limit to 5:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> wind_turbines = client.wind_turbine.list(limit=5)

        """
        filter_ = _create_wind_turbine_filter(
            self._view_id,
            blades,
            min_capacity,
            max_capacity,
            datasheets,
            description,
            description_prefix,
            nacelle,
            name,
            name_prefix,
            rotor,
            windfarm,
            windfarm_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
