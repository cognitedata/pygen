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
from wind_turbine.data_classes._power_inverter import (
    PowerInverterQuery,
    _create_power_inverter_filter,
)
from wind_turbine.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    PowerInverter,
    PowerInverterWrite,
    PowerInverterFields,
    PowerInverterList,
    PowerInverterWriteList,
    PowerInverterTextFields,
    Nacelle,
    SensorTimeSeries,
)


class PowerInverterAPI(NodeAPI[PowerInverter, PowerInverterWrite, PowerInverterList, PowerInverterWriteList]):
    _view_id = dm.ViewId("sp_pygen_power", "PowerInverter", "1")
    _properties_by_field: ClassVar[dict[str, str]] = {}
    _class_type = PowerInverter
    _class_list = PowerInverterList
    _class_write_list = PowerInverterWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> PowerInverter | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> PowerInverterList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> PowerInverter | PowerInverterList | None:
        """Retrieve one or more power inverters by id(s).

        Args:
            external_id: External id or list of external ids of the power inverters.
            space: The space where all the power inverters are located.
            retrieve_connections: Whether to retrieve `active_power_total`, `apparent_power_total`, `nacelle` and
            `reactive_power_total` for the power inverters. Defaults to 'skip'.'skip' will not retrieve any connections,
            'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full
            connected items.

        Returns:
            The requested power inverters.

        Examples:

            Retrieve power_inverter by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> power_inverter = client.power_inverter.retrieve(
                ...     "my_power_inverter"
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
        properties: PowerInverterTextFields | SequenceNotStr[PowerInverterTextFields] | None = None,
        active_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        apparent_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        reactive_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PowerInverterFields | SequenceNotStr[PowerInverterFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PowerInverterList:
        """Search power inverters

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            active_power_total: The active power total to filter on.
            apparent_power_total: The apparent power total to filter on.
            reactive_power_total: The reactive power total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of power inverters to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results power inverters matching the query.

        Examples:

           Search for 'my_power_inverter' in all text properties:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> power_inverters = client.power_inverter.search(
                ...     'my_power_inverter'
                ... )

        """
        filter_ = _create_power_inverter_filter(
            self._view_id,
            active_power_total,
            apparent_power_total,
            reactive_power_total,
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
        property: PowerInverterFields | SequenceNotStr[PowerInverterFields] | None = None,
        active_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        apparent_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        reactive_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        property: PowerInverterFields | SequenceNotStr[PowerInverterFields] | None = None,
        active_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        apparent_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        reactive_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        group_by: PowerInverterFields | SequenceNotStr[PowerInverterFields],
        property: PowerInverterFields | SequenceNotStr[PowerInverterFields] | None = None,
        active_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        apparent_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        reactive_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        group_by: PowerInverterFields | SequenceNotStr[PowerInverterFields] | None = None,
        property: PowerInverterFields | SequenceNotStr[PowerInverterFields] | None = None,
        active_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        apparent_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        reactive_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across power inverters

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            active_power_total: The active power total to filter on.
            apparent_power_total: The apparent power total to filter on.
            reactive_power_total: The reactive power total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of power inverters to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count power inverters in space `my_space`:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> result = client.power_inverter.aggregate("count", space="my_space")

        """

        filter_ = _create_power_inverter_filter(
            self._view_id,
            active_power_total,
            apparent_power_total,
            reactive_power_total,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=None,
            search_properties=None,
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: PowerInverterFields,
        interval: float,
        active_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        apparent_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        reactive_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for power inverters

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            active_power_total: The active power total to filter on.
            apparent_power_total: The apparent power total to filter on.
            reactive_power_total: The reactive power total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of power inverters to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_power_inverter_filter(
            self._view_id,
            active_power_total,
            apparent_power_total,
            reactive_power_total,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            None,
            None,
            limit,
            filter_,
        )

    def select(self) -> PowerInverterQuery:
        """Start selecting from power inverters."""
        return PowerInverterQuery(self._client)

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
                limit=limit,
                max_retrieve_batch_limit=chunk_size,
                has_container_fields=True,
            )
        )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_reverse_relation(
                    Nacelle._view_id,
                    through=dm.PropertyId(dm.ViewId("sp_pygen_power", "Nacelle", "1"), "power_inverter"),
                    connection_type=None,
                    connection_property=ViewPropertyId(self._view_id, "nacelle"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "active_power_total"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "apparent_power_total"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "reactive_power_total"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        active_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        apparent_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        reactive_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[PowerInverterList]:
        """Iterate over power inverters

        Args:
            chunk_size: The number of power inverters to return in each iteration. Defaults to 100.
            active_power_total: The active power total to filter on.
            apparent_power_total: The apparent power total to filter on.
            reactive_power_total: The reactive power total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `active_power_total`, `apparent_power_total`, `nacelle` and
            `reactive_power_total` for the power inverters. Defaults to 'skip'.'skip' will not retrieve any connections,
            'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full
            connected items.
            limit: Maximum number of power inverters to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of power inverters

        Examples:

            Iterate power inverters in chunks of 100 up to 2000 items:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> for power_inverters in client.power_inverter.iterate(chunk_size=100, limit=2000):
                ...     for power_inverter in power_inverters:
                ...         print(power_inverter.external_id)

            Iterate power inverters in chunks of 100 sorted by external_id in descending order:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> for power_inverters in client.power_inverter.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for power_inverter in power_inverters:
                ...         print(power_inverter.external_id)

            Iterate power inverters in chunks of 100 and use cursors to resume the iteration:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> for first_iteration in client.power_inverter.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for power_inverters in client.power_inverter.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for power_inverter in power_inverters:
                ...         print(power_inverter.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_power_inverter_filter(
            self._view_id,
            active_power_total,
            apparent_power_total,
            reactive_power_total,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        active_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        apparent_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        reactive_power_total: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> PowerInverterList:
        """List/filter power inverters

        Args:
            active_power_total: The active power total to filter on.
            apparent_power_total: The apparent power total to filter on.
            reactive_power_total: The reactive power total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of power inverters to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `active_power_total`, `apparent_power_total`, `nacelle` and
            `reactive_power_total` for the power inverters. Defaults to 'skip'.'skip' will not retrieve any connections,
            'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full
            connected items.

        Returns:
            List of requested power inverters

        Examples:

            List power inverters and limit to 5:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> power_inverters = client.power_inverter.list(limit=5)

        """
        filter_ = _create_power_inverter_filter(
            self._view_id,
            active_power_total,
            apparent_power_total,
            reactive_power_total,
            external_id_prefix,
            space,
            filter,
        )
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_)
        return self._query(filter_, limit, retrieve_connections, None, "list")
