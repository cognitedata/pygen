from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from wind_turbine._api._core import (
    DEFAULT_LIMIT_READ,
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
from wind_turbine.data_classes._high_speed_shaft import (
    HighSpeedShaftQuery,
    _create_high_speed_shaft_filter,
)
from wind_turbine.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    HighSpeedShaft,
    HighSpeedShaftWrite,
    HighSpeedShaftFields,
    HighSpeedShaftList,
    HighSpeedShaftWriteList,
    HighSpeedShaftTextFields,
    Nacelle,
    SensorTimeSeries,
)
from wind_turbine._api.high_speed_shaft_query import HighSpeedShaftQueryAPI


class HighSpeedShaftAPI(NodeAPI[HighSpeedShaft, HighSpeedShaftWrite, HighSpeedShaftList, HighSpeedShaftWriteList]):
    _view_id = dm.ViewId("sp_pygen_power", "HighSpeedShaft", "1")
    _properties_by_field: ClassVar[dict[str, str]] = {}
    _class_type = HighSpeedShaft
    _class_list = HighSpeedShaftList
    _class_write_list = HighSpeedShaftWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        bending_moment_y: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        bending_monent_x: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        torque: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> HighSpeedShaftQueryAPI[HighSpeedShaft, HighSpeedShaftList]:
        """Query starting at high speed shafts.

        Args:
            bending_moment_y: The bending moment y to filter on.
            bending_monent_x: The bending monent x to filter on.
            torque: The torque to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of high speed shafts to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for high speed shafts.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_high_speed_shaft_filter(
            self._view_id,
            bending_moment_y,
            bending_monent_x,
            torque,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return HighSpeedShaftQueryAPI(
            self._client, QueryBuilder(), self._class_type, self._class_list, None, filter_, limit
        )

    def apply(
        self,
        high_speed_shaft: HighSpeedShaftWrite | Sequence[HighSpeedShaftWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) high speed shafts.

        Args:
            high_speed_shaft: High speed shaft or
                sequence of high speed shafts to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None.
                However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new high_speed_shaft:

                >>> from wind_turbine import WindTurbineClient
                >>> from wind_turbine.data_classes import HighSpeedShaftWrite
                >>> client = WindTurbineClient()
                >>> high_speed_shaft = HighSpeedShaftWrite(
                ...     external_id="my_high_speed_shaft", ...
                ... )
                >>> result = client.high_speed_shaft.apply(high_speed_shaft)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.high_speed_shaft.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(high_speed_shaft, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more high speed shaft.

        Args:
            external_id: External id of the high speed shaft to delete.
            space: The space where all the high speed shaft are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete high_speed_shaft by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> client.high_speed_shaft.delete("my_high_speed_shaft")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.high_speed_shaft.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> HighSpeedShaft | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> HighSpeedShaftList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> HighSpeedShaft | HighSpeedShaftList | None:
        """Retrieve one or more high speed shafts by id(s).

        Args:
            external_id: External id or list of external ids of the high speed shafts.
            space: The space where all the high speed shafts are located.
            retrieve_connections: Whether to retrieve `bending_moment_y`, `bending_monent_x`, `nacelle` and `torque` for
            the high speed shafts. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only
            retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested high speed shafts.

        Examples:

            Retrieve high_speed_shaft by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> high_speed_shaft = client.high_speed_shaft.retrieve(
                ...     "my_high_speed_shaft"
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
        properties: HighSpeedShaftTextFields | SequenceNotStr[HighSpeedShaftTextFields] | None = None,
        bending_moment_y: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        bending_monent_x: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        torque: (
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
        sort_by: HighSpeedShaftFields | SequenceNotStr[HighSpeedShaftFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> HighSpeedShaftList:
        """Search high speed shafts

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            bending_moment_y: The bending moment y to filter on.
            bending_monent_x: The bending monent x to filter on.
            torque: The torque to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of high speed shafts to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results high speed shafts matching the query.

        Examples:

           Search for 'my_high_speed_shaft' in all text properties:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> high_speed_shafts = client.high_speed_shaft.search(
                ...     'my_high_speed_shaft'
                ... )

        """
        filter_ = _create_high_speed_shaft_filter(
            self._view_id,
            bending_moment_y,
            bending_monent_x,
            torque,
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
        property: HighSpeedShaftFields | SequenceNotStr[HighSpeedShaftFields] | None = None,
        bending_moment_y: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        bending_monent_x: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        torque: (
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
        property: HighSpeedShaftFields | SequenceNotStr[HighSpeedShaftFields] | None = None,
        bending_moment_y: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        bending_monent_x: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        torque: (
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
        group_by: HighSpeedShaftFields | SequenceNotStr[HighSpeedShaftFields],
        property: HighSpeedShaftFields | SequenceNotStr[HighSpeedShaftFields] | None = None,
        bending_moment_y: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        bending_monent_x: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        torque: (
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
        group_by: HighSpeedShaftFields | SequenceNotStr[HighSpeedShaftFields] | None = None,
        property: HighSpeedShaftFields | SequenceNotStr[HighSpeedShaftFields] | None = None,
        bending_moment_y: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        bending_monent_x: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        torque: (
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
        """Aggregate data across high speed shafts

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            bending_moment_y: The bending moment y to filter on.
            bending_monent_x: The bending monent x to filter on.
            torque: The torque to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of high speed shafts to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count high speed shafts in space `my_space`:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> result = client.high_speed_shaft.aggregate("count", space="my_space")

        """

        filter_ = _create_high_speed_shaft_filter(
            self._view_id,
            bending_moment_y,
            bending_monent_x,
            torque,
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
        property: HighSpeedShaftFields,
        interval: float,
        bending_moment_y: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        bending_monent_x: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        torque: (
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
        """Produces histograms for high speed shafts

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            bending_moment_y: The bending moment y to filter on.
            bending_monent_x: The bending monent x to filter on.
            torque: The torque to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of high speed shafts to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_high_speed_shaft_filter(
            self._view_id,
            bending_moment_y,
            bending_monent_x,
            torque,
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

    def query(self) -> HighSpeedShaftQuery:
        """Start a query for high speed shafts."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return HighSpeedShaftQuery(self._client)

    def select(self) -> HighSpeedShaftQuery:
        """Start selecting from high speed shafts."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return HighSpeedShaftQuery(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> HighSpeedShaftList:
        builder = QueryBuilder()
        factory = QueryStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                limit=limit,
                has_container_fields=True,
            )
        )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_reverse_relation(
                    Nacelle._view_id,
                    through=dm.PropertyId(dm.ViewId("sp_pygen_power", "Nacelle", "1"), "high_speed_shaft"),
                    connection_type=None,
                    connection_property=ViewPropertyId(self._view_id, "nacelle"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "bending_moment_y"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "bending_monent_x"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "torque"),
                    has_container_fields=True,
                )
            )
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        unpacked = QueryUnpacker(builder, edges=unpack_edges).unpack()
        return HighSpeedShaftList([HighSpeedShaft.model_validate(item) for item in unpacked])

    def list(
        self,
        bending_moment_y: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        bending_monent_x: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        torque: (
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
    ) -> HighSpeedShaftList:
        """List/filter high speed shafts

        Args:
            bending_moment_y: The bending moment y to filter on.
            bending_monent_x: The bending monent x to filter on.
            torque: The torque to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of high speed shafts to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `bending_moment_y`, `bending_monent_x`, `nacelle` and `torque` for
            the high speed shafts. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only
            retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested high speed shafts

        Examples:

            List high speed shafts and limit to 5:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> high_speed_shafts = client.high_speed_shaft.list(limit=5)

        """
        filter_ = _create_high_speed_shaft_filter(
            self._view_id,
            bending_moment_y,
            bending_monent_x,
            torque,
            external_id_prefix,
            space,
            filter,
        )
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_)
        return self._query(filter_, limit, retrieve_connections)
