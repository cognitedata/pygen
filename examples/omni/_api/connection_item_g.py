from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from omni._api.connection_item_g_inwards_multi_property import ConnectionItemGInwardsMultiPropertyAPI
from omni._api.connection_item_g_query import ConnectionItemGQueryAPI
from omni.data_classes import (
    ConnectionEdgeA,
    ConnectionItemF,
    ConnectionItemG,
    ConnectionItemGFields,
    ConnectionItemGList,
    ConnectionItemGTextFields,
    ConnectionItemGWrite,
    ConnectionItemGWriteList,
    ResourcesWriteResult,
)
from omni.data_classes._connection_item_g import (
    _CONNECTIONITEMG_PROPERTIES_BY_FIELD,
    ConnectionItemGQuery,
    _create_connection_item_g_filter,
)
from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    EdgeQueryStep,
    NodeQueryStep,
)


class ConnectionItemGAPI(NodeAPI[ConnectionItemG, ConnectionItemGWrite, ConnectionItemGList, ConnectionItemGWriteList]):
    _view_id = dm.ViewId("sp_pygen_models", "ConnectionItemG", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _CONNECTIONITEMG_PROPERTIES_BY_FIELD
    _class_type = ConnectionItemG
    _class_list = ConnectionItemGList
    _class_write_list = ConnectionItemGWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.inwards_multi_property_edge = ConnectionItemGInwardsMultiPropertyAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ConnectionItemGQueryAPI[ConnectionItemGList]:
        """Query starting at connection item gs.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item gs to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for connection item gs.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_connection_item_g_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(ConnectionItemGList)
        return ConnectionItemGQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        connection_item_g: ConnectionItemGWrite | Sequence[ConnectionItemGWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) connection item gs.

        Note: This method iterates through all nodes and timeseries linked to connection_item_g
        and creates them including the edges
        between the nodes. For example, if any of `inwards_multi_property` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            connection_item_g: Connection item g or
                sequence of connection item gs to upsert.
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

            Create a new connection_item_g:

                >>> from omni import OmniClient
                >>> from omni.data_classes import ConnectionItemGWrite
                >>> client = OmniClient()
                >>> connection_item_g = ConnectionItemGWrite(
                ...     external_id="my_connection_item_g", ...
                ... )
                >>> result = client.connection_item_g.apply(connection_item_g)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.connection_item_g.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(connection_item_g, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more connection item g.

        Args:
            external_id: External id of the connection item g to delete.
            space: The space where all the connection item g are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete connection_item_g by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.connection_item_g.delete("my_connection_item_g")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.connection_item_g.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ConnectionItemG | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ConnectionItemGList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> ConnectionItemG | ConnectionItemGList | None:
        """Retrieve one or more connection item gs by id(s).

        Args:
            external_id: External id or list of external ids of the connection item gs.
            space: The space where all the connection item gs are located.

        Returns:
            The requested connection item gs.

        Examples:

            Retrieve connection_item_g by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_g = client.connection_item_g.retrieve("my_connection_item_g")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.inwards_multi_property_edge,
                    "inwards_multi_property",
                    dm.DirectRelationReference("sp_pygen_models", "multiProperty"),
                    "inwards",
                    dm.ViewId("sp_pygen_models", "ConnectionItemF", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: ConnectionItemGTextFields | SequenceNotStr[ConnectionItemGTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemGFields | SequenceNotStr[ConnectionItemGFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ConnectionItemGList:
        """Search connection item gs

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item gs to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results connection item gs matching the query.

        Examples:

           Search for 'my_connection_item_g' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_gs = client.connection_item_g.search('my_connection_item_g')

        """
        filter_ = _create_connection_item_g_filter(
            self._view_id,
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
        property: ConnectionItemGFields | SequenceNotStr[ConnectionItemGFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemGTextFields | SequenceNotStr[ConnectionItemGTextFields] | None = None,
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
        property: ConnectionItemGFields | SequenceNotStr[ConnectionItemGFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemGTextFields | SequenceNotStr[ConnectionItemGTextFields] | None = None,
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
        group_by: ConnectionItemGFields | SequenceNotStr[ConnectionItemGFields],
        property: ConnectionItemGFields | SequenceNotStr[ConnectionItemGFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemGTextFields | SequenceNotStr[ConnectionItemGTextFields] | None = None,
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
        group_by: ConnectionItemGFields | SequenceNotStr[ConnectionItemGFields] | None = None,
        property: ConnectionItemGFields | SequenceNotStr[ConnectionItemGFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemGTextFields | SequenceNotStr[ConnectionItemGTextFields] | None = None,
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
        """Aggregate data across connection item gs

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item gs to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count connection item gs in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.connection_item_g.aggregate("count", space="my_space")

        """

        filter_ = _create_connection_item_g_filter(
            self._view_id,
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
        property: ConnectionItemGFields,
        interval: float,
        query: str | None = None,
        search_property: ConnectionItemGTextFields | SequenceNotStr[ConnectionItemGTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for connection item gs

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item gs to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_connection_item_g_filter(
            self._view_id,
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

    def query(self) -> ConnectionItemGQuery:
        """Start a query for connection item gs."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return ConnectionItemGQuery(self._client)

    def select(self) -> ConnectionItemGQuery:
        """Start selecting from connection item gs."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return ConnectionItemGQuery(self._client)

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemGFields | Sequence[ConnectionItemGFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemGList:
        """List/filter connection item gs

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item gs to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `inwards_multi_property`
                for the connection item gs. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the
                identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested connection item gs

        Examples:

            List connection item gs and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_gs = client.connection_item_g.list(limit=5)

        """
        filter_ = _create_connection_item_g_filter(
            self._view_id,
            name,
            name_prefix,
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

        builder = DataClassQueryBuilder(ConnectionItemGList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                ConnectionItemG,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        edge_inwards_multi_property = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_inwards_multi_property,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="inwards",
                    chain_to="destination",
                ),
                ConnectionEdgeA,
            )
        )
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name(edge_inwards_multi_property),
                    dm.query.NodeResultSetExpression(
                        from_=edge_inwards_multi_property,
                        filter=dm.filters.HasData(views=[ConnectionItemF._view_id]),
                    ),
                    ConnectionItemF,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
