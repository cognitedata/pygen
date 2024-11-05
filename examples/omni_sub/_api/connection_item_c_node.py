from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni_sub.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
from omni_sub.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ConnectionItemCNode,
    ConnectionItemCNodeWrite,
    ConnectionItemCNodeFields,
    ConnectionItemCNodeList,
    ConnectionItemCNodeWriteList,
    ConnectionItemCNodeTextFields,
    ConnectionItemA,
    ConnectionItemB,
)
from omni_sub.data_classes._connection_item_c_node import (
    ConnectionItemCNodeQuery,
    _create_connection_item_c_node_filter,
)
from omni_sub._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from omni_sub._api.connection_item_c_node_connection_item_a import ConnectionItemCNodeConnectionItemAAPI
from omni_sub._api.connection_item_c_node_connection_item_b import ConnectionItemCNodeConnectionItemBAPI
from omni_sub._api.connection_item_c_node_query import ConnectionItemCNodeQueryAPI


class ConnectionItemCNodeAPI(
    NodeAPI[ConnectionItemCNode, ConnectionItemCNodeWrite, ConnectionItemCNodeList, ConnectionItemCNodeWriteList]
):
    _view_id = dm.ViewId("sp_pygen_models", "ConnectionItemC", "1")
    _properties_by_field = {}
    _class_type = ConnectionItemCNode
    _class_list = ConnectionItemCNodeList
    _class_write_list = ConnectionItemCNodeWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.connection_item_a_edge = ConnectionItemCNodeConnectionItemAAPI(client)
        self.connection_item_b_edge = ConnectionItemCNodeConnectionItemBAPI(client)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ConnectionItemCNodeQueryAPI[ConnectionItemCNodeList]:
        """Query starting at connection item c nodes.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item c nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for connection item c nodes.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_connection_item_c_node_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(ConnectionItemCNodeList)
        return ConnectionItemCNodeQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        connection_item_c_node: ConnectionItemCNodeWrite | Sequence[ConnectionItemCNodeWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) connection item c nodes.

        Note: This method iterates through all nodes and timeseries linked to connection_item_c_node and creates them including the edges
        between the nodes. For example, if any of `connection_item_a` or `connection_item_b` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            connection_item_c_node: Connection item c node or sequence of connection item c nodes to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new connection_item_c_node:

                >>> from omni_sub import OmniSubClient
                >>> from omni_sub.data_classes import ConnectionItemCNodeWrite
                >>> client = OmniSubClient()
                >>> connection_item_c_node = ConnectionItemCNodeWrite(external_id="my_connection_item_c_node", ...)
                >>> result = client.connection_item_c_node.apply(connection_item_c_node)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.connection_item_c_node.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(connection_item_c_node, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str) -> dm.InstancesDeleteResult:
        """Delete one or more connection item c node.

        Args:
            external_id: External id of the connection item c node to delete.
            space: The space where all the connection item c node are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete connection_item_c_node by id:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> client.connection_item_c_node.delete("my_connection_item_c_node")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.connection_item_c_node.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str], space: str) -> ConnectionItemCNode | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str
    ) -> ConnectionItemCNodeList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
    ) -> ConnectionItemCNode | ConnectionItemCNodeList | None:
        """Retrieve one or more connection item c nodes by id(s).

        Args:
            external_id: External id or list of external ids of the connection item c nodes.
            space: The space where all the connection item c nodes are located.

        Returns:
            The requested connection item c nodes.

        Examples:

            Retrieve connection_item_c_node by id:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> connection_item_c_node = client.connection_item_c_node.retrieve("my_connection_item_c_node")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.connection_item_a_edge,
                    "connection_item_a",
                    dm.DirectRelationReference("sp_pygen_models", "unidirectional"),
                    "outwards",
                    dm.ViewId("sp_pygen_models", "ConnectionItemA", "1"),
                ),
                (
                    self.connection_item_b_edge,
                    "connection_item_b",
                    dm.DirectRelationReference("sp_pygen_models", "unidirectional"),
                    "outwards",
                    dm.ViewId("sp_pygen_models", "ConnectionItemB", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: ConnectionItemCNodeTextFields | SequenceNotStr[ConnectionItemCNodeTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemCNodeFields | SequenceNotStr[ConnectionItemCNodeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ConnectionItemCNodeList:
        """Search connection item c nodes

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item c nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results connection item c nodes matching the query.

        Examples:

           Search for 'my_connection_item_c_node' in all text properties:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> connection_item_c_nodes = client.connection_item_c_node.search('my_connection_item_c_node')

        """
        filter_ = _create_connection_item_c_node_filter(
            self._view_id,
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
        property: ConnectionItemCNodeFields | SequenceNotStr[ConnectionItemCNodeFields] | None = None,
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
        property: ConnectionItemCNodeFields | SequenceNotStr[ConnectionItemCNodeFields] | None = None,
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
        group_by: ConnectionItemCNodeFields | SequenceNotStr[ConnectionItemCNodeFields],
        property: ConnectionItemCNodeFields | SequenceNotStr[ConnectionItemCNodeFields] | None = None,
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
        group_by: ConnectionItemCNodeFields | SequenceNotStr[ConnectionItemCNodeFields] | None = None,
        property: ConnectionItemCNodeFields | SequenceNotStr[ConnectionItemCNodeFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across connection item c nodes

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item c nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count connection item c nodes in space `my_space`:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> result = client.connection_item_c_node.aggregate("count", space="my_space")

        """

        filter_ = _create_connection_item_c_node_filter(
            self._view_id,
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
        property: ConnectionItemCNodeFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for connection item c nodes

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item c nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_connection_item_c_node_filter(
            self._view_id,
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

    @property
    def query(self) -> ConnectionItemCNodeQuery:
        """Start a query for connection item c nodes."""
        warnings.warn("The .query is in alpha and is subject to breaking changes without notice.")
        return ConnectionItemCNodeQuery(self._client)

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemCNodeList:
        """List/filter connection item c nodes

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item c nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `connection_item_a` and `connection_item_b` for the connection item c nodes. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested connection item c nodes

        Examples:

            List connection item c nodes and limit to 5:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> connection_item_c_nodes = client.connection_item_c_node.list(limit=5)

        """
        filter_ = _create_connection_item_c_node_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )

        if retrieve_connections == "skip":
            return self._list(
                limit=limit,
                filter=filter_,
            )

        builder = DataClassQueryBuilder(ConnectionItemCNodeList)
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=filter_,
                ),
                ConnectionItemCNode,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        edge_connection_item_a = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_connection_item_a,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
            )
        )
        edge_connection_item_b = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_connection_item_b,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
            )
        )
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name(edge_connection_item_a),
                    dm.query.NodeResultSetExpression(
                        from_=edge_connection_item_a,
                        filter=dm.filters.HasData(views=[ConnectionItemA._view_id]),
                    ),
                    ConnectionItemA,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(edge_connection_item_b),
                    dm.query.NodeResultSetExpression(
                        from_=edge_connection_item_b,
                        filter=dm.filters.HasData(views=[ConnectionItemB._view_id]),
                    ),
                    ConnectionItemB,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
