from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni._api._core import (
    DEFAULT_LIMIT_READ,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from omni.data_classes._connection_item_c_node import (
    ConnectionItemCNodeQuery,
    _create_connection_item_c_node_filter,
)
from omni.data_classes import (
    DomainModel,
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
from omni._api.connection_item_c_node_connection_item_a import ConnectionItemCNodeConnectionItemAAPI
from omni._api.connection_item_c_node_connection_item_b import ConnectionItemCNodeConnectionItemBAPI


class ConnectionItemCNodeAPI(
    NodeAPI[ConnectionItemCNode, ConnectionItemCNodeWrite, ConnectionItemCNodeList, ConnectionItemCNodeWriteList]
):
    _view_id = dm.ViewId("sp_pygen_models", "ConnectionItemC", "1")
    _properties_by_field: ClassVar[dict[str, str]] = {}
    _class_type = ConnectionItemCNode
    _class_list = ConnectionItemCNodeList
    _class_write_list = ConnectionItemCNodeWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.connection_item_a_edge = ConnectionItemCNodeConnectionItemAAPI(client)
        self.connection_item_b_edge = ConnectionItemCNodeConnectionItemBAPI(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemCNode | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemCNodeList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemCNode | ConnectionItemCNodeList | None:
        """Retrieve one or more connection item c nodes by id(s).

        Args:
            external_id: External id or list of external ids of the connection item c nodes.
            space: The space where all the connection item c nodes are located.
            retrieve_connections: Whether to retrieve `connection_item_a` and `connection_item_b` for the connection
            item c nodes. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve
            the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested connection item c nodes.

        Examples:

            Retrieve connection_item_c_node by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_c_node = client.connection_item_c_node.retrieve(
                ...     "my_connection_item_c_node"
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
            limit: Maximum number of connection item c nodes to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results connection item c nodes matching the query.

        Examples:

           Search for 'my_connection_item_c_node' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_c_nodes = client.connection_item_c_node.search(
                ...     'my_connection_item_c_node'
                ... )

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
            limit: Maximum number of connection item c nodes to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count connection item c nodes in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
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
            limit: Maximum number of connection item c nodes to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

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

    def select(self) -> ConnectionItemCNodeQuery:
        """Start selecting from connection item c nodes."""
        return ConnectionItemCNodeQuery(self._client)

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
                limit=limit,
                has_container_fields=False,
            )
        )
        builder.extend(
            factory.from_edge(
                ConnectionItemA._view_id,
                "outwards",
                ViewPropertyId(self._view_id, "connectionItemA"),
                include_end_node=retrieve_connections == "full",
                has_container_fields=True,
            )
        )
        builder.extend(
            factory.from_edge(
                ConnectionItemB._view_id,
                "outwards",
                ViewPropertyId(self._view_id, "connectionItemB"),
                include_end_node=retrieve_connections == "full",
                has_container_fields=True,
            )
        )
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        return QueryUnpacker(builder, edges=unpack_edges).unpack()

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
            limit: Maximum number of connection item c nodes to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `connection_item_a` and `connection_item_b` for the connection
            item c nodes. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve
            the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested connection item c nodes

        Examples:

            List connection item c nodes and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_c_nodes = client.connection_item_c_node.list(limit=5)

        """
        filter_ = _create_connection_item_c_node_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_)
        values = self._query(filter_, limit, retrieve_connections)
        return self._class_list(instantiate_classes(self._class_type, values, "list"))
