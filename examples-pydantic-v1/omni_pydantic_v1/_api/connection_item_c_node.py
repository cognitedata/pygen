from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from omni_pydantic_v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    QueryBuilder,
)
from omni_pydantic_v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ConnectionItemCNode,
    ConnectionItemCNodeWrite,
    ConnectionItemCNodeList,
    ConnectionItemCNodeWriteList,
    ConnectionItemA,
    ConnectionItemB,
)
from omni_pydantic_v1.data_classes._connection_item_c_node import (
    ConnectionItemCNodeQuery,
    _create_connection_item_c_node_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from .connection_item_c_node_connection_item_a import ConnectionItemCNodeConnectionItemAAPI
from .connection_item_c_node_connection_item_b import ConnectionItemCNodeConnectionItemBAPI
from .connection_item_c_node_query import ConnectionItemCNodeQueryAPI


class ConnectionItemCNodeAPI(
    NodeAPI[ConnectionItemCNode, ConnectionItemCNodeWrite, ConnectionItemCNodeList, ConnectionItemCNodeWriteList]
):
    _view_id = dm.ViewId("pygen-models", "ConnectionItemC", "1")
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
        builder = QueryBuilder(ConnectionItemCNodeList)
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

                >>> from omni_pydantic_v1 import OmniClient
                >>> from omni_pydantic_v1.data_classes import ConnectionItemCNodeWrite
                >>> client = OmniClient()
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

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more connection item c node.

        Args:
            external_id: External id of the connection item c node to delete.
            space: The space where all the connection item c node are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete connection_item_c_node by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
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
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ConnectionItemCNode | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ConnectionItemCNodeList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ConnectionItemCNode | ConnectionItemCNodeList | None:
        """Retrieve one or more connection item c nodes by id(s).

        Args:
            external_id: External id or list of external ids of the connection item c nodes.
            space: The space where all the connection item c nodes are located.

        Returns:
            The requested connection item c nodes.

        Examples:

            Retrieve connection_item_c_node by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
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
                    dm.DirectRelationReference("pygen-models", "unidirectional"),
                    "outwards",
                    dm.ViewId("pygen-models", "ConnectionItemA", "1"),
                ),
                (
                    self.connection_item_b_edge,
                    "connection_item_b",
                    dm.DirectRelationReference("pygen-models", "unidirectional"),
                    "outwards",
                    dm.ViewId("pygen-models", "ConnectionItemB", "1"),
                ),
            ],
        )

    def query(self) -> ConnectionItemCNodeQuery:
        """Start a query for connection item as."""
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

                >>> from omni_pydantic_v1 import OmniClient
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
            return self._list(
                limit=limit,
                filter=filter_,
            )

        builder = QueryBuilder(ConnectionItemCNodeList)
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=filter_,
                ),
                ConnectionItemCNode,
                max_retrieve_limit=limit,
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

        return builder.execute(self._client)
