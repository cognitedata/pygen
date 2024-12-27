from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni_sub._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from omni_sub.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from omni_sub.data_classes._connection_item_b import (
    ConnectionItemBQuery,
    _CONNECTIONITEMB_PROPERTIES_BY_FIELD,
    _create_connection_item_b_filter,
)
from omni_sub.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ConnectionItemB,
    ConnectionItemBWrite,
    ConnectionItemBFields,
    ConnectionItemBList,
    ConnectionItemBWriteList,
    ConnectionItemBTextFields,
    ConnectionItemA,
)
from omni_sub._api.connection_item_b_inwards import ConnectionItemBInwardsAPI
from omni_sub._api.connection_item_b_self_edge import ConnectionItemBSelfEdgeAPI
from omni_sub._api.connection_item_b_query import ConnectionItemBQueryAPI


class ConnectionItemBAPI(NodeAPI[ConnectionItemB, ConnectionItemBWrite, ConnectionItemBList, ConnectionItemBWriteList]):
    _view_id = dm.ViewId("sp_pygen_models", "ConnectionItemB", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _CONNECTIONITEMB_PROPERTIES_BY_FIELD
    _class_type = ConnectionItemB
    _class_list = ConnectionItemBList
    _class_write_list = ConnectionItemBWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.inwards_edge = ConnectionItemBInwardsAPI(client)
        self.self_edge_edge = ConnectionItemBSelfEdgeAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ConnectionItemBQueryAPI[ConnectionItemBList]:
        """Query starting at connection item bs.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item bs to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for connection item bs.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_connection_item_b_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(ConnectionItemBList)
        return ConnectionItemBQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        connection_item_b: ConnectionItemBWrite | Sequence[ConnectionItemBWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) connection item bs.

        Args:
            connection_item_b: Connection item b or
                sequence of connection item bs to upsert.
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

            Create a new connection_item_b:

                >>> from omni_sub import OmniSubClient
                >>> from omni_sub.data_classes import ConnectionItemBWrite
                >>> client = OmniSubClient()
                >>> connection_item_b = ConnectionItemBWrite(
                ...     external_id="my_connection_item_b", ...
                ... )
                >>> result = client.connection_item_b.apply(connection_item_b)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.connection_item_b.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(connection_item_b, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str) -> dm.InstancesDeleteResult:
        """Delete one or more connection item b.

        Args:
            external_id: External id of the connection item b to delete.
            space: The space where all the connection item b are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete connection_item_b by id:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> client.connection_item_b.delete("my_connection_item_b")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.connection_item_b.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str], space: str) -> ConnectionItemB | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str
    ) -> ConnectionItemBList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
    ) -> ConnectionItemB | ConnectionItemBList | None:
        """Retrieve one or more connection item bs by id(s).

        Args:
            external_id: External id or list of external ids of the connection item bs.
            space: The space where all the connection item bs are located.

        Returns:
            The requested connection item bs.

        Examples:

            Retrieve connection_item_b by id:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> connection_item_b = client.connection_item_b.retrieve(
                ...     "my_connection_item_b"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.inwards_edge,
                    "inwards",
                    dm.DirectRelationReference("sp_pygen_models", "bidirectional"),
                    "inwards",
                    dm.ViewId("sp_pygen_models", "ConnectionItemA", "1"),
                ),
                (
                    self.self_edge_edge,
                    "self_edge",
                    dm.DirectRelationReference("sp_pygen_models", "reflexive"),
                    "outwards",
                    dm.ViewId("sp_pygen_models", "ConnectionItemB", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: ConnectionItemBTextFields | SequenceNotStr[ConnectionItemBTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemBFields | SequenceNotStr[ConnectionItemBFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ConnectionItemBList:
        """Search connection item bs

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item bs to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results connection item bs matching the query.

        Examples:

           Search for 'my_connection_item_b' in all text properties:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> connection_item_bs = client.connection_item_b.search(
                ...     'my_connection_item_b'
                ... )

        """
        filter_ = _create_connection_item_b_filter(
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
        property: ConnectionItemBFields | SequenceNotStr[ConnectionItemBFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemBTextFields | SequenceNotStr[ConnectionItemBTextFields] | None = None,
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
        property: ConnectionItemBFields | SequenceNotStr[ConnectionItemBFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemBTextFields | SequenceNotStr[ConnectionItemBTextFields] | None = None,
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
        group_by: ConnectionItemBFields | SequenceNotStr[ConnectionItemBFields],
        property: ConnectionItemBFields | SequenceNotStr[ConnectionItemBFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemBTextFields | SequenceNotStr[ConnectionItemBTextFields] | None = None,
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
        group_by: ConnectionItemBFields | SequenceNotStr[ConnectionItemBFields] | None = None,
        property: ConnectionItemBFields | SequenceNotStr[ConnectionItemBFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemBTextFields | SequenceNotStr[ConnectionItemBTextFields] | None = None,
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
        """Aggregate data across connection item bs

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
            limit: Maximum number of connection item bs to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count connection item bs in space `my_space`:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> result = client.connection_item_b.aggregate("count", space="my_space")

        """

        filter_ = _create_connection_item_b_filter(
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
        property: ConnectionItemBFields,
        interval: float,
        query: str | None = None,
        search_property: ConnectionItemBTextFields | SequenceNotStr[ConnectionItemBTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for connection item bs

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item bs to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_connection_item_b_filter(
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

    def query(self) -> ConnectionItemBQuery:
        """Start a query for connection item bs."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return ConnectionItemBQuery(self._client)

    def select(self) -> ConnectionItemBQuery:
        """Start selecting from connection item bs."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return ConnectionItemBQuery(self._client)

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemBFields | Sequence[ConnectionItemBFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemBList:
        """List/filter connection item bs

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item bs to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `inwards` and `self_edge` for the connection item bs. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested connection item bs

        Examples:

            List connection item bs and limit to 5:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> connection_item_bs = client.connection_item_b.list(limit=5)

        """
        filter_ = _create_connection_item_b_filter(
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

        builder = QueryBuilder()
        factory = QueryStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                limit=limit,
                has_container_fields=True,
            )
        )
        builder.extend(
            factory.from_edge(
                ConnectionItemA._view_id,
                "inwards",
                ViewPropertyId(self._view_id, "inwards"),
                include_end_node=retrieve_connections == "full",
                has_container_fields=True,
            )
        )
        builder.extend(
            factory.from_edge(
                ConnectionItemB._view_id,
                "outwards",
                ViewPropertyId(self._view_id, "selfEdge"),
                include_end_node=retrieve_connections == "full",
                has_container_fields=True,
            )
        )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        unpacked = QueryUnpacker(
            builder, unpack_edges=False, as_data_record=True, edge_type_key="edge_type", node_type_key="node_type"
        ).unpack()
        return ConnectionItemBList([ConnectionItemB.model_validate(item) for item in unpacked])
