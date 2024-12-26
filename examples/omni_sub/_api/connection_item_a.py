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
from omni_sub.data_classes._connection_item_a import (
    ConnectionItemAQuery,
    _CONNECTIONITEMA_PROPERTIES_BY_FIELD,
    _create_connection_item_a_filter,
)
from omni_sub.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ConnectionItemA,
    ConnectionItemAWrite,
    ConnectionItemAFields,
    ConnectionItemAList,
    ConnectionItemAWriteList,
    ConnectionItemATextFields,
    ConnectionItemB,
    ConnectionItemCNode,
)
from omni_sub._api.connection_item_a_outwards import ConnectionItemAOutwardsAPI
from omni_sub._api.connection_item_a_query import ConnectionItemAQueryAPI


class ConnectionItemAAPI(NodeAPI[ConnectionItemA, ConnectionItemAWrite, ConnectionItemAList, ConnectionItemAWriteList]):
    _view_id = dm.ViewId("sp_pygen_models", "ConnectionItemA", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _CONNECTIONITEMA_PROPERTIES_BY_FIELD
    _class_type = ConnectionItemA
    _class_list = ConnectionItemAList
    _class_write_list = ConnectionItemAWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.outwards_edge = ConnectionItemAOutwardsAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        other_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        self_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ConnectionItemAQueryAPI[ConnectionItemAList]:
        """Query starting at connection item as.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            other_direct: The other direct to filter on.
            self_direct: The self direct to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item as to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for connection item as.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_connection_item_a_filter(
            self._view_id,
            name,
            name_prefix,
            other_direct,
            self_direct,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(ConnectionItemAList)
        return ConnectionItemAQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        connection_item_a: ConnectionItemAWrite | Sequence[ConnectionItemAWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) connection item as.

        Args:
            connection_item_a: Connection item a or
                sequence of connection item as to upsert.
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

            Create a new connection_item_a:

                >>> from omni_sub import OmniSubClient
                >>> from omni_sub.data_classes import ConnectionItemAWrite
                >>> client = OmniSubClient()
                >>> connection_item_a = ConnectionItemAWrite(
                ...     external_id="my_connection_item_a", ...
                ... )
                >>> result = client.connection_item_a.apply(connection_item_a)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.connection_item_a.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(connection_item_a, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str) -> dm.InstancesDeleteResult:
        """Delete one or more connection item a.

        Args:
            external_id: External id of the connection item a to delete.
            space: The space where all the connection item a are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete connection_item_a by id:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> client.connection_item_a.delete("my_connection_item_a")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.connection_item_a.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str], space: str) -> ConnectionItemA | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str
    ) -> ConnectionItemAList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
    ) -> ConnectionItemA | ConnectionItemAList | None:
        """Retrieve one or more connection item as by id(s).

        Args:
            external_id: External id or list of external ids of the connection item as.
            space: The space where all the connection item as are located.

        Returns:
            The requested connection item as.

        Examples:

            Retrieve connection_item_a by id:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> connection_item_a = client.connection_item_a.retrieve(
                ...     "my_connection_item_a"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.outwards_edge,
                    "outwards",
                    dm.DirectRelationReference("sp_pygen_models", "bidirectional"),
                    "outwards",
                    dm.ViewId("sp_pygen_models", "ConnectionItemB", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: ConnectionItemATextFields | SequenceNotStr[ConnectionItemATextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        other_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        self_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemAFields | SequenceNotStr[ConnectionItemAFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ConnectionItemAList:
        """Search connection item as

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            other_direct: The other direct to filter on.
            self_direct: The self direct to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item as to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results connection item as matching the query.

        Examples:

           Search for 'my_connection_item_a' in all text properties:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> connection_item_as = client.connection_item_a.search(
                ...     'my_connection_item_a'
                ... )

        """
        filter_ = _create_connection_item_a_filter(
            self._view_id,
            name,
            name_prefix,
            other_direct,
            self_direct,
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
        property: ConnectionItemAFields | SequenceNotStr[ConnectionItemAFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemATextFields | SequenceNotStr[ConnectionItemATextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        other_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        self_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
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
        property: ConnectionItemAFields | SequenceNotStr[ConnectionItemAFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemATextFields | SequenceNotStr[ConnectionItemATextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        other_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        self_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
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
        group_by: ConnectionItemAFields | SequenceNotStr[ConnectionItemAFields],
        property: ConnectionItemAFields | SequenceNotStr[ConnectionItemAFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemATextFields | SequenceNotStr[ConnectionItemATextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        other_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        self_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
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
        group_by: ConnectionItemAFields | SequenceNotStr[ConnectionItemAFields] | None = None,
        property: ConnectionItemAFields | SequenceNotStr[ConnectionItemAFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemATextFields | SequenceNotStr[ConnectionItemATextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        other_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        self_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
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
        """Aggregate data across connection item as

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            other_direct: The other direct to filter on.
            self_direct: The self direct to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item as to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count connection item as in space `my_space`:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> result = client.connection_item_a.aggregate("count", space="my_space")

        """

        filter_ = _create_connection_item_a_filter(
            self._view_id,
            name,
            name_prefix,
            other_direct,
            self_direct,
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
        property: ConnectionItemAFields,
        interval: float,
        query: str | None = None,
        search_property: ConnectionItemATextFields | SequenceNotStr[ConnectionItemATextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        other_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        self_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for connection item as

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            other_direct: The other direct to filter on.
            self_direct: The self direct to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item as to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_connection_item_a_filter(
            self._view_id,
            name,
            name_prefix,
            other_direct,
            self_direct,
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

    def query(self) -> ConnectionItemAQuery:
        """Start a query for connection item as."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return ConnectionItemAQuery(self._client)

    def select(self) -> ConnectionItemAQuery:
        """Start selecting from connection item as."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return ConnectionItemAQuery(self._client)

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        other_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        self_direct: (
            tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemAFields | Sequence[ConnectionItemAFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemAList:
        """List/filter connection item as

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            other_direct: The other direct to filter on.
            self_direct: The self direct to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item as to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `other_direct`, `outwards` and `self_direct` for the connection
            item as. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the
            identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested connection item as

        Examples:

            List connection item as and limit to 5:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> connection_item_as = client.connection_item_a.list(limit=5)

        """
        filter_ = _create_connection_item_a_filter(
            self._view_id,
            name,
            name_prefix,
            other_direct,
            self_direct,
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
        factory = QueryStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="endNode")
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
                ConnectionItemB._view_id,
                "outwards",
                ViewPropertyId(self._view_id, "outwards"),
                include_end_node=retrieve_connections == "full",
                has_container_fields=True,
            )
        )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    ConnectionItemCNode._view_id,
                    ViewPropertyId(self._view_id, "otherDirect"),
                    has_container_fields=False,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    ConnectionItemA._view_id,
                    ViewPropertyId(self._view_id, "selfDirect"),
                    has_container_fields=True,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        unpacked = QueryUnpacker(builder, unpack_edges=False, as_data_record=True).unpack()
        return ConnectionItemAList([ConnectionItemA.model_validate(item) for item in unpacked])
