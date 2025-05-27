from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni_sub._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from omni_sub.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
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


class ConnectionItemAAPI(NodeAPI[ConnectionItemA, ConnectionItemAWrite, ConnectionItemAList, ConnectionItemAWriteList]):
    _view_id = dm.ViewId("sp_pygen_models", "ConnectionItemA", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _CONNECTIONITEMA_PROPERTIES_BY_FIELD
    _class_type = ConnectionItemA
    _class_list = ConnectionItemAList
    _class_write_list = ConnectionItemAWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.outwards_edge = ConnectionItemAOutwardsAPI(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemA | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemAList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemA | ConnectionItemAList | None:
        """Retrieve one or more connection item as by id(s).

        Args:
            external_id: External id or list of external ids of the connection item as.
            space: The space where all the connection item as are located.
            retrieve_connections: Whether to retrieve `other_direct`, `outwards` and `self_direct` for the connection
            item as. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the
            identifier of the connected items, and 'full' will retrieve the full connected items.

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
            retrieve_connections=retrieve_connections,
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
        properties_: str | list[str] | None = None,
        properties_prefix: str | None = None,
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
            properties_: The property to filter on.
            properties_prefix: The prefix of the property to filter on.
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
            properties_,
            properties_prefix,
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
        properties_: str | list[str] | None = None,
        properties_prefix: str | None = None,
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
        properties_: str | list[str] | None = None,
        properties_prefix: str | None = None,
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
        properties_: str | list[str] | None = None,
        properties_prefix: str | None = None,
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
        properties_: str | list[str] | None = None,
        properties_prefix: str | None = None,
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
            properties_: The property to filter on.
            properties_prefix: The prefix of the property to filter on.
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
            properties_,
            properties_prefix,
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
        properties_: str | list[str] | None = None,
        properties_prefix: str | None = None,
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
            properties_: The property to filter on.
            properties_prefix: The prefix of the property to filter on.
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
            properties_,
            properties_prefix,
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

    def select(self) -> ConnectionItemAQuery:
        """Start selecting from connection item as."""
        return ConnectionItemAQuery(self._client)

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
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
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
        properties_: str | list[str] | None = None,
        properties_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[ConnectionItemAList]:
        """Iterate over connection item as

        Args:
            chunk_size: The number of connection item as to return in each iteration. Defaults to 100.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            other_direct: The other direct to filter on.
            self_direct: The self direct to filter on.
            properties_: The property to filter on.
            properties_prefix: The prefix of the property to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `other_direct`, `outwards` and `self_direct` for the connection
            item as. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the
            identifier of the connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of connection item as to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of connection item as

        Examples:

            Iterate connection item as in chunks of 100 up to 2000 items:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> for connection_item_as in client.connection_item_a.iterate(chunk_size=100, limit=2000):
                ...     for connection_item_a in connection_item_as:
                ...         print(connection_item_a.external_id)

            Iterate connection item as in chunks of 100 sorted by external_id in descending order:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> for connection_item_as in client.connection_item_a.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for connection_item_a in connection_item_as:
                ...         print(connection_item_a.external_id)

            Iterate connection item as in chunks of 100 and use cursors to resume the iteration:

                >>> from omni_sub import OmniSubClient
                >>> client = OmniSubClient()
                >>> for first_iteration in client.connection_item_a.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for connection_item_as in client.connection_item_a.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for connection_item_a in connection_item_as:
                ...         print(connection_item_a.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_connection_item_a_filter(
            self._view_id,
            name,
            name_prefix,
            other_direct,
            self_direct,
            properties_,
            properties_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

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
        properties_: str | list[str] | None = None,
        properties_prefix: str | None = None,
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
            properties_: The property to filter on.
            properties_prefix: The prefix of the property to filter on.
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
            properties_,
            properties_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
