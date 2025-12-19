from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from omni.data_classes._connection_item_b import (
    ConnectionItemBQuery,
    _CONNECTIONITEMB_PROPERTIES_BY_FIELD,
    _create_connection_item_b_filter,
)
from omni.data_classes import (
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
from omni._api.connection_item_b_inwards import ConnectionItemBInwardsAPI
from omni._api.connection_item_b_self_edge import ConnectionItemBSelfEdgeAPI


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

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemB | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemBList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemB | ConnectionItemBList | None:
        """Retrieve one or more connection item bs by id(s).

        Args:
            external_id: External id or list of external ids of the connection item bs.
            space: The space where all the connection item bs are located.
            retrieve_connections: Whether to retrieve `inwards` and `self_edge` for the connection item bs. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested connection item bs.

        Examples:

            Retrieve connection_item_b by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_b = client.connection_item_b.retrieve(
                ...     "my_connection_item_b"
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

                >>> from omni import OmniClient
                >>> client = OmniClient()
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

                >>> from omni import OmniClient
                >>> client = OmniClient()
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

    def select(self) -> ConnectionItemBQuery:
        """Start selecting from connection item bs."""
        return ConnectionItemBQuery(self._client)

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
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[ConnectionItemBList]:
        """Iterate over connection item bs

        Args:
            chunk_size: The number of connection item bs to return in each iteration. Defaults to 100.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `inwards` and `self_edge` for the connection item bs. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of connection item bs to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of connection item bs

        Examples:

            Iterate connection item bs in chunks of 100 up to 2000 items:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for connection_item_bs in client.connection_item_b.iterate(chunk_size=100, limit=2000):
                ...     for connection_item_b in connection_item_bs:
                ...         print(connection_item_b.external_id)

            Iterate connection item bs in chunks of 100 sorted by external_id in descending order:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for connection_item_bs in client.connection_item_b.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for connection_item_b in connection_item_bs:
                ...         print(connection_item_b.external_id)

            Iterate connection item bs in chunks of 100 and use cursors to resume the iteration:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for first_iteration in client.connection_item_b.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for connection_item_bs in client.connection_item_b.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for connection_item_b in connection_item_bs:
                ...         print(connection_item_b.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_connection_item_b_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

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

                >>> from omni import OmniClient
                >>> client = OmniClient()
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
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
