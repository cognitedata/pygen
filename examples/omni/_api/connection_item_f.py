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
from omni.data_classes._connection_item_f import (
    ConnectionItemFQuery,
    _CONNECTIONITEMF_PROPERTIES_BY_FIELD,
    _create_connection_item_f_filter,
)
from omni.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ConnectionItemF,
    ConnectionItemFWrite,
    ConnectionItemFFields,
    ConnectionItemFList,
    ConnectionItemFWriteList,
    ConnectionItemFTextFields,
    ConnectionEdgeA,
    ConnectionEdgeAWrite,
    ConnectionEdgeAList,
    ConnectionEdgeA,
    ConnectionItemD,
    ConnectionItemE,
    ConnectionItemG,
)
from omni._api.connection_item_f_outwards_multi import ConnectionItemFOutwardsMultiAPI
from omni._api.connection_item_f_outwards_single import ConnectionItemFOutwardsSingleAPI


class ConnectionItemFAPI(NodeAPI[ConnectionItemF, ConnectionItemFWrite, ConnectionItemFList, ConnectionItemFWriteList]):
    _view_id = dm.ViewId("sp_pygen_models", "ConnectionItemF", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _CONNECTIONITEMF_PROPERTIES_BY_FIELD
    _class_type = ConnectionItemF
    _class_list = ConnectionItemFList
    _class_write_list = ConnectionItemFWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.outwards_multi_edge = ConnectionItemFOutwardsMultiAPI(client)
        self.outwards_single_edge = ConnectionItemFOutwardsSingleAPI(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemF | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemFList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemF | ConnectionItemFList | None:
        """Retrieve one or more connection item fs by id(s).

        Args:
            external_id: External id or list of external ids of the connection item fs.
            space: The space where all the connection item fs are located.
            retrieve_connections: Whether to retrieve `direct_list`, `outwards_multi` and `outwards_single` for the
            connection item fs. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only
            retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested connection item fs.

        Examples:

            Retrieve connection_item_f by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_f = client.connection_item_f.retrieve(
                ...     "my_connection_item_f"
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
        properties: ConnectionItemFTextFields | SequenceNotStr[ConnectionItemFTextFields] | None = None,
        direct_list: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemFFields | SequenceNotStr[ConnectionItemFFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ConnectionItemFList:
        """Search connection item fs

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            direct_list: The direct list to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item fs to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results connection item fs matching the query.

        Examples:

           Search for 'my_connection_item_f' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_fs = client.connection_item_f.search(
                ...     'my_connection_item_f'
                ... )

        """
        filter_ = _create_connection_item_f_filter(
            self._view_id,
            direct_list,
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
        property: ConnectionItemFFields | SequenceNotStr[ConnectionItemFFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemFTextFields | SequenceNotStr[ConnectionItemFTextFields] | None = None,
        direct_list: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        property: ConnectionItemFFields | SequenceNotStr[ConnectionItemFFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemFTextFields | SequenceNotStr[ConnectionItemFTextFields] | None = None,
        direct_list: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        group_by: ConnectionItemFFields | SequenceNotStr[ConnectionItemFFields],
        property: ConnectionItemFFields | SequenceNotStr[ConnectionItemFFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemFTextFields | SequenceNotStr[ConnectionItemFTextFields] | None = None,
        direct_list: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        group_by: ConnectionItemFFields | SequenceNotStr[ConnectionItemFFields] | None = None,
        property: ConnectionItemFFields | SequenceNotStr[ConnectionItemFFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemFTextFields | SequenceNotStr[ConnectionItemFTextFields] | None = None,
        direct_list: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        """Aggregate data across connection item fs

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            direct_list: The direct list to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item fs to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count connection item fs in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.connection_item_f.aggregate("count", space="my_space")

        """

        filter_ = _create_connection_item_f_filter(
            self._view_id,
            direct_list,
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
        property: ConnectionItemFFields,
        interval: float,
        query: str | None = None,
        search_property: ConnectionItemFTextFields | SequenceNotStr[ConnectionItemFTextFields] | None = None,
        direct_list: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for connection item fs

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            direct_list: The direct list to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item fs to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_connection_item_f_filter(
            self._view_id,
            direct_list,
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

    def select(self) -> ConnectionItemFQuery:
        """Start selecting from connection item fs."""
        return ConnectionItemFQuery(self._client)

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
                    ConnectionItemG._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "outwardsMulti"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                    edge_view=ConnectionEdgeA._view_id,
                )
            )
            builder.extend(
                factory.from_edge(
                    ConnectionItemE._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "outwardsSingle"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                    edge_view=ConnectionEdgeA._view_id,
                )
            )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    ConnectionItemD._view_id,
                    ViewPropertyId(self._view_id, "directList"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        direct_list: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[ConnectionItemFList]:
        """Iterate over connection item fs

        Args:
            chunk_size: The number of connection item fs to return in each iteration. Defaults to 100.
            direct_list: The direct list to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `direct_list`, `outwards_multi` and `outwards_single` for the
            connection item fs. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only
            retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of connection item fs to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of connection item fs

        Examples:

            Iterate connection item fs in chunks of 100 up to 2000 items:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for connection_item_fs in client.connection_item_f.iterate(chunk_size=100, limit=2000):
                ...     for connection_item_f in connection_item_fs:
                ...         print(connection_item_f.external_id)

            Iterate connection item fs in chunks of 100 sorted by external_id in descending order:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for connection_item_fs in client.connection_item_f.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for connection_item_f in connection_item_fs:
                ...         print(connection_item_f.external_id)

            Iterate connection item fs in chunks of 100 and use cursors to resume the iteration:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for first_iteration in client.connection_item_f.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for connection_item_fs in client.connection_item_f.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for connection_item_f in connection_item_fs:
                ...         print(connection_item_f.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_connection_item_f_filter(
            self._view_id,
            direct_list,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        direct_list: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemFFields | Sequence[ConnectionItemFFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemFList:
        """List/filter connection item fs

        Args:
            direct_list: The direct list to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item fs to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `direct_list`, `outwards_multi` and `outwards_single` for the
            connection item fs. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only
            retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested connection item fs

        Examples:

            List connection item fs and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_fs = client.connection_item_f.list(limit=5)

        """
        filter_ = _create_connection_item_f_filter(
            self._view_id,
            direct_list,
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
