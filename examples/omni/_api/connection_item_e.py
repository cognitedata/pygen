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
from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from omni.data_classes._connection_item_e import (
    ConnectionItemEQuery,
    _CONNECTIONITEME_PROPERTIES_BY_FIELD,
    _create_connection_item_e_filter,
)
from omni.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ConnectionItemE,
    ConnectionItemEWrite,
    ConnectionItemEFields,
    ConnectionItemEList,
    ConnectionItemEWriteList,
    ConnectionItemETextFields,
    ConnectionEdgeA,
    ConnectionEdgeAWrite,
    ConnectionEdgeAList,
    ConnectionEdgeA,
    ConnectionItemD,
    ConnectionItemF,
)
from omni._api.connection_item_e_inwards_single import ConnectionItemEInwardsSingleAPI
from omni._api.connection_item_e_inwards_single_property import ConnectionItemEInwardsSinglePropertyAPI
from omni._api.connection_item_e_query import ConnectionItemEQueryAPI


class ConnectionItemEAPI(NodeAPI[ConnectionItemE, ConnectionItemEWrite, ConnectionItemEList, ConnectionItemEWriteList]):
    _view_id = dm.ViewId("sp_pygen_models", "ConnectionItemE", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _CONNECTIONITEME_PROPERTIES_BY_FIELD
    _class_type = ConnectionItemE
    _class_list = ConnectionItemEList
    _class_write_list = ConnectionItemEWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.inwards_single_edge = ConnectionItemEInwardsSingleAPI(client)
        self.inwards_single_property_edge = ConnectionItemEInwardsSinglePropertyAPI(client)

    def __call__(
        self,
        direct_list_no_source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        direct_no_source: (
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
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ConnectionItemEQueryAPI[ConnectionItemEList]:
        """Query starting at connection item es.

        Args:
            direct_list_no_source: The direct list no source to filter on.
            direct_no_source: The direct no source to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item es to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for connection item es.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_connection_item_e_filter(
            self._view_id,
            direct_list_no_source,
            direct_no_source,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(ConnectionItemEList)
        return ConnectionItemEQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        connection_item_e: ConnectionItemEWrite | Sequence[ConnectionItemEWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) connection item es.

        Args:
            connection_item_e: Connection item e or
                sequence of connection item es to upsert.
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

            Create a new connection_item_e:

                >>> from omni import OmniClient
                >>> from omni.data_classes import ConnectionItemEWrite
                >>> client = OmniClient()
                >>> connection_item_e = ConnectionItemEWrite(
                ...     external_id="my_connection_item_e", ...
                ... )
                >>> result = client.connection_item_e.apply(connection_item_e)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.connection_item_e.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(connection_item_e, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more connection item e.

        Args:
            external_id: External id of the connection item e to delete.
            space: The space where all the connection item e are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete connection_item_e by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.connection_item_e.delete("my_connection_item_e")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.connection_item_e.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ConnectionItemE | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ConnectionItemEList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> ConnectionItemE | ConnectionItemEList | None:
        """Retrieve one or more connection item es by id(s).

        Args:
            external_id: External id or list of external ids of the connection item es.
            space: The space where all the connection item es are located.

        Returns:
            The requested connection item es.

        Examples:

            Retrieve connection_item_e by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_e = client.connection_item_e.retrieve(
                ...     "my_connection_item_e"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.inwards_single_edge,
                    "inwards_single",
                    dm.DirectRelationReference("sp_pygen_models", "bidirectionalSingle"),
                    "inwards",
                    dm.ViewId("sp_pygen_models", "ConnectionItemD", "1"),
                ),
                (
                    self.inwards_single_property_edge,
                    "inwards_single_property",
                    dm.DirectRelationReference("sp_pygen_models", "multiProperty"),
                    "inwards",
                    dm.ViewId("sp_pygen_models", "ConnectionItemF", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: ConnectionItemETextFields | SequenceNotStr[ConnectionItemETextFields] | None = None,
        direct_list_no_source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        direct_no_source: (
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
        sort_by: ConnectionItemEFields | SequenceNotStr[ConnectionItemEFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ConnectionItemEList:
        """Search connection item es

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            direct_list_no_source: The direct list no source to filter on.
            direct_no_source: The direct no source to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item es to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results connection item es matching the query.

        Examples:

           Search for 'my_connection_item_e' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_es = client.connection_item_e.search(
                ...     'my_connection_item_e'
                ... )

        """
        filter_ = _create_connection_item_e_filter(
            self._view_id,
            direct_list_no_source,
            direct_no_source,
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
        property: ConnectionItemEFields | SequenceNotStr[ConnectionItemEFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemETextFields | SequenceNotStr[ConnectionItemETextFields] | None = None,
        direct_list_no_source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        direct_no_source: (
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
        property: ConnectionItemEFields | SequenceNotStr[ConnectionItemEFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemETextFields | SequenceNotStr[ConnectionItemETextFields] | None = None,
        direct_list_no_source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        direct_no_source: (
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
        group_by: ConnectionItemEFields | SequenceNotStr[ConnectionItemEFields],
        property: ConnectionItemEFields | SequenceNotStr[ConnectionItemEFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemETextFields | SequenceNotStr[ConnectionItemETextFields] | None = None,
        direct_list_no_source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        direct_no_source: (
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
        group_by: ConnectionItemEFields | SequenceNotStr[ConnectionItemEFields] | None = None,
        property: ConnectionItemEFields | SequenceNotStr[ConnectionItemEFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemETextFields | SequenceNotStr[ConnectionItemETextFields] | None = None,
        direct_list_no_source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        direct_no_source: (
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
        """Aggregate data across connection item es

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            direct_list_no_source: The direct list no source to filter on.
            direct_no_source: The direct no source to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item es to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count connection item es in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.connection_item_e.aggregate("count", space="my_space")

        """

        filter_ = _create_connection_item_e_filter(
            self._view_id,
            direct_list_no_source,
            direct_no_source,
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
        property: ConnectionItemEFields,
        interval: float,
        query: str | None = None,
        search_property: ConnectionItemETextFields | SequenceNotStr[ConnectionItemETextFields] | None = None,
        direct_list_no_source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        direct_no_source: (
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
        """Produces histograms for connection item es

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            direct_list_no_source: The direct list no source to filter on.
            direct_no_source: The direct no source to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item es to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_connection_item_e_filter(
            self._view_id,
            direct_list_no_source,
            direct_no_source,
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

    def query(self) -> ConnectionItemEQuery:
        """Start a query for connection item es."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return ConnectionItemEQuery(self._client)

    def select(self) -> ConnectionItemEQuery:
        """Start selecting from connection item es."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return ConnectionItemEQuery(self._client)

    def list(
        self,
        direct_list_no_source: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        direct_no_source: (
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
        sort_by: ConnectionItemEFields | Sequence[ConnectionItemEFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemEList:
        """List/filter connection item es

        Args:
            direct_list_no_source: The direct list no source to filter on.
            direct_no_source: The direct no source to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item es to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `direct_reverse_multi`, `direct_reverse_single`, `inwards_single`
            and `inwards_single_property` for the connection item es. Defaults to 'skip'.'skip' will not retrieve any
            connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve
            the full connected items.

        Returns:
            List of requested connection item es

        Examples:

            List connection item es and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_es = client.connection_item_e.list(limit=5)

        """
        filter_ = _create_connection_item_e_filter(
            self._view_id,
            direct_list_no_source,
            direct_no_source,
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
        factory = QueryStepFactory(builder.create_name,  view_id=self._view_id, edge_connection_property="endNode")
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
                ConnectionItemD._view_id,
                "inwards",
                ViewPropertyId(self._view_id, "inwardsSingle"),
                include_end_node=retrieve_connections == "full",
                has_container_fields=True,
            )
        )
        builder.extend(
            factory.from_edge(
                ConnectionItemF._view_id,
                "inwards",
                ViewPropertyId(self._view_id, "inwardsSingleProperty"),
                include_end_node=retrieve_connections == "full",
                has_container_fields=True,
                edge_view=ConnectionEdgeA._view_id,
            )
        )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_reverse_relation(
                    ConnectionItemD._view_id,
                    through=dm.PropertyId(dm.ViewId("sp_pygen_models", "ConnectionItemD", "1"), "directMulti"),
                    connection_type="reverse-list",
                    connection_property=ViewPropertyId(self._view_id, "directReverseMulti"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_reverse_relation(
                    ConnectionItemD._view_id,
                    through=dm.PropertyId(dm.ViewId("sp_pygen_models", "ConnectionItemD", "1"), "directSingle"),
                    connection_type=None,
                    connection_property=ViewPropertyId(self._view_id, "directReverseSingle"),
                    has_container_fields=True,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        unpacked = QueryUnpacker(builder, unpack_edges=False, as_data_record=True, edge_type_key="edge_type", node_type_key="node_type").unpack()
        return ConnectionItemEList([ConnectionItemE.model_validate(item) for item in unpacked])
