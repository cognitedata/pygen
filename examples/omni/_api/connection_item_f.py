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
from omni._api.connection_item_f_query import ConnectionItemFQueryAPI


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

    def __call__(
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
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ConnectionItemFQueryAPI[ConnectionItemF, ConnectionItemFList]:
        """Query starting at connection item fs.

        Args:
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
            A query API for connection item fs.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_connection_item_f_filter(
            self._view_id,
            direct_list,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return ConnectionItemFQueryAPI(
            self._client, QueryBuilder(), self._class_type, self._class_list, None, filter_, limit
        )

    def apply(
        self,
        connection_item_f: ConnectionItemFWrite | Sequence[ConnectionItemFWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) connection item fs.

        Args:
            connection_item_f: Connection item f or
                sequence of connection item fs to upsert.
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

            Create a new connection_item_f:

                >>> from omni import OmniClient
                >>> from omni.data_classes import ConnectionItemFWrite
                >>> client = OmniClient()
                >>> connection_item_f = ConnectionItemFWrite(
                ...     external_id="my_connection_item_f", ...
                ... )
                >>> result = client.connection_item_f.apply(connection_item_f)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.connection_item_f.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(connection_item_f, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more connection item f.

        Args:
            external_id: External id of the connection item f to delete.
            space: The space where all the connection item f are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete connection_item_f by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.connection_item_f.delete("my_connection_item_f")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.connection_item_f.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

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
                sort=sort,
                limit=limit,
                has_container_fields=True,
            )
        )
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
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        return QueryUnpacker(builder, edges=unpack_edges).unpack()

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
        values = self._query(filter_, limit, retrieve_connections, sort_input)
        return self._class_list(instantiate_classes(self._class_type, values, "list"))
