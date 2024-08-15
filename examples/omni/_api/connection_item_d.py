from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    QueryBuilder,
)
from omni.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ConnectionItemD,
    ConnectionItemDWrite,
    ConnectionItemDFields,
    ConnectionItemDList,
    ConnectionItemDWriteList,
    ConnectionItemDTextFields,
    ConnectionItemE,
)
from omni.data_classes._connection_item_d import (
    ConnectionItemDQuery,
    _CONNECTIONITEMD_PROPERTIES_BY_FIELD,
    _create_connection_item_d_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from .connection_item_d_outwards_single import ConnectionItemDOutwardsSingleAPI
from .connection_item_d_query import ConnectionItemDQueryAPI


class ConnectionItemDAPI(NodeAPI[ConnectionItemD, ConnectionItemDWrite, ConnectionItemDList, ConnectionItemDWriteList]):
    _view_id = dm.ViewId("pygen-models", "ConnectionItemD", "1")
    _properties_by_field = _CONNECTIONITEMD_PROPERTIES_BY_FIELD
    _class_type = ConnectionItemD
    _class_list = ConnectionItemDList
    _class_write_list = ConnectionItemDWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.outwards_single_edge = ConnectionItemDOutwardsSingleAPI(client)

    def __call__(
        self,
        direct_multi: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        direct_single: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ConnectionItemDQueryAPI[ConnectionItemDList]:
        """Query starting at connection item ds.

        Args:
            direct_multi: The direct multi to filter on.
            direct_single: The direct single to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item ds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for connection item ds.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_connection_item_d_filter(
            self._view_id,
            direct_multi,
            direct_single,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ConnectionItemDList)
        return ConnectionItemDQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        connection_item_d: ConnectionItemDWrite | Sequence[ConnectionItemDWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) connection item ds.

        Args:
            connection_item_d: Connection item d or sequence of connection item ds to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new connection_item_d:

                >>> from omni import OmniClient
                >>> from omni.data_classes import ConnectionItemDWrite
                >>> client = OmniClient()
                >>> connection_item_d = ConnectionItemDWrite(external_id="my_connection_item_d", ...)
                >>> result = client.connection_item_d.apply(connection_item_d)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.connection_item_d.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(connection_item_d, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more connection item d.

        Args:
            external_id: External id of the connection item d to delete.
            space: The space where all the connection item d are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete connection_item_d by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.connection_item_d.delete("my_connection_item_d")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.connection_item_d.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ConnectionItemD | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ConnectionItemDList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ConnectionItemD | ConnectionItemDList | None:
        """Retrieve one or more connection item ds by id(s).

        Args:
            external_id: External id or list of external ids of the connection item ds.
            space: The space where all the connection item ds are located.

        Returns:
            The requested connection item ds.

        Examples:

            Retrieve connection_item_d by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_d = client.connection_item_d.retrieve("my_connection_item_d")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: ConnectionItemDTextFields | SequenceNotStr[ConnectionItemDTextFields] | None = None,
        direct_multi: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        direct_single: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemDFields | SequenceNotStr[ConnectionItemDFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ConnectionItemDList:
        """Search connection item ds

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            direct_multi: The direct multi to filter on.
            direct_single: The direct single to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item ds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results connection item ds matching the query.

        Examples:

           Search for 'my_connection_item_d' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_ds = client.connection_item_d.search('my_connection_item_d')

        """
        filter_ = _create_connection_item_d_filter(
            self._view_id,
            direct_multi,
            direct_single,
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
        property: ConnectionItemDFields | SequenceNotStr[ConnectionItemDFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemDTextFields | SequenceNotStr[ConnectionItemDTextFields] | None = None,
        direct_multi: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        direct_single: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: ConnectionItemDFields | SequenceNotStr[ConnectionItemDFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemDTextFields | SequenceNotStr[ConnectionItemDTextFields] | None = None,
        direct_multi: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        direct_single: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        group_by: ConnectionItemDFields | SequenceNotStr[ConnectionItemDFields],
        property: ConnectionItemDFields | SequenceNotStr[ConnectionItemDFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemDTextFields | SequenceNotStr[ConnectionItemDTextFields] | None = None,
        direct_multi: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        direct_single: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        group_by: ConnectionItemDFields | SequenceNotStr[ConnectionItemDFields] | None = None,
        property: ConnectionItemDFields | SequenceNotStr[ConnectionItemDFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemDTextFields | SequenceNotStr[ConnectionItemDTextFields] | None = None,
        direct_multi: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        direct_single: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        """Aggregate data across connection item ds

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            direct_multi: The direct multi to filter on.
            direct_single: The direct single to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item ds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count connection item ds in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.connection_item_d.aggregate("count", space="my_space")

        """

        filter_ = _create_connection_item_d_filter(
            self._view_id,
            direct_multi,
            direct_single,
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
        property: ConnectionItemDFields,
        interval: float,
        query: str | None = None,
        search_property: ConnectionItemDTextFields | SequenceNotStr[ConnectionItemDTextFields] | None = None,
        direct_multi: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        direct_single: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for connection item ds

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            direct_multi: The direct multi to filter on.
            direct_single: The direct single to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item ds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_connection_item_d_filter(
            self._view_id,
            direct_multi,
            direct_single,
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

    def query(self) -> ConnectionItemDQuery:
        """Start a query for connection item as."""
        warnings.warn("The .query is in alpha and is subject to breaking changes without notice.")
        return ConnectionItemDQuery(self._client)

    def list(
        self,
        direct_multi: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        direct_single: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemDFields | Sequence[ConnectionItemDFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ConnectionItemDList:
        """List/filter connection item ds

        Args:
            direct_multi: The direct multi to filter on.
            direct_single: The direct single to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item ds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `direct_multi`, `direct_single` and `outwards_single` for the connection item ds. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested connection item ds

        Examples:

            List connection item ds and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_ds = client.connection_item_d.list(limit=5)

        """
        filter_ = _create_connection_item_d_filter(
            self._view_id,
            direct_multi,
            direct_single,
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

        builder = QueryBuilder(ConnectionItemDList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                ConnectionItemD,
                max_retrieve_limit=limit,
            )
        )
        from_root = builder.get_from()
        edge_outwards_single = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_outwards_single,
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
                    builder.create_name(edge_outwards_single),
                    dm.query.NodeResultSetExpression(
                        from_=edge_outwards_single,
                        filter=dm.filters.HasData(views=[ConnectionItemE._view_id]),
                    ),
                    ConnectionItemE,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[ConnectionItemE._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("directMulti"),
                    ),
                    ConnectionItemE,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[ConnectionItemE._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("directSingle"),
                    ),
                    ConnectionItemE,
                )
            )

        return builder.execute(self._client)
