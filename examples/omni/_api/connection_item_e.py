from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ConnectionItemE,
    ConnectionItemEWrite,
    ConnectionItemEFields,
    ConnectionItemEList,
    ConnectionItemEWriteList,
    ConnectionItemETextFields,
)
from omni.data_classes._connection_item_e import (
    _CONNECTIONITEME_PROPERTIES_BY_FIELD,
    _create_connection_item_e_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .connection_item_e_inwards_single import ConnectionItemEInwardsSingleAPI
from .connection_item_e_query import ConnectionItemEQueryAPI


class ConnectionItemEAPI(NodeAPI[ConnectionItemE, ConnectionItemEWrite, ConnectionItemEList]):
    _view_id = dm.ViewId("pygen-models", "ConnectionItemE", "1")

    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            class_type=ConnectionItemE,
            class_list=ConnectionItemEList,
            class_write_list=ConnectionItemEWriteList,
        )
        self.inwards_single_edge = ConnectionItemEInwardsSingleAPI(client)

    def __call__(
        self,
        direct_no_source: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ConnectionItemEQueryAPI[ConnectionItemEList]:
        """Query starting at connection item es.

        Args:
            direct_no_source: The direct no source to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item es to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for connection item es.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_connection_item_e_filter(
            self._view_id,
            direct_no_source,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ConnectionItemEList)
        return ConnectionItemEQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        connection_item_e: ConnectionItemEWrite | Sequence[ConnectionItemEWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) connection item es.

        Note: This method iterates through all nodes and timeseries linked to connection_item_e and creates them including the edges
        between the nodes. For example, if any of `inwards_single` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            connection_item_e: Connection item e or sequence of connection item es to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new connection_item_e:

                >>> from omni import OmniClient
                >>> from omni.data_classes import ConnectionItemEWrite
                >>> client = OmniClient()
                >>> connection_item_e = ConnectionItemEWrite(external_id="my_connection_item_e", ...)
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
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ConnectionItemE | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ConnectionItemEList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
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
                >>> connection_item_e = client.connection_item_e.retrieve("my_connection_item_e")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.inwards_single_edge,
                    "inwards_single",
                    dm.DirectRelationReference("pygen-models", "bidirectionalSingle"),
                    "inwards",
                    dm.ViewId("pygen-models", "ConnectionItemD", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: ConnectionItemETextFields | Sequence[ConnectionItemETextFields] | None = None,
        direct_no_source: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemEFields | Sequence[ConnectionItemEFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ConnectionItemEList:
        """Search connection item es

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            direct_no_source: The direct no source to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item es to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results connection item es matching the query.

        Examples:

           Search for 'my_connection_item_e' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_es = client.connection_item_e.search('my_connection_item_e')

        """
        filter_ = _create_connection_item_e_filter(
            self._view_id,
            direct_no_source,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            view_id=self._view_id,
            query=query,
            properties_by_field=_CONNECTIONITEME_PROPERTIES_BY_FIELD,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ConnectionItemEFields | Sequence[ConnectionItemEFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ConnectionItemETextFields | Sequence[ConnectionItemETextFields] | None = None,
        direct_no_source: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ConnectionItemEFields | Sequence[ConnectionItemEFields] | None = None,
        group_by: ConnectionItemEFields | Sequence[ConnectionItemEFields] = None,
        query: str | None = None,
        search_properties: ConnectionItemETextFields | Sequence[ConnectionItemETextFields] | None = None,
        direct_no_source: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ConnectionItemEFields | Sequence[ConnectionItemEFields] | None = None,
        group_by: ConnectionItemEFields | Sequence[ConnectionItemEFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemETextFields | Sequence[ConnectionItemETextFields] | None = None,
        direct_no_source: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across connection item es

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            direct_no_source: The direct no source to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item es to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            direct_no_source,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _CONNECTIONITEME_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ConnectionItemEFields,
        interval: float,
        query: str | None = None,
        search_property: ConnectionItemETextFields | Sequence[ConnectionItemETextFields] | None = None,
        direct_no_source: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for connection item es

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            direct_no_source: The direct no source to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item es to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_connection_item_e_filter(
            self._view_id,
            direct_no_source,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _CONNECTIONITEME_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        direct_no_source: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemEFields | Sequence[ConnectionItemEFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_edges: bool = True,
    ) -> ConnectionItemEList:
        """List/filter connection item es

        Args:
            direct_no_source: The direct no source to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item es to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_edges: Whether to retrieve `inwards_single` external ids for the connection item es. Defaults to True.

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
            direct_no_source,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            properties_by_field=_CONNECTIONITEME_PROPERTIES_BY_FIELD,
            sort_by=sort_by,
            direction=direction,
            sort=sort,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.inwards_single_edge,
                    "inwards_single",
                    dm.DirectRelationReference("pygen-models", "bidirectionalSingle"),
                    "inwards",
                    dm.ViewId("pygen-models", "ConnectionItemD", "1"),
                ),
            ],
        )
