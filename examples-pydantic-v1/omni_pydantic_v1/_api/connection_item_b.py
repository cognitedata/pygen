from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from omni_pydantic_v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni_pydantic_v1.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    ConnectionItemB,
    ConnectionItemBApply,
    ConnectionItemBFields,
    ConnectionItemBList,
    ConnectionItemBApplyList,
    ConnectionItemBTextFields,
)
from omni_pydantic_v1.data_classes._connection_item_b import (
    _CONNECTIONITEMB_PROPERTIES_BY_FIELD,
    _create_connection_item_b_filter,
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
from .connection_item_b_inwards import ConnectionItemBInwardsAPI
from .connection_item_b_self_edge import ConnectionItemBSelfEdgeAPI
from .connection_item_b_query import ConnectionItemBQueryAPI


class ConnectionItemBAPI(NodeAPI[ConnectionItemB, ConnectionItemBApply, ConnectionItemBList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[ConnectionItemB]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ConnectionItemB,
            class_list=ConnectionItemBList,
            class_apply_list=ConnectionItemBApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
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
            limit: Maximum number of connection item bs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for connection item bs.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_connection_item_b_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ConnectionItemBList)
        return ConnectionItemBQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self, connection_item_b: ConnectionItemBApply | Sequence[ConnectionItemBApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) connection item bs.

        Note: This method iterates through all nodes and timeseries linked to connection_item_b and creates them including the edges
        between the nodes. For example, if any of `inwards` or `self_edge` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            connection_item_b: Connection item b or sequence of connection item bs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new connection_item_b:

                >>> from omni_pydantic_v1 import OmniClient
                >>> from omni_pydantic_v1.data_classes import ConnectionItemBApply
                >>> client = OmniClient()
                >>> connection_item_b = ConnectionItemBApply(external_id="my_connection_item_b", ...)
                >>> result = client.connection_item_b.apply(connection_item_b)

        """
        return self._apply(connection_item_b, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more connection item b.

        Args:
            external_id: External id of the connection item b to delete.
            space: The space where all the connection item b are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete connection_item_b by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> client.connection_item_b.delete("my_connection_item_b")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ConnectionItemB | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ConnectionItemBList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ConnectionItemB | ConnectionItemBList | None:
        """Retrieve one or more connection item bs by id(s).

        Args:
            external_id: External id or list of external ids of the connection item bs.
            space: The space where all the connection item bs are located.

        Returns:
            The requested connection item bs.

        Examples:

            Retrieve connection_item_b by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> connection_item_b = client.connection_item_b.retrieve("my_connection_item_b")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_quad=[
                (
                    self.inwards_edge,
                    "inwards",
                    dm.DirectRelationReference("pygen-models", "bidirectional"),
                    "inwards",
                ),
                (
                    self.self_edge_edge,
                    "self_edge",
                    dm.DirectRelationReference("pygen-models", "reflexive"),
                    "outwards",
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: ConnectionItemBTextFields | Sequence[ConnectionItemBTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ConnectionItemBList:
        """Search connection item bs

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item bs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results connection item bs matching the query.

        Examples:

           Search for 'my_connection_item_b' in all text properties:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> connection_item_bs = client.connection_item_b.search('my_connection_item_b')

        """
        filter_ = _create_connection_item_b_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _CONNECTIONITEMB_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ConnectionItemBFields | Sequence[ConnectionItemBFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ConnectionItemBTextFields | Sequence[ConnectionItemBTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ConnectionItemBFields | Sequence[ConnectionItemBFields] | None = None,
        group_by: ConnectionItemBFields | Sequence[ConnectionItemBFields] = None,
        query: str | None = None,
        search_properties: ConnectionItemBTextFields | Sequence[ConnectionItemBTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ConnectionItemBFields | Sequence[ConnectionItemBFields] | None = None,
        group_by: ConnectionItemBFields | Sequence[ConnectionItemBFields] | None = None,
        query: str | None = None,
        search_property: ConnectionItemBTextFields | Sequence[ConnectionItemBTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across connection item bs

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item bs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count connection item bs in space `my_space`:

                >>> from omni_pydantic_v1 import OmniClient
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
            self._view_id,
            aggregate,
            _CONNECTIONITEMB_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ConnectionItemBFields,
        interval: float,
        query: str | None = None,
        search_property: ConnectionItemBTextFields | Sequence[ConnectionItemBTextFields] | None = None,
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
            limit: Maximum number of connection item bs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            self._view_id,
            property,
            interval,
            _CONNECTIONITEMB_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ConnectionItemBList:
        """List/filter connection item bs

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item bs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `inwards` or `self_edge` external ids for the connection item bs. Defaults to True.

        Returns:
            List of requested connection item bs

        Examples:

            List connection item bs and limit to 5:

                >>> from omni_pydantic_v1 import OmniClient
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

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_quad=[
                (
                    self.inwards_edge,
                    "inwards",
                    dm.DirectRelationReference("pygen-models", "bidirectional"),
                    "inwards",
                ),
                (
                    self.self_edge_edge,
                    "self_edge",
                    dm.DirectRelationReference("pygen-models", "reflexive"),
                    "outwards",
                ),
            ],
        )