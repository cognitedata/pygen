from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from omni.data_classes import (
    DomainModelCore,
    ConnectionItemF,
    ConnectionEdgeA,
    ConnectionItemD,
)
from omni.data_classes._connection_item_g import (
    ConnectionItemG,
    _create_connection_item_g_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

from omni.data_classes._connection_edge_a import (
    _create_connection_edge_a_filter,
)

if TYPE_CHECKING:
    from .connection_item_g_query import ConnectionItemGQueryAPI


class ConnectionItemFQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_read_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("connection_item_f"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[ConnectionItemF], ["*"])]),
                result_cls=ConnectionItemF,
                max_retrieve_limit=limit,
            )
        )

    def outwards_multi(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        min_end_time_edge: datetime.datetime | None = None,
        max_end_time_edge: datetime.datetime | None = None,
        name_edge: str | list[str] | None = None,
        name_prefix_edge: str | None = None,
        min_start_time_edge: datetime.datetime | None = None,
        max_start_time_edge: datetime.datetime | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_direct_list: bool = False,
    ) -> ConnectionItemGQueryAPI[T_DomainModelList]:
        """Query along the outwards multi edges of the connection item f.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            min_end_time_edge: The minimum value of the end time to filter on.
            max_end_time_edge: The maximum value of the end time to filter on.
            name_edge: The name to filter on.
            name_prefix_edge: The prefix of the name to filter on.
            min_start_time_edge: The minimum value of the start time to filter on.
            max_start_time_edge: The maximum value of the start time to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of outwards multi edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_direct_list: Whether to retrieve the direct list for each connection item f or not.

        Returns:
            ConnectionItemGQueryAPI: The query API for the connection item g.
        """
        from .connection_item_g_query import ConnectionItemGQueryAPI

        from_ = self._builder[-1].name
        edge_view = self._view_by_read_class[ConnectionEdgeA]
        edge_filter = _create_connection_edge_a_filter(
            dm.DirectRelationReference("pygen-models", "multiProperty"),
            edge_view,
            min_end_time=min_end_time_edge,
            max_end_time=max_end_time_edge,
            name=name_edge,
            name_prefix=name_prefix_edge,
            min_start_time=min_start_time_edge,
            max_start_time=max_start_time_edge,
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("outwards_multi"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(edge_view, ["*"])],
                ),
                result_cls=ConnectionEdgeA,
                max_retrieve_limit=limit,
            )
        )

        view_id = self._view_by_read_class[ConnectionItemG]
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_connection_item_g_filter(
            view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_direct_list:
            self._query_append_direct_list(from_)
        return ConnectionItemGQueryAPI(self._client, self._builder, self._view_by_read_class, node_filer, limit)

    def query(
        self,
        retrieve_direct_list: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_direct_list: Whether to retrieve the direct list for each connection item f or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_direct_list:
            self._query_append_direct_list(from_)
        return self._query()

    def _query_append_direct_list(self, from_: str) -> None:
        view_id = self._view_by_read_class[ConnectionItemD]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("direct_list"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ConnectionItemF].as_property_ref("directList"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ConnectionItemD,
                is_single_direct_relation=True,
            ),
        )
