from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from omni.data_classes import (
    DomainModelCore,
    ConnectionItemF,
    ConnectionEdgeA,
)
from omni.data_classes._connection_item_e import (
    ConnectionItemE,
    _create_connection_item_e_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, NodeQueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

from omni.data_classes._connection_edge_a import (
    _create_connection_edge_a_filter,
)

if TYPE_CHECKING:
    from .connection_item_e_query import ConnectionItemEQueryAPI


class ConnectionItemFQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("pygen-models", "ConnectionItemF", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)
        from_ = self._builder.get_from()
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                result_cls=ConnectionItemF,
                max_retrieve_limit=limit,
            )
        )

    def outwards_multi(
        self,
        direct_no_source: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        limit: int = DEFAULT_QUERY_LIMIT,
    ) -> ConnectionItemEQueryAPI[T_DomainModelList]:
        """Query along the outwards multi edges of the connection item f.

        Args:
            direct_no_source: The direct no source to filter on.
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

        Returns:
            ConnectionItemEQueryAPI: The query API for the connection item e.
        """
        from .connection_item_e_query import ConnectionItemEQueryAPI

        from_ = self._builder.get_from()
        edge_view = ConnectionEdgeA._view_id
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
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                result_cls=ConnectionEdgeA,
                max_retrieve_limit=limit,
            )
        )

        view_id = ConnectionItemEQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_connection_item_e_filter(
            view_id,
            direct_no_source,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return ConnectionItemEQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
