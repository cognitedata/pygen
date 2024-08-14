from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from omni.data_classes import (
    DomainModelCore,
    ConnectionItemE,
)
from omni.data_classes._connection_item_d import (
    ConnectionItemD,
    _create_connection_item_d_filter,
)
from ._core import (
    DEFAULT_QUERY_LIMIT,
    EdgeQueryStep,
    NodeQueryStep,
    QueryBuilder,
    QueryAPI,
    T_DomainModelList,
    _create_edge_filter,
)

if TYPE_CHECKING:
    from .connection_item_d_query import ConnectionItemDQueryAPI


class ConnectionItemEQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("pygen-models", "ConnectionItemE", "1")

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
                result_cls=ConnectionItemE,
                max_retrieve_limit=limit,
            )
        )

    def inwards_single(
        self,
        direct_multi: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        direct_single: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ) -> ConnectionItemDQueryAPI[T_DomainModelList]:
        """Query along the inwards single edges of the connection item e.

        Args:
            direct_multi: The direct multi to filter on.
            direct_single: The direct single to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of inwards single edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ConnectionItemDQueryAPI: The query API for the connection item d.
        """
        from .connection_item_d_query import ConnectionItemDQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("pygen-models", "bidirectionalSingle"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            EdgeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="inwards",
                ),
                max_retrieve_limit=limit,
            )
        )

        view_id = ConnectionItemDQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_connection_item_d_filter(
            view_id,
            direct_multi,
            direct_single,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return ConnectionItemDQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
