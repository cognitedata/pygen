from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from omni.data_classes import (
    DomainModelCore,
    ConnectionItemD,
    ConnectionItemE,
    ConnectionItemE,
)
from omni.data_classes._connection_item_e import (
    ConnectionItemE,
    _create_connection_item_e_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .connection_item_e_query import ConnectionItemEQueryAPI


class ConnectionItemDQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("pygen-models", "ConnectionItemD", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("connection_item_d"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])]),
                result_cls=ConnectionItemD,
                max_retrieve_limit=limit,
            )
        )

    def outwards_single(
        self,
        direct_no_source: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_direct_multi: bool = False,
        retrieve_direct_single: bool = False,
    ) -> ConnectionItemEQueryAPI[T_DomainModelList]:
        """Query along the outwards single edges of the connection item d.

        Args:
            direct_no_source: The direct no source to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of outwards single edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_direct_multi: Whether to retrieve the direct multi for each connection item d or not.
            retrieve_direct_single: Whether to retrieve the direct single for each connection item d or not.

        Returns:
            ConnectionItemEQueryAPI: The query API for the connection item e.
        """
        from .connection_item_e_query import ConnectionItemEQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("pygen-models", "bidirectionalSingle"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("outwards_single"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
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
        if retrieve_direct_multi:
            self._query_append_direct_multi(from_)
        if retrieve_direct_single:
            self._query_append_direct_single(from_)
        return ConnectionItemEQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
        retrieve_direct_multi: bool = False,
        retrieve_direct_single: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_direct_multi: Whether to retrieve the direct multi for each connection item d or not.
            retrieve_direct_single: Whether to retrieve the direct single for each connection item d or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_direct_multi:
            self._query_append_direct_multi(from_)
        if retrieve_direct_single:
            self._query_append_direct_single(from_)
        return self._query()

    def _query_append_direct_multi(self, from_: str) -> None:
        view_id = ConnectionItemE._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("direct_multi"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("directMulti"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ConnectionItemE,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_direct_single(self, from_: str) -> None:
        view_id = ConnectionItemE._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("direct_single"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("directSingle"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ConnectionItemE,
                is_single_direct_relation=True,
            ),
        )
