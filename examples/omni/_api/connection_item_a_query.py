from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cdf.modules.contextualization.cdf_p_and_id_parser.functions.contextualization_p_and_id_annotater.handler import \
    ViewProperty
from omni.data_classes import (
    DomainModelCore,
    ConnectionItemA,
    ConnectionItemCNode,
    ConnectionItemA,
)
from omni.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    T_DomainModel,
    T_DomainModelList,
    ViewPropertyId,
    QueryBuilder,
    QueryStep,
)
from omni.data_classes._connection_item_b import (
    _create_connection_item_b_filter,
)
from omni._api._core import (
    QueryAPI,
    _create_edge_filter,
)

if TYPE_CHECKING:
    from omni._api.connection_item_b_query import ConnectionItemBQueryAPI


class ConnectionItemAQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_models", "ConnectionItemA", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder,
        result_cls: type[T_DomainModel],
        result_list_cls: type[T_DomainModelList],
        connection_property: ViewProperty | None = None,
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, result_cls, result_list_cls)
        from_ = self._builder.get_from()
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                max_retrieve_limit=limit,
                view_id=self._view_id,
                connection_property=connection_property,
            )
        )

    def outwards(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_other_direct: bool = False,
        retrieve_self_direct: bool = False,
    ) -> ConnectionItemBQueryAPI[T_DomainModelList]:
        """Query along the outward edges of the connection item a.

        Args:
            name:
            name_prefix:
            external_id_prefix:
            space:
            external_id_prefix_edge:
            space_edge:
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of outward edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.
            retrieve_other_direct: Whether to retrieve the other direct
                for each connection item a or not.
            retrieve_self_direct: Whether to retrieve the self direct
                for each connection item a or not.

        Returns:
            ConnectionItemBQueryAPI: The query API for the connection item b.
        """
        from .connection_item_b_query import ConnectionItemBQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_pygen_models", "bidirectional"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                max_retrieve_limit=limit,
                connection_property=ViewPropertyId(self._view_id, "outwards")
            )
        )

        view_id = ConnectionItemBQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_connection_item_b_filter(
            view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_other_direct:
            self._query_append_other_direct(from_)
        if retrieve_self_direct:
            self._query_append_self_direct(from_)
        return ConnectionItemBQueryAPI(
            self._client,
            self._builder,
            self._result_cls,
            self._result_list_cls,
            ViewPropertyId(self._view_id, "end_node"),
            node_filer,
            limit
        )

    def query(
        self,
        retrieve_other_direct: bool = False,
        retrieve_self_direct: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_other_direct: Whether to retrieve the
                other direct for each
                connection item a or not.
            retrieve_self_direct: Whether to retrieve the
                self direct for each
                connection item a or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_other_direct:
            self._query_append_other_direct(from_)
        if retrieve_self_direct:
            self._query_append_self_direct(from_)
        return self._query()

    def _query_append_other_direct(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("otherDirect"),
                    direction="outwards",
                ),
                view_id=ConnectionItemCNode._view_id,
                connection_property=ViewPropertyId(self._view_id, "otherDirect"),
            ),
        )

    def _query_append_self_direct(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("selfDirect"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ConnectionItemA._view_id]),
                ),
                view_id=ConnectionItemA._view_id,
                connection_property=ViewPropertyId(self._view_id, "selfDirect")
            ),
        )
