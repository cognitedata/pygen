from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from omni.data_classes import (
    DomainModelCore,
    ConnectionItemE,
    ConnectionEdgeA,
)
from omni.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    ViewPropertyId,
    T_DomainModel,
    T_DomainModelList,
    QueryBuilder,
    QueryStep,
)
from omni.data_classes._connection_item_d import (
    _create_connection_item_d_filter,
)
from omni.data_classes._connection_item_f import (
    _create_connection_item_f_filter,
)
from omni._api._core import (
    QueryAPI,
    _create_edge_filter,
)

from omni.data_classes._connection_edge_a import (
    _create_connection_edge_a_filter,
)

if TYPE_CHECKING:
    from omni._api.connection_item_d_query import ConnectionItemDQueryAPI
    from omni._api.connection_item_f_query import ConnectionItemFQueryAPI


class ConnectionItemEQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_models", "ConnectionItemE", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder,
        result_cls: type[T_DomainModel],
        result_list_cls: type[T_DomainModelList],
        connection_property: ViewPropertyId | None = None,
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

    def inwards_single(
        self,
        direct_multi: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        direct_single: (
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
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ) -> ConnectionItemDQueryAPI[T_DomainModel, T_DomainModelList]:
        """Query along the inwards single edges of the connection item e.

        Args:
            direct_multi:
            direct_single:
            name:
            name_prefix:
            external_id_prefix:
            space:
            external_id_prefix_edge:
            space_edge:
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of inwards single edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.

        Returns:
            ConnectionItemDQueryAPI: The query API for the connection item d.
        """
        from .connection_item_d_query import ConnectionItemDQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_pygen_models", "bidirectionalSingle"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="inwards",
                ),
                max_retrieve_limit=limit,
                connection_property=ViewPropertyId(self._view_id, "inwardsSingle"),
            )
        )

        view_id = ConnectionItemDQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filter = _create_connection_item_d_filter(
            view_id,
            direct_multi,
            direct_single,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return ConnectionItemDQueryAPI(
            self._client,
            self._builder,
            self._result_cls,
            self._result_list_cls,
            ViewPropertyId(self._view_id, "end_node"),
            node_filter,
            limit,
        )

    def inwards_single_property(
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
    ) -> ConnectionItemFQueryAPI[T_DomainModel, T_DomainModelList]:
        """Query along the inwards single property edges of the connection item e.

        Args:
            direct_list:
            name:
            name_prefix:
            external_id_prefix:
            space:
            min_end_time_edge:
            max_end_time_edge:
            name_edge:
            name_prefix_edge:
            min_start_time_edge:
            max_start_time_edge:
            external_id_prefix_edge:
            space_edge:
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of inwards single property edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.

        Returns:
            ConnectionItemFQueryAPI: The query API for the connection item f.
        """
        from .connection_item_f_query import ConnectionItemFQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_view = ConnectionEdgeA._view_id
        edge_filter = _create_connection_edge_a_filter(
            dm.DirectRelationReference("sp_pygen_models", "multiProperty"),
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
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="inwards",
                ),
                view_id=ConnectionEdgeA._view_id,
                max_retrieve_limit=limit,
                connection_property=ViewPropertyId(self._view_id, "inwardsSingleProperty"),
            )
        )

        view_id = ConnectionItemFQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filter = _create_connection_item_f_filter(
            view_id,
            direct_list,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return ConnectionItemFQueryAPI(
            self._client,
            self._builder,
            self._result_cls,
            self._result_list_cls,
            ViewPropertyId(self._view_id, "end_node"),
            node_filter,
            limit,
        )

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
