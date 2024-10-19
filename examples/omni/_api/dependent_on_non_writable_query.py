from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from omni.data_classes import (
    DomainModelCore,
    DependentOnNonWritable,
)
from omni.data_classes._implementation_1_non_writeable import (
    Implementation1NonWriteable,
    _create_implementation_1_non_writeable_filter,
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
    from .implementation_1_non_writeable_query import Implementation1NonWriteableQueryAPI


class DependentOnNonWritableQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_models", "DependentOnNonWritable", "1")

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
                result_cls=DependentOnNonWritable,
                max_retrieve_limit=limit,
            )
        )

    def to_non_writable(
        self,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ) -> Implementation1NonWriteableQueryAPI[T_DomainModelList]:
        """Query along the to non writable edges of the dependent on non writable.

        Args:
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of to non writable edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.

        Returns:
            Implementation1NonWriteableQueryAPI: The query API for the implementation 1 non writeable.
        """
        from .implementation_1_non_writeable_query import Implementation1NonWriteableQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_pygen_models", "toNonWritable"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            EdgeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                max_retrieve_limit=limit,
            )
        )

        view_id = Implementation1NonWriteableQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_implementation_1_non_writeable_filter(
            view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return Implementation1NonWriteableQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
