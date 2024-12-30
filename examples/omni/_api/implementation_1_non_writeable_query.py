from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from omni.data_classes import (
    DomainModelCore,
    Implementation1NonWriteable,
    Implementation1,
)
from omni.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    ViewPropertyId,
    T_DomainModel,
    T_DomainModelList,
    QueryBuilder,
    QueryStep,
)
from omni._api._core import (
    QueryAPI,
    _create_edge_filter,
)


class Implementation1NonWriteableQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_models", "Implementation1NonWriteable", "1")

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

    def query(
        self,
        retrieve_connection_value: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_connection_value: Whether to retrieve the
                connection value for each
                implementation 1 non writeable or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_connection_value:
            self._query_append_connection_value(from_)
        return self._query()

    def _query_append_connection_value(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("connectionValue"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Implementation1._view_id]),
                ),
                view_id=Implementation1._view_id,
                connection_property=ViewPropertyId(self._view_id, "connectionValue"),
            ),
        )
