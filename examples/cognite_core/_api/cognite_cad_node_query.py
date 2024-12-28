from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite_core.data_classes import (
    DomainModelCore,
    CogniteCADNode,
    CogniteCADModel,
    Cognite3DObject,
)
from cognite_core.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    ViewPropertyId,
    T_DomainModel,
    T_DomainModelList,
    QueryBuilder,
    QueryStep,
)
from cognite_core._api._core import (
    QueryAPI,
    _create_edge_filter,
)


class CogniteCADNodeQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("cdf_cdm", "CogniteCADNode", "v1")

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
        retrieve_model_3d: bool = False,
        retrieve_object_3d: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_model_3d: Whether to retrieve the
                model 3d for each
                Cognite cad node or not.
            retrieve_object_3d: Whether to retrieve the
                object 3d for each
                Cognite cad node or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_model_3d:
            self._query_append_model_3d(from_)
        if retrieve_object_3d:
            self._query_append_object_3d(from_)
        return self._query()

    def _query_append_model_3d(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("model3D"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteCADModel._view_id]),
                ),
                view_id=CogniteCADModel._view_id,
                connection_property=ViewPropertyId(self._view_id, "model3D"),
            ),
        )

    def _query_append_object_3d(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("object3D"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Cognite3DObject._view_id]),
                ),
                view_id=Cognite3DObject._view_id,
                connection_property=ViewPropertyId(self._view_id, "object3D"),
            ),
        )
