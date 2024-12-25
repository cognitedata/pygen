from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite_core.data_classes import (
    DomainModelCore,
    Cognite360ImageCollection,
    Cognite360ImageModel,
)
from cognite_core.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    T_DomainModelList,
    EdgeQueryStep,
    NodeQueryStep,
    DataClassQueryBuilder,
)
from cognite_core._api._core import (
    QueryAPI,
    _create_edge_filter,
)


class Cognite360ImageCollectionQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("cdf_cdm", "Cognite360ImageCollection", "v1")

    def __init__(
        self,
        client: CogniteClient,
        builder: DataClassQueryBuilder[T_DomainModelList],
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
                result_cls=Cognite360ImageCollection,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_model_3d: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_model_3d: Whether to retrieve the model 3d for each Cognite 360 image collection or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_model_3d:
            self._query_append_model_3d(from_)
        return self._query()

    def _query_append_model_3d(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("model3D"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Cognite360ImageModel._view_id]),
                ),
                result_cls=Cognite360ImageModel,
            ),
        )
