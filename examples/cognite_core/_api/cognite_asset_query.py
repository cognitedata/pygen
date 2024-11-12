from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite_core.data_classes import (
    DomainModelCore,
    CogniteAsset,
    CogniteAssetClass,
    Cognite3DObject,
    CogniteAsset,
    CogniteAsset,
    CogniteSourceSystem,
    CogniteAssetType,
)
from cognite_core._api._core import (
    DEFAULT_QUERY_LIMIT,
    EdgeQueryStep,
    NodeQueryStep,
    DataClassQueryBuilder,
    QueryAPI,
    T_DomainModelList,
    _create_edge_filter,
)


class CogniteAssetQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("cdf_cdm", "CogniteAsset", "v1")

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
                result_cls=CogniteAsset,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_asset_class: bool = False,
        retrieve_object_3d: bool = False,
        retrieve_parent: bool = False,
        retrieve_root: bool = False,
        retrieve_source: bool = False,
        retrieve_type_: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_asset_class: Whether to retrieve the asset clas for each Cognite asset or not.
            retrieve_object_3d: Whether to retrieve the object 3d for each Cognite asset or not.
            retrieve_parent: Whether to retrieve the parent for each Cognite asset or not.
            retrieve_root: Whether to retrieve the root for each Cognite asset or not.
            retrieve_source: Whether to retrieve the source for each Cognite asset or not.
            retrieve_type_: Whether to retrieve the type for each Cognite asset or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_asset_class:
            self._query_append_asset_class(from_)
        if retrieve_object_3d:
            self._query_append_object_3d(from_)
        if retrieve_parent:
            self._query_append_parent(from_)
        if retrieve_root:
            self._query_append_root(from_)
        if retrieve_source:
            self._query_append_source(from_)
        if retrieve_type_:
            self._query_append_type_(from_)
        return self._query()

    def _query_append_asset_class(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("assetClass"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteAssetClass._view_id]),
                ),
                result_cls=CogniteAssetClass,
            ),
        )

    def _query_append_object_3d(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("object3D"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Cognite3DObject._view_id]),
                ),
                result_cls=Cognite3DObject,
            ),
        )

    def _query_append_parent(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("parent"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteAsset._view_id]),
                ),
                result_cls=CogniteAsset,
            ),
        )

    def _query_append_root(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("root"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteAsset._view_id]),
                ),
                result_cls=CogniteAsset,
            ),
        )

    def _query_append_source(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("source"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteSourceSystem._view_id]),
                ),
                result_cls=CogniteSourceSystem,
            ),
        )

    def _query_append_type_(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("type"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteAssetType._view_id]),
                ),
                result_cls=CogniteAssetType,
            ),
        )
