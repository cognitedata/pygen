from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite_core._api._core import (
    QueryAPI,
)
from cognite_core.data_classes import (
    CogniteAssetClass,
    CogniteAssetType,
)
from cognite_core.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
    T_DomainModelList,
)


class CogniteAssetTypeQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("cdf_cdm", "CogniteAssetType", "v1")

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
                result_cls=CogniteAssetType,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_asset_class: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_asset_class: Whether to retrieve the asset clas
            for each Cognite asset type or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_asset_class:
            self._query_append_asset_class(from_)
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
