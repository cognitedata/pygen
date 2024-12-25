from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite_core._api._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
    QueryAPI,
    T_DomainModelList,
)
from cognite_core.data_classes import (
    CogniteCADModel,
    CogniteFile,
)


class CogniteCADModelQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("cdf_cdm", "CogniteCADModel", "v1")

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
                result_cls=CogniteCADModel,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_thumbnail: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_thumbnail: Whether to retrieve the thumbnail for each Cognite cad model or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_thumbnail:
            self._query_append_thumbnail(from_)
        return self._query()

    def _query_append_thumbnail(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("thumbnail"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                result_cls=CogniteFile,
            ),
        )
