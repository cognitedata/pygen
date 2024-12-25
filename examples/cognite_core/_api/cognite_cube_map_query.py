from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite_core._api._core import (
    QueryAPI,
)
from cognite_core.data_classes import (
    CogniteCubeMap,
    CogniteFile,
)
from cognite_core.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
    T_DomainModelList,
)


class CogniteCubeMapQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("cdf_cdm", "CogniteCubeMap", "v1")

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
                result_cls=CogniteCubeMap,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_back: bool = False,
        retrieve_bottom: bool = False,
        retrieve_front: bool = False,
        retrieve_left: bool = False,
        retrieve_right: bool = False,
        retrieve_top: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_back: Whether to retrieve the back for each Cognite cube map or not.
            retrieve_bottom: Whether to retrieve the bottom for each Cognite cube map or not.
            retrieve_front: Whether to retrieve the front for each Cognite cube map or not.
            retrieve_left: Whether to retrieve the left for each Cognite cube map or not.
            retrieve_right: Whether to retrieve the right for each Cognite cube map or not.
            retrieve_top: Whether to retrieve the top for each Cognite cube map or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_back:
            self._query_append_back(from_)
        if retrieve_bottom:
            self._query_append_bottom(from_)
        if retrieve_front:
            self._query_append_front(from_)
        if retrieve_left:
            self._query_append_left(from_)
        if retrieve_right:
            self._query_append_right(from_)
        if retrieve_top:
            self._query_append_top(from_)
        return self._query()

    def _query_append_back(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("back"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                result_cls=CogniteFile,
            ),
        )

    def _query_append_bottom(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("bottom"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                result_cls=CogniteFile,
            ),
        )

    def _query_append_front(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("front"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                result_cls=CogniteFile,
            ),
        )

    def _query_append_left(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("left"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                result_cls=CogniteFile,
            ),
        )

    def _query_append_right(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("right"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                result_cls=CogniteFile,
            ),
        )

    def _query_append_top(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("top"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[CogniteFile._view_id]),
                ),
                result_cls=CogniteFile,
            ),
        )
