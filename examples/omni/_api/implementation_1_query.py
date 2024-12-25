from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from omni._api._core import (
    QueryAPI,
)
from omni.data_classes import (
    Implementation1,
)
from omni.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
    T_DomainModelList,
)


class Implementation1QueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_models", "Implementation1", "1")

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
                result_cls=Implementation1,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
