from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite_core._api._core import (
    QueryAPI,
)
from cognite_core.data_classes import (
    CogniteDescribableNode,
)
from cognite_core.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
    T_DomainModelList,
)


class CogniteDescribableNodeQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("cdf_cdm", "CogniteDescribable", "v1")

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
                result_cls=CogniteDescribableNode,
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
