from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from markets.client.data_classes import (
    DomainModelApply,
    ValueTransformation,
    ValueTransformationApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList


class ValueTransformationQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_write_class: dict[type[DomainModelApply], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_write_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("value_transformation"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_write_class[ValueTransformationApply], ["*"])]
                ),
                result_cls=ValueTransformation,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        return self._query()
