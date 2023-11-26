from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter
from markets.client.data_classes import (
    DateTransformationPair,
    DateTransformationPairApply,
    DateTransformation,
    DateTransformationApply,
    DateTransformation,
    DateTransformationApply,
)
from markets.client.data_classes._date_transformation_pair import (
    _DATETRANSFORMATIONPAIR_PROPERTIES_BY_FIELD,
)
from markets.client.data_classes._date_transformation import (
    _DATETRANSFORMATION_PROPERTIES_BY_FIELD,
)
from markets.client.data_classes._date_transformation import (
    _DATETRANSFORMATION_PROPERTIES_BY_FIELD,
)

if TYPE_CHECKING:
    from .date_transformation_query import DateTransformationQueryAPI
    from .date_transformation_query import DateTransformationQueryAPI


class DateTransformationPairQueryAPI(QueryAPI[T_DomainModelList]):
    def end(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = None,
    ) -> DateTransformationQueryAPI[T_DomainModelList]:
        """Query along the end edges of the date transformation pair.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            DateTransformationQueryAPI: The query API for the date transformation.
        """
        from .date_transformation_query import DateTransformationQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("market", "DateTransformationPair.end"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("end"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("date_transformation"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[DateTransformationApply],
                            list(_DATETRANSFORMATION_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=DateTransformation,
                max_retrieve_limit=-1,
            ),
        )
        return DateTransformationQueryAPI(self._client, self._builder, self._view_by_write_class)

    def start(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = None,
    ) -> DateTransformationQueryAPI[T_DomainModelList]:
        """Query along the start edges of the date transformation pair.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            DateTransformationQueryAPI: The query API for the date transformation.
        """
        from .date_transformation_query import DateTransformationQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("market", "DateTransformationPair.start"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("start"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("date_transformation"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[DateTransformationApply],
                            list(_DATETRANSFORMATION_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=DateTransformation,
                max_retrieve_limit=-1,
            ),
        )
        return DateTransformationQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_date_transformation_pair: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_date_transformation_pair: Whether to retrieve the date transformation pair or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_date_transformation_pair and not self._builder[-1].name.startswith("date_transformation_pair"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("date_transformation_pair"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[DateTransformationPairApply],
                                list(_DATETRANSFORMATIONPAIR_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=DateTransformationPair,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
