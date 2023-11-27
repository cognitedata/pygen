from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from markets.client.data_classes import (
    DateTransformation,
    DateTransformationApply,
)
from markets.client.data_classes._date_transformation import (
    _DATETRANSFORMATION_PROPERTIES_BY_FIELD,
)


class DateTransformationQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_date_transformation: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_date_transformation: Whether to retrieve the date transformation or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_date_transformation and not self._builder[-1].name.startswith("date_transformation"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("date_transformation"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
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

        return self._query()
