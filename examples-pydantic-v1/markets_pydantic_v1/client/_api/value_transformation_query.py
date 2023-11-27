from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from markets_pydantic_v1.client.data_classes import (
    ValueTransformation,
    ValueTransformationApply,
)
from markets_pydantic_v1.client.data_classes._value_transformation import (
    _VALUETRANSFORMATION_PROPERTIES_BY_FIELD,
)


class ValueTransformationQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_value_transformation: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_value_transformation: Whether to retrieve the value transformation or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_value_transformation and not self._builder[-1].name.startswith("value_transformation"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("value_transformation"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[ValueTransformationApply],
                                list(_VALUETRANSFORMATION_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=ValueTransformation,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
