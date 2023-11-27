from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from osdu_wells_pydantic_v1.client.data_classes import (
    GeoContexts,
    GeoContextsApply,
)
from osdu_wells_pydantic_v1.client.data_classes._geo_contexts import (
    _GEOCONTEXTS_PROPERTIES_BY_FIELD,
)


class GeoContextsQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_geo_context: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_geo_context: Whether to retrieve the geo context or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_geo_context and not self._builder[-1].name.startswith("geo_context"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("geo_context"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[GeoContextsApply],
                                list(_GEOCONTEXTS_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=GeoContexts,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
