from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from osdu_wells.client.data_classes import (
    SpatialLocation,
    SpatialLocationApply,
)
from osdu_wells.client.data_classes._spatial_location import (
    _SPATIALLOCATION_PROPERTIES_BY_FIELD,
)


class SpatialLocationQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_spatial_location: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_spatial_location: Whether to retrieve the spatial location or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_spatial_location and not self._builder[-1].name.startswith("spatial_location"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("spatial_location"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[SpatialLocationApply],
                                list(_SPATIALLOCATION_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=SpatialLocation,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
