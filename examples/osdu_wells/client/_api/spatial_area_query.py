from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from osdu_wells.client.data_classes import (
    SpatialArea,
    SpatialAreaApply,
)
from osdu_wells.client.data_classes._spatial_area import (
    _SPATIALAREA_PROPERTIES_BY_FIELD,
)


class SpatialAreaQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_spatial_area: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_spatial_area: Whether to retrieve the spatial area or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_spatial_area and not self._builder[-1].name.startswith("spatial_area"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("spatial_area"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[SpatialAreaApply],
                                list(_SPATIALAREA_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=SpatialArea,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()