from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from osdu_wells.client.data_classes import (
    ProjectedBottomHoleLocation,
    ProjectedBottomHoleLocationApply,
)
from osdu_wells.client.data_classes._projected_bottom_hole_location import (
    _PROJECTEDBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD,
)


class ProjectedBottomHoleLocationQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_projected_bottom_hole_location: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_projected_bottom_hole_location: Whether to retrieve the projected bottom hole location or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_projected_bottom_hole_location and not self._builder[-1].name.startswith(
            "projected_bottom_hole_location"
        ):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("projected_bottom_hole_location"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[ProjectedBottomHoleLocationApply],
                                list(_PROJECTEDBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=ProjectedBottomHoleLocation,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
