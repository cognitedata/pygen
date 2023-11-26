from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from osdu_wells.client.data_classes import (
    VerticalMeasurements,
    VerticalMeasurementsApply,
)
from osdu_wells.client.data_classes._vertical_measurements import (
    _VERTICALMEASUREMENTS_PROPERTIES_BY_FIELD,
)


class VerticalMeasurementsQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_vertical_measurement: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_vertical_measurement: Whether to retrieve the vertical measurement or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_vertical_measurement and not self._builder[-1].name.startswith("vertical_measurement"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("vertical_measurement"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[VerticalMeasurementsApply],
                                list(_VERTICALMEASUREMENTS_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=VerticalMeasurements,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
