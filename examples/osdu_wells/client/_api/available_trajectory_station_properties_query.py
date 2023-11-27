from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from osdu_wells.client.data_classes import (
    AvailableTrajectoryStationProperties,
    AvailableTrajectoryStationPropertiesApply,
)
from osdu_wells.client.data_classes._available_trajectory_station_properties import (
    _AVAILABLETRAJECTORYSTATIONPROPERTIES_PROPERTIES_BY_FIELD,
)


class AvailableTrajectoryStationPropertiesQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_available_trajectory_station_property: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_available_trajectory_station_property: Whether to retrieve the available trajectory station property or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_available_trajectory_station_property and not self._builder[-1].name.startswith(
            "available_trajectory_station_property"
        ):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("available_trajectory_station_property"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[AvailableTrajectoryStationPropertiesApply],
                                list(_AVAILABLETRAJECTORYSTATIONPROPERTIES_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=AvailableTrajectoryStationProperties,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
