from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from osdu_wells.client.data_classes import (
    FacilityOperators,
    FacilityOperatorsApply,
)
from osdu_wells.client.data_classes._facility_operators import (
    _FACILITYOPERATORS_PROPERTIES_BY_FIELD,
)


class FacilityOperatorsQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_facility_operator: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_facility_operator: Whether to retrieve the facility operator or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_facility_operator and not self._builder[-1].name.startswith("facility_operator"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("facility_operator"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[FacilityOperatorsApply],
                                list(_FACILITYOPERATORS_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=FacilityOperators,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
