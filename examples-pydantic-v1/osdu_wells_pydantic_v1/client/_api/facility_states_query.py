from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from osdu_wells_pydantic_v1.client.data_classes import (
    FacilityStates,
    FacilityStatesApply,
)
from osdu_wells_pydantic_v1.client.data_classes._facility_states import (
    _FACILITYSTATES_PROPERTIES_BY_FIELD,
)


class FacilityStatesQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_facility_state: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_facility_state: Whether to retrieve the facility state or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_facility_state and not self._builder[-1].name.startswith("facility_state"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("facility_state"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[FacilityStatesApply],
                                list(_FACILITYSTATES_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=FacilityStates,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
