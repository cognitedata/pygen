from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from scenario_instance.client.data_classes import (
    ScenarioInstance,
    ScenarioInstanceApply,
)
from scenario_instance.client.data_classes._scenario_instance import (
    _SCENARIOINSTANCE_PROPERTIES_BY_FIELD,
)


class ScenarioInstanceQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_scenario_instance: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_scenario_instance: Whether to retrieve the scenario instance or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_scenario_instance and not self._builder[-1].name.startswith("scenario_instance"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("scenario_instance"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[ScenarioInstanceApply],
                                list(_SCENARIOINSTANCE_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=ScenarioInstance,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
