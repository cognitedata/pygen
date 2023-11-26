from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from osdu_wells.client.data_classes import (
    NameAliases,
    NameAliasesApply,
)
from osdu_wells.client.data_classes._name_aliases import (
    _NAMEALIASES_PROPERTIES_BY_FIELD,
)


class NameAliasesQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_name_alias: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_name_alias: Whether to retrieve the name alias or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_name_alias and not self._builder[-1].name.startswith("name_alias"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("name_alias"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[NameAliasesApply],
                                list(_NAMEALIASES_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=NameAliases,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
