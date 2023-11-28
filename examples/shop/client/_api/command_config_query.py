from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from shop.client.data_classes import (
    CommandConfig,
    CommandConfigApply,
)
from shop.client.data_classes._command_config import (
    _COMMANDCONFIG_PROPERTIES_BY_FIELD,
)


class CommandConfigQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_command_config: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_command_config: Whether to retrieve the command config or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_command_config and not self._builder[-1].name.startswith("command_config"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("command_config"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[CommandConfigApply],
                                list(_COMMANDCONFIG_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=CommandConfig,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
