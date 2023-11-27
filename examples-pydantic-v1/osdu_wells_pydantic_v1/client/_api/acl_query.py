from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from osdu_wells_pydantic_v1.client.data_classes import (
    Acl,
    AclApply,
)
from osdu_wells_pydantic_v1.client.data_classes._acl import (
    _ACL_PROPERTIES_BY_FIELD,
)


class AclQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_acl: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_acl: Whether to retrieve the acl or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_acl and not self._builder[-1].name.startswith("acl"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("acl"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[AclApply],
                                list(_ACL_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Acl,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
