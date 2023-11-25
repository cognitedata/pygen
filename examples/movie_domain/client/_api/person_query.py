from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from movie_domain.client.data_classes import (
    Person,
    PersonApply,
)
from movie_domain.client.data_classes._person import (
    _PERSON_PROPERTIES_BY_FIELD,
)
if TYPE_CHECKING:

    from role_query import RoleQueryAPI



class PersonQueryAPI(QueryAPI[T_DomainModelList]):
    def roles(
        self,
        limit: int | None = None,
    ) -> RoleQueryAPI[T_DomainModelList]:
        """Query along the role edges of the person.

        Args:
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            RoleQueryAPI: The query API for the role.
        """

    from role_query import RoleQueryAPI

        f = dm.filters
        edge_view = self._view_by_write_class[RoleApply]
        edge_filter = _create_role_filter(
            edge_view,
            None,
            None,
            None,
            None,
            f.Equals(
                ["edge", "type"],
                {"space": "REPLACE", "externalId": "REPLACE"},
            ),
        )
        self._builder.append(
            QueryStep(
                name="roles",
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                max_retrieve_limit=limit,
            )
        )
        return RoleQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_person: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_person: Whether to retrieve the person or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_person and not self._builder[-1].name.startswith("person"):
            self._builder.append(
                QueryStep(
                    name="person",
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[PersonApply],
                                list(_PERSON_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Person,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
