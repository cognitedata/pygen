from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter
from movie_domain.client.data_classes import (
    Role,
    RoleApply,
)
from movie_domain.client.data_classes._role import (
    _ROLE_PROPERTIES_BY_FIELD,
)

if TYPE_CHECKING:
    from movie_query import MovieQueryAPI
    from nomination_query import NominationQueryAPI


class RoleQueryAPI(QueryAPI[T_DomainModelList]):
    def movies(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = None,
    ) -> MovieQueryAPI[T_DomainModelList]:
        """Query along the movie edges of the role.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            MovieQueryAPI: The query API for the movie.
        """
        from movie_query import MovieQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Role.movies"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("movies"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                max_retrieve_limit=limit,
            )
        )
        return MovieQueryAPI(self._client, self._builder, self._view_by_write_class)

    def nomination(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = None,
    ) -> NominationQueryAPI[T_DomainModelList]:
        """Query along the nomination edges of the role.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            NominationQueryAPI: The query API for the nomination.
        """
        from nomination_query import NominationQueryAPI

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Role.nomination"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("nomination"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=self._builder[-1].name,
                ),
                max_retrieve_limit=limit,
            )
        )
        return NominationQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_role: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_role: Whether to retrieve the role or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_role and not self._builder[-1].name.startswith("role"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("role"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[RoleApply],
                                list(_ROLE_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Role,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
