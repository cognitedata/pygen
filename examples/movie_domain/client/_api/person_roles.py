from __future__ import annotations

import datetime

from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI


class PersonRolesAPI(EdgeAPI):
    def list(
        self,
        role: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        role_space: str = "IntegrationTestsImmutable",
        role: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        role_space: str = "IntegrationTestsImmutable",
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> RoleList:
        """List role edges of a person.

        Args:
            role: ID of the source roles.
            role_space: Location of the roles.
            role: ID of the target roles.
            role_space: Location of the roles.
            person: The person to filter on.
            won_oscar: The won oscar to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of role edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested role edges.

        Examples:

            List 5 role edges connected to "my_person":

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> person = client.person.roles_edge.list("my_person", limit=5)

        """
        f = dm.filters
        filter_ = _create_start_end_time_filter(
            self._view_id,
            role,
            role_space,
            role,
            role_space,
            person,
            won_oscar,
            external_id_prefix,
            space,
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
            ),
        )
        return self._list(filter_=filter_, limit=limit)
