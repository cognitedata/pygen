from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class PersonRolesAPI(EdgeAPI):
    def list(
        self,
        person: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        person_space: str = "IntegrationTestsImmutable",
        role: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        role_space: str = "IntegrationTestsImmutable",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List role edges of a person.

        Args:
            person: ID of the source persons.
            person_space: Location of the persons.
            role: ID of the target roles.
            role_space: Location of the roles.
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
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Person.roles"),
            person,
            person_space,
            role,
            role_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
