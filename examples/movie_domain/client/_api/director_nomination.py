from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class DirectorNominationAPI(EdgeAPI):
    def list(
        self,
        director: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        director_space: str = "IntegrationTestsImmutable",
        nomination: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        nomination_space: str = "IntegrationTestsImmutable",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List nomination edges of a director.

        Args:
            director: ID of the source directors.
            director_space: Location of the directors.
            nomination: ID of the target nominations.
            nomination_space: Location of the nominations.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of nomination edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested nomination edges.

        Examples:

            List 5 nomination edges connected to "my_director":

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> director = client.director.nomination_edge.list("my_director", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Role.nomination"),
            director,
            director_space,
            nomination,
            nomination_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
