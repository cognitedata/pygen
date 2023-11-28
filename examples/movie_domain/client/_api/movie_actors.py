from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class MovieActorsAPI(EdgeAPI):
    def list(
        self,
        movie: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        movie_space: str = "IntegrationTestsImmutable",
        actor: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        actor_space: str = "IntegrationTestsImmutable",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List actor edges of a movie.

        Args:
            movie: ID of the source movies.
            movie_space: Location of the movies.
            actor: ID of the target actors.
            actor_space: Location of the actors.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of actor edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested actor edges.

        Examples:

            List 5 actor edges connected to "my_movie":

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> movie = client.movie.actors_edge.list("my_movie", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.actors"),
            movie,
            movie_space,
            actor,
            actor_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
