from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from movie_domain.client.data_classes._core import DEFAULT_INSTANCE_SPACE


class DirectorMoviesAPI(EdgeAPI):
    def list(
        self,
        from_director: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_director_space: str = DEFAULT_INSTANCE_SPACE,
        to_movie: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_movie_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List movie edges of a director.

        Args:
            from_director: ID of the source director.
            from_director_space: Location of the directors.
            to_movie: ID of the target movie.
            to_movie_space: Location of the movies.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of movie edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested movie edges.

        Examples:

            List 5 movie edges connected to "my_director":

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> director = client.director.movies_edge.list("my_director", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.directors"),
            to_movie,
            to_movie_space,
            from_director,
            from_director_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
