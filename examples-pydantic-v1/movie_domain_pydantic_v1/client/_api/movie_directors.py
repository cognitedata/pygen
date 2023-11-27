from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class MovieDirectorsAPI(EdgeAPI):
    def list(
        self,
        movie: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        movie_space: str = "IntegrationTestsImmutable",
        director: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        director_space: str = "IntegrationTestsImmutable",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List director edges of a movie.

        Args:
            movie: ID of the source movies.
            movie_space: Location of the movies.
            director: ID of the target directors.
            director_space: Location of the directors.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of director edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested director edges.

        Examples:

            List 5 director edges connected to "my_movie":

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> movie = client.movie.directors_edge.list("my_movie", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Movie.directors"),
            movie,
            movie_space,
            director,
            director_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
