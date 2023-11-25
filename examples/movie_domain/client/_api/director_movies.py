from __future__ import annotations

import datetime

from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI


class DirectorMoviesAPI(EdgeAPI):
    def list(
        self,
        movie: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        movie_space: str = "IntegrationTestsImmutable",
        movie: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        movie_space: str = "IntegrationTestsImmutable",
        rating: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_release_year: int | None = None,
        max_release_year: int | None = None,
        min_run_time_minutes: float | None = None,
        max_run_time_minutes: float | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> MovieList:
        """List movie edges of a director.

        Args:
            movie: ID of the source movies.
            movie_space: Location of the movies.
            movie: ID of the target movies.
            movie_space: Location of the movies.
            rating: The rating to filter on.
            min_release_year: The minimum value of the release year to filter on.
            max_release_year: The maximum value of the release year to filter on.
            min_run_time_minutes: The minimum value of the run time minute to filter on.
            max_run_time_minutes: The maximum value of the run time minute to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
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
        f = dm.filters
        filter_ = _create_start_end_time_filter(
            self._view_id,
            movie,
            movie_space,
            movie,
            movie_space,
            rating,
            min_release_year,
            max_release_year,
            min_run_time_minutes,
            max_run_time_minutes,
            title,
            title_prefix,
            external_id_prefix,
            space,
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "Role.movies"},
            ),
        )
        return self._list(filter_=filter_, limit=limit)
