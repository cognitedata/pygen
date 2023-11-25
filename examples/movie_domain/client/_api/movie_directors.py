from __future__ import annotations

import datetime

from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI


class MovieDirectorsAPI(EdgeAPI):
    def list(
        self,
        director: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        director_space: str = "IntegrationTestsImmutable",
        director: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        director_space: str = "IntegrationTestsImmutable",
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> DirectorList:
        """List director edges of a movie.

        Args:
            director: ID of the source directors.
            director_space: Location of the directors.
            director: ID of the target directors.
            director_space: Location of the directors.
            person: The person to filter on.
            won_oscar: The won oscar to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of director edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested director edges.

        Examples:

            List 5 director edges connected to "my_movie":

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> movie = client.movie.directors_edge.list("my_movie", limit=5)

        """
        f = dm.filters
        filter_ = _create_start_end_time_filter(
            self._view_id,
            director,
            director_space,
            director,
            director_space,
            person,
            won_oscar,
            external_id_prefix,
            space,
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "Movie.directors"},
            ),
        )
        return self._list(filter_=filter_, limit=limit)
