from __future__ import annotations

import datetime

from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI


class MovieActorsAPI(EdgeAPI):
    def list(
        self,
        actor: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        actor_space: str = "IntegrationTestsImmutable",
        actor: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        actor_space: str = "IntegrationTestsImmutable",
        person: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> ActorList:
        """List actor edges of a movie.

        Args:
            actor: ID of the source actors.
            actor_space: Location of the actors.
            actor: ID of the target actors.
            actor_space: Location of the actors.
            person: The person to filter on.
            won_oscar: The won oscar to filter on.
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
        f = dm.filters
        filter_ = _create_start_end_time_filter(
            self._view_id,
            actor,
            actor_space,
            actor,
            actor_space,
            person,
            won_oscar,
            external_id_prefix,
            space,
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "Movie.actors"},
            ),
        )
        return self._list(filter_=filter_, limit=limit)
