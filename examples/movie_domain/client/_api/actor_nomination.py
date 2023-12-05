from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from movie_domain.client.data_classes._core import DEFAULT_INSTANCE_SPACE


class ActorNominationAPI(EdgeAPI):
    def list(
        self,
        from_actor: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_actor_space: str = DEFAULT_INSTANCE_SPACE,
        to_nomination: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_nomination_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List nomination edges of a actor.

        Args:
            from_actor: ID of the source actor.
            from_actor_space: Location of the actors.
            to_nomination: ID of the target nomination.
            to_nomination_space: Location of the nominations.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of nomination edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested nomination edges.

        Examples:

            List 5 nomination edges connected to "my_actor":

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> actor = client.actor.nomination_edge.list("my_actor", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Role.nomination"),
            from_actor,
            from_actor_space,
            to_nomination,
            to_nomination_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
