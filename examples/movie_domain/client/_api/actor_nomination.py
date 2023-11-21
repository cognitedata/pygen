from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from movie_domain.client.data_classes import Actor, ActorApply, ActorList, ActorApplyList, ActorFields, DomainModelApply
from movie_domain.client.data_classes._actor import _ACTOR_PROPERTIES_BY_FIELD


class ActorNominationAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def list(
        self,
        actor_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List nomination edges of a actor.

        Args:
            actor_id: ID of the source actor.
            limit: Maximum number of nomination edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the nomination edges are located.

        Returns:
            The requested nomination edges.

        Examples:

            List 5 nomination edges connected to "my_actor":

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> actor = client.actor.nomination.list("my_actor", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "Role.nomination"},
            )
        ]
        if actor_id:
            actor_ids = actor_id if isinstance(actor_id, list) else [actor_id]
            is_actors = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in actor_ids
                ],
            )
            filters.append(is_actors)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))
