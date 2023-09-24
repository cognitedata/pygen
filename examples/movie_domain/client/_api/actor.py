from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from movie_domain.client.data_classes import Actor, ActorApply, ActorList, ActorApplyList


class ActorMoviesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.movies"},
        )
        if isinstance(external_id, str):
            is_actor = f.Equals(
                ["edge", "startNode"],
                {"space": "IntegrationTestsImmutable", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_actor))

        else:
            is_actors = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_actors))

    def list(self, actor_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.movies"},
        )
        filters.append(is_edge_type)
        if actor_id:
            actor_ids = [actor_id] if isinstance(actor_id, str) else actor_id
            is_actors = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in actor_ids],
            )
            filters.append(is_actors)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class ActorNominationAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.nomination"},
        )
        if isinstance(external_id, str):
            is_actor = f.Equals(
                ["edge", "startNode"],
                {"space": "IntegrationTestsImmutable", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_actor))

        else:
            is_actors = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_actors))

    def list(self, actor_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.nomination"},
        )
        filters.append(is_edge_type)
        if actor_id:
            actor_ids = [actor_id] if isinstance(actor_id, str) else actor_id
            is_actors = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in actor_ids],
            )
            filters.append(is_actors)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class ActorAPI(TypeAPI[Actor, ActorApply, ActorList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Actor,
            class_apply_type=ActorApply,
            class_list=ActorList,
        )
        self.view_id = view_id
        self.movies = ActorMoviesAPI(client)
        self.nomination = ActorNominationAPI(client)

    def apply(self, actor: ActorApply | Sequence[ActorApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(actor, ActorApply):
            instances = actor.to_instances_apply()
        else:
            instances = ActorApplyList(actor).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ActorApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ActorApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Actor:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ActorList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Actor | ActorList:
        if isinstance(external_id, str):
            actor = self._retrieve((self.sources.space, external_id))

            movie_edges = self.movies.retrieve(external_id)
            actor.movies = [edge.end_node.external_id for edge in movie_edges]
            nomination_edges = self.nomination.retrieve(external_id)
            actor.nomination = [edge.end_node.external_id for edge in nomination_edges]

            return actor
        else:
            actors = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            movie_edges = self.movies.retrieve(external_id)
            self._set_movies(actors, movie_edges)
            nomination_edges = self.nomination.retrieve(external_id)
            self._set_nomination(actors, nomination_edges)

            return actors

    def list(
        self,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ActorList:
        filter_ = _create_filter(
            self.view_id,
            won_oscar,
            external_id_prefix,
            filter,
        )

        actors = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            movie_edges = self.movies.list(actors.as_external_ids(), limit=-1)
            self._set_movies(actors, movie_edges)
            nomination_edges = self.nomination.list(actors.as_external_ids(), limit=-1)
            self._set_nomination(actors, nomination_edges)

        return actors

    @staticmethod
    def _set_movies(actors: Sequence[Actor], movie_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in movie_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for actor in actors:
            node_id = actor.id_tuple()
            if node_id in edges_by_start_node:
                actor.movies = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_nomination(actors: Sequence[Actor], nomination_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in nomination_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for actor in actors:
            node_id = actor.id_tuple()
            if node_id in edges_by_start_node:
                actor.nomination = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    won_oscar: bool | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if won_oscar and isinstance(won_oscar, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("wonOscar"), value=won_oscar))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
