from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from movie_domain_pydantic_v1.client.data_classes import Actor, ActorApply, ActorList


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

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.movies"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ActorNominationsAPI:
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

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.nomination"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ActorsAPI(TypeAPI[Actor, ActorApply, ActorList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("IntegrationTestsImmutable", "Actor", "2"),
            class_type=Actor,
            class_apply_type=ActorApply,
            class_list=ActorList,
        )
        self.movies = ActorMoviesAPI(client)
        self.nominations = ActorNominationsAPI(client)

    def apply(self, actor: ActorApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = actor.to_instances_apply()
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
            nomination_edges = self.nominations.retrieve(external_id)
            actor.nomination = [edge.end_node.external_id for edge in nomination_edges]

            return actor
        else:
            actors = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            movie_edges = self.movies.retrieve(external_id)
            self._set_movies(actors, movie_edges)
            nomination_edges = self.nominations.retrieve(external_id)
            self._set_nomination(actors, nomination_edges)

            return actors

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> ActorList:
        actors = self._list(limit=limit)

        movie_edges = self.movies.list(limit=-1)
        self._set_movies(actors, movie_edges)
        nomination_edges = self.nominations.list(limit=-1)
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
