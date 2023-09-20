from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from movie_domain_pydantic_v1.client.data_classes import Director, DirectorApply, DirectorList


class DirectorMoviesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.movies"},
        )
        if isinstance(external_id, str):
            is_director = f.Equals(
                ["edge", "startNode"],
                {"space": "IntegrationTestsImmutable", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_director))

        else:
            is_directors = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_directors))

    def list(self, director_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.movies"},
        )
        filters.append(is_edge_type)
        if director_id:
            director_ids = [director_id] if isinstance(director_id, str) else director_id
            is_directors = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in director_ids],
            )
            filters.append(is_directors)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DirectorNominationAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.nomination"},
        )
        if isinstance(external_id, str):
            is_director = f.Equals(
                ["edge", "startNode"],
                {"space": "IntegrationTestsImmutable", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_director))

        else:
            is_directors = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_directors))

    def list(self, director_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.nomination"},
        )
        filters.append(is_edge_type)
        if director_id:
            director_ids = [director_id] if isinstance(director_id, str) else director_id
            is_directors = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in director_ids],
            )
            filters.append(is_directors)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DirectorAPI(TypeAPI[Director, DirectorApply, DirectorList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Director,
            class_apply_type=DirectorApply,
            class_list=DirectorList,
        )
        self.view_id = view_id
        self.movies = DirectorMoviesAPI(client)
        self.nomination = DirectorNominationAPI(client)

    def apply(self, director: DirectorApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = director.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(DirectorApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(DirectorApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Director:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DirectorList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Director | DirectorList:
        if isinstance(external_id, str):
            director = self._retrieve((self.sources.space, external_id))

            movie_edges = self.movies.retrieve(external_id)
            director.movies = [edge.end_node.external_id for edge in movie_edges]
            nomination_edges = self.nomination.retrieve(external_id)
            director.nomination = [edge.end_node.external_id for edge in nomination_edges]

            return director
        else:
            directors = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            movie_edges = self.movies.retrieve(external_id)
            self._set_movies(directors, movie_edges)
            nomination_edges = self.nomination.retrieve(external_id)
            self._set_nomination(directors, nomination_edges)

            return directors

    def list(
        self,
        won_oscar: bool | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> DirectorList:
        filters = []
        if won_oscar and isinstance(won_oscar, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("wonOscar"), value=won_oscar))
        if external_id_prefix:
            filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
        if filter:
            filters.append(filter)

        directors = self._list(limit=limit, filter=dm.filters.And(*filters) if filters else None)

        if retrieve_edges:
            movie_edges = self.movies.list(directors.as_external_ids(), limit=-1)
            self._set_movies(directors, movie_edges)
            nomination_edges = self.nomination.list(directors.as_external_ids(), limit=-1)
            self._set_nomination(directors, nomination_edges)

        return directors

    @staticmethod
    def _set_movies(directors: Sequence[Director], movie_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in movie_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for director in directors:
            node_id = director.id_tuple()
            if node_id in edges_by_start_node:
                director.movies = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_nomination(directors: Sequence[Director], nomination_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in nomination_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for director in directors:
            node_id = director.id_tuple()
            if node_id in edges_by_start_node:
                director.nomination = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
