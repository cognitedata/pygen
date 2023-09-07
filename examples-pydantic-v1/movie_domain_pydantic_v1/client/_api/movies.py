from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from movie_domain_pydantic_v1.client._api._core import TypeAPI
from movie_domain_pydantic_v1.client.data_classes import Movie, MovieApply, MovieList


class MovieActorsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Movie.actors"},
        )
        if isinstance(external_id, str):
            is_movie = f.Equals(
                ["edge", "startNode"],
                {"space": "IntegrationTestsImmutable", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_movie))

        else:
            is_movies = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_movies))

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Movie.actors"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class MovieDirectorsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Movie.directors"},
        )
        if isinstance(external_id, str):
            is_movie = f.Equals(
                ["edge", "startNode"],
                {"space": "IntegrationTestsImmutable", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_movie))

        else:
            is_movies = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_movies))

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Movie.directors"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class MoviesAPI(TypeAPI[Movie, MovieApply, MovieList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("IntegrationTestsImmutable", "Movie", "2"),
            class_type=Movie,
            class_apply_type=MovieApply,
            class_list=MovieList,
        )
        self.actors = MovieActorsAPI(client)
        self.directors = MovieDirectorsAPI(client)

    def apply(self, movie: MovieApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = movie.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(MovieApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(MovieApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Movie:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> MovieList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Movie | MovieList:
        if isinstance(external_id, str):
            movie = self._retrieve((self.sources.space, external_id))

            actor_edges = self.actors.retrieve(external_id)
            movie.actors = [edge.end_node.external_id for edge in actor_edges]
            director_edges = self.directors.retrieve(external_id)
            movie.directors = [edge.end_node.external_id for edge in director_edges]

            return movie
        else:
            movies = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            actor_edges = self.actors.retrieve(external_id)
            self._set_actors(movies, actor_edges)
            director_edges = self.directors.retrieve(external_id)
            self._set_directors(movies, director_edges)

            return movies

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> MovieList:
        movies = self._list(limit=limit)

        actor_edges = self.actors.list(limit=-1)
        self._set_actors(movies, actor_edges)
        director_edges = self.directors.list(limit=-1)
        self._set_directors(movies, director_edges)

        return movies

    @staticmethod
    def _set_actors(movies: Sequence[Movie], actor_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in actor_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for movie in movies:
            node_id = movie.id_tuple()
            if node_id in edges_by_start_node:
                movie.actors = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_directors(movies: Sequence[Movie], director_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in director_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for movie in movies:
            node_id = movie.id_tuple()
            if node_id in edges_by_start_node:
                movie.directors = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
