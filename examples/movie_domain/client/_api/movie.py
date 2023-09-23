from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from movie_domain.client.data_classes import Movie, MovieApply, MovieList, MovieApplyList


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

    def list(self, movie_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Movie.actors"},
        )
        filters.append(is_edge_type)
        if movie_id:
            movie_ids = [movie_id] if isinstance(movie_id, str) else movie_id
            is_movies = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in movie_ids],
            )
            filters.append(is_movies)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


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

    def list(self, movie_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Movie.directors"},
        )
        filters.append(is_edge_type)
        if movie_id:
            movie_ids = [movie_id] if isinstance(movie_id, str) else movie_id
            is_movies = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in movie_ids],
            )
            filters.append(is_movies)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class MovieAPI(TypeAPI[Movie, MovieApply, MovieList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Movie,
            class_apply_type=MovieApply,
            class_list=MovieList,
        )
        self.view_id = view_id
        self.actors = MovieActorsAPI(client)
        self.directors = MovieDirectorsAPI(client)

    def apply(self, movie: MovieApply | Sequence[MovieApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(movie, MovieApply):
            instances = movie.to_instances_apply()
        else:
            instances = MovieApplyList(movie).to_instances_apply()
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

    def list(
        self,
        min_release_year: int | None = None,
        max_release_year: int | None = None,
        min_run_time_minutes: float | None = None,
        max_run_time_minutes: float | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> MovieList:
        filters = []
        if min_release_year or max_release_year:
            filters.append(
                dm.filters.Range(
                    self.view_id.as_property_ref("releaseYear"), gte=min_release_year, lte=max_release_year
                )
            )
        if min_run_time_minutes or max_run_time_minutes:
            filters.append(
                dm.filters.Range(
                    self.view_id.as_property_ref("runTimeMinutes"), gte=min_run_time_minutes, lte=max_run_time_minutes
                )
            )
        if title and isinstance(title, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("title"), value=title))
        if title and isinstance(title, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("title"), values=title))
        if title_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("title"), value=title_prefix))
        if external_id_prefix:
            filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
        if filter:
            filters.append(filter)

        movies = self._list(limit=limit, filter=dm.filters.And(*filters) if filters else None)

        if retrieve_edges:
            actor_edges = self.actors.list(movies.as_external_ids(), limit=-1)
            self._set_actors(movies, actor_edges)
            director_edges = self.directors.list(movies.as_external_ids(), limit=-1)
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
