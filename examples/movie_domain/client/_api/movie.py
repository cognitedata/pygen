from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from movie_domain.client.data_classes import (
    Movie,
    MovieApply,
    MovieList,
    MovieApplyList,
    MovieFields,
    MovieTextFields,
    DomainModelApply,
)
from movie_domain.client.data_classes._movie import _MOVIE_PROPERTIES_BY_FIELD


class MovieActorsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Movie.actors"},
        )
        if isinstance(external_id, str):
            is_movie = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_movie))

        else:
            is_movies = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_movies))

    def list(
        self, movie_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Movie.actors"},
        )
        filters.append(is_edge_type)
        if movie_id:
            movie_ids = [movie_id] if isinstance(movie_id, str) else movie_id
            is_movies = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in movie_ids],
            )
            filters.append(is_movies)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class MovieDirectorsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Movie.directors"},
        )
        if isinstance(external_id, str):
            is_movie = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_movie))

        else:
            is_movies = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_movies))

    def list(
        self, movie_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Movie.directors"},
        )
        filters.append(is_edge_type)
        if movie_id:
            movie_ids = [movie_id] if isinstance(movie_id, str) else movie_id
            is_movies = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in movie_ids],
            )
            filters.append(is_movies)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class MovieAPI(TypeAPI[Movie, MovieApply, MovieList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[MovieApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Movie,
            class_apply_type=MovieApply,
            class_list=MovieList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.actors = MovieActorsAPI(client)
        self.directors = MovieDirectorsAPI(client)

    def apply(self, movie: MovieApply | Sequence[MovieApply], replace: bool = False) -> dm.InstancesApplyResult:
        """Add or update (upsert) movies.

        Note: This method iterates through all nodes linked to movie and create them including the edges
        between the nodes. For example, if any of `actors` or `directors` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            movie: Movie or sequence of movies to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new movie:

                >>> from movie_domain.client import MovieClient
                >>> from movie_domain.client.data_classes import MovieApply
                >>> client = MovieClient()
                >>> movie = MovieApply(external_id="my_movie", ...)
                >>> result = client.movie.apply(movie)

        """
        if isinstance(movie, MovieApply):
            instances = movie.to_instances_apply(self._view_by_write_class)
        else:
            instances = MovieApplyList(movie).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more movie.

        Args:
            external_id: External id of the movie to delete.
            space: The space where all the movie are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete movie by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> client.movie.delete("my_movie")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Movie:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> MovieList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable") -> Movie | MovieList:
        """Retrieve one or more movies by id(s).

        Args:
            external_id: External id or list of external ids of the movies.
            space: The space where all the movies are located.

        Returns:
            The requested movies.

        Examples:

            Retrieve movie by id:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> movie = client.movie.retrieve("my_movie")

        """
        if isinstance(external_id, str):
            movie = self._retrieve((space, external_id))

            actor_edges = self.actors.retrieve(external_id)
            movie.actors = [edge.end_node.external_id for edge in actor_edges]
            director_edges = self.directors.retrieve(external_id)
            movie.directors = [edge.end_node.external_id for edge in director_edges]

            return movie
        else:
            movies = self._retrieve([(space, ext_id) for ext_id in external_id])

            actor_edges = self.actors.retrieve(external_id)
            self._set_actors(movies, actor_edges)
            director_edges = self.directors.retrieve(external_id)
            self._set_directors(movies, director_edges)

            return movies

    def search(
        self,
        query: str,
        properties: MovieTextFields | Sequence[MovieTextFields] | None = None,
        rating: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_release_year: int | None = None,
        max_release_year: int | None = None,
        min_run_time_minutes: float | None = None,
        max_run_time_minutes: float | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MovieList:
        """Search movies

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            rating: The rating to filter on.
            min_release_year: The minimum value of the release year to filter on.
            max_release_year: The maximum value of the release year to filter on.
            min_run_time_minutes: The minimum value of the run time minute to filter on.
            max_run_time_minutes: The maximum value of the run time minute to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of movies to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `actors` or `directors` external ids for the movies. Defaults to True.

        Returns:
            Search results movies matching the query.

        Examples:

           Search for 'my_movie' in all text properties:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> movies = client.movie.search('my_movie')

        """
        filter_ = _create_filter(
            self._view_id,
            rating,
            min_release_year,
            max_release_year,
            min_run_time_minutes,
            max_run_time_minutes,
            title,
            title_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _MOVIE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MovieFields | Sequence[MovieFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MovieTextFields | Sequence[MovieTextFields] | None = None,
        rating: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_release_year: int | None = None,
        max_release_year: int | None = None,
        min_run_time_minutes: float | None = None,
        max_run_time_minutes: float | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MovieFields | Sequence[MovieFields] | None = None,
        group_by: MovieFields | Sequence[MovieFields] = None,
        query: str | None = None,
        search_properties: MovieTextFields | Sequence[MovieTextFields] | None = None,
        rating: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_release_year: int | None = None,
        max_release_year: int | None = None,
        min_run_time_minutes: float | None = None,
        max_run_time_minutes: float | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MovieFields | Sequence[MovieFields] | None = None,
        group_by: MovieFields | Sequence[MovieFields] | None = None,
        query: str | None = None,
        search_property: MovieTextFields | Sequence[MovieTextFields] | None = None,
        rating: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_release_year: int | None = None,
        max_release_year: int | None = None,
        min_run_time_minutes: float | None = None,
        max_run_time_minutes: float | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across movies

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            rating: The rating to filter on.
            min_release_year: The minimum value of the release year to filter on.
            max_release_year: The maximum value of the release year to filter on.
            min_run_time_minutes: The minimum value of the run time minute to filter on.
            max_run_time_minutes: The maximum value of the run time minute to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of movies to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `actors` or `directors` external ids for the movies. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count movies in space `my_space`:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> result = client.movie.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            rating,
            min_release_year,
            max_release_year,
            min_run_time_minutes,
            max_run_time_minutes,
            title,
            title_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _MOVIE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MovieFields,
        interval: float,
        query: str | None = None,
        search_property: MovieTextFields | Sequence[MovieTextFields] | None = None,
        rating: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_release_year: int | None = None,
        max_release_year: int | None = None,
        min_run_time_minutes: float | None = None,
        max_run_time_minutes: float | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for movies

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            rating: The rating to filter on.
            min_release_year: The minimum value of the release year to filter on.
            max_release_year: The maximum value of the release year to filter on.
            min_run_time_minutes: The minimum value of the run time minute to filter on.
            max_run_time_minutes: The maximum value of the run time minute to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of movies to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `actors` or `directors` external ids for the movies. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            rating,
            min_release_year,
            max_release_year,
            min_run_time_minutes,
            max_run_time_minutes,
            title,
            title_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _MOVIE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        rating: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_release_year: int | None = None,
        max_release_year: int | None = None,
        min_run_time_minutes: float | None = None,
        max_run_time_minutes: float | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> MovieList:
        """List/filter movies

        Args:
            rating: The rating to filter on.
            min_release_year: The minimum value of the release year to filter on.
            max_release_year: The maximum value of the release year to filter on.
            min_run_time_minutes: The minimum value of the run time minute to filter on.
            max_run_time_minutes: The maximum value of the run time minute to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of movies to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `actors` or `directors` external ids for the movies. Defaults to True.

        Returns:
            List of requested movies

        Examples:

            List movies and limit to 5:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> movies = client.movie.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            rating,
            min_release_year,
            max_release_year,
            min_run_time_minutes,
            max_run_time_minutes,
            title,
            title_prefix,
            external_id_prefix,
            space,
            filter,
        )

        movies = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := movies.as_external_ids()) > IN_FILTER_LIMIT:
                actor_edges = self.actors.list(limit=-1)
            else:
                actor_edges = self.actors.list(external_ids, limit=-1)
            self._set_actors(movies, actor_edges)
            if len(external_ids := movies.as_external_ids()) > IN_FILTER_LIMIT:
                director_edges = self.directors.list(limit=-1)
            else:
                director_edges = self.directors.list(external_ids, limit=-1)
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


def _create_filter(
    view_id: dm.ViewId,
    rating: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_release_year: int | None = None,
    max_release_year: int | None = None,
    min_run_time_minutes: float | None = None,
    max_run_time_minutes: float | None = None,
    title: str | list[str] | None = None,
    title_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if rating and isinstance(rating, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("rating"), value={"space": "IntegrationTestsImmutable", "externalId": rating}
            )
        )
    if rating and isinstance(rating, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("rating"), value={"space": rating[0], "externalId": rating[1]})
        )
    if rating and isinstance(rating, list) and isinstance(rating[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("rating"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in rating],
            )
        )
    if rating and isinstance(rating, list) and isinstance(rating[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("rating"), values=[{"space": item[0], "externalId": item[1]} for item in rating]
            )
        )
    if min_release_year or max_release_year:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("releaseYear"), gte=min_release_year, lte=max_release_year)
        )
    if min_run_time_minutes or max_run_time_minutes:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("runTimeMinutes"), gte=min_run_time_minutes, lte=max_run_time_minutes
            )
        )
    if title and isinstance(title, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("title"), value=title))
    if title and isinstance(title, list):
        filters.append(dm.filters.In(view_id.as_property_ref("title"), values=title))
    if title_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("title"), value=title_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
