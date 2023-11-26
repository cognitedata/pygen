from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from movie_domain.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Movie,
    MovieApply,
    MovieFields,
    MovieList,
    MovieTextFields,
)
from movie_domain.client.data_classes._movie import (
    _MOVIE_PROPERTIES_BY_FIELD,
    _create_movie_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .movie_actors import MovieActorsAPI
from .movie_directors import MovieDirectorsAPI
from .movie_query import MovieQueryAPI


class MovieAPI(NodeAPI[Movie, MovieApply, MovieList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[MovieApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Movie,
            class_apply_type=MovieApply,
            class_list=MovieList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.actors_edge = MovieActorsAPI(client)
        self.directors_edge = MovieDirectorsAPI(client)

    def __call__(
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
    ) -> MovieQueryAPI[MovieList]:
        """Query starting at movies.

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

        Returns:
            A query API for movies.

        """
        filter_ = _create_movie_filter(
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
        builder = QueryBuilder(
            MovieList,
            [
                QueryStep(
                    name="movie",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_MOVIE_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=Movie,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return MovieQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, movie: MovieApply | Sequence[MovieApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) movies.

        Note: This method iterates through all nodes and timeseries linked to movie and creates them including the edges
        between the nodes. For example, if any of `actors` or `directors` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            movie: Movie or sequence of movies to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new movie:

                >>> from movie_domain.client import MovieClient
                >>> from movie_domain.client.data_classes import MovieApply
                >>> client = MovieClient()
                >>> movie = MovieApply(external_id="my_movie", ...)
                >>> result = client.movie.apply(movie)

        """
        return self._apply(movie, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
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
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Movie:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> MovieList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> Movie | MovieList:
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
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_pairs=[
                (self.actors_edge, "actors"),
                (self.directors_edge, "directors"),
            ],
        )

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

        Returns:
            Search results movies matching the query.

        Examples:

           Search for 'my_movie' in all text properties:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> movies = client.movie.search('my_movie')

        """
        filter_ = _create_movie_filter(
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

        Returns:
            Aggregation results.

        Examples:

            Count movies in space `my_space`:

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> result = client.movie.aggregate("count", space="my_space")

        """

        filter_ = _create_movie_filter(
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

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_movie_filter(
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
        filter_ = _create_movie_filter(
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

        return self._list(
            limit=limit,
            filter=filter_,
            space=space,
            retrieve_edges=retrieve_edges,
            edge_api_name_pairs=[
                (self.actors_edge, "actors"),
                (self.directors_edge, "directors"),
            ],
        )
