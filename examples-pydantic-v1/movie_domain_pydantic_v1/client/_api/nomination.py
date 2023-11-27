from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from movie_domain_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Nomination,
    NominationApply,
    NominationFields,
    NominationList,
    NominationApplyList,
    NominationTextFields,
)
from movie_domain_pydantic_v1.client.data_classes._nomination import (
    _NOMINATION_PROPERTIES_BY_FIELD,
    _create_nomination_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .nomination_query import NominationQueryAPI


class NominationAPI(NodeAPI[Nomination, NominationApply, NominationList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[NominationApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Nomination,
            class_apply_type=NominationApply,
            class_list=NominationList,
            class_apply_list=NominationApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> NominationQueryAPI[NominationList]:
        """Query starting at nominations.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of nominations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for nominations.

        """
        filter_ = _create_nomination_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            NominationList,
            [
                QueryStep(
                    name="nomination",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_NOMINATION_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=Nomination,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return NominationQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, nomination: NominationApply | Sequence[NominationApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) nominations.

        Args:
            nomination: Nomination or sequence of nominations to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new nomination:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> from movie_domain_pydantic_v1.client.data_classes import NominationApply
                >>> client = MovieClient()
                >>> nomination = NominationApply(external_id="my_nomination", ...)
                >>> result = client.nomination.apply(nomination)

        """
        return self._apply(nomination, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more nomination.

        Args:
            external_id: External id of the nomination to delete.
            space: The space where all the nomination are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete nomination by id:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> client.nomination.delete("my_nomination")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Nomination:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> NominationList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> Nomination | NominationList:
        """Retrieve one or more nominations by id(s).

        Args:
            external_id: External id or list of external ids of the nominations.
            space: The space where all the nominations are located.

        Returns:
            The requested nominations.

        Examples:

            Retrieve nomination by id:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> nomination = client.nomination.retrieve("my_nomination")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: NominationTextFields | Sequence[NominationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> NominationList:
        """Search nominations

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of nominations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results nominations matching the query.

        Examples:

           Search for 'my_nomination' in all text properties:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> nominations = client.nomination.search('my_nomination')

        """
        filter_ = _create_nomination_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _NOMINATION_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: NominationFields | Sequence[NominationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: NominationTextFields | Sequence[NominationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
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
        property: NominationFields | Sequence[NominationFields] | None = None,
        group_by: NominationFields | Sequence[NominationFields] = None,
        query: str | None = None,
        search_properties: NominationTextFields | Sequence[NominationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
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
        property: NominationFields | Sequence[NominationFields] | None = None,
        group_by: NominationFields | Sequence[NominationFields] | None = None,
        query: str | None = None,
        search_property: NominationTextFields | Sequence[NominationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across nominations

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of nominations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count nominations in space `my_space`:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> result = client.nomination.aggregate("count", space="my_space")

        """

        filter_ = _create_nomination_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _NOMINATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: NominationFields,
        interval: float,
        query: str | None = None,
        search_property: NominationTextFields | Sequence[NominationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for nominations

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of nominations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_nomination_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _NOMINATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> NominationList:
        """List/filter nominations

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of nominations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested nominations

        Examples:

            List nominations and limit to 5:

                >>> from movie_domain_pydantic_v1.client import MovieClient
                >>> client = MovieClient()
                >>> nominations = client.nomination.list(limit=5)

        """
        filter_ = _create_nomination_filter(
            self._view_id,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
