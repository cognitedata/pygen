from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    HistoricalInterests,
    HistoricalInterestsApply,
    HistoricalInterestsFields,
    HistoricalInterestsList,
    HistoricalInterestsApplyList,
    HistoricalInterestsTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._historical_interests import (
    _HISTORICALINTERESTS_PROPERTIES_BY_FIELD,
    _create_historical_interest_filter,
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
from .historical_interests_query import HistoricalInterestsQueryAPI


class HistoricalInterestsAPI(NodeAPI[HistoricalInterests, HistoricalInterestsApply, HistoricalInterestsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[HistoricalInterestsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=HistoricalInterests,
            class_apply_type=HistoricalInterestsApply,
            class_list=HistoricalInterestsList,
            class_apply_list=HistoricalInterestsApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> HistoricalInterestsQueryAPI[HistoricalInterestsList]:
        """Query starting at historical interests.

        Args:
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of historical interests to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for historical interests.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_historical_interest_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            interest_type_id,
            interest_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(HistoricalInterestsList)
        return HistoricalInterestsQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, historical_interest: HistoricalInterestsApply | Sequence[HistoricalInterestsApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) historical interests.

        Args:
            historical_interest: Historical interest or sequence of historical interests to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new historical_interest:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import HistoricalInterestsApply
                >>> client = OSDUClient()
                >>> historical_interest = HistoricalInterestsApply(external_id="my_historical_interest", ...)
                >>> result = client.historical_interests.apply(historical_interest)

        """
        return self._apply(historical_interest, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more historical interest.

        Args:
            external_id: External id of the historical interest to delete.
            space: The space where all the historical interest are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete historical_interest by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.historical_interests.delete("my_historical_interest")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> HistoricalInterests | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> HistoricalInterestsList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> HistoricalInterests | HistoricalInterestsList | None:
        """Retrieve one or more historical interests by id(s).

        Args:
            external_id: External id or list of external ids of the historical interests.
            space: The space where all the historical interests are located.

        Returns:
            The requested historical interests.

        Examples:

            Retrieve historical_interest by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> historical_interest = client.historical_interests.retrieve("my_historical_interest")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: HistoricalInterestsTextFields | Sequence[HistoricalInterestsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> HistoricalInterestsList:
        """Search historical interests

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of historical interests to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results historical interests matching the query.

        Examples:

           Search for 'my_historical_interest' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> historical_interests = client.historical_interests.search('my_historical_interest')

        """
        filter_ = _create_historical_interest_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            interest_type_id,
            interest_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _HISTORICALINTERESTS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: HistoricalInterestsFields | Sequence[HistoricalInterestsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: HistoricalInterestsTextFields | Sequence[HistoricalInterestsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
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
        property: HistoricalInterestsFields | Sequence[HistoricalInterestsFields] | None = None,
        group_by: HistoricalInterestsFields | Sequence[HistoricalInterestsFields] = None,
        query: str | None = None,
        search_properties: HistoricalInterestsTextFields | Sequence[HistoricalInterestsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
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
        property: HistoricalInterestsFields | Sequence[HistoricalInterestsFields] | None = None,
        group_by: HistoricalInterestsFields | Sequence[HistoricalInterestsFields] | None = None,
        query: str | None = None,
        search_property: HistoricalInterestsTextFields | Sequence[HistoricalInterestsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across historical interests

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of historical interests to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count historical interests in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.historical_interests.aggregate("count", space="my_space")

        """

        filter_ = _create_historical_interest_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            interest_type_id,
            interest_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _HISTORICALINTERESTS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: HistoricalInterestsFields,
        interval: float,
        query: str | None = None,
        search_property: HistoricalInterestsTextFields | Sequence[HistoricalInterestsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for historical interests

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of historical interests to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_historical_interest_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            interest_type_id,
            interest_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _HISTORICALINTERESTS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> HistoricalInterestsList:
        """List/filter historical interests

        Args:
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of historical interests to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested historical interests

        Examples:

            List historical interests and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> historical_interests = client.historical_interests.list(limit=5)

        """
        filter_ = _create_historical_interest_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            interest_type_id,
            interest_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
