from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    FacilityStates,
    FacilityStatesApply,
    FacilityStatesFields,
    FacilityStatesList,
    FacilityStatesApplyList,
    FacilityStatesTextFields,
)
from osdu_wells.client.data_classes._facility_states import (
    _FACILITYSTATES_PROPERTIES_BY_FIELD,
    _create_facility_state_filter,
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
from .facility_states_query import FacilityStatesQueryAPI


class FacilityStatesAPI(NodeAPI[FacilityStates, FacilityStatesApply, FacilityStatesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[FacilityStatesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=FacilityStates,
            class_apply_type=FacilityStatesApply,
            class_list=FacilityStatesList,
            class_apply_list=FacilityStatesApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_state_type_id: str | list[str] | None = None,
        facility_state_type_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> FacilityStatesQueryAPI[FacilityStatesList]:
        """Query starting at facility states.

        Args:
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_state_type_id: The facility state type id to filter on.
            facility_state_type_id_prefix: The prefix of the facility state type id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility states to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for facility states.

        """
        filter_ = _create_facility_state_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_state_type_id,
            facility_state_type_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            FacilityStatesList,
            [
                QueryStep(
                    name="facility_state",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_FACILITYSTATES_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=FacilityStates,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return FacilityStatesQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, facility_state: FacilityStatesApply | Sequence[FacilityStatesApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) facility states.

        Args:
            facility_state: Facility state or sequence of facility states to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new facility_state:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import FacilityStatesApply
                >>> client = OSDUClient()
                >>> facility_state = FacilityStatesApply(external_id="my_facility_state", ...)
                >>> result = client.facility_states.apply(facility_state)

        """
        return self._apply(facility_state, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more facility state.

        Args:
            external_id: External id of the facility state to delete.
            space: The space where all the facility state are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete facility_state by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.facility_states.delete("my_facility_state")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> FacilityStates:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> FacilityStatesList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> FacilityStates | FacilityStatesList:
        """Retrieve one or more facility states by id(s).

        Args:
            external_id: External id or list of external ids of the facility states.
            space: The space where all the facility states are located.

        Returns:
            The requested facility states.

        Examples:

            Retrieve facility_state by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> facility_state = client.facility_states.retrieve("my_facility_state")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: FacilityStatesTextFields | Sequence[FacilityStatesTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_state_type_id: str | list[str] | None = None,
        facility_state_type_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FacilityStatesList:
        """Search facility states

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_state_type_id: The facility state type id to filter on.
            facility_state_type_id_prefix: The prefix of the facility state type id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility states to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results facility states matching the query.

        Examples:

           Search for 'my_facility_state' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> facility_states = client.facility_states.search('my_facility_state')

        """
        filter_ = _create_facility_state_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_state_type_id,
            facility_state_type_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _FACILITYSTATES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: FacilityStatesFields | Sequence[FacilityStatesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: FacilityStatesTextFields | Sequence[FacilityStatesTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_state_type_id: str | list[str] | None = None,
        facility_state_type_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
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
        property: FacilityStatesFields | Sequence[FacilityStatesFields] | None = None,
        group_by: FacilityStatesFields | Sequence[FacilityStatesFields] = None,
        query: str | None = None,
        search_properties: FacilityStatesTextFields | Sequence[FacilityStatesTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_state_type_id: str | list[str] | None = None,
        facility_state_type_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
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
        property: FacilityStatesFields | Sequence[FacilityStatesFields] | None = None,
        group_by: FacilityStatesFields | Sequence[FacilityStatesFields] | None = None,
        query: str | None = None,
        search_property: FacilityStatesTextFields | Sequence[FacilityStatesTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_state_type_id: str | list[str] | None = None,
        facility_state_type_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across facility states

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_state_type_id: The facility state type id to filter on.
            facility_state_type_id_prefix: The prefix of the facility state type id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility states to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count facility states in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.facility_states.aggregate("count", space="my_space")

        """

        filter_ = _create_facility_state_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_state_type_id,
            facility_state_type_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _FACILITYSTATES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: FacilityStatesFields,
        interval: float,
        query: str | None = None,
        search_property: FacilityStatesTextFields | Sequence[FacilityStatesTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_state_type_id: str | list[str] | None = None,
        facility_state_type_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for facility states

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_state_type_id: The facility state type id to filter on.
            facility_state_type_id_prefix: The prefix of the facility state type id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility states to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_facility_state_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_state_type_id,
            facility_state_type_id_prefix,
            remark,
            remark_prefix,
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
            _FACILITYSTATES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_state_type_id: str | list[str] | None = None,
        facility_state_type_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FacilityStatesList:
        """List/filter facility states

        Args:
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_state_type_id: The facility state type id to filter on.
            facility_state_type_id_prefix: The prefix of the facility state type id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility states to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested facility states

        Examples:

            List facility states and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> facility_states = client.facility_states.list(limit=5)

        """
        filter_ = _create_facility_state_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_state_type_id,
            facility_state_type_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
