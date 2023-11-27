from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    FacilityOperators,
    FacilityOperatorsApply,
    FacilityOperatorsFields,
    FacilityOperatorsList,
    FacilityOperatorsTextFields,
)
from osdu_wells.client.data_classes._facility_operators import (
    _FACILITYOPERATORS_PROPERTIES_BY_FIELD,
    _create_facility_operator_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .facility_operators_query import FacilityOperatorsQueryAPI


class FacilityOperatorsAPI(NodeAPI[FacilityOperators, FacilityOperatorsApply, FacilityOperatorsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[FacilityOperatorsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=FacilityOperators,
            class_apply_type=FacilityOperatorsApply,
            class_list=FacilityOperatorsList,
            class_apply_list=FacilityOperatorsApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_operator_id: str | list[str] | None = None,
        facility_operator_id_prefix: str | None = None,
        facility_operator_organisation_id: str | list[str] | None = None,
        facility_operator_organisation_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FacilityOperatorsQueryAPI[FacilityOperatorsList]:
        """Query starting at facility operators.

        Args:
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_operator_id: The facility operator id to filter on.
            facility_operator_id_prefix: The prefix of the facility operator id to filter on.
            facility_operator_organisation_id: The facility operator organisation id to filter on.
            facility_operator_organisation_id_prefix: The prefix of the facility operator organisation id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility operators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for facility operators.

        """
        filter_ = _create_facility_operator_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_operator_id,
            facility_operator_id_prefix,
            facility_operator_organisation_id,
            facility_operator_organisation_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            FacilityOperatorsList,
            [
                QueryStep(
                    name="facility_operator",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_FACILITYOPERATORS_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=FacilityOperators,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return FacilityOperatorsQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, facility_operator: FacilityOperatorsApply | Sequence[FacilityOperatorsApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) facility operators.

        Args:
            facility_operator: Facility operator or sequence of facility operators to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new facility_operator:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import FacilityOperatorsApply
                >>> client = OSDUClient()
                >>> facility_operator = FacilityOperatorsApply(external_id="my_facility_operator", ...)
                >>> result = client.facility_operators.apply(facility_operator)

        """
        return self._apply(facility_operator, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more facility operator.

        Args:
            external_id: External id of the facility operator to delete.
            space: The space where all the facility operator are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete facility_operator by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.facility_operators.delete("my_facility_operator")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> FacilityOperators:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> FacilityOperatorsList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> FacilityOperators | FacilityOperatorsList:
        """Retrieve one or more facility operators by id(s).

        Args:
            external_id: External id or list of external ids of the facility operators.
            space: The space where all the facility operators are located.

        Returns:
            The requested facility operators.

        Examples:

            Retrieve facility_operator by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> facility_operator = client.facility_operators.retrieve("my_facility_operator")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: FacilityOperatorsTextFields | Sequence[FacilityOperatorsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_operator_id: str | list[str] | None = None,
        facility_operator_id_prefix: str | None = None,
        facility_operator_organisation_id: str | list[str] | None = None,
        facility_operator_organisation_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FacilityOperatorsList:
        """Search facility operators

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_operator_id: The facility operator id to filter on.
            facility_operator_id_prefix: The prefix of the facility operator id to filter on.
            facility_operator_organisation_id: The facility operator organisation id to filter on.
            facility_operator_organisation_id_prefix: The prefix of the facility operator organisation id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility operators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results facility operators matching the query.

        Examples:

           Search for 'my_facility_operator' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> facility_operators = client.facility_operators.search('my_facility_operator')

        """
        filter_ = _create_facility_operator_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_operator_id,
            facility_operator_id_prefix,
            facility_operator_organisation_id,
            facility_operator_organisation_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _FACILITYOPERATORS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: FacilityOperatorsFields | Sequence[FacilityOperatorsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: FacilityOperatorsTextFields | Sequence[FacilityOperatorsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_operator_id: str | list[str] | None = None,
        facility_operator_id_prefix: str | None = None,
        facility_operator_organisation_id: str | list[str] | None = None,
        facility_operator_organisation_id_prefix: str | None = None,
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
        property: FacilityOperatorsFields | Sequence[FacilityOperatorsFields] | None = None,
        group_by: FacilityOperatorsFields | Sequence[FacilityOperatorsFields] = None,
        query: str | None = None,
        search_properties: FacilityOperatorsTextFields | Sequence[FacilityOperatorsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_operator_id: str | list[str] | None = None,
        facility_operator_id_prefix: str | None = None,
        facility_operator_organisation_id: str | list[str] | None = None,
        facility_operator_organisation_id_prefix: str | None = None,
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
        property: FacilityOperatorsFields | Sequence[FacilityOperatorsFields] | None = None,
        group_by: FacilityOperatorsFields | Sequence[FacilityOperatorsFields] | None = None,
        query: str | None = None,
        search_property: FacilityOperatorsTextFields | Sequence[FacilityOperatorsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_operator_id: str | list[str] | None = None,
        facility_operator_id_prefix: str | None = None,
        facility_operator_organisation_id: str | list[str] | None = None,
        facility_operator_organisation_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across facility operators

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_operator_id: The facility operator id to filter on.
            facility_operator_id_prefix: The prefix of the facility operator id to filter on.
            facility_operator_organisation_id: The facility operator organisation id to filter on.
            facility_operator_organisation_id_prefix: The prefix of the facility operator organisation id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility operators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count facility operators in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.facility_operators.aggregate("count", space="my_space")

        """

        filter_ = _create_facility_operator_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_operator_id,
            facility_operator_id_prefix,
            facility_operator_organisation_id,
            facility_operator_organisation_id_prefix,
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
            _FACILITYOPERATORS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: FacilityOperatorsFields,
        interval: float,
        query: str | None = None,
        search_property: FacilityOperatorsTextFields | Sequence[FacilityOperatorsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_operator_id: str | list[str] | None = None,
        facility_operator_id_prefix: str | None = None,
        facility_operator_organisation_id: str | list[str] | None = None,
        facility_operator_organisation_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for facility operators

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_operator_id: The facility operator id to filter on.
            facility_operator_id_prefix: The prefix of the facility operator id to filter on.
            facility_operator_organisation_id: The facility operator organisation id to filter on.
            facility_operator_organisation_id_prefix: The prefix of the facility operator organisation id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility operators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_facility_operator_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_operator_id,
            facility_operator_id_prefix,
            facility_operator_organisation_id,
            facility_operator_organisation_id_prefix,
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
            _FACILITYOPERATORS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_operator_id: str | list[str] | None = None,
        facility_operator_id_prefix: str | None = None,
        facility_operator_organisation_id: str | list[str] | None = None,
        facility_operator_organisation_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FacilityOperatorsList:
        """List/filter facility operators

        Args:
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_operator_id: The facility operator id to filter on.
            facility_operator_id_prefix: The prefix of the facility operator id to filter on.
            facility_operator_organisation_id: The facility operator organisation id to filter on.
            facility_operator_organisation_id_prefix: The prefix of the facility operator organisation id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility operators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested facility operators

        Examples:

            List facility operators and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> facility_operators = client.facility_operators.list(limit=5)

        """
        filter_ = _create_facility_operator_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_operator_id,
            facility_operator_id_prefix,
            facility_operator_organisation_id,
            facility_operator_organisation_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
