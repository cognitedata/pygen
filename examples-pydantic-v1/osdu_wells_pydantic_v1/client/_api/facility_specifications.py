from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    FacilitySpecifications,
    FacilitySpecificationsApply,
    FacilitySpecificationsList,
    FacilitySpecificationsApplyList,
    FacilitySpecificationsFields,
    FacilitySpecificationsTextFields,
    DomainModelApply,
)
from osdu_wells_pydantic_v1.client.data_classes._facility_specifications import (
    _FACILITYSPECIFICATIONS_PROPERTIES_BY_FIELD,
)


class FacilitySpecificationsAPI(
    TypeAPI[FacilitySpecifications, FacilitySpecificationsApply, FacilitySpecificationsList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[FacilitySpecificationsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=FacilitySpecifications,
            class_apply_type=FacilitySpecificationsApply,
            class_list=FacilitySpecificationsList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self,
        facility_specification: FacilitySpecificationsApply | Sequence[FacilitySpecificationsApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) facility specifications.

        Args:
            facility_specification: Facility specification or sequence of facility specifications to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new facility_specification:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import FacilitySpecificationsApply
                >>> client = OSDUClient()
                >>> facility_specification = FacilitySpecificationsApply(external_id="my_facility_specification", ...)
                >>> result = client.facility_specifications.apply(facility_specification)

        """
        if isinstance(facility_specification, FacilitySpecificationsApply):
            instances = facility_specification.to_instances_apply(self._view_by_write_class)
        else:
            instances = FacilitySpecificationsApplyList(facility_specification).to_instances_apply(
                self._view_by_write_class
            )
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
        """Delete one or more facility specification.

        Args:
            external_id: External id of the facility specification to delete.
            space: The space where all the facility specification are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete facility_specification by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.facility_specifications.delete("my_facility_specification")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> FacilitySpecifications:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> FacilitySpecificationsList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> FacilitySpecifications | FacilitySpecificationsList:
        """Retrieve one or more facility specifications by id(s).

        Args:
            external_id: External id or list of external ids of the facility specifications.
            space: The space where all the facility specifications are located.

        Returns:
            The requested facility specifications.

        Examples:

            Retrieve facility_specification by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> facility_specification = client.facility_specifications.retrieve("my_facility_specification")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: FacilitySpecificationsTextFields | Sequence[FacilitySpecificationsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_specification_date_time: str | list[str] | None = None,
        facility_specification_date_time_prefix: str | None = None,
        facility_specification_indicator: bool | None = None,
        min_facility_specification_quantity: float | None = None,
        max_facility_specification_quantity: float | None = None,
        facility_specification_text: str | list[str] | None = None,
        facility_specification_text_prefix: str | None = None,
        parameter_type_id: str | list[str] | None = None,
        parameter_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FacilitySpecificationsList:
        """Search facility specifications

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_specification_date_time: The facility specification date time to filter on.
            facility_specification_date_time_prefix: The prefix of the facility specification date time to filter on.
            facility_specification_indicator: The facility specification indicator to filter on.
            min_facility_specification_quantity: The minimum value of the facility specification quantity to filter on.
            max_facility_specification_quantity: The maximum value of the facility specification quantity to filter on.
            facility_specification_text: The facility specification text to filter on.
            facility_specification_text_prefix: The prefix of the facility specification text to filter on.
            parameter_type_id: The parameter type id to filter on.
            parameter_type_id_prefix: The prefix of the parameter type id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            unit_of_measure_id: The unit of measure id to filter on.
            unit_of_measure_id_prefix: The prefix of the unit of measure id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility specifications to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results facility specifications matching the query.

        Examples:

           Search for 'my_facility_specification' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> facility_specifications = client.facility_specifications.search('my_facility_specification')

        """
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_specification_date_time,
            facility_specification_date_time_prefix,
            facility_specification_indicator,
            min_facility_specification_quantity,
            max_facility_specification_quantity,
            facility_specification_text,
            facility_specification_text_prefix,
            parameter_type_id,
            parameter_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _FACILITYSPECIFICATIONS_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: FacilitySpecificationsFields | Sequence[FacilitySpecificationsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: FacilitySpecificationsTextFields | Sequence[FacilitySpecificationsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_specification_date_time: str | list[str] | None = None,
        facility_specification_date_time_prefix: str | None = None,
        facility_specification_indicator: bool | None = None,
        min_facility_specification_quantity: float | None = None,
        max_facility_specification_quantity: float | None = None,
        facility_specification_text: str | list[str] | None = None,
        facility_specification_text_prefix: str | None = None,
        parameter_type_id: str | list[str] | None = None,
        parameter_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
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
        property: FacilitySpecificationsFields | Sequence[FacilitySpecificationsFields] | None = None,
        group_by: FacilitySpecificationsFields | Sequence[FacilitySpecificationsFields] = None,
        query: str | None = None,
        search_properties: FacilitySpecificationsTextFields | Sequence[FacilitySpecificationsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_specification_date_time: str | list[str] | None = None,
        facility_specification_date_time_prefix: str | None = None,
        facility_specification_indicator: bool | None = None,
        min_facility_specification_quantity: float | None = None,
        max_facility_specification_quantity: float | None = None,
        facility_specification_text: str | list[str] | None = None,
        facility_specification_text_prefix: str | None = None,
        parameter_type_id: str | list[str] | None = None,
        parameter_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
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
        property: FacilitySpecificationsFields | Sequence[FacilitySpecificationsFields] | None = None,
        group_by: FacilitySpecificationsFields | Sequence[FacilitySpecificationsFields] | None = None,
        query: str | None = None,
        search_property: FacilitySpecificationsTextFields | Sequence[FacilitySpecificationsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_specification_date_time: str | list[str] | None = None,
        facility_specification_date_time_prefix: str | None = None,
        facility_specification_indicator: bool | None = None,
        min_facility_specification_quantity: float | None = None,
        max_facility_specification_quantity: float | None = None,
        facility_specification_text: str | list[str] | None = None,
        facility_specification_text_prefix: str | None = None,
        parameter_type_id: str | list[str] | None = None,
        parameter_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across facility specifications

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_specification_date_time: The facility specification date time to filter on.
            facility_specification_date_time_prefix: The prefix of the facility specification date time to filter on.
            facility_specification_indicator: The facility specification indicator to filter on.
            min_facility_specification_quantity: The minimum value of the facility specification quantity to filter on.
            max_facility_specification_quantity: The maximum value of the facility specification quantity to filter on.
            facility_specification_text: The facility specification text to filter on.
            facility_specification_text_prefix: The prefix of the facility specification text to filter on.
            parameter_type_id: The parameter type id to filter on.
            parameter_type_id_prefix: The prefix of the parameter type id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            unit_of_measure_id: The unit of measure id to filter on.
            unit_of_measure_id_prefix: The prefix of the unit of measure id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility specifications to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count facility specifications in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.facility_specifications.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_specification_date_time,
            facility_specification_date_time_prefix,
            facility_specification_indicator,
            min_facility_specification_quantity,
            max_facility_specification_quantity,
            facility_specification_text,
            facility_specification_text_prefix,
            parameter_type_id,
            parameter_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _FACILITYSPECIFICATIONS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: FacilitySpecificationsFields,
        interval: float,
        query: str | None = None,
        search_property: FacilitySpecificationsTextFields | Sequence[FacilitySpecificationsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_specification_date_time: str | list[str] | None = None,
        facility_specification_date_time_prefix: str | None = None,
        facility_specification_indicator: bool | None = None,
        min_facility_specification_quantity: float | None = None,
        max_facility_specification_quantity: float | None = None,
        facility_specification_text: str | list[str] | None = None,
        facility_specification_text_prefix: str | None = None,
        parameter_type_id: str | list[str] | None = None,
        parameter_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for facility specifications

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_specification_date_time: The facility specification date time to filter on.
            facility_specification_date_time_prefix: The prefix of the facility specification date time to filter on.
            facility_specification_indicator: The facility specification indicator to filter on.
            min_facility_specification_quantity: The minimum value of the facility specification quantity to filter on.
            max_facility_specification_quantity: The maximum value of the facility specification quantity to filter on.
            facility_specification_text: The facility specification text to filter on.
            facility_specification_text_prefix: The prefix of the facility specification text to filter on.
            parameter_type_id: The parameter type id to filter on.
            parameter_type_id_prefix: The prefix of the parameter type id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            unit_of_measure_id: The unit of measure id to filter on.
            unit_of_measure_id_prefix: The prefix of the unit of measure id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility specifications to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_specification_date_time,
            facility_specification_date_time_prefix,
            facility_specification_indicator,
            min_facility_specification_quantity,
            max_facility_specification_quantity,
            facility_specification_text,
            facility_specification_text_prefix,
            parameter_type_id,
            parameter_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _FACILITYSPECIFICATIONS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        facility_specification_date_time: str | list[str] | None = None,
        facility_specification_date_time_prefix: str | None = None,
        facility_specification_indicator: bool | None = None,
        min_facility_specification_quantity: float | None = None,
        max_facility_specification_quantity: float | None = None,
        facility_specification_text: str | list[str] | None = None,
        facility_specification_text_prefix: str | None = None,
        parameter_type_id: str | list[str] | None = None,
        parameter_type_id_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        unit_of_measure_id: str | list[str] | None = None,
        unit_of_measure_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FacilitySpecificationsList:
        """List/filter facility specifications

        Args:
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            facility_specification_date_time: The facility specification date time to filter on.
            facility_specification_date_time_prefix: The prefix of the facility specification date time to filter on.
            facility_specification_indicator: The facility specification indicator to filter on.
            min_facility_specification_quantity: The minimum value of the facility specification quantity to filter on.
            max_facility_specification_quantity: The maximum value of the facility specification quantity to filter on.
            facility_specification_text: The facility specification text to filter on.
            facility_specification_text_prefix: The prefix of the facility specification text to filter on.
            parameter_type_id: The parameter type id to filter on.
            parameter_type_id_prefix: The prefix of the parameter type id to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            unit_of_measure_id: The unit of measure id to filter on.
            unit_of_measure_id_prefix: The prefix of the unit of measure id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of facility specifications to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested facility specifications

        Examples:

            List facility specifications and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> facility_specifications = client.facility_specifications.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            facility_specification_date_time,
            facility_specification_date_time_prefix,
            facility_specification_indicator,
            min_facility_specification_quantity,
            max_facility_specification_quantity,
            facility_specification_text,
            facility_specification_text_prefix,
            parameter_type_id,
            parameter_type_id_prefix,
            termination_date_time,
            termination_date_time_prefix,
            unit_of_measure_id,
            unit_of_measure_id_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    facility_specification_date_time: str | list[str] | None = None,
    facility_specification_date_time_prefix: str | None = None,
    facility_specification_indicator: bool | None = None,
    min_facility_specification_quantity: float | None = None,
    max_facility_specification_quantity: float | None = None,
    facility_specification_text: str | list[str] | None = None,
    facility_specification_text_prefix: str | None = None,
    parameter_type_id: str | list[str] | None = None,
    parameter_type_id_prefix: str | None = None,
    termination_date_time: str | list[str] | None = None,
    termination_date_time_prefix: str | None = None,
    unit_of_measure_id: str | list[str] | None = None,
    unit_of_measure_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if effective_date_time and isinstance(effective_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time))
    if effective_date_time and isinstance(effective_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EffectiveDateTime"), values=effective_date_time))
    if effective_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time_prefix)
        )
    if facility_specification_date_time and isinstance(facility_specification_date_time, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("FacilitySpecificationDateTime"), value=facility_specification_date_time
            )
        )
    if facility_specification_date_time and isinstance(facility_specification_date_time, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("FacilitySpecificationDateTime"), values=facility_specification_date_time
            )
        )
    if facility_specification_date_time_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("FacilitySpecificationDateTime"), value=facility_specification_date_time_prefix
            )
        )
    if facility_specification_indicator and isinstance(facility_specification_indicator, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("FacilitySpecificationIndicator"), value=facility_specification_indicator
            )
        )
    if min_facility_specification_quantity or max_facility_specification_quantity:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("FacilitySpecificationQuantity"),
                gte=min_facility_specification_quantity,
                lte=max_facility_specification_quantity,
            )
        )
    if facility_specification_text and isinstance(facility_specification_text, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("FacilitySpecificationText"), value=facility_specification_text)
        )
    if facility_specification_text and isinstance(facility_specification_text, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("FacilitySpecificationText"), values=facility_specification_text)
        )
    if facility_specification_text_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("FacilitySpecificationText"), value=facility_specification_text_prefix
            )
        )
    if parameter_type_id and isinstance(parameter_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ParameterTypeID"), value=parameter_type_id))
    if parameter_type_id and isinstance(parameter_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ParameterTypeID"), values=parameter_type_id))
    if parameter_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ParameterTypeID"), value=parameter_type_id_prefix))
    if termination_date_time and isinstance(termination_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time))
    if termination_date_time and isinstance(termination_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TerminationDateTime"), values=termination_date_time))
    if termination_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time_prefix)
        )
    if unit_of_measure_id and isinstance(unit_of_measure_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("UnitOfMeasureID"), value=unit_of_measure_id))
    if unit_of_measure_id and isinstance(unit_of_measure_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("UnitOfMeasureID"), values=unit_of_measure_id))
    if unit_of_measure_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("UnitOfMeasureID"), value=unit_of_measure_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
