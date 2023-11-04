from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    FacilityOperators,
    FacilityOperatorsApply,
    FacilityOperatorsList,
    FacilityOperatorsApplyList,
    FacilityOperatorsFields,
    FacilityOperatorsTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._facility_operators import _FACILITYOPERATORS_PROPERTIES_BY_FIELD


class FacilityOperatorsAPI(TypeAPI[FacilityOperators, FacilityOperatorsApply, FacilityOperatorsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[FacilityOperatorsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=FacilityOperators,
            class_apply_type=FacilityOperatorsApply,
            class_list=FacilityOperatorsList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, facility_operator: FacilityOperatorsApply | Sequence[FacilityOperatorsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) facility operators.

        Args:
            facility_operator: Facility operator or sequence of facility operators to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new facility_operator:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import FacilityOperatorsApply
                >>> client = OSDUClient()
                >>> facility_operator = FacilityOperatorsApply(external_id="my_facility_operator", ...)
                >>> result = client.facility_operators.apply(facility_operator)

        """
        if isinstance(facility_operator, FacilityOperatorsApply):
            instances = facility_operator.to_instances_apply(self._view_by_write_class)
        else:
            instances = FacilityOperatorsApplyList(facility_operator).to_instances_apply(self._view_by_write_class)
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
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> FacilityOperators:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> FacilityOperatorsList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
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
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

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
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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


def _create_filter(
    view_id: dm.ViewId,
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
    if facility_operator_id and isinstance(facility_operator_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityOperatorID"), value=facility_operator_id))
    if facility_operator_id and isinstance(facility_operator_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityOperatorID"), values=facility_operator_id))
    if facility_operator_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("FacilityOperatorID"), value=facility_operator_id_prefix)
        )
    if facility_operator_organisation_id and isinstance(facility_operator_organisation_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("FacilityOperatorOrganisationID"), value=facility_operator_organisation_id
            )
        )
    if facility_operator_organisation_id and isinstance(facility_operator_organisation_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("FacilityOperatorOrganisationID"), values=facility_operator_organisation_id
            )
        )
    if facility_operator_organisation_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("FacilityOperatorOrganisationID"),
                value=facility_operator_organisation_id_prefix,
            )
        )
    if remark and isinstance(remark, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Remark"), value=remark))
    if remark and isinstance(remark, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Remark"), values=remark))
    if remark_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Remark"), value=remark_prefix))
    if termination_date_time and isinstance(termination_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time))
    if termination_date_time and isinstance(termination_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TerminationDateTime"), values=termination_date_time))
    if termination_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time_prefix)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
