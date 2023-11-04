from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    FacilityStates,
    FacilityStatesApply,
    FacilityStatesList,
    FacilityStatesApplyList,
    FacilityStatesFields,
    FacilityStatesTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._facility_states import _FACILITYSTATES_PROPERTIES_BY_FIELD


class FacilityStatesAPI(TypeAPI[FacilityStates, FacilityStatesApply, FacilityStatesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[FacilityStatesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=FacilityStates,
            class_apply_type=FacilityStatesApply,
            class_list=FacilityStatesList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, facility_state: FacilityStatesApply | Sequence[FacilityStatesApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) facility states.

        Args:
            facility_state: Facility state or sequence of facility states to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new facility_state:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import FacilityStatesApply
                >>> client = OSDUClient()
                >>> facility_state = FacilityStatesApply(external_id="my_facility_state", ...)
                >>> result = client.facility_states.apply(facility_state)

        """
        if isinstance(facility_state, FacilityStatesApply):
            instances = facility_state.to_instances_apply(self._view_by_write_class)
        else:
            instances = FacilityStatesApplyList(facility_state).to_instances_apply(self._view_by_write_class)
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
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> FacilityStates:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> FacilityStatesList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
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
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

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
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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


def _create_filter(
    view_id: dm.ViewId,
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
    if facility_state_type_id and isinstance(facility_state_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityStateTypeID"), value=facility_state_type_id))
    if facility_state_type_id and isinstance(facility_state_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityStateTypeID"), values=facility_state_type_id))
    if facility_state_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("FacilityStateTypeID"), value=facility_state_type_id_prefix)
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
