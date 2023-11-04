from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    HistoricalInterests,
    HistoricalInterestsApply,
    HistoricalInterestsList,
    HistoricalInterestsApplyList,
    HistoricalInterestsFields,
    HistoricalInterestsTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._historical_interests import _HISTORICALINTERESTS_PROPERTIES_BY_FIELD


class HistoricalInterestsAPI(TypeAPI[HistoricalInterests, HistoricalInterestsApply, HistoricalInterestsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[HistoricalInterestsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=HistoricalInterests,
            class_apply_type=HistoricalInterestsApply,
            class_list=HistoricalInterestsList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, historical_interest: HistoricalInterestsApply | Sequence[HistoricalInterestsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) historical interests.

        Args:
            historical_interest: Historical interest or sequence of historical interests to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new historical_interest:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import HistoricalInterestsApply
                >>> client = OSDUClient()
                >>> historical_interest = HistoricalInterestsApply(external_id="my_historical_interest", ...)
                >>> result = client.historical_interests.apply(historical_interest)

        """
        if isinstance(historical_interest, HistoricalInterestsApply):
            instances = historical_interest.to_instances_apply(self._view_by_write_class)
        else:
            instances = HistoricalInterestsApplyList(historical_interest).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more historical interest.

        Args:
            external_id: External id of the historical interest to delete.
            space: The space where all the historical interest are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete historical_interest by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.historical_interests.delete("my_historical_interest")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> HistoricalInterests:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> HistoricalInterestsList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> HistoricalInterests | HistoricalInterestsList:
        """Retrieve one or more historical interests by id(s).

        Args:
            external_id: External id or list of external ids of the historical interests.
            space: The space where all the historical interests are located.

        Returns:
            The requested historical interests.

        Examples:

            Retrieve historical_interest by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> historical_interest = client.historical_interests.retrieve("my_historical_interest")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

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
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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


def _create_filter(
    view_id: dm.ViewId,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    interest_type_id: str | list[str] | None = None,
    interest_type_id_prefix: str | None = None,
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
    if interest_type_id and isinstance(interest_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("InterestTypeID"), value=interest_type_id))
    if interest_type_id and isinstance(interest_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("InterestTypeID"), values=interest_type_id))
    if interest_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("InterestTypeID"), value=interest_type_id_prefix))
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
