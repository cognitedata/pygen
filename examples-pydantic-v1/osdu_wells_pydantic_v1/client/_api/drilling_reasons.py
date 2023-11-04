from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    DrillingReasons,
    DrillingReasonsApply,
    DrillingReasonsList,
    DrillingReasonsApplyList,
    DrillingReasonsFields,
    DrillingReasonsTextFields,
    DomainModelApply,
)
from osdu_wells_pydantic_v1.client.data_classes._drilling_reasons import _DRILLINGREASONS_PROPERTIES_BY_FIELD


class DrillingReasonsAPI(TypeAPI[DrillingReasons, DrillingReasonsApply, DrillingReasonsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[DrillingReasonsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=DrillingReasons,
            class_apply_type=DrillingReasonsApply,
            class_list=DrillingReasonsList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, drilling_reason: DrillingReasonsApply | Sequence[DrillingReasonsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) drilling reasons.

        Args:
            drilling_reason: Drilling reason or sequence of drilling reasons to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new drilling_reason:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import DrillingReasonsApply
                >>> client = OSDUClient()
                >>> drilling_reason = DrillingReasonsApply(external_id="my_drilling_reason", ...)
                >>> result = client.drilling_reasons.apply(drilling_reason)

        """
        if isinstance(drilling_reason, DrillingReasonsApply):
            instances = drilling_reason.to_instances_apply(self._view_by_write_class)
        else:
            instances = DrillingReasonsApplyList(drilling_reason).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more drilling reason.

        Args:
            external_id: External id of the drilling reason to delete.
            space: The space where all the drilling reason are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete drilling_reason by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.drilling_reasons.delete("my_drilling_reason")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DrillingReasons:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DrillingReasonsList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> DrillingReasons | DrillingReasonsList:
        """Retrieve one or more drilling reasons by id(s).

        Args:
            external_id: External id or list of external ids of the drilling reasons.
            space: The space where all the drilling reasons are located.

        Returns:
            The requested drilling reasons.

        Examples:

            Retrieve drilling_reason by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> drilling_reason = client.drilling_reasons.retrieve("my_drilling_reason")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: DrillingReasonsTextFields | Sequence[DrillingReasonsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        lahee_class_id: str | list[str] | None = None,
        lahee_class_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DrillingReasonsList:
        """Search drilling reasons

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            lahee_class_id: The lahee class id to filter on.
            lahee_class_id_prefix: The prefix of the lahee class id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of drilling reasons to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results drilling reasons matching the query.

        Examples:

           Search for 'my_drilling_reason' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> drilling_reasons = client.drilling_reasons.search('my_drilling_reason')

        """
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            lahee_class_id,
            lahee_class_id_prefix,
            remark,
            remark_prefix,
            termination_date_time,
            termination_date_time_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _DRILLINGREASONS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DrillingReasonsFields | Sequence[DrillingReasonsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: DrillingReasonsTextFields | Sequence[DrillingReasonsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        lahee_class_id: str | list[str] | None = None,
        lahee_class_id_prefix: str | None = None,
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
        property: DrillingReasonsFields | Sequence[DrillingReasonsFields] | None = None,
        group_by: DrillingReasonsFields | Sequence[DrillingReasonsFields] = None,
        query: str | None = None,
        search_properties: DrillingReasonsTextFields | Sequence[DrillingReasonsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        lahee_class_id: str | list[str] | None = None,
        lahee_class_id_prefix: str | None = None,
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
        property: DrillingReasonsFields | Sequence[DrillingReasonsFields] | None = None,
        group_by: DrillingReasonsFields | Sequence[DrillingReasonsFields] | None = None,
        query: str | None = None,
        search_property: DrillingReasonsTextFields | Sequence[DrillingReasonsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        lahee_class_id: str | list[str] | None = None,
        lahee_class_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across drilling reasons

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            lahee_class_id: The lahee class id to filter on.
            lahee_class_id_prefix: The prefix of the lahee class id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of drilling reasons to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count drilling reasons in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.drilling_reasons.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            lahee_class_id,
            lahee_class_id_prefix,
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
            _DRILLINGREASONS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: DrillingReasonsFields,
        interval: float,
        query: str | None = None,
        search_property: DrillingReasonsTextFields | Sequence[DrillingReasonsTextFields] | None = None,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        lahee_class_id: str | list[str] | None = None,
        lahee_class_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for drilling reasons

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            lahee_class_id: The lahee class id to filter on.
            lahee_class_id_prefix: The prefix of the lahee class id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of drilling reasons to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            lahee_class_id,
            lahee_class_id_prefix,
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
            _DRILLINGREASONS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        effective_date_time: str | list[str] | None = None,
        effective_date_time_prefix: str | None = None,
        lahee_class_id: str | list[str] | None = None,
        lahee_class_id_prefix: str | None = None,
        remark: str | list[str] | None = None,
        remark_prefix: str | None = None,
        termination_date_time: str | list[str] | None = None,
        termination_date_time_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DrillingReasonsList:
        """List/filter drilling reasons

        Args:
            effective_date_time: The effective date time to filter on.
            effective_date_time_prefix: The prefix of the effective date time to filter on.
            lahee_class_id: The lahee class id to filter on.
            lahee_class_id_prefix: The prefix of the lahee class id to filter on.
            remark: The remark to filter on.
            remark_prefix: The prefix of the remark to filter on.
            termination_date_time: The termination date time to filter on.
            termination_date_time_prefix: The prefix of the termination date time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of drilling reasons to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested drilling reasons

        Examples:

            List drilling reasons and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> drilling_reasons = client.drilling_reasons.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            effective_date_time,
            effective_date_time_prefix,
            lahee_class_id,
            lahee_class_id_prefix,
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
    lahee_class_id: str | list[str] | None = None,
    lahee_class_id_prefix: str | None = None,
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
    if lahee_class_id and isinstance(lahee_class_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("LaheeClassID"), value=lahee_class_id))
    if lahee_class_id and isinstance(lahee_class_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("LaheeClassID"), values=lahee_class_id))
    if lahee_class_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("LaheeClassID"), value=lahee_class_id_prefix))
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
