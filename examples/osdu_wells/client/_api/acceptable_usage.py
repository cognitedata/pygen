from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    AcceptableUsage,
    AcceptableUsageApply,
    AcceptableUsageList,
    AcceptableUsageApplyList,
    AcceptableUsageFields,
    AcceptableUsageTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._acceptable_usage import _ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD


class AcceptableUsageAPI(TypeAPI[AcceptableUsage, AcceptableUsageApply, AcceptableUsageList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[AcceptableUsageApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=AcceptableUsage,
            class_apply_type=AcceptableUsageApply,
            class_list=AcceptableUsageList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, acceptable_usage: AcceptableUsageApply | Sequence[AcceptableUsageApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) acceptable usages.

        Args:
            acceptable_usage: Acceptable usage or sequence of acceptable usages to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new acceptable_usage:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import AcceptableUsageApply
                >>> client = OSDUClient()
                >>> acceptable_usage = AcceptableUsageApply(external_id="my_acceptable_usage", ...)
                >>> result = client.acceptable_usage.apply(acceptable_usage)

        """
        if isinstance(acceptable_usage, AcceptableUsageApply):
            instances = acceptable_usage.to_instances_apply(self._view_by_write_class)
        else:
            instances = AcceptableUsageApplyList(acceptable_usage).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more acceptable usage.

        Args:
            external_id: External id of the acceptable usage to delete.
            space: The space where all the acceptable usage are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete acceptable_usage by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.acceptable_usage.delete("my_acceptable_usage")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> AcceptableUsage:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> AcceptableUsageList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> AcceptableUsage | AcceptableUsageList:
        """Retrieve one or more acceptable usages by id(s).

        Args:
            external_id: External id or list of external ids of the acceptable usages.
            space: The space where all the acceptable usages are located.

        Returns:
            The requested acceptable usages.

        Examples:

            Retrieve acceptable_usage by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> acceptable_usage = client.acceptable_usage.retrieve("my_acceptable_usage")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: AcceptableUsageTextFields | Sequence[AcceptableUsageTextFields] | None = None,
        data_quality_id: str | list[str] | None = None,
        data_quality_id_prefix: str | None = None,
        data_quality_rule_set_id: str | list[str] | None = None,
        data_quality_rule_set_id_prefix: str | None = None,
        value_chain_status_type_id: str | list[str] | None = None,
        value_chain_status_type_id_prefix: str | None = None,
        workflow_persona_type_id: str | list[str] | None = None,
        workflow_persona_type_id_prefix: str | None = None,
        workflow_usage_type_id: str | list[str] | None = None,
        workflow_usage_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AcceptableUsageList:
        """Search acceptable usages

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            data_quality_id: The data quality id to filter on.
            data_quality_id_prefix: The prefix of the data quality id to filter on.
            data_quality_rule_set_id: The data quality rule set id to filter on.
            data_quality_rule_set_id_prefix: The prefix of the data quality rule set id to filter on.
            value_chain_status_type_id: The value chain status type id to filter on.
            value_chain_status_type_id_prefix: The prefix of the value chain status type id to filter on.
            workflow_persona_type_id: The workflow persona type id to filter on.
            workflow_persona_type_id_prefix: The prefix of the workflow persona type id to filter on.
            workflow_usage_type_id: The workflow usage type id to filter on.
            workflow_usage_type_id_prefix: The prefix of the workflow usage type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of acceptable usages to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results acceptable usages matching the query.

        Examples:

           Search for 'my_acceptable_usage' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> acceptable_usages = client.acceptable_usage.search('my_acceptable_usage')

        """
        filter_ = _create_filter(
            self._view_id,
            data_quality_id,
            data_quality_id_prefix,
            data_quality_rule_set_id,
            data_quality_rule_set_id_prefix,
            value_chain_status_type_id,
            value_chain_status_type_id_prefix,
            workflow_persona_type_id,
            workflow_persona_type_id_prefix,
            workflow_usage_type_id,
            workflow_usage_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AcceptableUsageFields | Sequence[AcceptableUsageFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: AcceptableUsageTextFields | Sequence[AcceptableUsageTextFields] | None = None,
        data_quality_id: str | list[str] | None = None,
        data_quality_id_prefix: str | None = None,
        data_quality_rule_set_id: str | list[str] | None = None,
        data_quality_rule_set_id_prefix: str | None = None,
        value_chain_status_type_id: str | list[str] | None = None,
        value_chain_status_type_id_prefix: str | None = None,
        workflow_persona_type_id: str | list[str] | None = None,
        workflow_persona_type_id_prefix: str | None = None,
        workflow_usage_type_id: str | list[str] | None = None,
        workflow_usage_type_id_prefix: str | None = None,
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
        property: AcceptableUsageFields | Sequence[AcceptableUsageFields] | None = None,
        group_by: AcceptableUsageFields | Sequence[AcceptableUsageFields] = None,
        query: str | None = None,
        search_properties: AcceptableUsageTextFields | Sequence[AcceptableUsageTextFields] | None = None,
        data_quality_id: str | list[str] | None = None,
        data_quality_id_prefix: str | None = None,
        data_quality_rule_set_id: str | list[str] | None = None,
        data_quality_rule_set_id_prefix: str | None = None,
        value_chain_status_type_id: str | list[str] | None = None,
        value_chain_status_type_id_prefix: str | None = None,
        workflow_persona_type_id: str | list[str] | None = None,
        workflow_persona_type_id_prefix: str | None = None,
        workflow_usage_type_id: str | list[str] | None = None,
        workflow_usage_type_id_prefix: str | None = None,
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
        property: AcceptableUsageFields | Sequence[AcceptableUsageFields] | None = None,
        group_by: AcceptableUsageFields | Sequence[AcceptableUsageFields] | None = None,
        query: str | None = None,
        search_property: AcceptableUsageTextFields | Sequence[AcceptableUsageTextFields] | None = None,
        data_quality_id: str | list[str] | None = None,
        data_quality_id_prefix: str | None = None,
        data_quality_rule_set_id: str | list[str] | None = None,
        data_quality_rule_set_id_prefix: str | None = None,
        value_chain_status_type_id: str | list[str] | None = None,
        value_chain_status_type_id_prefix: str | None = None,
        workflow_persona_type_id: str | list[str] | None = None,
        workflow_persona_type_id_prefix: str | None = None,
        workflow_usage_type_id: str | list[str] | None = None,
        workflow_usage_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            data_quality_id,
            data_quality_id_prefix,
            data_quality_rule_set_id,
            data_quality_rule_set_id_prefix,
            value_chain_status_type_id,
            value_chain_status_type_id_prefix,
            workflow_persona_type_id,
            workflow_persona_type_id_prefix,
            workflow_usage_type_id,
            workflow_usage_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: AcceptableUsageFields,
        interval: float,
        query: str | None = None,
        search_property: AcceptableUsageTextFields | Sequence[AcceptableUsageTextFields] | None = None,
        data_quality_id: str | list[str] | None = None,
        data_quality_id_prefix: str | None = None,
        data_quality_rule_set_id: str | list[str] | None = None,
        data_quality_rule_set_id_prefix: str | None = None,
        value_chain_status_type_id: str | list[str] | None = None,
        value_chain_status_type_id_prefix: str | None = None,
        workflow_persona_type_id: str | list[str] | None = None,
        workflow_persona_type_id_prefix: str | None = None,
        workflow_usage_type_id: str | list[str] | None = None,
        workflow_usage_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            data_quality_id,
            data_quality_id_prefix,
            data_quality_rule_set_id,
            data_quality_rule_set_id_prefix,
            value_chain_status_type_id,
            value_chain_status_type_id_prefix,
            workflow_persona_type_id,
            workflow_persona_type_id_prefix,
            workflow_usage_type_id,
            workflow_usage_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        data_quality_id: str | list[str] | None = None,
        data_quality_id_prefix: str | None = None,
        data_quality_rule_set_id: str | list[str] | None = None,
        data_quality_rule_set_id_prefix: str | None = None,
        value_chain_status_type_id: str | list[str] | None = None,
        value_chain_status_type_id_prefix: str | None = None,
        workflow_persona_type_id: str | list[str] | None = None,
        workflow_persona_type_id_prefix: str | None = None,
        workflow_usage_type_id: str | list[str] | None = None,
        workflow_usage_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AcceptableUsageList:
        """List/filter acceptable usages

        Args:
            data_quality_id: The data quality id to filter on.
            data_quality_id_prefix: The prefix of the data quality id to filter on.
            data_quality_rule_set_id: The data quality rule set id to filter on.
            data_quality_rule_set_id_prefix: The prefix of the data quality rule set id to filter on.
            value_chain_status_type_id: The value chain status type id to filter on.
            value_chain_status_type_id_prefix: The prefix of the value chain status type id to filter on.
            workflow_persona_type_id: The workflow persona type id to filter on.
            workflow_persona_type_id_prefix: The prefix of the workflow persona type id to filter on.
            workflow_usage_type_id: The workflow usage type id to filter on.
            workflow_usage_type_id_prefix: The prefix of the workflow usage type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of acceptable usages to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested acceptable usages

        Examples:

            List acceptable usages and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> acceptable_usages = client.acceptable_usage.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            data_quality_id,
            data_quality_id_prefix,
            data_quality_rule_set_id,
            data_quality_rule_set_id_prefix,
            value_chain_status_type_id,
            value_chain_status_type_id_prefix,
            workflow_persona_type_id,
            workflow_persona_type_id_prefix,
            workflow_usage_type_id,
            workflow_usage_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    data_quality_id: str | list[str] | None = None,
    data_quality_id_prefix: str | None = None,
    data_quality_rule_set_id: str | list[str] | None = None,
    data_quality_rule_set_id_prefix: str | None = None,
    value_chain_status_type_id: str | list[str] | None = None,
    value_chain_status_type_id_prefix: str | None = None,
    workflow_persona_type_id: str | list[str] | None = None,
    workflow_persona_type_id_prefix: str | None = None,
    workflow_usage_type_id: str | list[str] | None = None,
    workflow_usage_type_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if data_quality_id and isinstance(data_quality_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("DataQualityID"), value=data_quality_id))
    if data_quality_id and isinstance(data_quality_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("DataQualityID"), values=data_quality_id))
    if data_quality_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("DataQualityID"), value=data_quality_id_prefix))
    if data_quality_rule_set_id and isinstance(data_quality_rule_set_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("DataQualityRuleSetID"), value=data_quality_rule_set_id)
        )
    if data_quality_rule_set_id and isinstance(data_quality_rule_set_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("DataQualityRuleSetID"), values=data_quality_rule_set_id))
    if data_quality_rule_set_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("DataQualityRuleSetID"), value=data_quality_rule_set_id_prefix)
        )
    if value_chain_status_type_id and isinstance(value_chain_status_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("ValueChainStatusTypeID"), value=value_chain_status_type_id)
        )
    if value_chain_status_type_id and isinstance(value_chain_status_type_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("ValueChainStatusTypeID"), values=value_chain_status_type_id)
        )
    if value_chain_status_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("ValueChainStatusTypeID"), value=value_chain_status_type_id_prefix
            )
        )
    if workflow_persona_type_id and isinstance(workflow_persona_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("WorkflowPersonaTypeID"), value=workflow_persona_type_id)
        )
    if workflow_persona_type_id and isinstance(workflow_persona_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("WorkflowPersonaTypeID"), values=workflow_persona_type_id))
    if workflow_persona_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("WorkflowPersonaTypeID"), value=workflow_persona_type_id_prefix)
        )
    if workflow_usage_type_id and isinstance(workflow_usage_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("WorkflowUsageTypeID"), value=workflow_usage_type_id))
    if workflow_usage_type_id and isinstance(workflow_usage_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("WorkflowUsageTypeID"), values=workflow_usage_type_id))
    if workflow_usage_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("WorkflowUsageTypeID"), value=workflow_usage_type_id_prefix)
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
