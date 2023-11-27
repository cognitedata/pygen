from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    UnacceptableUsage,
    UnacceptableUsageApply,
    UnacceptableUsageFields,
    UnacceptableUsageList,
    UnacceptableUsageApplyList,
    UnacceptableUsageTextFields,
)
from osdu_wells.client.data_classes._unacceptable_usage import (
    _UNACCEPTABLEUSAGE_PROPERTIES_BY_FIELD,
    _create_unacceptable_usage_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .unacceptable_usage_query import UnacceptableUsageQueryAPI


class UnacceptableUsageAPI(NodeAPI[UnacceptableUsage, UnacceptableUsageApply, UnacceptableUsageList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[UnacceptableUsageApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=UnacceptableUsage,
            class_apply_type=UnacceptableUsageApply,
            class_list=UnacceptableUsageList,
            class_apply_list=UnacceptableUsageApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
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
    ) -> UnacceptableUsageQueryAPI[UnacceptableUsageList]:
        """Query starting at unacceptable usages.

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
            limit: Maximum number of unacceptable usages to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for unacceptable usages.

        """
        filter_ = _create_unacceptable_usage_filter(
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
        builder = QueryBuilder(
            UnacceptableUsageList,
            [
                QueryStep(
                    name="unacceptable_usage",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_UNACCEPTABLEUSAGE_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=UnacceptableUsage,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return UnacceptableUsageQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, unacceptable_usage: UnacceptableUsageApply | Sequence[UnacceptableUsageApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) unacceptable usages.

        Args:
            unacceptable_usage: Unacceptable usage or sequence of unacceptable usages to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new unacceptable_usage:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import UnacceptableUsageApply
                >>> client = OSDUClient()
                >>> unacceptable_usage = UnacceptableUsageApply(external_id="my_unacceptable_usage", ...)
                >>> result = client.unacceptable_usage.apply(unacceptable_usage)

        """
        return self._apply(unacceptable_usage, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more unacceptable usage.

        Args:
            external_id: External id of the unacceptable usage to delete.
            space: The space where all the unacceptable usage are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete unacceptable_usage by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.unacceptable_usage.delete("my_unacceptable_usage")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> UnacceptableUsage:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> UnacceptableUsageList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> UnacceptableUsage | UnacceptableUsageList:
        """Retrieve one or more unacceptable usages by id(s).

        Args:
            external_id: External id or list of external ids of the unacceptable usages.
            space: The space where all the unacceptable usages are located.

        Returns:
            The requested unacceptable usages.

        Examples:

            Retrieve unacceptable_usage by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> unacceptable_usage = client.unacceptable_usage.retrieve("my_unacceptable_usage")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: UnacceptableUsageTextFields | Sequence[UnacceptableUsageTextFields] | None = None,
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
    ) -> UnacceptableUsageList:
        """Search unacceptable usages

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
            limit: Maximum number of unacceptable usages to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results unacceptable usages matching the query.

        Examples:

           Search for 'my_unacceptable_usage' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> unacceptable_usages = client.unacceptable_usage.search('my_unacceptable_usage')

        """
        filter_ = _create_unacceptable_usage_filter(
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
        return self._search(self._view_id, query, _UNACCEPTABLEUSAGE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: UnacceptableUsageFields | Sequence[UnacceptableUsageFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: UnacceptableUsageTextFields | Sequence[UnacceptableUsageTextFields] | None = None,
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
        property: UnacceptableUsageFields | Sequence[UnacceptableUsageFields] | None = None,
        group_by: UnacceptableUsageFields | Sequence[UnacceptableUsageFields] = None,
        query: str | None = None,
        search_properties: UnacceptableUsageTextFields | Sequence[UnacceptableUsageTextFields] | None = None,
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
        property: UnacceptableUsageFields | Sequence[UnacceptableUsageFields] | None = None,
        group_by: UnacceptableUsageFields | Sequence[UnacceptableUsageFields] | None = None,
        query: str | None = None,
        search_property: UnacceptableUsageTextFields | Sequence[UnacceptableUsageTextFields] | None = None,
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
        """Aggregate data across unacceptable usages

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
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
            limit: Maximum number of unacceptable usages to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count unacceptable usages in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.unacceptable_usage.aggregate("count", space="my_space")

        """

        filter_ = _create_unacceptable_usage_filter(
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
            _UNACCEPTABLEUSAGE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: UnacceptableUsageFields,
        interval: float,
        query: str | None = None,
        search_property: UnacceptableUsageTextFields | Sequence[UnacceptableUsageTextFields] | None = None,
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
        """Produces histograms for unacceptable usages

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
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
            limit: Maximum number of unacceptable usages to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_unacceptable_usage_filter(
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
            _UNACCEPTABLEUSAGE_PROPERTIES_BY_FIELD,
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
    ) -> UnacceptableUsageList:
        """List/filter unacceptable usages

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
            limit: Maximum number of unacceptable usages to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested unacceptable usages

        Examples:

            List unacceptable usages and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> unacceptable_usages = client.unacceptable_usage.list(limit=5)

        """
        filter_ = _create_unacceptable_usage_filter(
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
