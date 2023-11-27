from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    AcceptableUsage,
    AcceptableUsageApply,
    AcceptableUsageFields,
    AcceptableUsageList,
    AcceptableUsageApplyList,
    AcceptableUsageTextFields,
)
from osdu_wells.client.data_classes._acceptable_usage import (
    _ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD,
    _create_acceptable_usage_filter,
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
from .acceptable_usage_query import AcceptableUsageQueryAPI


class AcceptableUsageAPI(NodeAPI[AcceptableUsage, AcceptableUsageApply, AcceptableUsageList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[AcceptableUsageApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=AcceptableUsage,
            class_apply_type=AcceptableUsageApply,
            class_list=AcceptableUsageList,
            class_apply_list=AcceptableUsageApplyList,
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
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> AcceptableUsageQueryAPI[AcceptableUsageList]:
        """Query starting at acceptable usages.

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
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for acceptable usages.

        """
        filter_ = _create_acceptable_usage_filter(
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
            AcceptableUsageList,
            [
                QueryStep(
                    name="acceptable_usage",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=AcceptableUsage,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return AcceptableUsageQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, acceptable_usage: AcceptableUsageApply | Sequence[AcceptableUsageApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) acceptable usages.

        Args:
            acceptable_usage: Acceptable usage or sequence of acceptable usages to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new acceptable_usage:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import AcceptableUsageApply
                >>> client = OSDUClient()
                >>> acceptable_usage = AcceptableUsageApply(external_id="my_acceptable_usage", ...)
                >>> result = client.acceptable_usage.apply(acceptable_usage)

        """
        return self._apply(acceptable_usage, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
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
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> AcceptableUsage | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> AcceptableUsageList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> AcceptableUsage | AcceptableUsageList | None:
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
        return self._retrieve(external_id, space)

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
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results acceptable usages matching the query.

        Examples:

           Search for 'my_acceptable_usage' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> acceptable_usages = client.acceptable_usage.search('my_acceptable_usage')

        """
        filter_ = _create_acceptable_usage_filter(
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
        """Aggregate data across acceptable usages

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
            limit: Maximum number of acceptable usages to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count acceptable usages in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.acceptable_usage.aggregate("count", space="my_space")

        """

        filter_ = _create_acceptable_usage_filter(
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
        """Produces histograms for acceptable usages

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
            limit: Maximum number of acceptable usages to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_acceptable_usage_filter(
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
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested acceptable usages

        Examples:

            List acceptable usages and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> acceptable_usages = client.acceptable_usage.list(limit=5)

        """
        filter_ = _create_acceptable_usage_filter(
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
