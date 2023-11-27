from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    WellboreCosts,
    WellboreCostsApply,
    WellboreCostsFields,
    WellboreCostsList,
    WellboreCostsApplyList,
    WellboreCostsTextFields,
)
from osdu_wells.client.data_classes._wellbore_costs import (
    _WELLBORECOSTS_PROPERTIES_BY_FIELD,
    _create_wellbore_cost_filter,
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
from .wellbore_costs_query import WellboreCostsQueryAPI


class WellboreCostsAPI(NodeAPI[WellboreCosts, WellboreCostsApply, WellboreCostsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WellboreCostsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WellboreCosts,
            class_apply_type=WellboreCostsApply,
            class_list=WellboreCostsList,
            class_apply_list=WellboreCostsApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> WellboreCostsQueryAPI[WellboreCostsList]:
        """Query starting at wellbore costs.

        Args:
            activity_type_id: The activity type id to filter on.
            activity_type_id_prefix: The prefix of the activity type id to filter on.
            min_cost: The minimum value of the cost to filter on.
            max_cost: The maximum value of the cost to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore costs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for wellbore costs.

        """
        filter_ = _create_wellbore_cost_filter(
            self._view_id,
            activity_type_id,
            activity_type_id_prefix,
            min_cost,
            max_cost,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            WellboreCostsList,
            [
                QueryStep(
                    name="wellbore_cost",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_WELLBORECOSTS_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=WellboreCosts,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return WellboreCostsQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, wellbore_cost: WellboreCostsApply | Sequence[WellboreCostsApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) wellbore costs.

        Args:
            wellbore_cost: Wellbore cost or sequence of wellbore costs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new wellbore_cost:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import WellboreCostsApply
                >>> client = OSDUClient()
                >>> wellbore_cost = WellboreCostsApply(external_id="my_wellbore_cost", ...)
                >>> result = client.wellbore_costs.apply(wellbore_cost)

        """
        return self._apply(wellbore_cost, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more wellbore cost.

        Args:
            external_id: External id of the wellbore cost to delete.
            space: The space where all the wellbore cost are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete wellbore_cost by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.wellbore_costs.delete("my_wellbore_cost")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> WellboreCosts:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> WellboreCostsList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> WellboreCosts | WellboreCostsList:
        """Retrieve one or more wellbore costs by id(s).

        Args:
            external_id: External id or list of external ids of the wellbore costs.
            space: The space where all the wellbore costs are located.

        Returns:
            The requested wellbore costs.

        Examples:

            Retrieve wellbore_cost by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_cost = client.wellbore_costs.retrieve("my_wellbore_cost")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: WellboreCostsTextFields | Sequence[WellboreCostsTextFields] | None = None,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellboreCostsList:
        """Search wellbore costs

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            activity_type_id: The activity type id to filter on.
            activity_type_id_prefix: The prefix of the activity type id to filter on.
            min_cost: The minimum value of the cost to filter on.
            max_cost: The maximum value of the cost to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore costs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results wellbore costs matching the query.

        Examples:

           Search for 'my_wellbore_cost' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_costs = client.wellbore_costs.search('my_wellbore_cost')

        """
        filter_ = _create_wellbore_cost_filter(
            self._view_id,
            activity_type_id,
            activity_type_id_prefix,
            min_cost,
            max_cost,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WELLBORECOSTS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellboreCostsFields | Sequence[WellboreCostsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WellboreCostsTextFields | Sequence[WellboreCostsTextFields] | None = None,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
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
        property: WellboreCostsFields | Sequence[WellboreCostsFields] | None = None,
        group_by: WellboreCostsFields | Sequence[WellboreCostsFields] = None,
        query: str | None = None,
        search_properties: WellboreCostsTextFields | Sequence[WellboreCostsTextFields] | None = None,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
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
        property: WellboreCostsFields | Sequence[WellboreCostsFields] | None = None,
        group_by: WellboreCostsFields | Sequence[WellboreCostsFields] | None = None,
        query: str | None = None,
        search_property: WellboreCostsTextFields | Sequence[WellboreCostsTextFields] | None = None,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across wellbore costs

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            activity_type_id: The activity type id to filter on.
            activity_type_id_prefix: The prefix of the activity type id to filter on.
            min_cost: The minimum value of the cost to filter on.
            max_cost: The maximum value of the cost to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore costs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count wellbore costs in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.wellbore_costs.aggregate("count", space="my_space")

        """

        filter_ = _create_wellbore_cost_filter(
            self._view_id,
            activity_type_id,
            activity_type_id_prefix,
            min_cost,
            max_cost,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WELLBORECOSTS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WellboreCostsFields,
        interval: float,
        query: str | None = None,
        search_property: WellboreCostsTextFields | Sequence[WellboreCostsTextFields] | None = None,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for wellbore costs

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            activity_type_id: The activity type id to filter on.
            activity_type_id_prefix: The prefix of the activity type id to filter on.
            min_cost: The minimum value of the cost to filter on.
            max_cost: The maximum value of the cost to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore costs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_wellbore_cost_filter(
            self._view_id,
            activity_type_id,
            activity_type_id_prefix,
            min_cost,
            max_cost,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WELLBORECOSTS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        activity_type_id: str | list[str] | None = None,
        activity_type_id_prefix: str | None = None,
        min_cost: float | None = None,
        max_cost: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellboreCostsList:
        """List/filter wellbore costs

        Args:
            activity_type_id: The activity type id to filter on.
            activity_type_id_prefix: The prefix of the activity type id to filter on.
            min_cost: The minimum value of the cost to filter on.
            max_cost: The maximum value of the cost to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore costs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested wellbore costs

        Examples:

            List wellbore costs and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_costs = client.wellbore_costs.list(limit=5)

        """
        filter_ = _create_wellbore_cost_filter(
            self._view_id,
            activity_type_id,
            activity_type_id_prefix,
            min_cost,
            max_cost,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
