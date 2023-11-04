from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    WellboreCosts,
    WellboreCostsApply,
    WellboreCostsList,
    WellboreCostsApplyList,
    WellboreCostsFields,
    WellboreCostsTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._wellbore_costs import _WELLBORECOSTS_PROPERTIES_BY_FIELD


class WellboreCostsAPI(TypeAPI[WellboreCosts, WellboreCostsApply, WellboreCostsList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WellboreCostsApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WellboreCosts,
            class_apply_type=WellboreCostsApply,
            class_list=WellboreCostsList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self, wellbore_cost: WellboreCostsApply | Sequence[WellboreCostsApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) wellbore costs.

        Args:
            wellbore_cost: Wellbore cost or sequence of wellbore costs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new wellbore_cost:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import WellboreCostsApply
                >>> client = OSDUClient()
                >>> wellbore_cost = WellboreCostsApply(external_id="my_wellbore_cost", ...)
                >>> result = client.wellbore_costs.apply(wellbore_cost)

        """
        if isinstance(wellbore_cost, WellboreCostsApply):
            instances = wellbore_cost.to_instances_apply(self._view_by_write_class)
        else:
            instances = WellboreCostsApplyList(wellbore_cost).to_instances_apply(self._view_by_write_class)
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
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> WellboreCosts:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WellboreCostsList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
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
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

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
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results wellbore costs matching the query.

        Examples:

           Search for 'my_wellbore_cost' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_costs = client.wellbore_costs.search('my_wellbore_cost')

        """
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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
            filter: (Advanced) If the filtering available in the above is not sufficent, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested wellbore costs

        Examples:

            List wellbore costs and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_costs = client.wellbore_costs.list(limit=5)

        """
        filter_ = _create_filter(
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


def _create_filter(
    view_id: dm.ViewId,
    activity_type_id: str | list[str] | None = None,
    activity_type_id_prefix: str | None = None,
    min_cost: float | None = None,
    max_cost: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if activity_type_id and isinstance(activity_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ActivityTypeID"), value=activity_type_id))
    if activity_type_id and isinstance(activity_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ActivityTypeID"), values=activity_type_id))
    if activity_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ActivityTypeID"), value=activity_type_id_prefix))
    if min_cost or max_cost:
        filters.append(dm.filters.Range(view_id.as_property_ref("Cost"), gte=min_cost, lte=max_cost))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
