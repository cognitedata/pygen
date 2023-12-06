from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from tutorial_apm_simple.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    WorkItem,
    WorkItemApply,
    WorkItemFields,
    WorkItemList,
    WorkItemApplyList,
    WorkItemTextFields,
)
from tutorial_apm_simple.client.data_classes._work_item import (
    _WORKITEM_PROPERTIES_BY_FIELD,
    _create_work_item_filter,
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
from .work_item_linked_assets import WorkItemLinkedAssetsAPI
from .work_item_query import WorkItemQueryAPI


class WorkItemAPI(NodeAPI[WorkItem, WorkItemApply, WorkItemList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WorkItemApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WorkItem,
            class_apply_type=WorkItemApply,
            class_list=WorkItemList,
            class_apply_list=WorkItemApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.linked_assets_edge = WorkItemLinkedAssetsAPI(client)

    def __call__(
        self,
        criticality: str | list[str] | None = None,
        criticality_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_completed: bool | None = None,
        item_info: str | list[str] | None = None,
        item_info_prefix: str | None = None,
        item_name: str | list[str] | None = None,
        item_name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        to_be_done: bool | None = None,
        work_order: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> WorkItemQueryAPI[WorkItemList]:
        """Query starting at work items.

        Args:
            criticality: The criticality to filter on.
            criticality_prefix: The prefix of the criticality to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_completed: The is completed to filter on.
            item_info: The item info to filter on.
            item_info_prefix: The prefix of the item info to filter on.
            item_name: The item name to filter on.
            item_name_prefix: The prefix of the item name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            to_be_done: The to be done to filter on.
            work_order: The work order to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work items to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for work items.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_work_item_filter(
            self._view_id,
            criticality,
            criticality_prefix,
            description,
            description_prefix,
            is_completed,
            item_info,
            item_info_prefix,
            item_name,
            item_name_prefix,
            method,
            method_prefix,
            title,
            title_prefix,
            to_be_done,
            work_order,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(WorkItemList)
        return WorkItemQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(self, work_item: WorkItemApply | Sequence[WorkItemApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) work items.

        Note: This method iterates through all nodes and timeseries linked to work_item and creates them including the edges
        between the nodes. For example, if any of `linked_assets` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            work_item: Work item or sequence of work items to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new work_item:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> from tutorial_apm_simple.client.data_classes import WorkItemApply
                >>> client = ApmSimpleClient()
                >>> work_item = WorkItemApply(external_id="my_work_item", ...)
                >>> result = client.work_item.apply(work_item)

        """
        return self._apply(work_item, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "tutorial_apm_simple"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more work item.

        Args:
            external_id: External id of the work item to delete.
            space: The space where all the work item are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete work_item by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> client.work_item.delete("my_work_item")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> WorkItem | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> WorkItemList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "tutorial_apm_simple"
    ) -> WorkItem | WorkItemList | None:
        """Retrieve one or more work items by id(s).

        Args:
            external_id: External id or list of external ids of the work items.
            space: The space where all the work items are located.

        Returns:
            The requested work items.

        Examples:

            Retrieve work_item by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_item = client.work_item.retrieve("my_work_item")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_triple=[
                (
                    self.linked_assets_edge,
                    "linked_assets",
                    dm.DirectRelationReference("tutorial_apm_simple", "WorkItem.linkedAssets"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: WorkItemTextFields | Sequence[WorkItemTextFields] | None = None,
        criticality: str | list[str] | None = None,
        criticality_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_completed: bool | None = None,
        item_info: str | list[str] | None = None,
        item_info_prefix: str | None = None,
        item_name: str | list[str] | None = None,
        item_name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        to_be_done: bool | None = None,
        work_order: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WorkItemList:
        """Search work items

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            criticality: The criticality to filter on.
            criticality_prefix: The prefix of the criticality to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_completed: The is completed to filter on.
            item_info: The item info to filter on.
            item_info_prefix: The prefix of the item info to filter on.
            item_name: The item name to filter on.
            item_name_prefix: The prefix of the item name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            to_be_done: The to be done to filter on.
            work_order: The work order to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work items to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results work items matching the query.

        Examples:

           Search for 'my_work_item' in all text properties:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_items = client.work_item.search('my_work_item')

        """
        filter_ = _create_work_item_filter(
            self._view_id,
            criticality,
            criticality_prefix,
            description,
            description_prefix,
            is_completed,
            item_info,
            item_info_prefix,
            item_name,
            item_name_prefix,
            method,
            method_prefix,
            title,
            title_prefix,
            to_be_done,
            work_order,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WORKITEM_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WorkItemFields | Sequence[WorkItemFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WorkItemTextFields | Sequence[WorkItemTextFields] | None = None,
        criticality: str | list[str] | None = None,
        criticality_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_completed: bool | None = None,
        item_info: str | list[str] | None = None,
        item_info_prefix: str | None = None,
        item_name: str | list[str] | None = None,
        item_name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        to_be_done: bool | None = None,
        work_order: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: WorkItemFields | Sequence[WorkItemFields] | None = None,
        group_by: WorkItemFields | Sequence[WorkItemFields] = None,
        query: str | None = None,
        search_properties: WorkItemTextFields | Sequence[WorkItemTextFields] | None = None,
        criticality: str | list[str] | None = None,
        criticality_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_completed: bool | None = None,
        item_info: str | list[str] | None = None,
        item_info_prefix: str | None = None,
        item_name: str | list[str] | None = None,
        item_name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        to_be_done: bool | None = None,
        work_order: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: WorkItemFields | Sequence[WorkItemFields] | None = None,
        group_by: WorkItemFields | Sequence[WorkItemFields] | None = None,
        query: str | None = None,
        search_property: WorkItemTextFields | Sequence[WorkItemTextFields] | None = None,
        criticality: str | list[str] | None = None,
        criticality_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_completed: bool | None = None,
        item_info: str | list[str] | None = None,
        item_info_prefix: str | None = None,
        item_name: str | list[str] | None = None,
        item_name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        to_be_done: bool | None = None,
        work_order: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across work items

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            criticality: The criticality to filter on.
            criticality_prefix: The prefix of the criticality to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_completed: The is completed to filter on.
            item_info: The item info to filter on.
            item_info_prefix: The prefix of the item info to filter on.
            item_name: The item name to filter on.
            item_name_prefix: The prefix of the item name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            to_be_done: The to be done to filter on.
            work_order: The work order to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work items to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count work items in space `my_space`:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> result = client.work_item.aggregate("count", space="my_space")

        """

        filter_ = _create_work_item_filter(
            self._view_id,
            criticality,
            criticality_prefix,
            description,
            description_prefix,
            is_completed,
            item_info,
            item_info_prefix,
            item_name,
            item_name_prefix,
            method,
            method_prefix,
            title,
            title_prefix,
            to_be_done,
            work_order,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WORKITEM_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WorkItemFields,
        interval: float,
        query: str | None = None,
        search_property: WorkItemTextFields | Sequence[WorkItemTextFields] | None = None,
        criticality: str | list[str] | None = None,
        criticality_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_completed: bool | None = None,
        item_info: str | list[str] | None = None,
        item_info_prefix: str | None = None,
        item_name: str | list[str] | None = None,
        item_name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        to_be_done: bool | None = None,
        work_order: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for work items

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            criticality: The criticality to filter on.
            criticality_prefix: The prefix of the criticality to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_completed: The is completed to filter on.
            item_info: The item info to filter on.
            item_info_prefix: The prefix of the item info to filter on.
            item_name: The item name to filter on.
            item_name_prefix: The prefix of the item name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            to_be_done: The to be done to filter on.
            work_order: The work order to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work items to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_work_item_filter(
            self._view_id,
            criticality,
            criticality_prefix,
            description,
            description_prefix,
            is_completed,
            item_info,
            item_info_prefix,
            item_name,
            item_name_prefix,
            method,
            method_prefix,
            title,
            title_prefix,
            to_be_done,
            work_order,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WORKITEM_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        criticality: str | list[str] | None = None,
        criticality_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_completed: bool | None = None,
        item_info: str | list[str] | None = None,
        item_info_prefix: str | None = None,
        item_name: str | list[str] | None = None,
        item_name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        to_be_done: bool | None = None,
        work_order: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WorkItemList:
        """List/filter work items

        Args:
            criticality: The criticality to filter on.
            criticality_prefix: The prefix of the criticality to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_completed: The is completed to filter on.
            item_info: The item info to filter on.
            item_info_prefix: The prefix of the item info to filter on.
            item_name: The item name to filter on.
            item_name_prefix: The prefix of the item name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            to_be_done: The to be done to filter on.
            work_order: The work order to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work items to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `linked_assets` external ids for the work items. Defaults to True.

        Returns:
            List of requested work items

        Examples:

            List work items and limit to 5:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_items = client.work_item.list(limit=5)

        """
        filter_ = _create_work_item_filter(
            self._view_id,
            criticality,
            criticality_prefix,
            description,
            description_prefix,
            is_completed,
            item_info,
            item_info_prefix,
            item_name,
            item_name_prefix,
            method,
            method_prefix,
            title,
            title_prefix,
            to_be_done,
            work_order,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_triple=[
                (
                    self.linked_assets_edge,
                    "linked_assets",
                    dm.DirectRelationReference("tutorial_apm_simple", "WorkItem.linkedAssets"),
                ),
            ],
        )
