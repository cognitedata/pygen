from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from tutorial_apm_simple.client.data_classes import (
    WorkItem,
    WorkItemApply,
    WorkItemList,
    WorkItemApplyList,
    WorkItemFields,
    WorkItemTextFields,
    DomainModelApply,
)
from tutorial_apm_simple.client.data_classes._work_item import _WORKITEM_PROPERTIES_BY_FIELD


class WorkItemLinkedAssetsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space: str = "tutorial_apm_simple") -> dm.EdgeList:
        """Retrieve one or more linked_assets edges by id(s) of a work item.

        Args:
            external_id: External id or list of external ids source work item.
            space: The space where all the linked asset edges are located.

        Returns:
            The requested linked asset edges.

        Examples:

            Retrieve linked_assets edge by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_item = client.work_item.linked_assets.retrieve("my_linked_assets")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "WorkItem.linkedAssets"},
        )
        if isinstance(external_id, str):
            is_work_items = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
        else:
            is_work_items = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_work_items))

    def list(
        self,
        work_item_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "tutorial_apm_simple",
    ) -> dm.EdgeList:
        """List linked_assets edges of a work item.

        Args:
            work_item_id: ID of the source work item.
            limit: Maximum number of linked asset edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the linked asset edges are located.

        Returns:
            The requested linked asset edges.

        Examples:

            List 5 linked_assets edges connected to "my_work_item":

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_item = client.work_item.linked_assets.list("my_work_item", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "tutorial_apm_simple", "externalId": "WorkItem.linkedAssets"},
            )
        ]
        if work_item_id:
            work_item_ids = work_item_id if isinstance(work_item_id, list) else [work_item_id]
            is_work_items = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in work_item_ids
                ],
            )
            filters.append(is_work_items)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WorkItemAPI(TypeAPI[WorkItem, WorkItemApply, WorkItemList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WorkItemApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WorkItem,
            class_apply_type=WorkItemApply,
            class_list=WorkItemList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.linked_assets = WorkItemLinkedAssetsAPI(client)

    def apply(
        self, work_item: WorkItemApply | Sequence[WorkItemApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) work items.

        Note: This method iterates through all nodes linked to work_item and create them including the edges
        between the nodes. For example, if any of `linked_assets` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            work_item: Work item or sequence of work items to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new work_item:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> from tutorial_apm_simple.client.data_classes import WorkItemApply
                >>> client = ApmSimpleClient()
                >>> work_item = WorkItemApply(external_id="my_work_item", ...)
                >>> result = client.work_item.apply(work_item)

        """
        if isinstance(work_item, WorkItemApply):
            instances = work_item.to_instances_apply(self._view_by_write_class)
        else:
            instances = WorkItemApplyList(work_item).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space: str = "tutorial_apm_simple") -> dm.InstancesDeleteResult:
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
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> WorkItem:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WorkItemList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "tutorial_apm_simple") -> WorkItem | WorkItemList:
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
        if isinstance(external_id, str):
            work_item = self._retrieve((space, external_id))

            linked_asset_edges = self.linked_assets.retrieve(external_id, space=space)
            work_item.linked_assets = [edge.end_node.external_id for edge in linked_asset_edges]

            return work_item
        else:
            work_items = self._retrieve([(space, ext_id) for ext_id in external_id])

            linked_asset_edges = self.linked_assets.retrieve(work_items.as_node_ids())
            self._set_linked_assets(work_items, linked_asset_edges)

            return work_items

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
            retrieve_edges: Whether to retrieve `linked_assets` external ids for the work items. Defaults to True.

        Returns:
            Search results work items matching the query.

        Examples:

           Search for 'my_work_item' in all text properties:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_items = client.work_item.search('my_work_item')

        """
        filter_ = _create_filter(
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
            retrieve_edges: Whether to retrieve `linked_assets` external ids for the work items. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count work items in space `my_space`:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> result = client.work_item.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
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
            retrieve_edges: Whether to retrieve `linked_assets` external ids for the work items. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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

        work_items = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            space_arg = {"space": space} if space else {}
            if len(ids := work_items.as_node_ids()) > IN_FILTER_LIMIT:
                linked_asset_edges = self.linked_assets.list(limit=-1, **space_arg)
            else:
                linked_asset_edges = self.linked_assets.list(ids, limit=-1)
            self._set_linked_assets(work_items, linked_asset_edges)

        return work_items

    @staticmethod
    def _set_linked_assets(work_items: Sequence[WorkItem], linked_asset_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in linked_asset_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for work_item in work_items:
            node_id = work_item.id_tuple()
            if node_id in edges_by_start_node:
                work_item.linked_assets = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
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
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if criticality and isinstance(criticality, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("criticality"), value=criticality))
    if criticality and isinstance(criticality, list):
        filters.append(dm.filters.In(view_id.as_property_ref("criticality"), values=criticality))
    if criticality_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("criticality"), value=criticality_prefix))
    if description and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if is_completed and isinstance(is_completed, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isCompleted"), value=is_completed))
    if item_info and isinstance(item_info, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("itemInfo"), value=item_info))
    if item_info and isinstance(item_info, list):
        filters.append(dm.filters.In(view_id.as_property_ref("itemInfo"), values=item_info))
    if item_info_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("itemInfo"), value=item_info_prefix))
    if item_name and isinstance(item_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("itemName"), value=item_name))
    if item_name and isinstance(item_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("itemName"), values=item_name))
    if item_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("itemName"), value=item_name_prefix))
    if method and isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value=method))
    if method and isinstance(method, list):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=method))
    if method_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("method"), value=method_prefix))
    if title and isinstance(title, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("title"), value=title))
    if title and isinstance(title, list):
        filters.append(dm.filters.In(view_id.as_property_ref("title"), values=title))
    if title_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("title"), value=title_prefix))
    if to_be_done and isinstance(to_be_done, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("toBeDone"), value=to_be_done))
    if work_order and isinstance(work_order, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("workOrder"), value={"space": "tutorial_apm_simple", "externalId": work_order}
            )
        )
    if work_order and isinstance(work_order, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("workOrder"), value={"space": work_order[0], "externalId": work_order[1]}
            )
        )
    if work_order and isinstance(work_order, list) and isinstance(work_order[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("workOrder"),
                values=[{"space": "tutorial_apm_simple", "externalId": item} for item in work_order],
            )
        )
    if work_order and isinstance(work_order, list) and isinstance(work_order[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("workOrder"),
                values=[{"space": item[0], "externalId": item[1]} for item in work_order],
            )
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
