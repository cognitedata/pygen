from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class WorkItemLinkedAssetsAPI(EdgeAPI):
    def list(
        self,
        work_item: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        work_item_space: str = "tutorial_apm_simple",
        asset: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        asset_space: str = "tutorial_apm_simple",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List linked asset edges of a work item.

        Args:
            work_item: ID of the source work items.
            work_item_space: Location of the work items.
            asset: ID of the target assets.
            asset_space: Location of the assets.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of linked asset edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested linked asset edges.

        Examples:

            List 5 linked asset edges connected to "my_work_item":

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_item = client.work_item.linked_assets_edge.list("my_work_item", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("tutorial_apm_simple", "WorkItem.linkedAssets"),
            work_item,
            work_item_space,
            asset,
            asset_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
