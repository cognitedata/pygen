from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class WorkOrderLinkedAssetsAPI(EdgeAPI):
    def list(
        self,
        work_order: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        work_order_space: str = "tutorial_apm_simple",
        asset: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        asset_space: str = "tutorial_apm_simple",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List linked asset edges of a work order.

        Args:
            work_order: ID of the source work orders.
            work_order_space: Location of the work orders.
            asset: ID of the target assets.
            asset_space: Location of the assets.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of linked asset edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested linked asset edges.

        Examples:

            List 5 linked asset edges connected to "my_work_order":

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> work_order = client.work_order.linked_assets_edge.list("my_work_order", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("tutorial_apm_simple", "WorkOrder.linkedAssets"),
            work_order,
            work_order_space,
            asset,
            asset_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
