from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class AssetChildrenAPI(EdgeAPI):
    def list(
        self,
        asset: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        asset_space: str = "tutorial_apm_simple",
        asset: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        asset_space: str = "tutorial_apm_simple",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List child edges of a asset.

        Args:
            asset: ID of the source assets.
            asset_space: Location of the assets.
            asset: ID of the target assets.
            asset_space: Location of the assets.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of child edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested child edges.

        Examples:

            List 5 child edges connected to "my_asset":

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> asset = client.asset.children_edge.list("my_asset", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("tutorial_apm_simple", "Asset.children"),
            asset,
            asset_space,
            asset,
            asset_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
