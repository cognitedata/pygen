from __future__ import annotations


from cognite.client import data_modeling as dm

from tutorial_apm_simple.client.data_classes import (
    CdfConnectionPropertiesList,
)
from tutorial_apm_simple.client.data_classes._cdf_3_d_connection_properties import (
    _create_cdf_3_d_connection_property_filter,
)

from ._core import DEFAULT_LIMIT_READ, EdgePropertyAPI
from tutorial_apm_simple.client.data_classes._core import DEFAULT_INSTANCE_SPACE


class AssetInModelDAPI(EdgePropertyAPI):
    def list(
        self,
        from_asset: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_asset_space: str = DEFAULT_INSTANCE_SPACE,
        to_cdf_3_d_entity: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_cdf_3_d_entity_space: str = DEFAULT_INSTANCE_SPACE,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> CdfConnectionPropertiesList:
        """List in model 3 d edges of a asset.

        Args:
            from_asset: ID of the source asset.
            from_asset_space: Location of the assets.
            to_cdf_3_d_entity: ID of the target cdf 3 d entity.
            to_cdf_3_d_entity_space: Location of the cdf 3 d entities.
            min_revision_id: The minimum value of the revision id to filter on.
            max_revision_id: The maximum value of the revision id to filter on.
            min_revision_node_id: The minimum value of the revision node id to filter on.
            max_revision_node_id: The maximum value of the revision node id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of in model 3 d edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested in model 3 d edges.

        Examples:

            List 5 in model 3 d edges connected to "my_asset":

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> asset = client.asset.in_model_3_d_edge.list("my_asset", limit=5)

        """
        filter_ = _create_cdf_3_d_connection_property_filter(
            dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection"),
            self._view_id,
            from_asset,
            from_asset_space,
            to_cdf_3_d_entity,
            to_cdf_3_d_entity_space,
            min_revision_id,
            max_revision_id,
            min_revision_node_id,
            max_revision_node_id,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
