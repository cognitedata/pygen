from __future__ import annotations


from cognite.client import data_modeling as dm

from tutorial_apm_simple.client.data_classes import (
    CdfConnectionPropertiesList,
)
from tutorial_apm_simple.client.data_classes._cdf_3_d_connection_properties import (
    _create_cdf_3_d_connection_property_filter,
)

from ._core import DEFAULT_LIMIT_READ, EdgePropertyAPI


class AssetInModelAPI(EdgePropertyAPI):
    def list(
        self,
        cdf_3_d_model: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        cdf_3_d_model_space: str = "tutorial_apm_simple",
        cdf_3_d_entity: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        cdf_3_d_entity_space: str = "tutorial_apm_simple",
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> CdfConnectionPropertiesList:
        """List in model 3 d edges of a cdf 3 d model.

        Args:
            cdf_3_d_model: ID of the source cdf 3 d models.
            cdf_3_d_model_space: Location of the cdf 3 d models.
            cdf_3_d_entity: ID of the target cdf 3 d entities.
            cdf_3_d_entity_space: Location of the cdf 3 d entities.
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

            List 5 in model 3 d edges connected to "my_cdf_3_d_model":

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_model = client.asset.in_model_3_d_edge.list("my_cdf_3_d_model", limit=5)

        """
        filter_ = _create_cdf_3_d_connection_property_filter(
            dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection"),
            self._view_id,
            cdf_3_d_model,
            cdf_3_d_model_space,
            cdf_3_d_entity,
            cdf_3_d_entity_space,
            min_revision_id,
            max_revision_id,
            min_revision_node_id,
            max_revision_node_id,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
