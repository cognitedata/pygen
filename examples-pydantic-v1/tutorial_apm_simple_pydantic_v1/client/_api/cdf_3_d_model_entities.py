from __future__ import annotations


from cognite.client import data_modeling as dm

from tutorial_apm_simple_pydantic_v1.client.data_classes import (
    CdfConnectionPropertiesList,
)
from tutorial_apm_simple_pydantic_v1.client.data_classes._cdf_3_d_connection_properties import (
    _create_cdf_3_d_connection_property_filter,
)

from ._core import DEFAULT_LIMIT_READ, EdgePropertyAPI


class CdfModelEntitiesAPI(EdgePropertyAPI):
    def list(
        self,
        cdf_3_d_model: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        cdf_3_d_model_space: str = "cdf_3d_schema",
        cdf_3_d_entity: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        cdf_3_d_entity_space: str = "cdf_3d_schema",
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        min_revision_node_id: int | None = None,
        max_revision_node_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> CdfConnectionPropertiesList:
        """List entity edges of a cdf 3 d model.

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
            limit: Maximum number of entity edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested entity edges.

        Examples:

            List 5 entity edges connected to "my_cdf_3_d_model":

                >>> from tutorial_apm_simple_pydantic_v1.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> cdf_3_d_model = client.cdf_3_d_model.entities_edge.list("my_cdf_3_d_model", limit=5)

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
