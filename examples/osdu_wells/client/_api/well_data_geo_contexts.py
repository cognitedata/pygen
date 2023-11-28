from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class WellDataGeoContextsAPI(EdgeAPI):
    def list(
        self,
        well_datum: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        well_datum_space: str = "IntegrationTestsImmutable",
        geo_context: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        geo_context_space: str = "IntegrationTestsImmutable",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List geo context edges of a well datum.

        Args:
            well_datum: ID of the source well data.
            well_datum_space: Location of the well data.
            geo_context: ID of the target geo contexts.
            geo_context_space: Location of the geo contexts.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of geo context edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested geo context edges.

        Examples:

            List 5 geo context edges connected to "my_well_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> well_datum = client.well_data.geo_contexts_edge.list("my_well_datum", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.GeoContexts"),
            well_datum,
            well_datum_space,
            geo_context,
            geo_context_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
