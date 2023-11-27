from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class AsIngestedCoordinatesFeaturesAPI(EdgeAPI):
    def list(
        self,
        as_ingested_coordinate: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        as_ingested_coordinate_space: str = "IntegrationTestsImmutable",
        feature: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        feature_space: str = "IntegrationTestsImmutable",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List feature edges of a as ingested coordinate.

        Args:
            as_ingested_coordinate: ID of the source as ingested coordinates.
            as_ingested_coordinate_space: Location of the as ingested coordinates.
            feature: ID of the target features.
            feature_space: Location of the features.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of feature edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested feature edges.

        Examples:

            List 5 feature edges connected to "my_as_ingested_coordinate":

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> as_ingested_coordinate = client.as_ingested_coordinates.features_edge.list("my_as_ingested_coordinate", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "AsIngestedCoordinates.features"),
            as_ingested_coordinate,
            as_ingested_coordinate_space,
            feature,
            feature_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
