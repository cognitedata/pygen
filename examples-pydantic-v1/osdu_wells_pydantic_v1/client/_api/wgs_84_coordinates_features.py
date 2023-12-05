from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from osdu_wells_pydantic_v1.client.data_classes._core import DEFAULT_INSTANCE_SPACE


class WgsCoordinatesFeaturesAPI(EdgeAPI):
    def list(
        self,
        from_wgs_84_coordinate: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_wgs_84_coordinate_space: str = DEFAULT_INSTANCE_SPACE,
        to_feature: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_feature_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List feature edges of a wgs 84 coordinate.

        Args:
            from_wgs_84_coordinate: ID of the source wgs 84 coordinate.
            from_wgs_84_coordinate_space: Location of the wgs 84 coordinates.
            to_feature: ID of the target feature.
            to_feature_space: Location of the features.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of feature edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested feature edges.

        Examples:

            List 5 feature edges connected to "my_wgs_84_coordinate":

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wgs_84_coordinate = client.wgs_84_coordinates.features_edge.list("my_wgs_84_coordinate", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Wgs84Coordinates.features"),
            from_wgs_84_coordinate,
            from_wgs_84_coordinate_space,
            to_feature,
            to_feature_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
