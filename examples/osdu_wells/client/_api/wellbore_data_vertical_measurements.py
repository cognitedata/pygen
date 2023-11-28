from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class WellboreDataVerticalMeasurementsAPI(EdgeAPI):
    def list(
        self,
        wellbore_datum: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        wellbore_datum_space: str = "IntegrationTestsImmutable",
        vertical_measurement: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        vertical_measurement_space: str = "IntegrationTestsImmutable",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List vertical measurement edges of a wellbore datum.

        Args:
            wellbore_datum: ID of the source wellbore data.
            wellbore_datum_space: Location of the wellbore data.
            vertical_measurement: ID of the target vertical measurements.
            vertical_measurement_space: Location of the vertical measurements.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of vertical measurement edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested vertical measurement edges.

        Examples:

            List 5 vertical measurement edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.vertical_measurements_edge.list("my_wellbore_datum", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.VerticalMeasurements"),
            wellbore_datum,
            wellbore_datum_space,
            vertical_measurement,
            vertical_measurement_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
