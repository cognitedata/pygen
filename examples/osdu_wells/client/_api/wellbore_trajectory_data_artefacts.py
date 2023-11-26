from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class WellboreTrajectoryDataArtefactsAPI(EdgeAPI):
    def list(
        self,
        wellbore_trajectory_datum: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        wellbore_trajectory_datum_space: str = "IntegrationTestsImmutable",
        artefact: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        artefact_space: str = "IntegrationTestsImmutable",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List artefact edges of a wellbore trajectory datum.

        Args:
            wellbore_trajectory_datum: ID of the source wellbore trajectory data.
            wellbore_trajectory_datum_space: Location of the wellbore trajectory data.
            artefact: ID of the target artefacts.
            artefact_space: Location of the artefacts.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of artefact edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested artefact edges.

        Examples:

            List 5 artefact edges connected to "my_wellbore_trajectory_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.artefacts_edge.list("my_wellbore_trajectory_datum", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.Artefacts"),
            wellbore_trajectory_datum,
            wellbore_trajectory_datum_space,
            artefact,
            artefact_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
