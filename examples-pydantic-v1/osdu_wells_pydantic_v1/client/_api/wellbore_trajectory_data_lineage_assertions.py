from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from osdu_wells_pydantic_v1.client.data_classes._core import DEFAULT_INSTANCE_SPACE


class WellboreTrajectoryDataLineageAssertionsAPI(EdgeAPI):
    def list(
        self,
        from_wellbore_trajectory_datum: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_wellbore_trajectory_datum_space: str = DEFAULT_INSTANCE_SPACE,
        to_lineage_assertion: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_lineage_assertion_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List lineage assertion edges of a wellbore trajectory datum.

        Args:
            from_wellbore_trajectory_datum: ID of the source wellbore trajectory datum.
            from_wellbore_trajectory_datum_space: Location of the wellbore trajectory data.
            to_lineage_assertion: ID of the target lineage assertion.
            to_lineage_assertion_space: Location of the lineage assertions.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of lineage assertion edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested lineage assertion edges.

        Examples:

            List 5 lineage assertion edges connected to "my_wellbore_trajectory_datum":

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.lineage_assertions_edge.list("my_wellbore_trajectory_datum", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.LineageAssertions"),
            from_wellbore_trajectory_datum,
            from_wellbore_trajectory_datum_space,
            to_lineage_assertion,
            to_lineage_assertion_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
