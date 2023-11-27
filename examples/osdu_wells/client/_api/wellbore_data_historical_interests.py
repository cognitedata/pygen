from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class WellboreDataHistoricalInterestsAPI(EdgeAPI):
    def list(
        self,
        wellbore_datum: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        wellbore_datum_space: str = "IntegrationTestsImmutable",
        historical_interest: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        historical_interest_space: str = "IntegrationTestsImmutable",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List historical interest edges of a wellbore datum.

        Args:
            wellbore_datum: ID of the source wellbore data.
            wellbore_datum_space: Location of the wellbore data.
            historical_interest: ID of the target historical interests.
            historical_interest_space: Location of the historical interests.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of historical interest edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested historical interest edges.

        Examples:

            List 5 historical interest edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.historical_interests_edge.list("my_wellbore_datum", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.HistoricalInterests"),
            wellbore_datum,
            wellbore_datum_space,
            historical_interest,
            historical_interest_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
