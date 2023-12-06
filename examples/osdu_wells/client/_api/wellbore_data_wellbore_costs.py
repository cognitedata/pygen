from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from osdu_wells.client.data_classes._core import DEFAULT_INSTANCE_SPACE


class WellboreDataWellboreCostsAPI(EdgeAPI):
    def list(
        self,
        from_wellbore_datum: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_wellbore_datum_space: str = DEFAULT_INSTANCE_SPACE,
        to_wellbore_cost: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_wellbore_cost_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List wellbore cost edges of a wellbore datum.

        Args:
            from_wellbore_datum: ID of the source wellbore datum.
            from_wellbore_datum_space: Location of the wellbore data.
            to_wellbore_cost: ID of the target wellbore cost.
            to_wellbore_cost_space: Location of the wellbore costs.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore cost edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested wellbore cost edges.

        Examples:

            List 5 wellbore cost edges connected to "my_wellbore_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.wellbore_costs_edge.list("my_wellbore_datum", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.WellboreCosts"),
            from_wellbore_datum,
            from_wellbore_datum_space,
            to_wellbore_cost,
            to_wellbore_cost_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
