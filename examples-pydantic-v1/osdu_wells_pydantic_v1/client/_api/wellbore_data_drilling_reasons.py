from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from osdu_wells_pydantic_v1.client.data_classes._core import DEFAULT_INSTANCE_SPACE


class WellboreDataDrillingReasonsAPI(EdgeAPI):
    def list(
        self,
        from_wellbore_datum: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_wellbore_datum_space: str = DEFAULT_INSTANCE_SPACE,
        to_drilling_reason: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_drilling_reason_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List drilling reason edges of a wellbore datum.

        Args:
            from_wellbore_datum: ID of the source wellbore datum.
            from_wellbore_datum_space: Location of the wellbore data.
            to_drilling_reason: ID of the target drilling reason.
            to_drilling_reason_space: Location of the drilling reasons.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of drilling reason edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested drilling reason edges.

        Examples:

            List 5 drilling reason edges connected to "my_wellbore_datum":

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.drilling_reasons_edge.list("my_wellbore_datum", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.DrillingReasons"),
            from_wellbore_datum,
            from_wellbore_datum_space,
            to_drilling_reason,
            to_drilling_reason_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
