from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class TechnicalAssurancesUnacceptableUsageAPI(EdgeAPI):
    def list(
        self,
        technical_assurance: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        technical_assurance_space: str = "IntegrationTestsImmutable",
        unacceptable_usage: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        unacceptable_usage_space: str = "IntegrationTestsImmutable",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List unacceptable usage edges of a technical assurance.

        Args:
            technical_assurance: ID of the source technical assurances.
            technical_assurance_space: Location of the technical assurances.
            unacceptable_usage: ID of the target unacceptable usages.
            unacceptable_usage_space: Location of the unacceptable usages.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of unacceptable usage edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested unacceptable usage edges.

        Examples:

            List 5 unacceptable usage edges connected to "my_technical_assurance":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurance = client.technical_assurances.unacceptable_usage_edge.list("my_technical_assurance", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.UnacceptableUsage"),
            technical_assurance,
            technical_assurance_space,
            unacceptable_usage,
            unacceptable_usage_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
