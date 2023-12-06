from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from osdu_wells.client.data_classes._core import DEFAULT_INSTANCE_SPACE


class TechnicalAssurancesAcceptableUsageAPI(EdgeAPI):
    def list(
        self,
        from_technical_assurance: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_technical_assurance_space: str = DEFAULT_INSTANCE_SPACE,
        to_acceptable_usage: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_acceptable_usage_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List acceptable usage edges of a technical assurance.

        Args:
            from_technical_assurance: ID of the source technical assurance.
            from_technical_assurance_space: Location of the technical assurances.
            to_acceptable_usage: ID of the target acceptable usage.
            to_acceptable_usage_space: Location of the acceptable usages.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of acceptable usage edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested acceptable usage edges.

        Examples:

            List 5 acceptable usage edges connected to "my_technical_assurance":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurance = client.technical_assurances.acceptable_usage_edge.list("my_technical_assurance", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.AcceptableUsage"),
            from_technical_assurance,
            from_technical_assurance_space,
            to_acceptable_usage,
            to_acceptable_usage_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
