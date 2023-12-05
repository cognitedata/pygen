from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from osdu_wells_pydantic_v1.client.data_classes._core import DEFAULT_INSTANCE_SPACE


class TechnicalAssurancesReviewersAPI(EdgeAPI):
    def list(
        self,
        from_technical_assurance: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_technical_assurance_space: str = DEFAULT_INSTANCE_SPACE,
        to_reviewer: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_reviewer_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List reviewer edges of a technical assurance.

        Args:
            from_technical_assurance: ID of the source technical assurance.
            from_technical_assurance_space: Location of the technical assurances.
            to_reviewer: ID of the target reviewer.
            to_reviewer_space: Location of the reviewers.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of reviewer edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested reviewer edges.

        Examples:

            List 5 reviewer edges connected to "my_technical_assurance":

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurance = client.technical_assurances.reviewers_edge.list("my_technical_assurance", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.Reviewers"),
            from_technical_assurance,
            from_technical_assurance_space,
            to_reviewer,
            to_reviewer_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
