from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from osdu_wells.client.data_classes._core import DEFAULT_INSTANCE_SPACE


class WellboreMetaAPI(EdgeAPI):
    def list(
        self,
        from_wellbore: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_wellbore_space: str = DEFAULT_INSTANCE_SPACE,
        to_meta: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_meta_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List meta edges of a wellbore.

        Args:
            from_wellbore: ID of the source wellbore.
            from_wellbore_space: Location of the wellbores.
            to_meta: ID of the target meta.
            to_meta_space: Location of the metas.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of meta edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested meta edges.

        Examples:

            List 5 meta edges connected to "my_wellbore":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore = client.wellbore.meta_edge.list("my_wellbore", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Wellbore.meta"),
            from_wellbore,
            from_wellbore_space,
            to_meta,
            to_meta_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
