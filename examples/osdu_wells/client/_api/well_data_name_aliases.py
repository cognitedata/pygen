from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class WellDataNameAliasesAPI(EdgeAPI):
    def list(
        self,
        well_datum: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        well_datum_space: str = "IntegrationTestsImmutable",
        name_alias: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        name_alias_space: str = "IntegrationTestsImmutable",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List name alias edges of a well datum.

        Args:
            well_datum: ID of the source well data.
            well_datum_space: Location of the well data.
            name_alias: ID of the target name aliases.
            name_alias_space: Location of the name aliases.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of name alias edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested name alias edges.

        Examples:

            List 5 name alias edges connected to "my_well_datum":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> well_datum = client.well_data.name_aliases_edge.list("my_well_datum", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.NameAliases"),
            well_datum,
            well_datum_space,
            name_alias,
            name_alias_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
