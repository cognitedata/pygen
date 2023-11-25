from __future__ import annotations

import datetime

from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI


class RoleNominationAPI(EdgeAPI):
    def list(
        self,
        nomination: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        nomination_space: str = "IntegrationTestsImmutable",
        nomination: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        nomination_space: str = "IntegrationTestsImmutable",
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> NominationList:
        """List nomination edges of a role.

        Args:
            nomination: ID of the source nominations.
            nomination_space: Location of the nominations.
            nomination: ID of the target nominations.
            nomination_space: Location of the nominations.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_year: The minimum value of the year to filter on.
            max_year: The maximum value of the year to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of nomination edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested nomination edges.

        Examples:

            List 5 nomination edges connected to "my_role":

                >>> from movie_domain.client import MovieClient
                >>> client = MovieClient()
                >>> role = client.role.nomination_edge.list("my_role", limit=5)

        """
        f = dm.filters
        filter_ = _create_start_end_time_filter(
            self._view_id,
            nomination,
            nomination_space,
            nomination,
            nomination_space,
            name,
            name_prefix,
            min_year,
            max_year,
            external_id_prefix,
            space,
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "Role.nomination"},
            ),
        )
        return self._list(filter_=filter_, limit=limit)
