from __future__ import annotations

from collections.abc import Sequence
from typing import Literal
from cognite.client import data_modeling as dm

from omni._api._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from omni.data_classes._core import DEFAULT_INSTANCE_SPACE


class DependentOnNonWritableToNonWritableAPI(EdgeAPI):
    def list(
        self,
        from_dependent_on_non_writable: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_dependent_on_non_writable_space: str = DEFAULT_INSTANCE_SPACE,
        to_implementation_1_non_writeable: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_implementation_1_non_writeable_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List to non writable edges of a dependent on non writable.

        Args:
            from_dependent_on_non_writable: ID of the source dependent on non writable.
            from_dependent_on_non_writable_space: Location of the dependent on non writables.
            to_implementation_1_non_writeable: ID of the target implementation 1 non writeable.
            to_implementation_1_non_writeable_space: Location of the implementation 1 non writeables.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of to non writable edges to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.

        Returns:
            The requested to non writable edges.

        Examples:

            List 5 to non writable edges connected to "my_dependent_on_non_writable":

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> dependent_on_non_writable = client.dependent_on_non_writable.to_non_writable_edge.list(
                ...     "my_dependent_on_non_writable", limit=5
                ... )

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_pygen_models", "toNonWritable"),
            from_dependent_on_non_writable,
            from_dependent_on_non_writable_space,
            to_implementation_1_non_writeable,
            to_implementation_1_non_writeable_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
