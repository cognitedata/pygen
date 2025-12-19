from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Literal
from cognite.client import data_modeling as dm

from omni.data_classes import (
    ConnectionEdgeA,
    ConnectionEdgeAList,
    ConnectionEdgeAWrite,
)
from omni.data_classes._connection_edge_a import _create_connection_edge_a_filter

from omni._api._core import DEFAULT_LIMIT_READ, EdgePropertyAPI
from omni.data_classes._core import DEFAULT_INSTANCE_SPACE


class ConnectionItemEInwardsSinglePropertyAPI(EdgePropertyAPI):
    _view_id = dm.ViewId("sp_pygen_models", "ConnectionEdgeA", "1")
    _class_type = ConnectionEdgeA
    _class_write_type = ConnectionEdgeAWrite
    _class_list = ConnectionEdgeAList

    def list(
        self,
        from_connection_item_e: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_connection_item_e_space: str = DEFAULT_INSTANCE_SPACE,
        to_connection_item_f: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_connection_item_f_space: str = DEFAULT_INSTANCE_SPACE,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> ConnectionEdgeAList:
        """List inwards single property edges of a connection item e.

        Args:
            from_connection_item_e: ID of the source connection item e.
            from_connection_item_e_space: Location of the connection item es.
            to_connection_item_f: ID of the target connection item f.
            to_connection_item_f_space: Location of the connection item fs.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of inwards single property edges to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.

        Returns:
            The requested inwards single property edges.

        Examples:

            List 5 inwards single property edges connected to "my_connection_item_e":

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_e = client.connection_item_e.inwards_single_property_edge.list(
                ...     "my_connection_item_e", limit=5
                ... )

        """
        filter_ = _create_connection_edge_a_filter(
            dm.DirectRelationReference("sp_pygen_models", "multiProperty"),
            self._view_id,
            to_connection_item_f,
            to_connection_item_f_space,
            from_connection_item_e,
            from_connection_item_e_space,
            min_end_time,
            max_end_time,
            name,
            name_prefix,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
