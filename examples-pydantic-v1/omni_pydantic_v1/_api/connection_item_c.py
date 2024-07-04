from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from omni_pydantic_v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni_pydantic_v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ConnectionItemC,
    ConnectionItemCWrite,
    ConnectionItemCList,
    ConnectionItemCWriteList,
)
from omni_pydantic_v1.data_classes._connection_item_c import (
    _create_connection_item_c_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .connection_item_c_connection_item_a import ConnectionItemCConnectionItemAAPI
from .connection_item_c_connection_item_b import ConnectionItemCConnectionItemBAPI
from .connection_item_c_query import ConnectionItemCQueryAPI


class ConnectionItemCAPI(NodeAPI[ConnectionItemC, ConnectionItemCWrite, ConnectionItemCList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[ConnectionItemC]
        super().__init__(
            client=client,
            sources=None,
            class_type=ConnectionItemC,
            class_list=ConnectionItemCList,
            class_write_list=ConnectionItemCWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.connection_item_a_edge = ConnectionItemCConnectionItemAAPI(client)
        self.connection_item_b_edge = ConnectionItemCConnectionItemBAPI(client)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ConnectionItemCQueryAPI[ConnectionItemCList]:
        """Query starting at connection item cs.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item cs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for connection item cs.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_connection_item_c_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ConnectionItemCList)
        return ConnectionItemCQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        connection_item_c: ConnectionItemCWrite | Sequence[ConnectionItemCWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) connection item cs.

        Note: This method iterates through all nodes and timeseries linked to connection_item_c and creates them including the edges
        between the nodes. For example, if any of `connection_item_a` or `connection_item_b` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            connection_item_c: Connection item c or sequence of connection item cs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new connection_item_c:

                >>> from omni_pydantic_v1 import OmniClient
                >>> from omni_pydantic_v1.data_classes import ConnectionItemCWrite
                >>> client = OmniClient()
                >>> connection_item_c = ConnectionItemCWrite(external_id="my_connection_item_c", ...)
                >>> result = client.connection_item_c.apply(connection_item_c)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.connection_item_c.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(connection_item_c, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more connection item c.

        Args:
            external_id: External id of the connection item c to delete.
            space: The space where all the connection item c are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete connection_item_c by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> client.connection_item_c.delete("my_connection_item_c")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.connection_item_c.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ConnectionItemC | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ConnectionItemCList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ConnectionItemC | ConnectionItemCList | None:
        """Retrieve one or more connection item cs by id(s).

        Args:
            external_id: External id or list of external ids of the connection item cs.
            space: The space where all the connection item cs are located.

        Returns:
            The requested connection item cs.

        Examples:

            Retrieve connection_item_c by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> connection_item_c = client.connection_item_c.retrieve("my_connection_item_c")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.connection_item_a_edge,
                    "connection_item_a",
                    dm.DirectRelationReference("pygen-models", "unidirectional"),
                    "outwards",
                    dm.ViewId("pygen-models", "ConnectionItemA", "1"),
                ),
                (
                    self.connection_item_b_edge,
                    "connection_item_b",
                    dm.DirectRelationReference("pygen-models", "unidirectional"),
                    "outwards",
                    dm.ViewId("pygen-models", "ConnectionItemB", "1"),
                ),
            ],
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ConnectionItemCFields | Sequence[ConnectionItemCFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_edges: bool = True,
    ) -> ConnectionItemCList:
        """List/filter connection item cs

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item cs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_edges: Whether to retrieve `connection_item_a` or `connection_item_b` external ids for the connection item cs. Defaults to True.

        Returns:
            List of requested connection item cs

        Examples:

            List connection item cs and limit to 5:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> connection_item_cs = client.connection_item_c.list(limit=5)

        """
        filter_ = _create_connection_item_c_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            properties_by_field=_CONNECTIONITEMC_PROPERTIES_BY_FIELD,
            sort_by=sort_by,
            direction=direction,
            sort=sort,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.connection_item_a_edge,
                    "connection_item_a",
                    dm.DirectRelationReference("pygen-models", "unidirectional"),
                    "outwards",
                    dm.ViewId("pygen-models", "ConnectionItemA", "1"),
                ),
                (
                    self.connection_item_b_edge,
                    "connection_item_b",
                    dm.DirectRelationReference("pygen-models", "unidirectional"),
                    "outwards",
                    dm.ViewId("pygen-models", "ConnectionItemB", "1"),
                ),
            ],
        )
