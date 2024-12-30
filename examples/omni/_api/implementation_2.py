from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni.config import global_config
from omni._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from omni.data_classes._implementation_2 import (
    Implementation2Query,
    _IMPLEMENTATION2_PROPERTIES_BY_FIELD,
    _create_implementation_2_filter,
)
from omni.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Implementation2,
    Implementation2Write,
    Implementation2Fields,
    Implementation2List,
    Implementation2WriteList,
    Implementation2TextFields,
)
from omni._api.implementation_2_query import Implementation2QueryAPI


class Implementation2API(NodeAPI[Implementation2, Implementation2Write, Implementation2List, Implementation2WriteList]):
    _view_id = dm.ViewId("sp_pygen_models", "Implementation2", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _IMPLEMENTATION2_PROPERTIES_BY_FIELD
    _class_type = Implementation2
    _class_list = Implementation2List
    _class_write_list = Implementation2WriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> Implementation2QueryAPI[Implementation2, Implementation2List]:
        """Query starting at implementation 2.

        Args:
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 2 to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for implementation 2.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_implementation_2_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return Implementation2QueryAPI(
            self._client, QueryBuilder(), self._class_type, self._class_list, None, filter_, limit
        )

    def apply(
        self,
        implementation_2: Implementation2Write | Sequence[Implementation2Write],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) implementation 2.

        Args:
            implementation_2: Implementation 2 or
                sequence of implementation 2 to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None.
                However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new implementation_2:

                >>> from omni import OmniClient
                >>> from omni.data_classes import Implementation2Write
                >>> client = OmniClient()
                >>> implementation_2 = Implementation2Write(
                ...     external_id="my_implementation_2", ...
                ... )
                >>> result = client.implementation_2.apply(implementation_2)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.implementation_2.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(implementation_2, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more implementation 2.

        Args:
            external_id: External id of the implementation 2 to delete.
            space: The space where all the implementation 2 are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete implementation_2 by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.implementation_2.delete("my_implementation_2")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.implementation_2.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> Implementation2 | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> Implementation2List: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> Implementation2 | Implementation2List | None:
        """Retrieve one or more implementation 2 by id(s).

        Args:
            external_id: External id or list of external ids of the implementation 2.
            space: The space where all the implementation 2 are located.

        Returns:
            The requested implementation 2.

        Examples:

            Retrieve implementation_2 by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> implementation_2 = client.implementation_2.retrieve(
                ...     "my_implementation_2"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: Implementation2TextFields | SequenceNotStr[Implementation2TextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Implementation2Fields | SequenceNotStr[Implementation2Fields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Implementation2List:
        """Search implementation 2

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 2 to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results implementation 2 matching the query.

        Examples:

           Search for 'my_implementation_2' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> implementation_2_list = client.implementation_2.search(
                ...     'my_implementation_2'
                ... )

        """
        filter_ = _create_implementation_2_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: Implementation2Fields | SequenceNotStr[Implementation2Fields] | None = None,
        query: str | None = None,
        search_property: Implementation2TextFields | SequenceNotStr[Implementation2TextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue: ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: Implementation2Fields | SequenceNotStr[Implementation2Fields] | None = None,
        query: str | None = None,
        search_property: Implementation2TextFields | SequenceNotStr[Implementation2TextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: Implementation2Fields | SequenceNotStr[Implementation2Fields],
        property: Implementation2Fields | SequenceNotStr[Implementation2Fields] | None = None,
        query: str | None = None,
        search_property: Implementation2TextFields | SequenceNotStr[Implementation2TextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: Implementation2Fields | SequenceNotStr[Implementation2Fields] | None = None,
        property: Implementation2Fields | SequenceNotStr[Implementation2Fields] | None = None,
        query: str | None = None,
        search_property: Implementation2TextFields | SequenceNotStr[Implementation2TextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across implementation 2

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 2 to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count implementation 2 in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.implementation_2.aggregate("count", space="my_space")

        """

        filter_ = _create_implementation_2_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: Implementation2Fields,
        interval: float,
        query: str | None = None,
        search_property: Implementation2TextFields | SequenceNotStr[Implementation2TextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for implementation 2

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 2 to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_implementation_2_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    def select(self) -> Implementation2Query:
        """Start selecting from implementation 2."""
        return Implementation2Query(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> Implementation2List:
        builder = QueryBuilder()
        factory = QueryStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                sort=sort,
                limit=limit,
                has_container_fields=True,
            )
        )
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        unpacked = QueryUnpacker(builder, edges=unpack_edges).unpack()
        if global_config.validate_retrieve:
            retrieved = [Implementation2.model_validate(item) for item in unpacked]
        else:
            retrieved = [Implementation2.model_construct(**item) for item in unpacked]
        return Implementation2List(retrieved)

    def list(
        self,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Implementation2Fields | Sequence[Implementation2Fields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Implementation2List:
        """List/filter implementation 2

        Args:
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 2 to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested implementation 2

        Examples:

            List implementation 2 and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> implementation_2_list = client.implementation_2.list(limit=5)

        """
        filter_ = _create_implementation_2_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
