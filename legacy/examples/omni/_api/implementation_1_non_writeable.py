from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeReadAPI,
    SequenceNotStr,
)
from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from omni.data_classes._implementation_1_non_writeable import (
    Implementation1NonWriteableQuery,
    _IMPLEMENTATION1NONWRITEABLE_PROPERTIES_BY_FIELD,
    _create_implementation_1_non_writeable_filter,
)
from omni.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Implementation1NonWriteable,
    Implementation1NonWriteableFields,
    Implementation1NonWriteableList,
    Implementation1NonWriteableTextFields,
    Implementation1,
)


class Implementation1NonWriteableAPI(NodeReadAPI[Implementation1NonWriteable, Implementation1NonWriteableList]):
    _view_id = dm.ViewId("sp_pygen_models", "Implementation1NonWriteable", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _IMPLEMENTATION1NONWRITEABLE_PROPERTIES_BY_FIELD
    _class_type = Implementation1NonWriteable
    _class_list = Implementation1NonWriteableList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Implementation1NonWriteable | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Implementation1NonWriteableList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Implementation1NonWriteable | Implementation1NonWriteableList | None:
        """Retrieve one or more implementation 1 non writeables by id(s).

        Args:
            external_id: External id or list of external ids of the implementation 1 non writeables.
            space: The space where all the implementation 1 non writeables are located.
            retrieve_connections: Whether to retrieve `connection_value` for the implementation 1 non writeables.
            Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier
            of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested implementation 1 non writeables.

        Examples:

            Retrieve implementation_1_non_writeable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> implementation_1_non_writeable = client.implementation_1_non_writeable.retrieve(
                ...     "my_implementation_1_non_writeable"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
        )

    def search(
        self,
        query: str,
        properties: (
            Implementation1NonWriteableTextFields | SequenceNotStr[Implementation1NonWriteableTextFields] | None
        ) = None,
        connection_value: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Implementation1NonWriteableList:
        """Search implementation 1 non writeables

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            connection_value: The connection value to filter on.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 non writeables to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results implementation 1 non writeables matching the query.

        Examples:

           Search for 'my_implementation_1_non_writeable' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> implementation_1_non_writeables = client.implementation_1_non_writeable.search(
                ...     'my_implementation_1_non_writeable'
                ... )

        """
        filter_ = _create_implementation_1_non_writeable_filter(
            self._view_id,
            connection_value,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
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
        property: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields] | None = None,
        query: str | None = None,
        search_property: (
            Implementation1NonWriteableTextFields | SequenceNotStr[Implementation1NonWriteableTextFields] | None
        ) = None,
        connection_value: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
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
        property: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields] | None = None,
        query: str | None = None,
        search_property: (
            Implementation1NonWriteableTextFields | SequenceNotStr[Implementation1NonWriteableTextFields] | None
        ) = None,
        connection_value: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
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
        group_by: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields],
        property: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields] | None = None,
        query: str | None = None,
        search_property: (
            Implementation1NonWriteableTextFields | SequenceNotStr[Implementation1NonWriteableTextFields] | None
        ) = None,
        connection_value: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
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
        group_by: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields] | None = None,
        property: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields] | None = None,
        query: str | None = None,
        search_property: (
            Implementation1NonWriteableTextFields | SequenceNotStr[Implementation1NonWriteableTextFields] | None
        ) = None,
        connection_value: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across implementation 1 non writeables

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            connection_value: The connection value to filter on.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 non writeables to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count implementation 1 non writeables in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.implementation_1_non_writeable.aggregate("count", space="my_space")

        """

        filter_ = _create_implementation_1_non_writeable_filter(
            self._view_id,
            connection_value,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
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
        property: Implementation1NonWriteableFields,
        interval: float,
        query: str | None = None,
        search_property: (
            Implementation1NonWriteableTextFields | SequenceNotStr[Implementation1NonWriteableTextFields] | None
        ) = None,
        connection_value: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for implementation 1 non writeables

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            connection_value: The connection value to filter on.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 non writeables to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_implementation_1_non_writeable_filter(
            self._view_id,
            connection_value,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
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

    def select(self) -> Implementation1NonWriteableQuery:
        """Start selecting from implementation 1 non writeables."""
        return Implementation1NonWriteableQuery(self._client)

    def _build(
        self,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        chunk_size: int | None = None,
    ) -> QueryExecutor:
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                sort=sort,
                limit=limit,
                max_retrieve_batch_limit=chunk_size,
                has_container_fields=True,
            )
        )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    Implementation1._view_id,
                    ViewPropertyId(self._view_id, "connectionValue"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        connection_value: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[Implementation1NonWriteableList]:
        """Iterate over implementation 1 non writeables

        Args:
            chunk_size: The number of implementation 1 non writeables to return in each iteration. Defaults to 100.
            connection_value: The connection value to filter on.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `connection_value` for the implementation 1 non writeables.
            Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier
            of the connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of implementation 1 non writeables to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of implementation 1 non writeables

        Examples:

            Iterate implementation 1 non writeables in chunks of 100 up to 2000 items:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for implementation_1_non_writeables in client.implementation_1_non_writeable.iterate(chunk_size=100, limit=2000):
                ...     for implementation_1_non_writeable in implementation_1_non_writeables:
                ...         print(implementation_1_non_writeable.external_id)

            Iterate implementation 1 non writeables in chunks of 100 sorted by external_id in descending order:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for implementation_1_non_writeables in client.implementation_1_non_writeable.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for implementation_1_non_writeable in implementation_1_non_writeables:
                ...         print(implementation_1_non_writeable.external_id)

            Iterate implementation 1 non writeables in chunks of 100 and use cursors to resume the iteration:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for first_iteration in client.implementation_1_non_writeable.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for implementation_1_non_writeables in client.implementation_1_non_writeable.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for implementation_1_non_writeable in implementation_1_non_writeables:
                ...         print(implementation_1_non_writeable.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_implementation_1_non_writeable_filter(
            self._view_id,
            connection_value,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        connection_value: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Implementation1NonWriteableFields | Sequence[Implementation1NonWriteableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Implementation1NonWriteableList:
        """List/filter implementation 1 non writeables

        Args:
            connection_value: The connection value to filter on.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 non writeables to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `connection_value` for the implementation 1 non writeables.
            Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier
            of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested implementation 1 non writeables

        Examples:

            List implementation 1 non writeables and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> implementation_1_non_writeables = client.implementation_1_non_writeable.list(limit=5)

        """
        filter_ = _create_implementation_1_non_writeable_filter(
            self._view_id,
            connection_value,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
