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
    NodeAPI,
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
from omni.data_classes._dependent_on_non_writable import (
    DependentOnNonWritableQuery,
    _DEPENDENTONNONWRITABLE_PROPERTIES_BY_FIELD,
    _create_dependent_on_non_writable_filter,
)
from omni.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    DependentOnNonWritable,
    DependentOnNonWritableWrite,
    DependentOnNonWritableFields,
    DependentOnNonWritableList,
    DependentOnNonWritableWriteList,
    DependentOnNonWritableTextFields,
    Implementation1NonWriteable,
)
from omni._api.dependent_on_non_writable_to_non_writable import DependentOnNonWritableToNonWritableAPI


class DependentOnNonWritableAPI(
    NodeAPI[
        DependentOnNonWritable, DependentOnNonWritableWrite, DependentOnNonWritableList, DependentOnNonWritableWriteList
    ]
):
    _view_id = dm.ViewId("sp_pygen_models", "DependentOnNonWritable", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _DEPENDENTONNONWRITABLE_PROPERTIES_BY_FIELD
    _class_type = DependentOnNonWritable
    _class_list = DependentOnNonWritableList
    _class_write_list = DependentOnNonWritableWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.to_non_writable_edge = DependentOnNonWritableToNonWritableAPI(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> DependentOnNonWritable | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> DependentOnNonWritableList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> DependentOnNonWritable | DependentOnNonWritableList | None:
        """Retrieve one or more dependent on non writables by id(s).

        Args:
            external_id: External id or list of external ids of the dependent on non writables.
            space: The space where all the dependent on non writables are located.
            retrieve_connections: Whether to retrieve `to_non_writable` for the dependent on non writables. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested dependent on non writables.

        Examples:

            Retrieve dependent_on_non_writable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> dependent_on_non_writable = client.dependent_on_non_writable.retrieve(
                ...     "my_dependent_on_non_writable"
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
        properties: DependentOnNonWritableTextFields | SequenceNotStr[DependentOnNonWritableTextFields] | None = None,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> DependentOnNonWritableList:
        """Search dependent on non writables

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            a_value: The a value to filter on.
            a_value_prefix: The prefix of the a value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of dependent on non writables to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results dependent on non writables matching the query.

        Examples:

           Search for 'my_dependent_on_non_writable' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> dependent_on_non_writables = client.dependent_on_non_writable.search(
                ...     'my_dependent_on_non_writable'
                ... )

        """
        filter_ = _create_dependent_on_non_writable_filter(
            self._view_id,
            a_value,
            a_value_prefix,
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
        property: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields] | None = None,
        query: str | None = None,
        search_property: (
            DependentOnNonWritableTextFields | SequenceNotStr[DependentOnNonWritableTextFields] | None
        ) = None,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
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
        property: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields] | None = None,
        query: str | None = None,
        search_property: (
            DependentOnNonWritableTextFields | SequenceNotStr[DependentOnNonWritableTextFields] | None
        ) = None,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
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
        group_by: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields],
        property: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields] | None = None,
        query: str | None = None,
        search_property: (
            DependentOnNonWritableTextFields | SequenceNotStr[DependentOnNonWritableTextFields] | None
        ) = None,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
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
        group_by: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields] | None = None,
        property: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields] | None = None,
        query: str | None = None,
        search_property: (
            DependentOnNonWritableTextFields | SequenceNotStr[DependentOnNonWritableTextFields] | None
        ) = None,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across dependent on non writables

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            a_value: The a value to filter on.
            a_value_prefix: The prefix of the a value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of dependent on non writables to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count dependent on non writables in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.dependent_on_non_writable.aggregate("count", space="my_space")

        """

        filter_ = _create_dependent_on_non_writable_filter(
            self._view_id,
            a_value,
            a_value_prefix,
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
        property: DependentOnNonWritableFields,
        interval: float,
        query: str | None = None,
        search_property: (
            DependentOnNonWritableTextFields | SequenceNotStr[DependentOnNonWritableTextFields] | None
        ) = None,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for dependent on non writables

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            a_value: The a value to filter on.
            a_value_prefix: The prefix of the a value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of dependent on non writables to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_dependent_on_non_writable_filter(
            self._view_id,
            a_value,
            a_value_prefix,
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

    def select(self) -> DependentOnNonWritableQuery:
        """Start selecting from dependent on non writables."""
        return DependentOnNonWritableQuery(self._client)

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
        if retrieve_connections == "identifier" or retrieve_connections == "full":
            builder.extend(
                factory.from_edge(
                    Implementation1NonWriteable._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "toNonWritable"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[DependentOnNonWritableList]:
        """Iterate over dependent on non writables

        Args:
            chunk_size: The number of dependent on non writables to return in each iteration. Defaults to 100.
            a_value: The a value to filter on.
            a_value_prefix: The prefix of the a value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `to_non_writable` for the dependent on non writables. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of dependent on non writables to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of dependent on non writables

        Examples:

            Iterate dependent on non writables in chunks of 100 up to 2000 items:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for dependent_on_non_writables in client.dependent_on_non_writable.iterate(chunk_size=100, limit=2000):
                ...     for dependent_on_non_writable in dependent_on_non_writables:
                ...         print(dependent_on_non_writable.external_id)

            Iterate dependent on non writables in chunks of 100 sorted by external_id in descending order:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for dependent_on_non_writables in client.dependent_on_non_writable.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for dependent_on_non_writable in dependent_on_non_writables:
                ...         print(dependent_on_non_writable.external_id)

            Iterate dependent on non writables in chunks of 100 and use cursors to resume the iteration:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for first_iteration in client.dependent_on_non_writable.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for dependent_on_non_writables in client.dependent_on_non_writable.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for dependent_on_non_writable in dependent_on_non_writables:
                ...         print(dependent_on_non_writable.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_dependent_on_non_writable_filter(
            self._view_id,
            a_value,
            a_value_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: DependentOnNonWritableFields | Sequence[DependentOnNonWritableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> DependentOnNonWritableList:
        """List/filter dependent on non writables

        Args:
            a_value: The a value to filter on.
            a_value_prefix: The prefix of the a value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of dependent on non writables to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `to_non_writable` for the dependent on non writables. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested dependent on non writables

        Examples:

            List dependent on non writables and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> dependent_on_non_writables = client.dependent_on_non_writable.list(limit=5)

        """
        filter_ = _create_dependent_on_non_writable_filter(
            self._view_id,
            a_value,
            a_value_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
