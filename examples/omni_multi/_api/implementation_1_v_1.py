from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni_multi._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from omni_multi.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from omni_multi.data_classes._implementation_1_v_1 import (
    Implementation1v1Query,
    _IMPLEMENTATION1V1_PROPERTIES_BY_FIELD,
    _create_implementation_1_v_1_filter,
)
from omni_multi.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Implementation1v1,
    Implementation1v1Write,
    Implementation1v1Fields,
    Implementation1v1List,
    Implementation1v1WriteList,
    Implementation1v1TextFields,
)


class Implementation1v1API(
    NodeAPI[Implementation1v1, Implementation1v1Write, Implementation1v1List, Implementation1v1WriteList]
):
    _view_id = dm.ViewId("pygen-models-other", "Implementation1", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _IMPLEMENTATION1V1_PROPERTIES_BY_FIELD
    _class_type = Implementation1v1
    _class_list = Implementation1v1List
    _class_write_list = Implementation1v1WriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str,
    ) -> Implementation1v1 | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
    ) -> Implementation1v1List: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
    ) -> Implementation1v1 | Implementation1v1List | None:
        """Retrieve one or more implementation 1 v 1 by id(s).

        Args:
            external_id: External id or list of external ids of the implementation 1 v 1.
            space: The space where all the implementation 1 v 1 are located.

        Returns:
            The requested implementation 1 v 1.

        Examples:

            Retrieve implementation_1_v_1 by id:

                >>> from omni_multi import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> implementation_1_v_1 = client.implementation_1_v_1.retrieve(
                ...     "my_implementation_1_v_1"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: Implementation1v1TextFields | SequenceNotStr[Implementation1v1TextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Implementation1v1Fields | SequenceNotStr[Implementation1v1Fields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Implementation1v1List:
        """Search implementation 1 v 1

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            value_2: The value 2 to filter on.
            value_2_prefix: The prefix of the value 2 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 v 1 to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results implementation 1 v 1 matching the query.

        Examples:

           Search for 'my_implementation_1_v_1' in all text properties:

                >>> from omni_multi import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> implementation_1_v_1_list = client.implementation_1_v_1.search(
                ...     'my_implementation_1_v_1'
                ... )

        """
        filter_ = _create_implementation_1_v_1_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            value_1,
            value_1_prefix,
            value_2,
            value_2_prefix,
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
        property: Implementation1v1Fields | SequenceNotStr[Implementation1v1Fields] | None = None,
        query: str | None = None,
        search_property: Implementation1v1TextFields | SequenceNotStr[Implementation1v1TextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
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
        property: Implementation1v1Fields | SequenceNotStr[Implementation1v1Fields] | None = None,
        query: str | None = None,
        search_property: Implementation1v1TextFields | SequenceNotStr[Implementation1v1TextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
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
        group_by: Implementation1v1Fields | SequenceNotStr[Implementation1v1Fields],
        property: Implementation1v1Fields | SequenceNotStr[Implementation1v1Fields] | None = None,
        query: str | None = None,
        search_property: Implementation1v1TextFields | SequenceNotStr[Implementation1v1TextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
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
        group_by: Implementation1v1Fields | SequenceNotStr[Implementation1v1Fields] | None = None,
        property: Implementation1v1Fields | SequenceNotStr[Implementation1v1Fields] | None = None,
        query: str | None = None,
        search_property: Implementation1v1TextFields | SequenceNotStr[Implementation1v1TextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across implementation 1 v 1

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            value_2: The value 2 to filter on.
            value_2_prefix: The prefix of the value 2 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 v 1 to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count implementation 1 v 1 in space `my_space`:

                >>> from omni_multi import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> result = client.implementation_1_v_1.aggregate("count", space="my_space")

        """

        filter_ = _create_implementation_1_v_1_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            value_1,
            value_1_prefix,
            value_2,
            value_2_prefix,
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
        property: Implementation1v1Fields,
        interval: float,
        query: str | None = None,
        search_property: Implementation1v1TextFields | SequenceNotStr[Implementation1v1TextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for implementation 1 v 1

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            value_2: The value 2 to filter on.
            value_2_prefix: The prefix of the value 2 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 v 1 to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_implementation_1_v_1_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            value_1,
            value_1_prefix,
            value_2,
            value_2_prefix,
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

    def select(self) -> Implementation1v1Query:
        """Start selecting from implementation 1 v 1."""
        return Implementation1v1Query(self._client)

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
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[Implementation1v1List]:
        """Iterate over implementation 1 v 1

        Args:
            chunk_size: The number of implementation 1 v 1 to return in each iteration. Defaults to 100.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            value_2: The value 2 to filter on.
            value_2_prefix: The prefix of the value 2 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of implementation 1 v 1 to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of implementation 1 v 1

        Examples:

            Iterate implementation 1 v 1 in chunks of 100 up to 2000 items:

                >>> from omni_multi import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> for implementation_1_v_1_list in client.implementation_1_v_1.iterate(chunk_size=100, limit=2000):
                ...     for implementation_1_v_1 in implementation_1_v_1_list:
                ...         print(implementation_1_v_1.external_id)

            Iterate implementation 1 v 1 in chunks of 100 sorted by external_id in descending order:

                >>> from omni_multi import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> for implementation_1_v_1_list in client.implementation_1_v_1.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for implementation_1_v_1 in implementation_1_v_1_list:
                ...         print(implementation_1_v_1.external_id)

            Iterate implementation 1 v 1 in chunks of 100 and use cursors to resume the iteration:

                >>> from omni_multi import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> for first_iteration in client.implementation_1_v_1.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for implementation_1_v_1_list in client.implementation_1_v_1.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for implementation_1_v_1 in implementation_1_v_1_list:
                ...         print(implementation_1_v_1.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_implementation_1_v_1_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            value_1,
            value_1_prefix,
            value_2,
            value_2_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

    def list(
        self,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Implementation1v1Fields | Sequence[Implementation1v1Fields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Implementation1v1List:
        """List/filter implementation 1 v 1

        Args:
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            value_2: The value 2 to filter on.
            value_2_prefix: The prefix of the value 2 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 v 1 to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested implementation 1 v 1

        Examples:

            List implementation 1 v 1 and limit to 5:

                >>> from omni_multi import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> implementation_1_v_1_list = client.implementation_1_v_1.list(limit=5)

        """
        filter_ = _create_implementation_1_v_1_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            value_1,
            value_1_prefix,
            value_2,
            value_2_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
