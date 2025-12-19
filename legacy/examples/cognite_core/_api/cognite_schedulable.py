from __future__ import annotations

import datetime
import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite_core.data_classes._cognite_schedulable import (
    CogniteSchedulableQuery,
    _COGNITESCHEDULABLE_PROPERTIES_BY_FIELD,
    _create_cognite_schedulable_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CogniteSchedulable,
    CogniteSchedulableWrite,
    CogniteSchedulableFields,
    CogniteSchedulableList,
    CogniteSchedulableWriteList,
    CogniteSchedulableTextFields,
    CogniteActivity,
)


class CogniteSchedulableAPI(
    NodeAPI[CogniteSchedulable, CogniteSchedulableWrite, CogniteSchedulableList, CogniteSchedulableWriteList]
):
    _view_id = dm.ViewId("cdf_cdm", "CogniteSchedulable", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITESCHEDULABLE_PROPERTIES_BY_FIELD
    _direct_children_by_external_id: ClassVar[dict[str, type[DomainModel]]] = {
        "CogniteActivity": CogniteActivity,
    }
    _class_type = CogniteSchedulable
    _class_list = CogniteSchedulableList
    _class_write_list = CogniteSchedulableWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["CogniteActivity"]] | None = None,
    ) -> CogniteSchedulable | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["CogniteActivity"]] | None = None,
    ) -> CogniteSchedulableList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["CogniteActivity"]] | None = None,
    ) -> CogniteSchedulable | CogniteSchedulableList | None:
        """Retrieve one or more Cognite schedulables by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite schedulables.
            space: The space where all the Cognite schedulables are located.
            as_child_class: If you want to retrieve the Cognite schedulables as a child class,
                you can specify the child class here. Note that if one node has properties in
                multiple child classes, you will get duplicate nodes in the result.

        Returns:
            The requested Cognite schedulables.

        Examples:

            Retrieve cognite_schedulable by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_schedulable = client.cognite_schedulable.retrieve(
                ...     "my_cognite_schedulable"
                ... )

        """
        return self._retrieve(external_id, space, as_child_class=as_child_class)

    def search(
        self,
        query: str,
        properties: CogniteSchedulableTextFields | SequenceNotStr[CogniteSchedulableTextFields] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteSchedulableList:
        """Search Cognite schedulables

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_scheduled_end_time: The minimum value of the scheduled end time to filter on.
            max_scheduled_end_time: The maximum value of the scheduled end time to filter on.
            min_scheduled_start_time: The minimum value of the scheduled start time to filter on.
            max_scheduled_start_time: The maximum value of the scheduled start time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite schedulables to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite schedulables matching the query.

        Examples:

           Search for 'my_cognite_schedulable' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_schedulables = client.cognite_schedulable.search(
                ...     'my_cognite_schedulable'
                ... )

        """
        filter_ = _create_cognite_schedulable_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_scheduled_end_time,
            max_scheduled_end_time,
            min_scheduled_start_time,
            max_scheduled_start_time,
            min_start_time,
            max_start_time,
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
        property: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
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
        property: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
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
        group_by: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields],
        property: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
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
        group_by: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields] | None = None,
        property: CogniteSchedulableFields | SequenceNotStr[CogniteSchedulableFields] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite schedulables

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_scheduled_end_time: The minimum value of the scheduled end time to filter on.
            max_scheduled_end_time: The maximum value of the scheduled end time to filter on.
            min_scheduled_start_time: The minimum value of the scheduled start time to filter on.
            max_scheduled_start_time: The maximum value of the scheduled start time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite schedulables to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite schedulables in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_schedulable.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_schedulable_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_scheduled_end_time,
            max_scheduled_end_time,
            min_scheduled_start_time,
            max_scheduled_start_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=None,
            search_properties=None,
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: CogniteSchedulableFields,
        interval: float,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite schedulables

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_scheduled_end_time: The minimum value of the scheduled end time to filter on.
            max_scheduled_end_time: The maximum value of the scheduled end time to filter on.
            min_scheduled_start_time: The minimum value of the scheduled start time to filter on.
            max_scheduled_start_time: The maximum value of the scheduled start time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite schedulables to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_schedulable_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_scheduled_end_time,
            max_scheduled_end_time,
            min_scheduled_start_time,
            max_scheduled_start_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            None,
            None,
            limit,
            filter_,
        )

    def select(self) -> CogniteSchedulableQuery:
        """Start selecting from Cognite schedulables."""
        return CogniteSchedulableQuery(self._client)

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
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[CogniteSchedulableList]:
        """Iterate over Cognite schedulables

        Args:
            chunk_size: The number of Cognite schedulables to return in each iteration. Defaults to 100.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_scheduled_end_time: The minimum value of the scheduled end time to filter on.
            max_scheduled_end_time: The maximum value of the scheduled end time to filter on.
            min_scheduled_start_time: The minimum value of the scheduled start time to filter on.
            max_scheduled_start_time: The maximum value of the scheduled start time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of Cognite schedulables to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of Cognite schedulables

        Examples:

            Iterate Cognite schedulables in chunks of 100 up to 2000 items:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_schedulables in client.cognite_schedulable.iterate(chunk_size=100, limit=2000):
                ...     for cognite_schedulable in cognite_schedulables:
                ...         print(cognite_schedulable.external_id)

            Iterate Cognite schedulables in chunks of 100 sorted by external_id in descending order:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_schedulables in client.cognite_schedulable.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for cognite_schedulable in cognite_schedulables:
                ...         print(cognite_schedulable.external_id)

            Iterate Cognite schedulables in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for first_iteration in client.cognite_schedulable.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for cognite_schedulables in client.cognite_schedulable.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for cognite_schedulable in cognite_schedulables:
                ...         print(cognite_schedulable.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_cognite_schedulable_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_scheduled_end_time,
            max_scheduled_end_time,
            min_scheduled_start_time,
            max_scheduled_start_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

    def list(
        self,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_scheduled_end_time: datetime.datetime | None = None,
        max_scheduled_end_time: datetime.datetime | None = None,
        min_scheduled_start_time: datetime.datetime | None = None,
        max_scheduled_start_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteSchedulableFields | Sequence[CogniteSchedulableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteSchedulableList:
        """List/filter Cognite schedulables

        Args:
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_scheduled_end_time: The minimum value of the scheduled end time to filter on.
            max_scheduled_end_time: The maximum value of the scheduled end time to filter on.
            min_scheduled_start_time: The minimum value of the scheduled start time to filter on.
            max_scheduled_start_time: The maximum value of the scheduled start time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite schedulables to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested Cognite schedulables

        Examples:

            List Cognite schedulables and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_schedulables = client.cognite_schedulable.list(limit=5)

        """
        filter_ = _create_cognite_schedulable_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_scheduled_end_time,
            max_scheduled_end_time,
            min_scheduled_start_time,
            max_scheduled_start_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
