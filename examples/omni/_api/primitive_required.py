from __future__ import annotations

import datetime
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
from omni.data_classes._primitive_required import (
    PrimitiveRequiredQuery,
    _PRIMITIVEREQUIRED_PROPERTIES_BY_FIELD,
    _create_primitive_required_filter,
)
from omni.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    PrimitiveRequired,
    PrimitiveRequiredWrite,
    PrimitiveRequiredFields,
    PrimitiveRequiredList,
    PrimitiveRequiredWriteList,
    PrimitiveRequiredTextFields,
)


class PrimitiveRequiredAPI(
    NodeAPI[PrimitiveRequired, PrimitiveRequiredWrite, PrimitiveRequiredList, PrimitiveRequiredWriteList]
):
    _view_id = dm.ViewId("sp_pygen_models", "PrimitiveRequired", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _PRIMITIVEREQUIRED_PROPERTIES_BY_FIELD
    _class_type = PrimitiveRequired
    _class_list = PrimitiveRequiredList
    _class_write_list = PrimitiveRequiredWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> PrimitiveRequired | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> PrimitiveRequiredList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> PrimitiveRequired | PrimitiveRequiredList | None:
        """Retrieve one or more primitive requireds by id(s).

        Args:
            external_id: External id or list of external ids of the primitive requireds.
            space: The space where all the primitive requireds are located.

        Returns:
            The requested primitive requireds.

        Examples:

            Retrieve primitive_required by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_required = client.primitive_required.retrieve(
                ...     "my_primitive_required"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: PrimitiveRequiredTextFields | SequenceNotStr[PrimitiveRequiredTextFields] | None = None,
        boolean: bool | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_float_32: float | None = None,
        max_float_32: float | None = None,
        min_float_64: float | None = None,
        max_float_64: float | None = None,
        min_int_32: int | None = None,
        max_int_32: int | None = None,
        min_int_64: int | None = None,
        max_int_64: int | None = None,
        text: str | list[str] | None = None,
        text_prefix: str | None = None,
        min_timestamp: datetime.datetime | None = None,
        max_timestamp: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PrimitiveRequiredFields | SequenceNotStr[PrimitiveRequiredFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PrimitiveRequiredList:
        """Search primitive requireds

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            boolean: The boolean to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            min_float_32: The minimum value of the float 32 to filter on.
            max_float_32: The maximum value of the float 32 to filter on.
            min_float_64: The minimum value of the float 64 to filter on.
            max_float_64: The maximum value of the float 64 to filter on.
            min_int_32: The minimum value of the int 32 to filter on.
            max_int_32: The maximum value of the int 32 to filter on.
            min_int_64: The minimum value of the int 64 to filter on.
            max_int_64: The maximum value of the int 64 to filter on.
            text: The text to filter on.
            text_prefix: The prefix of the text to filter on.
            min_timestamp: The minimum value of the timestamp to filter on.
            max_timestamp: The maximum value of the timestamp to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive requireds to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results primitive requireds matching the query.

        Examples:

           Search for 'my_primitive_required' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_requireds = client.primitive_required.search(
                ...     'my_primitive_required'
                ... )

        """
        filter_ = _create_primitive_required_filter(
            self._view_id,
            boolean,
            min_date,
            max_date,
            min_float_32,
            max_float_32,
            min_float_64,
            max_float_64,
            min_int_32,
            max_int_32,
            min_int_64,
            max_int_64,
            text,
            text_prefix,
            min_timestamp,
            max_timestamp,
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
        property: PrimitiveRequiredFields | SequenceNotStr[PrimitiveRequiredFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveRequiredTextFields | SequenceNotStr[PrimitiveRequiredTextFields] | None = None,
        boolean: bool | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_float_32: float | None = None,
        max_float_32: float | None = None,
        min_float_64: float | None = None,
        max_float_64: float | None = None,
        min_int_32: int | None = None,
        max_int_32: int | None = None,
        min_int_64: int | None = None,
        max_int_64: int | None = None,
        text: str | list[str] | None = None,
        text_prefix: str | None = None,
        min_timestamp: datetime.datetime | None = None,
        max_timestamp: datetime.datetime | None = None,
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
        property: PrimitiveRequiredFields | SequenceNotStr[PrimitiveRequiredFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveRequiredTextFields | SequenceNotStr[PrimitiveRequiredTextFields] | None = None,
        boolean: bool | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_float_32: float | None = None,
        max_float_32: float | None = None,
        min_float_64: float | None = None,
        max_float_64: float | None = None,
        min_int_32: int | None = None,
        max_int_32: int | None = None,
        min_int_64: int | None = None,
        max_int_64: int | None = None,
        text: str | list[str] | None = None,
        text_prefix: str | None = None,
        min_timestamp: datetime.datetime | None = None,
        max_timestamp: datetime.datetime | None = None,
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
        group_by: PrimitiveRequiredFields | SequenceNotStr[PrimitiveRequiredFields],
        property: PrimitiveRequiredFields | SequenceNotStr[PrimitiveRequiredFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveRequiredTextFields | SequenceNotStr[PrimitiveRequiredTextFields] | None = None,
        boolean: bool | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_float_32: float | None = None,
        max_float_32: float | None = None,
        min_float_64: float | None = None,
        max_float_64: float | None = None,
        min_int_32: int | None = None,
        max_int_32: int | None = None,
        min_int_64: int | None = None,
        max_int_64: int | None = None,
        text: str | list[str] | None = None,
        text_prefix: str | None = None,
        min_timestamp: datetime.datetime | None = None,
        max_timestamp: datetime.datetime | None = None,
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
        group_by: PrimitiveRequiredFields | SequenceNotStr[PrimitiveRequiredFields] | None = None,
        property: PrimitiveRequiredFields | SequenceNotStr[PrimitiveRequiredFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveRequiredTextFields | SequenceNotStr[PrimitiveRequiredTextFields] | None = None,
        boolean: bool | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_float_32: float | None = None,
        max_float_32: float | None = None,
        min_float_64: float | None = None,
        max_float_64: float | None = None,
        min_int_32: int | None = None,
        max_int_32: int | None = None,
        min_int_64: int | None = None,
        max_int_64: int | None = None,
        text: str | list[str] | None = None,
        text_prefix: str | None = None,
        min_timestamp: datetime.datetime | None = None,
        max_timestamp: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across primitive requireds

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            boolean: The boolean to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            min_float_32: The minimum value of the float 32 to filter on.
            max_float_32: The maximum value of the float 32 to filter on.
            min_float_64: The minimum value of the float 64 to filter on.
            max_float_64: The maximum value of the float 64 to filter on.
            min_int_32: The minimum value of the int 32 to filter on.
            max_int_32: The maximum value of the int 32 to filter on.
            min_int_64: The minimum value of the int 64 to filter on.
            max_int_64: The maximum value of the int 64 to filter on.
            text: The text to filter on.
            text_prefix: The prefix of the text to filter on.
            min_timestamp: The minimum value of the timestamp to filter on.
            max_timestamp: The maximum value of the timestamp to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive requireds to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count primitive requireds in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.primitive_required.aggregate("count", space="my_space")

        """

        filter_ = _create_primitive_required_filter(
            self._view_id,
            boolean,
            min_date,
            max_date,
            min_float_32,
            max_float_32,
            min_float_64,
            max_float_64,
            min_int_32,
            max_int_32,
            min_int_64,
            max_int_64,
            text,
            text_prefix,
            min_timestamp,
            max_timestamp,
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
        property: PrimitiveRequiredFields,
        interval: float,
        query: str | None = None,
        search_property: PrimitiveRequiredTextFields | SequenceNotStr[PrimitiveRequiredTextFields] | None = None,
        boolean: bool | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_float_32: float | None = None,
        max_float_32: float | None = None,
        min_float_64: float | None = None,
        max_float_64: float | None = None,
        min_int_32: int | None = None,
        max_int_32: int | None = None,
        min_int_64: int | None = None,
        max_int_64: int | None = None,
        text: str | list[str] | None = None,
        text_prefix: str | None = None,
        min_timestamp: datetime.datetime | None = None,
        max_timestamp: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for primitive requireds

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            boolean: The boolean to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            min_float_32: The minimum value of the float 32 to filter on.
            max_float_32: The maximum value of the float 32 to filter on.
            min_float_64: The minimum value of the float 64 to filter on.
            max_float_64: The maximum value of the float 64 to filter on.
            min_int_32: The minimum value of the int 32 to filter on.
            max_int_32: The maximum value of the int 32 to filter on.
            min_int_64: The minimum value of the int 64 to filter on.
            max_int_64: The maximum value of the int 64 to filter on.
            text: The text to filter on.
            text_prefix: The prefix of the text to filter on.
            min_timestamp: The minimum value of the timestamp to filter on.
            max_timestamp: The maximum value of the timestamp to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive requireds to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_primitive_required_filter(
            self._view_id,
            boolean,
            min_date,
            max_date,
            min_float_32,
            max_float_32,
            min_float_64,
            max_float_64,
            min_int_32,
            max_int_32,
            min_int_64,
            max_int_64,
            text,
            text_prefix,
            min_timestamp,
            max_timestamp,
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

    def select(self) -> PrimitiveRequiredQuery:
        """Start selecting from primitive requireds."""
        return PrimitiveRequiredQuery(self._client)

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
        boolean: bool | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_float_32: float | None = None,
        max_float_32: float | None = None,
        min_float_64: float | None = None,
        max_float_64: float | None = None,
        min_int_32: int | None = None,
        max_int_32: int | None = None,
        min_int_64: int | None = None,
        max_int_64: int | None = None,
        text: str | list[str] | None = None,
        text_prefix: str | None = None,
        min_timestamp: datetime.datetime | None = None,
        max_timestamp: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[PrimitiveRequiredList]:
        """Iterate over primitive requireds

        Args:
            chunk_size: The number of primitive requireds to return in each iteration. Defaults to 100.
            boolean: The boolean to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            min_float_32: The minimum value of the float 32 to filter on.
            max_float_32: The maximum value of the float 32 to filter on.
            min_float_64: The minimum value of the float 64 to filter on.
            max_float_64: The maximum value of the float 64 to filter on.
            min_int_32: The minimum value of the int 32 to filter on.
            max_int_32: The maximum value of the int 32 to filter on.
            min_int_64: The minimum value of the int 64 to filter on.
            max_int_64: The maximum value of the int 64 to filter on.
            text: The text to filter on.
            text_prefix: The prefix of the text to filter on.
            min_timestamp: The minimum value of the timestamp to filter on.
            max_timestamp: The maximum value of the timestamp to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of primitive requireds to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of primitive requireds

        Examples:

            Iterate primitive requireds in chunks of 100 up to 2000 items:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for primitive_requireds in client.primitive_required.iterate(chunk_size=100, limit=2000):
                ...     for primitive_required in primitive_requireds:
                ...         print(primitive_required.external_id)

            Iterate primitive requireds in chunks of 100 sorted by external_id in descending order:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for primitive_requireds in client.primitive_required.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for primitive_required in primitive_requireds:
                ...         print(primitive_required.external_id)

            Iterate primitive requireds in chunks of 100 and use cursors to resume the iteration:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for first_iteration in client.primitive_required.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for primitive_requireds in client.primitive_required.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for primitive_required in primitive_requireds:
                ...         print(primitive_required.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_primitive_required_filter(
            self._view_id,
            boolean,
            min_date,
            max_date,
            min_float_32,
            max_float_32,
            min_float_64,
            max_float_64,
            min_int_32,
            max_int_32,
            min_int_64,
            max_int_64,
            text,
            text_prefix,
            min_timestamp,
            max_timestamp,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

    def list(
        self,
        boolean: bool | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        min_float_32: float | None = None,
        max_float_32: float | None = None,
        min_float_64: float | None = None,
        max_float_64: float | None = None,
        min_int_32: int | None = None,
        max_int_32: int | None = None,
        min_int_64: int | None = None,
        max_int_64: int | None = None,
        text: str | list[str] | None = None,
        text_prefix: str | None = None,
        min_timestamp: datetime.datetime | None = None,
        max_timestamp: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PrimitiveRequiredFields | Sequence[PrimitiveRequiredFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PrimitiveRequiredList:
        """List/filter primitive requireds

        Args:
            boolean: The boolean to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            min_float_32: The minimum value of the float 32 to filter on.
            max_float_32: The maximum value of the float 32 to filter on.
            min_float_64: The minimum value of the float 64 to filter on.
            max_float_64: The maximum value of the float 64 to filter on.
            min_int_32: The minimum value of the int 32 to filter on.
            max_int_32: The maximum value of the int 32 to filter on.
            min_int_64: The minimum value of the int 64 to filter on.
            max_int_64: The maximum value of the int 64 to filter on.
            text: The text to filter on.
            text_prefix: The prefix of the text to filter on.
            min_timestamp: The minimum value of the timestamp to filter on.
            max_timestamp: The maximum value of the timestamp to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive requireds to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested primitive requireds

        Examples:

            List primitive requireds and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_requireds = client.primitive_required.list(limit=5)

        """
        filter_ = _create_primitive_required_filter(
            self._view_id,
            boolean,
            min_date,
            max_date,
            min_float_32,
            max_float_32,
            min_float_64,
            max_float_64,
            min_int_32,
            max_int_32,
            min_int_64,
            max_int_64,
            text,
            text_prefix,
            min_timestamp,
            max_timestamp,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
