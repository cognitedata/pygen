from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni._api._core import (
    DEFAULT_LIMIT_READ,
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
    QueryUnpacker,
    ViewPropertyId,
)
from omni.data_classes._primitive_nullable import (
    PrimitiveNullableQuery,
    _PRIMITIVENULLABLE_PROPERTIES_BY_FIELD,
    _create_primitive_nullable_filter,
)
from omni.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    PrimitiveNullable,
    PrimitiveNullableWrite,
    PrimitiveNullableFields,
    PrimitiveNullableList,
    PrimitiveNullableWriteList,
    PrimitiveNullableTextFields,
)


class PrimitiveNullableAPI(
    NodeAPI[PrimitiveNullable, PrimitiveNullableWrite, PrimitiveNullableList, PrimitiveNullableWriteList]
):
    _view_id = dm.ViewId("sp_pygen_models", "PrimitiveNullable", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _PRIMITIVENULLABLE_PROPERTIES_BY_FIELD
    _class_type = PrimitiveNullable
    _class_list = PrimitiveNullableList
    _class_write_list = PrimitiveNullableWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> PrimitiveNullable | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> PrimitiveNullableList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> PrimitiveNullable | PrimitiveNullableList | None:
        """Retrieve one or more primitive nullables by id(s).

        Args:
            external_id: External id or list of external ids of the primitive nullables.
            space: The space where all the primitive nullables are located.

        Returns:
            The requested primitive nullables.

        Examples:

            Retrieve primitive_nullable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullable = client.primitive_nullable.retrieve(
                ...     "my_primitive_nullable"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: PrimitiveNullableTextFields | SequenceNotStr[PrimitiveNullableTextFields] | None = None,
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
        sort_by: PrimitiveNullableFields | SequenceNotStr[PrimitiveNullableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PrimitiveNullableList:
        """Search primitive nullables

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
            limit: Maximum number of primitive nullables to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results primitive nullables matching the query.

        Examples:

           Search for 'my_primitive_nullable' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullables = client.primitive_nullable.search(
                ...     'my_primitive_nullable'
                ... )

        """
        filter_ = _create_primitive_nullable_filter(
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
        property: PrimitiveNullableFields | SequenceNotStr[PrimitiveNullableFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveNullableTextFields | SequenceNotStr[PrimitiveNullableTextFields] | None = None,
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
        property: PrimitiveNullableFields | SequenceNotStr[PrimitiveNullableFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveNullableTextFields | SequenceNotStr[PrimitiveNullableTextFields] | None = None,
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
        group_by: PrimitiveNullableFields | SequenceNotStr[PrimitiveNullableFields],
        property: PrimitiveNullableFields | SequenceNotStr[PrimitiveNullableFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveNullableTextFields | SequenceNotStr[PrimitiveNullableTextFields] | None = None,
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
        group_by: PrimitiveNullableFields | SequenceNotStr[PrimitiveNullableFields] | None = None,
        property: PrimitiveNullableFields | SequenceNotStr[PrimitiveNullableFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveNullableTextFields | SequenceNotStr[PrimitiveNullableTextFields] | None = None,
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
        """Aggregate data across primitive nullables

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
            limit: Maximum number of primitive nullables to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count primitive nullables in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.primitive_nullable.aggregate("count", space="my_space")

        """

        filter_ = _create_primitive_nullable_filter(
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
        property: PrimitiveNullableFields,
        interval: float,
        query: str | None = None,
        search_property: PrimitiveNullableTextFields | SequenceNotStr[PrimitiveNullableTextFields] | None = None,
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
        """Produces histograms for primitive nullables

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
            limit: Maximum number of primitive nullables to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_primitive_nullable_filter(
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

    def select(self) -> PrimitiveNullableQuery:
        """Start selecting from primitive nullables."""
        return PrimitiveNullableQuery(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> list[dict[str, Any]]:
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                sort=sort,
                limit=limit,
                has_container_fields=True,
            )
        )
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        executor = builder.build()
        results = executor.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        return QueryUnpacker(results, edges=unpack_edges).unpack()

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
        sort_by: PrimitiveNullableFields | Sequence[PrimitiveNullableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PrimitiveNullableList:
        """List/filter primitive nullables

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
            limit: Maximum number of primitive nullables to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested primitive nullables

        Examples:

            List primitive nullables and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullables = client.primitive_nullable.list(limit=5)

        """
        filter_ = _create_primitive_nullable_filter(
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
