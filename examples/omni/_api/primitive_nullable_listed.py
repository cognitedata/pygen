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
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from omni.data_classes._primitive_nullable_listed import (
    PrimitiveNullableListedQuery,
    _PRIMITIVENULLABLELISTED_PROPERTIES_BY_FIELD,
    _create_primitive_nullable_listed_filter,
)
from omni.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    PrimitiveNullableListed,
    PrimitiveNullableListedWrite,
    PrimitiveNullableListedFields,
    PrimitiveNullableListedList,
    PrimitiveNullableListedWriteList,
    PrimitiveNullableListedTextFields,
)


class PrimitiveNullableListedAPI(
    NodeAPI[
        PrimitiveNullableListed,
        PrimitiveNullableListedWrite,
        PrimitiveNullableListedList,
        PrimitiveNullableListedWriteList,
    ]
):
    _view_id = dm.ViewId("sp_pygen_models", "PrimitiveNullableListed", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _PRIMITIVENULLABLELISTED_PROPERTIES_BY_FIELD
    _class_type = PrimitiveNullableListed
    _class_list = PrimitiveNullableListedList
    _class_write_list = PrimitiveNullableListedWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> PrimitiveNullableListed | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> PrimitiveNullableListedList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> PrimitiveNullableListed | PrimitiveNullableListedList | None:
        """Retrieve one or more primitive nullable listeds by id(s).

        Args:
            external_id: External id or list of external ids of the primitive nullable listeds.
            space: The space where all the primitive nullable listeds are located.

        Returns:
            The requested primitive nullable listeds.

        Examples:

            Retrieve primitive_nullable_listed by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullable_listed = client.primitive_nullable_listed.retrieve(
                ...     "my_primitive_nullable_listed"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: PrimitiveNullableListedTextFields | SequenceNotStr[PrimitiveNullableListedTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PrimitiveNullableListedFields | SequenceNotStr[PrimitiveNullableListedFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PrimitiveNullableListedList:
        """Search primitive nullable listeds

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listeds to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results primitive nullable listeds matching the query.

        Examples:

           Search for 'my_primitive_nullable_listed' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullable_listeds = client.primitive_nullable_listed.search(
                ...     'my_primitive_nullable_listed'
                ... )

        """
        filter_ = _create_primitive_nullable_listed_filter(
            self._view_id,
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
        property: PrimitiveNullableListedFields | SequenceNotStr[PrimitiveNullableListedFields] | None = None,
        query: str | None = None,
        search_property: (
            PrimitiveNullableListedTextFields | SequenceNotStr[PrimitiveNullableListedTextFields] | None
        ) = None,
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
        property: PrimitiveNullableListedFields | SequenceNotStr[PrimitiveNullableListedFields] | None = None,
        query: str | None = None,
        search_property: (
            PrimitiveNullableListedTextFields | SequenceNotStr[PrimitiveNullableListedTextFields] | None
        ) = None,
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
        group_by: PrimitiveNullableListedFields | SequenceNotStr[PrimitiveNullableListedFields],
        property: PrimitiveNullableListedFields | SequenceNotStr[PrimitiveNullableListedFields] | None = None,
        query: str | None = None,
        search_property: (
            PrimitiveNullableListedTextFields | SequenceNotStr[PrimitiveNullableListedTextFields] | None
        ) = None,
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
        group_by: PrimitiveNullableListedFields | SequenceNotStr[PrimitiveNullableListedFields] | None = None,
        property: PrimitiveNullableListedFields | SequenceNotStr[PrimitiveNullableListedFields] | None = None,
        query: str | None = None,
        search_property: (
            PrimitiveNullableListedTextFields | SequenceNotStr[PrimitiveNullableListedTextFields] | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across primitive nullable listeds

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listeds to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count primitive nullable listeds in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.primitive_nullable_listed.aggregate("count", space="my_space")

        """

        filter_ = _create_primitive_nullable_listed_filter(
            self._view_id,
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
        property: PrimitiveNullableListedFields,
        interval: float,
        query: str | None = None,
        search_property: (
            PrimitiveNullableListedTextFields | SequenceNotStr[PrimitiveNullableListedTextFields] | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for primitive nullable listeds

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listeds to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_primitive_nullable_listed_filter(
            self._view_id,
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

    def select(self) -> PrimitiveNullableListedQuery:
        """Start selecting from primitive nullable listeds."""
        return PrimitiveNullableListedQuery(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> list[dict[str, Any]]:
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
        return QueryUnpacker(builder, edges=unpack_edges).unpack()

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PrimitiveNullableListedFields | Sequence[PrimitiveNullableListedFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PrimitiveNullableListedList:
        """List/filter primitive nullable listeds

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listeds to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested primitive nullable listeds

        Examples:

            List primitive nullable listeds and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullable_listeds = client.primitive_nullable_listed.list(limit=5)

        """
        filter_ = _create_primitive_nullable_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
