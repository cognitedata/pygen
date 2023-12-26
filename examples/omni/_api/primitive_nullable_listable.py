from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from omni.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    PrimitiveNullableListable,
    PrimitiveNullableListableApply,
    PrimitiveNullableListableFields,
    PrimitiveNullableListableList,
    PrimitiveNullableListableApplyList,
    PrimitiveNullableListableTextFields,
)
from omni.data_classes._primitive_nullable_listable import (
    _PRIMITIVENULLABLELISTABLE_PROPERTIES_BY_FIELD,
    _create_primitive_nullable_listable_filter,
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
from .primitive_nullable_listable_query import PrimitiveNullableListableQueryAPI


class PrimitiveNullableListableAPI(
    NodeAPI[PrimitiveNullableListable, PrimitiveNullableListableApply, PrimitiveNullableListableList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[PrimitiveNullableListableApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PrimitiveNullableListable,
            class_apply_type=PrimitiveNullableListableApply,
            class_list=PrimitiveNullableListableList,
            class_apply_list=PrimitiveNullableListableApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PrimitiveNullableListableQueryAPI[PrimitiveNullableListableList]:
        """Query starting at primitive nullable listables.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for primitive nullable listables.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_primitive_nullable_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PrimitiveNullableListableList)
        return PrimitiveNullableListableQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self,
        primitive_nullable_listable: PrimitiveNullableListableApply | Sequence[PrimitiveNullableListableApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) primitive nullable listables.

        Args:
            primitive_nullable_listable: Primitive nullable listable or sequence of primitive nullable listables to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new primitive_nullable_listable:

                >>> from omni import OmniClient
                >>> from omni.data_classes import PrimitiveNullableListableApply
                >>> client = OmniClient()
                >>> primitive_nullable_listable = PrimitiveNullableListableApply(external_id="my_primitive_nullable_listable", ...)
                >>> result = client.primitive_nullable_listable.apply(primitive_nullable_listable)

        """
        return self._apply(primitive_nullable_listable, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more primitive nullable listable.

        Args:
            external_id: External id of the primitive nullable listable to delete.
            space: The space where all the primitive nullable listable are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete primitive_nullable_listable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.primitive_nullable_listable.delete("my_primitive_nullable_listable")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PrimitiveNullableListable | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveNullableListableList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveNullableListable | PrimitiveNullableListableList | None:
        """Retrieve one or more primitive nullable listables by id(s).

        Args:
            external_id: External id or list of external ids of the primitive nullable listables.
            space: The space where all the primitive nullable listables are located.

        Returns:
            The requested primitive nullable listables.

        Examples:

            Retrieve primitive_nullable_listable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullable_listable = client.primitive_nullable_listable.retrieve("my_primitive_nullable_listable")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: PrimitiveNullableListableTextFields | Sequence[PrimitiveNullableListableTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PrimitiveNullableListableList:
        """Search primitive nullable listables

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results primitive nullable listables matching the query.

        Examples:

           Search for 'my_primitive_nullable_listable' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullable_listables = client.primitive_nullable_listable.search('my_primitive_nullable_listable')

        """
        filter_ = _create_primitive_nullable_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _PRIMITIVENULLABLELISTABLE_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PrimitiveNullableListableFields | Sequence[PrimitiveNullableListableFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PrimitiveNullableListableTextFields
        | Sequence[PrimitiveNullableListableTextFields]
        | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PrimitiveNullableListableFields | Sequence[PrimitiveNullableListableFields] | None = None,
        group_by: PrimitiveNullableListableFields | Sequence[PrimitiveNullableListableFields] = None,
        query: str | None = None,
        search_properties: PrimitiveNullableListableTextFields
        | Sequence[PrimitiveNullableListableTextFields]
        | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PrimitiveNullableListableFields | Sequence[PrimitiveNullableListableFields] | None = None,
        group_by: PrimitiveNullableListableFields | Sequence[PrimitiveNullableListableFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveNullableListableTextFields
        | Sequence[PrimitiveNullableListableTextFields]
        | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across primitive nullable listables

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count primitive nullable listables in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.primitive_nullable_listable.aggregate("count", space="my_space")

        """

        filter_ = _create_primitive_nullable_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PRIMITIVENULLABLELISTABLE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PrimitiveNullableListableFields,
        interval: float,
        query: str | None = None,
        search_property: PrimitiveNullableListableTextFields
        | Sequence[PrimitiveNullableListableTextFields]
        | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for primitive nullable listables

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_primitive_nullable_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PRIMITIVENULLABLELISTABLE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PrimitiveNullableListableList:
        """List/filter primitive nullable listables

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested primitive nullable listables

        Examples:

            List primitive nullable listables and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullable_listables = client.primitive_nullable_listable.list(limit=5)

        """
        filter_ = _create_primitive_nullable_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
