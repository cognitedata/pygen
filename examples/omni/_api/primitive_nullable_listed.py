from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from omni.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    PrimitiveNullableListed,
    PrimitiveNullableListedApply,
    PrimitiveNullableListedFields,
    PrimitiveNullableListedList,
    PrimitiveNullableListedApplyList,
    PrimitiveNullableListedTextFields,
)
from omni.data_classes._primitive_nullable_listed import (
    _PRIMITIVENULLABLELISTED_PROPERTIES_BY_FIELD,
    _create_primitive_nullable_listed_filter,
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
from .primitive_nullable_listed_query import PrimitiveNullableListedQueryAPI


class PrimitiveNullableListedAPI(
    NodeAPI[PrimitiveNullableListed, PrimitiveNullableListedApply, PrimitiveNullableListedList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[PrimitiveNullableListed]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PrimitiveNullableListed,
            class_list=PrimitiveNullableListedList,
            class_apply_list=PrimitiveNullableListedApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PrimitiveNullableListedQueryAPI[PrimitiveNullableListedList]:
        """Query starting at primitive nullable listeds.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for primitive nullable listeds.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_primitive_nullable_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PrimitiveNullableListedList)
        return PrimitiveNullableListedQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self,
        primitive_nullable_listed: PrimitiveNullableListedApply | Sequence[PrimitiveNullableListedApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) primitive nullable listeds.

        Args:
            primitive_nullable_listed: Primitive nullable listed or sequence of primitive nullable listeds to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new primitive_nullable_listed:

                >>> from omni import OmniClient
                >>> from omni.data_classes import PrimitiveNullableListedApply
                >>> client = OmniClient()
                >>> primitive_nullable_listed = PrimitiveNullableListedApply(external_id="my_primitive_nullable_listed", ...)
                >>> result = client.primitive_nullable_listed.apply(primitive_nullable_listed)

        """
        return self._apply(primitive_nullable_listed, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more primitive nullable listed.

        Args:
            external_id: External id of the primitive nullable listed to delete.
            space: The space where all the primitive nullable listed are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete primitive_nullable_listed by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.primitive_nullable_listed.delete("my_primitive_nullable_listed")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PrimitiveNullableListed | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveNullableListedList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
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
                >>> primitive_nullable_listed = client.primitive_nullable_listed.retrieve("my_primitive_nullable_listed")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: PrimitiveNullableListedTextFields | Sequence[PrimitiveNullableListedTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PrimitiveNullableListedList:
        """Search primitive nullable listeds

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results primitive nullable listeds matching the query.

        Examples:

           Search for 'my_primitive_nullable_listed' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullable_listeds = client.primitive_nullable_listed.search('my_primitive_nullable_listed')

        """
        filter_ = _create_primitive_nullable_listed_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _PRIMITIVENULLABLELISTED_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PrimitiveNullableListedFields | Sequence[PrimitiveNullableListedFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PrimitiveNullableListedTextFields
        | Sequence[PrimitiveNullableListedTextFields]
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
        property: PrimitiveNullableListedFields | Sequence[PrimitiveNullableListedFields] | None = None,
        group_by: PrimitiveNullableListedFields | Sequence[PrimitiveNullableListedFields] = None,
        query: str | None = None,
        search_properties: PrimitiveNullableListedTextFields
        | Sequence[PrimitiveNullableListedTextFields]
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
        property: PrimitiveNullableListedFields | Sequence[PrimitiveNullableListedFields] | None = None,
        group_by: PrimitiveNullableListedFields | Sequence[PrimitiveNullableListedFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveNullableListedTextFields | Sequence[PrimitiveNullableListedTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across primitive nullable listeds

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            self._view_id,
            aggregate,
            _PRIMITIVENULLABLELISTED_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PrimitiveNullableListedFields,
        interval: float,
        query: str | None = None,
        search_property: PrimitiveNullableListedTextFields | Sequence[PrimitiveNullableListedTextFields] | None = None,
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
            limit: Maximum number of primitive nullable listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            self._view_id,
            property,
            interval,
            _PRIMITIVENULLABLELISTED_PROPERTIES_BY_FIELD,
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
    ) -> PrimitiveNullableListedList:
        """List/filter primitive nullable listeds

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable listeds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
        return self._list(limit=limit, filter=filter_)
