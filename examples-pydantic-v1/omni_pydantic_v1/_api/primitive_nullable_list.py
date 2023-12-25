from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from omni_pydantic_v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni_pydantic_v1.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    PrimitiveNullableList,
    PrimitiveNullableListApply,
    PrimitiveNullableListFields,
    PrimitiveNullableListList,
    PrimitiveNullableListApplyList,
    PrimitiveNullableListTextFields,
)
from omni_pydantic_v1.data_classes._primitive_nullable_list import (
    _PRIMITIVENULLABLELIST_PROPERTIES_BY_FIELD,
    _create_primitive_nullable_list_filter,
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
from .primitive_nullable_list_query import PrimitiveNullableListQueryAPI


class PrimitiveNullableListAPI(NodeAPI[PrimitiveNullableList, PrimitiveNullableListApply, PrimitiveNullableListList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[PrimitiveNullableListApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PrimitiveNullableList,
            class_apply_type=PrimitiveNullableListApply,
            class_list=PrimitiveNullableListList,
            class_apply_list=PrimitiveNullableListApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PrimitiveNullableListQueryAPI[PrimitiveNullableListList]:
        """Query starting at primitive nullable lists.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable lists to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for primitive nullable lists.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_primitive_nullable_list_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PrimitiveNullableListList)
        return PrimitiveNullableListQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self,
        primitive_nullable_list: PrimitiveNullableListApply | Sequence[PrimitiveNullableListApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) primitive nullable lists.

        Args:
            primitive_nullable_list: Primitive nullable list or sequence of primitive nullable lists to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new primitive_nullable_list:

                >>> from omni_pydantic_v1 import OmniClient
                >>> from omni_pydantic_v1.data_classes import PrimitiveNullableListApply
                >>> client = OmniClient()
                >>> primitive_nullable_list = PrimitiveNullableListApply(external_id="my_primitive_nullable_list", ...)
                >>> result = client.primitive_nullable_list.apply(primitive_nullable_list)

        """
        return self._apply(primitive_nullable_list, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more primitive nullable list.

        Args:
            external_id: External id of the primitive nullable list to delete.
            space: The space where all the primitive nullable list are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete primitive_nullable_list by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> client.primitive_nullable_list.delete("my_primitive_nullable_list")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PrimitiveNullableList | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveNullableListList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveNullableList | PrimitiveNullableListList | None:
        """Retrieve one or more primitive nullable lists by id(s).

        Args:
            external_id: External id or list of external ids of the primitive nullable lists.
            space: The space where all the primitive nullable lists are located.

        Returns:
            The requested primitive nullable lists.

        Examples:

            Retrieve primitive_nullable_list by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullable_list = client.primitive_nullable_list.retrieve("my_primitive_nullable_list")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: PrimitiveNullableListTextFields | Sequence[PrimitiveNullableListTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PrimitiveNullableListList:
        """Search primitive nullable lists

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable lists to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results primitive nullable lists matching the query.

        Examples:

           Search for 'my_primitive_nullable_list' in all text properties:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullable_lists = client.primitive_nullable_list.search('my_primitive_nullable_list')

        """
        filter_ = _create_primitive_nullable_list_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _PRIMITIVENULLABLELIST_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PrimitiveNullableListFields | Sequence[PrimitiveNullableListFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PrimitiveNullableListTextFields | Sequence[PrimitiveNullableListTextFields] | None = None,
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
        property: PrimitiveNullableListFields | Sequence[PrimitiveNullableListFields] | None = None,
        group_by: PrimitiveNullableListFields | Sequence[PrimitiveNullableListFields] = None,
        query: str | None = None,
        search_properties: PrimitiveNullableListTextFields | Sequence[PrimitiveNullableListTextFields] | None = None,
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
        property: PrimitiveNullableListFields | Sequence[PrimitiveNullableListFields] | None = None,
        group_by: PrimitiveNullableListFields | Sequence[PrimitiveNullableListFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveNullableListTextFields | Sequence[PrimitiveNullableListTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across primitive nullable lists

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable lists to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count primitive nullable lists in space `my_space`:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> result = client.primitive_nullable_list.aggregate("count", space="my_space")

        """

        filter_ = _create_primitive_nullable_list_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PRIMITIVENULLABLELIST_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PrimitiveNullableListFields,
        interval: float,
        query: str | None = None,
        search_property: PrimitiveNullableListTextFields | Sequence[PrimitiveNullableListTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for primitive nullable lists

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable lists to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_primitive_nullable_list_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PRIMITIVENULLABLELIST_PROPERTIES_BY_FIELD,
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
    ) -> PrimitiveNullableListList:
        """List/filter primitive nullable lists

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive nullable lists to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested primitive nullable lists

        Examples:

            List primitive nullable lists and limit to 5:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> primitive_nullable_lists = client.primitive_nullable_list.list(limit=5)

        """
        filter_ = _create_primitive_nullable_list_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
