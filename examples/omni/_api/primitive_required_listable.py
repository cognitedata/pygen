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
    PrimitiveRequiredListable,
    PrimitiveRequiredListableApply,
    PrimitiveRequiredListableFields,
    PrimitiveRequiredListableList,
    PrimitiveRequiredListableApplyList,
    PrimitiveRequiredListableTextFields,
)
from omni.data_classes._primitive_required_listable import (
    _PRIMITIVEREQUIREDLISTABLE_PROPERTIES_BY_FIELD,
    _create_primitive_required_listable_filter,
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
from .primitive_required_listable_query import PrimitiveRequiredListableQueryAPI


class PrimitiveRequiredListableAPI(
    NodeAPI[PrimitiveRequiredListable, PrimitiveRequiredListableApply, PrimitiveRequiredListableList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[PrimitiveRequiredListableApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PrimitiveRequiredListable,
            class_apply_type=PrimitiveRequiredListableApply,
            class_list=PrimitiveRequiredListableList,
            class_apply_list=PrimitiveRequiredListableApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PrimitiveRequiredListableQueryAPI[PrimitiveRequiredListableList]:
        """Query starting at primitive required listables.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive required listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for primitive required listables.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_primitive_required_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PrimitiveRequiredListableList)
        return PrimitiveRequiredListableQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self,
        primitive_required_listable: PrimitiveRequiredListableApply | Sequence[PrimitiveRequiredListableApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) primitive required listables.

        Args:
            primitive_required_listable: Primitive required listable or sequence of primitive required listables to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new primitive_required_listable:

                >>> from omni import OmniClient
                >>> from omni.data_classes import PrimitiveRequiredListableApply
                >>> client = OmniClient()
                >>> primitive_required_listable = PrimitiveRequiredListableApply(external_id="my_primitive_required_listable", ...)
                >>> result = client.primitive_required_listable.apply(primitive_required_listable)

        """
        return self._apply(primitive_required_listable, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more primitive required listable.

        Args:
            external_id: External id of the primitive required listable to delete.
            space: The space where all the primitive required listable are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete primitive_required_listable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.primitive_required_listable.delete("my_primitive_required_listable")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PrimitiveRequiredListable | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveRequiredListableList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveRequiredListable | PrimitiveRequiredListableList | None:
        """Retrieve one or more primitive required listables by id(s).

        Args:
            external_id: External id or list of external ids of the primitive required listables.
            space: The space where all the primitive required listables are located.

        Returns:
            The requested primitive required listables.

        Examples:

            Retrieve primitive_required_listable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_required_listable = client.primitive_required_listable.retrieve("my_primitive_required_listable")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: PrimitiveRequiredListableTextFields | Sequence[PrimitiveRequiredListableTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PrimitiveRequiredListableList:
        """Search primitive required listables

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive required listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results primitive required listables matching the query.

        Examples:

           Search for 'my_primitive_required_listable' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_required_listables = client.primitive_required_listable.search('my_primitive_required_listable')

        """
        filter_ = _create_primitive_required_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _PRIMITIVEREQUIREDLISTABLE_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PrimitiveRequiredListableFields | Sequence[PrimitiveRequiredListableFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PrimitiveRequiredListableTextFields
        | Sequence[PrimitiveRequiredListableTextFields]
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
        property: PrimitiveRequiredListableFields | Sequence[PrimitiveRequiredListableFields] | None = None,
        group_by: PrimitiveRequiredListableFields | Sequence[PrimitiveRequiredListableFields] = None,
        query: str | None = None,
        search_properties: PrimitiveRequiredListableTextFields
        | Sequence[PrimitiveRequiredListableTextFields]
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
        property: PrimitiveRequiredListableFields | Sequence[PrimitiveRequiredListableFields] | None = None,
        group_by: PrimitiveRequiredListableFields | Sequence[PrimitiveRequiredListableFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveRequiredListableTextFields
        | Sequence[PrimitiveRequiredListableTextFields]
        | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across primitive required listables

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive required listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count primitive required listables in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.primitive_required_listable.aggregate("count", space="my_space")

        """

        filter_ = _create_primitive_required_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PRIMITIVEREQUIREDLISTABLE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PrimitiveRequiredListableFields,
        interval: float,
        query: str | None = None,
        search_property: PrimitiveRequiredListableTextFields
        | Sequence[PrimitiveRequiredListableTextFields]
        | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for primitive required listables

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive required listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_primitive_required_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PRIMITIVEREQUIREDLISTABLE_PROPERTIES_BY_FIELD,
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
    ) -> PrimitiveRequiredListableList:
        """List/filter primitive required listables

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of primitive required listables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested primitive required listables

        Examples:

            List primitive required listables and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> primitive_required_listables = client.primitive_required_listable.list(limit=5)

        """
        filter_ = _create_primitive_required_listable_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
