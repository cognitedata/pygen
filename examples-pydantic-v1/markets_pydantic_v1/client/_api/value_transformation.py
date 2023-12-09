from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from markets_pydantic_v1.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from markets_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    ValueTransformation,
    ValueTransformationApply,
    ValueTransformationFields,
    ValueTransformationList,
    ValueTransformationApplyList,
    ValueTransformationTextFields,
)
from markets_pydantic_v1.client.data_classes._value_transformation import (
    _VALUETRANSFORMATION_PROPERTIES_BY_FIELD,
    _create_value_transformation_filter,
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
from .value_transformation_query import ValueTransformationQueryAPI


class ValueTransformationAPI(NodeAPI[ValueTransformation, ValueTransformationApply, ValueTransformationList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[ValueTransformationApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ValueTransformation,
            class_apply_type=ValueTransformationApply,
            class_list=ValueTransformationList,
            class_apply_list=ValueTransformationApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ValueTransformationQueryAPI[ValueTransformationList]:
        """Query starting at value transformations.

        Args:
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of value transformations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for value transformations.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_value_transformation_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ValueTransformationList)
        return ValueTransformationQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, value_transformation: ValueTransformationApply | Sequence[ValueTransformationApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) value transformations.

        Args:
            value_transformation: Value transformation or sequence of value transformations to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new value_transformation:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> from markets_pydantic_v1.client.data_classes import ValueTransformationApply
                >>> client = MarketClient()
                >>> value_transformation = ValueTransformationApply(external_id="my_value_transformation", ...)
                >>> result = client.value_transformation.apply(value_transformation)

        """
        return self._apply(value_transformation, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more value transformation.

        Args:
            external_id: External id of the value transformation to delete.
            space: The space where all the value transformation are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete value_transformation by id:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> client.value_transformation.delete("my_value_transformation")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ValueTransformation | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ValueTransformationList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ValueTransformation | ValueTransformationList | None:
        """Retrieve one or more value transformations by id(s).

        Args:
            external_id: External id or list of external ids of the value transformations.
            space: The space where all the value transformations are located.

        Returns:
            The requested value transformations.

        Examples:

            Retrieve value_transformation by id:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> value_transformation = client.value_transformation.retrieve("my_value_transformation")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: ValueTransformationTextFields | Sequence[ValueTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ValueTransformationList:
        """Search value transformations

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of value transformations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results value transformations matching the query.

        Examples:

           Search for 'my_value_transformation' in all text properties:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> value_transformations = client.value_transformation.search('my_value_transformation')

        """
        filter_ = _create_value_transformation_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _VALUETRANSFORMATION_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ValueTransformationFields | Sequence[ValueTransformationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ValueTransformationTextFields | Sequence[ValueTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
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
        property: ValueTransformationFields | Sequence[ValueTransformationFields] | None = None,
        group_by: ValueTransformationFields | Sequence[ValueTransformationFields] = None,
        query: str | None = None,
        search_properties: ValueTransformationTextFields | Sequence[ValueTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
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
        property: ValueTransformationFields | Sequence[ValueTransformationFields] | None = None,
        group_by: ValueTransformationFields | Sequence[ValueTransformationFields] | None = None,
        query: str | None = None,
        search_property: ValueTransformationTextFields | Sequence[ValueTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across value transformations

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of value transformations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count value transformations in space `my_space`:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> result = client.value_transformation.aggregate("count", space="my_space")

        """

        filter_ = _create_value_transformation_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _VALUETRANSFORMATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ValueTransformationFields,
        interval: float,
        query: str | None = None,
        search_property: ValueTransformationTextFields | Sequence[ValueTransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for value transformations

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of value transformations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_value_transformation_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _VALUETRANSFORMATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ValueTransformationList:
        """List/filter value transformations

        Args:
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of value transformations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested value transformations

        Examples:

            List value transformations and limit to 5:

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> value_transformations = client.value_transformation.list(limit=5)

        """
        filter_ = _create_value_transformation_filter(
            self._view_id,
            method,
            method_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
