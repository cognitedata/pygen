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
    PrimitiveRequired,
    PrimitiveRequiredApply,
    PrimitiveRequiredFields,
    PrimitiveRequiredList,
    PrimitiveRequiredApplyList,
    PrimitiveRequiredTextFields,
)
from omni_pydantic_v1.data_classes._primitive_required import (
    _PRIMITIVEREQUIRED_PROPERTIES_BY_FIELD,
    _create_primitive_required_filter,
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
from .primitive_required_query import PrimitiveRequiredQueryAPI


class PrimitiveRequiredAPI(NodeAPI[PrimitiveRequired, PrimitiveRequiredApply, PrimitiveRequiredList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[PrimitiveRequiredApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PrimitiveRequired,
            class_apply_type=PrimitiveRequiredApply,
            class_list=PrimitiveRequiredList,
            class_apply_list=PrimitiveRequiredApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
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
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PrimitiveRequiredQueryAPI[PrimitiveRequiredList]:
        """Query starting at primitive requireds.

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
            limit: Maximum number of primitive requireds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for primitive requireds.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
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
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PrimitiveRequiredList)
        return PrimitiveRequiredQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, primitive_required: PrimitiveRequiredApply | Sequence[PrimitiveRequiredApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) primitive requireds.

        Args:
            primitive_required: Primitive required or sequence of primitive requireds to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new primitive_required:

                >>> from omni_pydantic_v1 import OmniClient
                >>> from omni_pydantic_v1.data_classes import PrimitiveRequiredApply
                >>> client = OmniClient()
                >>> primitive_required = PrimitiveRequiredApply(external_id="my_primitive_required", ...)
                >>> result = client.primitive_required.apply(primitive_required)

        """
        return self._apply(primitive_required, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more primitive required.

        Args:
            external_id: External id of the primitive required to delete.
            space: The space where all the primitive required are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete primitive_required by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> client.primitive_required.delete("my_primitive_required")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PrimitiveRequired | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> PrimitiveRequiredList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PrimitiveRequired | PrimitiveRequiredList | None:
        """Retrieve one or more primitive requireds by id(s).

        Args:
            external_id: External id or list of external ids of the primitive requireds.
            space: The space where all the primitive requireds are located.

        Returns:
            The requested primitive requireds.

        Examples:

            Retrieve primitive_required by id:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> primitive_required = client.primitive_required.retrieve("my_primitive_required")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: PrimitiveRequiredTextFields | Sequence[PrimitiveRequiredTextFields] | None = None,
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
            limit: Maximum number of primitive requireds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results primitive requireds matching the query.

        Examples:

           Search for 'my_primitive_required' in all text properties:

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> primitive_requireds = client.primitive_required.search('my_primitive_required')

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
        return self._search(self._view_id, query, _PRIMITIVEREQUIRED_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PrimitiveRequiredFields | Sequence[PrimitiveRequiredFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PrimitiveRequiredTextFields | Sequence[PrimitiveRequiredTextFields] | None = None,
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
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PrimitiveRequiredFields | Sequence[PrimitiveRequiredFields] | None = None,
        group_by: PrimitiveRequiredFields | Sequence[PrimitiveRequiredFields] = None,
        query: str | None = None,
        search_properties: PrimitiveRequiredTextFields | Sequence[PrimitiveRequiredTextFields] | None = None,
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
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PrimitiveRequiredFields | Sequence[PrimitiveRequiredFields] | None = None,
        group_by: PrimitiveRequiredFields | Sequence[PrimitiveRequiredFields] | None = None,
        query: str | None = None,
        search_property: PrimitiveRequiredTextFields | Sequence[PrimitiveRequiredTextFields] | None = None,
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
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across primitive requireds

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
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
            limit: Maximum number of primitive requireds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count primitive requireds in space `my_space`:

                >>> from omni_pydantic_v1 import OmniClient
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
            self._view_id,
            aggregate,
            _PRIMITIVEREQUIRED_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PrimitiveRequiredFields,
        interval: float,
        query: str | None = None,
        search_property: PrimitiveRequiredTextFields | Sequence[PrimitiveRequiredTextFields] | None = None,
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
            limit: Maximum number of primitive requireds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            self._view_id,
            property,
            interval,
            _PRIMITIVEREQUIRED_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

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
            limit: Maximum number of primitive requireds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested primitive requireds

        Examples:

            List primitive requireds and limit to 5:

                >>> from omni_pydantic_v1 import OmniClient
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
        return self._list(limit=limit, filter=filter_)