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
    Empty,
    EmptyApply,
    EmptyFields,
    EmptyList,
    EmptyApplyList,
    EmptyTextFields,
)
from omni.data_classes._empty import (
    _EMPTY_PROPERTIES_BY_FIELD,
    _create_empty_filter,
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
from .empty_query import EmptyQueryAPI


class EmptyAPI(NodeAPI[Empty, EmptyApply, EmptyList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[EmptyApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Empty,
            class_apply_type=EmptyApply,
            class_list=EmptyList,
            class_apply_list=EmptyApplyList,
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
    ) -> EmptyQueryAPI[EmptyList]:
        """Query starting at empties.

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
            limit: Maximum number of empties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for empties.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_empty_filter(
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
        builder = QueryBuilder(EmptyList)
        return EmptyQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(self, empty: EmptyApply | Sequence[EmptyApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) empties.

        Args:
            empty: Empty or sequence of empties to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new empty:

                >>> from omni import OmniClient
                >>> from omni.data_classes import EmptyApply
                >>> client = OmniClient()
                >>> empty = EmptyApply(external_id="my_empty", ...)
                >>> result = client.empty.apply(empty)

        """
        return self._apply(empty, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more empty.

        Args:
            external_id: External id of the empty to delete.
            space: The space where all the empty are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete empty by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.empty.delete("my_empty")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Empty | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> EmptyList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Empty | EmptyList | None:
        """Retrieve one or more empties by id(s).

        Args:
            external_id: External id or list of external ids of the empties.
            space: The space where all the empties are located.

        Returns:
            The requested empties.

        Examples:

            Retrieve empty by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> empty = client.empty.retrieve("my_empty")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: EmptyTextFields | Sequence[EmptyTextFields] | None = None,
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
    ) -> EmptyList:
        """Search empties

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
            limit: Maximum number of empties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results empties matching the query.

        Examples:

           Search for 'my_empty' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> empties = client.empty.search('my_empty')

        """
        filter_ = _create_empty_filter(
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
        return self._search(self._view_id, query, _EMPTY_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: EmptyFields | Sequence[EmptyFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: EmptyTextFields | Sequence[EmptyTextFields] | None = None,
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
        property: EmptyFields | Sequence[EmptyFields] | None = None,
        group_by: EmptyFields | Sequence[EmptyFields] = None,
        query: str | None = None,
        search_properties: EmptyTextFields | Sequence[EmptyTextFields] | None = None,
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
        property: EmptyFields | Sequence[EmptyFields] | None = None,
        group_by: EmptyFields | Sequence[EmptyFields] | None = None,
        query: str | None = None,
        search_property: EmptyTextFields | Sequence[EmptyTextFields] | None = None,
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
        """Aggregate data across empties

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
            limit: Maximum number of empties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count empties in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.empty.aggregate("count", space="my_space")

        """

        filter_ = _create_empty_filter(
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
            _EMPTY_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: EmptyFields,
        interval: float,
        query: str | None = None,
        search_property: EmptyTextFields | Sequence[EmptyTextFields] | None = None,
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
        """Produces histograms for empties

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
            limit: Maximum number of empties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_empty_filter(
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
            _EMPTY_PROPERTIES_BY_FIELD,
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
    ) -> EmptyList:
        """List/filter empties

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
            limit: Maximum number of empties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested empties

        Examples:

            List empties and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> empties = client.empty.list(limit=5)

        """
        filter_ = _create_empty_filter(
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