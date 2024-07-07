from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Empty,
    EmptyWrite,
    EmptyFields,
    EmptyList,
    EmptyWriteList,
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


class EmptyAPI(NodeAPI[Empty, EmptyWrite, EmptyList]):
    _view_id = dm.ViewId("pygen-models", "Empty", "1")
    _properties_by_field = _EMPTY_PROPERTIES_BY_FIELD

    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            class_type=Empty,
            class_list=EmptyList,
            class_write_list=EmptyWriteList,
        )

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
        limit: int | None = DEFAULT_QUERY_LIMIT,
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
        return EmptyQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        empty: EmptyWrite | Sequence[EmptyWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) empties.

        Args:
            empty: Empty or sequence of empties to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new empty:

                >>> from omni import OmniClient
                >>> from omni.data_classes import EmptyWrite
                >>> client = OmniClient()
                >>> empty = EmptyWrite(external_id="my_empty", ...)
                >>> result = client.empty.apply(empty)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.empty.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(empty, replace, write_none)

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
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.empty.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Empty | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> EmptyList: ...

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
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: EmptyFields | Sequence[EmptyFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
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
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

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
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
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
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
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
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
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
        limit: int | None = DEFAULT_LIMIT_READ,
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
            aggregate,
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
        limit: int | None = DEFAULT_LIMIT_READ,
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
            property,
            interval,
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
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: EmptyFields | Sequence[EmptyFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
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
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

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
        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,
            direction=direction,
            sort=sort,
        )
