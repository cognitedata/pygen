from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from omni.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Implementation1NonWriteable,
    Implementation1NonWriteableFields,
    Implementation1NonWriteableList,
    Implementation1NonWriteableTextFields,
)
from omni.data_classes._implementation_1_non_writeable import (
    _IMPLEMENTATION1NONWRITEABLE_PROPERTIES_BY_FIELD,
    _create_implementation_1_non_writeable_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeReadAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .implementation_1_non_writeable_query import Implementation1NonWriteableQueryAPI


class Implementation1NonWriteableAPI(NodeReadAPI[Implementation1NonWriteable, Implementation1NonWriteableList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[Implementation1NonWriteable]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Implementation1NonWriteable,
            class_list=Implementation1NonWriteableList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> Implementation1NonWriteableQueryAPI[Implementation1NonWriteableList]:
        """Query starting at implementation 1 non writeables.

        Args:
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 non writeables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for implementation 1 non writeables.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_implementation_1_non_writeable_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(Implementation1NonWriteableList)
        return Implementation1NonWriteableQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more implementation 1 non writeable.

        Args:
            external_id: External id of the implementation 1 non writeable to delete.
            space: The space where all the implementation 1 non writeable are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete implementation_1_non_writeable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.implementation_1_non_writeable.delete("my_implementation_1_non_writeable")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.implementation_1_non_writeable.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Implementation1NonWriteable | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Implementation1NonWriteableList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Implementation1NonWriteable | Implementation1NonWriteableList | None:
        """Retrieve one or more implementation 1 non writeables by id(s).

        Args:
            external_id: External id or list of external ids of the implementation 1 non writeables.
            space: The space where all the implementation 1 non writeables are located.

        Returns:
            The requested implementation 1 non writeables.

        Examples:

            Retrieve implementation_1_non_writeable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> implementation_1_non_writeable = client.implementation_1_non_writeable.retrieve("my_implementation_1_non_writeable")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: (
            Implementation1NonWriteableTextFields | Sequence[Implementation1NonWriteableTextFields] | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> Implementation1NonWriteableList:
        """Search implementation 1 non writeables

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 non writeables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results implementation 1 non writeables matching the query.

        Examples:

           Search for 'my_implementation_1_non_writeable' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> implementation_1_non_writeables = client.implementation_1_non_writeable.search('my_implementation_1_non_writeable')

        """
        filter_ = _create_implementation_1_non_writeable_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _IMPLEMENTATION1NONWRITEABLE_PROPERTIES_BY_FIELD, properties, filter_, limit
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
        property: Implementation1NonWriteableFields | Sequence[Implementation1NonWriteableFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: (
            Implementation1NonWriteableTextFields | Sequence[Implementation1NonWriteableTextFields] | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
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
        property: Implementation1NonWriteableFields | Sequence[Implementation1NonWriteableFields] | None = None,
        group_by: Implementation1NonWriteableFields | Sequence[Implementation1NonWriteableFields] = None,
        query: str | None = None,
        search_properties: (
            Implementation1NonWriteableTextFields | Sequence[Implementation1NonWriteableTextFields] | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
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
        property: Implementation1NonWriteableFields | Sequence[Implementation1NonWriteableFields] | None = None,
        group_by: Implementation1NonWriteableFields | Sequence[Implementation1NonWriteableFields] | None = None,
        query: str | None = None,
        search_property: (
            Implementation1NonWriteableTextFields | Sequence[Implementation1NonWriteableTextFields] | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across implementation 1 non writeables

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 non writeables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count implementation 1 non writeables in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.implementation_1_non_writeable.aggregate("count", space="my_space")

        """

        filter_ = _create_implementation_1_non_writeable_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _IMPLEMENTATION1NONWRITEABLE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: Implementation1NonWriteableFields,
        interval: float,
        query: str | None = None,
        search_property: (
            Implementation1NonWriteableTextFields | Sequence[Implementation1NonWriteableTextFields] | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for implementation 1 non writeables

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 non writeables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_implementation_1_non_writeable_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _IMPLEMENTATION1NONWRITEABLE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Implementation1NonWriteableFields | Sequence[Implementation1NonWriteableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
    ) -> Implementation1NonWriteableList:
        """List/filter implementation 1 non writeables

        Args:
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 non writeables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.

        Returns:
            List of requested implementation 1 non writeables

        Examples:

            List implementation 1 non writeables and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> implementation_1_non_writeables = client.implementation_1_non_writeable.list(limit=5)

        """
        filter_ = _create_implementation_1_non_writeable_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(
            limit=limit,
            filter=filter_,
            properties_by_field=_IMPLEMENTATION1NONWRITEABLE_PROPERTIES_BY_FIELD,
            sort_by=sort_by,
            direction=direction,
        )
