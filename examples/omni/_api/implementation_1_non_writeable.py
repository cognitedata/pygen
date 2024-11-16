from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
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
    Implementation1NonWriteableQuery,
    _IMPLEMENTATION1NONWRITEABLE_PROPERTIES_BY_FIELD,
    _create_implementation_1_non_writeable_filter,
)
from omni._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeReadAPI,
    SequenceNotStr,
)
from omni._api.implementation_1_non_writeable_query import Implementation1NonWriteableQueryAPI


class Implementation1NonWriteableAPI(NodeReadAPI[Implementation1NonWriteable, Implementation1NonWriteableList]):
    _view_id = dm.ViewId("sp_pygen_models", "Implementation1NonWriteable", "1")
    _properties_by_field = _IMPLEMENTATION1NONWRITEABLE_PROPERTIES_BY_FIELD
    _class_type = Implementation1NonWriteable
    _class_list = Implementation1NonWriteableList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

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
        limit: int = DEFAULT_QUERY_LIMIT,
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
        builder = DataClassQueryBuilder(Implementation1NonWriteableList)
        return Implementation1NonWriteableQueryAPI(self._client, builder, filter_, limit)

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
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Implementation1NonWriteable | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Implementation1NonWriteableList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
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
            Implementation1NonWriteableTextFields | SequenceNotStr[Implementation1NonWriteableTextFields] | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
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
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

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
        property: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields] | None = None,
        query: str | None = None,
        search_property: (
            Implementation1NonWriteableTextFields | SequenceNotStr[Implementation1NonWriteableTextFields] | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
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
        property: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields] | None = None,
        query: str | None = None,
        search_property: (
            Implementation1NonWriteableTextFields | SequenceNotStr[Implementation1NonWriteableTextFields] | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
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
        group_by: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields],
        property: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields] | None = None,
        query: str | None = None,
        search_property: (
            Implementation1NonWriteableTextFields | SequenceNotStr[Implementation1NonWriteableTextFields] | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
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
        group_by: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields] | None = None,
        property: Implementation1NonWriteableFields | SequenceNotStr[Implementation1NonWriteableFields] | None = None,
        query: str | None = None,
        search_property: (
            Implementation1NonWriteableTextFields | SequenceNotStr[Implementation1NonWriteableTextFields] | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across implementation 1 non writeables

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
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
        property: Implementation1NonWriteableFields,
        interval: float,
        query: str | None = None,
        search_property: (
            Implementation1NonWriteableTextFields | SequenceNotStr[Implementation1NonWriteableTextFields] | None
        ) = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
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
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    def query(self) -> Implementation1NonWriteableQuery:
        """Start a query for implementation 1 non writeables."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return Implementation1NonWriteableQuery(self._client)

    def select(self) -> Implementation1NonWriteableQuery:
        """Start selecting from implementation 1 non writeables."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return Implementation1NonWriteableQuery(self._client)

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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Implementation1NonWriteableFields | Sequence[Implementation1NonWriteableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
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
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

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
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )
