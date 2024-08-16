from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni_multi_pydantic_v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    QueryBuilder,
)
from omni_multi_pydantic_v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    MainInterface,
    MainInterfaceWrite,
    MainInterfaceFields,
    MainInterfaceList,
    MainInterfaceWriteList,
    MainInterfaceTextFields,
    SubInterface,
)
from omni_multi_pydantic_v1.data_classes._main_interface import (
    MainInterfaceQuery,
    _MAININTERFACE_PROPERTIES_BY_FIELD,
    _create_main_interface_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from .main_interface_query import MainInterfaceQueryAPI


class MainInterfaceAPI(NodeAPI[MainInterface, MainInterfaceWrite, MainInterfaceList, MainInterfaceWriteList]):
    _view_id = dm.ViewId("pygen-models", "MainInterface", "1")
    _properties_by_field = _MAININTERFACE_PROPERTIES_BY_FIELD
    _direct_children_by_external_id = {
        "SubInterface": SubInterface,
    }
    _class_type = MainInterface
    _class_list = MainInterfaceList
    _class_write_list = MainInterfaceWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> MainInterfaceQueryAPI[MainInterfaceList]:
        """Query starting at main interfaces.

        Args:
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main interfaces to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for main interfaces.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_main_interface_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(MainInterfaceList)
        return MainInterfaceQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        main_interface: MainInterfaceWrite | Sequence[MainInterfaceWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) main interfaces.

        Args:
            main_interface: Main interface or sequence of main interfaces to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new main_interface:

                >>> from omni_multi_pydantic_v1 import OmniMultiClient
                >>> from omni_multi_pydantic_v1.data_classes import MainInterfaceWrite
                >>> client = OmniMultiClient()
                >>> main_interface = MainInterfaceWrite(external_id="my_main_interface", ...)
                >>> result = client.main_interface.apply(main_interface)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.main_interface.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(main_interface, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more main interface.

        Args:
            external_id: External id of the main interface to delete.
            space: The space where all the main interface are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete main_interface by id:

                >>> from omni_multi_pydantic_v1 import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> client.main_interface.delete("my_main_interface")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.main_interface.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self,
        external_id: str,
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["SubInterface"]] | None = None,
    ) -> MainInterface | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["SubInterface"]] | None = None,
    ) -> MainInterfaceList: ...

    def retrieve(
        self,
        external_id: str | SequenceNotStr[str],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["SubInterface"]] | None = None,
    ) -> MainInterface | MainInterfaceList | None:
        """Retrieve one or more main interfaces by id(s).

        Args:
            external_id: External id or list of external ids of the main interfaces.
            space: The space where all the main interfaces are located.
            as_child_class: If you want to retrieve the main interfaces as a child class,
                you can specify the child class here. Note that if one node has properties in
                multiple child classes, you will get duplicate nodes in the result.

        Returns:
            The requested main interfaces.

        Examples:

            Retrieve main_interface by id:

                >>> from omni_multi_pydantic_v1 import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> main_interface = client.main_interface.retrieve("my_main_interface")

        """
        return self._retrieve(external_id, space, as_child_class=as_child_class)

    def search(
        self,
        query: str,
        properties: MainInterfaceTextFields | SequenceNotStr[MainInterfaceTextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: MainInterfaceFields | SequenceNotStr[MainInterfaceFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> MainInterfaceList:
        """Search main interfaces

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main interfaces to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results main interfaces matching the query.

        Examples:

           Search for 'my_main_interface' in all text properties:

                >>> from omni_multi_pydantic_v1 import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> main_interfaces = client.main_interface.search('my_main_interface')

        """
        filter_ = _create_main_interface_filter(
            self._view_id,
            main_value,
            main_value_prefix,
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
        property: MainInterfaceFields | SequenceNotStr[MainInterfaceFields] | None = None,
        query: str | None = None,
        search_property: MainInterfaceTextFields | SequenceNotStr[MainInterfaceTextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
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
        property: MainInterfaceFields | SequenceNotStr[MainInterfaceFields] | None = None,
        query: str | None = None,
        search_property: MainInterfaceTextFields | SequenceNotStr[MainInterfaceTextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
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
        group_by: MainInterfaceFields | SequenceNotStr[MainInterfaceFields],
        property: MainInterfaceFields | SequenceNotStr[MainInterfaceFields] | None = None,
        query: str | None = None,
        search_property: MainInterfaceTextFields | SequenceNotStr[MainInterfaceTextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
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
        group_by: MainInterfaceFields | SequenceNotStr[MainInterfaceFields] | None = None,
        property: MainInterfaceFields | SequenceNotStr[MainInterfaceFields] | None = None,
        query: str | None = None,
        search_property: MainInterfaceTextFields | SequenceNotStr[MainInterfaceTextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across main interfaces

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main interfaces to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count main interfaces in space `my_space`:

                >>> from omni_multi_pydantic_v1 import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> result = client.main_interface.aggregate("count", space="my_space")

        """

        filter_ = _create_main_interface_filter(
            self._view_id,
            main_value,
            main_value_prefix,
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
        property: MainInterfaceFields,
        interval: float,
        query: str | None = None,
        search_property: MainInterfaceTextFields | SequenceNotStr[MainInterfaceTextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for main interfaces

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main interfaces to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_main_interface_filter(
            self._view_id,
            main_value,
            main_value_prefix,
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

    def query(self) -> MainInterfaceQuery:
        """Start a query for connection item as."""
        warnings.warn("The .query is in alpha and is subject to breaking changes without notice.")
        return MainInterfaceQuery(self._client)

    def list(
        self,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: MainInterfaceFields | Sequence[MainInterfaceFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> MainInterfaceList:
        """List/filter main interfaces

        Args:
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main interfaces to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested main interfaces

        Examples:

            List main interfaces and limit to 5:

                >>> from omni_multi_pydantic_v1 import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> main_interfaces = client.main_interface.list(limit=5)

        """
        filter_ = _create_main_interface_filter(
            self._view_id,
            main_value,
            main_value_prefix,
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
