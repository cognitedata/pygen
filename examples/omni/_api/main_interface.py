from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from omni.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    MainInterface,
    MainInterfaceApply,
    MainInterfaceFields,
    MainInterfaceList,
    MainInterfaceApplyList,
    MainInterfaceTextFields,
)
from omni.data_classes._main_interface import (
    _MAININTERFACE_PROPERTIES_BY_FIELD,
    _create_main_interface_filter,
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
from .main_interface_query import MainInterfaceQueryAPI


class MainInterfaceAPI(NodeAPI[MainInterface, MainInterfaceApply, MainInterfaceList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[MainInterface]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=MainInterface,
            class_list=MainInterfaceList,
            class_apply_list=MainInterfaceApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
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
        return MainInterfaceQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        main_interface: MainInterfaceApply | Sequence[MainInterfaceApply],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesApplyResult:
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

                >>> from omni import OmniClient
                >>> from omni.data_classes import MainInterfaceApply
                >>> client = OmniClient()
                >>> main_interface = MainInterfaceApply(external_id="my_main_interface", ...)
                >>> result = client.main_interface.apply(main_interface)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .apply method on the client instead. This means instead of "
            "`my_client.main_interface.apply(my_items)` please use `my_client.apply(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient.",
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

                >>> from omni import OmniClient
                >>> client = OmniClient()
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
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> MainInterface | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> MainInterfaceList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> MainInterface | MainInterfaceList | None:
        """Retrieve one or more main interfaces by id(s).

        Args:
            external_id: External id or list of external ids of the main interfaces.
            space: The space where all the main interfaces are located.

        Returns:
            The requested main interfaces.

        Examples:

            Retrieve main_interface by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> main_interface = client.main_interface.retrieve("my_main_interface")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: MainInterfaceTextFields | Sequence[MainInterfaceTextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
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

        Returns:
            Search results main interfaces matching the query.

        Examples:

           Search for 'my_main_interface' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
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
        return self._search(self._view_id, query, _MAININTERFACE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: MainInterfaceFields | Sequence[MainInterfaceFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MainInterfaceTextFields | Sequence[MainInterfaceTextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
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
        property: MainInterfaceFields | Sequence[MainInterfaceFields] | None = None,
        group_by: MainInterfaceFields | Sequence[MainInterfaceFields] = None,
        query: str | None = None,
        search_properties: MainInterfaceTextFields | Sequence[MainInterfaceTextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
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
        property: MainInterfaceFields | Sequence[MainInterfaceFields] | None = None,
        group_by: MainInterfaceFields | Sequence[MainInterfaceFields] | None = None,
        query: str | None = None,
        search_property: MainInterfaceTextFields | Sequence[MainInterfaceTextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across main interfaces

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
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

                >>> from omni import OmniClient
                >>> client = OmniClient()
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
            self._view_id,
            aggregate,
            _MAININTERFACE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MainInterfaceFields,
        interval: float,
        query: str | None = None,
        search_property: MainInterfaceTextFields | Sequence[MainInterfaceTextFields] | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
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
            self._view_id,
            property,
            interval,
            _MAININTERFACE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MainInterfaceList:
        """List/filter main interfaces

        Args:
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main interfaces to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested main interfaces

        Examples:

            List main interfaces and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
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
        return self._list(limit=limit, filter=filter_)
