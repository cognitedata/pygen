from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from omni.data_classes._main_interface import (
    MainInterfaceQuery,
    _MAININTERFACE_PROPERTIES_BY_FIELD,
    _create_main_interface_filter,
)
from omni.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    MainInterface,
    MainInterfaceWrite,
    MainInterfaceFields,
    MainInterfaceList,
    MainInterfaceWriteList,
    MainInterfaceTextFields,
    Implementation1,
    SubInterface,
)


class MainInterfaceAPI(NodeAPI[MainInterface, MainInterfaceWrite, MainInterfaceList, MainInterfaceWriteList]):
    _view_id = dm.ViewId("sp_pygen_models", "MainInterface", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _MAININTERFACE_PROPERTIES_BY_FIELD
    _direct_children_by_external_id: ClassVar[dict[str, type[DomainModel]]] = {
        "Implementation1": Implementation1,
        "SubInterface": SubInterface,
    }
    _class_type = MainInterface
    _class_list = MainInterfaceList
    _class_write_list = MainInterfaceWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["Implementation1", "SubInterface"]] | None = None,
    ) -> MainInterface | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["Implementation1", "SubInterface"]] | None = None,
    ) -> MainInterfaceList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["Implementation1", "SubInterface"]] | None = None,
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

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> main_interface = client.main_interface.retrieve(
                ...     "my_main_interface"
                ... )

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
            limit: Maximum number of main interfaces to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results main interfaces matching the query.

        Examples:

           Search for 'my_main_interface' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> main_interfaces = client.main_interface.search(
                ...     'my_main_interface'
                ... )

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
            limit: Maximum number of main interfaces to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

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
            limit: Maximum number of main interfaces to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

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

    def select(self) -> MainInterfaceQuery:
        """Start selecting from main interfaces."""
        return MainInterfaceQuery(self._client)

    def _build(
        self,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        chunk_size: int | None = None,
    ) -> QueryExecutor:
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                sort=sort,
                limit=limit,
                max_retrieve_batch_limit=chunk_size,
                has_container_fields=True,
            )
        )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[MainInterfaceList]:
        """Iterate over main interfaces

        Args:
            chunk_size: The number of main interfaces to return in each iteration. Defaults to 100.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of main interfaces to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of main interfaces

        Examples:

            Iterate main interfaces in chunks of 100 up to 2000 items:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for main_interfaces in client.main_interface.iterate(chunk_size=100, limit=2000):
                ...     for main_interface in main_interfaces:
                ...         print(main_interface.external_id)

            Iterate main interfaces in chunks of 100 sorted by external_id in descending order:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for main_interfaces in client.main_interface.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for main_interface in main_interfaces:
                ...         print(main_interface.external_id)

            Iterate main interfaces in chunks of 100 and use cursors to resume the iteration:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> for first_iteration in client.main_interface.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for main_interfaces in client.main_interface.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for main_interface in main_interfaces:
                ...         print(main_interface.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_main_interface_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

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
            limit: Maximum number of main interfaces to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

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
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
