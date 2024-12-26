from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from wind_turbine._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from wind_turbine._api.solar_panel_query import SolarPanelQueryAPI
from wind_turbine.data_classes import (
    ResourcesWriteResult,
    SensorTimeSeries,
    SolarPanel,
    SolarPanelFields,
    SolarPanelList,
    SolarPanelTextFields,
    SolarPanelWrite,
    SolarPanelWriteList,
)
from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
)
from wind_turbine.data_classes._solar_panel import (
    _SOLARPANEL_PROPERTIES_BY_FIELD,
    SolarPanelQuery,
    _create_solar_panel_filter,
)


class SolarPanelAPI(NodeAPI[SolarPanel, SolarPanelWrite, SolarPanelList, SolarPanelWriteList]):
    _view_id = dm.ViewId("sp_pygen_power", "SolarPanel", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _SOLARPANEL_PROPERTIES_BY_FIELD
    _class_type = SolarPanel
    _class_list = SolarPanelList
    _class_write_list = SolarPanelWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        efficiency: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        orientation: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> SolarPanelQueryAPI[SolarPanelList]:
        """Query starting at solar panels.

        Args:
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            efficiency: The efficiency to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            orientation: The orientation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of solar panels to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for solar panels.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_solar_panel_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            description,
            description_prefix,
            efficiency,
            name,
            name_prefix,
            orientation,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(SolarPanelList)
        return SolarPanelQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        solar_panel: SolarPanelWrite | Sequence[SolarPanelWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) solar panels.

        Note: This method iterates through all nodes and timeseries linked to solar_panel
        and creates them including the edges
        between the nodes. For example, if any of
        `efficiency` or `orientation`
        are set, then these nodes as well as any nodes linked to them, and all the edges linking
        these nodes will be created.

        Args:
            solar_panel: Solar panel or
                sequence of solar panels to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None.
                However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new solar_panel:

                >>> from wind_turbine import WindTurbineClient
                >>> from wind_turbine.data_classes import SolarPanelWrite
                >>> client = WindTurbineClient()
                >>> solar_panel = SolarPanelWrite(
                ...     external_id="my_solar_panel", ...
                ... )
                >>> result = client.solar_panel.apply(solar_panel)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.solar_panel.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(solar_panel, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more solar panel.

        Args:
            external_id: External id of the solar panel to delete.
            space: The space where all the solar panel are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete solar_panel by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> client.solar_panel.delete("my_solar_panel")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.solar_panel.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SolarPanel | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SolarPanelList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> SolarPanel | SolarPanelList | None:
        """Retrieve one or more solar panels by id(s).

        Args:
            external_id: External id or list of external ids of the solar panels.
            space: The space where all the solar panels are located.

        Returns:
            The requested solar panels.

        Examples:

            Retrieve solar_panel by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> solar_panel = client.solar_panel.retrieve("my_solar_panel")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: SolarPanelTextFields | SequenceNotStr[SolarPanelTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        efficiency: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        orientation: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: SolarPanelFields | SequenceNotStr[SolarPanelFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> SolarPanelList:
        """Search solar panels

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            efficiency: The efficiency to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            orientation: The orientation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of solar panels to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results solar panels matching the query.

        Examples:

           Search for 'my_solar_panel' in all text properties:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> solar_panels = client.solar_panel.search('my_solar_panel')

        """
        filter_ = _create_solar_panel_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            description,
            description_prefix,
            efficiency,
            name,
            name_prefix,
            orientation,
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
        property: SolarPanelFields | SequenceNotStr[SolarPanelFields] | None = None,
        query: str | None = None,
        search_property: SolarPanelTextFields | SequenceNotStr[SolarPanelTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        efficiency: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        orientation: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        property: SolarPanelFields | SequenceNotStr[SolarPanelFields] | None = None,
        query: str | None = None,
        search_property: SolarPanelTextFields | SequenceNotStr[SolarPanelTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        efficiency: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        orientation: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        group_by: SolarPanelFields | SequenceNotStr[SolarPanelFields],
        property: SolarPanelFields | SequenceNotStr[SolarPanelFields] | None = None,
        query: str | None = None,
        search_property: SolarPanelTextFields | SequenceNotStr[SolarPanelTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        efficiency: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        orientation: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        group_by: SolarPanelFields | SequenceNotStr[SolarPanelFields] | None = None,
        property: SolarPanelFields | SequenceNotStr[SolarPanelFields] | None = None,
        query: str | None = None,
        search_property: SolarPanelTextFields | SequenceNotStr[SolarPanelTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        efficiency: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        orientation: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across solar panels

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            efficiency: The efficiency to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            orientation: The orientation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of solar panels to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count solar panels in space `my_space`:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> result = client.solar_panel.aggregate("count", space="my_space")

        """

        filter_ = _create_solar_panel_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            description,
            description_prefix,
            efficiency,
            name,
            name_prefix,
            orientation,
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
        property: SolarPanelFields,
        interval: float,
        query: str | None = None,
        search_property: SolarPanelTextFields | SequenceNotStr[SolarPanelTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        efficiency: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        orientation: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for solar panels

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            efficiency: The efficiency to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            orientation: The orientation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of solar panels to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_solar_panel_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            description,
            description_prefix,
            efficiency,
            name,
            name_prefix,
            orientation,
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

    def query(self) -> SolarPanelQuery:
        """Start a query for solar panels."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return SolarPanelQuery(self._client)

    def select(self) -> SolarPanelQuery:
        """Start selecting from solar panels."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return SolarPanelQuery(self._client)

    def list(
        self,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        efficiency: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        orientation: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: SolarPanelFields | Sequence[SolarPanelFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> SolarPanelList:
        """List/filter solar panels

        Args:
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            efficiency: The efficiency to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            orientation: The orientation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of solar panels to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `efficiency` and `orientation`
                for the solar panels. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the
                identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested solar panels

        Examples:

            List solar panels and limit to 5:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> solar_panels = client.solar_panel.list(limit=5)

        """
        filter_ = _create_solar_panel_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            description,
            description_prefix,
            efficiency,
            name,
            name_prefix,
            orientation,
            external_id_prefix,
            space,
            filter,
        )

        if retrieve_connections == "skip":
            return self._list(
                limit=limit,
                filter=filter_,
                sort_by=sort_by,  # type: ignore[arg-type]
                direction=direction,
                sort=sort,
            )

        builder = DataClassQueryBuilder(SolarPanelList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                SolarPanel,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("efficiency"),
                    ),
                    SensorTimeSeries,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("orientation"),
                    ),
                    SensorTimeSeries,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
