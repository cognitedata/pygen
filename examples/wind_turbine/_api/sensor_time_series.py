from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from wind_turbine._api._core import (
    DEFAULT_LIMIT_READ,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
)
from omni.data_classes._core.query import (
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from wind_turbine.data_classes._sensor_time_series import (
    SensorTimeSeriesQuery,
    _SENSORTIMESERIES_PROPERTIES_BY_FIELD,
    _create_sensor_time_series_filter,
)
from wind_turbine.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    SensorTimeSeries,
    SensorTimeSeriesWrite,
    SensorTimeSeriesFields,
    SensorTimeSeriesList,
    SensorTimeSeriesWriteList,
    SensorTimeSeriesTextFields,
)


class SensorTimeSeriesAPI(
    NodeAPI[SensorTimeSeries, SensorTimeSeriesWrite, SensorTimeSeriesList, SensorTimeSeriesWriteList]
):
    _view_id = dm.ViewId("sp_pygen_power", "SensorTimeSeries", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _SENSORTIMESERIES_PROPERTIES_BY_FIELD
    _class_type = SensorTimeSeries
    _class_list = SensorTimeSeriesList
    _class_write_list = SensorTimeSeriesWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> SensorTimeSeries | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> SensorTimeSeriesList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> SensorTimeSeries | SensorTimeSeriesList | None:
        """Retrieve one or more sensor time series by id(s).

        Args:
            external_id: External id or list of external ids of the sensor time series.
            space: The space where all the sensor time series are located.

        Returns:
            The requested sensor time series.

        Examples:

            Retrieve sensor_time_series by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> sensor_time_series = client.sensor_time_series.retrieve(
                ...     "my_sensor_time_series"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: SensorTimeSeriesTextFields | SequenceNotStr[SensorTimeSeriesTextFields] | None = None,
        concept_id: str | list[str] | None = None,
        concept_id_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        standard_name: str | list[str] | None = None,
        standard_name_prefix: str | None = None,
        type_: Literal["numeric", "string"] | list[Literal["numeric", "string"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: SensorTimeSeriesFields | SequenceNotStr[SensorTimeSeriesFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> SensorTimeSeriesList:
        """Search sensor time series

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            concept_id: The concept id to filter on.
            concept_id_prefix: The prefix of the concept id to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_step: The is step to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source_unit: The source unit to filter on.
            source_unit_prefix: The prefix of the source unit to filter on.
            standard_name: The standard name to filter on.
            standard_name_prefix: The prefix of the standard name to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor time series to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results sensor time series matching the query.

        Examples:

           Search for 'my_sensor_time_series' in all text properties:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> sensor_time_series_list = client.sensor_time_series.search(
                ...     'my_sensor_time_series'
                ... )

        """
        filter_ = _create_sensor_time_series_filter(
            self._view_id,
            concept_id,
            concept_id_prefix,
            description,
            description_prefix,
            is_step,
            name,
            name_prefix,
            source_unit,
            source_unit_prefix,
            standard_name,
            standard_name_prefix,
            type_,
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
        property: SensorTimeSeriesFields | SequenceNotStr[SensorTimeSeriesFields] | None = None,
        query: str | None = None,
        search_property: SensorTimeSeriesTextFields | SequenceNotStr[SensorTimeSeriesTextFields] | None = None,
        concept_id: str | list[str] | None = None,
        concept_id_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        standard_name: str | list[str] | None = None,
        standard_name_prefix: str | None = None,
        type_: Literal["numeric", "string"] | list[Literal["numeric", "string"]] | None = None,
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
        property: SensorTimeSeriesFields | SequenceNotStr[SensorTimeSeriesFields] | None = None,
        query: str | None = None,
        search_property: SensorTimeSeriesTextFields | SequenceNotStr[SensorTimeSeriesTextFields] | None = None,
        concept_id: str | list[str] | None = None,
        concept_id_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        standard_name: str | list[str] | None = None,
        standard_name_prefix: str | None = None,
        type_: Literal["numeric", "string"] | list[Literal["numeric", "string"]] | None = None,
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
        group_by: SensorTimeSeriesFields | SequenceNotStr[SensorTimeSeriesFields],
        property: SensorTimeSeriesFields | SequenceNotStr[SensorTimeSeriesFields] | None = None,
        query: str | None = None,
        search_property: SensorTimeSeriesTextFields | SequenceNotStr[SensorTimeSeriesTextFields] | None = None,
        concept_id: str | list[str] | None = None,
        concept_id_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        standard_name: str | list[str] | None = None,
        standard_name_prefix: str | None = None,
        type_: Literal["numeric", "string"] | list[Literal["numeric", "string"]] | None = None,
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
        group_by: SensorTimeSeriesFields | SequenceNotStr[SensorTimeSeriesFields] | None = None,
        property: SensorTimeSeriesFields | SequenceNotStr[SensorTimeSeriesFields] | None = None,
        query: str | None = None,
        search_property: SensorTimeSeriesTextFields | SequenceNotStr[SensorTimeSeriesTextFields] | None = None,
        concept_id: str | list[str] | None = None,
        concept_id_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        standard_name: str | list[str] | None = None,
        standard_name_prefix: str | None = None,
        type_: Literal["numeric", "string"] | list[Literal["numeric", "string"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across sensor time series

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            concept_id: The concept id to filter on.
            concept_id_prefix: The prefix of the concept id to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_step: The is step to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source_unit: The source unit to filter on.
            source_unit_prefix: The prefix of the source unit to filter on.
            standard_name: The standard name to filter on.
            standard_name_prefix: The prefix of the standard name to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor time series to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count sensor time series in space `my_space`:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> result = client.sensor_time_series.aggregate("count", space="my_space")

        """

        filter_ = _create_sensor_time_series_filter(
            self._view_id,
            concept_id,
            concept_id_prefix,
            description,
            description_prefix,
            is_step,
            name,
            name_prefix,
            source_unit,
            source_unit_prefix,
            standard_name,
            standard_name_prefix,
            type_,
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
        property: SensorTimeSeriesFields,
        interval: float,
        query: str | None = None,
        search_property: SensorTimeSeriesTextFields | SequenceNotStr[SensorTimeSeriesTextFields] | None = None,
        concept_id: str | list[str] | None = None,
        concept_id_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        standard_name: str | list[str] | None = None,
        standard_name_prefix: str | None = None,
        type_: Literal["numeric", "string"] | list[Literal["numeric", "string"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for sensor time series

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            concept_id: The concept id to filter on.
            concept_id_prefix: The prefix of the concept id to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_step: The is step to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source_unit: The source unit to filter on.
            source_unit_prefix: The prefix of the source unit to filter on.
            standard_name: The standard name to filter on.
            standard_name_prefix: The prefix of the standard name to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor time series to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_sensor_time_series_filter(
            self._view_id,
            concept_id,
            concept_id_prefix,
            description,
            description_prefix,
            is_step,
            name,
            name_prefix,
            source_unit,
            source_unit_prefix,
            standard_name,
            standard_name_prefix,
            type_,
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

    def select(self) -> SensorTimeSeriesQuery:
        """Start selecting from sensor time series."""
        return SensorTimeSeriesQuery(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> list[dict[str, Any]]:
        builder = QueryBuilder()
        factory = QueryStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                sort=sort,
                limit=limit,
                has_container_fields=True,
            )
        )
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        return QueryUnpacker(builder, edges=unpack_edges).unpack()

    def list(
        self,
        concept_id: str | list[str] | None = None,
        concept_id_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_step: bool | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        source_unit: str | list[str] | None = None,
        source_unit_prefix: str | None = None,
        standard_name: str | list[str] | None = None,
        standard_name_prefix: str | None = None,
        type_: Literal["numeric", "string"] | list[Literal["numeric", "string"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: SensorTimeSeriesFields | Sequence[SensorTimeSeriesFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> SensorTimeSeriesList:
        """List/filter sensor time series

        Args:
            concept_id: The concept id to filter on.
            concept_id_prefix: The prefix of the concept id to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_step: The is step to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            source_unit: The source unit to filter on.
            source_unit_prefix: The prefix of the source unit to filter on.
            standard_name: The standard name to filter on.
            standard_name_prefix: The prefix of the standard name to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor time series to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested sensor time series

        Examples:

            List sensor time series and limit to 5:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> sensor_time_series_list = client.sensor_time_series.list(limit=5)

        """
        filter_ = _create_sensor_time_series_filter(
            self._view_id,
            concept_id,
            concept_id_prefix,
            description,
            description_prefix,
            is_step,
            name,
            name_prefix,
            source_unit,
            source_unit_prefix,
            standard_name,
            standard_name_prefix,
            type_,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
