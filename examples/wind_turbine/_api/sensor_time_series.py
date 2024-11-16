from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
from wind_turbine.data_classes import (
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
from wind_turbine.data_classes._sensor_time_series import (
    SensorTimeSeriesQuery,
    _SENSORTIMESERIES_PROPERTIES_BY_FIELD,
    _create_sensor_time_series_filter,
)
from wind_turbine._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from wind_turbine._api.sensor_time_series_query import SensorTimeSeriesQueryAPI


class SensorTimeSeriesAPI(
    NodeAPI[SensorTimeSeries, SensorTimeSeriesWrite, SensorTimeSeriesList, SensorTimeSeriesWriteList]
):
    _view_id = dm.ViewId("sp_pygen_power", "SensorTimeSeries", "1")
    _properties_by_field = _SENSORTIMESERIES_PROPERTIES_BY_FIELD
    _class_type = SensorTimeSeries
    _class_list = SensorTimeSeriesList
    _class_write_list = SensorTimeSeriesWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
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
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> SensorTimeSeriesQueryAPI[SensorTimeSeriesList]:
        """Query starting at sensor time series.

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for sensor time series.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
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
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(SensorTimeSeriesList)
        return SensorTimeSeriesQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        sensor_time_series: SensorTimeSeriesWrite | Sequence[SensorTimeSeriesWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) sensor time series.

        Args:
            sensor_time_series: Sensor time series or sequence of sensor time series to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new sensor_time_series:

                >>> from wind_turbine import WindTurbineClient
                >>> from wind_turbine.data_classes import SensorTimeSeriesWrite
                >>> client = WindTurbineClient()
                >>> sensor_time_series = SensorTimeSeriesWrite(external_id="my_sensor_time_series", ...)
                >>> result = client.sensor_time_series.apply(sensor_time_series)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.sensor_time_series.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(sensor_time_series, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more sensor time series.

        Args:
            external_id: External id of the sensor time series to delete.
            space: The space where all the sensor time series are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete sensor_time_series by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> client.sensor_time_series.delete("my_sensor_time_series")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.sensor_time_series.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SensorTimeSeries | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
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
                >>> sensor_time_series = client.sensor_time_series.retrieve("my_sensor_time_series")

        """
        return self._retrieve(external_id, space)

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results sensor time series matching the query.

        Examples:

           Search for 'my_sensor_time_series' in all text properties:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> sensor_time_series_list = client.sensor_time_series.search('my_sensor_time_series')

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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

    def query(self) -> SensorTimeSeriesQuery:
        """Start a query for sensor time series."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return SensorTimeSeriesQuery(self._client)

    def select(self) -> SensorTimeSeriesQuery:
        """Start selecting from sensor time series."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return SensorTimeSeriesQuery(self._client)

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
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
