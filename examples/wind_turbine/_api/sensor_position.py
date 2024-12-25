from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from wind_turbine._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from wind_turbine._api.sensor_position_query import SensorPositionQueryAPI
from wind_turbine.data_classes import (
    Blade,
    ResourcesWriteResult,
    SensorPosition,
    SensorPositionFields,
    SensorPositionList,
    SensorPositionTextFields,
    SensorPositionWrite,
    SensorPositionWriteList,
    SensorTimeSeries,
)
from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
)
from wind_turbine.data_classes._sensor_position import (
    _SENSORPOSITION_PROPERTIES_BY_FIELD,
    SensorPositionQuery,
    _create_sensor_position_filter,
)


class SensorPositionAPI(NodeAPI[SensorPosition, SensorPositionWrite, SensorPositionList, SensorPositionWriteList]):
    _view_id = dm.ViewId("sp_pygen_power", "SensorPosition", "1")
    _properties_by_field = _SENSORPOSITION_PROPERTIES_BY_FIELD
    _class_type = SensorPosition
    _class_list = SensorPositionList
    _class_write_list = SensorPositionWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        blade: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewisewise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_position: float | None = None,
        max_position: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> SensorPositionQueryAPI[SensorPositionList]:
        """Query starting at sensor positions.

        Args:
            blade: The blade to filter on.
            edgewise_bend_mom_crosstalk_corrected: The edgewise bend mom crosstalk corrected to filter on.
            edgewise_bend_mom_offset: The edgewise bend mom offset to filter on.
            edgewise_bend_mom_offset_crosstalk_corrected: The edgewise bend mom offset crosstalk corrected to filter on.
            edgewisewise_bend_mom: The edgewisewise bend mom to filter on.
            flapwise_bend_mom: The flapwise bend mom to filter on.
            flapwise_bend_mom_crosstalk_corrected: The flapwise bend mom crosstalk corrected to filter on.
            flapwise_bend_mom_offset: The flapwise bend mom offset to filter on.
            flapwise_bend_mom_offset_crosstalk_corrected: The flapwise bend mom offset crosstalk corrected to filter on.
            min_position: The minimum value of the position to filter on.
            max_position: The maximum value of the position to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor positions to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for sensor positions.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_sensor_position_filter(
            self._view_id,
            blade,
            edgewise_bend_mom_crosstalk_corrected,
            edgewise_bend_mom_offset,
            edgewise_bend_mom_offset_crosstalk_corrected,
            edgewisewise_bend_mom,
            flapwise_bend_mom,
            flapwise_bend_mom_crosstalk_corrected,
            flapwise_bend_mom_offset,
            flapwise_bend_mom_offset_crosstalk_corrected,
            min_position,
            max_position,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(SensorPositionList)
        return SensorPositionQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        sensor_position: SensorPositionWrite | Sequence[SensorPositionWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) sensor positions.

        Note: This method iterates through all nodes and timeseries linked to sensor_position
        and creates them including the edges
        between the nodes. For example, if any of `blade`, `edgewise_bend_mom_crosstalk_corrected`, `edgewise_bend_mom_offset`, `edgewise_bend_mom_offset_crosstalk_corrected`, `edgewisewise_bend_mom`, `flapwise_bend_mom`, `flapwise_bend_mom_crosstalk_corrected`, `flapwise_bend_mom_offset` or `flapwise_bend_mom_offset_crosstalk_corrected` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            sensor_position: Sensor position or
                sequence of sensor positions to upsert.
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

            Create a new sensor_position:

                >>> from wind_turbine import WindTurbineClient
                >>> from wind_turbine.data_classes import SensorPositionWrite
                >>> client = WindTurbineClient()
                >>> sensor_position = SensorPositionWrite(external_id="my_sensor_position", ...)
                >>> result = client.sensor_position.apply(sensor_position)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.sensor_position.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(sensor_position, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more sensor position.

        Args:
            external_id: External id of the sensor position to delete.
            space: The space where all the sensor position are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete sensor_position by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> client.sensor_position.delete("my_sensor_position")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.sensor_position.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SensorPosition | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SensorPositionList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> SensorPosition | SensorPositionList | None:
        """Retrieve one or more sensor positions by id(s).

        Args:
            external_id: External id or list of external ids of the sensor positions.
            space: The space where all the sensor positions are located.

        Returns:
            The requested sensor positions.

        Examples:

            Retrieve sensor_position by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> sensor_position = client.sensor_position.retrieve("my_sensor_position")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: SensorPositionTextFields | SequenceNotStr[SensorPositionTextFields] | None = None,
        blade: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewisewise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_position: float | None = None,
        max_position: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: SensorPositionFields | SequenceNotStr[SensorPositionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> SensorPositionList:
        """Search sensor positions

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            blade: The blade to filter on.
            edgewise_bend_mom_crosstalk_corrected: The edgewise bend mom crosstalk corrected to filter on.
            edgewise_bend_mom_offset: The edgewise bend mom offset to filter on.
            edgewise_bend_mom_offset_crosstalk_corrected: The edgewise bend mom offset crosstalk corrected to filter on.
            edgewisewise_bend_mom: The edgewisewise bend mom to filter on.
            flapwise_bend_mom: The flapwise bend mom to filter on.
            flapwise_bend_mom_crosstalk_corrected: The flapwise bend mom crosstalk corrected to filter on.
            flapwise_bend_mom_offset: The flapwise bend mom offset to filter on.
            flapwise_bend_mom_offset_crosstalk_corrected: The flapwise bend mom offset crosstalk corrected to filter on.
            min_position: The minimum value of the position to filter on.
            max_position: The maximum value of the position to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor positions to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results sensor positions matching the query.

        Examples:

           Search for 'my_sensor_position' in all text properties:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> sensor_positions = client.sensor_position.search('my_sensor_position')

        """
        filter_ = _create_sensor_position_filter(
            self._view_id,
            blade,
            edgewise_bend_mom_crosstalk_corrected,
            edgewise_bend_mom_offset,
            edgewise_bend_mom_offset_crosstalk_corrected,
            edgewisewise_bend_mom,
            flapwise_bend_mom,
            flapwise_bend_mom_crosstalk_corrected,
            flapwise_bend_mom_offset,
            flapwise_bend_mom_offset_crosstalk_corrected,
            min_position,
            max_position,
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
        property: SensorPositionFields | SequenceNotStr[SensorPositionFields] | None = None,
        blade: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewisewise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_position: float | None = None,
        max_position: float | None = None,
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
        property: SensorPositionFields | SequenceNotStr[SensorPositionFields] | None = None,
        blade: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewisewise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_position: float | None = None,
        max_position: float | None = None,
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
        group_by: SensorPositionFields | SequenceNotStr[SensorPositionFields],
        property: SensorPositionFields | SequenceNotStr[SensorPositionFields] | None = None,
        blade: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewisewise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_position: float | None = None,
        max_position: float | None = None,
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
        group_by: SensorPositionFields | SequenceNotStr[SensorPositionFields] | None = None,
        property: SensorPositionFields | SequenceNotStr[SensorPositionFields] | None = None,
        blade: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewisewise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_position: float | None = None,
        max_position: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across sensor positions

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            blade: The blade to filter on.
            edgewise_bend_mom_crosstalk_corrected: The edgewise bend mom crosstalk corrected to filter on.
            edgewise_bend_mom_offset: The edgewise bend mom offset to filter on.
            edgewise_bend_mom_offset_crosstalk_corrected: The edgewise bend mom offset crosstalk corrected to filter on.
            edgewisewise_bend_mom: The edgewisewise bend mom to filter on.
            flapwise_bend_mom: The flapwise bend mom to filter on.
            flapwise_bend_mom_crosstalk_corrected: The flapwise bend mom crosstalk corrected to filter on.
            flapwise_bend_mom_offset: The flapwise bend mom offset to filter on.
            flapwise_bend_mom_offset_crosstalk_corrected: The flapwise bend mom offset crosstalk corrected to filter on.
            min_position: The minimum value of the position to filter on.
            max_position: The maximum value of the position to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor positions to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count sensor positions in space `my_space`:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> result = client.sensor_position.aggregate("count", space="my_space")

        """

        filter_ = _create_sensor_position_filter(
            self._view_id,
            blade,
            edgewise_bend_mom_crosstalk_corrected,
            edgewise_bend_mom_offset,
            edgewise_bend_mom_offset_crosstalk_corrected,
            edgewisewise_bend_mom,
            flapwise_bend_mom,
            flapwise_bend_mom_crosstalk_corrected,
            flapwise_bend_mom_offset,
            flapwise_bend_mom_offset_crosstalk_corrected,
            min_position,
            max_position,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=None,
            search_properties=None,
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: SensorPositionFields,
        interval: float,
        blade: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewisewise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_position: float | None = None,
        max_position: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for sensor positions

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            blade: The blade to filter on.
            edgewise_bend_mom_crosstalk_corrected: The edgewise bend mom crosstalk corrected to filter on.
            edgewise_bend_mom_offset: The edgewise bend mom offset to filter on.
            edgewise_bend_mom_offset_crosstalk_corrected: The edgewise bend mom offset crosstalk corrected to filter on.
            edgewisewise_bend_mom: The edgewisewise bend mom to filter on.
            flapwise_bend_mom: The flapwise bend mom to filter on.
            flapwise_bend_mom_crosstalk_corrected: The flapwise bend mom crosstalk corrected to filter on.
            flapwise_bend_mom_offset: The flapwise bend mom offset to filter on.
            flapwise_bend_mom_offset_crosstalk_corrected: The flapwise bend mom offset crosstalk corrected to filter on.
            min_position: The minimum value of the position to filter on.
            max_position: The maximum value of the position to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor positions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_sensor_position_filter(
            self._view_id,
            blade,
            edgewise_bend_mom_crosstalk_corrected,
            edgewise_bend_mom_offset,
            edgewise_bend_mom_offset_crosstalk_corrected,
            edgewisewise_bend_mom,
            flapwise_bend_mom,
            flapwise_bend_mom_crosstalk_corrected,
            flapwise_bend_mom_offset,
            flapwise_bend_mom_offset_crosstalk_corrected,
            min_position,
            max_position,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            None,
            None,
            limit,
            filter_,
        )

    def query(self) -> SensorPositionQuery:
        """Start a query for sensor positions."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return SensorPositionQuery(self._client)

    def select(self) -> SensorPositionQuery:
        """Start selecting from sensor positions."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return SensorPositionQuery(self._client)

    def list(
        self,
        blade: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        edgewisewise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        flapwise_bend_mom_offset_crosstalk_corrected: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_position: float | None = None,
        max_position: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: SensorPositionFields | Sequence[SensorPositionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> SensorPositionList:
        """List/filter sensor positions

        Args:
            blade: The blade to filter on.
            edgewise_bend_mom_crosstalk_corrected: The edgewise bend mom crosstalk corrected to filter on.
            edgewise_bend_mom_offset: The edgewise bend mom offset to filter on.
            edgewise_bend_mom_offset_crosstalk_corrected: The edgewise bend mom offset crosstalk corrected to filter on.
            edgewisewise_bend_mom: The edgewisewise bend mom to filter on.
            flapwise_bend_mom: The flapwise bend mom to filter on.
            flapwise_bend_mom_crosstalk_corrected: The flapwise bend mom crosstalk corrected to filter on.
            flapwise_bend_mom_offset: The flapwise bend mom offset to filter on.
            flapwise_bend_mom_offset_crosstalk_corrected: The flapwise bend mom offset crosstalk corrected to filter on.
            min_position: The minimum value of the position to filter on.
            max_position: The maximum value of the position to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor positions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `blade`, `edgewise_bend_mom_crosstalk_corrected`, `edgewise_bend_mom_offset`, `edgewise_bend_mom_offset_crosstalk_corrected`, `edgewisewise_bend_mom`, `flapwise_bend_mom`, `flapwise_bend_mom_crosstalk_corrected`, `flapwise_bend_mom_offset` and `flapwise_bend_mom_offset_crosstalk_corrected`
                for the sensor positions. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested sensor positions

        Examples:

            List sensor positions and limit to 5:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> sensor_positions = client.sensor_position.list(limit=5)

        """
        filter_ = _create_sensor_position_filter(
            self._view_id,
            blade,
            edgewise_bend_mom_crosstalk_corrected,
            edgewise_bend_mom_offset,
            edgewise_bend_mom_offset_crosstalk_corrected,
            edgewisewise_bend_mom,
            flapwise_bend_mom,
            flapwise_bend_mom_crosstalk_corrected,
            flapwise_bend_mom_offset,
            flapwise_bend_mom_offset_crosstalk_corrected,
            min_position,
            max_position,
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

        builder = DataClassQueryBuilder(SensorPositionList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                SensorPosition,
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
                        filter=dm.filters.HasData(views=[Blade._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("blade"),
                    ),
                    Blade,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("edgewise_bend_mom_crosstalk_corrected"),
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
                        through=self._view_id.as_property_ref("edgewise_bend_mom_offset"),
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
                        through=self._view_id.as_property_ref("edgewise_bend_mom_offset_crosstalk_corrected"),
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
                        through=self._view_id.as_property_ref("edgewisewise_bend_mom"),
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
                        through=self._view_id.as_property_ref("flapwise_bend_mom"),
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
                        through=self._view_id.as_property_ref("flapwise_bend_mom_crosstalk_corrected"),
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
                        through=self._view_id.as_property_ref("flapwise_bend_mom_offset"),
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
                        through=self._view_id.as_property_ref("flapwise_bend_mom_offset_crosstalk_corrected"),
                    ),
                    SensorTimeSeries,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
