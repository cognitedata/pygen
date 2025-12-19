from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from wind_turbine._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from wind_turbine.data_classes._sensor_position import (
    SensorPositionQuery,
    _SENSORPOSITION_PROPERTIES_BY_FIELD,
    _create_sensor_position_filter,
)
from wind_turbine.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    SensorPosition,
    SensorPositionWrite,
    SensorPositionFields,
    SensorPositionList,
    SensorPositionWriteList,
    SensorPositionTextFields,
    Blade,
    SensorTimeSeries,
)


class SensorPositionAPI(NodeAPI[SensorPosition, SensorPositionWrite, SensorPositionList, SensorPositionWriteList]):
    _view_id = dm.ViewId("sp_pygen_power", "SensorPosition", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _SENSORPOSITION_PROPERTIES_BY_FIELD
    _class_type = SensorPosition
    _class_list = SensorPositionList
    _class_write_list = SensorPositionWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> SensorPosition | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> SensorPositionList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> SensorPosition | SensorPositionList | None:
        """Retrieve one or more sensor positions by id(s).

        Args:
            external_id: External id or list of external ids of the sensor positions.
            space: The space where all the sensor positions are located.
            retrieve_connections: Whether to retrieve `blade`, `edgewise_bend_mom_crosstalk_corrected`,
            `edgewise_bend_mom_offset`, `edgewise_bend_mom_offset_crosstalk_corrected`, `edgewisewise_bend_mom`,
            `flapwise_bend_mom`, `flapwise_bend_mom_crosstalk_corrected`, `flapwise_bend_mom_offset` and
            `flapwise_bend_mom_offset_crosstalk_corrected` for the sensor positions. Defaults to 'skip'.'skip' will not
            retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full'
            will retrieve the full connected items.

        Returns:
            The requested sensor positions.

        Examples:

            Retrieve sensor_position by id:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> sensor_position = client.sensor_position.retrieve(
                ...     "my_sensor_position"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
        )

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
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results sensor positions matching the query.

        Examples:

           Search for 'my_sensor_position' in all text properties:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> sensor_positions = client.sensor_position.search(
                ...     'my_sensor_position'
                ... )

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
            limit: Maximum number of sensor positions to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

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

    def select(self) -> SensorPositionQuery:
        """Start selecting from sensor positions."""
        return SensorPositionQuery(self._client)

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
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    Blade._view_id,
                    ViewPropertyId(self._view_id, "blade"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "edgewise_bend_mom_crosstalk_corrected"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "edgewise_bend_mom_offset"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "edgewise_bend_mom_offset_crosstalk_corrected"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "edgewisewise_bend_mom"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "flapwise_bend_mom"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "flapwise_bend_mom_crosstalk_corrected"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "flapwise_bend_mom_offset"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    SensorTimeSeries._view_id,
                    ViewPropertyId(self._view_id, "flapwise_bend_mom_offset_crosstalk_corrected"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
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
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[SensorPositionList]:
        """Iterate over sensor positions

        Args:
            chunk_size: The number of sensor positions to return in each iteration. Defaults to 100.
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
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `blade`, `edgewise_bend_mom_crosstalk_corrected`,
            `edgewise_bend_mom_offset`, `edgewise_bend_mom_offset_crosstalk_corrected`, `edgewisewise_bend_mom`,
            `flapwise_bend_mom`, `flapwise_bend_mom_crosstalk_corrected`, `flapwise_bend_mom_offset` and
            `flapwise_bend_mom_offset_crosstalk_corrected` for the sensor positions. Defaults to 'skip'.'skip' will not
            retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full'
            will retrieve the full connected items.
            limit: Maximum number of sensor positions to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of sensor positions

        Examples:

            Iterate sensor positions in chunks of 100 up to 2000 items:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> for sensor_positions in client.sensor_position.iterate(chunk_size=100, limit=2000):
                ...     for sensor_position in sensor_positions:
                ...         print(sensor_position.external_id)

            Iterate sensor positions in chunks of 100 sorted by external_id in descending order:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> for sensor_positions in client.sensor_position.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for sensor_position in sensor_positions:
                ...         print(sensor_position.external_id)

            Iterate sensor positions in chunks of 100 and use cursors to resume the iteration:

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> for first_iteration in client.sensor_position.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for sensor_positions in client.sensor_position.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for sensor_position in sensor_positions:
                ...         print(sensor_position.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
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
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

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
            limit: Maximum number of sensor positions to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `blade`, `edgewise_bend_mom_crosstalk_corrected`,
            `edgewise_bend_mom_offset`, `edgewise_bend_mom_offset_crosstalk_corrected`, `edgewisewise_bend_mom`,
            `flapwise_bend_mom`, `flapwise_bend_mom_crosstalk_corrected`, `flapwise_bend_mom_offset` and
            `flapwise_bend_mom_offset_crosstalk_corrected` for the sensor positions. Defaults to 'skip'.'skip' will not
            retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full'
            will retrieve the full connected items.

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
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
