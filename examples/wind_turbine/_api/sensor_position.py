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
    QueryStepFactory,
    QueryBuilder,
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
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        return QueryUnpacker(builder, edges=unpack_edges).unpack()

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
        values = self._query(filter_, limit, retrieve_connections, sort_input)
        return self._class_list(instantiate_classes(self._class_type, values, "list"))
