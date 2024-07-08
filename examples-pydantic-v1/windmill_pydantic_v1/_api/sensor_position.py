from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from windmill_pydantic_v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from windmill_pydantic_v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    SensorPosition,
    SensorPositionWrite,
    SensorPositionFields,
    SensorPositionList,
    SensorPositionWriteList,
)
from windmill_pydantic_v1.data_classes._sensor_position import (
    _SENSORPOSITION_PROPERTIES_BY_FIELD,
    _create_sensor_position_filter,
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
from .sensor_position_edgewise_bend_mom_crosstalk_corrected import SensorPositionEdgewiseBendMomCrosstalkCorrectedAPI
from .sensor_position_edgewise_bend_mom_offset import SensorPositionEdgewiseBendMomOffsetAPI
from .sensor_position_edgewise_bend_mom_offset_crosstalk_corrected import (
    SensorPositionEdgewiseBendMomOffsetCrosstalkCorrectedAPI,
)
from .sensor_position_edgewisewise_bend_mom import SensorPositionEdgewisewiseBendMomAPI
from .sensor_position_flapwise_bend_mom import SensorPositionFlapwiseBendMomAPI
from .sensor_position_flapwise_bend_mom_crosstalk_corrected import SensorPositionFlapwiseBendMomCrosstalkCorrectedAPI
from .sensor_position_flapwise_bend_mom_offset import SensorPositionFlapwiseBendMomOffsetAPI
from .sensor_position_flapwise_bend_mom_offset_crosstalk_corrected import (
    SensorPositionFlapwiseBendMomOffsetCrosstalkCorrectedAPI,
)
from .sensor_position_query import SensorPositionQueryAPI


class SensorPositionAPI(NodeAPI[SensorPosition, SensorPositionWrite, SensorPositionList, SensorPositionWriteList]):
    _view_id = dm.ViewId("power-models", "SensorPosition", "1")
    _properties_by_field = _SENSORPOSITION_PROPERTIES_BY_FIELD
    _class_type = SensorPosition
    _class_list = SensorPositionList
    _class_write_list = SensorPositionWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.edgewise_bend_mom_crosstalk_corrected = SensorPositionEdgewiseBendMomCrosstalkCorrectedAPI(
            client, self._view_id
        )
        self.edgewise_bend_mom_offset = SensorPositionEdgewiseBendMomOffsetAPI(client, self._view_id)
        self.edgewise_bend_mom_offset_crosstalk_corrected = SensorPositionEdgewiseBendMomOffsetCrosstalkCorrectedAPI(
            client, self._view_id
        )
        self.edgewisewise_bend_mom = SensorPositionEdgewisewiseBendMomAPI(client, self._view_id)
        self.flapwise_bend_mom = SensorPositionFlapwiseBendMomAPI(client, self._view_id)
        self.flapwise_bend_mom_crosstalk_corrected = SensorPositionFlapwiseBendMomCrosstalkCorrectedAPI(
            client, self._view_id
        )
        self.flapwise_bend_mom_offset = SensorPositionFlapwiseBendMomOffsetAPI(client, self._view_id)
        self.flapwise_bend_mom_offset_crosstalk_corrected = SensorPositionFlapwiseBendMomOffsetCrosstalkCorrectedAPI(
            client, self._view_id
        )

    def __call__(
        self,
        min_position: float | None = None,
        max_position: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> SensorPositionQueryAPI[SensorPositionList]:
        """Query starting at sensor positions.

        Args:
            min_position: The minimum value of the position to filter on.
            max_position: The maximum value of the position to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor positions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for sensor positions.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_sensor_position_filter(
            self._view_id,
            min_position,
            max_position,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(SensorPositionList)
        return SensorPositionQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        sensor_position: SensorPositionWrite | Sequence[SensorPositionWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) sensor positions.

        Args:
            sensor_position: Sensor position or sequence of sensor positions to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new sensor_position:

                >>> from windmill_pydantic_v1 import WindmillClient
                >>> from windmill_pydantic_v1.data_classes import SensorPositionWrite
                >>> client = WindmillClient()
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

                >>> from windmill_pydantic_v1 import WindmillClient
                >>> client = WindmillClient()
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
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> SensorPosition | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> SensorPositionList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SensorPosition | SensorPositionList | None:
        """Retrieve one or more sensor positions by id(s).

        Args:
            external_id: External id or list of external ids of the sensor positions.
            space: The space where all the sensor positions are located.

        Returns:
            The requested sensor positions.

        Examples:

            Retrieve sensor_position by id:

                >>> from windmill_pydantic_v1 import WindmillClient
                >>> client = WindmillClient()
                >>> sensor_position = client.sensor_position.retrieve("my_sensor_position")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: SensorPositionFields | SequenceNotStr[SensorPositionFields] | None = None,
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
            min_position: The minimum value of the position to filter on.
            max_position: The maximum value of the position to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor positions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count sensor positions in space `my_space`:

                >>> from windmill_pydantic_v1 import WindmillClient
                >>> client = WindmillClient()
                >>> result = client.sensor_position.aggregate("count", space="my_space")

        """

        filter_ = _create_sensor_position_filter(
            self._view_id,
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

    def list(
        self,
        min_position: float | None = None,
        max_position: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: SensorPositionFields | Sequence[SensorPositionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> SensorPositionList:
        """List/filter sensor positions

        Args:
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

        Returns:
            List of requested sensor positions

        Examples:

            List sensor positions and limit to 5:

                >>> from windmill_pydantic_v1 import WindmillClient
                >>> client = WindmillClient()
                >>> sensor_positions = client.sensor_position.list(limit=5)

        """
        filter_ = _create_sensor_position_filter(
            self._view_id,
            min_position,
            max_position,
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
