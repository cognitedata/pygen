from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from equipment_unit_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    StartEndTime,
    StartEndTimeApply,
    StartEndTimeFields,
    StartEndTimeList,
    StartEndTimeApplyList,
)
from equipment_unit_pydantic_v1.client.data_classes._start_end_time import (
    _STARTENDTIME_PROPERTIES_BY_FIELD,
    _create_start_end_time_filter,
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
from .start_end_time_query import StartEndTimeQueryAPI


class StartEndTimeAPI(NodeAPI[StartEndTime, StartEndTimeApply, StartEndTimeList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[StartEndTimeApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=StartEndTime,
            class_apply_type=StartEndTimeApply,
            class_list=StartEndTimeList,
            class_apply_list=StartEndTimeApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> StartEndTimeQueryAPI[StartEndTimeList]:
        """Query starting at start end times.

        Args:
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of start end times to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for start end times.

        """
        filter_ = _create_start_end_time_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            StartEndTimeList,
            [
                QueryStep(
                    name="start_end_time",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_STARTENDTIME_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=StartEndTime,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return StartEndTimeQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, start_end_time: StartEndTimeApply | Sequence[StartEndTimeApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) start end times.

        Args:
            start_end_time: Start end time or sequence of start end times to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new start_end_time:

                >>> from equipment_unit_pydantic_v1.client import EquipmentUnitClient
                >>> from equipment_unit_pydantic_v1.client.data_classes import StartEndTimeApply
                >>> client = EquipmentUnitClient()
                >>> start_end_time = StartEndTimeApply(external_id="my_start_end_time", ...)
                >>> result = client.start_end_time.apply(start_end_time)

        """
        return self._apply(start_end_time, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more start end time.

        Args:
            external_id: External id of the start end time to delete.
            space: The space where all the start end time are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete start_end_time by id:

                >>> from equipment_unit_pydantic_v1.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> client.start_end_time.delete("my_start_end_time")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> StartEndTime:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> StartEndTimeList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> StartEndTime | StartEndTimeList:
        """Retrieve one or more start end times by id(s).

        Args:
            external_id: External id or list of external ids of the start end times.
            space: The space where all the start end times are located.

        Returns:
            The requested start end times.

        Examples:

            Retrieve start_end_time by id:

                >>> from equipment_unit_pydantic_v1.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> start_end_time = client.start_end_time.retrieve("my_start_end_time")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: StartEndTimeFields | Sequence[StartEndTimeFields] | None = None,
        group_by: None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: StartEndTimeFields | Sequence[StartEndTimeFields] | None = None,
        group_by: StartEndTimeFields | Sequence[StartEndTimeFields] = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: StartEndTimeFields | Sequence[StartEndTimeFields] | None = None,
        group_by: StartEndTimeFields | Sequence[StartEndTimeFields] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across start end times

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of start end times to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count start end times in space `my_space`:

                >>> from equipment_unit_pydantic_v1.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> result = client.start_end_time.aggregate("count", space="my_space")

        """

        filter_ = _create_start_end_time_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _STARTENDTIME_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: StartEndTimeFields,
        interval: float,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for start end times

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of start end times to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_start_end_time_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _STARTENDTIME_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> StartEndTimeList:
        """List/filter start end times

        Args:
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of start end times to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested start end times

        Examples:

            List start end times and limit to 5:

                >>> from equipment_unit_pydantic_v1.client import EquipmentUnitClient
                >>> client = EquipmentUnitClient()
                >>> start_end_times = client.start_end_time.list(limit=5)

        """
        filter_ = _create_start_end_time_filter(
            self._view_id,
            min_end_time,
            max_end_time,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
