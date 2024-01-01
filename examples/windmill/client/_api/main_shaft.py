from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from windmill.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from windmill.client.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    MainShaft,
    MainShaftApply,
    MainShaftFields,
    MainShaftList,
    MainShaftApplyList,
)
from windmill.client.data_classes._main_shaft import (
    _MAINSHAFT_PROPERTIES_BY_FIELD,
    _create_main_shaft_filter,
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
from .main_shaft_bending_x import MainShaftBendingXAPI
from .main_shaft_bending_y import MainShaftBendingYAPI
from .main_shaft_calculated_tilt_moment import MainShaftCalculatedTiltMomentAPI
from .main_shaft_calculated_yaw_moment import MainShaftCalculatedYawMomentAPI
from .main_shaft_torque import MainShaftTorqueAPI
from .main_shaft_query import MainShaftQueryAPI


class MainShaftAPI(NodeAPI[MainShaft, MainShaftApply, MainShaftList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[MainShaft]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=MainShaft,
            class_list=MainShaftList,
            class_apply_list=MainShaftApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.bending_x = MainShaftBendingXAPI(client, view_id)
        self.bending_y = MainShaftBendingYAPI(client, view_id)
        self.calculated_tilt_moment = MainShaftCalculatedTiltMomentAPI(client, view_id)
        self.calculated_yaw_moment = MainShaftCalculatedYawMomentAPI(client, view_id)
        self.torque = MainShaftTorqueAPI(client, view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> MainShaftQueryAPI[MainShaftList]:
        """Query starting at main shafts.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main shafts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for main shafts.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_main_shaft_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(MainShaftList)
        return MainShaftQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        main_shaft: MainShaftApply | Sequence[MainShaftApply],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) main shafts.

        Args:
            main_shaft: Main shaft or sequence of main shafts to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): Should we write None values to the API? If False, None values will be ignored. If True, None values will be written to the API.
                Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new main_shaft:

                >>> from windmill.client import WindmillClient
                >>> from windmill.client.data_classes import MainShaftApply
                >>> client = WindmillClient()
                >>> main_shaft = MainShaftApply(external_id="my_main_shaft", ...)
                >>> result = client.main_shaft.apply(main_shaft)

        """
        return self._apply(main_shaft, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more main shaft.

        Args:
            external_id: External id of the main shaft to delete.
            space: The space where all the main shaft are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete main_shaft by id:

                >>> from windmill.client import WindmillClient
                >>> client = WindmillClient()
                >>> client.main_shaft.delete("my_main_shaft")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> MainShaft | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> MainShaftList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> MainShaft | MainShaftList | None:
        """Retrieve one or more main shafts by id(s).

        Args:
            external_id: External id or list of external ids of the main shafts.
            space: The space where all the main shafts are located.

        Returns:
            The requested main shafts.

        Examples:

            Retrieve main_shaft by id:

                >>> from windmill.client import WindmillClient
                >>> client = WindmillClient()
                >>> main_shaft = client.main_shaft.retrieve("my_main_shaft")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MainShaftFields | Sequence[MainShaftFields] | None = None,
        group_by: None = None,
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
        property: MainShaftFields | Sequence[MainShaftFields] | None = None,
        group_by: MainShaftFields | Sequence[MainShaftFields] = None,
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
        property: MainShaftFields | Sequence[MainShaftFields] | None = None,
        group_by: MainShaftFields | Sequence[MainShaftFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across main shafts

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main shafts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count main shafts in space `my_space`:

                >>> from windmill.client import WindmillClient
                >>> client = WindmillClient()
                >>> result = client.main_shaft.aggregate("count", space="my_space")

        """

        filter_ = _create_main_shaft_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _MAINSHAFT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MainShaftFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for main shafts

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main shafts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_main_shaft_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _MAINSHAFT_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MainShaftList:
        """List/filter main shafts

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main shafts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested main shafts

        Examples:

            List main shafts and limit to 5:

                >>> from windmill.client import WindmillClient
                >>> client = WindmillClient()
                >>> main_shafts = client.main_shaft.list(limit=5)

        """
        filter_ = _create_main_shaft_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
