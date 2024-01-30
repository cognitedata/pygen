from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from windmill.data_classes._core import DEFAULT_INSTANCE_SPACE
from windmill.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    HighSpeedShaft,
    HighSpeedShaftApply,
    HighSpeedShaftFields,
    HighSpeedShaftList,
    HighSpeedShaftApplyList,
)
from windmill.data_classes._high_speed_shaft import (
    _HIGHSPEEDSHAFT_PROPERTIES_BY_FIELD,
    _create_high_speed_shaft_filter,
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
from .high_speed_shaft_bending_moment_y import HighSpeedShaftBendingMomentYAPI
from .high_speed_shaft_bending_monent_x import HighSpeedShaftBendingMonentXAPI
from .high_speed_shaft_torque import HighSpeedShaftTorqueAPI
from .high_speed_shaft_query import HighSpeedShaftQueryAPI


class HighSpeedShaftAPI(NodeAPI[HighSpeedShaft, HighSpeedShaftApply, HighSpeedShaftList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[HighSpeedShaft]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=HighSpeedShaft,
            class_list=HighSpeedShaftList,
            class_apply_list=HighSpeedShaftApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.bending_moment_y = HighSpeedShaftBendingMomentYAPI(client, view_id)
        self.bending_monent_x = HighSpeedShaftBendingMonentXAPI(client, view_id)
        self.torque = HighSpeedShaftTorqueAPI(client, view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> HighSpeedShaftQueryAPI[HighSpeedShaftList]:
        """Query starting at high speed shafts.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of high speed shafts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for high speed shafts.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_high_speed_shaft_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(HighSpeedShaftList)
        return HighSpeedShaftQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        high_speed_shaft: HighSpeedShaftApply | Sequence[HighSpeedShaftApply],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) high speed shafts.

        Args:
            high_speed_shaft: High speed shaft or sequence of high speed shafts to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new high_speed_shaft:

                >>> from windmill import WindmillClient
                >>> from windmill.data_classes import HighSpeedShaftApply
                >>> client = WindmillClient()
                >>> high_speed_shaft = HighSpeedShaftApply(external_id="my_high_speed_shaft", ...)
                >>> result = client.high_speed_shaft.apply(high_speed_shaft)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .apply method on the client instead. This means instead of "
            "`my_client.high_speed_shaft.apply(my_items)` please use `my_client.apply(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(high_speed_shaft, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more high speed shaft.

        Args:
            external_id: External id of the high speed shaft to delete.
            space: The space where all the high speed shaft are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete high_speed_shaft by id:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> client.high_speed_shaft.delete("my_high_speed_shaft")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.high_speed_shaft.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> HighSpeedShaft | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> HighSpeedShaftList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> HighSpeedShaft | HighSpeedShaftList | None:
        """Retrieve one or more high speed shafts by id(s).

        Args:
            external_id: External id or list of external ids of the high speed shafts.
            space: The space where all the high speed shafts are located.

        Returns:
            The requested high speed shafts.

        Examples:

            Retrieve high_speed_shaft by id:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> high_speed_shaft = client.high_speed_shaft.retrieve("my_high_speed_shaft")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: HighSpeedShaftFields | Sequence[HighSpeedShaftFields] | None = None,
        group_by: None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
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
        property: HighSpeedShaftFields | Sequence[HighSpeedShaftFields] | None = None,
        group_by: HighSpeedShaftFields | Sequence[HighSpeedShaftFields] = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: HighSpeedShaftFields | Sequence[HighSpeedShaftFields] | None = None,
        group_by: HighSpeedShaftFields | Sequence[HighSpeedShaftFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across high speed shafts

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of high speed shafts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count high speed shafts in space `my_space`:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> result = client.high_speed_shaft.aggregate("count", space="my_space")

        """

        filter_ = _create_high_speed_shaft_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _HIGHSPEEDSHAFT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: HighSpeedShaftFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for high speed shafts

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of high speed shafts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_high_speed_shaft_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _HIGHSPEEDSHAFT_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> HighSpeedShaftList:
        """List/filter high speed shafts

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of high speed shafts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested high speed shafts

        Examples:

            List high speed shafts and limit to 5:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> high_speed_shafts = client.high_speed_shaft.list(limit=5)

        """
        filter_ = _create_high_speed_shaft_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
