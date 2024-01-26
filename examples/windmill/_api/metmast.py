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
    Metmast,
    MetmastApply,
    MetmastFields,
    MetmastList,
    MetmastApplyList,
)
from windmill.data_classes._metmast import (
    _METMAST_PROPERTIES_BY_FIELD,
    _create_metmast_filter,
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
from .metmast_temperature import MetmastTemperatureAPI
from .metmast_tilt_angle import MetmastTiltAngleAPI
from .metmast_wind_speed import MetmastWindSpeedAPI
from .metmast_query import MetmastQueryAPI


class MetmastAPI(NodeAPI[Metmast, MetmastApply, MetmastList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[Metmast]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Metmast,
            class_list=MetmastList,
            class_apply_list=MetmastApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.temperature = MetmastTemperatureAPI(client, view_id)
        self.tilt_angle = MetmastTiltAngleAPI(client, view_id)
        self.wind_speed = MetmastWindSpeedAPI(client, view_id)

    def __call__(
        self,
        min_position: float | None = None,
        max_position: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> MetmastQueryAPI[MetmastList]:
        """Query starting at metmasts.

        Args:
            min_position: The minimum value of the position to filter on.
            max_position: The maximum value of the position to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metmasts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for metmasts.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_metmast_filter(
            self._view_id,
            min_position,
            max_position,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(MetmastList)
        return MetmastQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        metmast: MetmastApply | Sequence[MetmastApply],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) metmasts.

        Args:
            metmast: Metmast or sequence of metmasts to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new metmast:

                >>> from windmill import WindmillClient
                >>> from windmill.data_classes import MetmastApply
                >>> client = WindmillClient()
                >>> metmast = MetmastApply(external_id="my_metmast", ...)
                >>> result = client.metmast.apply(metmast)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .apply method on the client instead. This means instead of "
            "`my_client.metmast.apply(my_items)` please use `my_client.apply(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(metmast, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more metmast.

        Args:
            external_id: External id of the metmast to delete.
            space: The space where all the metmast are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete metmast by id:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> client.metmast.delete("my_metmast")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Metmast | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> MetmastList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Metmast | MetmastList | None:
        """Retrieve one or more metmasts by id(s).

        Args:
            external_id: External id or list of external ids of the metmasts.
            space: The space where all the metmasts are located.

        Returns:
            The requested metmasts.

        Examples:

            Retrieve metmast by id:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> metmast = client.metmast.retrieve("my_metmast")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MetmastFields | Sequence[MetmastFields] | None = None,
        group_by: None = None,
        min_position: float | None = None,
        max_position: float | None = None,
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
        property: MetmastFields | Sequence[MetmastFields] | None = None,
        group_by: MetmastFields | Sequence[MetmastFields] = None,
        min_position: float | None = None,
        max_position: float | None = None,
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
        property: MetmastFields | Sequence[MetmastFields] | None = None,
        group_by: MetmastFields | Sequence[MetmastFields] | None = None,
        min_position: float | None = None,
        max_position: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across metmasts

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            min_position: The minimum value of the position to filter on.
            max_position: The maximum value of the position to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metmasts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count metmasts in space `my_space`:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> result = client.metmast.aggregate("count", space="my_space")

        """

        filter_ = _create_metmast_filter(
            self._view_id,
            min_position,
            max_position,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _METMAST_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MetmastFields,
        interval: float,
        min_position: float | None = None,
        max_position: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for metmasts

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            min_position: The minimum value of the position to filter on.
            max_position: The maximum value of the position to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metmasts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_metmast_filter(
            self._view_id,
            min_position,
            max_position,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _METMAST_PROPERTIES_BY_FIELD,
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
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MetmastList:
        """List/filter metmasts

        Args:
            min_position: The minimum value of the position to filter on.
            max_position: The maximum value of the position to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metmasts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested metmasts

        Examples:

            List metmasts and limit to 5:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> metmasts = client.metmast.list(limit=5)

        """
        filter_ = _create_metmast_filter(
            self._view_id,
            min_position,
            max_position,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
