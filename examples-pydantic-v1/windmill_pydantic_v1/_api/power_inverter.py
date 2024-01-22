from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from windmill_pydantic_v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from windmill_pydantic_v1.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    PowerInverter,
    PowerInverterApply,
    PowerInverterFields,
    PowerInverterList,
    PowerInverterApplyList,
)
from windmill_pydantic_v1.data_classes._power_inverter import (
    _POWERINVERTER_PROPERTIES_BY_FIELD,
    _create_power_inverter_filter,
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
from .power_inverter_active_power_total import PowerInverterActivePowerTotalAPI
from .power_inverter_apparent_power_total import PowerInverterApparentPowerTotalAPI
from .power_inverter_reactive_power_total import PowerInverterReactivePowerTotalAPI
from .power_inverter_query import PowerInverterQueryAPI


class PowerInverterAPI(NodeAPI[PowerInverter, PowerInverterApply, PowerInverterList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[PowerInverter]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PowerInverter,
            class_list=PowerInverterList,
            class_apply_list=PowerInverterApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.active_power_total = PowerInverterActivePowerTotalAPI(client, view_id)
        self.apparent_power_total = PowerInverterApparentPowerTotalAPI(client, view_id)
        self.reactive_power_total = PowerInverterReactivePowerTotalAPI(client, view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PowerInverterQueryAPI[PowerInverterList]:
        """Query starting at power inverters.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of power inverters to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for power inverters.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_power_inverter_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PowerInverterList)
        return PowerInverterQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        power_inverter: PowerInverterApply | Sequence[PowerInverterApply],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) power inverters.

        Args:
            power_inverter: Power inverter or sequence of power inverters to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new power_inverter:

                >>> from windmill_pydantic_v1 import WindmillClient
                >>> from windmill_pydantic_v1.data_classes import PowerInverterApply
                >>> client = WindmillClient()
                >>> power_inverter = PowerInverterApply(external_id="my_power_inverter", ...)
                >>> result = client.power_inverter.apply(power_inverter)

        """
        return self._apply(power_inverter, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more power inverter.

        Args:
            external_id: External id of the power inverter to delete.
            space: The space where all the power inverter are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete power_inverter by id:

                >>> from windmill_pydantic_v1 import WindmillClient
                >>> client = WindmillClient()
                >>> client.power_inverter.delete("my_power_inverter")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PowerInverter | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> PowerInverterList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PowerInverter | PowerInverterList | None:
        """Retrieve one or more power inverters by id(s).

        Args:
            external_id: External id or list of external ids of the power inverters.
            space: The space where all the power inverters are located.

        Returns:
            The requested power inverters.

        Examples:

            Retrieve power_inverter by id:

                >>> from windmill_pydantic_v1 import WindmillClient
                >>> client = WindmillClient()
                >>> power_inverter = client.power_inverter.retrieve("my_power_inverter")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PowerInverterFields | Sequence[PowerInverterFields] | None = None,
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
        property: PowerInverterFields | Sequence[PowerInverterFields] | None = None,
        group_by: PowerInverterFields | Sequence[PowerInverterFields] = None,
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
        property: PowerInverterFields | Sequence[PowerInverterFields] | None = None,
        group_by: PowerInverterFields | Sequence[PowerInverterFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across power inverters

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of power inverters to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count power inverters in space `my_space`:

                >>> from windmill_pydantic_v1 import WindmillClient
                >>> client = WindmillClient()
                >>> result = client.power_inverter.aggregate("count", space="my_space")

        """

        filter_ = _create_power_inverter_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _POWERINVERTER_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PowerInverterFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for power inverters

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of power inverters to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_power_inverter_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _POWERINVERTER_PROPERTIES_BY_FIELD,
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
    ) -> PowerInverterList:
        """List/filter power inverters

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of power inverters to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested power inverters

        Examples:

            List power inverters and limit to 5:

                >>> from windmill_pydantic_v1 import WindmillClient
                >>> client = WindmillClient()
                >>> power_inverters = client.power_inverter.list(limit=5)

        """
        filter_ = _create_power_inverter_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
