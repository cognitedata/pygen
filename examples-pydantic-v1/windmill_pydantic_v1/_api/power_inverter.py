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
    PowerInverter,
    PowerInverterWrite,
    PowerInverterFields,
    PowerInverterList,
    PowerInverterWriteList,
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
    NodeQueryStep,
    EdgeQueryStep,
    QueryBuilder,
)
from .power_inverter_active_power_total import PowerInverterActivePowerTotalAPI
from .power_inverter_apparent_power_total import PowerInverterApparentPowerTotalAPI
from .power_inverter_reactive_power_total import PowerInverterReactivePowerTotalAPI
from .power_inverter_query import PowerInverterQueryAPI


class PowerInverterAPI(NodeAPI[PowerInverter, PowerInverterWrite, PowerInverterList, PowerInverterWriteList]):
    _view_id = dm.ViewId("power-models", "PowerInverter", "1")
    _properties_by_field = _POWERINVERTER_PROPERTIES_BY_FIELD
    _class_type = PowerInverter
    _class_list = PowerInverterList
    _class_write_list = PowerInverterWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.active_power_total = PowerInverterActivePowerTotalAPI(client, self._view_id)
        self.apparent_power_total = PowerInverterApparentPowerTotalAPI(client, self._view_id)
        self.reactive_power_total = PowerInverterReactivePowerTotalAPI(client, self._view_id)

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
        return PowerInverterQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        power_inverter: PowerInverterWrite | Sequence[PowerInverterWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
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
                >>> from windmill_pydantic_v1.data_classes import PowerInverterWrite
                >>> client = WindmillClient()
                >>> power_inverter = PowerInverterWrite(external_id="my_power_inverter", ...)
                >>> result = client.power_inverter.apply(power_inverter)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.power_inverter.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
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
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.power_inverter.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PowerInverter | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> PowerInverterList: ...

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
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: PowerInverterFields | SequenceNotStr[PowerInverterFields] | None = None,
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
        property: PowerInverterFields | SequenceNotStr[PowerInverterFields] | None = None,
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
        group_by: PowerInverterFields | SequenceNotStr[PowerInverterFields],
        property: PowerInverterFields | SequenceNotStr[PowerInverterFields] | None = None,
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
        group_by: PowerInverterFields | SequenceNotStr[PowerInverterFields] | None = None,
        property: PowerInverterFields | SequenceNotStr[PowerInverterFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across power inverters

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
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
            property,
            interval,
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
        sort_by: PowerInverterFields | Sequence[PowerInverterFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PowerInverterList:
        """List/filter power inverters

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of power inverters to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

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

        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )
