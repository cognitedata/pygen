from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from windmill.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from windmill.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Rotor,
    RotorApply,
    RotorFields,
    RotorList,
    RotorApplyList,
)
from windmill.client.data_classes._rotor import (
    _ROTOR_PROPERTIES_BY_FIELD,
    _create_rotor_filter,
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
from .rotor_rotor_speed_controller import RotorRotorSpeedControllerAPI
from .rotor_rpm_low_speed_shaft import RotorRpmLowSpeedShaftAPI
from .rotor_query import RotorQueryAPI


class RotorAPI(NodeAPI[Rotor, RotorApply, RotorList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[RotorApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Rotor,
            class_apply_type=RotorApply,
            class_list=RotorList,
            class_apply_list=RotorApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.rotor_speed_controller = RotorRotorSpeedControllerAPI(client, view_id)
        self.rpm_low_speed_shaft = RotorRpmLowSpeedShaftAPI(client, view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> RotorQueryAPI[RotorList]:
        """Query starting at rotors.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of rotors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for rotors.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_rotor_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(RotorList)
        return RotorQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(self, rotor: RotorApply | Sequence[RotorApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) rotors.

        Args:
            rotor: Rotor or sequence of rotors to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new rotor:

                >>> from windmill.client import WindmillClient
                >>> from windmill.client.data_classes import RotorApply
                >>> client = WindmillClient()
                >>> rotor = RotorApply(external_id="my_rotor", ...)
                >>> result = client.rotor.apply(rotor)

        """
        return self._apply(rotor, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more rotor.

        Args:
            external_id: External id of the rotor to delete.
            space: The space where all the rotor are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete rotor by id:

                >>> from windmill.client import WindmillClient
                >>> client = WindmillClient()
                >>> client.rotor.delete("my_rotor")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Rotor | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> RotorList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Rotor | RotorList | None:
        """Retrieve one or more rotors by id(s).

        Args:
            external_id: External id or list of external ids of the rotors.
            space: The space where all the rotors are located.

        Returns:
            The requested rotors.

        Examples:

            Retrieve rotor by id:

                >>> from windmill.client import WindmillClient
                >>> client = WindmillClient()
                >>> rotor = client.rotor.retrieve("my_rotor")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: RotorFields | Sequence[RotorFields] | None = None,
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
        property: RotorFields | Sequence[RotorFields] | None = None,
        group_by: RotorFields | Sequence[RotorFields] = None,
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
        property: RotorFields | Sequence[RotorFields] | None = None,
        group_by: RotorFields | Sequence[RotorFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across rotors

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of rotors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count rotors in space `my_space`:

                >>> from windmill.client import WindmillClient
                >>> client = WindmillClient()
                >>> result = client.rotor.aggregate("count", space="my_space")

        """

        filter_ = _create_rotor_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ROTOR_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: RotorFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for rotors

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of rotors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_rotor_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ROTOR_PROPERTIES_BY_FIELD,
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
    ) -> RotorList:
        """List/filter rotors

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of rotors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested rotors

        Examples:

            List rotors and limit to 5:

                >>> from windmill.client import WindmillClient
                >>> client = WindmillClient()
                >>> rotors = client.rotor.list(limit=5)

        """
        filter_ = _create_rotor_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
