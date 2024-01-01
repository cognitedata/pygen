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
    Nacelle,
    NacelleApply,
    NacelleFields,
    NacelleList,
    NacelleApplyList,
)
from windmill.client.data_classes._nacelle import (
    _NACELLE_PROPERTIES_BY_FIELD,
    _create_nacelle_filter,
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
from .nacelle_acc_from_back_side_x import NacelleAccFromBackSideXAPI
from .nacelle_acc_from_back_side_y import NacelleAccFromBackSideYAPI
from .nacelle_acc_from_back_side_z import NacelleAccFromBackSideZAPI
from .nacelle_yaw_direction import NacelleYawDirectionAPI
from .nacelle_yaw_error import NacelleYawErrorAPI
from .nacelle_query import NacelleQueryAPI


class NacelleAPI(NodeAPI[Nacelle, NacelleApply, NacelleList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[Nacelle]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Nacelle,
            class_list=NacelleList,
            class_apply_list=NacelleApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.acc_from_back_side_x = NacelleAccFromBackSideXAPI(client, view_id)
        self.acc_from_back_side_y = NacelleAccFromBackSideYAPI(client, view_id)
        self.acc_from_back_side_z = NacelleAccFromBackSideZAPI(client, view_id)
        self.yaw_direction = NacelleYawDirectionAPI(client, view_id)
        self.yaw_error = NacelleYawErrorAPI(client, view_id)

    def __call__(
        self,
        gearbox: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        generator: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        high_speed_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        main_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_inverter: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> NacelleQueryAPI[NacelleList]:
        """Query starting at nacelles.

        Args:
            gearbox: The gearbox to filter on.
            generator: The generator to filter on.
            high_speed_shaft: The high speed shaft to filter on.
            main_shaft: The main shaft to filter on.
            power_inverter: The power inverter to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of nacelles to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for nacelles.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_nacelle_filter(
            self._view_id,
            gearbox,
            generator,
            high_speed_shaft,
            main_shaft,
            power_inverter,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(NacelleList)
        return NacelleQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        nacelle: NacelleApply | Sequence[NacelleApply],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) nacelles.

        Args:
            nacelle: Nacelle or sequence of nacelles to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new nacelle:

                >>> from windmill.client import WindmillClient
                >>> from windmill.client.data_classes import NacelleApply
                >>> client = WindmillClient()
                >>> nacelle = NacelleApply(external_id="my_nacelle", ...)
                >>> result = client.nacelle.apply(nacelle)

        """
        return self._apply(nacelle, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more nacelle.

        Args:
            external_id: External id of the nacelle to delete.
            space: The space where all the nacelle are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete nacelle by id:

                >>> from windmill.client import WindmillClient
                >>> client = WindmillClient()
                >>> client.nacelle.delete("my_nacelle")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Nacelle | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> NacelleList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Nacelle | NacelleList | None:
        """Retrieve one or more nacelles by id(s).

        Args:
            external_id: External id or list of external ids of the nacelles.
            space: The space where all the nacelles are located.

        Returns:
            The requested nacelles.

        Examples:

            Retrieve nacelle by id:

                >>> from windmill.client import WindmillClient
                >>> client = WindmillClient()
                >>> nacelle = client.nacelle.retrieve("my_nacelle")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: NacelleFields | Sequence[NacelleFields] | None = None,
        group_by: None = None,
        gearbox: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        generator: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        high_speed_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        main_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_inverter: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: NacelleFields | Sequence[NacelleFields] | None = None,
        group_by: NacelleFields | Sequence[NacelleFields] = None,
        gearbox: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        generator: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        high_speed_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        main_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_inverter: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: NacelleFields | Sequence[NacelleFields] | None = None,
        group_by: NacelleFields | Sequence[NacelleFields] | None = None,
        gearbox: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        generator: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        high_speed_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        main_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_inverter: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across nacelles

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            gearbox: The gearbox to filter on.
            generator: The generator to filter on.
            high_speed_shaft: The high speed shaft to filter on.
            main_shaft: The main shaft to filter on.
            power_inverter: The power inverter to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of nacelles to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count nacelles in space `my_space`:

                >>> from windmill.client import WindmillClient
                >>> client = WindmillClient()
                >>> result = client.nacelle.aggregate("count", space="my_space")

        """

        filter_ = _create_nacelle_filter(
            self._view_id,
            gearbox,
            generator,
            high_speed_shaft,
            main_shaft,
            power_inverter,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _NACELLE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: NacelleFields,
        interval: float,
        gearbox: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        generator: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        high_speed_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        main_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_inverter: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for nacelles

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            gearbox: The gearbox to filter on.
            generator: The generator to filter on.
            high_speed_shaft: The high speed shaft to filter on.
            main_shaft: The main shaft to filter on.
            power_inverter: The power inverter to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of nacelles to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_nacelle_filter(
            self._view_id,
            gearbox,
            generator,
            high_speed_shaft,
            main_shaft,
            power_inverter,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _NACELLE_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        gearbox: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        generator: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        high_speed_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        main_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_inverter: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> NacelleList:
        """List/filter nacelles

        Args:
            gearbox: The gearbox to filter on.
            generator: The generator to filter on.
            high_speed_shaft: The high speed shaft to filter on.
            main_shaft: The main shaft to filter on.
            power_inverter: The power inverter to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of nacelles to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested nacelles

        Examples:

            List nacelles and limit to 5:

                >>> from windmill.client import WindmillClient
                >>> client = WindmillClient()
                >>> nacelles = client.nacelle.list(limit=5)

        """
        filter_ = _create_nacelle_filter(
            self._view_id,
            gearbox,
            generator,
            high_speed_shaft,
            main_shaft,
            power_inverter,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
