from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from windmill.data_classes._core import DEFAULT_INSTANCE_SPACE
from windmill.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Nacelle,
    NacelleWrite,
    NacelleFields,
    NacelleList,
    NacelleWriteList,
)
from windmill.data_classes._nacelle import (
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


class NacelleAPI(NodeAPI[Nacelle, NacelleWrite, NacelleList]):
    _view_id = dm.ViewId("power-models", "Nacelle", "1")

    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            class_type=Nacelle,
            class_list=NacelleList,
            class_write_list=NacelleWriteList,
        )
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
        limit: int | None = DEFAULT_QUERY_LIMIT,
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
        return NacelleQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        nacelle: NacelleWrite | Sequence[NacelleWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
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

                >>> from windmill import WindmillClient
                >>> from windmill.data_classes import NacelleWrite
                >>> client = WindmillClient()
                >>> nacelle = NacelleWrite(external_id="my_nacelle", ...)
                >>> result = client.nacelle.apply(nacelle)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.nacelle.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
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

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> client.nacelle.delete("my_nacelle")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.nacelle.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Nacelle | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> NacelleList: ...

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

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> nacelle = client.nacelle.retrieve("my_nacelle")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: NacelleFields | Sequence[NacelleFields] | None = None,
        group_by: None = None,
        gearbox: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        generator: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        high_speed_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        main_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_inverter: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: NacelleFields | Sequence[NacelleFields] | None = None,
        group_by: NacelleFields | Sequence[NacelleFields] = None,
        gearbox: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        generator: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        high_speed_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        main_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_inverter: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: NacelleFields | Sequence[NacelleFields] | None = None,
        group_by: NacelleFields | Sequence[NacelleFields] | None = None,
        gearbox: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        generator: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        high_speed_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        main_shaft: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_inverter: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
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

                >>> from windmill import WindmillClient
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
        limit: int | None = DEFAULT_LIMIT_READ,
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
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: NacelleFields | Sequence[NacelleFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
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
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested nacelles

        Examples:

            List nacelles and limit to 5:

                >>> from windmill import WindmillClient
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
        return self._list(
            limit=limit,
            filter=filter_,
            properties_by_field=_NACELLE_PROPERTIES_BY_FIELD,
            sort_by=sort_by,
            direction=direction,
            sort=sort,
        )
