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
    Gearbox,
    GearboxWrite,
    GearboxFields,
    GearboxList,
    GearboxWriteList,
)
from windmill.data_classes._gearbox import (
    _GEARBOX_PROPERTIES_BY_FIELD,
    _create_gearbox_filter,
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
from .gearbox_displacement_x import GearboxDisplacementXAPI
from .gearbox_displacement_y import GearboxDisplacementYAPI
from .gearbox_displacement_z import GearboxDisplacementZAPI
from .gearbox_query import GearboxQueryAPI


class GearboxAPI(NodeAPI[Gearbox, GearboxWrite, GearboxList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[Gearbox]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Gearbox,
            class_list=GearboxList,
            class_write_list=GearboxWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.displacement_x = GearboxDisplacementXAPI(client, view_id)
        self.displacement_y = GearboxDisplacementYAPI(client, view_id)
        self.displacement_z = GearboxDisplacementZAPI(client, view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> GearboxQueryAPI[GearboxList]:
        """Query starting at gearboxes.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of gearboxes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for gearboxes.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_gearbox_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(GearboxList)
        return GearboxQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        gearbox: GearboxWrite | Sequence[GearboxWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) gearboxes.

        Args:
            gearbox: Gearbox or sequence of gearboxes to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new gearbox:

                >>> from windmill import WindmillClient
                >>> from windmill.data_classes import GearboxWrite
                >>> client = WindmillClient()
                >>> gearbox = GearboxWrite(external_id="my_gearbox", ...)
                >>> result = client.gearbox.apply(gearbox)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.gearbox.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(gearbox, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more gearbox.

        Args:
            external_id: External id of the gearbox to delete.
            space: The space where all the gearbox are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete gearbox by id:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> client.gearbox.delete("my_gearbox")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.gearbox.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Gearbox | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> GearboxList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Gearbox | GearboxList | None:
        """Retrieve one or more gearboxes by id(s).

        Args:
            external_id: External id or list of external ids of the gearboxes.
            space: The space where all the gearboxes are located.

        Returns:
            The requested gearboxes.

        Examples:

            Retrieve gearbox by id:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> gearbox = client.gearbox.retrieve("my_gearbox")

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
        property: GearboxFields | Sequence[GearboxFields] | None = None,
        group_by: None = None,
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
        property: GearboxFields | Sequence[GearboxFields] | None = None,
        group_by: GearboxFields | Sequence[GearboxFields] = None,
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
        property: GearboxFields | Sequence[GearboxFields] | None = None,
        group_by: GearboxFields | Sequence[GearboxFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across gearboxes

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of gearboxes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count gearboxes in space `my_space`:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> result = client.gearbox.aggregate("count", space="my_space")

        """

        filter_ = _create_gearbox_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _GEARBOX_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: GearboxFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for gearboxes

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of gearboxes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_gearbox_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _GEARBOX_PROPERTIES_BY_FIELD,
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
        sort_by: GearboxFields | Sequence[GearboxFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> GearboxList:
        """List/filter gearboxes

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of gearboxes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested gearboxes

        Examples:

            List gearboxes and limit to 5:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> gearboxes = client.gearbox.list(limit=5)

        """
        filter_ = _create_gearbox_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(
            limit=limit,
            filter=filter_,
            properties_by_field=_GEARBOX_PROPERTIES_BY_FIELD,
            sort_by=sort_by,
            direction=direction,
            sort=sort,
        )
