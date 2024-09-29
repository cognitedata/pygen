from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from windmill.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    QueryBuilder,
)
from windmill.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    MainShaft,
    MainShaftWrite,
    MainShaftFields,
    MainShaftList,
    MainShaftWriteList,
    MainShaftTextFields,
)
from windmill.data_classes._main_shaft import (
    MainShaftQuery,
    _MAINSHAFT_PROPERTIES_BY_FIELD,
    _create_main_shaft_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from .main_shaft_bending_x import MainShaftBendingXAPI
from .main_shaft_bending_y import MainShaftBendingYAPI
from .main_shaft_calculated_tilt_moment import MainShaftCalculatedTiltMomentAPI
from .main_shaft_calculated_yaw_moment import MainShaftCalculatedYawMomentAPI
from .main_shaft_torque import MainShaftTorqueAPI
from .main_shaft_query import MainShaftQueryAPI


class MainShaftAPI(NodeAPI[MainShaft, MainShaftWrite, MainShaftList, MainShaftWriteList]):
    _view_id = dm.ViewId("power-models", "MainShaft", "1")
    _properties_by_field = _MAINSHAFT_PROPERTIES_BY_FIELD
    _class_type = MainShaft
    _class_list = MainShaftList
    _class_write_list = MainShaftWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.bending_x = MainShaftBendingXAPI(client, self._view_id)
        self.bending_y = MainShaftBendingYAPI(client, self._view_id)
        self.calculated_tilt_moment = MainShaftCalculatedTiltMomentAPI(client, self._view_id)
        self.calculated_yaw_moment = MainShaftCalculatedYawMomentAPI(client, self._view_id)
        self.torque = MainShaftTorqueAPI(client, self._view_id)

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
        return MainShaftQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        main_shaft: MainShaftWrite | Sequence[MainShaftWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) main shafts.

        Args:
            main_shaft: Main shaft or sequence of main shafts to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new main_shaft:

                >>> from windmill import WindmillClient
                >>> from windmill.data_classes import MainShaftWrite
                >>> client = WindmillClient()
                >>> main_shaft = MainShaftWrite(external_id="my_main_shaft", ...)
                >>> result = client.main_shaft.apply(main_shaft)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.main_shaft.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
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

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> client.main_shaft.delete("my_main_shaft")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.main_shaft.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> MainShaft | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> MainShaftList: ...

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

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> main_shaft = client.main_shaft.retrieve("my_main_shaft")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: MainShaftTextFields | SequenceNotStr[MainShaftTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: MainShaftFields | SequenceNotStr[MainShaftFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> MainShaftList:
        """Search main shafts

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main shafts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results main shafts matching the query.

        Examples:

           Search for 'my_main_shaft' in all text properties:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> main_shafts = client.main_shaft.search('my_main_shaft')

        """
        filter_ = _create_main_shaft_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: MainShaftFields | SequenceNotStr[MainShaftFields] | None = None,
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
        property: MainShaftFields | SequenceNotStr[MainShaftFields] | None = None,
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
        group_by: MainShaftFields | SequenceNotStr[MainShaftFields],
        property: MainShaftFields | SequenceNotStr[MainShaftFields] | None = None,
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
        group_by: MainShaftFields | SequenceNotStr[MainShaftFields] | None = None,
        property: MainShaftFields | SequenceNotStr[MainShaftFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across main shafts

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main shafts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count main shafts in space `my_space`:

                >>> from windmill import WindmillClient
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
            property,
            interval,
            None,
            None,
            limit,
            filter_,
        )

    def query(self) -> MainShaftQuery:
        """Start a query for main shafts."""
        warnings.warn("The .query is in alpha and is subject to breaking changes without notice.")
        return MainShaftQuery(self._client)

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: MainShaftFields | Sequence[MainShaftFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> MainShaftList:
        """List/filter main shafts

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of main shafts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested main shafts

        Examples:

            List main shafts and limit to 5:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> main_shafts = client.main_shaft.list(limit=5)

        """
        filter_ = _create_main_shaft_filter(
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
