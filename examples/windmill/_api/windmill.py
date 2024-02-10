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
    Windmill,
    WindmillApply,
    WindmillFields,
    WindmillList,
    WindmillApplyList,
    WindmillTextFields,
)
from windmill.data_classes._windmill import (
    _WINDMILL_PROPERTIES_BY_FIELD,
    _create_windmill_filter,
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
from .windmill_blades import WindmillBladesAPI
from .windmill_metmast import WindmillMetmastAPI
from .windmill_query import WindmillQueryAPI


class WindmillAPI(NodeAPI[Windmill, WindmillApply, WindmillList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[Windmill]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Windmill,
            class_list=WindmillList,
            class_apply_list=WindmillApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.blades_edge = WindmillBladesAPI(client)
        self.metmast_edge = WindmillMetmastAPI(client)

    def __call__(
        self,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> WindmillQueryAPI[WindmillList]:
        """Query starting at windmills.

        Args:
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            nacelle: The nacelle to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            rotor: The rotor to filter on.
            windfarm: The windfarm to filter on.
            windfarm_prefix: The prefix of the windfarm to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of windmills to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for windmills.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_windmill_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            nacelle,
            name,
            name_prefix,
            rotor,
            windfarm,
            windfarm_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(WindmillList)
        return WindmillQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        windmill: WindmillApply | Sequence[WindmillApply],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) windmills.

        Note: This method iterates through all nodes and timeseries linked to windmill and creates them including the edges
        between the nodes. For example, if any of `blades` or `metmast` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            windmill: Windmill or sequence of windmills to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new windmill:

                >>> from windmill import WindmillClient
                >>> from windmill.data_classes import WindmillApply
                >>> client = WindmillClient()
                >>> windmill = WindmillApply(external_id="my_windmill", ...)
                >>> result = client.windmill.apply(windmill)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.windmill.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(windmill, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more windmill.

        Args:
            external_id: External id of the windmill to delete.
            space: The space where all the windmill are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete windmill by id:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> client.windmill.delete("my_windmill")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.windmill.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Windmill | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> WindmillList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Windmill | WindmillList | None:
        """Retrieve one or more windmills by id(s).

        Args:
            external_id: External id or list of external ids of the windmills.
            space: The space where all the windmills are located.

        Returns:
            The requested windmills.

        Examples:

            Retrieve windmill by id:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> windmill = client.windmill.retrieve("my_windmill")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_quad=[
                (
                    self.blades_edge,
                    "blades",
                    dm.DirectRelationReference("power-models", "Windmill.blades"),
                    "outwards",
                ),
                (
                    self.metmast_edge,
                    "metmast",
                    dm.DirectRelationReference("power-models", "Windmill.metmast"),
                    "outwards",
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: WindmillTextFields | Sequence[WindmillTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WindmillList:
        """Search windmills

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            nacelle: The nacelle to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            rotor: The rotor to filter on.
            windfarm: The windfarm to filter on.
            windfarm_prefix: The prefix of the windfarm to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of windmills to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results windmills matching the query.

        Examples:

           Search for 'my_windmill' in all text properties:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> windmills = client.windmill.search('my_windmill')

        """
        filter_ = _create_windmill_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            nacelle,
            name,
            name_prefix,
            rotor,
            windfarm,
            windfarm_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WINDMILL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: WindmillFields | Sequence[WindmillFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WindmillTextFields | Sequence[WindmillTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
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
        property: WindmillFields | Sequence[WindmillFields] | None = None,
        group_by: WindmillFields | Sequence[WindmillFields] = None,
        query: str | None = None,
        search_properties: WindmillTextFields | Sequence[WindmillTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
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
        property: WindmillFields | Sequence[WindmillFields] | None = None,
        group_by: WindmillFields | Sequence[WindmillFields] | None = None,
        query: str | None = None,
        search_property: WindmillTextFields | Sequence[WindmillTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across windmills

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            nacelle: The nacelle to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            rotor: The rotor to filter on.
            windfarm: The windfarm to filter on.
            windfarm_prefix: The prefix of the windfarm to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of windmills to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count windmills in space `my_space`:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> result = client.windmill.aggregate("count", space="my_space")

        """

        filter_ = _create_windmill_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            nacelle,
            name,
            name_prefix,
            rotor,
            windfarm,
            windfarm_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WINDMILL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WindmillFields,
        interval: float,
        query: str | None = None,
        search_property: WindmillTextFields | Sequence[WindmillTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for windmills

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            nacelle: The nacelle to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            rotor: The rotor to filter on.
            windfarm: The windfarm to filter on.
            windfarm_prefix: The prefix of the windfarm to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of windmills to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_windmill_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            nacelle,
            name,
            name_prefix,
            rotor,
            windfarm,
            windfarm_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WINDMILL_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WindmillList:
        """List/filter windmills

        Args:
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            nacelle: The nacelle to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            rotor: The rotor to filter on.
            windfarm: The windfarm to filter on.
            windfarm_prefix: The prefix of the windfarm to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of windmills to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `blades` or `metmast` external ids for the windmills. Defaults to True.

        Returns:
            List of requested windmills

        Examples:

            List windmills and limit to 5:

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> windmills = client.windmill.list(limit=5)

        """
        filter_ = _create_windmill_filter(
            self._view_id,
            min_capacity,
            max_capacity,
            nacelle,
            name,
            name_prefix,
            rotor,
            windfarm,
            windfarm_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_quad=[
                (
                    self.blades_edge,
                    "blades",
                    dm.DirectRelationReference("power-models", "Windmill.blades"),
                    "outwards",
                ),
                (
                    self.metmast_edge,
                    "metmast",
                    dm.DirectRelationReference("power-models", "Windmill.metmast"),
                    "outwards",
                ),
            ],
        )
