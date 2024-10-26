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
    Windmill,
    WindmillWrite,
    WindmillFields,
    WindmillList,
    WindmillWriteList,
    WindmillTextFields,
    Blade,
    Metmast,
    Nacelle,
    Rotor,
)
from windmill.data_classes._windmill import (
    WindmillQuery,
    _WINDMILL_PROPERTIES_BY_FIELD,
    _create_windmill_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from .windmill_blades import WindmillBladesAPI
from .windmill_metmast import WindmillMetmastAPI
from .windmill_query import WindmillQueryAPI


class WindmillAPI(NodeAPI[Windmill, WindmillWrite, WindmillList, WindmillWriteList]):
    _view_id = dm.ViewId("power-models", "Windmill", "1")
    _properties_by_field = _WINDMILL_PROPERTIES_BY_FIELD
    _class_type = Windmill
    _class_list = WindmillList
    _class_write_list = WindmillWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.blades_edge = WindmillBladesAPI(client)
        self.metmast_edge = WindmillMetmastAPI(client)

    def __call__(
        self,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
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
        return WindmillQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        windmill: WindmillWrite | Sequence[WindmillWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) windmills.

        Note: This method iterates through all nodes and timeseries linked to windmill and creates them including the edges
        between the nodes. For example, if any of `blades`, `metmast`, `nacelle` or `rotor` are set, then these
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
                >>> from windmill.data_classes import WindmillWrite
                >>> client = WindmillClient()
                >>> windmill = WindmillWrite(external_id="my_windmill", ...)
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
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Windmill | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> WindmillList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
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
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.blades_edge,
                    "blades",
                    dm.DirectRelationReference("power-models", "Windmill.blades"),
                    "outwards",
                    dm.ViewId("power-models", "Blade", "1"),
                ),
                (
                    self.metmast_edge,
                    "metmast",
                    dm.DirectRelationReference("power-models", "Windmill.metmast"),
                    "outwards",
                    dm.ViewId("power-models", "Metmast", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: WindmillTextFields | SequenceNotStr[WindmillTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: WindmillFields | SequenceNotStr[WindmillFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
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
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

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
        property: WindmillFields | SequenceNotStr[WindmillFields] | None = None,
        query: str | None = None,
        search_property: WindmillTextFields | SequenceNotStr[WindmillTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
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
        property: WindmillFields | SequenceNotStr[WindmillFields] | None = None,
        query: str | None = None,
        search_property: WindmillTextFields | SequenceNotStr[WindmillTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
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
        group_by: WindmillFields | SequenceNotStr[WindmillFields],
        property: WindmillFields | SequenceNotStr[WindmillFields] | None = None,
        query: str | None = None,
        search_property: WindmillTextFields | SequenceNotStr[WindmillTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
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
        group_by: WindmillFields | SequenceNotStr[WindmillFields] | None = None,
        property: WindmillFields | SequenceNotStr[WindmillFields] | None = None,
        query: str | None = None,
        search_property: WindmillTextFields | SequenceNotStr[WindmillTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across windmills

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
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
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: WindmillFields,
        interval: float,
        query: str | None = None,
        search_property: WindmillTextFields | SequenceNotStr[WindmillTextFields] | None = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
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
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    @property
    def query(self) -> WindmillQuery:
        """Start a query for windmills."""
        warnings.warn("The .query is in alpha and is subject to breaking changes without notice.")
        return WindmillQuery(self._client)

    def list(
        self,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: WindmillFields | Sequence[WindmillFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
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
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `blades`, `metmast`, `nacelle` and `rotor` for the windmills. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

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

        if retrieve_connections == "skip":
            return self._list(
                limit=limit,
                filter=filter_,
                sort_by=sort_by,  # type: ignore[arg-type]
                direction=direction,
                sort=sort,
            )

        builder = QueryBuilder(WindmillList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                Windmill,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        edge_blades = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_blades,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
            )
        )
        edge_metmast = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_metmast,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
            )
        )
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name(edge_blades),
                    dm.query.NodeResultSetExpression(
                        from_=edge_blades,
                        filter=dm.filters.HasData(views=[Blade._view_id]),
                    ),
                    Blade,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(edge_metmast),
                    dm.query.NodeResultSetExpression(
                        from_=edge_metmast,
                        filter=dm.filters.HasData(views=[Metmast._view_id]),
                    ),
                    Metmast,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[Nacelle._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("nacelle"),
                    ),
                    Nacelle,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[Rotor._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("rotor"),
                    ),
                    Rotor,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        return builder.execute(self._client, remove_not_connected=False)
