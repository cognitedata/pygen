from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    QueryBuilder,
)
from omni.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    DependentOnNonWritable,
    DependentOnNonWritableWrite,
    DependentOnNonWritableFields,
    DependentOnNonWritableList,
    DependentOnNonWritableWriteList,
    DependentOnNonWritableTextFields,
    Implementation1NonWriteable,
)
from omni.data_classes._dependent_on_non_writable import (
    DependentOnNonWritableQuery,
    _DEPENDENTONNONWRITABLE_PROPERTIES_BY_FIELD,
    _create_dependent_on_non_writable_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from .dependent_on_non_writable_to_non_writable import DependentOnNonWritableToNonWritableAPI
from .dependent_on_non_writable_query import DependentOnNonWritableQueryAPI


class DependentOnNonWritableAPI(
    NodeAPI[
        DependentOnNonWritable, DependentOnNonWritableWrite, DependentOnNonWritableList, DependentOnNonWritableWriteList
    ]
):
    _view_id = dm.ViewId("pygen-models", "DependentOnNonWritable", "1")
    _properties_by_field = _DEPENDENTONNONWRITABLE_PROPERTIES_BY_FIELD
    _class_type = DependentOnNonWritable
    _class_list = DependentOnNonWritableList
    _class_write_list = DependentOnNonWritableWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.to_non_writable_edge = DependentOnNonWritableToNonWritableAPI(client)

    def __call__(
        self,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> DependentOnNonWritableQueryAPI[DependentOnNonWritableList]:
        """Query starting at dependent on non writables.

        Args:
            a_value: The a value to filter on.
            a_value_prefix: The prefix of the a value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of dependent on non writables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for dependent on non writables.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_dependent_on_non_writable_filter(
            self._view_id,
            a_value,
            a_value_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(DependentOnNonWritableList)
        return DependentOnNonWritableQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        dependent_on_non_writable: DependentOnNonWritableWrite | Sequence[DependentOnNonWritableWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) dependent on non writables.

        Note: This method iterates through all nodes and timeseries linked to dependent_on_non_writable and creates them including the edges
        between the nodes. For example, if any of `to_non_writable` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            dependent_on_non_writable: Dependent on non writable or sequence of dependent on non writables to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new dependent_on_non_writable:

                >>> from omni import OmniClient
                >>> from omni.data_classes import DependentOnNonWritableWrite
                >>> client = OmniClient()
                >>> dependent_on_non_writable = DependentOnNonWritableWrite(external_id="my_dependent_on_non_writable", ...)
                >>> result = client.dependent_on_non_writable.apply(dependent_on_non_writable)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.dependent_on_non_writable.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(dependent_on_non_writable, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more dependent on non writable.

        Args:
            external_id: External id of the dependent on non writable to delete.
            space: The space where all the dependent on non writable are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete dependent_on_non_writable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.dependent_on_non_writable.delete("my_dependent_on_non_writable")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.dependent_on_non_writable.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> DependentOnNonWritable | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> DependentOnNonWritableList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> DependentOnNonWritable | DependentOnNonWritableList | None:
        """Retrieve one or more dependent on non writables by id(s).

        Args:
            external_id: External id or list of external ids of the dependent on non writables.
            space: The space where all the dependent on non writables are located.

        Returns:
            The requested dependent on non writables.

        Examples:

            Retrieve dependent_on_non_writable by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> dependent_on_non_writable = client.dependent_on_non_writable.retrieve("my_dependent_on_non_writable")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.to_non_writable_edge,
                    "to_non_writable",
                    dm.DirectRelationReference("pygen-models", "toNonWritable"),
                    "outwards",
                    dm.ViewId("pygen-models", "Implementation1NonWriteable", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: DependentOnNonWritableTextFields | SequenceNotStr[DependentOnNonWritableTextFields] | None = None,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> DependentOnNonWritableList:
        """Search dependent on non writables

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            a_value: The a value to filter on.
            a_value_prefix: The prefix of the a value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of dependent on non writables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results dependent on non writables matching the query.

        Examples:

           Search for 'my_dependent_on_non_writable' in all text properties:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> dependent_on_non_writables = client.dependent_on_non_writable.search('my_dependent_on_non_writable')

        """
        filter_ = _create_dependent_on_non_writable_filter(
            self._view_id,
            a_value,
            a_value_prefix,
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
        property: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields] | None = None,
        query: str | None = None,
        search_property: (
            DependentOnNonWritableTextFields | SequenceNotStr[DependentOnNonWritableTextFields] | None
        ) = None,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
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
        property: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields] | None = None,
        query: str | None = None,
        search_property: (
            DependentOnNonWritableTextFields | SequenceNotStr[DependentOnNonWritableTextFields] | None
        ) = None,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
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
        group_by: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields],
        property: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields] | None = None,
        query: str | None = None,
        search_property: (
            DependentOnNonWritableTextFields | SequenceNotStr[DependentOnNonWritableTextFields] | None
        ) = None,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
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
        group_by: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields] | None = None,
        property: DependentOnNonWritableFields | SequenceNotStr[DependentOnNonWritableFields] | None = None,
        query: str | None = None,
        search_property: (
            DependentOnNonWritableTextFields | SequenceNotStr[DependentOnNonWritableTextFields] | None
        ) = None,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across dependent on non writables

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            a_value: The a value to filter on.
            a_value_prefix: The prefix of the a value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of dependent on non writables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count dependent on non writables in space `my_space`:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> result = client.dependent_on_non_writable.aggregate("count", space="my_space")

        """

        filter_ = _create_dependent_on_non_writable_filter(
            self._view_id,
            a_value,
            a_value_prefix,
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
        property: DependentOnNonWritableFields,
        interval: float,
        query: str | None = None,
        search_property: (
            DependentOnNonWritableTextFields | SequenceNotStr[DependentOnNonWritableTextFields] | None
        ) = None,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for dependent on non writables

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            a_value: The a value to filter on.
            a_value_prefix: The prefix of the a value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of dependent on non writables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_dependent_on_non_writable_filter(
            self._view_id,
            a_value,
            a_value_prefix,
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

    def query(self) -> DependentOnNonWritableQuery:
        """Start a query for dependent on non writables."""
        warnings.warn("The .query is in alpha and is subject to breaking changes without notice.")
        return DependentOnNonWritableQuery(self._client)

    def list(
        self,
        a_value: str | list[str] | None = None,
        a_value_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: DependentOnNonWritableFields | Sequence[DependentOnNonWritableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> DependentOnNonWritableList:
        """List/filter dependent on non writables

        Args:
            a_value: The a value to filter on.
            a_value_prefix: The prefix of the a value to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of dependent on non writables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `to_non_writable` for the dependent on non writables. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested dependent on non writables

        Examples:

            List dependent on non writables and limit to 5:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> dependent_on_non_writables = client.dependent_on_non_writable.list(limit=5)

        """
        filter_ = _create_dependent_on_non_writable_filter(
            self._view_id,
            a_value,
            a_value_prefix,
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

        builder = QueryBuilder(DependentOnNonWritableList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                DependentOnNonWritable,
                max_retrieve_limit=limit,
            )
        )
        from_root = builder.get_from()
        edge_to_non_writable = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_to_non_writable,
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
                    builder.create_name(edge_to_non_writable),
                    dm.query.NodeResultSetExpression(
                        from_=edge_to_non_writable,
                        filter=dm.filters.HasData(views=[Implementation1NonWriteable._view_id]),
                    ),
                    Implementation1NonWriteable,
                )
            )

        return builder.execute(self._client)
