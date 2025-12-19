from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite_core.data_classes._cognite_visualizable import (
    CogniteVisualizableQuery,
    _create_cognite_visualizable_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CogniteVisualizable,
    CogniteVisualizableWrite,
    CogniteVisualizableFields,
    CogniteVisualizableList,
    CogniteVisualizableWriteList,
    CogniteVisualizableTextFields,
    Cognite3DObject,
    CogniteAsset,
)


class CogniteVisualizableAPI(
    NodeAPI[CogniteVisualizable, CogniteVisualizableWrite, CogniteVisualizableList, CogniteVisualizableWriteList]
):
    _view_id = dm.ViewId("cdf_cdm", "CogniteVisualizable", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = {}
    _direct_children_by_external_id: ClassVar[dict[str, type[DomainModel]]] = {
        "CogniteAsset": CogniteAsset,
    }
    _class_type = CogniteVisualizable
    _class_list = CogniteVisualizableList
    _class_write_list = CogniteVisualizableWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["CogniteAsset"]] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CogniteVisualizable | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["CogniteAsset"]] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CogniteVisualizableList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["CogniteAsset"]] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CogniteVisualizable | CogniteVisualizableList | None:
        """Retrieve one or more Cognite visualizables by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite visualizables.
            space: The space where all the Cognite visualizables are located.
            as_child_class: If you want to retrieve the Cognite visualizables as a child class,
                you can specify the child class here. Note that if one node has properties in
                multiple child classes, you will get duplicate nodes in the result.
            retrieve_connections: Whether to retrieve `object_3d` for the Cognite visualizables. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested Cognite visualizables.

        Examples:

            Retrieve cognite_visualizable by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_visualizable = client.cognite_visualizable.retrieve(
                ...     "my_cognite_visualizable"
                ... )

        """
        return self._retrieve(
            external_id, space, retrieve_connections=retrieve_connections, as_child_class=as_child_class
        )

    def search(
        self,
        query: str,
        properties: CogniteVisualizableTextFields | SequenceNotStr[CogniteVisualizableTextFields] | None = None,
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteVisualizableFields | SequenceNotStr[CogniteVisualizableFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteVisualizableList:
        """Search Cognite visualizables

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            object_3d: The object 3d to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite visualizables to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite visualizables matching the query.

        Examples:

           Search for 'my_cognite_visualizable' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_visualizables = client.cognite_visualizable.search(
                ...     'my_cognite_visualizable'
                ... )

        """
        filter_ = _create_cognite_visualizable_filter(
            self._view_id,
            object_3d,
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
        property: CogniteVisualizableFields | SequenceNotStr[CogniteVisualizableFields] | None = None,
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        property: CogniteVisualizableFields | SequenceNotStr[CogniteVisualizableFields] | None = None,
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        group_by: CogniteVisualizableFields | SequenceNotStr[CogniteVisualizableFields],
        property: CogniteVisualizableFields | SequenceNotStr[CogniteVisualizableFields] | None = None,
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
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
        group_by: CogniteVisualizableFields | SequenceNotStr[CogniteVisualizableFields] | None = None,
        property: CogniteVisualizableFields | SequenceNotStr[CogniteVisualizableFields] | None = None,
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite visualizables

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            object_3d: The object 3d to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite visualizables to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite visualizables in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_visualizable.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_visualizable_filter(
            self._view_id,
            object_3d,
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
        property: CogniteVisualizableFields,
        interval: float,
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite visualizables

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            object_3d: The object 3d to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite visualizables to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_visualizable_filter(
            self._view_id,
            object_3d,
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

    def select(self) -> CogniteVisualizableQuery:
        """Start selecting from Cognite visualizables."""
        return CogniteVisualizableQuery(self._client)

    def _build(
        self,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        chunk_size: int | None = None,
    ) -> QueryExecutor:
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                limit=limit,
                max_retrieve_batch_limit=chunk_size,
                has_container_fields=True,
            )
        )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    Cognite3DObject._view_id,
                    ViewPropertyId(self._view_id, "object3D"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[CogniteVisualizableList]:
        """Iterate over Cognite visualizables

        Args:
            chunk_size: The number of Cognite visualizables to return in each iteration. Defaults to 100.
            object_3d: The object 3d to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `object_3d` for the Cognite visualizables. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of Cognite visualizables to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of Cognite visualizables

        Examples:

            Iterate Cognite visualizables in chunks of 100 up to 2000 items:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_visualizables in client.cognite_visualizable.iterate(chunk_size=100, limit=2000):
                ...     for cognite_visualizable in cognite_visualizables:
                ...         print(cognite_visualizable.external_id)

            Iterate Cognite visualizables in chunks of 100 sorted by external_id in descending order:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_visualizables in client.cognite_visualizable.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for cognite_visualizable in cognite_visualizables:
                ...         print(cognite_visualizable.external_id)

            Iterate Cognite visualizables in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for first_iteration in client.cognite_visualizable.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for cognite_visualizables in client.cognite_visualizable.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for cognite_visualizable in cognite_visualizables:
                ...         print(cognite_visualizable.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_cognite_visualizable_filter(
            self._view_id,
            object_3d,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CogniteVisualizableList:
        """List/filter Cognite visualizables

        Args:
            object_3d: The object 3d to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite visualizables to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `object_3d` for the Cognite visualizables. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested Cognite visualizables

        Examples:

            List Cognite visualizables and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_visualizables = client.cognite_visualizable.list(limit=5)

        """
        filter_ = _create_cognite_visualizable_filter(
            self._view_id,
            object_3d,
            external_id_prefix,
            space,
            filter,
        )
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_)
        return self._query(filter_, limit, retrieve_connections, None, "list")
