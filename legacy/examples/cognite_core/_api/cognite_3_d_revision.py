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
from cognite_core.data_classes._cognite_3_d_revision import (
    Cognite3DRevisionQuery,
    _COGNITE3DREVISION_PROPERTIES_BY_FIELD,
    _create_cognite_3_d_revision_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Cognite3DRevision,
    Cognite3DRevisionWrite,
    Cognite3DRevisionFields,
    Cognite3DRevisionList,
    Cognite3DRevisionWriteList,
    Cognite3DRevisionTextFields,
    Cognite3DModel,
    Cognite360ImageCollection,
    CogniteCADRevision,
    CognitePointCloudRevision,
)


class Cognite3DRevisionAPI(
    NodeAPI[Cognite3DRevision, Cognite3DRevisionWrite, Cognite3DRevisionList, Cognite3DRevisionWriteList]
):
    _view_id = dm.ViewId("cdf_cdm", "Cognite3DRevision", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITE3DREVISION_PROPERTIES_BY_FIELD
    _direct_children_by_external_id: ClassVar[dict[str, type[DomainModel]]] = {
        "Cognite360ImageCollection": Cognite360ImageCollection,
        "CogniteCADRevision": CogniteCADRevision,
        "CognitePointCloudRevision": CognitePointCloudRevision,
    }
    _class_type = Cognite3DRevision
    _class_list = Cognite3DRevisionList
    _class_write_list = Cognite3DRevisionWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: (
            SequenceNotStr[Literal["Cognite360ImageCollection", "CogniteCADRevision", "CognitePointCloudRevision"]]
            | None
        ) = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite3DRevision | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: (
            SequenceNotStr[Literal["Cognite360ImageCollection", "CogniteCADRevision", "CognitePointCloudRevision"]]
            | None
        ) = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite3DRevisionList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: (
            SequenceNotStr[Literal["Cognite360ImageCollection", "CogniteCADRevision", "CognitePointCloudRevision"]]
            | None
        ) = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite3DRevision | Cognite3DRevisionList | None:
        """Retrieve one or more Cognite 3D revisions by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite 3D revisions.
            space: The space where all the Cognite 3D revisions are located.
            as_child_class: If you want to retrieve the Cognite 3D revisions as a child class,
                you can specify the child class here. Note that if one node has properties in
                multiple child classes, you will get duplicate nodes in the result.
            retrieve_connections: Whether to retrieve `model_3d` for the Cognite 3D revisions. Defaults to 'skip'.'skip'
            will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items,
            and 'full' will retrieve the full connected items.

        Returns:
            The requested Cognite 3D revisions.

        Examples:

            Retrieve cognite_3_d_revision by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_revision = client.cognite_3_d_revision.retrieve(
                ...     "my_cognite_3_d_revision"
                ... )

        """
        return self._retrieve(
            external_id, space, retrieve_connections=retrieve_connections, as_child_class=as_child_class
        )

    def search(
        self,
        query: str,
        properties: Cognite3DRevisionTextFields | SequenceNotStr[Cognite3DRevisionTextFields] | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        status: (
            Literal["Done", "Failed", "Processing", "Queued"]
            | list[Literal["Done", "Failed", "Processing", "Queued"]]
            | None
        ) = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | list[Literal["CAD", "Image360", "PointCloud"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Cognite3DRevisionFields | SequenceNotStr[Cognite3DRevisionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Cognite3DRevisionList:
        """Search Cognite 3D revisions

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            model_3d: The model 3d to filter on.
            published: The published to filter on.
            status: The status to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D revisions to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite 3D revisions matching the query.

        Examples:

           Search for 'my_cognite_3_d_revision' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_revisions = client.cognite_3_d_revision.search(
                ...     'my_cognite_3_d_revision'
                ... )

        """
        filter_ = _create_cognite_3_d_revision_filter(
            self._view_id,
            model_3d,
            published,
            status,
            type_,
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
        property: Cognite3DRevisionFields | SequenceNotStr[Cognite3DRevisionFields] | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        status: (
            Literal["Done", "Failed", "Processing", "Queued"]
            | list[Literal["Done", "Failed", "Processing", "Queued"]]
            | None
        ) = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | list[Literal["CAD", "Image360", "PointCloud"]] | None = None,
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
        property: Cognite3DRevisionFields | SequenceNotStr[Cognite3DRevisionFields] | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        status: (
            Literal["Done", "Failed", "Processing", "Queued"]
            | list[Literal["Done", "Failed", "Processing", "Queued"]]
            | None
        ) = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | list[Literal["CAD", "Image360", "PointCloud"]] | None = None,
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
        group_by: Cognite3DRevisionFields | SequenceNotStr[Cognite3DRevisionFields],
        property: Cognite3DRevisionFields | SequenceNotStr[Cognite3DRevisionFields] | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        status: (
            Literal["Done", "Failed", "Processing", "Queued"]
            | list[Literal["Done", "Failed", "Processing", "Queued"]]
            | None
        ) = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | list[Literal["CAD", "Image360", "PointCloud"]] | None = None,
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
        group_by: Cognite3DRevisionFields | SequenceNotStr[Cognite3DRevisionFields] | None = None,
        property: Cognite3DRevisionFields | SequenceNotStr[Cognite3DRevisionFields] | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        status: (
            Literal["Done", "Failed", "Processing", "Queued"]
            | list[Literal["Done", "Failed", "Processing", "Queued"]]
            | None
        ) = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | list[Literal["CAD", "Image360", "PointCloud"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite 3D revisions

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            model_3d: The model 3d to filter on.
            published: The published to filter on.
            status: The status to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D revisions to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite 3D revisions in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_3_d_revision.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_3_d_revision_filter(
            self._view_id,
            model_3d,
            published,
            status,
            type_,
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
        property: Cognite3DRevisionFields,
        interval: float,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        status: (
            Literal["Done", "Failed", "Processing", "Queued"]
            | list[Literal["Done", "Failed", "Processing", "Queued"]]
            | None
        ) = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | list[Literal["CAD", "Image360", "PointCloud"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite 3D revisions

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            model_3d: The model 3d to filter on.
            published: The published to filter on.
            status: The status to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D revisions to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_3_d_revision_filter(
            self._view_id,
            model_3d,
            published,
            status,
            type_,
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

    def select(self) -> Cognite3DRevisionQuery:
        """Start selecting from Cognite 3D revisions."""
        return Cognite3DRevisionQuery(self._client)

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
                sort=sort,
                limit=limit,
                max_retrieve_batch_limit=chunk_size,
                has_container_fields=True,
            )
        )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    Cognite3DModel._view_id,
                    ViewPropertyId(self._view_id, "model3D"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        status: (
            Literal["Done", "Failed", "Processing", "Queued"]
            | list[Literal["Done", "Failed", "Processing", "Queued"]]
            | None
        ) = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | list[Literal["CAD", "Image360", "PointCloud"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[Cognite3DRevisionList]:
        """Iterate over Cognite 3D revisions

        Args:
            chunk_size: The number of Cognite 3D revisions to return in each iteration. Defaults to 100.
            model_3d: The model 3d to filter on.
            published: The published to filter on.
            status: The status to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `model_3d` for the Cognite 3D revisions. Defaults to 'skip'.'skip'
            will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items,
            and 'full' will retrieve the full connected items.
            limit: Maximum number of Cognite 3D revisions to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of Cognite 3D revisions

        Examples:

            Iterate Cognite 3D revisions in chunks of 100 up to 2000 items:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_3_d_revisions in client.cognite_3_d_revision.iterate(chunk_size=100, limit=2000):
                ...     for cognite_3_d_revision in cognite_3_d_revisions:
                ...         print(cognite_3_d_revision.external_id)

            Iterate Cognite 3D revisions in chunks of 100 sorted by external_id in descending order:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_3_d_revisions in client.cognite_3_d_revision.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for cognite_3_d_revision in cognite_3_d_revisions:
                ...         print(cognite_3_d_revision.external_id)

            Iterate Cognite 3D revisions in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for first_iteration in client.cognite_3_d_revision.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for cognite_3_d_revisions in client.cognite_3_d_revision.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for cognite_3_d_revision in cognite_3_d_revisions:
                ...         print(cognite_3_d_revision.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_cognite_3_d_revision_filter(
            self._view_id,
            model_3d,
            published,
            status,
            type_,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        status: (
            Literal["Done", "Failed", "Processing", "Queued"]
            | list[Literal["Done", "Failed", "Processing", "Queued"]]
            | None
        ) = None,
        type_: Literal["CAD", "Image360", "PointCloud"] | list[Literal["CAD", "Image360", "PointCloud"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Cognite3DRevisionFields | Sequence[Cognite3DRevisionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite3DRevisionList:
        """List/filter Cognite 3D revisions

        Args:
            model_3d: The model 3d to filter on.
            published: The published to filter on.
            status: The status to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D revisions to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `model_3d` for the Cognite 3D revisions. Defaults to 'skip'.'skip'
            will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items,
            and 'full' will retrieve the full connected items.

        Returns:
            List of requested Cognite 3D revisions

        Examples:

            List Cognite 3D revisions and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_revisions = client.cognite_3_d_revision.list(limit=5)

        """
        filter_ = _create_cognite_3_d_revision_filter(
            self._view_id,
            model_3d,
            published,
            status,
            type_,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
