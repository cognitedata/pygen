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
from cognite_core.data_classes._cognite_360_image_collection import (
    Cognite360ImageCollectionQuery,
    _COGNITE360IMAGECOLLECTION_PROPERTIES_BY_FIELD,
    _create_cognite_360_image_collection_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Cognite360ImageCollection,
    Cognite360ImageCollectionWrite,
    Cognite360ImageCollectionFields,
    Cognite360ImageCollectionList,
    Cognite360ImageCollectionWriteList,
    Cognite360ImageCollectionTextFields,
    Cognite360ImageModel,
)


class Cognite360ImageCollectionAPI(
    NodeAPI[
        Cognite360ImageCollection,
        Cognite360ImageCollectionWrite,
        Cognite360ImageCollectionList,
        Cognite360ImageCollectionWriteList,
    ]
):
    _view_id = dm.ViewId("cdf_cdm", "Cognite360ImageCollection", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITE360IMAGECOLLECTION_PROPERTIES_BY_FIELD
    _class_type = Cognite360ImageCollection
    _class_list = Cognite360ImageCollectionList
    _class_write_list = Cognite360ImageCollectionWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite360ImageCollection | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite360ImageCollectionList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite360ImageCollection | Cognite360ImageCollectionList | None:
        """Retrieve one or more Cognite 360 image collections by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite 360 image collections.
            space: The space where all the Cognite 360 image collections are located.
            retrieve_connections: Whether to retrieve `model_3d` for the Cognite 360 image collections. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested Cognite 360 image collections.

        Examples:

            Retrieve cognite_360_image_collection by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_360_image_collection = client.cognite_360_image_collection.retrieve(
                ...     "my_cognite_360_image_collection"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
        )

    def search(
        self,
        query: str,
        properties: (
            Cognite360ImageCollectionTextFields | SequenceNotStr[Cognite360ImageCollectionTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        sort_by: Cognite360ImageCollectionFields | SequenceNotStr[Cognite360ImageCollectionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Cognite360ImageCollectionList:
        """Search Cognite 360 image collections

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            published: The published to filter on.
            status: The status to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 360 image collections to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite 360 image collections matching the query.

        Examples:

           Search for 'my_cognite_360_image_collection' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_360_image_collections = client.cognite_360_image_collection.search(
                ...     'my_cognite_360_image_collection'
                ... )

        """
        filter_ = _create_cognite_360_image_collection_filter(
            self._view_id,
            description,
            description_prefix,
            model_3d,
            name,
            name_prefix,
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
        property: Cognite360ImageCollectionFields | SequenceNotStr[Cognite360ImageCollectionFields] | None = None,
        query: str | None = None,
        search_property: (
            Cognite360ImageCollectionTextFields | SequenceNotStr[Cognite360ImageCollectionTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: Cognite360ImageCollectionFields | SequenceNotStr[Cognite360ImageCollectionFields] | None = None,
        query: str | None = None,
        search_property: (
            Cognite360ImageCollectionTextFields | SequenceNotStr[Cognite360ImageCollectionTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        group_by: Cognite360ImageCollectionFields | SequenceNotStr[Cognite360ImageCollectionFields],
        property: Cognite360ImageCollectionFields | SequenceNotStr[Cognite360ImageCollectionFields] | None = None,
        query: str | None = None,
        search_property: (
            Cognite360ImageCollectionTextFields | SequenceNotStr[Cognite360ImageCollectionTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        group_by: Cognite360ImageCollectionFields | SequenceNotStr[Cognite360ImageCollectionFields] | None = None,
        property: Cognite360ImageCollectionFields | SequenceNotStr[Cognite360ImageCollectionFields] | None = None,
        query: str | None = None,
        search_property: (
            Cognite360ImageCollectionTextFields | SequenceNotStr[Cognite360ImageCollectionTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        """Aggregate data across Cognite 360 image collections

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            published: The published to filter on.
            status: The status to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 360 image collections to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite 360 image collections in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_360_image_collection.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_360_image_collection_filter(
            self._view_id,
            description,
            description_prefix,
            model_3d,
            name,
            name_prefix,
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
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: Cognite360ImageCollectionFields,
        interval: float,
        query: str | None = None,
        search_property: (
            Cognite360ImageCollectionTextFields | SequenceNotStr[Cognite360ImageCollectionTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        """Produces histograms for Cognite 360 image collections

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            published: The published to filter on.
            status: The status to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 360 image collections to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_360_image_collection_filter(
            self._view_id,
            description,
            description_prefix,
            model_3d,
            name,
            name_prefix,
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
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    def select(self) -> Cognite360ImageCollectionQuery:
        """Start selecting from Cognite 360 image collections."""
        return Cognite360ImageCollectionQuery(self._client)

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
                    Cognite360ImageModel._view_id,
                    ViewPropertyId(self._view_id, "model3D"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
    ) -> Iterator[Cognite360ImageCollectionList]:
        """Iterate over Cognite 360 image collections

        Args:
            chunk_size: The number of Cognite 360 image collections to return in each iteration. Defaults to 100.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            published: The published to filter on.
            status: The status to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `model_3d` for the Cognite 360 image collections. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of Cognite 360 image collections to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of Cognite 360 image collections

        Examples:

            Iterate Cognite 360 image collections in chunks of 100 up to 2000 items:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_360_image_collections in client.cognite_360_image_collection.iterate(chunk_size=100, limit=2000):
                ...     for cognite_360_image_collection in cognite_360_image_collections:
                ...         print(cognite_360_image_collection.external_id)

            Iterate Cognite 360 image collections in chunks of 100 sorted by external_id in descending order:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_360_image_collections in client.cognite_360_image_collection.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for cognite_360_image_collection in cognite_360_image_collections:
                ...         print(cognite_360_image_collection.external_id)

            Iterate Cognite 360 image collections in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for first_iteration in client.cognite_360_image_collection.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for cognite_360_image_collections in client.cognite_360_image_collection.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for cognite_360_image_collection in cognite_360_image_collections:
                ...         print(cognite_360_image_collection.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_cognite_360_image_collection_filter(
            self._view_id,
            description,
            description_prefix,
            model_3d,
            name,
            name_prefix,
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
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        sort_by: Cognite360ImageCollectionFields | Sequence[Cognite360ImageCollectionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite360ImageCollectionList:
        """List/filter Cognite 360 image collections

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            published: The published to filter on.
            status: The status to filter on.
            type_: The type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 360 image collections to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `model_3d` for the Cognite 360 image collections. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested Cognite 360 image collections

        Examples:

            List Cognite 360 image collections and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_360_image_collections = client.cognite_360_image_collection.list(limit=5)

        """
        filter_ = _create_cognite_360_image_collection_filter(
            self._view_id,
            description,
            description_prefix,
            model_3d,
            name,
            name_prefix,
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
