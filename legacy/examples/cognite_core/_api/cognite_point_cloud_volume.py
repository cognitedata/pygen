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
from cognite_core.data_classes._cognite_point_cloud_volume import (
    CognitePointCloudVolumeQuery,
    _COGNITEPOINTCLOUDVOLUME_PROPERTIES_BY_FIELD,
    _create_cognite_point_cloud_volume_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CognitePointCloudVolume,
    CognitePointCloudVolumeWrite,
    CognitePointCloudVolumeFields,
    CognitePointCloudVolumeList,
    CognitePointCloudVolumeWriteList,
    CognitePointCloudVolumeTextFields,
    Cognite3DObject,
    CogniteCADModel,
    CogniteCADRevision,
)


class CognitePointCloudVolumeAPI(
    NodeAPI[
        CognitePointCloudVolume,
        CognitePointCloudVolumeWrite,
        CognitePointCloudVolumeList,
        CognitePointCloudVolumeWriteList,
    ]
):
    _view_id = dm.ViewId("cdf_cdm", "CognitePointCloudVolume", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITEPOINTCLOUDVOLUME_PROPERTIES_BY_FIELD
    _class_type = CognitePointCloudVolume
    _class_list = CognitePointCloudVolumeList
    _class_write_list = CognitePointCloudVolumeWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CognitePointCloudVolume | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CognitePointCloudVolumeList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CognitePointCloudVolume | CognitePointCloudVolumeList | None:
        """Retrieve one or more Cognite point cloud volumes by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite point cloud volumes.
            space: The space where all the Cognite point cloud volumes are located.
            retrieve_connections: Whether to retrieve `model_3d`, `object_3d` and `revisions` for the Cognite point
            cloud volumes. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve
            the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested Cognite point cloud volumes.

        Examples:

            Retrieve cognite_point_cloud_volume by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_point_cloud_volume = client.cognite_point_cloud_volume.retrieve(
                ...     "my_cognite_point_cloud_volume"
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
        properties: CognitePointCloudVolumeTextFields | SequenceNotStr[CognitePointCloudVolumeTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        format_version: str | list[str] | None = None,
        format_version_prefix: str | None = None,
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
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        revisions: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        volume_type: Literal["Box", "Cylinder"] | list[Literal["Box", "Cylinder"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CognitePointCloudVolumeFields | SequenceNotStr[CognitePointCloudVolumeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CognitePointCloudVolumeList:
        """Search Cognite point cloud volumes

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            format_version: The format version to filter on.
            format_version_prefix: The prefix of the format version to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_3d: The object 3d to filter on.
            revisions: The revision to filter on.
            volume_type: The volume type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite point cloud volumes to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite point cloud volumes matching the query.

        Examples:

           Search for 'my_cognite_point_cloud_volume' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_point_cloud_volumes = client.cognite_point_cloud_volume.search(
                ...     'my_cognite_point_cloud_volume'
                ... )

        """
        filter_ = _create_cognite_point_cloud_volume_filter(
            self._view_id,
            description,
            description_prefix,
            format_version,
            format_version_prefix,
            model_3d,
            name,
            name_prefix,
            object_3d,
            revisions,
            volume_type,
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
        property: CognitePointCloudVolumeFields | SequenceNotStr[CognitePointCloudVolumeFields] | None = None,
        query: str | None = None,
        search_property: (
            CognitePointCloudVolumeTextFields | SequenceNotStr[CognitePointCloudVolumeTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        format_version: str | list[str] | None = None,
        format_version_prefix: str | None = None,
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
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        revisions: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        volume_type: Literal["Box", "Cylinder"] | list[Literal["Box", "Cylinder"]] | None = None,
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
        property: CognitePointCloudVolumeFields | SequenceNotStr[CognitePointCloudVolumeFields] | None = None,
        query: str | None = None,
        search_property: (
            CognitePointCloudVolumeTextFields | SequenceNotStr[CognitePointCloudVolumeTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        format_version: str | list[str] | None = None,
        format_version_prefix: str | None = None,
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
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        revisions: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        volume_type: Literal["Box", "Cylinder"] | list[Literal["Box", "Cylinder"]] | None = None,
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
        group_by: CognitePointCloudVolumeFields | SequenceNotStr[CognitePointCloudVolumeFields],
        property: CognitePointCloudVolumeFields | SequenceNotStr[CognitePointCloudVolumeFields] | None = None,
        query: str | None = None,
        search_property: (
            CognitePointCloudVolumeTextFields | SequenceNotStr[CognitePointCloudVolumeTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        format_version: str | list[str] | None = None,
        format_version_prefix: str | None = None,
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
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        revisions: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        volume_type: Literal["Box", "Cylinder"] | list[Literal["Box", "Cylinder"]] | None = None,
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
        group_by: CognitePointCloudVolumeFields | SequenceNotStr[CognitePointCloudVolumeFields] | None = None,
        property: CognitePointCloudVolumeFields | SequenceNotStr[CognitePointCloudVolumeFields] | None = None,
        query: str | None = None,
        search_property: (
            CognitePointCloudVolumeTextFields | SequenceNotStr[CognitePointCloudVolumeTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        format_version: str | list[str] | None = None,
        format_version_prefix: str | None = None,
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
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        revisions: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        volume_type: Literal["Box", "Cylinder"] | list[Literal["Box", "Cylinder"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite point cloud volumes

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            format_version: The format version to filter on.
            format_version_prefix: The prefix of the format version to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_3d: The object 3d to filter on.
            revisions: The revision to filter on.
            volume_type: The volume type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite point cloud volumes to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite point cloud volumes in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_point_cloud_volume.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_point_cloud_volume_filter(
            self._view_id,
            description,
            description_prefix,
            format_version,
            format_version_prefix,
            model_3d,
            name,
            name_prefix,
            object_3d,
            revisions,
            volume_type,
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
        property: CognitePointCloudVolumeFields,
        interval: float,
        query: str | None = None,
        search_property: (
            CognitePointCloudVolumeTextFields | SequenceNotStr[CognitePointCloudVolumeTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        format_version: str | list[str] | None = None,
        format_version_prefix: str | None = None,
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
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        revisions: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        volume_type: Literal["Box", "Cylinder"] | list[Literal["Box", "Cylinder"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite point cloud volumes

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            format_version: The format version to filter on.
            format_version_prefix: The prefix of the format version to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_3d: The object 3d to filter on.
            revisions: The revision to filter on.
            volume_type: The volume type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite point cloud volumes to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_point_cloud_volume_filter(
            self._view_id,
            description,
            description_prefix,
            format_version,
            format_version_prefix,
            model_3d,
            name,
            name_prefix,
            object_3d,
            revisions,
            volume_type,
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

    def select(self) -> CognitePointCloudVolumeQuery:
        """Start selecting from Cognite point cloud volumes."""
        return CognitePointCloudVolumeQuery(self._client)

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
                    CogniteCADModel._view_id,
                    ViewPropertyId(self._view_id, "model3D"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    Cognite3DObject._view_id,
                    ViewPropertyId(self._view_id, "object3D"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    CogniteCADRevision._view_id,
                    ViewPropertyId(self._view_id, "revisions"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        format_version: str | list[str] | None = None,
        format_version_prefix: str | None = None,
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
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        revisions: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        volume_type: Literal["Box", "Cylinder"] | list[Literal["Box", "Cylinder"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[CognitePointCloudVolumeList]:
        """Iterate over Cognite point cloud volumes

        Args:
            chunk_size: The number of Cognite point cloud volumes to return in each iteration. Defaults to 100.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            format_version: The format version to filter on.
            format_version_prefix: The prefix of the format version to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_3d: The object 3d to filter on.
            revisions: The revision to filter on.
            volume_type: The volume type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `model_3d`, `object_3d` and `revisions` for the Cognite point
            cloud volumes. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve
            the identifier of the connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of Cognite point cloud volumes to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of Cognite point cloud volumes

        Examples:

            Iterate Cognite point cloud volumes in chunks of 100 up to 2000 items:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_point_cloud_volumes in client.cognite_point_cloud_volume.iterate(chunk_size=100, limit=2000):
                ...     for cognite_point_cloud_volume in cognite_point_cloud_volumes:
                ...         print(cognite_point_cloud_volume.external_id)

            Iterate Cognite point cloud volumes in chunks of 100 sorted by external_id in descending order:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_point_cloud_volumes in client.cognite_point_cloud_volume.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for cognite_point_cloud_volume in cognite_point_cloud_volumes:
                ...         print(cognite_point_cloud_volume.external_id)

            Iterate Cognite point cloud volumes in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for first_iteration in client.cognite_point_cloud_volume.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for cognite_point_cloud_volumes in client.cognite_point_cloud_volume.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for cognite_point_cloud_volume in cognite_point_cloud_volumes:
                ...         print(cognite_point_cloud_volume.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_cognite_point_cloud_volume_filter(
            self._view_id,
            description,
            description_prefix,
            format_version,
            format_version_prefix,
            model_3d,
            name,
            name_prefix,
            object_3d,
            revisions,
            volume_type,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        format_version: str | list[str] | None = None,
        format_version_prefix: str | None = None,
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
        object_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        revisions: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        volume_type: Literal["Box", "Cylinder"] | list[Literal["Box", "Cylinder"]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CognitePointCloudVolumeFields | Sequence[CognitePointCloudVolumeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CognitePointCloudVolumeList:
        """List/filter Cognite point cloud volumes

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            format_version: The format version to filter on.
            format_version_prefix: The prefix of the format version to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_3d: The object 3d to filter on.
            revisions: The revision to filter on.
            volume_type: The volume type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite point cloud volumes to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `model_3d`, `object_3d` and `revisions` for the Cognite point
            cloud volumes. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve
            the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested Cognite point cloud volumes

        Examples:

            List Cognite point cloud volumes and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_point_cloud_volumes = client.cognite_point_cloud_volume.list(limit=5)

        """
        filter_ = _create_cognite_point_cloud_volume_filter(
            self._view_id,
            description,
            description_prefix,
            format_version,
            format_version_prefix,
            model_3d,
            name,
            name_prefix,
            object_3d,
            revisions,
            volume_type,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
