from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core.config import global_config
from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryStepFactory,
    QueryBuilder,
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
from cognite_core._api.cognite_point_cloud_volume_query import CognitePointCloudVolumeQueryAPI


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

    def __call__(
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
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CognitePointCloudVolumeQueryAPI[CognitePointCloudVolume, CognitePointCloudVolumeList]:
        """Query starting at Cognite point cloud volumes.

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite point cloud volumes to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite point cloud volumes.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
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
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return CognitePointCloudVolumeQueryAPI(
            self._client, QueryBuilder(), self._class_type, self._class_list, None, filter_, limit
        )

    def apply(
        self,
        cognite_point_cloud_volume: CognitePointCloudVolumeWrite | Sequence[CognitePointCloudVolumeWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite point cloud volumes.

        Args:
            cognite_point_cloud_volume: Cognite point cloud volume or
                sequence of Cognite point cloud volumes to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None.
                However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cognite_point_cloud_volume:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import CognitePointCloudVolumeWrite
                >>> client = CogniteCoreClient()
                >>> cognite_point_cloud_volume = CognitePointCloudVolumeWrite(
                ...     external_id="my_cognite_point_cloud_volume", ...
                ... )
                >>> result = client.cognite_point_cloud_volume.apply(cognite_point_cloud_volume)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_point_cloud_volume.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_point_cloud_volume, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite point cloud volume.

        Args:
            external_id: External id of the Cognite point cloud volume to delete.
            space: The space where all the Cognite point cloud volume are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_point_cloud_volume by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_point_cloud_volume.delete("my_cognite_point_cloud_volume")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_point_cloud_volume.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

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

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> CognitePointCloudVolumeList:
        builder = QueryBuilder()
        factory = QueryStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(
            factory.root(
                filter=filter_,
                sort=sort,
                limit=limit,
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
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        unpacked = QueryUnpacker(builder, edges=unpack_edges).unpack()
        if global_config.validate_retrieve:
            retrieved = [CognitePointCloudVolume.model_validate(item) for item in unpacked]
        else:
            retrieved = [CognitePointCloudVolume.model_construct(**item) for item in unpacked]
        return CognitePointCloudVolumeList(retrieved)

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
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input)
