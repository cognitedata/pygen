from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core._api.cognite_point_cloud_revision_query import CognitePointCloudRevisionQueryAPI
from cognite_core.data_classes import (
    CognitePointCloudModel,
    CognitePointCloudRevision,
    CognitePointCloudRevisionFields,
    CognitePointCloudRevisionList,
    CognitePointCloudRevisionTextFields,
    CognitePointCloudRevisionWrite,
    CognitePointCloudRevisionWriteList,
    ResourcesWriteResult,
)
from cognite_core.data_classes._cognite_point_cloud_revision import (
    _COGNITEPOINTCLOUDREVISION_PROPERTIES_BY_FIELD,
    CognitePointCloudRevisionQuery,
    _create_cognite_point_cloud_revision_filter,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
)


class CognitePointCloudRevisionAPI(
    NodeAPI[
        CognitePointCloudRevision,
        CognitePointCloudRevisionWrite,
        CognitePointCloudRevisionList,
        CognitePointCloudRevisionWriteList,
    ]
):
    _view_id = dm.ViewId("cdf_cdm", "CognitePointCloudRevision", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITEPOINTCLOUDREVISION_PROPERTIES_BY_FIELD
    _class_type = CognitePointCloudRevision
    _class_list = CognitePointCloudRevisionList
    _class_write_list = CognitePointCloudRevisionWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
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
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CognitePointCloudRevisionQueryAPI[CognitePointCloudRevisionList]:
        """Query starting at Cognite point cloud revisions.

        Args:
            model_3d: The model 3d to filter on.
            published: The published to filter on.
            min_revision_id: The minimum value of the revision id to filter on.
            max_revision_id: The maximum value of the revision id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite point cloud revisions to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite point cloud revisions.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cognite_point_cloud_revision_filter(
            self._view_id,
            model_3d,
            published,
            min_revision_id,
            max_revision_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(CognitePointCloudRevisionList)
        return CognitePointCloudRevisionQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        cognite_point_cloud_revision: CognitePointCloudRevisionWrite | Sequence[CognitePointCloudRevisionWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite point cloud revisions.

        Note: This method iterates through all nodes and timeseries linked to cognite_point_cloud_revision
        and creates them including the edges
        between the nodes. For example, if any of
        `model_3d`
        are set, then these nodes as well as any nodes linked to them, and all the edges linking
        these nodes will be created.

        Args:
            cognite_point_cloud_revision: Cognite point cloud revision or
                sequence of Cognite point cloud revisions to upsert.
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

            Create a new cognite_point_cloud_revision:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import CognitePointCloudRevisionWrite
                >>> client = CogniteCoreClient()
                >>> cognite_point_cloud_revision = CognitePointCloudRevisionWrite(
                ...     external_id="my_cognite_point_cloud_revision", ...
                ... )
                >>> result = client.cognite_point_cloud_revision.apply(cognite_point_cloud_revision)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_point_cloud_revision.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_point_cloud_revision, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite point cloud revision.

        Args:
            external_id: External id of the Cognite point cloud revision to delete.
            space: The space where all the Cognite point cloud revision are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_point_cloud_revision by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_point_cloud_revision.delete("my_cognite_point_cloud_revision")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_point_cloud_revision.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CognitePointCloudRevision | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CognitePointCloudRevisionList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CognitePointCloudRevision | CognitePointCloudRevisionList | None:
        """Retrieve one or more Cognite point cloud revisions by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite point cloud revisions.
            space: The space where all the Cognite point cloud revisions are located.

        Returns:
            The requested Cognite point cloud revisions.

        Examples:

            Retrieve cognite_point_cloud_revision by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_point_cloud_revision = client.cognite_point_cloud_revision.retrieve("my_cognite_point_cloud_revision")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: (
            CognitePointCloudRevisionTextFields | SequenceNotStr[CognitePointCloudRevisionTextFields] | None
        ) = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CognitePointCloudRevisionFields | SequenceNotStr[CognitePointCloudRevisionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CognitePointCloudRevisionList:
        """Search Cognite point cloud revisions

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            model_3d: The model 3d to filter on.
            published: The published to filter on.
            min_revision_id: The minimum value of the revision id to filter on.
            max_revision_id: The maximum value of the revision id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite point cloud revisions to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite point cloud revisions matching the query.

        Examples:

           Search for 'my_cognite_point_cloud_revision' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_point_cloud_revisions = client.cognite_point_cloud_revision.search('my_cognite_point_cloud_revision')

        """
        filter_ = _create_cognite_point_cloud_revision_filter(
            self._view_id,
            model_3d,
            published,
            min_revision_id,
            max_revision_id,
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
        property: CognitePointCloudRevisionFields | SequenceNotStr[CognitePointCloudRevisionFields] | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
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
        property: CognitePointCloudRevisionFields | SequenceNotStr[CognitePointCloudRevisionFields] | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
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
        group_by: CognitePointCloudRevisionFields | SequenceNotStr[CognitePointCloudRevisionFields],
        property: CognitePointCloudRevisionFields | SequenceNotStr[CognitePointCloudRevisionFields] | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
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
        group_by: CognitePointCloudRevisionFields | SequenceNotStr[CognitePointCloudRevisionFields] | None = None,
        property: CognitePointCloudRevisionFields | SequenceNotStr[CognitePointCloudRevisionFields] | None = None,
        model_3d: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        published: bool | None = None,
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite point cloud revisions

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            model_3d: The model 3d to filter on.
            published: The published to filter on.
            min_revision_id: The minimum value of the revision id to filter on.
            max_revision_id: The maximum value of the revision id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite point cloud revisions to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite point cloud revisions in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_point_cloud_revision.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_point_cloud_revision_filter(
            self._view_id,
            model_3d,
            published,
            min_revision_id,
            max_revision_id,
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
        property: CognitePointCloudRevisionFields,
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
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite point cloud revisions

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            model_3d: The model 3d to filter on.
            published: The published to filter on.
            min_revision_id: The minimum value of the revision id to filter on.
            max_revision_id: The maximum value of the revision id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite point cloud revisions to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_point_cloud_revision_filter(
            self._view_id,
            model_3d,
            published,
            min_revision_id,
            max_revision_id,
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

    def query(self) -> CognitePointCloudRevisionQuery:
        """Start a query for Cognite point cloud revisions."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return CognitePointCloudRevisionQuery(self._client)

    def select(self) -> CognitePointCloudRevisionQuery:
        """Start selecting from Cognite point cloud revisions."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return CognitePointCloudRevisionQuery(self._client)

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
        min_revision_id: int | None = None,
        max_revision_id: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CognitePointCloudRevisionFields | Sequence[CognitePointCloudRevisionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CognitePointCloudRevisionList:
        """List/filter Cognite point cloud revisions

        Args:
            model_3d: The model 3d to filter on.
            published: The published to filter on.
            min_revision_id: The minimum value of the revision id to filter on.
            max_revision_id: The maximum value of the revision id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite point cloud revisions to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `model_3d` for the Cognite point cloud revisions. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested Cognite point cloud revisions

        Examples:

            List Cognite point cloud revisions and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_point_cloud_revisions = client.cognite_point_cloud_revision.list(limit=5)

        """
        filter_ = _create_cognite_point_cloud_revision_filter(
            self._view_id,
            model_3d,
            published,
            min_revision_id,
            max_revision_id,
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

        builder = DataClassQueryBuilder(CognitePointCloudRevisionList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                CognitePointCloudRevision,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CognitePointCloudModel._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("model3D"),
                    ),
                    CognitePointCloudModel,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
