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
from cognite_core.data_classes._cognite_3_d_object import (
    Cognite3DObjectQuery,
    _COGNITE3DOBJECT_PROPERTIES_BY_FIELD,
    _create_cognite_3_d_object_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Cognite3DObject,
    Cognite3DObjectWrite,
    Cognite3DObjectFields,
    Cognite3DObjectList,
    Cognite3DObjectWriteList,
    Cognite3DObjectTextFields,
    Cognite360ImageAnnotation,
    Cognite360ImageAnnotationWrite,
    Cognite360ImageAnnotationList,
    Cognite360Image,
    Cognite360ImageAnnotation,
    CogniteAsset,
    CogniteCADNode,
    CognitePointCloudVolume,
)
from cognite_core._api.cognite_3_d_object_images_360 import Cognite3DObjectImages360API


class Cognite3DObjectAPI(NodeAPI[Cognite3DObject, Cognite3DObjectWrite, Cognite3DObjectList, Cognite3DObjectWriteList]):
    _view_id = dm.ViewId("cdf_cdm", "Cognite3DObject", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITE3DOBJECT_PROPERTIES_BY_FIELD
    _class_type = Cognite3DObject
    _class_list = Cognite3DObjectList
    _class_write_list = Cognite3DObjectWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.images_360_edge = Cognite3DObjectImages360API(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite3DObject | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite3DObjectList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite3DObject | Cognite3DObjectList | None:
        """Retrieve one or more Cognite 3D objects by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite 3D objects.
            space: The space where all the Cognite 3D objects are located.
            retrieve_connections: Whether to retrieve `asset`, `cad_nodes`, `images_360` and `point_cloud_volumes` for
            the Cognite 3D objects. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only
            retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested Cognite 3D objects.

        Examples:

            Retrieve cognite_3_d_object by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_object = client.cognite_3_d_object.retrieve(
                ...     "my_cognite_3_d_object"
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
        properties: Cognite3DObjectTextFields | SequenceNotStr[Cognite3DObjectTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Cognite3DObjectList:
        """Search Cognite 3D objects

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_x_max: The minimum value of the x max to filter on.
            max_x_max: The maximum value of the x max to filter on.
            min_x_min: The minimum value of the x min to filter on.
            max_x_min: The maximum value of the x min to filter on.
            min_y_max: The minimum value of the y max to filter on.
            max_y_max: The maximum value of the y max to filter on.
            min_y_min: The minimum value of the y min to filter on.
            max_y_min: The maximum value of the y min to filter on.
            min_z_max: The minimum value of the z max to filter on.
            max_z_max: The maximum value of the z max to filter on.
            min_z_min: The minimum value of the z min to filter on.
            max_z_min: The maximum value of the z min to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D objects to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite 3D objects matching the query.

        Examples:

           Search for 'my_cognite_3_d_object' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_objects = client.cognite_3_d_object.search(
                ...     'my_cognite_3_d_object'
                ... )

        """
        filter_ = _create_cognite_3_d_object_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            min_x_max,
            max_x_max,
            min_x_min,
            max_x_min,
            min_y_max,
            max_y_max,
            min_y_min,
            max_y_min,
            min_z_max,
            max_z_max,
            min_z_min,
            max_z_min,
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
        property: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields] | None = None,
        query: str | None = None,
        search_property: Cognite3DObjectTextFields | SequenceNotStr[Cognite3DObjectTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
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
        property: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields] | None = None,
        query: str | None = None,
        search_property: Cognite3DObjectTextFields | SequenceNotStr[Cognite3DObjectTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
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
        group_by: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields],
        property: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields] | None = None,
        query: str | None = None,
        search_property: Cognite3DObjectTextFields | SequenceNotStr[Cognite3DObjectTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
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
        group_by: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields] | None = None,
        property: Cognite3DObjectFields | SequenceNotStr[Cognite3DObjectFields] | None = None,
        query: str | None = None,
        search_property: Cognite3DObjectTextFields | SequenceNotStr[Cognite3DObjectTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite 3D objects

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_x_max: The minimum value of the x max to filter on.
            max_x_max: The maximum value of the x max to filter on.
            min_x_min: The minimum value of the x min to filter on.
            max_x_min: The maximum value of the x min to filter on.
            min_y_max: The minimum value of the y max to filter on.
            max_y_max: The maximum value of the y max to filter on.
            min_y_min: The minimum value of the y min to filter on.
            max_y_min: The maximum value of the y min to filter on.
            min_z_max: The minimum value of the z max to filter on.
            max_z_max: The maximum value of the z max to filter on.
            min_z_min: The minimum value of the z min to filter on.
            max_z_min: The maximum value of the z min to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D objects to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite 3D objects in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_3_d_object.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_3_d_object_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            min_x_max,
            max_x_max,
            min_x_min,
            max_x_min,
            min_y_max,
            max_y_max,
            min_y_min,
            max_y_min,
            min_z_max,
            max_z_max,
            min_z_min,
            max_z_min,
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
        property: Cognite3DObjectFields,
        interval: float,
        query: str | None = None,
        search_property: Cognite3DObjectTextFields | SequenceNotStr[Cognite3DObjectTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite 3D objects

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_x_max: The minimum value of the x max to filter on.
            max_x_max: The maximum value of the x max to filter on.
            min_x_min: The minimum value of the x min to filter on.
            max_x_min: The maximum value of the x min to filter on.
            min_y_max: The minimum value of the y max to filter on.
            max_y_max: The maximum value of the y max to filter on.
            min_y_min: The minimum value of the y min to filter on.
            max_y_min: The maximum value of the y min to filter on.
            min_z_max: The minimum value of the z max to filter on.
            max_z_max: The maximum value of the z max to filter on.
            min_z_min: The minimum value of the z min to filter on.
            max_z_min: The maximum value of the z min to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D objects to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_3_d_object_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            min_x_max,
            max_x_max,
            min_x_min,
            max_x_min,
            min_y_max,
            max_y_max,
            min_y_min,
            max_y_min,
            min_z_max,
            max_z_max,
            min_z_min,
            max_z_min,
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

    def select(self) -> Cognite3DObjectQuery:
        """Start selecting from Cognite 3D objects."""
        return Cognite3DObjectQuery(self._client)

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
        if retrieve_connections == "identifier" or retrieve_connections == "full":
            builder.extend(
                factory.from_edge(
                    Cognite360Image._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "images360"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                    edge_view=Cognite360ImageAnnotation._view_id,
                )
            )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_reverse_relation(
                    CogniteAsset._view_id,
                    through=dm.PropertyId(dm.ViewId("cdf_cdm", "CogniteAsset", "v1"), "object3D"),
                    connection_type=None,
                    connection_property=ViewPropertyId(self._view_id, "asset"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_reverse_relation(
                    CogniteCADNode._view_id,
                    through=dm.PropertyId(dm.ViewId("cdf_cdm", "CogniteCADNode", "v1"), "object3D"),
                    connection_type=None,
                    connection_property=ViewPropertyId(self._view_id, "cadNodes"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_reverse_relation(
                    CognitePointCloudVolume._view_id,
                    through=dm.PropertyId(dm.ViewId("cdf_cdm", "CognitePointCloudVolume", "v1"), "object3D"),
                    connection_type=None,
                    connection_property=ViewPropertyId(self._view_id, "pointCloudVolumes"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[Cognite3DObjectList]:
        """Iterate over Cognite 3D objects

        Args:
            chunk_size: The number of Cognite 3D objects to return in each iteration. Defaults to 100.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_x_max: The minimum value of the x max to filter on.
            max_x_max: The maximum value of the x max to filter on.
            min_x_min: The minimum value of the x min to filter on.
            max_x_min: The maximum value of the x min to filter on.
            min_y_max: The minimum value of the y max to filter on.
            max_y_max: The maximum value of the y max to filter on.
            min_y_min: The minimum value of the y min to filter on.
            max_y_min: The maximum value of the y min to filter on.
            min_z_max: The minimum value of the z max to filter on.
            max_z_max: The maximum value of the z max to filter on.
            min_z_min: The minimum value of the z min to filter on.
            max_z_min: The maximum value of the z min to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `asset`, `cad_nodes`, `images_360` and `point_cloud_volumes` for
            the Cognite 3D objects. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only
            retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of Cognite 3D objects to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of Cognite 3D objects

        Examples:

            Iterate Cognite 3D objects in chunks of 100 up to 2000 items:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_3_d_objects in client.cognite_3_d_object.iterate(chunk_size=100, limit=2000):
                ...     for cognite_3_d_object in cognite_3_d_objects:
                ...         print(cognite_3_d_object.external_id)

            Iterate Cognite 3D objects in chunks of 100 sorted by external_id in descending order:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_3_d_objects in client.cognite_3_d_object.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for cognite_3_d_object in cognite_3_d_objects:
                ...         print(cognite_3_d_object.external_id)

            Iterate Cognite 3D objects in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for first_iteration in client.cognite_3_d_object.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for cognite_3_d_objects in client.cognite_3_d_object.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for cognite_3_d_object in cognite_3_d_objects:
                ...         print(cognite_3_d_object.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_cognite_3_d_object_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            min_x_max,
            max_x_max,
            min_x_min,
            max_x_min,
            min_y_max,
            max_y_max,
            min_y_min,
            max_y_min,
            min_z_max,
            max_z_max,
            min_z_min,
            max_z_min,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_x_max: float | None = None,
        max_x_max: float | None = None,
        min_x_min: float | None = None,
        max_x_min: float | None = None,
        min_y_max: float | None = None,
        max_y_max: float | None = None,
        min_y_min: float | None = None,
        max_y_min: float | None = None,
        min_z_max: float | None = None,
        max_z_max: float | None = None,
        min_z_min: float | None = None,
        max_z_min: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Cognite3DObjectFields | Sequence[Cognite3DObjectFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> Cognite3DObjectList:
        """List/filter Cognite 3D objects

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_x_max: The minimum value of the x max to filter on.
            max_x_max: The maximum value of the x max to filter on.
            min_x_min: The minimum value of the x min to filter on.
            max_x_min: The maximum value of the x min to filter on.
            min_y_max: The minimum value of the y max to filter on.
            max_y_max: The maximum value of the y max to filter on.
            min_y_min: The minimum value of the y min to filter on.
            max_y_min: The maximum value of the y min to filter on.
            min_z_max: The minimum value of the z max to filter on.
            max_z_max: The maximum value of the z max to filter on.
            min_z_min: The minimum value of the z min to filter on.
            max_z_min: The maximum value of the z min to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D objects to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `asset`, `cad_nodes`, `images_360` and `point_cloud_volumes` for
            the Cognite 3D objects. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only
            retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested Cognite 3D objects

        Examples:

            List Cognite 3D objects and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_objects = client.cognite_3_d_object.list(limit=5)

        """
        filter_ = _create_cognite_3_d_object_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            min_x_max,
            max_x_max,
            min_x_min,
            max_x_min,
            min_y_max,
            max_y_max,
            min_y_min,
            max_y_min,
            min_z_max,
            max_z_max,
            min_z_min,
            max_z_min,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit, filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
