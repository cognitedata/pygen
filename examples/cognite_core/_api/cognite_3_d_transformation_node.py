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
from cognite_core.data_classes._cognite_3_d_transformation_node import (
    Cognite3DTransformationNodeQuery,
    _COGNITE3DTRANSFORMATIONNODE_PROPERTIES_BY_FIELD,
    _create_cognite_3_d_transformation_node_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Cognite3DTransformationNode,
    Cognite3DTransformationNodeWrite,
    Cognite3DTransformationNodeFields,
    Cognite3DTransformationNodeList,
    Cognite3DTransformationNodeWriteList,
    Cognite3DTransformationNodeTextFields,
    Cognite360Image,
)


class Cognite3DTransformationNodeAPI(
    NodeAPI[
        Cognite3DTransformationNode,
        Cognite3DTransformationNodeWrite,
        Cognite3DTransformationNodeList,
        Cognite3DTransformationNodeWriteList,
    ]
):
    _view_id = dm.ViewId("cdf_cdm", "Cognite3DTransformation", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITE3DTRANSFORMATIONNODE_PROPERTIES_BY_FIELD
    _direct_children_by_external_id: ClassVar[dict[str, type[DomainModel]]] = {
        "Cognite360Image": Cognite360Image,
    }
    _class_type = Cognite3DTransformationNode
    _class_list = Cognite3DTransformationNodeList
    _class_write_list = Cognite3DTransformationNodeWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["Cognite360Image"]] | None = None,
    ) -> Cognite3DTransformationNode | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["Cognite360Image"]] | None = None,
    ) -> Cognite3DTransformationNodeList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["Cognite360Image"]] | None = None,
    ) -> Cognite3DTransformationNode | Cognite3DTransformationNodeList | None:
        """Retrieve one or more Cognite 3D transformation nodes by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite 3D transformation nodes.
            space: The space where all the Cognite 3D transformation nodes are located.
            as_child_class: If you want to retrieve the Cognite 3D transformation nodes as a child class,
                you can specify the child class here. Note that if one node has properties in
                multiple child classes, you will get duplicate nodes in the result.

        Returns:
            The requested Cognite 3D transformation nodes.

        Examples:

            Retrieve cognite_3_d_transformation_node by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_transformation_node = client.cognite_3_d_transformation_node.retrieve(
                ...     "my_cognite_3_d_transformation_node"
                ... )

        """
        return self._retrieve(external_id, space, as_child_class=as_child_class)

    def search(
        self,
        query: str,
        properties: (
            Cognite3DTransformationNodeTextFields | SequenceNotStr[Cognite3DTransformationNodeTextFields] | None
        ) = None,
        min_euler_rotation_x: float | None = None,
        max_euler_rotation_x: float | None = None,
        min_euler_rotation_y: float | None = None,
        max_euler_rotation_y: float | None = None,
        min_euler_rotation_z: float | None = None,
        max_euler_rotation_z: float | None = None,
        min_scale_x: float | None = None,
        max_scale_x: float | None = None,
        min_scale_y: float | None = None,
        max_scale_y: float | None = None,
        min_scale_z: float | None = None,
        max_scale_z: float | None = None,
        min_translation_x: float | None = None,
        max_translation_x: float | None = None,
        min_translation_y: float | None = None,
        max_translation_y: float | None = None,
        min_translation_z: float | None = None,
        max_translation_z: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Cognite3DTransformationNodeFields | SequenceNotStr[Cognite3DTransformationNodeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Cognite3DTransformationNodeList:
        """Search Cognite 3D transformation nodes

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_euler_rotation_x: The minimum value of the euler rotation x to filter on.
            max_euler_rotation_x: The maximum value of the euler rotation x to filter on.
            min_euler_rotation_y: The minimum value of the euler rotation y to filter on.
            max_euler_rotation_y: The maximum value of the euler rotation y to filter on.
            min_euler_rotation_z: The minimum value of the euler rotation z to filter on.
            max_euler_rotation_z: The maximum value of the euler rotation z to filter on.
            min_scale_x: The minimum value of the scale x to filter on.
            max_scale_x: The maximum value of the scale x to filter on.
            min_scale_y: The minimum value of the scale y to filter on.
            max_scale_y: The maximum value of the scale y to filter on.
            min_scale_z: The minimum value of the scale z to filter on.
            max_scale_z: The maximum value of the scale z to filter on.
            min_translation_x: The minimum value of the translation x to filter on.
            max_translation_x: The maximum value of the translation x to filter on.
            min_translation_y: The minimum value of the translation y to filter on.
            max_translation_y: The maximum value of the translation y to filter on.
            min_translation_z: The minimum value of the translation z to filter on.
            max_translation_z: The maximum value of the translation z to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D transformation nodes to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite 3D transformation nodes matching the query.

        Examples:

           Search for 'my_cognite_3_d_transformation_node' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_transformation_nodes = client.cognite_3_d_transformation_node.search(
                ...     'my_cognite_3_d_transformation_node'
                ... )

        """
        filter_ = _create_cognite_3_d_transformation_node_filter(
            self._view_id,
            min_euler_rotation_x,
            max_euler_rotation_x,
            min_euler_rotation_y,
            max_euler_rotation_y,
            min_euler_rotation_z,
            max_euler_rotation_z,
            min_scale_x,
            max_scale_x,
            min_scale_y,
            max_scale_y,
            min_scale_z,
            max_scale_z,
            min_translation_x,
            max_translation_x,
            min_translation_y,
            max_translation_y,
            min_translation_z,
            max_translation_z,
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
        property: Cognite3DTransformationNodeFields | SequenceNotStr[Cognite3DTransformationNodeFields] | None = None,
        min_euler_rotation_x: float | None = None,
        max_euler_rotation_x: float | None = None,
        min_euler_rotation_y: float | None = None,
        max_euler_rotation_y: float | None = None,
        min_euler_rotation_z: float | None = None,
        max_euler_rotation_z: float | None = None,
        min_scale_x: float | None = None,
        max_scale_x: float | None = None,
        min_scale_y: float | None = None,
        max_scale_y: float | None = None,
        min_scale_z: float | None = None,
        max_scale_z: float | None = None,
        min_translation_x: float | None = None,
        max_translation_x: float | None = None,
        min_translation_y: float | None = None,
        max_translation_y: float | None = None,
        min_translation_z: float | None = None,
        max_translation_z: float | None = None,
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
        property: Cognite3DTransformationNodeFields | SequenceNotStr[Cognite3DTransformationNodeFields] | None = None,
        min_euler_rotation_x: float | None = None,
        max_euler_rotation_x: float | None = None,
        min_euler_rotation_y: float | None = None,
        max_euler_rotation_y: float | None = None,
        min_euler_rotation_z: float | None = None,
        max_euler_rotation_z: float | None = None,
        min_scale_x: float | None = None,
        max_scale_x: float | None = None,
        min_scale_y: float | None = None,
        max_scale_y: float | None = None,
        min_scale_z: float | None = None,
        max_scale_z: float | None = None,
        min_translation_x: float | None = None,
        max_translation_x: float | None = None,
        min_translation_y: float | None = None,
        max_translation_y: float | None = None,
        min_translation_z: float | None = None,
        max_translation_z: float | None = None,
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
        group_by: Cognite3DTransformationNodeFields | SequenceNotStr[Cognite3DTransformationNodeFields],
        property: Cognite3DTransformationNodeFields | SequenceNotStr[Cognite3DTransformationNodeFields] | None = None,
        min_euler_rotation_x: float | None = None,
        max_euler_rotation_x: float | None = None,
        min_euler_rotation_y: float | None = None,
        max_euler_rotation_y: float | None = None,
        min_euler_rotation_z: float | None = None,
        max_euler_rotation_z: float | None = None,
        min_scale_x: float | None = None,
        max_scale_x: float | None = None,
        min_scale_y: float | None = None,
        max_scale_y: float | None = None,
        min_scale_z: float | None = None,
        max_scale_z: float | None = None,
        min_translation_x: float | None = None,
        max_translation_x: float | None = None,
        min_translation_y: float | None = None,
        max_translation_y: float | None = None,
        min_translation_z: float | None = None,
        max_translation_z: float | None = None,
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
        group_by: Cognite3DTransformationNodeFields | SequenceNotStr[Cognite3DTransformationNodeFields] | None = None,
        property: Cognite3DTransformationNodeFields | SequenceNotStr[Cognite3DTransformationNodeFields] | None = None,
        min_euler_rotation_x: float | None = None,
        max_euler_rotation_x: float | None = None,
        min_euler_rotation_y: float | None = None,
        max_euler_rotation_y: float | None = None,
        min_euler_rotation_z: float | None = None,
        max_euler_rotation_z: float | None = None,
        min_scale_x: float | None = None,
        max_scale_x: float | None = None,
        min_scale_y: float | None = None,
        max_scale_y: float | None = None,
        min_scale_z: float | None = None,
        max_scale_z: float | None = None,
        min_translation_x: float | None = None,
        max_translation_x: float | None = None,
        min_translation_y: float | None = None,
        max_translation_y: float | None = None,
        min_translation_z: float | None = None,
        max_translation_z: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite 3D transformation nodes

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            min_euler_rotation_x: The minimum value of the euler rotation x to filter on.
            max_euler_rotation_x: The maximum value of the euler rotation x to filter on.
            min_euler_rotation_y: The minimum value of the euler rotation y to filter on.
            max_euler_rotation_y: The maximum value of the euler rotation y to filter on.
            min_euler_rotation_z: The minimum value of the euler rotation z to filter on.
            max_euler_rotation_z: The maximum value of the euler rotation z to filter on.
            min_scale_x: The minimum value of the scale x to filter on.
            max_scale_x: The maximum value of the scale x to filter on.
            min_scale_y: The minimum value of the scale y to filter on.
            max_scale_y: The maximum value of the scale y to filter on.
            min_scale_z: The minimum value of the scale z to filter on.
            max_scale_z: The maximum value of the scale z to filter on.
            min_translation_x: The minimum value of the translation x to filter on.
            max_translation_x: The maximum value of the translation x to filter on.
            min_translation_y: The minimum value of the translation y to filter on.
            max_translation_y: The maximum value of the translation y to filter on.
            min_translation_z: The minimum value of the translation z to filter on.
            max_translation_z: The maximum value of the translation z to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D transformation nodes to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite 3D transformation nodes in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_3_d_transformation_node.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_3_d_transformation_node_filter(
            self._view_id,
            min_euler_rotation_x,
            max_euler_rotation_x,
            min_euler_rotation_y,
            max_euler_rotation_y,
            min_euler_rotation_z,
            max_euler_rotation_z,
            min_scale_x,
            max_scale_x,
            min_scale_y,
            max_scale_y,
            min_scale_z,
            max_scale_z,
            min_translation_x,
            max_translation_x,
            min_translation_y,
            max_translation_y,
            min_translation_z,
            max_translation_z,
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
        property: Cognite3DTransformationNodeFields,
        interval: float,
        min_euler_rotation_x: float | None = None,
        max_euler_rotation_x: float | None = None,
        min_euler_rotation_y: float | None = None,
        max_euler_rotation_y: float | None = None,
        min_euler_rotation_z: float | None = None,
        max_euler_rotation_z: float | None = None,
        min_scale_x: float | None = None,
        max_scale_x: float | None = None,
        min_scale_y: float | None = None,
        max_scale_y: float | None = None,
        min_scale_z: float | None = None,
        max_scale_z: float | None = None,
        min_translation_x: float | None = None,
        max_translation_x: float | None = None,
        min_translation_y: float | None = None,
        max_translation_y: float | None = None,
        min_translation_z: float | None = None,
        max_translation_z: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite 3D transformation nodes

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            min_euler_rotation_x: The minimum value of the euler rotation x to filter on.
            max_euler_rotation_x: The maximum value of the euler rotation x to filter on.
            min_euler_rotation_y: The minimum value of the euler rotation y to filter on.
            max_euler_rotation_y: The maximum value of the euler rotation y to filter on.
            min_euler_rotation_z: The minimum value of the euler rotation z to filter on.
            max_euler_rotation_z: The maximum value of the euler rotation z to filter on.
            min_scale_x: The minimum value of the scale x to filter on.
            max_scale_x: The maximum value of the scale x to filter on.
            min_scale_y: The minimum value of the scale y to filter on.
            max_scale_y: The maximum value of the scale y to filter on.
            min_scale_z: The minimum value of the scale z to filter on.
            max_scale_z: The maximum value of the scale z to filter on.
            min_translation_x: The minimum value of the translation x to filter on.
            max_translation_x: The maximum value of the translation x to filter on.
            min_translation_y: The minimum value of the translation y to filter on.
            max_translation_y: The maximum value of the translation y to filter on.
            min_translation_z: The minimum value of the translation z to filter on.
            max_translation_z: The maximum value of the translation z to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D transformation nodes to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_3_d_transformation_node_filter(
            self._view_id,
            min_euler_rotation_x,
            max_euler_rotation_x,
            min_euler_rotation_y,
            max_euler_rotation_y,
            min_euler_rotation_z,
            max_euler_rotation_z,
            min_scale_x,
            max_scale_x,
            min_scale_y,
            max_scale_y,
            min_scale_z,
            max_scale_z,
            min_translation_x,
            max_translation_x,
            min_translation_y,
            max_translation_y,
            min_translation_z,
            max_translation_z,
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

    def select(self) -> Cognite3DTransformationNodeQuery:
        """Start selecting from Cognite 3D transformation nodes."""
        return Cognite3DTransformationNodeQuery(self._client)

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
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        min_euler_rotation_x: float | None = None,
        max_euler_rotation_x: float | None = None,
        min_euler_rotation_y: float | None = None,
        max_euler_rotation_y: float | None = None,
        min_euler_rotation_z: float | None = None,
        max_euler_rotation_z: float | None = None,
        min_scale_x: float | None = None,
        max_scale_x: float | None = None,
        min_scale_y: float | None = None,
        max_scale_y: float | None = None,
        min_scale_z: float | None = None,
        max_scale_z: float | None = None,
        min_translation_x: float | None = None,
        max_translation_x: float | None = None,
        min_translation_y: float | None = None,
        max_translation_y: float | None = None,
        min_translation_z: float | None = None,
        max_translation_z: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[Cognite3DTransformationNodeList]:
        """Iterate over Cognite 3D transformation nodes

        Args:
            chunk_size: The number of Cognite 3D transformation nodes to return in each iteration. Defaults to 100.
            min_euler_rotation_x: The minimum value of the euler rotation x to filter on.
            max_euler_rotation_x: The maximum value of the euler rotation x to filter on.
            min_euler_rotation_y: The minimum value of the euler rotation y to filter on.
            max_euler_rotation_y: The maximum value of the euler rotation y to filter on.
            min_euler_rotation_z: The minimum value of the euler rotation z to filter on.
            max_euler_rotation_z: The maximum value of the euler rotation z to filter on.
            min_scale_x: The minimum value of the scale x to filter on.
            max_scale_x: The maximum value of the scale x to filter on.
            min_scale_y: The minimum value of the scale y to filter on.
            max_scale_y: The maximum value of the scale y to filter on.
            min_scale_z: The minimum value of the scale z to filter on.
            max_scale_z: The maximum value of the scale z to filter on.
            min_translation_x: The minimum value of the translation x to filter on.
            max_translation_x: The maximum value of the translation x to filter on.
            min_translation_y: The minimum value of the translation y to filter on.
            max_translation_y: The maximum value of the translation y to filter on.
            min_translation_z: The minimum value of the translation z to filter on.
            max_translation_z: The maximum value of the translation z to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of Cognite 3D transformation nodes to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of Cognite 3D transformation nodes

        Examples:

            Iterate Cognite 3D transformation nodes in chunks of 100 up to 2000 items:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_3_d_transformation_nodes in client.cognite_3_d_transformation_node.iterate(chunk_size=100, limit=2000):
                ...     for cognite_3_d_transformation_node in cognite_3_d_transformation_nodes:
                ...         print(cognite_3_d_transformation_node.external_id)

            Iterate Cognite 3D transformation nodes in chunks of 100 sorted by external_id in descending order:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_3_d_transformation_nodes in client.cognite_3_d_transformation_node.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for cognite_3_d_transformation_node in cognite_3_d_transformation_nodes:
                ...         print(cognite_3_d_transformation_node.external_id)

            Iterate Cognite 3D transformation nodes in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for first_iteration in client.cognite_3_d_transformation_node.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for cognite_3_d_transformation_nodes in client.cognite_3_d_transformation_node.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for cognite_3_d_transformation_node in cognite_3_d_transformation_nodes:
                ...         print(cognite_3_d_transformation_node.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_cognite_3_d_transformation_node_filter(
            self._view_id,
            min_euler_rotation_x,
            max_euler_rotation_x,
            min_euler_rotation_y,
            max_euler_rotation_y,
            min_euler_rotation_z,
            max_euler_rotation_z,
            min_scale_x,
            max_scale_x,
            min_scale_y,
            max_scale_y,
            min_scale_z,
            max_scale_z,
            min_translation_x,
            max_translation_x,
            min_translation_y,
            max_translation_y,
            min_translation_z,
            max_translation_z,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

    def list(
        self,
        min_euler_rotation_x: float | None = None,
        max_euler_rotation_x: float | None = None,
        min_euler_rotation_y: float | None = None,
        max_euler_rotation_y: float | None = None,
        min_euler_rotation_z: float | None = None,
        max_euler_rotation_z: float | None = None,
        min_scale_x: float | None = None,
        max_scale_x: float | None = None,
        min_scale_y: float | None = None,
        max_scale_y: float | None = None,
        min_scale_z: float | None = None,
        max_scale_z: float | None = None,
        min_translation_x: float | None = None,
        max_translation_x: float | None = None,
        min_translation_y: float | None = None,
        max_translation_y: float | None = None,
        min_translation_z: float | None = None,
        max_translation_z: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: Cognite3DTransformationNodeFields | Sequence[Cognite3DTransformationNodeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> Cognite3DTransformationNodeList:
        """List/filter Cognite 3D transformation nodes

        Args:
            min_euler_rotation_x: The minimum value of the euler rotation x to filter on.
            max_euler_rotation_x: The maximum value of the euler rotation x to filter on.
            min_euler_rotation_y: The minimum value of the euler rotation y to filter on.
            max_euler_rotation_y: The maximum value of the euler rotation y to filter on.
            min_euler_rotation_z: The minimum value of the euler rotation z to filter on.
            max_euler_rotation_z: The maximum value of the euler rotation z to filter on.
            min_scale_x: The minimum value of the scale x to filter on.
            max_scale_x: The maximum value of the scale x to filter on.
            min_scale_y: The minimum value of the scale y to filter on.
            max_scale_y: The maximum value of the scale y to filter on.
            min_scale_z: The minimum value of the scale z to filter on.
            max_scale_z: The maximum value of the scale z to filter on.
            min_translation_x: The minimum value of the translation x to filter on.
            max_translation_x: The maximum value of the translation x to filter on.
            min_translation_y: The minimum value of the translation y to filter on.
            max_translation_y: The maximum value of the translation y to filter on.
            min_translation_z: The minimum value of the translation z to filter on.
            max_translation_z: The maximum value of the translation z to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite 3D transformation nodes to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested Cognite 3D transformation nodes

        Examples:

            List Cognite 3D transformation nodes and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_3_d_transformation_nodes = client.cognite_3_d_transformation_node.list(limit=5)

        """
        filter_ = _create_cognite_3_d_transformation_node_filter(
            self._view_id,
            min_euler_rotation_x,
            max_euler_rotation_x,
            min_euler_rotation_y,
            max_euler_rotation_y,
            min_euler_rotation_z,
            max_euler_rotation_z,
            min_scale_x,
            max_scale_x,
            min_scale_y,
            max_scale_y,
            min_scale_z,
            max_scale_z,
            min_translation_x,
            max_translation_x,
            min_translation_y,
            max_translation_y,
            min_translation_z,
            max_translation_z,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
