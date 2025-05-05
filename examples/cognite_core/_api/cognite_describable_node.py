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
from cognite_core.data_classes._cognite_describable_node import (
    CogniteDescribableNodeQuery,
    _COGNITEDESCRIBABLENODE_PROPERTIES_BY_FIELD,
    _create_cognite_describable_node_filter,
)
from cognite_core.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    CogniteDescribableNode,
    CogniteDescribableNodeWrite,
    CogniteDescribableNodeFields,
    CogniteDescribableNodeList,
    CogniteDescribableNodeWriteList,
    CogniteDescribableNodeTextFields,
    Cognite360ImageCollection,
    Cognite360ImageModel,
    Cognite360ImageStation,
    Cognite3DModel,
    Cognite3DObject,
    CogniteActivity,
    CogniteAsset,
    CogniteAssetClass,
    CogniteAssetType,
    CogniteCADModel,
    CogniteCADNode,
    CogniteEquipment,
    CogniteEquipmentType,
    CogniteFile,
    CogniteFileCategory,
    CognitePointCloudModel,
    CognitePointCloudVolume,
    CogniteSourceSystem,
    CogniteTimeSeries,
    CogniteUnit,
)


class CogniteDescribableNodeAPI(
    NodeAPI[
        CogniteDescribableNode, CogniteDescribableNodeWrite, CogniteDescribableNodeList, CogniteDescribableNodeWriteList
    ]
):
    _view_id = dm.ViewId("cdf_cdm", "CogniteDescribable", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITEDESCRIBABLENODE_PROPERTIES_BY_FIELD
    _direct_children_by_external_id: ClassVar[dict[str, type[DomainModel]]] = {
        "Cognite360ImageCollection": Cognite360ImageCollection,
        "Cognite360ImageModel": Cognite360ImageModel,
        "Cognite360ImageStation": Cognite360ImageStation,
        "Cognite3DModel": Cognite3DModel,
        "Cognite3DObject": Cognite3DObject,
        "CogniteActivity": CogniteActivity,
        "CogniteAsset": CogniteAsset,
        "CogniteAssetClass": CogniteAssetClass,
        "CogniteAssetType": CogniteAssetType,
        "CogniteCADModel": CogniteCADModel,
        "CogniteCADNode": CogniteCADNode,
        "CogniteEquipment": CogniteEquipment,
        "CogniteEquipmentType": CogniteEquipmentType,
        "CogniteFile": CogniteFile,
        "CogniteFileCategory": CogniteFileCategory,
        "CognitePointCloudModel": CognitePointCloudModel,
        "CognitePointCloudVolume": CognitePointCloudVolume,
        "CogniteSourceSystem": CogniteSourceSystem,
        "CogniteTimeSeries": CogniteTimeSeries,
        "CogniteUnit": CogniteUnit,
    }
    _class_type = CogniteDescribableNode
    _class_list = CogniteDescribableNodeList
    _class_write_list = CogniteDescribableNodeWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: (
            SequenceNotStr[
                Literal[
                    "Cognite360ImageCollection",
                    "Cognite360ImageModel",
                    "Cognite360ImageStation",
                    "Cognite3DModel",
                    "Cognite3DObject",
                    "CogniteActivity",
                    "CogniteAsset",
                    "CogniteAssetClass",
                    "CogniteAssetType",
                    "CogniteCADModel",
                    "CogniteCADNode",
                    "CogniteEquipment",
                    "CogniteEquipmentType",
                    "CogniteFile",
                    "CogniteFileCategory",
                    "CognitePointCloudModel",
                    "CognitePointCloudVolume",
                    "CogniteSourceSystem",
                    "CogniteTimeSeries",
                    "CogniteUnit",
                ]
            ]
            | None
        ) = None,
    ) -> CogniteDescribableNode | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: (
            SequenceNotStr[
                Literal[
                    "Cognite360ImageCollection",
                    "Cognite360ImageModel",
                    "Cognite360ImageStation",
                    "Cognite3DModel",
                    "Cognite3DObject",
                    "CogniteActivity",
                    "CogniteAsset",
                    "CogniteAssetClass",
                    "CogniteAssetType",
                    "CogniteCADModel",
                    "CogniteCADNode",
                    "CogniteEquipment",
                    "CogniteEquipmentType",
                    "CogniteFile",
                    "CogniteFileCategory",
                    "CognitePointCloudModel",
                    "CognitePointCloudVolume",
                    "CogniteSourceSystem",
                    "CogniteTimeSeries",
                    "CogniteUnit",
                ]
            ]
            | None
        ) = None,
    ) -> CogniteDescribableNodeList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: (
            SequenceNotStr[
                Literal[
                    "Cognite360ImageCollection",
                    "Cognite360ImageModel",
                    "Cognite360ImageStation",
                    "Cognite3DModel",
                    "Cognite3DObject",
                    "CogniteActivity",
                    "CogniteAsset",
                    "CogniteAssetClass",
                    "CogniteAssetType",
                    "CogniteCADModel",
                    "CogniteCADNode",
                    "CogniteEquipment",
                    "CogniteEquipmentType",
                    "CogniteFile",
                    "CogniteFileCategory",
                    "CognitePointCloudModel",
                    "CognitePointCloudVolume",
                    "CogniteSourceSystem",
                    "CogniteTimeSeries",
                    "CogniteUnit",
                ]
            ]
            | None
        ) = None,
    ) -> CogniteDescribableNode | CogniteDescribableNodeList | None:
        """Retrieve one or more Cognite describable nodes by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite describable nodes.
            space: The space where all the Cognite describable nodes are located.
            as_child_class: If you want to retrieve the Cognite describable nodes as a child class,
                you can specify the child class here. Note that if one node has properties in
                multiple child classes, you will get duplicate nodes in the result.

        Returns:
            The requested Cognite describable nodes.

        Examples:

            Retrieve cognite_describable_node by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_describable_node = client.cognite_describable_node.retrieve(
                ...     "my_cognite_describable_node"
                ... )

        """
        return self._retrieve(external_id, space, as_child_class=as_child_class)

    def search(
        self,
        query: str,
        properties: CogniteDescribableNodeTextFields | SequenceNotStr[CogniteDescribableNodeTextFields] | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteDescribableNodeFields | SequenceNotStr[CogniteDescribableNodeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteDescribableNodeList:
        """Search Cognite describable nodes

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite describable nodes to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite describable nodes matching the query.

        Examples:

           Search for 'my_cognite_describable_node' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_describable_nodes = client.cognite_describable_node.search(
                ...     'my_cognite_describable_node'
                ... )

        """
        filter_ = _create_cognite_describable_node_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
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
        property: CogniteDescribableNodeFields | SequenceNotStr[CogniteDescribableNodeFields] | None = None,
        query: str | None = None,
        search_property: (
            CogniteDescribableNodeTextFields | SequenceNotStr[CogniteDescribableNodeTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: CogniteDescribableNodeFields | SequenceNotStr[CogniteDescribableNodeFields] | None = None,
        query: str | None = None,
        search_property: (
            CogniteDescribableNodeTextFields | SequenceNotStr[CogniteDescribableNodeTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        group_by: CogniteDescribableNodeFields | SequenceNotStr[CogniteDescribableNodeFields],
        property: CogniteDescribableNodeFields | SequenceNotStr[CogniteDescribableNodeFields] | None = None,
        query: str | None = None,
        search_property: (
            CogniteDescribableNodeTextFields | SequenceNotStr[CogniteDescribableNodeTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        group_by: CogniteDescribableNodeFields | SequenceNotStr[CogniteDescribableNodeFields] | None = None,
        property: CogniteDescribableNodeFields | SequenceNotStr[CogniteDescribableNodeFields] | None = None,
        query: str | None = None,
        search_property: (
            CogniteDescribableNodeTextFields | SequenceNotStr[CogniteDescribableNodeTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across Cognite describable nodes

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite describable nodes to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite describable nodes in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_describable_node.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_describable_node_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
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
        property: CogniteDescribableNodeFields,
        interval: float,
        query: str | None = None,
        search_property: (
            CogniteDescribableNodeTextFields | SequenceNotStr[CogniteDescribableNodeTextFields] | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for Cognite describable nodes

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite describable nodes to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_describable_node_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
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

    def select(self) -> CogniteDescribableNodeQuery:
        """Start selecting from Cognite describable nodes."""
        return CogniteDescribableNodeQuery(self._client)

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
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[CogniteDescribableNodeList]:
        """Iterate over Cognite describable nodes

        Args:
            chunk_size: The number of Cognite describable nodes to return in each iteration. Defaults to 100.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of Cognite describable nodes to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of Cognite describable nodes

        Examples:

            Iterate Cognite describable nodes in chunks of 100 up to 2000 items:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_describable_nodes in client.cognite_describable_node.iterate(chunk_size=100, limit=2000):
                ...     for cognite_describable_node in cognite_describable_nodes:
                ...         print(cognite_describable_node.external_id)

            Iterate Cognite describable nodes in chunks of 100 sorted by external_id in descending order:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for cognite_describable_nodes in client.cognite_describable_node.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for cognite_describable_node in cognite_describable_nodes:
                ...         print(cognite_describable_node.external_id)

            Iterate Cognite describable nodes in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> for first_iteration in client.cognite_describable_node.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for cognite_describable_nodes in client.cognite_describable_node.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for cognite_describable_node in cognite_describable_nodes:
                ...         print(cognite_describable_node.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_cognite_describable_node_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

    def list(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: CogniteDescribableNodeFields | Sequence[CogniteDescribableNodeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteDescribableNodeList:
        """List/filter Cognite describable nodes

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite describable nodes to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested Cognite describable nodes

        Examples:

            List Cognite describable nodes and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_describable_nodes = client.cognite_describable_node.list(limit=5)

        """
        filter_ = _create_cognite_describable_node_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input = self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit, filter=filter_, sort=sort_input)
