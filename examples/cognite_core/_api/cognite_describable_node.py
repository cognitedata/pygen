from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
from cognite_core.data_classes import (
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
from cognite_core.data_classes._cognite_describable_node import (
    CogniteDescribableNodeQuery,
    _COGNITEDESCRIBABLENODE_PROPERTIES_BY_FIELD,
    _create_cognite_describable_node_filter,
)
from cognite_core._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite_core._api.cognite_describable_node_query import CogniteDescribableNodeQueryAPI


class CogniteDescribableNodeAPI(
    NodeAPI[
        CogniteDescribableNode, CogniteDescribableNodeWrite, CogniteDescribableNodeList, CogniteDescribableNodeWriteList
    ]
):
    _view_id = dm.ViewId("cdf_cdm", "CogniteDescribable", "v1")
    _properties_by_field = _COGNITEDESCRIBABLENODE_PROPERTIES_BY_FIELD
    _direct_children_by_external_id = {
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

    def __call__(
        self,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CogniteDescribableNodeQueryAPI[CogniteDescribableNodeList]:
        """Query starting at Cognite describable nodes.

        Args:
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite describable nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite describable nodes.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cognite_describable_node_filter(
            self._view_id,
            description,
            description_prefix,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(CogniteDescribableNodeList)
        return CogniteDescribableNodeQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        cognite_describable_node: CogniteDescribableNodeWrite | Sequence[CogniteDescribableNodeWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite describable nodes.

        Args:
            cognite_describable_node: Cognite describable node or sequence of Cognite describable nodes to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new cognite_describable_node:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import CogniteDescribableNodeWrite
                >>> client = CogniteCoreClient()
                >>> cognite_describable_node = CogniteDescribableNodeWrite(external_id="my_cognite_describable_node", ...)
                >>> result = client.cognite_describable_node.apply(cognite_describable_node)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_describable_node.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_describable_node, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite describable node.

        Args:
            external_id: External id of the Cognite describable node to delete.
            space: The space where all the Cognite describable node are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_describable_node by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_describable_node.delete("my_cognite_describable_node")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_describable_node.delete(my_ids)` please use `my_client.delete(my_ids)`."
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
                >>> cognite_describable_node = client.cognite_describable_node.retrieve("my_cognite_describable_node")

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
            limit: Maximum number of Cognite describable nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite describable nodes matching the query.

        Examples:

           Search for 'my_cognite_describable_node' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_describable_nodes = client.cognite_describable_node.search('my_cognite_describable_node')

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
            limit: Maximum number of Cognite describable nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            limit: Maximum number of Cognite describable nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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

    def query(self) -> CogniteDescribableNodeQuery:
        """Start a query for Cognite describable nodes."""
        warnings.warn("The .query is in alpha and is subject to breaking changes without notice.")
        return CogniteDescribableNodeQuery(self._client)

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
            limit: Maximum number of Cognite describable nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
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

        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )
