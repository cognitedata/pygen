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
from cognite_core._api.cognite_cad_node_query import CogniteCADNodeQueryAPI
from cognite_core.data_classes import (
    Cognite3DObject,
    CogniteCADModel,
    CogniteCADNode,
    CogniteCADNodeFields,
    CogniteCADNodeList,
    CogniteCADNodeTextFields,
    CogniteCADNodeWrite,
    CogniteCADNodeWriteList,
    CogniteCADRevision,
    ResourcesWriteResult,
)
from cognite_core.data_classes._cognite_cad_node import (
    _COGNITECADNODE_PROPERTIES_BY_FIELD,
    CogniteCADNodeQuery,
    _create_cognite_cad_node_filter,
)
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
)


class CogniteCADNodeAPI(NodeAPI[CogniteCADNode, CogniteCADNodeWrite, CogniteCADNodeList, CogniteCADNodeWriteList]):
    _view_id = dm.ViewId("cdf_cdm", "CogniteCADNode", "v1")
    _properties_by_field: ClassVar[dict[str, str]] = _COGNITECADNODE_PROPERTIES_BY_FIELD
    _class_type = CogniteCADNode
    _class_list = CogniteCADNodeList
    _class_write_list = CogniteCADNodeWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

    def __call__(
        self,
        cad_node_reference: str | list[str] | None = None,
        cad_node_reference_prefix: str | None = None,
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
    ) -> CogniteCADNodeQueryAPI[CogniteCADNodeList]:
        """Query starting at Cognite cad nodes.

        Args:
            cad_node_reference: The cad node reference to filter on.
            cad_node_reference_prefix: The prefix of the cad node reference to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_3d: The object 3d to filter on.
            revisions: The revision to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite cad nodes to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for Cognite cad nodes.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. " "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_cognite_cad_node_filter(
            self._view_id,
            cad_node_reference,
            cad_node_reference_prefix,
            description,
            description_prefix,
            model_3d,
            name,
            name_prefix,
            object_3d,
            revisions,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(CogniteCADNodeList)
        return CogniteCADNodeQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        cognite_cad_node: CogniteCADNodeWrite | Sequence[CogniteCADNodeWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) Cognite cad nodes.

        Args:
            cognite_cad_node: Cognite cad node or
                sequence of Cognite cad nodes to upsert.
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

            Create a new cognite_cad_node:

                >>> from cognite_core import CogniteCoreClient
                >>> from cognite_core.data_classes import CogniteCADNodeWrite
                >>> client = CogniteCoreClient()
                >>> cognite_cad_node = CogniteCADNodeWrite(
                ...     external_id="my_cognite_cad_node", ...
                ... )
                >>> result = client.cognite_cad_node.apply(cognite_cad_node)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.cognite_cad_node.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(cognite_cad_node, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more Cognite cad node.

        Args:
            external_id: External id of the Cognite cad node to delete.
            space: The space where all the Cognite cad node are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete cognite_cad_node by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> client.cognite_cad_node.delete("my_cognite_cad_node")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.cognite_cad_node.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CogniteCADNode | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE
    ) -> CogniteCADNodeList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> CogniteCADNode | CogniteCADNodeList | None:
        """Retrieve one or more Cognite cad nodes by id(s).

        Args:
            external_id: External id or list of external ids of the Cognite cad nodes.
            space: The space where all the Cognite cad nodes are located.

        Returns:
            The requested Cognite cad nodes.

        Examples:

            Retrieve cognite_cad_node by id:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_cad_node = client.cognite_cad_node.retrieve(
                ...     "my_cognite_cad_node"
                ... )

        """
        return self._retrieve(external_id, space, retrieve_edges=True, edge_api_name_type_direction_view_id_penta=[])

    def search(
        self,
        query: str,
        properties: CogniteCADNodeTextFields | SequenceNotStr[CogniteCADNodeTextFields] | None = None,
        cad_node_reference: str | list[str] | None = None,
        cad_node_reference_prefix: str | None = None,
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
        sort_by: CogniteCADNodeFields | SequenceNotStr[CogniteCADNodeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> CogniteCADNodeList:
        """Search Cognite cad nodes

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            cad_node_reference: The cad node reference to filter on.
            cad_node_reference_prefix: The prefix of the cad node reference to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_3d: The object 3d to filter on.
            revisions: The revision to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite cad nodes to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results Cognite cad nodes matching the query.

        Examples:

           Search for 'my_cognite_cad_node' in all text properties:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_cad_nodes = client.cognite_cad_node.search(
                ...     'my_cognite_cad_node'
                ... )

        """
        filter_ = _create_cognite_cad_node_filter(
            self._view_id,
            cad_node_reference,
            cad_node_reference_prefix,
            description,
            description_prefix,
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
        property: CogniteCADNodeFields | SequenceNotStr[CogniteCADNodeFields] | None = None,
        query: str | None = None,
        search_property: CogniteCADNodeTextFields | SequenceNotStr[CogniteCADNodeTextFields] | None = None,
        cad_node_reference: str | list[str] | None = None,
        cad_node_reference_prefix: str | None = None,
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
        property: CogniteCADNodeFields | SequenceNotStr[CogniteCADNodeFields] | None = None,
        query: str | None = None,
        search_property: CogniteCADNodeTextFields | SequenceNotStr[CogniteCADNodeTextFields] | None = None,
        cad_node_reference: str | list[str] | None = None,
        cad_node_reference_prefix: str | None = None,
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
        group_by: CogniteCADNodeFields | SequenceNotStr[CogniteCADNodeFields],
        property: CogniteCADNodeFields | SequenceNotStr[CogniteCADNodeFields] | None = None,
        query: str | None = None,
        search_property: CogniteCADNodeTextFields | SequenceNotStr[CogniteCADNodeTextFields] | None = None,
        cad_node_reference: str | list[str] | None = None,
        cad_node_reference_prefix: str | None = None,
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
        group_by: CogniteCADNodeFields | SequenceNotStr[CogniteCADNodeFields] | None = None,
        property: CogniteCADNodeFields | SequenceNotStr[CogniteCADNodeFields] | None = None,
        query: str | None = None,
        search_property: CogniteCADNodeTextFields | SequenceNotStr[CogniteCADNodeTextFields] | None = None,
        cad_node_reference: str | list[str] | None = None,
        cad_node_reference_prefix: str | None = None,
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
        """Aggregate data across Cognite cad nodes

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            cad_node_reference: The cad node reference to filter on.
            cad_node_reference_prefix: The prefix of the cad node reference to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_3d: The object 3d to filter on.
            revisions: The revision to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite cad nodes to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count Cognite cad nodes in space `my_space`:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> result = client.cognite_cad_node.aggregate("count", space="my_space")

        """

        filter_ = _create_cognite_cad_node_filter(
            self._view_id,
            cad_node_reference,
            cad_node_reference_prefix,
            description,
            description_prefix,
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
        property: CogniteCADNodeFields,
        interval: float,
        query: str | None = None,
        search_property: CogniteCADNodeTextFields | SequenceNotStr[CogniteCADNodeTextFields] | None = None,
        cad_node_reference: str | list[str] | None = None,
        cad_node_reference_prefix: str | None = None,
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
        """Produces histograms for Cognite cad nodes

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            cad_node_reference: The cad node reference to filter on.
            cad_node_reference_prefix: The prefix of the cad node reference to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_3d: The object 3d to filter on.
            revisions: The revision to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite cad nodes to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_cognite_cad_node_filter(
            self._view_id,
            cad_node_reference,
            cad_node_reference_prefix,
            description,
            description_prefix,
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

    def query(self) -> CogniteCADNodeQuery:
        """Start a query for Cognite cad nodes."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return CogniteCADNodeQuery(self._client)

    def select(self) -> CogniteCADNodeQuery:
        """Start selecting from Cognite cad nodes."""
        warnings.warn(
            "The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2
        )
        return CogniteCADNodeQuery(self._client)

    def list(
        self,
        cad_node_reference: str | list[str] | None = None,
        cad_node_reference_prefix: str | None = None,
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
        sort_by: CogniteCADNodeFields | Sequence[CogniteCADNodeFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> CogniteCADNodeList:
        """List/filter Cognite cad nodes

        Args:
            cad_node_reference: The cad node reference to filter on.
            cad_node_reference_prefix: The prefix of the cad node reference to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            model_3d: The model 3d to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_3d: The object 3d to filter on.
            revisions: The revision to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of Cognite cad nodes to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `model_3d`, `object_3d` and `revisions` for the Cognite cad nodes.
            Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier
            of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested Cognite cad nodes

        Examples:

            List Cognite cad nodes and limit to 5:

                >>> from cognite_core import CogniteCoreClient
                >>> client = CogniteCoreClient()
                >>> cognite_cad_nodes = client.cognite_cad_node.list(limit=5)

        """
        filter_ = _create_cognite_cad_node_filter(
            self._view_id,
            cad_node_reference,
            cad_node_reference_prefix,
            description,
            description_prefix,
            model_3d,
            name,
            name_prefix,
            object_3d,
            revisions,
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

        builder = DataClassQueryBuilder(CogniteCADNodeList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                CogniteCADNode,
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
                        filter=dm.filters.HasData(views=[CogniteCADModel._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("model3D"),
                    ),
                    CogniteCADModel,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[Cognite3DObject._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("object3D"),
                    ),
                    Cognite3DObject,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[CogniteCADRevision._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("revisions"),
                    ),
                    CogniteCADRevision,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
