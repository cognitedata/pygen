from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from cognite_core.config import global_config
from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    T_DomainModelList,
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    DirectRelationFilter,
)
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_3_d_object import (
        Cognite3DObject,
        Cognite3DObjectList,
        Cognite3DObjectGraphQL,
        Cognite3DObjectWrite,
        Cognite3DObjectWriteList,
    )
    from cognite_core.data_classes._cognite_cad_model import (
        CogniteCADModel,
        CogniteCADModelList,
        CogniteCADModelGraphQL,
        CogniteCADModelWrite,
        CogniteCADModelWriteList,
    )
    from cognite_core.data_classes._cognite_cad_revision import (
        CogniteCADRevision,
        CogniteCADRevisionList,
        CogniteCADRevisionGraphQL,
        CogniteCADRevisionWrite,
        CogniteCADRevisionWriteList,
    )


__all__ = [
    "CogniteCADNode",
    "CogniteCADNodeWrite",
    "CogniteCADNodeList",
    "CogniteCADNodeWriteList",
    "CogniteCADNodeFields",
    "CogniteCADNodeTextFields",
    "CogniteCADNodeGraphQL",
]


CogniteCADNodeTextFields = Literal["external_id", "aliases", "cad_node_reference", "description", "name", "tags"]
CogniteCADNodeFields = Literal[
    "external_id", "aliases", "cad_node_reference", "description", "name", "sub_tree_sizes", "tags", "tree_indexes"
]

_COGNITECADNODE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "cad_node_reference": "cadNodeReference",
    "description": "description",
    "name": "name",
    "sub_tree_sizes": "subTreeSizes",
    "tags": "tags",
    "tree_indexes": "treeIndexes",
}


class CogniteCADNodeGraphQL(GraphQLCore, protected_namespaces=()):
    """This represents the reading version of Cognite cad node, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cad node.
        data_record: The data record of the Cognite cad node node.
        aliases: Alternative names for the node
        cad_node_reference: Reference to a node within a CAD model from the 3D API
        description: Description of the instance
        model_3d: Direct relation to Cognite3DModel
        name: Name of the instance
        object_3d: Direct relation to object3D grouping for this node
        revisions: List of direct relations to instances of Cognite3DRevision which this CogniteCADNode exists in.
        sub_tree_sizes: List of subtree sizes in the same order as revisions. Used by Reveal and similar applications
            to know how many nodes exists below this node in the hierarchy
        tags: Text based labels for generic use, limited to 1000
        tree_indexes: List of tree indexes in the same order as revisions. Used by Reveal and similar applications to
            map from CogniteCADNode to tree index
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteCADNode", "v1")
    aliases: Optional[list[str]] = None
    cad_node_reference: Optional[str] = Field(None, alias="cadNodeReference")
    description: Optional[str] = None
    model_3d: Optional[CogniteCADModelGraphQL] = Field(default=None, repr=False, alias="model3D")
    name: Optional[str] = None
    object_3d: Optional[Cognite3DObjectGraphQL] = Field(default=None, repr=False, alias="object3D")
    revisions: Optional[list[CogniteCADRevisionGraphQL]] = Field(default=None, repr=False)
    sub_tree_sizes: Optional[list[int]] = Field(None, alias="subTreeSizes")
    tags: Optional[list[str]] = None
    tree_indexes: Optional[list[int]] = Field(None, alias="treeIndexes")

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    @field_validator("model_3d", "object_3d", "revisions", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> CogniteCADNode:
        """Convert this GraphQL format of Cognite cad node to the reading format."""
        return CogniteCADNode.model_validate(as_read_args(self))

    def as_write(self) -> CogniteCADNodeWrite:
        """Convert this GraphQL format of Cognite cad node to the writing format."""
        return CogniteCADNodeWrite.model_validate(as_write_args(self))


class CogniteCADNode(CogniteDescribableNode, protected_namespaces=()):
    """This represents the reading version of Cognite cad node.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cad node.
        data_record: The data record of the Cognite cad node node.
        aliases: Alternative names for the node
        cad_node_reference: Reference to a node within a CAD model from the 3D API
        description: Description of the instance
        model_3d: Direct relation to Cognite3DModel
        name: Name of the instance
        object_3d: Direct relation to object3D grouping for this node
        revisions: List of direct relations to instances of Cognite3DRevision which this CogniteCADNode exists in.
        sub_tree_sizes: List of subtree sizes in the same order as revisions. Used by Reveal and similar applications
            to know how many nodes exists below this node in the hierarchy
        tags: Text based labels for generic use, limited to 1000
        tree_indexes: List of tree indexes in the same order as revisions. Used by Reveal and similar applications to
            map from CogniteCADNode to tree index
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteCADNode", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    cad_node_reference: Optional[str] = Field(None, alias="cadNodeReference")
    model_3d: Union[CogniteCADModel, str, dm.NodeId, None] = Field(default=None, repr=False, alias="model3D")
    object_3d: Union[Cognite3DObject, str, dm.NodeId, None] = Field(default=None, repr=False, alias="object3D")
    revisions: Optional[list[Union[CogniteCADRevision, str, dm.NodeId]]] = Field(default=None, repr=False)
    sub_tree_sizes: Optional[list[int]] = Field(None, alias="subTreeSizes")
    tree_indexes: Optional[list[int]] = Field(None, alias="treeIndexes")

    @field_validator("model_3d", "object_3d", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("revisions", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> CogniteCADNodeWrite:
        """Convert this read version of Cognite cad node to the writing version."""
        return CogniteCADNodeWrite.model_validate(as_write_args(self))


class CogniteCADNodeWrite(CogniteDescribableNodeWrite, protected_namespaces=()):
    """This represents the writing version of Cognite cad node.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cad node.
        data_record: The data record of the Cognite cad node node.
        aliases: Alternative names for the node
        cad_node_reference: Reference to a node within a CAD model from the 3D API
        description: Description of the instance
        model_3d: Direct relation to Cognite3DModel
        name: Name of the instance
        object_3d: Direct relation to object3D grouping for this node
        revisions: List of direct relations to instances of Cognite3DRevision which this CogniteCADNode exists in.
        sub_tree_sizes: List of subtree sizes in the same order as revisions. Used by Reveal and similar applications
            to know how many nodes exists below this node in the hierarchy
        tags: Text based labels for generic use, limited to 1000
        tree_indexes: List of tree indexes in the same order as revisions. Used by Reveal and similar applications to
            map from CogniteCADNode to tree index
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "cad_node_reference",
        "description",
        "model_3d",
        "name",
        "object_3d",
        "revisions",
        "sub_tree_sizes",
        "tags",
        "tree_indexes",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "model_3d",
        "object_3d",
        "revisions",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteCADNode", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    cad_node_reference: Optional[str] = Field(None, alias="cadNodeReference")
    model_3d: Union[CogniteCADModelWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="model3D")
    object_3d: Union[Cognite3DObjectWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="object3D")
    revisions: Optional[list[Union[CogniteCADRevisionWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    sub_tree_sizes: Optional[list[int]] = Field(None, alias="subTreeSizes")
    tree_indexes: Optional[list[int]] = Field(None, alias="treeIndexes")

    @field_validator("model_3d", "object_3d", "revisions", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class CogniteCADNodeList(DomainModelList[CogniteCADNode]):
    """List of Cognite cad nodes in the read version."""

    _INSTANCE = CogniteCADNode

    def as_write(self) -> CogniteCADNodeWriteList:
        """Convert these read versions of Cognite cad node to the writing versions."""
        return CogniteCADNodeWriteList([node.as_write() for node in self.data])

    @property
    def model_3d(self) -> CogniteCADModelList:
        from ._cognite_cad_model import CogniteCADModel, CogniteCADModelList

        return CogniteCADModelList([item.model_3d for item in self.data if isinstance(item.model_3d, CogniteCADModel)])

    @property
    def object_3d(self) -> Cognite3DObjectList:
        from ._cognite_3_d_object import Cognite3DObject, Cognite3DObjectList

        return Cognite3DObjectList(
            [item.object_3d for item in self.data if isinstance(item.object_3d, Cognite3DObject)]
        )

    @property
    def revisions(self) -> CogniteCADRevisionList:
        from ._cognite_cad_revision import CogniteCADRevision, CogniteCADRevisionList

        return CogniteCADRevisionList(
            [item for items in self.data for item in items.revisions or [] if isinstance(item, CogniteCADRevision)]
        )


class CogniteCADNodeWriteList(DomainModelWriteList[CogniteCADNodeWrite]):
    """List of Cognite cad nodes in the writing version."""

    _INSTANCE = CogniteCADNodeWrite

    @property
    def model_3d(self) -> CogniteCADModelWriteList:
        from ._cognite_cad_model import CogniteCADModelWrite, CogniteCADModelWriteList

        return CogniteCADModelWriteList(
            [item.model_3d for item in self.data if isinstance(item.model_3d, CogniteCADModelWrite)]
        )

    @property
    def object_3d(self) -> Cognite3DObjectWriteList:
        from ._cognite_3_d_object import Cognite3DObjectWrite, Cognite3DObjectWriteList

        return Cognite3DObjectWriteList(
            [item.object_3d for item in self.data if isinstance(item.object_3d, Cognite3DObjectWrite)]
        )

    @property
    def revisions(self) -> CogniteCADRevisionWriteList:
        from ._cognite_cad_revision import CogniteCADRevisionWrite, CogniteCADRevisionWriteList

        return CogniteCADRevisionWriteList(
            [item for items in self.data for item in items.revisions or [] if isinstance(item, CogniteCADRevisionWrite)]
        )


def _create_cognite_cad_node_filter(
    view_id: dm.ViewId,
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
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(cad_node_reference, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("cadNodeReference"), value=cad_node_reference))
    if cad_node_reference and isinstance(cad_node_reference, list):
        filters.append(dm.filters.In(view_id.as_property_ref("cadNodeReference"), values=cad_node_reference))
    if cad_node_reference_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("cadNodeReference"), value=cad_node_reference_prefix))
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(model_3d, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(model_3d):
        filters.append(dm.filters.Equals(view_id.as_property_ref("model3D"), value=as_instance_dict_id(model_3d)))
    if model_3d and isinstance(model_3d, Sequence) and not isinstance(model_3d, str) and not is_tuple_id(model_3d):
        filters.append(
            dm.filters.In(view_id.as_property_ref("model3D"), values=[as_instance_dict_id(item) for item in model_3d])
        )
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(object_3d, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(object_3d):
        filters.append(dm.filters.Equals(view_id.as_property_ref("object3D"), value=as_instance_dict_id(object_3d)))
    if object_3d and isinstance(object_3d, Sequence) and not isinstance(object_3d, str) and not is_tuple_id(object_3d):
        filters.append(
            dm.filters.In(view_id.as_property_ref("object3D"), values=[as_instance_dict_id(item) for item in object_3d])
        )
    if isinstance(revisions, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(revisions):
        filters.append(dm.filters.Equals(view_id.as_property_ref("revisions"), value=as_instance_dict_id(revisions)))
    if revisions and isinstance(revisions, Sequence) and not isinstance(revisions, str) and not is_tuple_id(revisions):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("revisions"), values=[as_instance_dict_id(item) for item in revisions]
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _CogniteCADNodeQuery(NodeQueryCore[T_DomainModelList, CogniteCADNodeList]):
    _view_id = CogniteCADNode._view_id
    _result_cls = CogniteCADNode
    _result_list_cls_end = CogniteCADNodeList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
    ):
        from ._cognite_3_d_object import _Cognite3DObjectQuery
        from ._cognite_cad_model import _CogniteCADModelQuery
        from ._cognite_cad_revision import _CogniteCADRevisionQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _CogniteCADModelQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.model_3d = _CogniteCADModelQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("model3D"),
                    direction="outwards",
                ),
                connection_name="model_3d",
                connection_property=ViewPropertyId(self._view_id, "model3D"),
            )

        if _Cognite3DObjectQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.object_3d = _Cognite3DObjectQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("object3D"),
                    direction="outwards",
                ),
                connection_name="object_3d",
                connection_property=ViewPropertyId(self._view_id, "object3D"),
            )

        if _CogniteCADRevisionQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.revisions = _CogniteCADRevisionQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("revisions"),
                    direction="outwards",
                ),
                connection_name="revisions",
                connection_property=ViewPropertyId(self._view_id, "revisions"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.cad_node_reference = StringFilter(self, self._view_id.as_property_ref("cadNodeReference"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.model_3d_filter = DirectRelationFilter(self, self._view_id.as_property_ref("model3D"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.object_3d_filter = DirectRelationFilter(self, self._view_id.as_property_ref("object3D"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.cad_node_reference,
                self.description,
                self.model_3d_filter,
                self.name,
                self.object_3d_filter,
            ]
        )

    def list_cognite_cad_node(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteCADNodeList:
        return self._list(limit=limit)


class CogniteCADNodeQuery(_CogniteCADNodeQuery[CogniteCADNodeList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteCADNodeList)
