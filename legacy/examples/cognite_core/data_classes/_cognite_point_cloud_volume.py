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
    "CognitePointCloudVolume",
    "CognitePointCloudVolumeWrite",
    "CognitePointCloudVolumeList",
    "CognitePointCloudVolumeWriteList",
    "CognitePointCloudVolumeFields",
    "CognitePointCloudVolumeTextFields",
    "CognitePointCloudVolumeGraphQL",
]


CognitePointCloudVolumeTextFields = Literal[
    "external_id", "aliases", "description", "format_version", "name", "tags", "volume_references"
]
CognitePointCloudVolumeFields = Literal[
    "external_id",
    "aliases",
    "description",
    "format_version",
    "name",
    "tags",
    "volume",
    "volume_references",
    "volume_type",
]

_COGNITEPOINTCLOUDVOLUME_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "format_version": "formatVersion",
    "name": "name",
    "tags": "tags",
    "volume": "volume",
    "volume_references": "volumeReferences",
    "volume_type": "volumeType",
}


class CognitePointCloudVolumeGraphQL(GraphQLCore, protected_namespaces=()):
    """This represents the reading version of Cognite point cloud volume, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud volume.
        data_record: The data record of the Cognite point cloud volume node.
        aliases: Alternative names for the node
        description: Description of the instance
        format_version: Specifies the version the 'volume' field is following. Volume definition is today 9 floats
            (property volume)
        model_3d: Direct relation to Cognite3DModel instance
        name: Name of the instance
        object_3d: Direct relation to object3D grouping for this node
        revisions: List of direct relations to revision information
        tags: Text based labels for generic use, limited to 1000
        volume: Relevant coordinates for the volume type, 9 floats in total, that defines the volume
        volume_references: Unique volume metric hashes used to access the 3D specialized data storage
        volume_type: Type of volume (Cylinder or Box)
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CognitePointCloudVolume", "v1")
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    format_version: Optional[str] = Field(None, alias="formatVersion")
    model_3d: Optional[CogniteCADModelGraphQL] = Field(default=None, repr=False, alias="model3D")
    name: Optional[str] = None
    object_3d: Optional[Cognite3DObjectGraphQL] = Field(default=None, repr=False, alias="object3D")
    revisions: Optional[list[CogniteCADRevisionGraphQL]] = Field(default=None, repr=False)
    tags: Optional[list[str]] = None
    volume: Optional[list[float]] = None
    volume_references: Optional[list[str]] = Field(None, alias="volumeReferences")
    volume_type: Optional[Literal["Box", "Cylinder"]] = Field(None, alias="volumeType")

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

    def as_read(self) -> CognitePointCloudVolume:
        """Convert this GraphQL format of Cognite point cloud volume to the reading format."""
        return CognitePointCloudVolume.model_validate(as_read_args(self))

    def as_write(self) -> CognitePointCloudVolumeWrite:
        """Convert this GraphQL format of Cognite point cloud volume to the writing format."""
        return CognitePointCloudVolumeWrite.model_validate(as_write_args(self))


class CognitePointCloudVolume(CogniteDescribableNode, protected_namespaces=()):
    """This represents the reading version of Cognite point cloud volume.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud volume.
        data_record: The data record of the Cognite point cloud volume node.
        aliases: Alternative names for the node
        description: Description of the instance
        format_version: Specifies the version the 'volume' field is following. Volume definition is today 9 floats
            (property volume)
        model_3d: Direct relation to Cognite3DModel instance
        name: Name of the instance
        object_3d: Direct relation to object3D grouping for this node
        revisions: List of direct relations to revision information
        tags: Text based labels for generic use, limited to 1000
        volume: Relevant coordinates for the volume type, 9 floats in total, that defines the volume
        volume_references: Unique volume metric hashes used to access the 3D specialized data storage
        volume_type: Type of volume (Cylinder or Box)
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CognitePointCloudVolume", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    format_version: Optional[str] = Field(None, alias="formatVersion")
    model_3d: Union[CogniteCADModel, str, dm.NodeId, None] = Field(default=None, repr=False, alias="model3D")
    object_3d: Union[Cognite3DObject, str, dm.NodeId, None] = Field(default=None, repr=False, alias="object3D")
    revisions: Optional[list[Union[CogniteCADRevision, str, dm.NodeId]]] = Field(default=None, repr=False)
    volume: Optional[list[float]] = None
    volume_references: Optional[list[str]] = Field(None, alias="volumeReferences")
    volume_type: Optional[Literal["Box", "Cylinder"]] | str = Field(None, alias="volumeType")

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

    def as_write(self) -> CognitePointCloudVolumeWrite:
        """Convert this read version of Cognite point cloud volume to the writing version."""
        return CognitePointCloudVolumeWrite.model_validate(as_write_args(self))


class CognitePointCloudVolumeWrite(CogniteDescribableNodeWrite, protected_namespaces=()):
    """This represents the writing version of Cognite point cloud volume.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud volume.
        data_record: The data record of the Cognite point cloud volume node.
        aliases: Alternative names for the node
        description: Description of the instance
        format_version: Specifies the version the 'volume' field is following. Volume definition is today 9 floats
            (property volume)
        model_3d: Direct relation to Cognite3DModel instance
        name: Name of the instance
        object_3d: Direct relation to object3D grouping for this node
        revisions: List of direct relations to revision information
        tags: Text based labels for generic use, limited to 1000
        volume: Relevant coordinates for the volume type, 9 floats in total, that defines the volume
        volume_references: Unique volume metric hashes used to access the 3D specialized data storage
        volume_type: Type of volume (Cylinder or Box)
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "description",
        "format_version",
        "model_3d",
        "name",
        "object_3d",
        "revisions",
        "tags",
        "volume",
        "volume_references",
        "volume_type",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "model_3d",
        "object_3d",
        "revisions",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CognitePointCloudVolume", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    format_version: Optional[str] = Field(None, alias="formatVersion")
    model_3d: Union[CogniteCADModelWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="model3D")
    object_3d: Union[Cognite3DObjectWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="object3D")
    revisions: Optional[list[Union[CogniteCADRevisionWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    volume: Optional[list[float]] = None
    volume_references: Optional[list[str]] = Field(None, alias="volumeReferences")
    volume_type: Optional[Literal["Box", "Cylinder"]] = Field(None, alias="volumeType")

    @field_validator("model_3d", "object_3d", "revisions", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class CognitePointCloudVolumeList(DomainModelList[CognitePointCloudVolume]):
    """List of Cognite point cloud volumes in the read version."""

    _INSTANCE = CognitePointCloudVolume

    def as_write(self) -> CognitePointCloudVolumeWriteList:
        """Convert these read versions of Cognite point cloud volume to the writing versions."""
        return CognitePointCloudVolumeWriteList([node.as_write() for node in self.data])

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


class CognitePointCloudVolumeWriteList(DomainModelWriteList[CognitePointCloudVolumeWrite]):
    """List of Cognite point cloud volumes in the writing version."""

    _INSTANCE = CognitePointCloudVolumeWrite

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


def _create_cognite_point_cloud_volume_filter(
    view_id: dm.ViewId,
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
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(format_version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("formatVersion"), value=format_version))
    if format_version and isinstance(format_version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("formatVersion"), values=format_version))
    if format_version_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("formatVersion"), value=format_version_prefix))
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
    if isinstance(volume_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("volumeType"), value=volume_type))
    if volume_type and isinstance(volume_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("volumeType"), values=volume_type))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _CognitePointCloudVolumeQuery(NodeQueryCore[T_DomainModelList, CognitePointCloudVolumeList]):
    _view_id = CognitePointCloudVolume._view_id
    _result_cls = CognitePointCloudVolume
    _result_list_cls_end = CognitePointCloudVolumeList

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
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.format_version = StringFilter(self, self._view_id.as_property_ref("formatVersion"))
        self.model_3d_filter = DirectRelationFilter(self, self._view_id.as_property_ref("model3D"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.object_3d_filter = DirectRelationFilter(self, self._view_id.as_property_ref("object3D"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.format_version,
                self.model_3d_filter,
                self.name,
                self.object_3d_filter,
            ]
        )

    def list_cognite_point_cloud_volume(self, limit: int = DEFAULT_QUERY_LIMIT) -> CognitePointCloudVolumeList:
        return self._list(limit=limit)


class CognitePointCloudVolumeQuery(_CognitePointCloudVolumeQuery[CognitePointCloudVolumeList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CognitePointCloudVolumeList)
