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
from cognite_core.data_classes._cognite_3_d_model import Cognite3DModel, Cognite3DModelWrite

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_360_image_collection import (
        Cognite360ImageCollection,
        Cognite360ImageCollectionList,
        Cognite360ImageCollectionGraphQL,
        Cognite360ImageCollectionWrite,
        Cognite360ImageCollectionWriteList,
    )
    from cognite_core.data_classes._cognite_file import (
        CogniteFile,
        CogniteFileList,
        CogniteFileGraphQL,
        CogniteFileWrite,
        CogniteFileWriteList,
    )


__all__ = [
    "Cognite360ImageModel",
    "Cognite360ImageModelWrite",
    "Cognite360ImageModelList",
    "Cognite360ImageModelWriteList",
    "Cognite360ImageModelFields",
    "Cognite360ImageModelTextFields",
    "Cognite360ImageModelGraphQL",
]


Cognite360ImageModelTextFields = Literal["external_id", "aliases", "description", "name", "tags"]
Cognite360ImageModelFields = Literal["external_id", "aliases", "description", "name", "tags", "type_"]

_COGNITE360IMAGEMODEL_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "name": "name",
    "tags": "tags",
    "type_": "type",
}


class Cognite360ImageModelGraphQL(GraphQLCore):
    """This represents the reading version of Cognite 360 image model, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image model.
        data_record: The data record of the Cognite 360 image model node.
        aliases: Alternative names for the node
        collections: List Cognite360Image collections for this instance
        description: Description of the instance
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
        thumbnail: Thumbnail of the 3D model
        type_: CAD, PointCloud or Image360
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageModel", "v1")
    aliases: Optional[list[str]] = None
    collections: Optional[list[Cognite360ImageCollectionGraphQL]] = Field(default=None, repr=False)
    description: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[list[str]] = None
    thumbnail: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)
    type_: Optional[Literal["CAD", "Image360", "PointCloud"]] = Field(None, alias="type")

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

    @field_validator("collections", "thumbnail", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Cognite360ImageModel:
        """Convert this GraphQL format of Cognite 360 image model to the reading format."""
        return Cognite360ImageModel.model_validate(as_read_args(self))

    def as_write(self) -> Cognite360ImageModelWrite:
        """Convert this GraphQL format of Cognite 360 image model to the writing format."""
        return Cognite360ImageModelWrite.model_validate(as_write_args(self))


class Cognite360ImageModel(Cognite3DModel):
    """This represents the reading version of Cognite 360 image model.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image model.
        data_record: The data record of the Cognite 360 image model node.
        aliases: Alternative names for the node
        collections: List Cognite360Image collections for this instance
        description: Description of the instance
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
        thumbnail: Thumbnail of the 3D model
        type_: CAD, PointCloud or Image360
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageModel", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    collections: Optional[list[Cognite360ImageCollection]] = Field(default=None, repr=False)

    @field_validator("thumbnail", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("collections", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> Cognite360ImageModelWrite:
        """Convert this read version of Cognite 360 image model to the writing version."""
        return Cognite360ImageModelWrite.model_validate(as_write_args(self))


class Cognite360ImageModelWrite(Cognite3DModelWrite):
    """This represents the writing version of Cognite 360 image model.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image model.
        data_record: The data record of the Cognite 360 image model node.
        aliases: Alternative names for the node
        description: Description of the instance
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
        thumbnail: Thumbnail of the 3D model
        type_: CAD, PointCloud or Image360
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "description",
        "name",
        "tags",
        "thumbnail",
        "type_",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = ("thumbnail",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageModel", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None


class Cognite360ImageModelList(DomainModelList[Cognite360ImageModel]):
    """List of Cognite 360 image models in the read version."""

    _INSTANCE = Cognite360ImageModel

    def as_write(self) -> Cognite360ImageModelWriteList:
        """Convert these read versions of Cognite 360 image model to the writing versions."""
        return Cognite360ImageModelWriteList([node.as_write() for node in self.data])

    @property
    def collections(self) -> Cognite360ImageCollectionList:
        from ._cognite_360_image_collection import Cognite360ImageCollection, Cognite360ImageCollectionList

        return Cognite360ImageCollectionList(
            [
                item
                for items in self.data
                for item in items.collections or []
                if isinstance(item, Cognite360ImageCollection)
            ]
        )

    @property
    def thumbnail(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.thumbnail for item in self.data if isinstance(item.thumbnail, CogniteFile)])


class Cognite360ImageModelWriteList(DomainModelWriteList[Cognite360ImageModelWrite]):
    """List of Cognite 360 image models in the writing version."""

    _INSTANCE = Cognite360ImageModelWrite

    @property
    def thumbnail(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList(
            [item.thumbnail for item in self.data if isinstance(item.thumbnail, CogniteFileWrite)]
        )


def _create_cognite_360_image_model_filter(
    view_id: dm.ViewId,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    thumbnail: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    type_: Literal["CAD", "Image360", "PointCloud"] | list[Literal["CAD", "Image360", "PointCloud"]] | None = None,
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
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(thumbnail, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(thumbnail):
        filters.append(dm.filters.Equals(view_id.as_property_ref("thumbnail"), value=as_instance_dict_id(thumbnail)))
    if thumbnail and isinstance(thumbnail, Sequence) and not isinstance(thumbnail, str) and not is_tuple_id(thumbnail):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("thumbnail"), values=[as_instance_dict_id(item) for item in thumbnail]
            )
        )
    if isinstance(type_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type_))
    if type_ and isinstance(type_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type_))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _Cognite360ImageModelQuery(NodeQueryCore[T_DomainModelList, Cognite360ImageModelList]):
    _view_id = Cognite360ImageModel._view_id
    _result_cls = Cognite360ImageModel
    _result_list_cls_end = Cognite360ImageModelList

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
        from ._cognite_360_image_collection import _Cognite360ImageCollectionQuery
        from ._cognite_file import _CogniteFileQuery

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

        if (
            _Cognite360ImageCollectionQuery not in created_types
            and len(creation_path) + 1 < global_config.max_select_depth
        ):
            self.collections = _Cognite360ImageCollectionQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "Cognite3DRevision", "v1").as_property_ref("model3D"),
                    direction="inwards",
                ),
                connection_name="collections",
                connection_property=ViewPropertyId(self._view_id, "collections"),
            )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.thumbnail = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("thumbnail"),
                    direction="outwards",
                ),
                connection_name="thumbnail",
                connection_property=ViewPropertyId(self._view_id, "thumbnail"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.thumbnail_filter = DirectRelationFilter(self, self._view_id.as_property_ref("thumbnail"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.name,
                self.thumbnail_filter,
            ]
        )

    def list_cognite_360_image_model(self, limit: int = DEFAULT_QUERY_LIMIT) -> Cognite360ImageModelList:
        return self._list(limit=limit)


class Cognite360ImageModelQuery(_Cognite360ImageModelQuery[Cognite360ImageModelList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, Cognite360ImageModelList)
