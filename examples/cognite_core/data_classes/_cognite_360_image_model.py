from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,
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
    "Cognite360ImageModelApply",
    "Cognite360ImageModelList",
    "Cognite360ImageModelWriteList",
    "Cognite360ImageModelApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Cognite360ImageModel:
        """Convert this GraphQL format of Cognite 360 image model to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Cognite360ImageModel(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            aliases=self.aliases,
            collections=(
                [collection.as_read() for collection in self.collections] if self.collections is not None else None
            ),
            description=self.description,
            name=self.name,
            tags=self.tags,
            thumbnail=self.thumbnail.as_read() if isinstance(self.thumbnail, GraphQLCore) else self.thumbnail,
            type_=self.type_,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> Cognite360ImageModelWrite:
        """Convert this GraphQL format of Cognite 360 image model to the writing format."""
        return Cognite360ImageModelWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aliases=self.aliases,
            description=self.description,
            name=self.name,
            tags=self.tags,
            thumbnail=self.thumbnail.as_write() if isinstance(self.thumbnail, GraphQLCore) else self.thumbnail,
            type_=self.type_,
        )


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

    def as_write(self) -> Cognite360ImageModelWrite:
        """Convert this read version of Cognite 360 image model to the writing version."""
        return Cognite360ImageModelWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aliases=self.aliases,
            description=self.description,
            name=self.name,
            tags=self.tags,
            thumbnail=self.thumbnail.as_write() if isinstance(self.thumbnail, DomainModel) else self.thumbnail,
            type_=self.type_,
        )

    def as_apply(self) -> Cognite360ImageModelWrite:
        """Convert this read version of Cognite 360 image model to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, Cognite360ImageModel],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._cognite_360_image_collection import Cognite360ImageCollection
        from ._cognite_file import CogniteFile

        for instance in instances.values():
            if (
                isinstance(instance.thumbnail, (dm.NodeId, str))
                and (thumbnail := nodes_by_id.get(instance.thumbnail))
                and isinstance(thumbnail, CogniteFile)
            ):
                instance.thumbnail = thumbnail
        for node in nodes_by_id.values():
            if (
                isinstance(node, Cognite360ImageCollection)
                and node.model_3d is not None
                and (model_3d := instances.get(as_pygen_node_id(node.model_3d)))
            ):
                if model_3d.collections is None:
                    model_3d.collections = []
                model_3d.collections.append(node)


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageModel", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.aliases is not None or write_none:
            properties["aliases"] = self.aliases

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.tags is not None or write_none:
            properties["tags"] = self.tags

        if self.thumbnail is not None:
            properties["thumbnail"] = {
                "space": self.space if isinstance(self.thumbnail, str) else self.thumbnail.space,
                "externalId": self.thumbnail if isinstance(self.thumbnail, str) else self.thumbnail.external_id,
            }

        if self.type_ is not None or write_none:
            properties["type"] = self.type_

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=as_direct_relation_reference(self.node_type),
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.thumbnail, DomainModelWrite):
            other_resources = self.thumbnail._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class Cognite360ImageModelApply(Cognite360ImageModelWrite):
    def __new__(cls, *args, **kwargs) -> Cognite360ImageModelApply:
        warnings.warn(
            "Cognite360ImageModelApply is deprecated and will be removed in v1.0. Use Cognite360ImageModelWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Cognite360ImageModel.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class Cognite360ImageModelList(DomainModelList[Cognite360ImageModel]):
    """List of Cognite 360 image models in the read version."""

    _INSTANCE = Cognite360ImageModel

    def as_write(self) -> Cognite360ImageModelWriteList:
        """Convert these read versions of Cognite 360 image model to the writing versions."""
        return Cognite360ImageModelWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> Cognite360ImageModelWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class Cognite360ImageModelApplyList(Cognite360ImageModelWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _Cognite360ImageCollectionQuery not in created_types:
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
            )

        if _CogniteFileQuery not in created_types:
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.name,
            ]
        )

    def list_cognite_360_image_model(self, limit: int = DEFAULT_QUERY_LIMIT) -> Cognite360ImageModelList:
        return self._list(limit=limit)


class Cognite360ImageModelQuery(_Cognite360ImageModelQuery[Cognite360ImageModelList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, Cognite360ImageModelList)
