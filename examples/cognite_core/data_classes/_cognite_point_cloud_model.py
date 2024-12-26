from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union, no_type_check

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from pydantic import Field, field_validator, model_validator

from cognite_core.data_classes._cognite_3_d_model import Cognite3DModel, Cognite3DModelWrite
from cognite_core.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelList,
    DomainModelWrite,
    DomainModelWriteList,
    DomainRelation,
    GraphQLCore,
    NodeQueryCore,
    QueryCore,
    ResourcesWrite,
    StringFilter,
    T_DomainModelList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_pygen_node_id,
    is_tuple_id,
)

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_file import (
        CogniteFileGraphQL,
        CogniteFileList,
        CogniteFileWriteList,
    )
    from cognite_core.data_classes._cognite_point_cloud_revision import (
        CognitePointCloudRevision,
        CognitePointCloudRevisionGraphQL,
        CognitePointCloudRevisionList,
    )


__all__ = [
    "CognitePointCloudModel",
    "CognitePointCloudModelWrite",
    "CognitePointCloudModelApply",
    "CognitePointCloudModelList",
    "CognitePointCloudModelWriteList",
    "CognitePointCloudModelApplyList",
    "CognitePointCloudModelFields",
    "CognitePointCloudModelTextFields",
    "CognitePointCloudModelGraphQL",
]


CognitePointCloudModelTextFields = Literal["external_id", "aliases", "description", "name", "tags"]
CognitePointCloudModelFields = Literal["external_id", "aliases", "description", "name", "tags", "type_"]

_COGNITEPOINTCLOUDMODEL_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "name": "name",
    "tags": "tags",
    "type_": "type",
}


class CognitePointCloudModelGraphQL(GraphQLCore):
    """This represents the reading version of Cognite point cloud model, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud model.
        data_record: The data record of the Cognite point cloud model node.
        aliases: Alternative names for the node
        description: Description of the instance
        name: Name of the instance
        revisions: List of revisions for this PointCloud model
        tags: Text based labels for generic use, limited to 1000
        thumbnail: Thumbnail of the 3D model
        type_: CAD, PointCloud or Image360
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CognitePointCloudModel", "v1")
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    name: Optional[str] = None
    revisions: Optional[list[CognitePointCloudRevisionGraphQL]] = Field(default=None, repr=False)
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

    @field_validator("revisions", "thumbnail", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CognitePointCloudModel:
        """Convert this GraphQL format of Cognite point cloud model to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CognitePointCloudModel(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            aliases=self.aliases,
            description=self.description,
            name=self.name,
            revisions=[revision.as_read() for revision in self.revisions] if self.revisions is not None else None,
            tags=self.tags,
            thumbnail=self.thumbnail.as_read() if isinstance(self.thumbnail, GraphQLCore) else self.thumbnail,
            type_=self.type_,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CognitePointCloudModelWrite:
        """Convert this GraphQL format of Cognite point cloud model to the writing format."""
        return CognitePointCloudModelWrite(
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


class CognitePointCloudModel(Cognite3DModel):
    """This represents the reading version of Cognite point cloud model.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud model.
        data_record: The data record of the Cognite point cloud model node.
        aliases: Alternative names for the node
        description: Description of the instance
        name: Name of the instance
        revisions: List of revisions for this PointCloud model
        tags: Text based labels for generic use, limited to 1000
        thumbnail: Thumbnail of the 3D model
        type_: CAD, PointCloud or Image360
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CognitePointCloudModel", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    revisions: Optional[list[CognitePointCloudRevision]] = Field(default=None, repr=False)

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CognitePointCloudModelWrite:
        """Convert this read version of Cognite point cloud model to the writing version."""
        return CognitePointCloudModelWrite(
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

    def as_apply(self) -> CognitePointCloudModelWrite:
        """Convert this read version of Cognite point cloud model to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, CognitePointCloudModel],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._cognite_file import CogniteFile
        from ._cognite_point_cloud_revision import CognitePointCloudRevision

        for instance in instances.values():
            if (
                isinstance(instance.thumbnail, dm.NodeId | str)
                and (thumbnail := nodes_by_id.get(instance.thumbnail))
                and isinstance(thumbnail, CogniteFile)
            ):
                instance.thumbnail = thumbnail
        for node in nodes_by_id.values():
            if (
                isinstance(node, CognitePointCloudRevision)
                and node.model_3d is not None
                and (model_3d := instances.get(as_pygen_node_id(node.model_3d)))
            ):
                if model_3d.revisions is None:
                    model_3d.revisions = []
                model_3d.revisions.append(node)


class CognitePointCloudModelWrite(Cognite3DModelWrite):
    """This represents the writing version of Cognite point cloud model.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud model.
        data_record: The data record of the Cognite point cloud model node.
        aliases: Alternative names for the node
        description: Description of the instance
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
        thumbnail: Thumbnail of the 3D model
        type_: CAD, PointCloud or Image360
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CognitePointCloudModel", "v1")

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


class CognitePointCloudModelApply(CognitePointCloudModelWrite):
    def __new__(cls, *args, **kwargs) -> CognitePointCloudModelApply:
        warnings.warn(
            "CognitePointCloudModelApply is deprecated and will be removed in v1.0. "
            "Use CognitePointCloudModelWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CognitePointCloudModel.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CognitePointCloudModelList(DomainModelList[CognitePointCloudModel]):
    """List of Cognite point cloud models in the read version."""

    _INSTANCE = CognitePointCloudModel

    def as_write(self) -> CognitePointCloudModelWriteList:
        """Convert these read versions of Cognite point cloud model to the writing versions."""
        return CognitePointCloudModelWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CognitePointCloudModelWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def revisions(self) -> CognitePointCloudRevisionList:
        from ._cognite_point_cloud_revision import CognitePointCloudRevision, CognitePointCloudRevisionList

        return CognitePointCloudRevisionList(
            [
                item
                for items in self.data
                for item in items.revisions or []
                if isinstance(item, CognitePointCloudRevision)
            ]
        )

    @property
    def thumbnail(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.thumbnail for item in self.data if isinstance(item.thumbnail, CogniteFile)])


class CognitePointCloudModelWriteList(DomainModelWriteList[CognitePointCloudModelWrite]):
    """List of Cognite point cloud models in the writing version."""

    _INSTANCE = CognitePointCloudModelWrite

    @property
    def thumbnail(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList(
            [item.thumbnail for item in self.data if isinstance(item.thumbnail, CogniteFileWrite)]
        )


class CognitePointCloudModelApplyList(CognitePointCloudModelWriteList): ...


def _create_cognite_point_cloud_model_filter(
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


class _CognitePointCloudModelQuery(NodeQueryCore[T_DomainModelList, CognitePointCloudModelList]):
    _view_id = CognitePointCloudModel._view_id
    _result_cls = CognitePointCloudModel
    _result_list_cls_end = CognitePointCloudModelList

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
        from ._cognite_file import _CogniteFileQuery
        from ._cognite_point_cloud_revision import _CognitePointCloudRevisionQuery

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

        if _CognitePointCloudRevisionQuery not in created_types:
            self.revisions = _CognitePointCloudRevisionQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "Cognite3DRevision", "v1").as_property_ref("model3D"),
                    direction="inwards",
                ),
                connection_name="revisions",
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

    def list_cognite_point_cloud_model(self, limit: int = DEFAULT_QUERY_LIMIT) -> CognitePointCloudModelList:
        return self._list(limit=limit)


class CognitePointCloudModelQuery(_CognitePointCloudModelQuery[CognitePointCloudModelList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CognitePointCloudModelList)
