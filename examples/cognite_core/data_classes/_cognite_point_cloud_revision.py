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
    BooleanFilter,
    DirectRelationFilter,
    IntFilter,
)
from cognite_core.data_classes._cognite_3_d_revision import Cognite3DRevision, Cognite3DRevisionWrite

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_point_cloud_model import (
        CognitePointCloudModel,
        CognitePointCloudModelList,
        CognitePointCloudModelGraphQL,
        CognitePointCloudModelWrite,
        CognitePointCloudModelWriteList,
    )


__all__ = [
    "CognitePointCloudRevision",
    "CognitePointCloudRevisionWrite",
    "CognitePointCloudRevisionList",
    "CognitePointCloudRevisionWriteList",
    "CognitePointCloudRevisionFields",
    "CognitePointCloudRevisionGraphQL",
]


CognitePointCloudRevisionTextFields = Literal["external_id",]
CognitePointCloudRevisionFields = Literal["external_id", "published", "revision_id", "status", "type_"]

_COGNITEPOINTCLOUDREVISION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "published": "published",
    "revision_id": "revisionId",
    "status": "status",
    "type_": "type",
}


class CognitePointCloudRevisionGraphQL(GraphQLCore, protected_namespaces=()):
    """This represents the reading version of Cognite point cloud revision, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud revision.
        data_record: The data record of the Cognite point cloud revision node.
        model_3d: .
        published: The published field.
        revision_id: The 3D API revision identifier for this PointCloud model
        status: The status field.
        type_: The type field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CognitePointCloudRevision", "v1")
    model_3d: Optional[CognitePointCloudModelGraphQL] = Field(default=None, repr=False, alias="model3D")
    published: Optional[bool] = None
    revision_id: Optional[int] = Field(None, alias="revisionId")
    status: Optional[Literal["Done", "Failed", "Processing", "Queued"]] = None
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

    @field_validator("model_3d", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> CognitePointCloudRevision:
        """Convert this GraphQL format of Cognite point cloud revision to the reading format."""
        return CognitePointCloudRevision.model_validate(as_read_args(self))

    def as_write(self) -> CognitePointCloudRevisionWrite:
        """Convert this GraphQL format of Cognite point cloud revision to the writing format."""
        return CognitePointCloudRevisionWrite.model_validate(as_write_args(self))


class CognitePointCloudRevision(Cognite3DRevision, protected_namespaces=()):
    """This represents the reading version of Cognite point cloud revision.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud revision.
        data_record: The data record of the Cognite point cloud revision node.
        model_3d: .
        published: The published field.
        revision_id: The 3D API revision identifier for this PointCloud model
        status: The status field.
        type_: The type field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CognitePointCloudRevision", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    model_3d: Union[CognitePointCloudModel, str, dm.NodeId, None] = Field(default=None, repr=False, alias="model3D")
    revision_id: Optional[int] = Field(None, alias="revisionId")

    @field_validator("model_3d", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> CognitePointCloudRevisionWrite:
        """Convert this read version of Cognite point cloud revision to the writing version."""
        return CognitePointCloudRevisionWrite.model_validate(as_write_args(self))


class CognitePointCloudRevisionWrite(Cognite3DRevisionWrite, protected_namespaces=()):
    """This represents the writing version of Cognite point cloud revision.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite point cloud revision.
        data_record: The data record of the Cognite point cloud revision node.
        model_3d: .
        published: The published field.
        revision_id: The 3D API revision identifier for this PointCloud model
        status: The status field.
        type_: The type field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "model_3d",
        "published",
        "revision_id",
        "status",
        "type_",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = ("model_3d",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CognitePointCloudRevision", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    model_3d: Union[CognitePointCloudModelWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="model3D"
    )
    revision_id: Optional[int] = Field(None, alias="revisionId")

    @field_validator("model_3d", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class CognitePointCloudRevisionList(DomainModelList[CognitePointCloudRevision]):
    """List of Cognite point cloud revisions in the read version."""

    _INSTANCE = CognitePointCloudRevision

    def as_write(self) -> CognitePointCloudRevisionWriteList:
        """Convert these read versions of Cognite point cloud revision to the writing versions."""
        return CognitePointCloudRevisionWriteList([node.as_write() for node in self.data])

    @property
    def model_3d(self) -> CognitePointCloudModelList:
        from ._cognite_point_cloud_model import CognitePointCloudModel, CognitePointCloudModelList

        return CognitePointCloudModelList(
            [item.model_3d for item in self.data if isinstance(item.model_3d, CognitePointCloudModel)]
        )


class CognitePointCloudRevisionWriteList(DomainModelWriteList[CognitePointCloudRevisionWrite]):
    """List of Cognite point cloud revisions in the writing version."""

    _INSTANCE = CognitePointCloudRevisionWrite

    @property
    def model_3d(self) -> CognitePointCloudModelWriteList:
        from ._cognite_point_cloud_model import CognitePointCloudModelWrite, CognitePointCloudModelWriteList

        return CognitePointCloudModelWriteList(
            [item.model_3d for item in self.data if isinstance(item.model_3d, CognitePointCloudModelWrite)]
        )


def _create_cognite_point_cloud_revision_filter(
    view_id: dm.ViewId,
    model_3d: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    published: bool | None = None,
    min_revision_id: int | None = None,
    max_revision_id: int | None = None,
    status: (
        Literal["Done", "Failed", "Processing", "Queued"]
        | list[Literal["Done", "Failed", "Processing", "Queued"]]
        | None
    ) = None,
    type_: Literal["CAD", "Image360", "PointCloud"] | list[Literal["CAD", "Image360", "PointCloud"]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(model_3d, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(model_3d):
        filters.append(dm.filters.Equals(view_id.as_property_ref("model3D"), value=as_instance_dict_id(model_3d)))
    if model_3d and isinstance(model_3d, Sequence) and not isinstance(model_3d, str) and not is_tuple_id(model_3d):
        filters.append(
            dm.filters.In(view_id.as_property_ref("model3D"), values=[as_instance_dict_id(item) for item in model_3d])
        )
    if isinstance(published, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("published"), value=published))
    if min_revision_id is not None or max_revision_id is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("revisionId"), gte=min_revision_id, lte=max_revision_id)
        )
    if isinstance(status, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("status"), value=status))
    if status and isinstance(status, list):
        filters.append(dm.filters.In(view_id.as_property_ref("status"), values=status))
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


class _CognitePointCloudRevisionQuery(NodeQueryCore[T_DomainModelList, CognitePointCloudRevisionList]):
    _view_id = CognitePointCloudRevision._view_id
    _result_cls = CognitePointCloudRevision
    _result_list_cls_end = CognitePointCloudRevisionList

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
        from ._cognite_point_cloud_model import _CognitePointCloudModelQuery

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
            _CognitePointCloudModelQuery not in created_types
            and len(creation_path) + 1 < global_config.max_select_depth
        ):
            self.model_3d = _CognitePointCloudModelQuery(
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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.model_3d_filter = DirectRelationFilter(self, self._view_id.as_property_ref("model3D"))
        self.published = BooleanFilter(self, self._view_id.as_property_ref("published"))
        self.revision_id = IntFilter(self, self._view_id.as_property_ref("revisionId"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.model_3d_filter,
                self.published,
                self.revision_id,
            ]
        )

    def list_cognite_point_cloud_revision(self, limit: int = DEFAULT_QUERY_LIMIT) -> CognitePointCloudRevisionList:
        return self._list(limit=limit)


class CognitePointCloudRevisionQuery(_CognitePointCloudRevisionQuery[CognitePointCloudRevisionList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CognitePointCloudRevisionList)
