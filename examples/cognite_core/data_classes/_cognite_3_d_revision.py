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
)

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_3_d_model import (
        Cognite3DModel,
        Cognite3DModelList,
        Cognite3DModelGraphQL,
        Cognite3DModelWrite,
        Cognite3DModelWriteList,
    )


__all__ = [
    "Cognite3DRevision",
    "Cognite3DRevisionWrite",
    "Cognite3DRevisionList",
    "Cognite3DRevisionWriteList",
    "Cognite3DRevisionFields",
    "Cognite3DRevisionGraphQL",
]


Cognite3DRevisionTextFields = Literal["external_id",]
Cognite3DRevisionFields = Literal["external_id", "published", "status", "type_"]

_COGNITE3DREVISION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "published": "published",
    "status": "status",
    "type_": "type",
}


class Cognite3DRevisionGraphQL(GraphQLCore, protected_namespaces=()):
    """This represents the reading version of Cognite 3D revision, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D revision.
        data_record: The data record of the Cognite 3D revision node.
        model_3d: The model 3d field.
        published: The published field.
        status: The status field.
        type_: The type field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite3DRevision", "v1")
    model_3d: Optional[Cognite3DModelGraphQL] = Field(default=None, repr=False, alias="model3D")
    published: Optional[bool] = None
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

    def as_read(self) -> Cognite3DRevision:
        """Convert this GraphQL format of Cognite 3D revision to the reading format."""
        return Cognite3DRevision.model_validate(as_read_args(self))

    def as_write(self) -> Cognite3DRevisionWrite:
        """Convert this GraphQL format of Cognite 3D revision to the writing format."""
        return Cognite3DRevisionWrite.model_validate(as_write_args(self))


class Cognite3DRevision(DomainModel, protected_namespaces=()):
    """This represents the reading version of Cognite 3D revision.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D revision.
        data_record: The data record of the Cognite 3D revision node.
        model_3d: The model 3d field.
        published: The published field.
        status: The status field.
        type_: The type field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite3DRevision", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    model_3d: Union[Cognite3DModel, str, dm.NodeId, None] = Field(default=None, repr=False, alias="model3D")
    published: Optional[bool] = None
    status: Optional[Literal["Done", "Failed", "Processing", "Queued"]] | str = None
    type_: Optional[Literal["CAD", "Image360", "PointCloud"]] | str = Field(None, alias="type")

    @field_validator("model_3d", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> Cognite3DRevisionWrite:
        """Convert this read version of Cognite 3D revision to the writing version."""
        return Cognite3DRevisionWrite.model_validate(as_write_args(self))


class Cognite3DRevisionWrite(DomainModelWrite, protected_namespaces=()):
    """This represents the writing version of Cognite 3D revision.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D revision.
        data_record: The data record of the Cognite 3D revision node.
        model_3d: The model 3d field.
        published: The published field.
        status: The status field.
        type_: The type field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "model_3d",
        "published",
        "status",
        "type_",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = ("model_3d",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite3DRevision", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    model_3d: Union[Cognite3DModelWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="model3D")
    published: Optional[bool] = None
    status: Optional[Literal["Done", "Failed", "Processing", "Queued"]] = None
    type_: Optional[Literal["CAD", "Image360", "PointCloud"]] = Field(None, alias="type")

    @field_validator("model_3d", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class Cognite3DRevisionList(DomainModelList[Cognite3DRevision]):
    """List of Cognite 3D revisions in the read version."""

    _INSTANCE = Cognite3DRevision

    def as_write(self) -> Cognite3DRevisionWriteList:
        """Convert these read versions of Cognite 3D revision to the writing versions."""
        return Cognite3DRevisionWriteList([node.as_write() for node in self.data])

    @property
    def model_3d(self) -> Cognite3DModelList:
        from ._cognite_3_d_model import Cognite3DModel, Cognite3DModelList

        return Cognite3DModelList([item.model_3d for item in self.data if isinstance(item.model_3d, Cognite3DModel)])


class Cognite3DRevisionWriteList(DomainModelWriteList[Cognite3DRevisionWrite]):
    """List of Cognite 3D revisions in the writing version."""

    _INSTANCE = Cognite3DRevisionWrite

    @property
    def model_3d(self) -> Cognite3DModelWriteList:
        from ._cognite_3_d_model import Cognite3DModelWrite, Cognite3DModelWriteList

        return Cognite3DModelWriteList(
            [item.model_3d for item in self.data if isinstance(item.model_3d, Cognite3DModelWrite)]
        )


def _create_cognite_3_d_revision_filter(
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


class _Cognite3DRevisionQuery(NodeQueryCore[T_DomainModelList, Cognite3DRevisionList]):
    _view_id = Cognite3DRevision._view_id
    _result_cls = Cognite3DRevision
    _result_list_cls_end = Cognite3DRevisionList

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
        from ._cognite_3_d_model import _Cognite3DModelQuery

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

        if _Cognite3DModelQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.model_3d = _Cognite3DModelQuery(
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
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.model_3d_filter,
                self.published,
            ]
        )

    def list_cognite_3_d_revision(self, limit: int = DEFAULT_QUERY_LIMIT) -> Cognite3DRevisionList:
        return self._list(limit=limit)


class Cognite3DRevisionQuery(_Cognite3DRevisionQuery[Cognite3DRevisionList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, Cognite3DRevisionList)
