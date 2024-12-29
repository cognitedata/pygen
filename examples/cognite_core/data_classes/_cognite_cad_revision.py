from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

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
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    BooleanFilter,
    IntFilter,
)
from cognite_core.data_classes._cognite_3_d_revision import Cognite3DRevision, Cognite3DRevisionWrite

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_cad_model import (
        CogniteCADModel,
        CogniteCADModelList,
        CogniteCADModelGraphQL,
        CogniteCADModelWrite,
        CogniteCADModelWriteList,
    )


__all__ = [
    "CogniteCADRevision",
    "CogniteCADRevisionWrite",
    "CogniteCADRevisionApply",
    "CogniteCADRevisionList",
    "CogniteCADRevisionWriteList",
    "CogniteCADRevisionApplyList",
    "CogniteCADRevisionFields",
    "CogniteCADRevisionGraphQL",
]


CogniteCADRevisionTextFields = Literal["external_id",]
CogniteCADRevisionFields = Literal["external_id", "published", "revision_id", "status", "type_"]

_COGNITECADREVISION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "published": "published",
    "revision_id": "revisionId",
    "status": "status",
    "type_": "type",
}


class CogniteCADRevisionGraphQL(GraphQLCore, protected_namespaces=()):
    """This represents the reading version of Cognite cad revision, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cad revision.
        data_record: The data record of the Cognite cad revision node.
        model_3d: .
        published: The published field.
        revision_id: The 3D API revision identifier for this CAD model
        status: The status field.
        type_: The type field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteCADRevision", "v1")
    model_3d: Optional[CogniteCADModelGraphQL] = Field(default=None, repr=False, alias="model3D")
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CogniteCADRevision:
        """Convert this GraphQL format of Cognite cad revision to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteCADRevision(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            model_3d=self.model_3d.as_read() if isinstance(self.model_3d, GraphQLCore) else self.model_3d,
            published=self.published,
            revision_id=self.revision_id,
            status=self.status,
            type_=self.type_,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteCADRevisionWrite:
        """Convert this GraphQL format of Cognite cad revision to the writing format."""
        return CogniteCADRevisionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            model_3d=self.model_3d.as_write() if isinstance(self.model_3d, GraphQLCore) else self.model_3d,
            published=self.published,
            revision_id=self.revision_id,
            status=self.status,
            type_=self.type_,
        )


class CogniteCADRevision(Cognite3DRevision, protected_namespaces=()):
    """This represents the reading version of Cognite cad revision.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cad revision.
        data_record: The data record of the Cognite cad revision node.
        model_3d: .
        published: The published field.
        revision_id: The 3D API revision identifier for this CAD model
        status: The status field.
        type_: The type field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteCADRevision", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    revision_id: Optional[int] = Field(None, alias="revisionId")

    @field_validator("model_3d", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> CogniteCADRevisionWrite:
        """Convert this read version of Cognite cad revision to the writing version."""
        return CogniteCADRevisionWrite.model_validate(as_write_args(self))

    def as_apply(self) -> CogniteCADRevisionWrite:
        """Convert this read version of Cognite cad revision to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CogniteCADRevisionWrite(Cognite3DRevisionWrite, protected_namespaces=()):
    """This represents the writing version of Cognite cad revision.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cad revision.
        data_record: The data record of the Cognite cad revision node.
        model_3d: .
        published: The published field.
        revision_id: The 3D API revision identifier for this CAD model
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteCADRevision", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    revision_id: Optional[int] = Field(None, alias="revisionId")


class CogniteCADRevisionApply(CogniteCADRevisionWrite):
    def __new__(cls, *args, **kwargs) -> CogniteCADRevisionApply:
        warnings.warn(
            "CogniteCADRevisionApply is deprecated and will be removed in v1.0. "
            "Use CogniteCADRevisionWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteCADRevision.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteCADRevisionList(DomainModelList[CogniteCADRevision]):
    """List of Cognite cad revisions in the read version."""

    _INSTANCE = CogniteCADRevision

    def as_write(self) -> CogniteCADRevisionWriteList:
        """Convert these read versions of Cognite cad revision to the writing versions."""
        return CogniteCADRevisionWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteCADRevisionWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def model_3d(self) -> CogniteCADModelList:
        from ._cognite_cad_model import CogniteCADModel, CogniteCADModelList

        return CogniteCADModelList([item.model_3d for item in self.data if isinstance(item.model_3d, CogniteCADModel)])


class CogniteCADRevisionWriteList(DomainModelWriteList[CogniteCADRevisionWrite]):
    """List of Cognite cad revisions in the writing version."""

    _INSTANCE = CogniteCADRevisionWrite

    @property
    def model_3d(self) -> CogniteCADModelWriteList:
        from ._cognite_cad_model import CogniteCADModelWrite, CogniteCADModelWriteList

        return CogniteCADModelWriteList(
            [item.model_3d for item in self.data if isinstance(item.model_3d, CogniteCADModelWrite)]
        )


class CogniteCADRevisionApplyList(CogniteCADRevisionWriteList): ...


def _create_cognite_cad_revision_filter(
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _CogniteCADRevisionQuery(NodeQueryCore[T_DomainModelList, CogniteCADRevisionList]):
    _view_id = CogniteCADRevision._view_id
    _result_cls = CogniteCADRevision
    _result_list_cls_end = CogniteCADRevisionList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._cognite_cad_model import _CogniteCADModelQuery

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

        if _CogniteCADModelQuery not in created_types:
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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.published = BooleanFilter(self, self._view_id.as_property_ref("published"))
        self.revision_id = IntFilter(self, self._view_id.as_property_ref("revisionId"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.published,
                self.revision_id,
            ]
        )

    def list_cognite_cad_revision(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteCADRevisionList:
        return self._list(limit=limit)


class CogniteCADRevisionQuery(_CogniteCADRevisionQuery[CogniteCADRevisionList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteCADRevisionList)
