from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
import pydantic
from pydantic import field_validator, model_validator, ValidationInfo

from field.client.data_classes._core import (
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

if TYPE_CHECKING:
    from field.client.data_classes._field import Field, FieldList, FieldGraphQL, FieldWrite, FieldWriteList


__all__ = [
    "Area",
    "AreaWrite",
    "AreaApply",
    "AreaList",
    "AreaWriteList",
    "AreaApplyList",
    "AreaGraphQL",
]


AreaTextFields = Literal["external_id",]
AreaFields = Literal["external_id",]

_AREA_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class AreaGraphQL(GraphQLCore):
    """This represents the reading version of area, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the area.
        data_record: The data record of the area node.
        field: The field field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("fields-space", "Area", "d6aca0459d82b7")
    field: Optional[FieldGraphQL] = pydantic.Field(default=None, repr=False)

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

    @field_validator("field", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Area:
        """Convert this GraphQL format of area to the reading format."""
        return Area.model_validate(as_read_args(self))

    def as_write(self) -> AreaWrite:
        """Convert this GraphQL format of area to the writing format."""
        return AreaWrite.model_validate(as_write_args(self))


class Area(DomainModel):
    """This represents the reading version of area.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the area.
        data_record: The data record of the area node.
        field: The field field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("fields-space", "Area", "d6aca0459d82b7")

    space: str
    node_type: Union[dm.DirectRelationReference, None] = None
    field: Union[Field, dm.NodeId, None] = pydantic.Field(default=None, repr=False)

    @field_validator("field", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> AreaWrite:
        """Convert this read version of area to the writing version."""
        return AreaWrite.model_validate(as_write_args(self))

    def as_apply(self) -> AreaWrite:
        """Convert this read version of area to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class AreaWrite(DomainModelWrite):
    """This represents the writing version of area.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the area.
        data_record: The data record of the area node.
        field: The field field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = ("field",)
    _direct_relations: ClassVar[tuple[str, ...]] = ("field",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("fields-space", "Area", "d6aca0459d82b7")

    space: str
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    field: Union[FieldWrite, dm.NodeId, None] = pydantic.Field(default=None, repr=False)

    @field_validator("field", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class AreaApply(AreaWrite):
    def __new__(cls, *args, **kwargs) -> AreaApply:
        warnings.warn(
            "AreaApply is deprecated and will be removed in v1.0. "
            "Use AreaWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Area.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class AreaList(DomainModelList[Area]):
    """List of areas in the read version."""

    _INSTANCE = Area

    def as_write(self) -> AreaWriteList:
        """Convert these read versions of area to the writing versions."""
        return AreaWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> AreaWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def field(self) -> FieldList:
        from ._field import Field, FieldList

        return FieldList([item.field for item in self.data if isinstance(item.field, Field)])


class AreaWriteList(DomainModelWriteList[AreaWrite]):
    """List of areas in the writing version."""

    _INSTANCE = AreaWrite

    @property
    def field(self) -> FieldWriteList:
        from ._field import FieldWrite, FieldWriteList

        return FieldWriteList([item.field for item in self.data if isinstance(item.field, FieldWrite)])


class AreaApplyList(AreaWriteList): ...


def _create_area_filter(
    view_id: dm.ViewId,
    field: (
        tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(field, dm.NodeId | dm.DirectRelationReference) or is_tuple_id(field):
        filters.append(dm.filters.Equals(view_id.as_property_ref("field"), value=as_instance_dict_id(field)))
    if field and isinstance(field, Sequence) and not isinstance(field, str) and not is_tuple_id(field):
        filters.append(
            dm.filters.In(view_id.as_property_ref("field"), values=[as_instance_dict_id(item) for item in field])
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


class _AreaQuery(NodeQueryCore[T_DomainModelList, AreaList]):
    _view_id = Area._view_id
    _result_cls = Area
    _result_list_cls_end = AreaList

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
        from ._field import _FieldQuery

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

        if _FieldQuery not in created_types:
            self.field = _FieldQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("field"),
                    direction="outwards",
                ),
                connection_name="field",
                connection_property=ViewPropertyId(self._view_id, "field"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.field_filter = DirectRelationFilter(self, self._view_id.as_property_ref("field"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.field_filter,
            ]
        )

    def list_area(self, limit: int = DEFAULT_QUERY_LIMIT) -> AreaList:
        return self._list(limit=limit)


class AreaQuery(_AreaQuery[AreaList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, AreaList)
