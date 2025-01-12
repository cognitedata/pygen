from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
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
)


__all__ = [
    "Field",
    "FieldWrite",
    "FieldApply",
    "FieldList",
    "FieldWriteList",
    "FieldApplyList",
    "FieldFields",
    "FieldTextFields",
    "FieldGraphQL",
]


FieldTextFields = Literal["external_id", "name"]
FieldFields = Literal["external_id", "name"]

_FIELD_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
}


class FieldGraphQL(GraphQLCore):
    """This represents the reading version of field, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the field.
        data_record: The data record of the field node.
        name: The name field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("fields-space", "Field", "a41a0c9a746803")
    name: Optional[str] = None

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

    def as_read(self) -> Field:
        """Convert this GraphQL format of field to the reading format."""
        return Field.model_validate(as_read_args(self))

    def as_write(self) -> FieldWrite:
        """Convert this GraphQL format of field to the writing format."""
        return FieldWrite.model_validate(as_write_args(self))


class Field(DomainModel):
    """This represents the reading version of field.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the field.
        data_record: The data record of the field node.
        name: The name field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("fields-space", "Field", "a41a0c9a746803")

    space: str
    node_type: Union[dm.DirectRelationReference, None] = None
    name: Optional[str] = None

    def as_write(self) -> FieldWrite:
        """Convert this read version of field to the writing version."""
        return FieldWrite.model_validate(as_write_args(self))

    def as_apply(self) -> FieldWrite:
        """Convert this read version of field to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class FieldWrite(DomainModelWrite):
    """This represents the writing version of field.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the field.
        data_record: The data record of the field node.
        name: The name field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = ("name",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("fields-space", "Field", "a41a0c9a746803")

    space: str
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    name: Optional[str] = None


class FieldApply(FieldWrite):
    def __new__(cls, *args, **kwargs) -> FieldApply:
        warnings.warn(
            "FieldApply is deprecated and will be removed in v1.0. "
            "Use FieldWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Field.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class FieldList(DomainModelList[Field]):
    """List of fields in the read version."""

    _INSTANCE = Field

    def as_write(self) -> FieldWriteList:
        """Convert these read versions of field to the writing versions."""
        return FieldWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> FieldWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class FieldWriteList(DomainModelWriteList[FieldWrite]):
    """List of fields in the writing version."""

    _INSTANCE = FieldWrite


class FieldApplyList(FieldWriteList): ...


def _create_field_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _FieldQuery(NodeQueryCore[T_DomainModelList, FieldList]):
    _view_id = Field._view_id
    _result_cls = Field
    _result_list_cls_end = FieldList

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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.name,
            ]
        )

    def list_field(self, limit: int = DEFAULT_QUERY_LIMIT) -> FieldList:
        return self._list(limit=limit)


class FieldQuery(_FieldQuery[FieldList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, FieldList)
