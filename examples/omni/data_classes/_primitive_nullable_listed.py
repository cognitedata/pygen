from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from omni.config import global_config
from omni.data_classes._core import (
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
)


__all__ = [
    "PrimitiveNullableListed",
    "PrimitiveNullableListedWrite",
    "PrimitiveNullableListedList",
    "PrimitiveNullableListedWriteList",
    "PrimitiveNullableListedFields",
    "PrimitiveNullableListedTextFields",
    "PrimitiveNullableListedGraphQL",
]


PrimitiveNullableListedTextFields = Literal["external_id", "text"]
PrimitiveNullableListedFields = Literal[
    "external_id", "boolean", "date", "float_32", "float_64", "int_32", "int_64", "json_", "text", "timestamp"
]

_PRIMITIVENULLABLELISTED_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "boolean": "boolean",
    "date": "date",
    "float_32": "float32",
    "float_64": "float64",
    "int_32": "int32",
    "int_64": "int64",
    "json_": "json",
    "text": "text",
    "timestamp": "timestamp",
}


class PrimitiveNullableListedGraphQL(GraphQLCore):
    """This represents the reading version of primitive nullable listed, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive nullable listed.
        data_record: The data record of the primitive nullable listed node.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "PrimitiveNullableListed", "1")
    boolean: Optional[list[bool]] = None
    date: Optional[list[datetime.date]] = None
    float_32: Optional[list[float]] = Field(None, alias="float32")
    float_64: Optional[list[float]] = Field(None, alias="float64")
    int_32: Optional[list[int]] = Field(None, alias="int32")
    int_64: Optional[list[int]] = Field(None, alias="int64")
    json_: Optional[list[dict]] = Field(None, alias="json")
    text: Optional[list[str]] = None
    timestamp: Optional[list[datetime.datetime]] = None

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

    def as_read(self) -> PrimitiveNullableListed:
        """Convert this GraphQL format of primitive nullable listed to the reading format."""
        return PrimitiveNullableListed.model_validate(as_read_args(self))

    def as_write(self) -> PrimitiveNullableListedWrite:
        """Convert this GraphQL format of primitive nullable listed to the writing format."""
        return PrimitiveNullableListedWrite.model_validate(as_write_args(self))


class PrimitiveNullableListed(DomainModel):
    """This represents the reading version of primitive nullable listed.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive nullable listed.
        data_record: The data record of the primitive nullable listed node.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "PrimitiveNullableListed", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    boolean: Optional[list[bool]] = None
    date: Optional[list[datetime.date]] = None
    float_32: Optional[list[float]] = Field(None, alias="float32")
    float_64: Optional[list[float]] = Field(None, alias="float64")
    int_32: Optional[list[int]] = Field(None, alias="int32")
    int_64: Optional[list[int]] = Field(None, alias="int64")
    json_: Optional[list[dict]] = Field(None, alias="json")
    text: Optional[list[str]] = None
    timestamp: Optional[list[datetime.datetime]] = None

    def as_write(self) -> PrimitiveNullableListedWrite:
        """Convert this read version of primitive nullable listed to the writing version."""
        return PrimitiveNullableListedWrite.model_validate(as_write_args(self))


class PrimitiveNullableListedWrite(DomainModelWrite):
    """This represents the writing version of primitive nullable listed.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive nullable listed.
        data_record: The data record of the primitive nullable listed node.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "boolean",
        "date",
        "float_32",
        "float_64",
        "int_32",
        "int_64",
        "json_",
        "text",
        "timestamp",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "PrimitiveNullableListed", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    boolean: Optional[list[bool]] = None
    date: Optional[list[datetime.date]] = None
    float_32: Optional[list[float]] = Field(None, alias="float32")
    float_64: Optional[list[float]] = Field(None, alias="float64")
    int_32: Optional[list[int]] = Field(None, alias="int32")
    int_64: Optional[list[int]] = Field(None, alias="int64")
    json_: Optional[list[dict]] = Field(None, alias="json")
    text: Optional[list[str]] = None
    timestamp: Optional[list[datetime.datetime]] = None


class PrimitiveNullableListedList(DomainModelList[PrimitiveNullableListed]):
    """List of primitive nullable listeds in the read version."""

    _INSTANCE = PrimitiveNullableListed

    def as_write(self) -> PrimitiveNullableListedWriteList:
        """Convert these read versions of primitive nullable listed to the writing versions."""
        return PrimitiveNullableListedWriteList([node.as_write() for node in self.data])


class PrimitiveNullableListedWriteList(DomainModelWriteList[PrimitiveNullableListedWrite]):
    """List of primitive nullable listeds in the writing version."""

    _INSTANCE = PrimitiveNullableListedWrite


def _create_primitive_nullable_listed_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _PrimitiveNullableListedQuery(NodeQueryCore[T_DomainModelList, PrimitiveNullableListedList]):
    _view_id = PrimitiveNullableListed._view_id
    _result_cls = PrimitiveNullableListed
    _result_list_cls_end = PrimitiveNullableListedList

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
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
            ]
        )

    def list_primitive_nullable_listed(self, limit: int = DEFAULT_QUERY_LIMIT) -> PrimitiveNullableListedList:
        return self._list(limit=limit)


class PrimitiveNullableListedQuery(_PrimitiveNullableListedQuery[PrimitiveNullableListedList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PrimitiveNullableListedList)
