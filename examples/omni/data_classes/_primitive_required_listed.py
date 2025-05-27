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
    "PrimitiveRequiredListed",
    "PrimitiveRequiredListedWrite",
    "PrimitiveRequiredListedList",
    "PrimitiveRequiredListedWriteList",
    "PrimitiveRequiredListedFields",
    "PrimitiveRequiredListedTextFields",
    "PrimitiveRequiredListedGraphQL",
]


PrimitiveRequiredListedTextFields = Literal["external_id", "text"]
PrimitiveRequiredListedFields = Literal[
    "external_id", "boolean", "date", "float_32", "float_64", "int_32", "int_64", "json_", "text", "timestamp"
]

_PRIMITIVEREQUIREDLISTED_PROPERTIES_BY_FIELD = {
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


class PrimitiveRequiredListedGraphQL(GraphQLCore):
    """This represents the reading version of primitive required listed, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required listed.
        data_record: The data record of the primitive required listed node.
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

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "PrimitiveRequiredListed", "1")
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

    def as_read(self) -> PrimitiveRequiredListed:
        """Convert this GraphQL format of primitive required listed to the reading format."""
        return PrimitiveRequiredListed.model_validate(as_read_args(self))

    def as_write(self) -> PrimitiveRequiredListedWrite:
        """Convert this GraphQL format of primitive required listed to the writing format."""
        return PrimitiveRequiredListedWrite.model_validate(as_write_args(self))


class PrimitiveRequiredListed(DomainModel):
    """This represents the reading version of primitive required listed.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required listed.
        data_record: The data record of the primitive required listed node.
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "PrimitiveRequiredListed", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    boolean: list[bool]
    date: list[datetime.date]
    float_32: list[float] = Field(alias="float32")
    float_64: list[float] = Field(alias="float64")
    int_32: list[int] = Field(alias="int32")
    int_64: list[int] = Field(alias="int64")
    json_: list[dict] = Field(alias="json")
    text: list[str]
    timestamp: list[datetime.datetime]

    def as_write(self) -> PrimitiveRequiredListedWrite:
        """Convert this read version of primitive required listed to the writing version."""
        return PrimitiveRequiredListedWrite.model_validate(as_write_args(self))


class PrimitiveRequiredListedWrite(DomainModelWrite):
    """This represents the writing version of primitive required listed.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required listed.
        data_record: The data record of the primitive required listed node.
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "PrimitiveRequiredListed", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    boolean: list[bool]
    date: list[datetime.date]
    float_32: list[float] = Field(alias="float32")
    float_64: list[float] = Field(alias="float64")
    int_32: list[int] = Field(alias="int32")
    int_64: list[int] = Field(alias="int64")
    json_: list[dict] = Field(alias="json")
    text: list[str]
    timestamp: list[datetime.datetime]


class PrimitiveRequiredListedList(DomainModelList[PrimitiveRequiredListed]):
    """List of primitive required listeds in the read version."""

    _INSTANCE = PrimitiveRequiredListed

    def as_write(self) -> PrimitiveRequiredListedWriteList:
        """Convert these read versions of primitive required listed to the writing versions."""
        return PrimitiveRequiredListedWriteList([node.as_write() for node in self.data])


class PrimitiveRequiredListedWriteList(DomainModelWriteList[PrimitiveRequiredListedWrite]):
    """List of primitive required listeds in the writing version."""

    _INSTANCE = PrimitiveRequiredListedWrite


def _create_primitive_required_listed_filter(
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


class _PrimitiveRequiredListedQuery(NodeQueryCore[T_DomainModelList, PrimitiveRequiredListedList]):
    _view_id = PrimitiveRequiredListed._view_id
    _result_cls = PrimitiveRequiredListed
    _result_list_cls_end = PrimitiveRequiredListedList

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

    def list_primitive_required_listed(self, limit: int = DEFAULT_QUERY_LIMIT) -> PrimitiveRequiredListedList:
        return self._list(limit=limit)


class PrimitiveRequiredListedQuery(_PrimitiveRequiredListedQuery[PrimitiveRequiredListedList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PrimitiveRequiredListedList)
