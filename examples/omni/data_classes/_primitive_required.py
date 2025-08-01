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
    BooleanFilter,
    DateFilter,
    FloatFilter,
    IntFilter,
    TimestampFilter,
)


__all__ = [
    "PrimitiveRequired",
    "PrimitiveRequiredWrite",
    "PrimitiveRequiredList",
    "PrimitiveRequiredWriteList",
    "PrimitiveRequiredFields",
    "PrimitiveRequiredTextFields",
    "PrimitiveRequiredGraphQL",
]


PrimitiveRequiredTextFields = Literal["external_id", "text"]
PrimitiveRequiredFields = Literal[
    "external_id", "boolean", "date", "float_32", "float_64", "int_32", "int_64", "json_", "text", "timestamp"
]

_PRIMITIVEREQUIRED_PROPERTIES_BY_FIELD = {
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


class PrimitiveRequiredGraphQL(GraphQLCore):
    """This represents the reading version of primitive required, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required.
        data_record: The data record of the primitive required node.
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

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "PrimitiveRequired", "1")
    boolean: Optional[bool] = None
    date: Optional[datetime.date] = None
    float_32: Optional[float] = Field(None, alias="float32")
    float_64: Optional[float] = Field(None, alias="float64")
    int_32: Optional[int] = Field(None, alias="int32")
    int_64: Optional[int] = Field(None, alias="int64")
    json_: Optional[dict] = Field(None, alias="json")
    text: Optional[str] = None
    timestamp: Optional[datetime.datetime] = None

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

    def as_read(self) -> PrimitiveRequired:
        """Convert this GraphQL format of primitive required to the reading format."""
        return PrimitiveRequired.model_validate(as_read_args(self))

    def as_write(self) -> PrimitiveRequiredWrite:
        """Convert this GraphQL format of primitive required to the writing format."""
        return PrimitiveRequiredWrite.model_validate(as_write_args(self))


class PrimitiveRequired(DomainModel):
    """This represents the reading version of primitive required.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required.
        data_record: The data record of the primitive required node.
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "PrimitiveRequired", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    boolean: bool
    date: datetime.date
    float_32: float = Field(alias="float32")
    float_64: float = Field(alias="float64")
    int_32: int = Field(alias="int32")
    int_64: int = Field(alias="int64")
    json_: dict = Field(alias="json")
    text: str
    timestamp: datetime.datetime

    def as_write(self) -> PrimitiveRequiredWrite:
        """Convert this read version of primitive required to the writing version."""
        return PrimitiveRequiredWrite.model_validate(as_write_args(self))


class PrimitiveRequiredWrite(DomainModelWrite):
    """This represents the writing version of primitive required.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required.
        data_record: The data record of the primitive required node.
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "PrimitiveRequired", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    boolean: bool
    date: datetime.date
    float_32: float = Field(alias="float32")
    float_64: float = Field(alias="float64")
    int_32: int = Field(alias="int32")
    int_64: int = Field(alias="int64")
    json_: dict = Field(alias="json")
    text: str
    timestamp: datetime.datetime


class PrimitiveRequiredList(DomainModelList[PrimitiveRequired]):
    """List of primitive requireds in the read version."""

    _INSTANCE = PrimitiveRequired

    def as_write(self) -> PrimitiveRequiredWriteList:
        """Convert these read versions of primitive required to the writing versions."""
        return PrimitiveRequiredWriteList([node.as_write() for node in self.data])


class PrimitiveRequiredWriteList(DomainModelWriteList[PrimitiveRequiredWrite]):
    """List of primitive requireds in the writing version."""

    _INSTANCE = PrimitiveRequiredWrite


def _create_primitive_required_filter(
    view_id: dm.ViewId,
    boolean: bool | None = None,
    min_date: datetime.date | None = None,
    max_date: datetime.date | None = None,
    min_float_32: float | None = None,
    max_float_32: float | None = None,
    min_float_64: float | None = None,
    max_float_64: float | None = None,
    min_int_32: int | None = None,
    max_int_32: int | None = None,
    min_int_64: int | None = None,
    max_int_64: int | None = None,
    text: str | list[str] | None = None,
    text_prefix: str | None = None,
    min_timestamp: datetime.datetime | None = None,
    max_timestamp: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(boolean, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("boolean"), value=boolean))
    if min_date is not None or max_date is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("date"),
                gte=min_date.isoformat() if min_date else None,
                lte=max_date.isoformat() if max_date else None,
            )
        )
    if min_float_32 is not None or max_float_32 is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("float32"), gte=min_float_32, lte=max_float_32))
    if min_float_64 is not None or max_float_64 is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("float64"), gte=min_float_64, lte=max_float_64))
    if min_int_32 is not None or max_int_32 is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("int32"), gte=min_int_32, lte=max_int_32))
    if min_int_64 is not None or max_int_64 is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("int64"), gte=min_int_64, lte=max_int_64))
    if isinstance(text, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("text"), value=text))
    if text and isinstance(text, list):
        filters.append(dm.filters.In(view_id.as_property_ref("text"), values=text))
    if text_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("text"), value=text_prefix))
    if min_timestamp is not None or max_timestamp is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("timestamp"),
                gte=min_timestamp.isoformat(timespec="milliseconds") if min_timestamp else None,
                lte=max_timestamp.isoformat(timespec="milliseconds") if max_timestamp else None,
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


class _PrimitiveRequiredQuery(NodeQueryCore[T_DomainModelList, PrimitiveRequiredList]):
    _view_id = PrimitiveRequired._view_id
    _result_cls = PrimitiveRequired
    _result_list_cls_end = PrimitiveRequiredList

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
        self.boolean = BooleanFilter(self, self._view_id.as_property_ref("boolean"))
        self.date = DateFilter(self, self._view_id.as_property_ref("date"))
        self.float_32 = FloatFilter(self, self._view_id.as_property_ref("float32"))
        self.float_64 = FloatFilter(self, self._view_id.as_property_ref("float64"))
        self.int_32 = IntFilter(self, self._view_id.as_property_ref("int32"))
        self.int_64 = IntFilter(self, self._view_id.as_property_ref("int64"))
        self.text = StringFilter(self, self._view_id.as_property_ref("text"))
        self.timestamp = TimestampFilter(self, self._view_id.as_property_ref("timestamp"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.boolean,
                self.date,
                self.float_32,
                self.float_64,
                self.int_32,
                self.int_64,
                self.text,
                self.timestamp,
            ]
        )

    def list_primitive_required(self, limit: int = DEFAULT_QUERY_LIMIT) -> PrimitiveRequiredList:
        return self._list(limit=limit)


class PrimitiveRequiredQuery(_PrimitiveRequiredQuery[PrimitiveRequiredList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PrimitiveRequiredList)
