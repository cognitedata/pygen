from __future__ import annotations

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
    FloatFilter,
    IntFilter,
)


__all__ = [
    "PrimitiveWithDefaults",
    "PrimitiveWithDefaultsWrite",
    "PrimitiveWithDefaultsList",
    "PrimitiveWithDefaultsWriteList",
    "PrimitiveWithDefaultsFields",
    "PrimitiveWithDefaultsTextFields",
    "PrimitiveWithDefaultsGraphQL",
]


PrimitiveWithDefaultsTextFields = Literal["external_id", "default_string"]
PrimitiveWithDefaultsFields = Literal[
    "external_id", "auto_increment_int_32", "default_boolean", "default_float_32", "default_object", "default_string"
]

_PRIMITIVEWITHDEFAULTS_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "auto_increment_int_32": "autoIncrementInt32",
    "default_boolean": "defaultBoolean",
    "default_float_32": "defaultFloat32",
    "default_object": "defaultObject",
    "default_string": "defaultString",
}


class PrimitiveWithDefaultsGraphQL(GraphQLCore):
    """This represents the reading version of primitive with default, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive with default.
        data_record: The data record of the primitive with default node.
        auto_increment_int_32: The auto increment int 32 field.
        default_boolean: The default boolean field.
        default_float_32: The default float 32 field.
        default_object: The default object field.
        default_string: The default string field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "PrimitiveWithDefaults", "1")
    auto_increment_int_32: Optional[int] = Field(None, alias="autoIncrementInt32")
    default_boolean: Optional[bool] = Field(None, alias="defaultBoolean")
    default_float_32: Optional[float] = Field(None, alias="defaultFloat32")
    default_object: Optional[dict] = Field(None, alias="defaultObject")
    default_string: Optional[str] = Field(None, alias="defaultString")

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

    def as_read(self) -> PrimitiveWithDefaults:
        """Convert this GraphQL format of primitive with default to the reading format."""
        return PrimitiveWithDefaults.model_validate(as_read_args(self))

    def as_write(self) -> PrimitiveWithDefaultsWrite:
        """Convert this GraphQL format of primitive with default to the writing format."""
        return PrimitiveWithDefaultsWrite.model_validate(as_write_args(self))


class PrimitiveWithDefaults(DomainModel):
    """This represents the reading version of primitive with default.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive with default.
        data_record: The data record of the primitive with default node.
        auto_increment_int_32: The auto increment int 32 field.
        default_boolean: The default boolean field.
        default_float_32: The default float 32 field.
        default_object: The default object field.
        default_string: The default string field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "PrimitiveWithDefaults", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    auto_increment_int_32: int = Field(alias="autoIncrementInt32")
    default_boolean: Optional[bool] = Field(None, alias="defaultBoolean")
    default_float_32: Optional[float] = Field(None, alias="defaultFloat32")
    default_object: Optional[dict] = Field(None, alias="defaultObject")
    default_string: Optional[str] = Field(None, alias="defaultString")

    def as_write(self) -> PrimitiveWithDefaultsWrite:
        """Convert this read version of primitive with default to the writing version."""
        return PrimitiveWithDefaultsWrite.model_validate(as_write_args(self))


class PrimitiveWithDefaultsWrite(DomainModelWrite):
    """This represents the writing version of primitive with default.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive with default.
        data_record: The data record of the primitive with default node.
        auto_increment_int_32: The auto increment int 32 field.
        default_boolean: The default boolean field.
        default_float_32: The default float 32 field.
        default_object: The default object field.
        default_string: The default string field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "auto_increment_int_32",
        "default_boolean",
        "default_float_32",
        "default_object",
        "default_string",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "PrimitiveWithDefaults", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    auto_increment_int_32: int = Field(alias="autoIncrementInt32")
    default_boolean: Optional[bool] = Field(True, alias="defaultBoolean")
    default_float_32: Optional[float] = Field(0.42, alias="defaultFloat32")
    default_object: Optional[dict] = Field({"foo": "bar"}, alias="defaultObject")
    default_string: Optional[str] = Field("my default text", alias="defaultString")


class PrimitiveWithDefaultsList(DomainModelList[PrimitiveWithDefaults]):
    """List of primitive with defaults in the read version."""

    _INSTANCE = PrimitiveWithDefaults

    def as_write(self) -> PrimitiveWithDefaultsWriteList:
        """Convert these read versions of primitive with default to the writing versions."""
        return PrimitiveWithDefaultsWriteList([node.as_write() for node in self.data])


class PrimitiveWithDefaultsWriteList(DomainModelWriteList[PrimitiveWithDefaultsWrite]):
    """List of primitive with defaults in the writing version."""

    _INSTANCE = PrimitiveWithDefaultsWrite


def _create_primitive_with_default_filter(
    view_id: dm.ViewId,
    min_auto_increment_int_32: int | None = None,
    max_auto_increment_int_32: int | None = None,
    default_boolean: bool | None = None,
    min_default_float_32: float | None = None,
    max_default_float_32: float | None = None,
    default_string: str | list[str] | None = None,
    default_string_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if min_auto_increment_int_32 is not None or max_auto_increment_int_32 is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("autoIncrementInt32"),
                gte=min_auto_increment_int_32,
                lte=max_auto_increment_int_32,
            )
        )
    if isinstance(default_boolean, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("defaultBoolean"), value=default_boolean))
    if min_default_float_32 is not None or max_default_float_32 is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("defaultFloat32"), gte=min_default_float_32, lte=max_default_float_32
            )
        )
    if isinstance(default_string, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("defaultString"), value=default_string))
    if default_string and isinstance(default_string, list):
        filters.append(dm.filters.In(view_id.as_property_ref("defaultString"), values=default_string))
    if default_string_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("defaultString"), value=default_string_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _PrimitiveWithDefaultsQuery(NodeQueryCore[T_DomainModelList, PrimitiveWithDefaultsList]):
    _view_id = PrimitiveWithDefaults._view_id
    _result_cls = PrimitiveWithDefaults
    _result_list_cls_end = PrimitiveWithDefaultsList

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
        self.auto_increment_int_32 = IntFilter(self, self._view_id.as_property_ref("autoIncrementInt32"))
        self.default_boolean = BooleanFilter(self, self._view_id.as_property_ref("defaultBoolean"))
        self.default_float_32 = FloatFilter(self, self._view_id.as_property_ref("defaultFloat32"))
        self.default_string = StringFilter(self, self._view_id.as_property_ref("defaultString"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.auto_increment_int_32,
                self.default_boolean,
                self.default_float_32,
                self.default_string,
            ]
        )

    def list_primitive_with_default(self, limit: int = DEFAULT_QUERY_LIMIT) -> PrimitiveWithDefaultsList:
        return self._list(limit=limit)


class PrimitiveWithDefaultsQuery(_PrimitiveWithDefaultsQuery[PrimitiveWithDefaultsList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PrimitiveWithDefaultsList)
