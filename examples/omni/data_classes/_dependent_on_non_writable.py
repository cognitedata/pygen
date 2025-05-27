from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

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

if TYPE_CHECKING:
    from omni.data_classes._implementation_1_non_writeable import (
        Implementation1NonWriteable,
        Implementation1NonWriteableList,
        Implementation1NonWriteableGraphQL,
    )


__all__ = [
    "DependentOnNonWritable",
    "DependentOnNonWritableWrite",
    "DependentOnNonWritableList",
    "DependentOnNonWritableWriteList",
    "DependentOnNonWritableFields",
    "DependentOnNonWritableTextFields",
    "DependentOnNonWritableGraphQL",
]


DependentOnNonWritableTextFields = Literal["external_id", "a_value"]
DependentOnNonWritableFields = Literal["external_id", "a_value"]

_DEPENDENTONNONWRITABLE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "a_value": "aValue",
}


class DependentOnNonWritableGraphQL(GraphQLCore):
    """This represents the reading version of dependent on non writable, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the dependent on non writable.
        data_record: The data record of the dependent on non writable node.
        a_value: The a value field.
        to_non_writable: The to non writable field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "DependentOnNonWritable", "1")
    a_value: Optional[str] = Field(None, alias="aValue")
    to_non_writable: Optional[list[Implementation1NonWriteableGraphQL]] = Field(
        default=None, repr=False, alias="toNonWritable"
    )

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

    @field_validator("to_non_writable", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> DependentOnNonWritable:
        """Convert this GraphQL format of dependent on non writable to the reading format."""
        return DependentOnNonWritable.model_validate(as_read_args(self))

    def as_write(self) -> DependentOnNonWritableWrite:
        """Convert this GraphQL format of dependent on non writable to the writing format."""
        return DependentOnNonWritableWrite.model_validate(as_write_args(self))


class DependentOnNonWritable(DomainModel):
    """This represents the reading version of dependent on non writable.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the dependent on non writable.
        data_record: The data record of the dependent on non writable node.
        a_value: The a value field.
        to_non_writable: The to non writable field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "DependentOnNonWritable", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_pygen_models", "DependentOnNonWritable"
    )
    a_value: Optional[str] = Field(None, alias="aValue")
    to_non_writable: Optional[list[Union[Implementation1NonWriteable, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="toNonWritable"
    )

    @field_validator("to_non_writable", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> DependentOnNonWritableWrite:
        """Convert this read version of dependent on non writable to the writing version."""
        return DependentOnNonWritableWrite.model_validate(as_write_args(self))


class DependentOnNonWritableWrite(DomainModelWrite):
    """This represents the writing version of dependent on non writable.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the dependent on non writable.
        data_record: The data record of the dependent on non writable node.
        a_value: The a value field.
        to_non_writable: The to non writable field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = ("a_value",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (
        ("to_non_writable", dm.DirectRelationReference("sp_pygen_models", "toNonWritable")),
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "DependentOnNonWritable", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference(
        "sp_pygen_models", "DependentOnNonWritable"
    )
    a_value: Optional[str] = Field(None, alias="aValue")
    to_non_writable: Optional[list[Union[str, dm.NodeId]]] = Field(default=None, alias="toNonWritable")

    @field_validator("to_non_writable", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class DependentOnNonWritableList(DomainModelList[DependentOnNonWritable]):
    """List of dependent on non writables in the read version."""

    _INSTANCE = DependentOnNonWritable

    def as_write(self) -> DependentOnNonWritableWriteList:
        """Convert these read versions of dependent on non writable to the writing versions."""
        return DependentOnNonWritableWriteList([node.as_write() for node in self.data])

    @property
    def to_non_writable(self) -> Implementation1NonWriteableList:
        from ._implementation_1_non_writeable import Implementation1NonWriteable, Implementation1NonWriteableList

        return Implementation1NonWriteableList(
            [
                item
                for items in self.data
                for item in items.to_non_writable or []
                if isinstance(item, Implementation1NonWriteable)
            ]
        )


class DependentOnNonWritableWriteList(DomainModelWriteList[DependentOnNonWritableWrite]):
    """List of dependent on non writables in the writing version."""

    _INSTANCE = DependentOnNonWritableWrite


def _create_dependent_on_non_writable_filter(
    view_id: dm.ViewId,
    a_value: str | list[str] | None = None,
    a_value_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(a_value, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("aValue"), value=a_value))
    if a_value and isinstance(a_value, list):
        filters.append(dm.filters.In(view_id.as_property_ref("aValue"), values=a_value))
    if a_value_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("aValue"), value=a_value_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _DependentOnNonWritableQuery(NodeQueryCore[T_DomainModelList, DependentOnNonWritableList]):
    _view_id = DependentOnNonWritable._view_id
    _result_cls = DependentOnNonWritable
    _result_list_cls_end = DependentOnNonWritableList

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
        from ._implementation_1_non_writeable import _Implementation1NonWriteableQuery

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
            _Implementation1NonWriteableQuery not in created_types
            and len(creation_path) + 1 < global_config.max_select_depth
        ):
            self.to_non_writable = _Implementation1NonWriteableQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="to_non_writable",
                connection_property=ViewPropertyId(self._view_id, "toNonWritable"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.a_value = StringFilter(self, self._view_id.as_property_ref("aValue"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.a_value,
            ]
        )

    def list_dependent_on_non_writable(self, limit: int = DEFAULT_QUERY_LIMIT) -> DependentOnNonWritableList:
        return self._list(limit=limit)


class DependentOnNonWritableQuery(_DependentOnNonWritableQuery[DependentOnNonWritableList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, DependentOnNonWritableList)
