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
    DirectRelationFilter,
)
from omni.data_classes._sub_interface import SubInterface

if TYPE_CHECKING:
    from omni.data_classes._implementation_1 import (
        Implementation1,
        Implementation1List,
        Implementation1GraphQL,
        Implementation1Write,
        Implementation1WriteList,
    )


__all__ = [
    "Implementation1NonWriteable",
    "Implementation1NonWriteableList",
    "Implementation1NonWriteableFields",
    "Implementation1NonWriteableTextFields",
    "Implementation1NonWriteableGraphQL",
]


Implementation1NonWriteableTextFields = Literal["external_id", "main_value", "sub_value", "value_1"]
Implementation1NonWriteableFields = Literal["external_id", "main_value", "sub_value", "value_1"]

_IMPLEMENTATION1NONWRITEABLE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "main_value": "mainValue",
    "sub_value": "subValue",
    "value_1": "value1",
}


class Implementation1NonWriteableGraphQL(GraphQLCore):
    """This represents the reading version of implementation 1 non writeable, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 non writeable.
        data_record: The data record of the implementation 1 non writeable node.
        connection_value: The connection value field.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "Implementation1NonWriteable", "1")
    connection_value: Optional[Implementation1GraphQL] = Field(default=None, repr=False, alias="connectionValue")
    main_value: Optional[str] = Field(None, alias="mainValue")
    sub_value: Optional[str] = Field(None, alias="subValue")
    value_1: Optional[str] = Field(None, alias="value1")

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

    @field_validator("connection_value", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Implementation1NonWriteable:
        """Convert this GraphQL format of implementation 1 non writeable to the reading format."""
        return Implementation1NonWriteable.model_validate(as_read_args(self))


class Implementation1NonWriteable(SubInterface):
    """This represents the reading version of implementation 1 non writeable.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 non writeable.
        data_record: The data record of the implementation 1 non writeable node.
        connection_value: The connection value field.
        main_value: The main value field.
        sub_value: The sub value field.
        value_1: The value 1 field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "Implementation1NonWriteable", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_pygen_models", "Implementation1"
    )
    connection_value: Union[Implementation1, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="connectionValue"
    )
    value_1: Optional[str] = Field(None, alias="value1")

    @field_validator("connection_value", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)


class Implementation1NonWriteableList(DomainModelList[Implementation1NonWriteable]):
    """List of implementation 1 non writeables in the read version."""

    _INSTANCE = Implementation1NonWriteable


def _create_implementation_1_non_writeable_filter(
    view_id: dm.ViewId,
    connection_value: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    main_value: str | list[str] | None = None,
    main_value_prefix: str | None = None,
    sub_value: str | list[str] | None = None,
    sub_value_prefix: str | None = None,
    value_1: str | list[str] | None = None,
    value_1_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(connection_value, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(connection_value):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("connectionValue"), value=as_instance_dict_id(connection_value))
        )
    if (
        connection_value
        and isinstance(connection_value, Sequence)
        and not isinstance(connection_value, str)
        and not is_tuple_id(connection_value)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("connectionValue"),
                values=[as_instance_dict_id(item) for item in connection_value],
            )
        )
    if isinstance(main_value, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("mainValue"), value=main_value))
    if main_value and isinstance(main_value, list):
        filters.append(dm.filters.In(view_id.as_property_ref("mainValue"), values=main_value))
    if main_value_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("mainValue"), value=main_value_prefix))
    if isinstance(sub_value, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("subValue"), value=sub_value))
    if sub_value and isinstance(sub_value, list):
        filters.append(dm.filters.In(view_id.as_property_ref("subValue"), values=sub_value))
    if sub_value_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("subValue"), value=sub_value_prefix))
    if isinstance(value_1, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("value1"), value=value_1))
    if value_1 and isinstance(value_1, list):
        filters.append(dm.filters.In(view_id.as_property_ref("value1"), values=value_1))
    if value_1_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("value1"), value=value_1_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _Implementation1NonWriteableQuery(NodeQueryCore[T_DomainModelList, Implementation1NonWriteableList]):
    _view_id = Implementation1NonWriteable._view_id
    _result_cls = Implementation1NonWriteable
    _result_list_cls_end = Implementation1NonWriteableList

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
        from ._implementation_1 import _Implementation1Query

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

        if _Implementation1Query not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.connection_value = _Implementation1Query(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("connectionValue"),
                    direction="outwards",
                ),
                connection_name="connection_value",
                connection_property=ViewPropertyId(self._view_id, "connectionValue"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.connection_value_filter = DirectRelationFilter(self, self._view_id.as_property_ref("connectionValue"))
        self.main_value = StringFilter(self, self._view_id.as_property_ref("mainValue"))
        self.sub_value = StringFilter(self, self._view_id.as_property_ref("subValue"))
        self.value_1 = StringFilter(self, self._view_id.as_property_ref("value1"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.connection_value_filter,
                self.main_value,
                self.sub_value,
                self.value_1,
            ]
        )

    def list_implementation_1_non_writeable(self, limit: int = DEFAULT_QUERY_LIMIT) -> Implementation1NonWriteableList:
        return self._list(limit=limit)


class Implementation1NonWriteableQuery(_Implementation1NonWriteableQuery[Implementation1NonWriteableList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, Implementation1NonWriteableList)
