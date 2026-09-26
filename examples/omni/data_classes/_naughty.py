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

if TYPE_CHECKING:
    from omni.data_classes._naughty_companion import (
        NaughtyCompanion,
        NaughtyCompanionList,
        NaughtyCompanionGraphQL,
        NaughtyCompanionWrite,
        NaughtyCompanionWriteList,
    )


__all__ = [
    "Naughty",
    "NaughtyWrite",
    "NaughtyList",
    "NaughtyWriteList",
    "NaughtyFields",
    "NaughtyTextFields",
    "NaughtyGraphQL",
]


NaughtyTextFields = Literal["external_id", "type_"]
NaughtyFields = Literal["external_id", "type_"]

_NAUGHTY_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "type_": "type",
}


class NaughtyGraphQL(GraphQLCore):
    """This represents the reading version of naughty, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the naughty.
        data_record: The data record of the naughty node.
        friend: The friend field.
        type_: The type field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "Naughty", "1")
    friend: Optional[NaughtyCompanionGraphQL] = Field(default=None, repr=False)
    type_: Optional[str] = Field(None, alias="type")

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

    @field_validator("friend", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Naughty:
        """Convert this GraphQL format of naughty to the reading format."""
        return Naughty.model_validate(as_read_args(self))

    def as_write(self) -> NaughtyWrite:
        """Convert this GraphQL format of naughty to the writing format."""
        return NaughtyWrite.model_validate(as_write_args(self))


class Naughty(DomainModel):
    """This represents the reading version of naughty.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the naughty.
        data_record: The data record of the naughty node.
        friend: The friend field.
        type_: The type field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "Naughty", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    friend: Union[NaughtyCompanion, str, dm.NodeId, None] = Field(default=None, repr=False)
    type_: Optional[str] = Field(None, alias="type")

    @field_validator("friend", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> NaughtyWrite:
        """Convert this read version of naughty to the writing version."""
        return NaughtyWrite.model_validate(as_write_args(self))


class NaughtyWrite(DomainModelWrite):
    """This represents the writing version of naughty.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the naughty.
        data_record: The data record of the naughty node.
        friend: The friend field.
        type_: The type field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "friend",
        "type_",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = ("friend",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "Naughty", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    friend: Union[NaughtyCompanionWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    type_: Optional[str] = Field(None, alias="type")

    @field_validator("friend", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class NaughtyList(DomainModelList[Naughty]):
    """List of naughties in the read version."""

    _INSTANCE = Naughty

    def as_write(self) -> NaughtyWriteList:
        """Convert these read versions of naughty to the writing versions."""
        return NaughtyWriteList([node.as_write() for node in self.data])

    @property
    def friend(self) -> NaughtyCompanionList:
        from ._naughty_companion import NaughtyCompanion, NaughtyCompanionList

        return NaughtyCompanionList([item.friend for item in self.data if isinstance(item.friend, NaughtyCompanion)])


class NaughtyWriteList(DomainModelWriteList[NaughtyWrite]):
    """List of naughties in the writing version."""

    _INSTANCE = NaughtyWrite

    @property
    def friend(self) -> NaughtyCompanionWriteList:
        from ._naughty_companion import NaughtyCompanionWrite, NaughtyCompanionWriteList

        return NaughtyCompanionWriteList(
            [item.friend for item in self.data if isinstance(item.friend, NaughtyCompanionWrite)]
        )


def _create_naughty_filter(
    view_id: dm.ViewId,
    friend: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    type_: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(friend, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(friend):
        filters.append(dm.filters.Equals(view_id.as_property_ref("friend"), value=as_instance_dict_id(friend)))
    if friend and isinstance(friend, Sequence) and not isinstance(friend, str) and not is_tuple_id(friend):
        filters.append(
            dm.filters.In(view_id.as_property_ref("friend"), values=[as_instance_dict_id(item) for item in friend])
        )
    if isinstance(type_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type_))
    if type_ and isinstance(type_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type_))
    if type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _NaughtyQuery(NodeQueryCore[T_DomainModelList, NaughtyList]):
    _view_id = Naughty._view_id
    _result_cls = Naughty
    _result_list_cls_end = NaughtyList

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
        from ._naughty_companion import _NaughtyCompanionQuery

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

        if _NaughtyCompanionQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.friend = _NaughtyCompanionQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("friend"),
                    direction="outwards",
                ),
                connection_name="friend",
                connection_property=ViewPropertyId(self._view_id, "friend"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.friend_filter = DirectRelationFilter(self, self._view_id.as_property_ref("friend"))
        self.type_ = StringFilter(self, self._view_id.as_property_ref("type"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.friend_filter,
                self.type_,
            ]
        )

    def list_naughty(self, limit: int = DEFAULT_QUERY_LIMIT) -> NaughtyList:
        return self._list(limit=limit)


class NaughtyQuery(_NaughtyQuery[NaughtyList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, NaughtyList)
