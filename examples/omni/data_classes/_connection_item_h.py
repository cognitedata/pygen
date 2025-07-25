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
    from omni.data_classes._main_interface import (
        MainInterface,
        MainInterfaceList,
        MainInterfaceGraphQL,
        MainInterfaceWrite,
        MainInterfaceWriteList,
    )


__all__ = [
    "ConnectionItemH",
    "ConnectionItemHWrite",
    "ConnectionItemHList",
    "ConnectionItemHWriteList",
    "ConnectionItemHFields",
    "ConnectionItemHTextFields",
    "ConnectionItemHGraphQL",
]


ConnectionItemHTextFields = Literal["external_id", "name"]
ConnectionItemHFields = Literal["external_id", "name"]

_CONNECTIONITEMH_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
}


class ConnectionItemHGraphQL(GraphQLCore):
    """This represents the reading version of connection item h, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item h.
        data_record: The data record of the connection item h node.
        direct_parent_multi: The direct parent multi field.
        direct_parent_single: The direct parent single field.
        name: The name field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemH", "1")
    direct_parent_multi: Optional[list[MainInterfaceGraphQL]] = Field(
        default=None, repr=False, alias="directParentMulti"
    )
    direct_parent_single: Optional[MainInterfaceGraphQL] = Field(default=None, repr=False, alias="directParentSingle")
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

    @field_validator("direct_parent_multi", "direct_parent_single", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ConnectionItemH:
        """Convert this GraphQL format of connection item h to the reading format."""
        return ConnectionItemH.model_validate(as_read_args(self))

    def as_write(self) -> ConnectionItemHWrite:
        """Convert this GraphQL format of connection item h to the writing format."""
        return ConnectionItemHWrite.model_validate(as_write_args(self))


class ConnectionItemH(DomainModel):
    """This represents the reading version of connection item h.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item h.
        data_record: The data record of the connection item h node.
        direct_parent_multi: The direct parent multi field.
        direct_parent_single: The direct parent single field.
        name: The name field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemH", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemH"
    )
    direct_parent_multi: Optional[list[Union[MainInterface, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="directParentMulti"
    )
    direct_parent_single: Union[MainInterface, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="directParentSingle"
    )
    name: Optional[str] = None

    @field_validator("direct_parent_single", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("direct_parent_multi", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ConnectionItemHWrite:
        """Convert this read version of connection item h to the writing version."""
        return ConnectionItemHWrite.model_validate(as_write_args(self))


class ConnectionItemHWrite(DomainModelWrite):
    """This represents the writing version of connection item h.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item h.
        data_record: The data record of the connection item h node.
        direct_parent_multi: The direct parent multi field.
        direct_parent_single: The direct parent single field.
        name: The name field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "direct_parent_multi",
        "direct_parent_single",
        "name",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "direct_parent_multi",
        "direct_parent_single",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "ConnectionItemH", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference(
        "sp_pygen_models", "ConnectionItemH"
    )
    direct_parent_multi: Optional[list[Union[MainInterfaceWrite, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="directParentMulti"
    )
    direct_parent_single: Union[MainInterfaceWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="directParentSingle"
    )
    name: Optional[str] = None

    @field_validator("direct_parent_multi", "direct_parent_single", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ConnectionItemHList(DomainModelList[ConnectionItemH]):
    """List of connection item hs in the read version."""

    _INSTANCE = ConnectionItemH

    def as_write(self) -> ConnectionItemHWriteList:
        """Convert these read versions of connection item h to the writing versions."""
        return ConnectionItemHWriteList([node.as_write() for node in self.data])

    @property
    def direct_parent_multi(self) -> MainInterfaceList:
        from ._main_interface import MainInterface, MainInterfaceList

        return MainInterfaceList(
            [item for items in self.data for item in items.direct_parent_multi or [] if isinstance(item, MainInterface)]
        )

    @property
    def direct_parent_single(self) -> MainInterfaceList:
        from ._main_interface import MainInterface, MainInterfaceList

        return MainInterfaceList(
            [item.direct_parent_single for item in self.data if isinstance(item.direct_parent_single, MainInterface)]
        )


class ConnectionItemHWriteList(DomainModelWriteList[ConnectionItemHWrite]):
    """List of connection item hs in the writing version."""

    _INSTANCE = ConnectionItemHWrite

    @property
    def direct_parent_multi(self) -> MainInterfaceWriteList:
        from ._main_interface import MainInterfaceWrite, MainInterfaceWriteList

        return MainInterfaceWriteList(
            [
                item
                for items in self.data
                for item in items.direct_parent_multi or []
                if isinstance(item, MainInterfaceWrite)
            ]
        )

    @property
    def direct_parent_single(self) -> MainInterfaceWriteList:
        from ._main_interface import MainInterfaceWrite, MainInterfaceWriteList

        return MainInterfaceWriteList(
            [
                item.direct_parent_single
                for item in self.data
                if isinstance(item.direct_parent_single, MainInterfaceWrite)
            ]
        )


def _create_connection_item_h_filter(
    view_id: dm.ViewId,
    direct_parent_multi: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    direct_parent_single: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(direct_parent_multi, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        direct_parent_multi
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("directParentMulti"), value=as_instance_dict_id(direct_parent_multi)
            )
        )
    if (
        direct_parent_multi
        and isinstance(direct_parent_multi, Sequence)
        and not isinstance(direct_parent_multi, str)
        and not is_tuple_id(direct_parent_multi)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directParentMulti"),
                values=[as_instance_dict_id(item) for item in direct_parent_multi],
            )
        )
    if isinstance(direct_parent_single, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(
        direct_parent_single
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("directParentSingle"), value=as_instance_dict_id(direct_parent_single)
            )
        )
    if (
        direct_parent_single
        and isinstance(direct_parent_single, Sequence)
        and not isinstance(direct_parent_single, str)
        and not is_tuple_id(direct_parent_single)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directParentSingle"),
                values=[as_instance_dict_id(item) for item in direct_parent_single],
            )
        )
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


class _ConnectionItemHQuery(NodeQueryCore[T_DomainModelList, ConnectionItemHList]):
    _view_id = ConnectionItemH._view_id
    _result_cls = ConnectionItemH
    _result_list_cls_end = ConnectionItemHList

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
        from ._main_interface import _MainInterfaceQuery

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

        if _MainInterfaceQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.direct_parent_multi = _MainInterfaceQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("directParentMulti"),
                    direction="outwards",
                ),
                connection_name="direct_parent_multi",
                connection_property=ViewPropertyId(self._view_id, "directParentMulti"),
            )

        if _MainInterfaceQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.direct_parent_single = _MainInterfaceQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("directParentSingle"),
                    direction="outwards",
                ),
                connection_name="direct_parent_single",
                connection_property=ViewPropertyId(self._view_id, "directParentSingle"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.direct_parent_single_filter = DirectRelationFilter(
            self, self._view_id.as_property_ref("directParentSingle")
        )
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.direct_parent_single_filter,
                self.name,
            ]
        )

    def list_connection_item_h(self, limit: int = DEFAULT_QUERY_LIMIT) -> ConnectionItemHList:
        return self._list(limit=limit)


class ConnectionItemHQuery(_ConnectionItemHQuery[ConnectionItemHList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ConnectionItemHList)
