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
        type_: The type field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "Naughty", "1")
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
        type_: The type field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "Naughty", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    type_: Optional[str] = Field(None, alias="type")

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
        type_: The type field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = ("type_",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "Naughty", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    type_: Optional[str] = Field(None, alias="type")


class NaughtyList(DomainModelList[Naughty]):
    """List of naughties in the read version."""

    _INSTANCE = Naughty

    def as_write(self) -> NaughtyWriteList:
        """Convert these read versions of naughty to the writing versions."""
        return NaughtyWriteList([node.as_write() for node in self.data])


class NaughtyWriteList(DomainModelWriteList[NaughtyWrite]):
    """List of naughties in the writing version."""

    _INSTANCE = NaughtyWrite


def _create_naughty_filter(
    view_id: dm.ViewId,
    type_: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
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
        self.type_ = StringFilter(self, self._view_id.as_property_ref("type"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.type_,
            ]
        )

    def list_naughty(self, limit: int = DEFAULT_QUERY_LIMIT) -> NaughtyList:
        return self._list(limit=limit)


class NaughtyQuery(_NaughtyQuery[NaughtyList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, NaughtyList)
