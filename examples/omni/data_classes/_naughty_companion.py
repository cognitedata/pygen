from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
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
    "NaughtyCompanion",
    "NaughtyCompanionWrite",
    "NaughtyCompanionList",
    "NaughtyCompanionWriteList",
    "NaughtyCompanionFields",
    "NaughtyCompanionTextFields",
    "NaughtyCompanionGraphQL",
]


NaughtyCompanionTextFields = Literal["external_id", "name"]
NaughtyCompanionFields = Literal["external_id", "name"]

_NAUGHTYCOMPANION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
}


class NaughtyCompanionGraphQL(GraphQLCore):
    """This represents the reading version of naughty companion, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the naughty companion.
        data_record: The data record of the naughty companion node.
        name: The name field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "NaughtyCompanion", "1")
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

    def as_read(self) -> NaughtyCompanion:
        """Convert this GraphQL format of naughty companion to the reading format."""
        return NaughtyCompanion.model_validate(as_read_args(self))

    def as_write(self) -> NaughtyCompanionWrite:
        """Convert this GraphQL format of naughty companion to the writing format."""
        return NaughtyCompanionWrite.model_validate(as_write_args(self))


class NaughtyCompanion(DomainModel):
    """This represents the reading version of naughty companion.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the naughty companion.
        data_record: The data record of the naughty companion node.
        name: The name field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "NaughtyCompanion", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: Optional[str] = None

    def as_write(self) -> NaughtyCompanionWrite:
        """Convert this read version of naughty companion to the writing version."""
        return NaughtyCompanionWrite.model_validate(as_write_args(self))


class NaughtyCompanionWrite(DomainModelWrite):
    """This represents the writing version of naughty companion.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the naughty companion.
        data_record: The data record of the naughty companion node.
        name: The name field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = ("name",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "NaughtyCompanion", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    name: Optional[str] = None


class NaughtyCompanionList(DomainModelList[NaughtyCompanion]):
    """List of naughty companions in the read version."""

    _INSTANCE = NaughtyCompanion

    def as_write(self) -> NaughtyCompanionWriteList:
        """Convert these read versions of naughty companion to the writing versions."""
        return NaughtyCompanionWriteList([node.as_write() for node in self.data])


class NaughtyCompanionWriteList(DomainModelWriteList[NaughtyCompanionWrite]):
    """List of naughty companions in the writing version."""

    _INSTANCE = NaughtyCompanionWrite


def _create_naughty_companion_filter(
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


class _NaughtyCompanionQuery(NodeQueryCore[T_DomainModelList, NaughtyCompanionList]):
    _view_id = NaughtyCompanion._view_id
    _result_cls = NaughtyCompanion
    _result_list_cls_end = NaughtyCompanionList

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
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.name,
            ]
        )

    def list_naughty_companion(self, limit: int = DEFAULT_QUERY_LIMIT) -> NaughtyCompanionList:
        return self._list(limit=limit)


class NaughtyCompanionQuery(_NaughtyCompanionQuery[NaughtyCompanionList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, NaughtyCompanionList)
