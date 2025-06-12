from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import field_validator, model_validator, ValidationInfo

from cognite_core.config import global_config
from cognite_core.data_classes._core import (
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
    "CogniteDescribableNode",
    "CogniteDescribableNodeWrite",
    "CogniteDescribableNodeList",
    "CogniteDescribableNodeWriteList",
    "CogniteDescribableNodeFields",
    "CogniteDescribableNodeTextFields",
    "CogniteDescribableNodeGraphQL",
]


CogniteDescribableNodeTextFields = Literal["external_id", "aliases", "description", "name", "tags"]
CogniteDescribableNodeFields = Literal["external_id", "aliases", "description", "name", "tags"]

_COGNITEDESCRIBABLENODE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "name": "name",
    "tags": "tags",
}


class CogniteDescribableNodeGraphQL(GraphQLCore):
    """This represents the reading version of Cognite describable node, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite describable node.
        data_record: The data record of the Cognite describable node node.
        aliases: Alternative names for the node
        description: Description of the instance
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteDescribable", "v1")
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[list[str]] = None

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

    def as_read(self) -> CogniteDescribableNode:
        """Convert this GraphQL format of Cognite describable node to the reading format."""
        return CogniteDescribableNode.model_validate(as_read_args(self))

    def as_write(self) -> CogniteDescribableNodeWrite:
        """Convert this GraphQL format of Cognite describable node to the writing format."""
        return CogniteDescribableNodeWrite.model_validate(as_write_args(self))


class CogniteDescribableNode(DomainModel):
    """This represents the reading version of Cognite describable node.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite describable node.
        data_record: The data record of the Cognite describable node node.
        aliases: Alternative names for the node
        description: Description of the instance
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteDescribable", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[list[str]] = None

    def as_write(self) -> CogniteDescribableNodeWrite:
        """Convert this read version of Cognite describable node to the writing version."""
        return CogniteDescribableNodeWrite.model_validate(as_write_args(self))


class CogniteDescribableNodeWrite(DomainModelWrite):
    """This represents the writing version of Cognite describable node.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite describable node.
        data_record: The data record of the Cognite describable node node.
        aliases: Alternative names for the node
        description: Description of the instance
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "description",
        "name",
        "tags",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteDescribable", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[list[str]] = None


class CogniteDescribableNodeList(DomainModelList[CogniteDescribableNode]):
    """List of Cognite describable nodes in the read version."""

    _INSTANCE = CogniteDescribableNode

    def as_write(self) -> CogniteDescribableNodeWriteList:
        """Convert these read versions of Cognite describable node to the writing versions."""
        return CogniteDescribableNodeWriteList([node.as_write() for node in self.data])


class CogniteDescribableNodeWriteList(DomainModelWriteList[CogniteDescribableNodeWrite]):
    """List of Cognite describable nodes in the writing version."""

    _INSTANCE = CogniteDescribableNodeWrite


def _create_cognite_describable_node_filter(
    view_id: dm.ViewId,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
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


class _CogniteDescribableNodeQuery(NodeQueryCore[T_DomainModelList, CogniteDescribableNodeList]):
    _view_id = CogniteDescribableNode._view_id
    _result_cls = CogniteDescribableNode
    _result_list_cls_end = CogniteDescribableNodeList

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
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.name,
            ]
        )

    def list_cognite_describable_node(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteDescribableNodeList:
        return self._list(limit=limit)


class CogniteDescribableNodeQuery(_CogniteDescribableNodeQuery[CogniteDescribableNodeList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteDescribableNodeList)
