from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import field_validator, model_validator

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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,
)


__all__ = [
    "CogniteDescribableNode",
    "CogniteDescribableNodeWrite",
    "CogniteDescribableNodeApply",
    "CogniteDescribableNodeList",
    "CogniteDescribableNodeWriteList",
    "CogniteDescribableNodeApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CogniteDescribableNode:
        """Convert this GraphQL format of Cognite describable node to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteDescribableNode(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            aliases=self.aliases,
            description=self.description,
            name=self.name,
            tags=self.tags,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteDescribableNodeWrite:
        """Convert this GraphQL format of Cognite describable node to the writing format."""
        return CogniteDescribableNodeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aliases=self.aliases,
            description=self.description,
            name=self.name,
            tags=self.tags,
        )


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
        return CogniteDescribableNodeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aliases=self.aliases,
            description=self.description,
            name=self.name,
            tags=self.tags,
        )

    def as_apply(self) -> CogniteDescribableNodeWrite:
        """Convert this read version of Cognite describable node to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteDescribable", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[list[str]] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.aliases is not None or write_none:
            properties["aliases"] = self.aliases

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.tags is not None or write_none:
            properties["tags"] = self.tags

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=as_direct_relation_reference(self.node_type),
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class CogniteDescribableNodeApply(CogniteDescribableNodeWrite):
    def __new__(cls, *args, **kwargs) -> CogniteDescribableNodeApply:
        warnings.warn(
            "CogniteDescribableNodeApply is deprecated and will be removed in v1.0. Use CogniteDescribableNodeWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteDescribableNode.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteDescribableNodeList(DomainModelList[CogniteDescribableNode]):
    """List of Cognite describable nodes in the read version."""

    _INSTANCE = CogniteDescribableNode

    def as_write(self) -> CogniteDescribableNodeWriteList:
        """Convert these read versions of Cognite describable node to the writing versions."""
        return CogniteDescribableNodeWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteDescribableNodeWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CogniteDescribableNodeWriteList(DomainModelWriteList[CogniteDescribableNodeWrite]):
    """List of Cognite describable nodes in the writing version."""

    _INSTANCE = CogniteDescribableNodeWrite


class CogniteDescribableNodeApplyList(CogniteDescribableNodeWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
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