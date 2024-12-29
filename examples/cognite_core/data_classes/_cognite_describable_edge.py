from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, TYPE_CHECKING, Union

from cognite.client import data_modeling as dm, CogniteClient

from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainRelation,
    DomainRelationWrite,
    DomainRelationList,
    DomainRelationWriteList,
    GraphQLCore,
    ResourcesWrite,
    DomainModelList,
    T_DomainList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    is_tuple_id,
    EdgeQueryCore,
    NodeQueryCore,
    QueryCore,
    StringFilter,
    ViewPropertyId,
)


__all__ = [
    "CogniteDescribableEdge",
    "CogniteDescribableEdgeWrite",
    "CogniteDescribableEdgeApply",
    "CogniteDescribableEdgeList",
    "CogniteDescribableEdgeWriteList",
    "CogniteDescribableEdgeApplyList",
    "CogniteDescribableEdgeFields",
    "CogniteDescribableEdgeTextFields",
]


CogniteDescribableEdgeTextFields = Literal["external_id", "aliases", "description", "name", "tags"]
CogniteDescribableEdgeFields = Literal["external_id", "aliases", "description", "name", "tags"]
_COGNITEDESCRIBABLEEDGE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "name": "name",
    "tags": "tags",
}


class CogniteDescribableEdgeGraphQL(GraphQLCore):
    """This represents the reading version of Cognite describable edge, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite describable edge.
        data_record: The data record of the Cognite describable edge node.
        end_node: The end node of this edge.
        aliases: Alternative names for the node
        description: Description of the instance
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteDescribable", "v1")
    end_node: Union[dm.NodeId, None] = None
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[list[str]] = None

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CogniteDescribableEdge:
        """Convert this GraphQL format of Cognite describable edge to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteDescribableEdge(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            end_node=self.end_node.as_read() if isinstance(self.end_node, GraphQLCore) else self.end_node,
            aliases=self.aliases,
            description=self.description,
            name=self.name,
            tags=self.tags,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteDescribableEdgeWrite:
        """Convert this GraphQL format of Cognite describable edge to the writing format."""
        return CogniteDescribableEdgeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            end_node=self.end_node,
            aliases=self.aliases,
            description=self.description,
            name=self.name,
            tags=self.tags,
        )


class CogniteDescribableEdge(DomainRelation):
    """This represents the reading version of Cognite describable edge.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite describable edge.
        data_record: The data record of the Cognite describable edge edge.
        end_node: The end node of this edge.
        aliases: Alternative names for the node
        description: Description of the instance
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteDescribable", "v1")
    space: str = DEFAULT_INSTANCE_SPACE
    end_node: Union[str, dm.NodeId]
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[list[str]] = None

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteDescribableEdgeWrite:
        """Convert this read version of Cognite describable edge to the writing version."""
        return CogniteDescribableEdgeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            end_node=self.end_node,
            aliases=self.aliases,
            description=self.description,
            name=self.name,
            tags=self.tags,
        )

    def as_apply(self) -> CogniteDescribableEdgeWrite:
        """Convert this read version of Cognite describable edge to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


_EXPECTED_START_NODES_BY_END_NODE: dict[type[DomainModelWrite], set[type[DomainModelWrite]]] = {}


def _validate_end_node(start_node: DomainModelWrite, end_node: Union[str, dm.NodeId]) -> None:
    if isinstance(end_node, str | dm.NodeId):
        # Nothing to validate
        return
    if type(end_node) not in _EXPECTED_START_NODES_BY_END_NODE:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. "
            f"Should be one of {[t.__name__ for t in _EXPECTED_START_NODES_BY_END_NODE.keys()]}"
        )
    if type(start_node) not in _EXPECTED_START_NODES_BY_END_NODE[type(end_node)]:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. "
            f"Expected one of: {_EXPECTED_START_NODES_BY_END_NODE[type(end_node)]}"
        )


class CogniteDescribableEdgeWrite(DomainRelationWrite):
    """This represents the writing version of Cognite describable edge.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite describable edge.
        data_record: The data record of the Cognite describable edge edge.
        end_node: The end node of this edge.
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
    _validate_end_node = _validate_end_node

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteDescribable", "v1")
    end_node: Union[str, dm.NodeId]
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[list[str]] = None


class CogniteDescribableEdgeApply(CogniteDescribableEdgeWrite):
    def __new__(cls, *args, **kwargs) -> CogniteDescribableEdgeApply:
        warnings.warn(
            "CogniteDescribableEdgeApply is deprecated and will be removed in v1.0. "
            "Use CogniteDescribableEdgeWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteDescribableEdge.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteDescribableEdgeList(DomainRelationList[CogniteDescribableEdge]):
    """List of Cognite describable edges in the reading version."""

    _INSTANCE = CogniteDescribableEdge

    def as_write(self) -> CogniteDescribableEdgeWriteList:
        """Convert this read version of Cognite describable edge list to the writing version."""
        return CogniteDescribableEdgeWriteList([edge.as_write() for edge in self])

    def as_apply(self) -> CogniteDescribableEdgeWriteList:
        """Convert these read versions of Cognite describable edge list to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CogniteDescribableEdgeWriteList(DomainRelationWriteList[CogniteDescribableEdgeWrite]):
    """List of Cognite describable edges in the writing version."""

    _INSTANCE = CogniteDescribableEdgeWrite


class CogniteDescribableEdgeApplyList(CogniteDescribableEdgeWriteList): ...


def _create_cognite_describable_edge_filter(
    edge_type: dm.DirectRelationReference,
    view_id: dm.ViewId,
    start_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    start_node_space: str = DEFAULT_INSTANCE_SPACE,
    end_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    space_end_node: str = DEFAULT_INSTANCE_SPACE,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter:
    filters: list[dm.Filter] = [
        dm.filters.Equals(
            ["edge", "type"],
            {"space": edge_type.space, "externalId": edge_type.external_id},
        )
    ]
    if start_node and isinstance(start_node, str):
        filters.append(
            dm.filters.Equals(["edge", "startNode"], value={"space": start_node_space, "externalId": start_node})
        )
    if start_node and isinstance(start_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(
                ["edge", "startNode"], value=start_node.dump(camel_case=True, include_instance_type=False)
            )
        )
    if start_node and isinstance(start_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "startNode"],
                values=[
                    (
                        {"space": start_node_space, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in start_node
                ],
            )
        )
    if end_node and isinstance(end_node, str):
        filters.append(dm.filters.Equals(["edge", "endNode"], value={"space": space_end_node, "externalId": end_node}))
    if end_node and isinstance(end_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(["edge", "endNode"], value=end_node.dump(camel_case=True, include_instance_type=False))
        )
    if end_node and isinstance(end_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "endNode"],
                values=[
                    (
                        {"space": space_end_node, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in end_node
                ],
            )
        )
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
        filters.append(dm.filters.Prefix(["edge", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters)


class _CogniteDescribableEdgeQuery(EdgeQueryCore[T_DomainList, CogniteDescribableEdgeList]):
    _view_id = CogniteDescribableEdge._view_id
    _result_cls = CogniteDescribableEdge
    _result_list_cls_end = CogniteDescribableEdgeList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainList],
        end_node_cls: type[NodeQueryCore],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
    ):

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            None,
            connection_name,
            connection_property,
        )
        if end_node_cls not in created_types:
            self.end_node = end_node_cls(
                created_types=created_types.copy(),
                creation_path=self._creation_path,
                client=client,
                result_list_cls=result_list_cls,  # type: ignore[type-var]
                expression=dm.query.NodeResultSetExpression(),
                connection_property=ViewPropertyId(self._view_id, "end_node"),
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
