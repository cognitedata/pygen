from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
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
    as_pygen_node_id,
    are_nodes_equal,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,
)

if TYPE_CHECKING:
    from ._implementation_1_non_writeable import Implementation1NonWriteable, Implementation1NonWriteableGraphQL


__all__ = [
    "DependentOnNonWritable",
    "DependentOnNonWritableWrite",
    "DependentOnNonWritableApply",
    "DependentOnNonWritableList",
    "DependentOnNonWritableWriteList",
    "DependentOnNonWritableApplyList",
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

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "DependentOnNonWritable", "1")
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> DependentOnNonWritable:
        """Convert this GraphQL format of dependent on non writable to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return DependentOnNonWritable(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            a_value=self.a_value,
            to_non_writable=[to_non_writable.as_read() for to_non_writable in self.to_non_writable or []],
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> DependentOnNonWritableWrite:
        """Convert this GraphQL format of dependent on non writable to the writing format."""
        return DependentOnNonWritableWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            a_value=self.a_value,
            to_non_writable=[to_non_writable.as_write() for to_non_writable in self.to_non_writable or []],
        )


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "DependentOnNonWritable", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "pygen-models", "DependentOnNonWritable"
    )
    a_value: Optional[str] = Field(None, alias="aValue")
    to_non_writable: Optional[list[Union[Implementation1NonWriteable, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="toNonWritable"
    )

    def as_write(self) -> DependentOnNonWritableWrite:
        """Convert this read version of dependent on non writable to the writing version."""
        return DependentOnNonWritableWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            a_value=self.a_value,
            to_non_writable=[
                to_non_writable.as_id() if isinstance(to_non_writable, DomainModel) else to_non_writable
                for to_non_writable in self.to_non_writable or []
            ],
        )

    def as_apply(self) -> DependentOnNonWritableWrite:
        """Convert this read version of dependent on non writable to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, DependentOnNonWritable],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._implementation_1_non_writeable import Implementation1NonWriteable

        for instance in instances.values():
            if edges := edges_by_source_node.get(instance.as_id()):
                to_non_writable: list[Implementation1NonWriteable | str | dm.NodeId] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("pygen-models", "toNonWritable") and isinstance(
                        value, (Implementation1NonWriteable, str, dm.NodeId)
                    ):
                        to_non_writable.append(value)

                instance.to_non_writable = to_non_writable or None


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "DependentOnNonWritable", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "pygen-models", "DependentOnNonWritable"
    )
    a_value: Optional[str] = Field(None, alias="aValue")
    to_non_writable: Optional[list[Union[str, dm.NodeId]]] = Field(default=None, alias="toNonWritable")

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

        if self.a_value is not None or write_none:
            properties["aValue"] = self.a_value

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("pygen-models", "toNonWritable")
        for to_non_writable in self.to_non_writable or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=to_non_writable,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        return resources


class DependentOnNonWritableApply(DependentOnNonWritableWrite):
    def __new__(cls, *args, **kwargs) -> DependentOnNonWritableApply:
        warnings.warn(
            "DependentOnNonWritableApply is deprecated and will be removed in v1.0. Use DependentOnNonWritableWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "DependentOnNonWritable.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class DependentOnNonWritableList(DomainModelList[DependentOnNonWritable]):
    """List of dependent on non writables in the read version."""

    _INSTANCE = DependentOnNonWritable

    def as_write(self) -> DependentOnNonWritableWriteList:
        """Convert these read versions of dependent on non writable to the writing versions."""
        return DependentOnNonWritableWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> DependentOnNonWritableWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class DependentOnNonWritableWriteList(DomainModelWriteList[DependentOnNonWritableWrite]):
    """List of dependent on non writables in the writing version."""

    _INSTANCE = DependentOnNonWritableWrite


class DependentOnNonWritableApplyList(DependentOnNonWritableWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
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
        )

        if _Implementation1NonWriteableQuery not in created_types:
            self.to_non_writable = _Implementation1NonWriteableQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                "to_non_writable",
            )

        self.a_value = StringFilter(self, self._view_id.as_property_ref("aValue"))
        self._filter_classes.extend(
            [
                self.a_value,
            ]
        )


class DependentOnNonWritableQuery(_DependentOnNonWritableQuery[DependentOnNonWritableList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, DependentOnNonWritableList)
