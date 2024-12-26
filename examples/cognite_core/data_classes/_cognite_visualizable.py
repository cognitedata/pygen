from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union, no_type_check

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from pydantic import Field, field_validator, model_validator

from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelList,
    DomainModelWrite,
    DomainModelWriteList,
    DomainRelation,
    GraphQLCore,
    NodeQueryCore,
    QueryCore,
    ResourcesWrite,
    StringFilter,
    T_DomainModelList,
    as_direct_relation_reference,
    as_instance_dict_id,
    is_tuple_id,
)

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_3_d_object import (
        Cognite3DObject,
        Cognite3DObjectGraphQL,
        Cognite3DObjectList,
        Cognite3DObjectWrite,
        Cognite3DObjectWriteList,
    )


__all__ = [
    "CogniteVisualizable",
    "CogniteVisualizableWrite",
    "CogniteVisualizableApply",
    "CogniteVisualizableList",
    "CogniteVisualizableWriteList",
    "CogniteVisualizableApplyList",
    "CogniteVisualizableGraphQL",
]


CogniteVisualizableTextFields = Literal["external_id",]
CogniteVisualizableFields = Literal["external_id",]

_COGNITEVISUALIZABLE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class CogniteVisualizableGraphQL(GraphQLCore):
    """This represents the reading version of Cognite visualizable, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite visualizable.
        data_record: The data record of the Cognite visualizable node.
        object_3d: Direct relation to an Object3D instance representing the 3D resource
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteVisualizable", "v1")
    object_3d: Optional[Cognite3DObjectGraphQL] = Field(default=None, repr=False, alias="object3D")

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

    @field_validator("object_3d", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CogniteVisualizable:
        """Convert this GraphQL format of Cognite visualizable to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteVisualizable(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            object_3d=self.object_3d.as_read() if isinstance(self.object_3d, GraphQLCore) else self.object_3d,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteVisualizableWrite:
        """Convert this GraphQL format of Cognite visualizable to the writing format."""
        return CogniteVisualizableWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            object_3d=self.object_3d.as_write() if isinstance(self.object_3d, GraphQLCore) else self.object_3d,
        )


class CogniteVisualizable(DomainModel):
    """This represents the reading version of Cognite visualizable.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite visualizable.
        data_record: The data record of the Cognite visualizable node.
        object_3d: Direct relation to an Object3D instance representing the 3D resource
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteVisualizable", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    object_3d: Union[Cognite3DObject, str, dm.NodeId, None] = Field(default=None, repr=False, alias="object3D")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteVisualizableWrite:
        """Convert this read version of Cognite visualizable to the writing version."""
        return CogniteVisualizableWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            object_3d=self.object_3d.as_write() if isinstance(self.object_3d, DomainModel) else self.object_3d,
        )

    def as_apply(self) -> CogniteVisualizableWrite:
        """Convert this read version of Cognite visualizable to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, CogniteVisualizable],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._cognite_3_d_object import Cognite3DObject

        for instance in instances.values():
            if (
                isinstance(instance.object_3d, dm.NodeId | str)
                and (object_3d := nodes_by_id.get(instance.object_3d))
                and isinstance(object_3d, Cognite3DObject)
            ):
                instance.object_3d = object_3d


class CogniteVisualizableWrite(DomainModelWrite):
    """This represents the writing version of Cognite visualizable.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite visualizable.
        data_record: The data record of the Cognite visualizable node.
        object_3d: Direct relation to an Object3D instance representing the 3D resource
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteVisualizable", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    object_3d: Union[Cognite3DObjectWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="object3D")

    @field_validator("object_3d", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value

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

        if self.object_3d is not None:
            properties["object3D"] = {
                "space": self.space if isinstance(self.object_3d, str) else self.object_3d.space,
                "externalId": self.object_3d if isinstance(self.object_3d, str) else self.object_3d.external_id,
            }

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

        if isinstance(self.object_3d, DomainModelWrite):
            other_resources = self.object_3d._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class CogniteVisualizableApply(CogniteVisualizableWrite):
    def __new__(cls, *args, **kwargs) -> CogniteVisualizableApply:
        warnings.warn(
            "CogniteVisualizableApply is deprecated and will be removed in v1.0. "
            "Use CogniteVisualizableWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteVisualizable.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteVisualizableList(DomainModelList[CogniteVisualizable]):
    """List of Cognite visualizables in the read version."""

    _INSTANCE = CogniteVisualizable

    def as_write(self) -> CogniteVisualizableWriteList:
        """Convert these read versions of Cognite visualizable to the writing versions."""
        return CogniteVisualizableWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteVisualizableWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def object_3d(self) -> Cognite3DObjectList:
        from ._cognite_3_d_object import Cognite3DObject, Cognite3DObjectList

        return Cognite3DObjectList(
            [item.object_3d for item in self.data if isinstance(item.object_3d, Cognite3DObject)]
        )


class CogniteVisualizableWriteList(DomainModelWriteList[CogniteVisualizableWrite]):
    """List of Cognite visualizables in the writing version."""

    _INSTANCE = CogniteVisualizableWrite

    @property
    def object_3d(self) -> Cognite3DObjectWriteList:
        from ._cognite_3_d_object import Cognite3DObjectWrite, Cognite3DObjectWriteList

        return Cognite3DObjectWriteList(
            [item.object_3d for item in self.data if isinstance(item.object_3d, Cognite3DObjectWrite)]
        )


class CogniteVisualizableApplyList(CogniteVisualizableWriteList): ...


def _create_cognite_visualizable_filter(
    view_id: dm.ViewId,
    object_3d: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(object_3d, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(object_3d):
        filters.append(dm.filters.Equals(view_id.as_property_ref("object3D"), value=as_instance_dict_id(object_3d)))
    if object_3d and isinstance(object_3d, Sequence) and not isinstance(object_3d, str) and not is_tuple_id(object_3d):
        filters.append(
            dm.filters.In(view_id.as_property_ref("object3D"), values=[as_instance_dict_id(item) for item in object_3d])
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _CogniteVisualizableQuery(NodeQueryCore[T_DomainModelList, CogniteVisualizableList]):
    _view_id = CogniteVisualizable._view_id
    _result_cls = CogniteVisualizable
    _result_list_cls_end = CogniteVisualizableList

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
        from ._cognite_3_d_object import _Cognite3DObjectQuery

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

        if _Cognite3DObjectQuery not in created_types:
            self.object_3d = _Cognite3DObjectQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("object3D"),
                    direction="outwards",
                ),
                connection_name="object_3d",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])

    def list_cognite_visualizable(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteVisualizableList:
        return self._list(limit=limit)


class CogniteVisualizableQuery(_CogniteVisualizableQuery[CogniteVisualizableList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteVisualizableList)
