from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
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
    DirectRelationFilter,
)

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_3_d_object import (
        Cognite3DObject,
        Cognite3DObjectList,
        Cognite3DObjectGraphQL,
        Cognite3DObjectWrite,
        Cognite3DObjectWriteList,
    )


__all__ = [
    "CogniteVisualizable",
    "CogniteVisualizableWrite",
    "CogniteVisualizableList",
    "CogniteVisualizableWriteList",
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

    def as_read(self) -> CogniteVisualizable:
        """Convert this GraphQL format of Cognite visualizable to the reading format."""
        return CogniteVisualizable.model_validate(as_read_args(self))

    def as_write(self) -> CogniteVisualizableWrite:
        """Convert this GraphQL format of Cognite visualizable to the writing format."""
        return CogniteVisualizableWrite.model_validate(as_write_args(self))


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

    @field_validator("object_3d", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> CogniteVisualizableWrite:
        """Convert this read version of Cognite visualizable to the writing version."""
        return CogniteVisualizableWrite.model_validate(as_write_args(self))


class CogniteVisualizableWrite(DomainModelWrite):
    """This represents the writing version of Cognite visualizable.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite visualizable.
        data_record: The data record of the Cognite visualizable node.
        object_3d: Direct relation to an Object3D instance representing the 3D resource
    """

    _container_fields: ClassVar[tuple[str, ...]] = ("object_3d",)
    _direct_relations: ClassVar[tuple[str, ...]] = ("object_3d",)

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


class CogniteVisualizableList(DomainModelList[CogniteVisualizable]):
    """List of Cognite visualizables in the read version."""

    _INSTANCE = CogniteVisualizable

    def as_write(self) -> CogniteVisualizableWriteList:
        """Convert these read versions of Cognite visualizable to the writing versions."""
        return CogniteVisualizableWriteList([node.as_write() for node in self.data])

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
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _Cognite3DObjectQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "object3D"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.object_3d_filter = DirectRelationFilter(self, self._view_id.as_property_ref("object3D"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.object_3d_filter,
            ]
        )

    def list_cognite_visualizable(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteVisualizableList:
        return self._list(limit=limit)


class CogniteVisualizableQuery(_CogniteVisualizableQuery[CogniteVisualizableList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteVisualizableList)
