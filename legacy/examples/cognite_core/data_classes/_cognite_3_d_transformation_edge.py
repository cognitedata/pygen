from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, TYPE_CHECKING, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field

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
    as_read_args,
    as_write_args,
    as_pygen_node_id,
    is_tuple_id,
    EdgeQueryCore,
    NodeQueryCore,
    QueryCore,
    StringFilter,
    ViewPropertyId,
    FloatFilter,
)


__all__ = [
    "Cognite3DTransformationEdge",
    "Cognite3DTransformationEdgeWrite",
    "Cognite3DTransformationEdgeList",
    "Cognite3DTransformationEdgeWriteList",
    "Cognite3DTransformationEdgeFields",
]


Cognite3DTransformationEdgeTextFields = Literal["external_id",]
Cognite3DTransformationEdgeFields = Literal[
    "external_id",
    "euler_rotation_x",
    "euler_rotation_y",
    "euler_rotation_z",
    "scale_x",
    "scale_y",
    "scale_z",
    "translation_x",
    "translation_y",
    "translation_z",
]
_COGNITE3DTRANSFORMATIONEDGE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "euler_rotation_x": "eulerRotationX",
    "euler_rotation_y": "eulerRotationY",
    "euler_rotation_z": "eulerRotationZ",
    "scale_x": "scaleX",
    "scale_y": "scaleY",
    "scale_z": "scaleZ",
    "translation_x": "translationX",
    "translation_y": "translationY",
    "translation_z": "translationZ",
}


class Cognite3DTransformationEdgeGraphQL(GraphQLCore):
    """This represents the reading version of Cognite 3D transformation edge, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D transformation edge.
        data_record: The data record of the Cognite 3D transformation edge node.
        end_node: The end node of this edge.
        euler_rotation_x: The rotation of the object around the X-axis in radians
        euler_rotation_y: The rotation of the object around the Y-axis in radians
        euler_rotation_z: The rotation of the object around the Z-axis in radians
        scale_x: The scaling factor applied to the object along the X-axis
        scale_y: The scaling factor applied to the object along the Y-axis
        scale_z: The scaling factor applied to the object along the Z-axis
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite3DTransformation", "v1")
    end_node: Union[dm.NodeId, None] = Field(None, alias="endNode")
    euler_rotation_x: Optional[float] = Field(None, alias="eulerRotationX")
    euler_rotation_y: Optional[float] = Field(None, alias="eulerRotationY")
    euler_rotation_z: Optional[float] = Field(None, alias="eulerRotationZ")
    scale_x: Optional[float] = Field(None, alias="scaleX")
    scale_y: Optional[float] = Field(None, alias="scaleY")
    scale_z: Optional[float] = Field(None, alias="scaleZ")
    translation_x: Optional[float] = Field(None, alias="translationX")
    translation_y: Optional[float] = Field(None, alias="translationY")
    translation_z: Optional[float] = Field(None, alias="translationZ")

    def as_read(self) -> Cognite3DTransformationEdge:
        """Convert this GraphQL format of Cognite 3D transformation edge to the reading format."""
        return Cognite3DTransformationEdge.model_validate(as_read_args(self))

    def as_write(self) -> Cognite3DTransformationEdgeWrite:
        """Convert this GraphQL format of Cognite 3D transformation edge to the writing format."""
        return Cognite3DTransformationEdgeWrite.model_validate(as_write_args(self))


class Cognite3DTransformationEdge(DomainRelation):
    """This represents the reading version of Cognite 3D transformation edge.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D transformation edge.
        data_record: The data record of the Cognite 3D transformation edge edge.
        end_node: The end node of this edge.
        euler_rotation_x: The rotation of the object around the X-axis in radians
        euler_rotation_y: The rotation of the object around the Y-axis in radians
        euler_rotation_z: The rotation of the object around the Z-axis in radians
        scale_x: The scaling factor applied to the object along the X-axis
        scale_y: The scaling factor applied to the object along the Y-axis
        scale_z: The scaling factor applied to the object along the Z-axis
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite3DTransformation", "v1")
    space: str = DEFAULT_INSTANCE_SPACE
    end_node: Union[str, dm.NodeId] = Field(alias="endNode")
    euler_rotation_x: Optional[float] = Field(None, alias="eulerRotationX")
    euler_rotation_y: Optional[float] = Field(None, alias="eulerRotationY")
    euler_rotation_z: Optional[float] = Field(None, alias="eulerRotationZ")
    scale_x: Optional[float] = Field(None, alias="scaleX")
    scale_y: Optional[float] = Field(None, alias="scaleY")
    scale_z: Optional[float] = Field(None, alias="scaleZ")
    translation_x: Optional[float] = Field(None, alias="translationX")
    translation_y: Optional[float] = Field(None, alias="translationY")
    translation_z: Optional[float] = Field(None, alias="translationZ")

    def as_write(self) -> Cognite3DTransformationEdgeWrite:
        """Convert this read version of Cognite 3D transformation edge to the writing version."""
        return Cognite3DTransformationEdgeWrite.model_validate(as_write_args(self))


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


class Cognite3DTransformationEdgeWrite(DomainRelationWrite):
    """This represents the writing version of Cognite 3D transformation edge.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D transformation edge.
        data_record: The data record of the Cognite 3D transformation edge edge.
        end_node: The end node of this edge.
        euler_rotation_x: The rotation of the object around the X-axis in radians
        euler_rotation_y: The rotation of the object around the Y-axis in radians
        euler_rotation_z: The rotation of the object around the Z-axis in radians
        scale_x: The scaling factor applied to the object along the X-axis
        scale_y: The scaling factor applied to the object along the Y-axis
        scale_z: The scaling factor applied to the object along the Z-axis
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "euler_rotation_x",
        "euler_rotation_y",
        "euler_rotation_z",
        "scale_x",
        "scale_y",
        "scale_z",
        "translation_x",
        "translation_y",
        "translation_z",
    )
    _validate_end_node = _validate_end_node

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite3DTransformation", "v1")
    end_node: Union[str, dm.NodeId] = Field(alias="endNode")
    euler_rotation_x: Optional[float] = Field(None, alias="eulerRotationX")
    euler_rotation_y: Optional[float] = Field(None, alias="eulerRotationY")
    euler_rotation_z: Optional[float] = Field(None, alias="eulerRotationZ")
    scale_x: Optional[float] = Field(None, alias="scaleX")
    scale_y: Optional[float] = Field(None, alias="scaleY")
    scale_z: Optional[float] = Field(None, alias="scaleZ")
    translation_x: Optional[float] = Field(None, alias="translationX")
    translation_y: Optional[float] = Field(None, alias="translationY")
    translation_z: Optional[float] = Field(None, alias="translationZ")


class Cognite3DTransformationEdgeList(DomainRelationList[Cognite3DTransformationEdge]):
    """List of Cognite 3D transformation edges in the reading version."""

    _INSTANCE = Cognite3DTransformationEdge

    def as_write(self) -> Cognite3DTransformationEdgeWriteList:
        """Convert this read version of Cognite 3D transformation edge list to the writing version."""
        return Cognite3DTransformationEdgeWriteList([edge.as_write() for edge in self])


class Cognite3DTransformationEdgeWriteList(DomainRelationWriteList[Cognite3DTransformationEdgeWrite]):
    """List of Cognite 3D transformation edges in the writing version."""

    _INSTANCE = Cognite3DTransformationEdgeWrite


def _create_cognite_3_d_transformation_edge_filter(
    edge_type: dm.DirectRelationReference,
    view_id: dm.ViewId,
    start_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    start_node_space: str = DEFAULT_INSTANCE_SPACE,
    end_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    space_end_node: str = DEFAULT_INSTANCE_SPACE,
    min_euler_rotation_x: float | None = None,
    max_euler_rotation_x: float | None = None,
    min_euler_rotation_y: float | None = None,
    max_euler_rotation_y: float | None = None,
    min_euler_rotation_z: float | None = None,
    max_euler_rotation_z: float | None = None,
    min_scale_x: float | None = None,
    max_scale_x: float | None = None,
    min_scale_y: float | None = None,
    max_scale_y: float | None = None,
    min_scale_z: float | None = None,
    max_scale_z: float | None = None,
    min_translation_x: float | None = None,
    max_translation_x: float | None = None,
    min_translation_y: float | None = None,
    max_translation_y: float | None = None,
    min_translation_z: float | None = None,
    max_translation_z: float | None = None,
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
    if min_euler_rotation_x is not None or max_euler_rotation_x is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("eulerRotationX"), gte=min_euler_rotation_x, lte=max_euler_rotation_x
            )
        )
    if min_euler_rotation_y is not None or max_euler_rotation_y is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("eulerRotationY"), gte=min_euler_rotation_y, lte=max_euler_rotation_y
            )
        )
    if min_euler_rotation_z is not None or max_euler_rotation_z is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("eulerRotationZ"), gte=min_euler_rotation_z, lte=max_euler_rotation_z
            )
        )
    if min_scale_x is not None or max_scale_x is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("scaleX"), gte=min_scale_x, lte=max_scale_x))
    if min_scale_y is not None or max_scale_y is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("scaleY"), gte=min_scale_y, lte=max_scale_y))
    if min_scale_z is not None or max_scale_z is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("scaleZ"), gte=min_scale_z, lte=max_scale_z))
    if min_translation_x is not None or max_translation_x is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("translationX"), gte=min_translation_x, lte=max_translation_x)
        )
    if min_translation_y is not None or max_translation_y is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("translationY"), gte=min_translation_y, lte=max_translation_y)
        )
    if min_translation_z is not None or max_translation_z is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("translationZ"), gte=min_translation_z, lte=max_translation_z)
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["edge", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters)


class _Cognite3DTransformationEdgeQuery(EdgeQueryCore[T_DomainList, Cognite3DTransformationEdgeList]):
    _view_id = Cognite3DTransformationEdge._view_id
    _result_cls = Cognite3DTransformationEdge
    _result_list_cls_end = Cognite3DTransformationEdgeList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainList],
        end_node_cls: type[NodeQueryCore],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
        self.euler_rotation_x = FloatFilter(self, self._view_id.as_property_ref("eulerRotationX"))
        self.euler_rotation_y = FloatFilter(self, self._view_id.as_property_ref("eulerRotationY"))
        self.euler_rotation_z = FloatFilter(self, self._view_id.as_property_ref("eulerRotationZ"))
        self.scale_x = FloatFilter(self, self._view_id.as_property_ref("scaleX"))
        self.scale_y = FloatFilter(self, self._view_id.as_property_ref("scaleY"))
        self.scale_z = FloatFilter(self, self._view_id.as_property_ref("scaleZ"))
        self.translation_x = FloatFilter(self, self._view_id.as_property_ref("translationX"))
        self.translation_y = FloatFilter(self, self._view_id.as_property_ref("translationY"))
        self.translation_z = FloatFilter(self, self._view_id.as_property_ref("translationZ"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.euler_rotation_x,
                self.euler_rotation_y,
                self.euler_rotation_z,
                self.scale_x,
                self.scale_y,
                self.scale_z,
                self.translation_x,
                self.translation_y,
                self.translation_z,
            ]
        )
