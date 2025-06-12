from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

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
    FloatFilter,
)


__all__ = [
    "Cognite3DTransformationNode",
    "Cognite3DTransformationNodeWrite",
    "Cognite3DTransformationNodeList",
    "Cognite3DTransformationNodeWriteList",
    "Cognite3DTransformationNodeFields",
    "Cognite3DTransformationNodeGraphQL",
]


Cognite3DTransformationNodeTextFields = Literal["external_id",]
Cognite3DTransformationNodeFields = Literal[
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

_COGNITE3DTRANSFORMATIONNODE_PROPERTIES_BY_FIELD = {
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


class Cognite3DTransformationNodeGraphQL(GraphQLCore):
    """This represents the reading version of Cognite 3D transformation node, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D transformation node.
        data_record: The data record of the Cognite 3D transformation node node.
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
    euler_rotation_x: Optional[float] = Field(None, alias="eulerRotationX")
    euler_rotation_y: Optional[float] = Field(None, alias="eulerRotationY")
    euler_rotation_z: Optional[float] = Field(None, alias="eulerRotationZ")
    scale_x: Optional[float] = Field(None, alias="scaleX")
    scale_y: Optional[float] = Field(None, alias="scaleY")
    scale_z: Optional[float] = Field(None, alias="scaleZ")
    translation_x: Optional[float] = Field(None, alias="translationX")
    translation_y: Optional[float] = Field(None, alias="translationY")
    translation_z: Optional[float] = Field(None, alias="translationZ")

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

    def as_read(self) -> Cognite3DTransformationNode:
        """Convert this GraphQL format of Cognite 3D transformation node to the reading format."""
        return Cognite3DTransformationNode.model_validate(as_read_args(self))

    def as_write(self) -> Cognite3DTransformationNodeWrite:
        """Convert this GraphQL format of Cognite 3D transformation node to the writing format."""
        return Cognite3DTransformationNodeWrite.model_validate(as_write_args(self))


class Cognite3DTransformationNode(DomainModel):
    """This represents the reading version of Cognite 3D transformation node.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D transformation node.
        data_record: The data record of the Cognite 3D transformation node node.
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
    node_type: Union[dm.DirectRelationReference, None] = None
    euler_rotation_x: Optional[float] = Field(None, alias="eulerRotationX")
    euler_rotation_y: Optional[float] = Field(None, alias="eulerRotationY")
    euler_rotation_z: Optional[float] = Field(None, alias="eulerRotationZ")
    scale_x: Optional[float] = Field(None, alias="scaleX")
    scale_y: Optional[float] = Field(None, alias="scaleY")
    scale_z: Optional[float] = Field(None, alias="scaleZ")
    translation_x: Optional[float] = Field(None, alias="translationX")
    translation_y: Optional[float] = Field(None, alias="translationY")
    translation_z: Optional[float] = Field(None, alias="translationZ")

    def as_write(self) -> Cognite3DTransformationNodeWrite:
        """Convert this read version of Cognite 3D transformation node to the writing version."""
        return Cognite3DTransformationNodeWrite.model_validate(as_write_args(self))


class Cognite3DTransformationNodeWrite(DomainModelWrite):
    """This represents the writing version of Cognite 3D transformation node.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D transformation node.
        data_record: The data record of the Cognite 3D transformation node node.
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite3DTransformation", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    euler_rotation_x: Optional[float] = Field(None, alias="eulerRotationX")
    euler_rotation_y: Optional[float] = Field(None, alias="eulerRotationY")
    euler_rotation_z: Optional[float] = Field(None, alias="eulerRotationZ")
    scale_x: Optional[float] = Field(None, alias="scaleX")
    scale_y: Optional[float] = Field(None, alias="scaleY")
    scale_z: Optional[float] = Field(None, alias="scaleZ")
    translation_x: Optional[float] = Field(None, alias="translationX")
    translation_y: Optional[float] = Field(None, alias="translationY")
    translation_z: Optional[float] = Field(None, alias="translationZ")


class Cognite3DTransformationNodeList(DomainModelList[Cognite3DTransformationNode]):
    """List of Cognite 3D transformation nodes in the read version."""

    _INSTANCE = Cognite3DTransformationNode

    def as_write(self) -> Cognite3DTransformationNodeWriteList:
        """Convert these read versions of Cognite 3D transformation node to the writing versions."""
        return Cognite3DTransformationNodeWriteList([node.as_write() for node in self.data])


class Cognite3DTransformationNodeWriteList(DomainModelWriteList[Cognite3DTransformationNodeWrite]):
    """List of Cognite 3D transformation nodes in the writing version."""

    _INSTANCE = Cognite3DTransformationNodeWrite


def _create_cognite_3_d_transformation_node_filter(
    view_id: dm.ViewId,
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
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
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
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _Cognite3DTransformationNodeQuery(NodeQueryCore[T_DomainModelList, Cognite3DTransformationNodeList]):
    _view_id = Cognite3DTransformationNode._view_id
    _result_cls = Cognite3DTransformationNode
    _result_list_cls_end = Cognite3DTransformationNodeList

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

    def list_cognite_3_d_transformation_node(self, limit: int = DEFAULT_QUERY_LIMIT) -> Cognite3DTransformationNodeList:
        return self._list(limit=limit)


class Cognite3DTransformationNodeQuery(_Cognite3DTransformationNodeQuery[Cognite3DTransformationNodeList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, Cognite3DTransformationNodeList)
