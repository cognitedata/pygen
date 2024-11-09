from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
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
    FloatFilter,
)


__all__ = [
    "Cognite3DTransformationNode",
    "Cognite3DTransformationNodeWrite",
    "Cognite3DTransformationNodeApply",
    "Cognite3DTransformationNodeList",
    "Cognite3DTransformationNodeWriteList",
    "Cognite3DTransformationNodeApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Cognite3DTransformationNode:
        """Convert this GraphQL format of Cognite 3D transformation node to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Cognite3DTransformationNode(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            euler_rotation_x=self.euler_rotation_x,
            euler_rotation_y=self.euler_rotation_y,
            euler_rotation_z=self.euler_rotation_z,
            scale_x=self.scale_x,
            scale_y=self.scale_y,
            scale_z=self.scale_z,
            translation_x=self.translation_x,
            translation_y=self.translation_y,
            translation_z=self.translation_z,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> Cognite3DTransformationNodeWrite:
        """Convert this GraphQL format of Cognite 3D transformation node to the writing format."""
        return Cognite3DTransformationNodeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            euler_rotation_x=self.euler_rotation_x,
            euler_rotation_y=self.euler_rotation_y,
            euler_rotation_z=self.euler_rotation_z,
            scale_x=self.scale_x,
            scale_y=self.scale_y,
            scale_z=self.scale_z,
            translation_x=self.translation_x,
            translation_y=self.translation_y,
            translation_z=self.translation_z,
        )


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
        return Cognite3DTransformationNodeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            euler_rotation_x=self.euler_rotation_x,
            euler_rotation_y=self.euler_rotation_y,
            euler_rotation_z=self.euler_rotation_z,
            scale_x=self.scale_x,
            scale_y=self.scale_y,
            scale_z=self.scale_z,
            translation_x=self.translation_x,
            translation_y=self.translation_y,
            translation_z=self.translation_z,
        )

    def as_apply(self) -> Cognite3DTransformationNodeWrite:
        """Convert this read version of Cognite 3D transformation node to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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

        if self.euler_rotation_x is not None or write_none:
            properties["eulerRotationX"] = self.euler_rotation_x

        if self.euler_rotation_y is not None or write_none:
            properties["eulerRotationY"] = self.euler_rotation_y

        if self.euler_rotation_z is not None or write_none:
            properties["eulerRotationZ"] = self.euler_rotation_z

        if self.scale_x is not None or write_none:
            properties["scaleX"] = self.scale_x

        if self.scale_y is not None or write_none:
            properties["scaleY"] = self.scale_y

        if self.scale_z is not None or write_none:
            properties["scaleZ"] = self.scale_z

        if self.translation_x is not None or write_none:
            properties["translationX"] = self.translation_x

        if self.translation_y is not None or write_none:
            properties["translationY"] = self.translation_y

        if self.translation_z is not None or write_none:
            properties["translationZ"] = self.translation_z

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


class Cognite3DTransformationNodeApply(Cognite3DTransformationNodeWrite):
    def __new__(cls, *args, **kwargs) -> Cognite3DTransformationNodeApply:
        warnings.warn(
            "Cognite3DTransformationNodeApply is deprecated and will be removed in v1.0. Use Cognite3DTransformationNodeWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Cognite3DTransformationNode.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class Cognite3DTransformationNodeList(DomainModelList[Cognite3DTransformationNode]):
    """List of Cognite 3D transformation nodes in the read version."""

    _INSTANCE = Cognite3DTransformationNode

    def as_write(self) -> Cognite3DTransformationNodeWriteList:
        """Convert these read versions of Cognite 3D transformation node to the writing versions."""
        return Cognite3DTransformationNodeWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> Cognite3DTransformationNodeWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class Cognite3DTransformationNodeWriteList(DomainModelWriteList[Cognite3DTransformationNodeWrite]):
    """List of Cognite 3D transformation nodes in the writing version."""

    _INSTANCE = Cognite3DTransformationNodeWrite


class Cognite3DTransformationNodeApplyList(Cognite3DTransformationNodeWriteList): ...


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
