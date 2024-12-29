from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

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
    as_write_args,
    is_tuple_id,
    select_best_node,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    FloatFilter,
)
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_360_image_annotation import (
        Cognite360ImageAnnotation,
        Cognite360ImageAnnotationList,
        Cognite360ImageAnnotationGraphQL,
        Cognite360ImageAnnotationWrite,
        Cognite360ImageAnnotationWriteList,
    )
    from cognite_core.data_classes._cognite_asset import (
        CogniteAsset,
        CogniteAssetList,
        CogniteAssetGraphQL,
        CogniteAssetWrite,
        CogniteAssetWriteList,
    )
    from cognite_core.data_classes._cognite_cad_node import (
        CogniteCADNode,
        CogniteCADNodeList,
        CogniteCADNodeGraphQL,
        CogniteCADNodeWrite,
        CogniteCADNodeWriteList,
    )
    from cognite_core.data_classes._cognite_point_cloud_volume import (
        CognitePointCloudVolume,
        CognitePointCloudVolumeList,
        CognitePointCloudVolumeGraphQL,
        CognitePointCloudVolumeWrite,
        CognitePointCloudVolumeWriteList,
    )


__all__ = [
    "Cognite3DObject",
    "Cognite3DObjectWrite",
    "Cognite3DObjectApply",
    "Cognite3DObjectList",
    "Cognite3DObjectWriteList",
    "Cognite3DObjectApplyList",
    "Cognite3DObjectFields",
    "Cognite3DObjectTextFields",
    "Cognite3DObjectGraphQL",
]


Cognite3DObjectTextFields = Literal["external_id", "aliases", "description", "name", "tags"]
Cognite3DObjectFields = Literal[
    "external_id", "aliases", "description", "name", "tags", "x_max", "x_min", "y_max", "y_min", "z_max", "z_min"
]

_COGNITE3DOBJECT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "name": "name",
    "tags": "tags",
    "x_max": "xMax",
    "x_min": "xMin",
    "y_max": "yMax",
    "y_min": "yMin",
    "z_max": "zMax",
    "z_min": "zMin",
}


class Cognite3DObjectGraphQL(GraphQLCore):
    """This represents the reading version of Cognite 3D object, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D object.
        data_record: The data record of the Cognite 3D object node.
        aliases: Alternative names for the node
        asset: Asset that is tied to this 3D object
        cad_nodes: List of up to 1000 CADNodes that represents the connected CogniteAsset
        description: Description of the instance
        images_360: Edge connection to Cognite360Image annotations that represents the connected CogniteAsset
        name: Name of the instance
        point_cloud_volumes: List of up to 1000 PointCloudVolumes that represents the connected CogniteAsset
        tags: Text based labels for generic use, limited to 1000
        x_max: Highest X value in bounding box
        x_min: Lowest X value in bounding box
        y_max: Highest Y value in bounding box
        y_min: Lowest Y value in bounding box
        z_max: Highest Z value in bounding box
        z_min: Lowest Z value in bounding box
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite3DObject", "v1")
    aliases: Optional[list[str]] = None
    asset: Optional[CogniteAssetGraphQL] = Field(default=None, repr=False)
    cad_nodes: Optional[list[CogniteCADNodeGraphQL]] = Field(default=None, repr=False, alias="cadNodes")
    description: Optional[str] = None
    images_360: Optional[list[Cognite360ImageAnnotationGraphQL]] = Field(default=None, repr=False, alias="images360")
    name: Optional[str] = None
    point_cloud_volumes: Optional[list[CognitePointCloudVolumeGraphQL]] = Field(
        default=None, repr=False, alias="pointCloudVolumes"
    )
    tags: Optional[list[str]] = None
    x_max: Optional[float] = Field(None, alias="xMax")
    x_min: Optional[float] = Field(None, alias="xMin")
    y_max: Optional[float] = Field(None, alias="yMax")
    y_min: Optional[float] = Field(None, alias="yMin")
    z_max: Optional[float] = Field(None, alias="zMax")
    z_min: Optional[float] = Field(None, alias="zMin")

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

    @field_validator("asset", "cad_nodes", "images_360", "point_cloud_volumes", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Cognite3DObject:
        """Convert this GraphQL format of Cognite 3D object to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Cognite3DObject(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            aliases=self.aliases,
            asset=self.asset.as_read() if isinstance(self.asset, GraphQLCore) else self.asset,
            cad_nodes=[cad_node.as_read() for cad_node in self.cad_nodes] if self.cad_nodes is not None else None,
            description=self.description,
            images_360=(
                [images_360.as_read() for images_360 in self.images_360] if self.images_360 is not None else None
            ),
            name=self.name,
            point_cloud_volumes=(
                [point_cloud_volume.as_read() for point_cloud_volume in self.point_cloud_volumes]
                if self.point_cloud_volumes is not None
                else None
            ),
            tags=self.tags,
            x_max=self.x_max,
            x_min=self.x_min,
            y_max=self.y_max,
            y_min=self.y_min,
            z_max=self.z_max,
            z_min=self.z_min,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> Cognite3DObjectWrite:
        """Convert this GraphQL format of Cognite 3D object to the writing format."""
        return Cognite3DObjectWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aliases=self.aliases,
            description=self.description,
            images_360=(
                [images_360.as_write() for images_360 in self.images_360] if self.images_360 is not None else None
            ),
            name=self.name,
            tags=self.tags,
            x_max=self.x_max,
            x_min=self.x_min,
            y_max=self.y_max,
            y_min=self.y_min,
            z_max=self.z_max,
            z_min=self.z_min,
        )


class Cognite3DObject(CogniteDescribableNode):
    """This represents the reading version of Cognite 3D object.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D object.
        data_record: The data record of the Cognite 3D object node.
        aliases: Alternative names for the node
        asset: Asset that is tied to this 3D object
        cad_nodes: List of up to 1000 CADNodes that represents the connected CogniteAsset
        description: Description of the instance
        images_360: Edge connection to Cognite360Image annotations that represents the connected CogniteAsset
        name: Name of the instance
        point_cloud_volumes: List of up to 1000 PointCloudVolumes that represents the connected CogniteAsset
        tags: Text based labels for generic use, limited to 1000
        x_max: Highest X value in bounding box
        x_min: Lowest X value in bounding box
        y_max: Highest Y value in bounding box
        y_min: Lowest Y value in bounding box
        z_max: Highest Z value in bounding box
        z_min: Lowest Z value in bounding box
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite3DObject", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    asset: Optional[CogniteAsset] = Field(default=None, repr=False)
    cad_nodes: Optional[list[CogniteCADNode]] = Field(default=None, repr=False, alias="cadNodes")
    images_360: Optional[list[Cognite360ImageAnnotation]] = Field(default=None, repr=False, alias="images360")
    point_cloud_volumes: Optional[list[CognitePointCloudVolume]] = Field(
        default=None, repr=False, alias="pointCloudVolumes"
    )
    x_max: Optional[float] = Field(None, alias="xMax")
    x_min: Optional[float] = Field(None, alias="xMin")
    y_max: Optional[float] = Field(None, alias="yMax")
    y_min: Optional[float] = Field(None, alias="yMin")
    z_max: Optional[float] = Field(None, alias="zMax")
    z_min: Optional[float] = Field(None, alias="zMin")

    @field_validator("asset", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("cad_nodes", "images_360", "point_cloud_volumes", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> Cognite3DObjectWrite:
        """Convert this read version of Cognite 3D object to the writing version."""
        return Cognite3DObjectWrite.model_validate(as_write_args(self))

    def as_apply(self) -> Cognite3DObjectWrite:
        """Convert this read version of Cognite 3D object to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class Cognite3DObjectWrite(CogniteDescribableNodeWrite):
    """This represents the writing version of Cognite 3D object.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 3D object.
        data_record: The data record of the Cognite 3D object node.
        aliases: Alternative names for the node
        description: Description of the instance
        images_360: Edge connection to Cognite360Image annotations that represents the connected CogniteAsset
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
        x_max: Highest X value in bounding box
        x_min: Lowest X value in bounding box
        y_max: Highest Y value in bounding box
        y_min: Lowest Y value in bounding box
        z_max: Highest Z value in bounding box
        z_min: Lowest Z value in bounding box
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "description",
        "name",
        "tags",
        "x_max",
        "x_min",
        "y_max",
        "y_min",
        "z_max",
        "z_min",
    )
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (
        ("images_360", dm.DirectRelationReference("cdf_cdm", "image-360-annotation")),
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite3DObject", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    images_360: Optional[list[Cognite360ImageAnnotationWrite]] = Field(default=None, repr=False, alias="images360")
    x_max: Optional[float] = Field(None, alias="xMax")
    x_min: Optional[float] = Field(None, alias="xMin")
    y_max: Optional[float] = Field(None, alias="yMax")
    y_min: Optional[float] = Field(None, alias="yMin")
    z_max: Optional[float] = Field(None, alias="zMax")
    z_min: Optional[float] = Field(None, alias="zMin")

    @field_validator("images_360", mode="before")
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

        if self.aliases is not None or write_none:
            properties["aliases"] = self.aliases

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.tags is not None or write_none:
            properties["tags"] = self.tags

        if self.x_max is not None or write_none:
            properties["xMax"] = self.x_max

        if self.x_min is not None or write_none:
            properties["xMin"] = self.x_min

        if self.y_max is not None or write_none:
            properties["yMax"] = self.y_max

        if self.y_min is not None or write_none:
            properties["yMin"] = self.y_min

        if self.z_max is not None or write_none:
            properties["zMax"] = self.z_max

        if self.z_min is not None or write_none:
            properties["zMin"] = self.z_min

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

        for images_360 in self.images_360 or []:
            if isinstance(images_360, DomainRelationWrite):
                other_resources = images_360._to_instances_write(
                    cache,
                    self,
                    dm.DirectRelationReference("cdf_cdm", "image-360-annotation"),
                )
                resources.extend(other_resources)

        return resources


class Cognite3DObjectApply(Cognite3DObjectWrite):
    def __new__(cls, *args, **kwargs) -> Cognite3DObjectApply:
        warnings.warn(
            "Cognite3DObjectApply is deprecated and will be removed in v1.0. "
            "Use Cognite3DObjectWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Cognite3DObject.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class Cognite3DObjectList(DomainModelList[Cognite3DObject]):
    """List of Cognite 3D objects in the read version."""

    _INSTANCE = Cognite3DObject

    def as_write(self) -> Cognite3DObjectWriteList:
        """Convert these read versions of Cognite 3D object to the writing versions."""
        return Cognite3DObjectWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> Cognite3DObjectWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def asset(self) -> CogniteAssetList:
        from ._cognite_asset import CogniteAsset, CogniteAssetList

        return CogniteAssetList([item.asset for item in self.data if isinstance(item.asset, CogniteAsset)])

    @property
    def cad_nodes(self) -> CogniteCADNodeList:
        from ._cognite_cad_node import CogniteCADNode, CogniteCADNodeList

        return CogniteCADNodeList(
            [item for items in self.data for item in items.cad_nodes or [] if isinstance(item, CogniteCADNode)]
        )

    @property
    def images_360(self) -> Cognite360ImageAnnotationList:
        from ._cognite_360_image_annotation import Cognite360ImageAnnotation, Cognite360ImageAnnotationList

        return Cognite360ImageAnnotationList(
            [
                item
                for items in self.data
                for item in items.images_360 or []
                if isinstance(item, Cognite360ImageAnnotation)
            ]
        )

    @property
    def point_cloud_volumes(self) -> CognitePointCloudVolumeList:
        from ._cognite_point_cloud_volume import CognitePointCloudVolume, CognitePointCloudVolumeList

        return CognitePointCloudVolumeList(
            [
                item
                for items in self.data
                for item in items.point_cloud_volumes or []
                if isinstance(item, CognitePointCloudVolume)
            ]
        )


class Cognite3DObjectWriteList(DomainModelWriteList[Cognite3DObjectWrite]):
    """List of Cognite 3D objects in the writing version."""

    _INSTANCE = Cognite3DObjectWrite

    @property
    def images_360(self) -> Cognite360ImageAnnotationWriteList:
        from ._cognite_360_image_annotation import Cognite360ImageAnnotationWrite, Cognite360ImageAnnotationWriteList

        return Cognite360ImageAnnotationWriteList(
            [
                item
                for items in self.data
                for item in items.images_360 or []
                if isinstance(item, Cognite360ImageAnnotationWrite)
            ]
        )


class Cognite3DObjectApplyList(Cognite3DObjectWriteList): ...


def _create_cognite_3_d_object_filter(
    view_id: dm.ViewId,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_x_max: float | None = None,
    max_x_max: float | None = None,
    min_x_min: float | None = None,
    max_x_min: float | None = None,
    min_y_max: float | None = None,
    max_y_max: float | None = None,
    min_y_min: float | None = None,
    max_y_min: float | None = None,
    min_z_max: float | None = None,
    max_z_max: float | None = None,
    min_z_min: float | None = None,
    max_z_min: float | None = None,
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
    if min_x_max is not None or max_x_max is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("xMax"), gte=min_x_max, lte=max_x_max))
    if min_x_min is not None or max_x_min is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("xMin"), gte=min_x_min, lte=max_x_min))
    if min_y_max is not None or max_y_max is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("yMax"), gte=min_y_max, lte=max_y_max))
    if min_y_min is not None or max_y_min is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("yMin"), gte=min_y_min, lte=max_y_min))
    if min_z_max is not None or max_z_max is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("zMax"), gte=min_z_max, lte=max_z_max))
    if min_z_min is not None or max_z_min is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("zMin"), gte=min_z_min, lte=max_z_min))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _Cognite3DObjectQuery(NodeQueryCore[T_DomainModelList, Cognite3DObjectList]):
    _view_id = Cognite3DObject._view_id
    _result_cls = Cognite3DObject
    _result_list_cls_end = Cognite3DObjectList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._cognite_360_image import _Cognite360ImageQuery
        from ._cognite_360_image_annotation import _Cognite360ImageAnnotationQuery
        from ._cognite_asset import _CogniteAssetQuery
        from ._cognite_cad_node import _CogniteCADNodeQuery
        from ._cognite_point_cloud_volume import _CognitePointCloudVolumeQuery

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

        if _CogniteAssetQuery not in created_types:
            self.asset = _CogniteAssetQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "CogniteAsset", "v1").as_property_ref("object3D"),
                    direction="inwards",
                ),
                connection_name="asset",
                connection_property=ViewPropertyId(self._view_id, "asset"),
            )

        if _CogniteCADNodeQuery not in created_types:
            self.cad_nodes = _CogniteCADNodeQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "CogniteCADNode", "v1").as_property_ref("object3D"),
                    direction="inwards",
                ),
                connection_name="cad_nodes",
                connection_property=ViewPropertyId(self._view_id, "cadNodes"),
            )

        if _Cognite360ImageAnnotationQuery not in created_types:
            self.images_360 = _Cognite360ImageAnnotationQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                _Cognite360ImageQuery,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="images_360",
                connection_property=ViewPropertyId(self._view_id, "images360"),
            )

        if _CognitePointCloudVolumeQuery not in created_types:
            self.point_cloud_volumes = _CognitePointCloudVolumeQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "CognitePointCloudVolume", "v1").as_property_ref("object3D"),
                    direction="inwards",
                ),
                connection_name="point_cloud_volumes",
                connection_property=ViewPropertyId(self._view_id, "pointCloudVolumes"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.x_max = FloatFilter(self, self._view_id.as_property_ref("xMax"))
        self.x_min = FloatFilter(self, self._view_id.as_property_ref("xMin"))
        self.y_max = FloatFilter(self, self._view_id.as_property_ref("yMax"))
        self.y_min = FloatFilter(self, self._view_id.as_property_ref("yMin"))
        self.z_max = FloatFilter(self, self._view_id.as_property_ref("zMax"))
        self.z_min = FloatFilter(self, self._view_id.as_property_ref("zMin"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.name,
                self.x_max,
                self.x_min,
                self.y_max,
                self.y_min,
                self.z_max,
                self.z_min,
            ]
        )

    def list_cognite_3_d_object(self, limit: int = DEFAULT_QUERY_LIMIT) -> Cognite3DObjectList:
        return self._list(limit=limit)


class Cognite3DObjectQuery(_Cognite3DObjectQuery[Cognite3DObjectList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, Cognite3DObjectList)
