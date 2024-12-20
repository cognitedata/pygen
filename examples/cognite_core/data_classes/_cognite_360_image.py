from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

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
    TimestampFilter,
)
from cognite_core.data_classes._cognite_3_d_transformation_node import (
    Cognite3DTransformationNode,
    Cognite3DTransformationNodeWrite,
)
from cognite_core.data_classes._cognite_cube_map import CogniteCubeMap, CogniteCubeMapWrite

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_360_image_collection import (
        Cognite360ImageCollection,
        Cognite360ImageCollectionList,
        Cognite360ImageCollectionGraphQL,
        Cognite360ImageCollectionWrite,
        Cognite360ImageCollectionWriteList,
    )
    from cognite_core.data_classes._cognite_360_image_station import (
        Cognite360ImageStation,
        Cognite360ImageStationList,
        Cognite360ImageStationGraphQL,
        Cognite360ImageStationWrite,
        Cognite360ImageStationWriteList,
    )
    from cognite_core.data_classes._cognite_file import (
        CogniteFile,
        CogniteFileList,
        CogniteFileGraphQL,
        CogniteFileWrite,
        CogniteFileWriteList,
    )


__all__ = [
    "Cognite360Image",
    "Cognite360ImageWrite",
    "Cognite360ImageApply",
    "Cognite360ImageList",
    "Cognite360ImageWriteList",
    "Cognite360ImageApplyList",
    "Cognite360ImageFields",
    "Cognite360ImageGraphQL",
]


Cognite360ImageTextFields = Literal["external_id",]
Cognite360ImageFields = Literal[
    "external_id",
    "euler_rotation_x",
    "euler_rotation_y",
    "euler_rotation_z",
    "scale_x",
    "scale_y",
    "scale_z",
    "taken_at",
    "translation_x",
    "translation_y",
    "translation_z",
]

_COGNITE360IMAGE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "euler_rotation_x": "eulerRotationX",
    "euler_rotation_y": "eulerRotationY",
    "euler_rotation_z": "eulerRotationZ",
    "scale_x": "scaleX",
    "scale_y": "scaleY",
    "scale_z": "scaleZ",
    "taken_at": "takenAt",
    "translation_x": "translationX",
    "translation_y": "translationY",
    "translation_z": "translationZ",
}


class Cognite360ImageGraphQL(GraphQLCore):
    """This represents the reading version of Cognite 360 image, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image.
        data_record: The data record of the Cognite 360 image node.
        back: Direct relation to a file holding the back projection of the cube map
        bottom: Direct relation to a file holding the bottom projection of the cube map
        collection_360: Direct relation to Cognite360ImageCollection
        euler_rotation_x: The rotation of the object around the X-axis in radians
        euler_rotation_y: The rotation of the object around the Y-axis in radians
        euler_rotation_z: The rotation of the object around the Z-axis in radians
        front: Direct relation to a file holding the front projection of the cube map
        left: Direct relation to a file holding the left projection of the cube map
        right: Direct relation to a file holding the right projection of the cube map
        scale_x: The scaling factor applied to the object along the X-axis
        scale_y: The scaling factor applied to the object along the Y-axis
        scale_z: The scaling factor applied to the object along the Z-axis
        station_360: Direct relation to Cognite3DGroup instance that groups different Cognite360Image instances to the same station
        taken_at: The timestamp when the 6 photos were taken
        top: Direct relation to a file holding the top projection of the cube map
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360Image", "v1")
    back: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)
    bottom: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)
    collection_360: Optional[Cognite360ImageCollectionGraphQL] = Field(default=None, repr=False, alias="collection360")
    euler_rotation_x: Optional[float] = Field(None, alias="eulerRotationX")
    euler_rotation_y: Optional[float] = Field(None, alias="eulerRotationY")
    euler_rotation_z: Optional[float] = Field(None, alias="eulerRotationZ")
    front: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)
    left: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)
    right: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)
    scale_x: Optional[float] = Field(None, alias="scaleX")
    scale_y: Optional[float] = Field(None, alias="scaleY")
    scale_z: Optional[float] = Field(None, alias="scaleZ")
    station_360: Optional[Cognite360ImageStationGraphQL] = Field(default=None, repr=False, alias="station360")
    taken_at: Optional[datetime.datetime] = Field(None, alias="takenAt")
    top: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)
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

    @field_validator("back", "bottom", "collection_360", "front", "left", "right", "station_360", "top", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Cognite360Image:
        """Convert this GraphQL format of Cognite 360 image to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Cognite360Image(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            back=self.back.as_read() if isinstance(self.back, GraphQLCore) else self.back,
            bottom=self.bottom.as_read() if isinstance(self.bottom, GraphQLCore) else self.bottom,
            collection_360=(
                self.collection_360.as_read() if isinstance(self.collection_360, GraphQLCore) else self.collection_360
            ),
            euler_rotation_x=self.euler_rotation_x,
            euler_rotation_y=self.euler_rotation_y,
            euler_rotation_z=self.euler_rotation_z,
            front=self.front.as_read() if isinstance(self.front, GraphQLCore) else self.front,
            left=self.left.as_read() if isinstance(self.left, GraphQLCore) else self.left,
            right=self.right.as_read() if isinstance(self.right, GraphQLCore) else self.right,
            scale_x=self.scale_x,
            scale_y=self.scale_y,
            scale_z=self.scale_z,
            station_360=self.station_360.as_read() if isinstance(self.station_360, GraphQLCore) else self.station_360,
            taken_at=self.taken_at,
            top=self.top.as_read() if isinstance(self.top, GraphQLCore) else self.top,
            translation_x=self.translation_x,
            translation_y=self.translation_y,
            translation_z=self.translation_z,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> Cognite360ImageWrite:
        """Convert this GraphQL format of Cognite 360 image to the writing format."""
        return Cognite360ImageWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            back=self.back.as_write() if isinstance(self.back, GraphQLCore) else self.back,
            bottom=self.bottom.as_write() if isinstance(self.bottom, GraphQLCore) else self.bottom,
            collection_360=(
                self.collection_360.as_write() if isinstance(self.collection_360, GraphQLCore) else self.collection_360
            ),
            euler_rotation_x=self.euler_rotation_x,
            euler_rotation_y=self.euler_rotation_y,
            euler_rotation_z=self.euler_rotation_z,
            front=self.front.as_write() if isinstance(self.front, GraphQLCore) else self.front,
            left=self.left.as_write() if isinstance(self.left, GraphQLCore) else self.left,
            right=self.right.as_write() if isinstance(self.right, GraphQLCore) else self.right,
            scale_x=self.scale_x,
            scale_y=self.scale_y,
            scale_z=self.scale_z,
            station_360=self.station_360.as_write() if isinstance(self.station_360, GraphQLCore) else self.station_360,
            taken_at=self.taken_at,
            top=self.top.as_write() if isinstance(self.top, GraphQLCore) else self.top,
            translation_x=self.translation_x,
            translation_y=self.translation_y,
            translation_z=self.translation_z,
        )


class Cognite360Image(Cognite3DTransformationNode, CogniteCubeMap):
    """This represents the reading version of Cognite 360 image.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image.
        data_record: The data record of the Cognite 360 image node.
        back: Direct relation to a file holding the back projection of the cube map
        bottom: Direct relation to a file holding the bottom projection of the cube map
        collection_360: Direct relation to Cognite360ImageCollection
        euler_rotation_x: The rotation of the object around the X-axis in radians
        euler_rotation_y: The rotation of the object around the Y-axis in radians
        euler_rotation_z: The rotation of the object around the Z-axis in radians
        front: Direct relation to a file holding the front projection of the cube map
        left: Direct relation to a file holding the left projection of the cube map
        right: Direct relation to a file holding the right projection of the cube map
        scale_x: The scaling factor applied to the object along the X-axis
        scale_y: The scaling factor applied to the object along the Y-axis
        scale_z: The scaling factor applied to the object along the Z-axis
        station_360: Direct relation to Cognite3DGroup instance that groups different Cognite360Image instances to the same station
        taken_at: The timestamp when the 6 photos were taken
        top: Direct relation to a file holding the top projection of the cube map
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360Image", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    collection_360: Union[Cognite360ImageCollection, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="collection360"
    )
    station_360: Union[Cognite360ImageStation, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="station360"
    )
    taken_at: Optional[datetime.datetime] = Field(None, alias="takenAt")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> Cognite360ImageWrite:
        """Convert this read version of Cognite 360 image to the writing version."""
        return Cognite360ImageWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            back=self.back.as_write() if isinstance(self.back, DomainModel) else self.back,
            bottom=self.bottom.as_write() if isinstance(self.bottom, DomainModel) else self.bottom,
            collection_360=(
                self.collection_360.as_write() if isinstance(self.collection_360, DomainModel) else self.collection_360
            ),
            euler_rotation_x=self.euler_rotation_x,
            euler_rotation_y=self.euler_rotation_y,
            euler_rotation_z=self.euler_rotation_z,
            front=self.front.as_write() if isinstance(self.front, DomainModel) else self.front,
            left=self.left.as_write() if isinstance(self.left, DomainModel) else self.left,
            right=self.right.as_write() if isinstance(self.right, DomainModel) else self.right,
            scale_x=self.scale_x,
            scale_y=self.scale_y,
            scale_z=self.scale_z,
            station_360=self.station_360.as_write() if isinstance(self.station_360, DomainModel) else self.station_360,
            taken_at=self.taken_at,
            top=self.top.as_write() if isinstance(self.top, DomainModel) else self.top,
            translation_x=self.translation_x,
            translation_y=self.translation_y,
            translation_z=self.translation_z,
        )

    def as_apply(self) -> Cognite360ImageWrite:
        """Convert this read version of Cognite 360 image to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, Cognite360Image],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._cognite_360_image_collection import Cognite360ImageCollection
        from ._cognite_360_image_station import Cognite360ImageStation
        from ._cognite_file import CogniteFile

        for instance in instances.values():
            if (
                isinstance(instance.back, (dm.NodeId, str))
                and (back := nodes_by_id.get(instance.back))
                and isinstance(back, CogniteFile)
            ):
                instance.back = back
            if (
                isinstance(instance.bottom, (dm.NodeId, str))
                and (bottom := nodes_by_id.get(instance.bottom))
                and isinstance(bottom, CogniteFile)
            ):
                instance.bottom = bottom
            if (
                isinstance(instance.collection_360, (dm.NodeId, str))
                and (collection_360 := nodes_by_id.get(instance.collection_360))
                and isinstance(collection_360, Cognite360ImageCollection)
            ):
                instance.collection_360 = collection_360
            if (
                isinstance(instance.front, (dm.NodeId, str))
                and (front := nodes_by_id.get(instance.front))
                and isinstance(front, CogniteFile)
            ):
                instance.front = front
            if (
                isinstance(instance.left, (dm.NodeId, str))
                and (left := nodes_by_id.get(instance.left))
                and isinstance(left, CogniteFile)
            ):
                instance.left = left
            if (
                isinstance(instance.right, (dm.NodeId, str))
                and (right := nodes_by_id.get(instance.right))
                and isinstance(right, CogniteFile)
            ):
                instance.right = right
            if (
                isinstance(instance.station_360, (dm.NodeId, str))
                and (station_360 := nodes_by_id.get(instance.station_360))
                and isinstance(station_360, Cognite360ImageStation)
            ):
                instance.station_360 = station_360
            if (
                isinstance(instance.top, (dm.NodeId, str))
                and (top := nodes_by_id.get(instance.top))
                and isinstance(top, CogniteFile)
            ):
                instance.top = top


class Cognite360ImageWrite(Cognite3DTransformationNodeWrite, CogniteCubeMapWrite):
    """This represents the writing version of Cognite 360 image.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image.
        data_record: The data record of the Cognite 360 image node.
        back: Direct relation to a file holding the back projection of the cube map
        bottom: Direct relation to a file holding the bottom projection of the cube map
        collection_360: Direct relation to Cognite360ImageCollection
        euler_rotation_x: The rotation of the object around the X-axis in radians
        euler_rotation_y: The rotation of the object around the Y-axis in radians
        euler_rotation_z: The rotation of the object around the Z-axis in radians
        front: Direct relation to a file holding the front projection of the cube map
        left: Direct relation to a file holding the left projection of the cube map
        right: Direct relation to a file holding the right projection of the cube map
        scale_x: The scaling factor applied to the object along the X-axis
        scale_y: The scaling factor applied to the object along the Y-axis
        scale_z: The scaling factor applied to the object along the Z-axis
        station_360: Direct relation to Cognite3DGroup instance that groups different Cognite360Image instances to the same station
        taken_at: The timestamp when the 6 photos were taken
        top: Direct relation to a file holding the top projection of the cube map
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360Image", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    collection_360: Union[Cognite360ImageCollectionWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="collection360"
    )
    station_360: Union[Cognite360ImageStationWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="station360"
    )
    taken_at: Optional[datetime.datetime] = Field(None, alias="takenAt")

    @field_validator("collection_360", "station_360", mode="before")
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

        if self.back is not None:
            properties["back"] = {
                "space": self.space if isinstance(self.back, str) else self.back.space,
                "externalId": self.back if isinstance(self.back, str) else self.back.external_id,
            }

        if self.bottom is not None:
            properties["bottom"] = {
                "space": self.space if isinstance(self.bottom, str) else self.bottom.space,
                "externalId": self.bottom if isinstance(self.bottom, str) else self.bottom.external_id,
            }

        if self.collection_360 is not None:
            properties["collection360"] = {
                "space": self.space if isinstance(self.collection_360, str) else self.collection_360.space,
                "externalId": (
                    self.collection_360 if isinstance(self.collection_360, str) else self.collection_360.external_id
                ),
            }

        if self.euler_rotation_x is not None or write_none:
            properties["eulerRotationX"] = self.euler_rotation_x

        if self.euler_rotation_y is not None or write_none:
            properties["eulerRotationY"] = self.euler_rotation_y

        if self.euler_rotation_z is not None or write_none:
            properties["eulerRotationZ"] = self.euler_rotation_z

        if self.front is not None:
            properties["front"] = {
                "space": self.space if isinstance(self.front, str) else self.front.space,
                "externalId": self.front if isinstance(self.front, str) else self.front.external_id,
            }

        if self.left is not None:
            properties["left"] = {
                "space": self.space if isinstance(self.left, str) else self.left.space,
                "externalId": self.left if isinstance(self.left, str) else self.left.external_id,
            }

        if self.right is not None:
            properties["right"] = {
                "space": self.space if isinstance(self.right, str) else self.right.space,
                "externalId": self.right if isinstance(self.right, str) else self.right.external_id,
            }

        if self.scale_x is not None or write_none:
            properties["scaleX"] = self.scale_x

        if self.scale_y is not None or write_none:
            properties["scaleY"] = self.scale_y

        if self.scale_z is not None or write_none:
            properties["scaleZ"] = self.scale_z

        if self.station_360 is not None:
            properties["station360"] = {
                "space": self.space if isinstance(self.station_360, str) else self.station_360.space,
                "externalId": self.station_360 if isinstance(self.station_360, str) else self.station_360.external_id,
            }

        if self.taken_at is not None or write_none:
            properties["takenAt"] = self.taken_at.isoformat(timespec="milliseconds") if self.taken_at else None

        if self.top is not None:
            properties["top"] = {
                "space": self.space if isinstance(self.top, str) else self.top.space,
                "externalId": self.top if isinstance(self.top, str) else self.top.external_id,
            }

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

        if isinstance(self.back, DomainModelWrite):
            other_resources = self.back._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.bottom, DomainModelWrite):
            other_resources = self.bottom._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.collection_360, DomainModelWrite):
            other_resources = self.collection_360._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.front, DomainModelWrite):
            other_resources = self.front._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.left, DomainModelWrite):
            other_resources = self.left._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.right, DomainModelWrite):
            other_resources = self.right._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.station_360, DomainModelWrite):
            other_resources = self.station_360._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.top, DomainModelWrite):
            other_resources = self.top._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class Cognite360ImageApply(Cognite360ImageWrite):
    def __new__(cls, *args, **kwargs) -> Cognite360ImageApply:
        warnings.warn(
            "Cognite360ImageApply is deprecated and will be removed in v1.0. Use Cognite360ImageWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Cognite360Image.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class Cognite360ImageList(DomainModelList[Cognite360Image]):
    """List of Cognite 360 images in the read version."""

    _INSTANCE = Cognite360Image

    def as_write(self) -> Cognite360ImageWriteList:
        """Convert these read versions of Cognite 360 image to the writing versions."""
        return Cognite360ImageWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> Cognite360ImageWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def back(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.back for item in self.data if isinstance(item.back, CogniteFile)])

    @property
    def bottom(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.bottom for item in self.data if isinstance(item.bottom, CogniteFile)])

    @property
    def collection_360(self) -> Cognite360ImageCollectionList:
        from ._cognite_360_image_collection import Cognite360ImageCollection, Cognite360ImageCollectionList

        return Cognite360ImageCollectionList(
            [item.collection_360 for item in self.data if isinstance(item.collection_360, Cognite360ImageCollection)]
        )

    @property
    def front(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.front for item in self.data if isinstance(item.front, CogniteFile)])

    @property
    def left(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.left for item in self.data if isinstance(item.left, CogniteFile)])

    @property
    def right(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.right for item in self.data if isinstance(item.right, CogniteFile)])

    @property
    def station_360(self) -> Cognite360ImageStationList:
        from ._cognite_360_image_station import Cognite360ImageStation, Cognite360ImageStationList

        return Cognite360ImageStationList(
            [item.station_360 for item in self.data if isinstance(item.station_360, Cognite360ImageStation)]
        )

    @property
    def top(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.top for item in self.data if isinstance(item.top, CogniteFile)])


class Cognite360ImageWriteList(DomainModelWriteList[Cognite360ImageWrite]):
    """List of Cognite 360 images in the writing version."""

    _INSTANCE = Cognite360ImageWrite

    @property
    def back(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList([item.back for item in self.data if isinstance(item.back, CogniteFileWrite)])

    @property
    def bottom(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList([item.bottom for item in self.data if isinstance(item.bottom, CogniteFileWrite)])

    @property
    def collection_360(self) -> Cognite360ImageCollectionWriteList:
        from ._cognite_360_image_collection import Cognite360ImageCollectionWrite, Cognite360ImageCollectionWriteList

        return Cognite360ImageCollectionWriteList(
            [
                item.collection_360
                for item in self.data
                if isinstance(item.collection_360, Cognite360ImageCollectionWrite)
            ]
        )

    @property
    def front(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList([item.front for item in self.data if isinstance(item.front, CogniteFileWrite)])

    @property
    def left(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList([item.left for item in self.data if isinstance(item.left, CogniteFileWrite)])

    @property
    def right(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList([item.right for item in self.data if isinstance(item.right, CogniteFileWrite)])

    @property
    def station_360(self) -> Cognite360ImageStationWriteList:
        from ._cognite_360_image_station import Cognite360ImageStationWrite, Cognite360ImageStationWriteList

        return Cognite360ImageStationWriteList(
            [item.station_360 for item in self.data if isinstance(item.station_360, Cognite360ImageStationWrite)]
        )

    @property
    def top(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList([item.top for item in self.data if isinstance(item.top, CogniteFileWrite)])


class Cognite360ImageApplyList(Cognite360ImageWriteList): ...


def _create_cognite_360_image_filter(
    view_id: dm.ViewId,
    back: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    bottom: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    collection_360: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    min_euler_rotation_x: float | None = None,
    max_euler_rotation_x: float | None = None,
    min_euler_rotation_y: float | None = None,
    max_euler_rotation_y: float | None = None,
    min_euler_rotation_z: float | None = None,
    max_euler_rotation_z: float | None = None,
    front: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    left: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    right: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    min_scale_x: float | None = None,
    max_scale_x: float | None = None,
    min_scale_y: float | None = None,
    max_scale_y: float | None = None,
    min_scale_z: float | None = None,
    max_scale_z: float | None = None,
    station_360: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    min_taken_at: datetime.datetime | None = None,
    max_taken_at: datetime.datetime | None = None,
    top: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
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
    if isinstance(back, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(back):
        filters.append(dm.filters.Equals(view_id.as_property_ref("back"), value=as_instance_dict_id(back)))
    if back and isinstance(back, Sequence) and not isinstance(back, str) and not is_tuple_id(back):
        filters.append(
            dm.filters.In(view_id.as_property_ref("back"), values=[as_instance_dict_id(item) for item in back])
        )
    if isinstance(bottom, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bottom):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bottom"), value=as_instance_dict_id(bottom)))
    if bottom and isinstance(bottom, Sequence) and not isinstance(bottom, str) and not is_tuple_id(bottom):
        filters.append(
            dm.filters.In(view_id.as_property_ref("bottom"), values=[as_instance_dict_id(item) for item in bottom])
        )
    if isinstance(collection_360, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(collection_360):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("collection360"), value=as_instance_dict_id(collection_360))
        )
    if (
        collection_360
        and isinstance(collection_360, Sequence)
        and not isinstance(collection_360, str)
        and not is_tuple_id(collection_360)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("collection360"), values=[as_instance_dict_id(item) for item in collection_360]
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
    if isinstance(front, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(front):
        filters.append(dm.filters.Equals(view_id.as_property_ref("front"), value=as_instance_dict_id(front)))
    if front and isinstance(front, Sequence) and not isinstance(front, str) and not is_tuple_id(front):
        filters.append(
            dm.filters.In(view_id.as_property_ref("front"), values=[as_instance_dict_id(item) for item in front])
        )
    if isinstance(left, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(left):
        filters.append(dm.filters.Equals(view_id.as_property_ref("left"), value=as_instance_dict_id(left)))
    if left and isinstance(left, Sequence) and not isinstance(left, str) and not is_tuple_id(left):
        filters.append(
            dm.filters.In(view_id.as_property_ref("left"), values=[as_instance_dict_id(item) for item in left])
        )
    if isinstance(right, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(right):
        filters.append(dm.filters.Equals(view_id.as_property_ref("right"), value=as_instance_dict_id(right)))
    if right and isinstance(right, Sequence) and not isinstance(right, str) and not is_tuple_id(right):
        filters.append(
            dm.filters.In(view_id.as_property_ref("right"), values=[as_instance_dict_id(item) for item in right])
        )
    if min_scale_x is not None or max_scale_x is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("scaleX"), gte=min_scale_x, lte=max_scale_x))
    if min_scale_y is not None or max_scale_y is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("scaleY"), gte=min_scale_y, lte=max_scale_y))
    if min_scale_z is not None or max_scale_z is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("scaleZ"), gte=min_scale_z, lte=max_scale_z))
    if isinstance(station_360, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(station_360):
        filters.append(dm.filters.Equals(view_id.as_property_ref("station360"), value=as_instance_dict_id(station_360)))
    if (
        station_360
        and isinstance(station_360, Sequence)
        and not isinstance(station_360, str)
        and not is_tuple_id(station_360)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("station360"), values=[as_instance_dict_id(item) for item in station_360]
            )
        )
    if min_taken_at is not None or max_taken_at is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("takenAt"),
                gte=min_taken_at.isoformat(timespec="milliseconds") if min_taken_at else None,
                lte=max_taken_at.isoformat(timespec="milliseconds") if max_taken_at else None,
            )
        )
    if isinstance(top, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(top):
        filters.append(dm.filters.Equals(view_id.as_property_ref("top"), value=as_instance_dict_id(top)))
    if top and isinstance(top, Sequence) and not isinstance(top, str) and not is_tuple_id(top):
        filters.append(
            dm.filters.In(view_id.as_property_ref("top"), values=[as_instance_dict_id(item) for item in top])
        )
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


class _Cognite360ImageQuery(NodeQueryCore[T_DomainModelList, Cognite360ImageList]):
    _view_id = Cognite360Image._view_id
    _result_cls = Cognite360Image
    _result_list_cls_end = Cognite360ImageList

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
        from ._cognite_360_image_collection import _Cognite360ImageCollectionQuery
        from ._cognite_360_image_station import _Cognite360ImageStationQuery
        from ._cognite_file import _CogniteFileQuery

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

        if _CogniteFileQuery not in created_types:
            self.back = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("back"),
                    direction="outwards",
                ),
                connection_name="back",
            )

        if _CogniteFileQuery not in created_types:
            self.bottom = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("bottom"),
                    direction="outwards",
                ),
                connection_name="bottom",
            )

        if _Cognite360ImageCollectionQuery not in created_types:
            self.collection_360 = _Cognite360ImageCollectionQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("collection360"),
                    direction="outwards",
                ),
                connection_name="collection_360",
            )

        if _CogniteFileQuery not in created_types:
            self.front = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("front"),
                    direction="outwards",
                ),
                connection_name="front",
            )

        if _CogniteFileQuery not in created_types:
            self.left = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("left"),
                    direction="outwards",
                ),
                connection_name="left",
            )

        if _CogniteFileQuery not in created_types:
            self.right = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("right"),
                    direction="outwards",
                ),
                connection_name="right",
            )

        if _Cognite360ImageStationQuery not in created_types:
            self.station_360 = _Cognite360ImageStationQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("station360"),
                    direction="outwards",
                ),
                connection_name="station_360",
            )

        if _CogniteFileQuery not in created_types:
            self.top = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("top"),
                    direction="outwards",
                ),
                connection_name="top",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.euler_rotation_x = FloatFilter(self, self._view_id.as_property_ref("eulerRotationX"))
        self.euler_rotation_y = FloatFilter(self, self._view_id.as_property_ref("eulerRotationY"))
        self.euler_rotation_z = FloatFilter(self, self._view_id.as_property_ref("eulerRotationZ"))
        self.scale_x = FloatFilter(self, self._view_id.as_property_ref("scaleX"))
        self.scale_y = FloatFilter(self, self._view_id.as_property_ref("scaleY"))
        self.scale_z = FloatFilter(self, self._view_id.as_property_ref("scaleZ"))
        self.taken_at = TimestampFilter(self, self._view_id.as_property_ref("takenAt"))
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
                self.taken_at,
                self.translation_x,
                self.translation_y,
                self.translation_z,
            ]
        )

    def list_cognite_360_image(self, limit: int = DEFAULT_QUERY_LIMIT) -> Cognite360ImageList:
        return self._list(limit=limit)


class Cognite360ImageQuery(_Cognite360ImageQuery[Cognite360ImageList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, Cognite360ImageList)
