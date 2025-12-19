from __future__ import annotations

import datetime
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
    "Cognite360ImageList",
    "Cognite360ImageWriteList",
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
        station_360: Direct relation to Cognite3DGroup instance that groups different Cognite360Image instances to the
            same station
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

    def as_read(self) -> Cognite360Image:
        """Convert this GraphQL format of Cognite 360 image to the reading format."""
        return Cognite360Image.model_validate(as_read_args(self))

    def as_write(self) -> Cognite360ImageWrite:
        """Convert this GraphQL format of Cognite 360 image to the writing format."""
        return Cognite360ImageWrite.model_validate(as_write_args(self))


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
        station_360: Direct relation to Cognite3DGroup instance that groups different Cognite360Image instances to the
            same station
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

    @field_validator("back", "bottom", "collection_360", "front", "left", "right", "station_360", "top", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> Cognite360ImageWrite:
        """Convert this read version of Cognite 360 image to the writing version."""
        return Cognite360ImageWrite.model_validate(as_write_args(self))


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
        station_360: Direct relation to Cognite3DGroup instance that groups different Cognite360Image instances to the
            same station
        taken_at: The timestamp when the 6 photos were taken
        top: Direct relation to a file holding the top projection of the cube map
        translation_x: The displacement of the object along the X-axis in the 3D coordinate system
        translation_y: The displacement of the object along the Y-axis in the 3D coordinate system
        translation_z: The displacement of the object along the Z-axis in the 3D coordinate system
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "back",
        "bottom",
        "collection_360",
        "euler_rotation_x",
        "euler_rotation_y",
        "euler_rotation_z",
        "front",
        "left",
        "right",
        "scale_x",
        "scale_y",
        "scale_z",
        "station_360",
        "taken_at",
        "top",
        "translation_x",
        "translation_y",
        "translation_z",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "back",
        "bottom",
        "collection_360",
        "front",
        "left",
        "right",
        "station_360",
        "top",
    )

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


class Cognite360ImageList(DomainModelList[Cognite360Image]):
    """List of Cognite 360 images in the read version."""

    _INSTANCE = Cognite360Image

    def as_write(self) -> Cognite360ImageWriteList:
        """Convert these read versions of Cognite 360 image to the writing versions."""
        return Cognite360ImageWriteList([node.as_write() for node in self.data])

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
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "back"),
            )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "bottom"),
            )

        if (
            _Cognite360ImageCollectionQuery not in created_types
            and len(creation_path) + 1 < global_config.max_select_depth
        ):
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
                connection_property=ViewPropertyId(self._view_id, "collection360"),
            )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "front"),
            )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "left"),
            )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "right"),
            )

        if (
            _Cognite360ImageStationQuery not in created_types
            and len(creation_path) + 1 < global_config.max_select_depth
        ):
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
                connection_property=ViewPropertyId(self._view_id, "station360"),
            )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "top"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.back_filter = DirectRelationFilter(self, self._view_id.as_property_ref("back"))
        self.bottom_filter = DirectRelationFilter(self, self._view_id.as_property_ref("bottom"))
        self.collection_360_filter = DirectRelationFilter(self, self._view_id.as_property_ref("collection360"))
        self.euler_rotation_x = FloatFilter(self, self._view_id.as_property_ref("eulerRotationX"))
        self.euler_rotation_y = FloatFilter(self, self._view_id.as_property_ref("eulerRotationY"))
        self.euler_rotation_z = FloatFilter(self, self._view_id.as_property_ref("eulerRotationZ"))
        self.front_filter = DirectRelationFilter(self, self._view_id.as_property_ref("front"))
        self.left_filter = DirectRelationFilter(self, self._view_id.as_property_ref("left"))
        self.right_filter = DirectRelationFilter(self, self._view_id.as_property_ref("right"))
        self.scale_x = FloatFilter(self, self._view_id.as_property_ref("scaleX"))
        self.scale_y = FloatFilter(self, self._view_id.as_property_ref("scaleY"))
        self.scale_z = FloatFilter(self, self._view_id.as_property_ref("scaleZ"))
        self.station_360_filter = DirectRelationFilter(self, self._view_id.as_property_ref("station360"))
        self.taken_at = TimestampFilter(self, self._view_id.as_property_ref("takenAt"))
        self.top_filter = DirectRelationFilter(self, self._view_id.as_property_ref("top"))
        self.translation_x = FloatFilter(self, self._view_id.as_property_ref("translationX"))
        self.translation_y = FloatFilter(self, self._view_id.as_property_ref("translationY"))
        self.translation_z = FloatFilter(self, self._view_id.as_property_ref("translationZ"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.back_filter,
                self.bottom_filter,
                self.collection_360_filter,
                self.euler_rotation_x,
                self.euler_rotation_y,
                self.euler_rotation_z,
                self.front_filter,
                self.left_filter,
                self.right_filter,
                self.scale_x,
                self.scale_y,
                self.scale_z,
                self.station_360_filter,
                self.taken_at,
                self.top_filter,
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
