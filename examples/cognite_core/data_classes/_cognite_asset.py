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
    TimestampFilter,
)
from cognite_core.data_classes._cognite_visualizable import CogniteVisualizable, CogniteVisualizableWrite
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite
from cognite_core.data_classes._cognite_sourceable_node import CogniteSourceableNode, CogniteSourceableNodeWrite

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_3_d_object import (
        Cognite3DObject,
        Cognite3DObjectList,
        Cognite3DObjectGraphQL,
        Cognite3DObjectWrite,
        Cognite3DObjectWriteList,
    )
    from cognite_core.data_classes._cognite_activity import (
        CogniteActivity,
        CogniteActivityList,
        CogniteActivityGraphQL,
        CogniteActivityWrite,
        CogniteActivityWriteList,
    )
    from cognite_core.data_classes._cognite_asset_class import (
        CogniteAssetClass,
        CogniteAssetClassList,
        CogniteAssetClassGraphQL,
        CogniteAssetClassWrite,
        CogniteAssetClassWriteList,
    )
    from cognite_core.data_classes._cognite_asset_type import (
        CogniteAssetType,
        CogniteAssetTypeList,
        CogniteAssetTypeGraphQL,
        CogniteAssetTypeWrite,
        CogniteAssetTypeWriteList,
    )
    from cognite_core.data_classes._cognite_equipment import (
        CogniteEquipment,
        CogniteEquipmentList,
        CogniteEquipmentGraphQL,
        CogniteEquipmentWrite,
        CogniteEquipmentWriteList,
    )
    from cognite_core.data_classes._cognite_file import (
        CogniteFile,
        CogniteFileList,
        CogniteFileGraphQL,
        CogniteFileWrite,
        CogniteFileWriteList,
    )
    from cognite_core.data_classes._cognite_source_system import (
        CogniteSourceSystem,
        CogniteSourceSystemList,
        CogniteSourceSystemGraphQL,
        CogniteSourceSystemWrite,
        CogniteSourceSystemWriteList,
    )
    from cognite_core.data_classes._cognite_time_series import (
        CogniteTimeSeries,
        CogniteTimeSeriesList,
        CogniteTimeSeriesGraphQL,
        CogniteTimeSeriesWrite,
        CogniteTimeSeriesWriteList,
    )


__all__ = [
    "CogniteAsset",
    "CogniteAssetWrite",
    "CogniteAssetList",
    "CogniteAssetWriteList",
    "CogniteAssetFields",
    "CogniteAssetTextFields",
    "CogniteAssetGraphQL",
]


CogniteAssetTextFields = Literal[
    "external_id",
    "aliases",
    "description",
    "name",
    "source_context",
    "source_created_user",
    "source_id",
    "source_updated_user",
    "tags",
]
CogniteAssetFields = Literal[
    "external_id",
    "aliases",
    "description",
    "name",
    "path_last_updated_time",
    "source_context",
    "source_created_time",
    "source_created_user",
    "source_id",
    "source_updated_time",
    "source_updated_user",
    "tags",
]

_COGNITEASSET_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "name": "name",
    "path_last_updated_time": "pathLastUpdatedTime",
    "source_context": "sourceContext",
    "source_created_time": "sourceCreatedTime",
    "source_created_user": "sourceCreatedUser",
    "source_id": "sourceId",
    "source_updated_time": "sourceUpdatedTime",
    "source_updated_user": "sourceUpdatedUser",
    "tags": "tags",
}


class CogniteAssetGraphQL(GraphQLCore):
    """This represents the reading version of Cognite asset, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite asset.
        data_record: The data record of the Cognite asset node.
        activities: An automatically updated list of activities related to the asset.
        aliases: Alternative names for the node
        asset_class: Specifies the class of the asset. It's a direct relation to CogniteAssetClass.
        children: An automatically updated list of assets with this asset as their parent.
        description: Description of the instance
        equipment: An automatically updated list of equipment related to the asset.
        files: An automatically updated list of files related to the asset.
        name: Name of the instance
        object_3d: Direct relation to an Object3D instance representing the 3D resource
        parent: The parent of the asset.
        path: An automatically updated ordered list of this asset's ancestors, starting with the root asset. Enables
            subtree filtering to find all assets under a parent.
        path_last_updated_time: The last time the path was updated for this asset.
        root: An automatically updated reference to the top-level asset of the hierarchy.
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is
            not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This
            identifier is not guaranteed to match the user identifiers in CDF
        tags: Text based labels for generic use, limited to 1000
        time_series: An automatically updated list of time series related to the asset.
        type_: Specifies the type of the asset. It's a direct relation to CogniteAssetType.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteAsset", "v1")
    activities: Optional[list[CogniteActivityGraphQL]] = Field(default=None, repr=False)
    aliases: Optional[list[str]] = None
    asset_class: Optional[CogniteAssetClassGraphQL] = Field(default=None, repr=False, alias="assetClass")
    children: Optional[list[CogniteAssetGraphQL]] = Field(default=None, repr=False)
    description: Optional[str] = None
    equipment: Optional[list[CogniteEquipmentGraphQL]] = Field(default=None, repr=False)
    files: Optional[list[CogniteFileGraphQL]] = Field(default=None, repr=False)
    name: Optional[str] = None
    object_3d: Optional[Cognite3DObjectGraphQL] = Field(default=None, repr=False, alias="object3D")
    parent: Optional[CogniteAssetGraphQL] = Field(default=None, repr=False)
    path: Optional[list[CogniteAssetGraphQL]] = Field(default=None, repr=False)
    path_last_updated_time: Optional[datetime.datetime] = Field(None, alias="pathLastUpdatedTime")
    root: Optional[CogniteAssetGraphQL] = Field(default=None, repr=False)
    source: Optional[CogniteSourceSystemGraphQL] = Field(default=None, repr=False)
    source_context: Optional[str] = Field(None, alias="sourceContext")
    source_created_time: Optional[datetime.datetime] = Field(None, alias="sourceCreatedTime")
    source_created_user: Optional[str] = Field(None, alias="sourceCreatedUser")
    source_id: Optional[str] = Field(None, alias="sourceId")
    source_updated_time: Optional[datetime.datetime] = Field(None, alias="sourceUpdatedTime")
    source_updated_user: Optional[str] = Field(None, alias="sourceUpdatedUser")
    tags: Optional[list[str]] = None
    time_series: Optional[list[CogniteTimeSeriesGraphQL]] = Field(default=None, repr=False, alias="timeSeries")
    type_: Optional[CogniteAssetTypeGraphQL] = Field(default=None, repr=False, alias="type")

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

    @field_validator(
        "activities",
        "asset_class",
        "children",
        "equipment",
        "files",
        "object_3d",
        "parent",
        "path",
        "root",
        "source",
        "time_series",
        "type_",
        mode="before",
    )
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> CogniteAsset:
        """Convert this GraphQL format of Cognite asset to the reading format."""
        return CogniteAsset.model_validate(as_read_args(self))

    def as_write(self) -> CogniteAssetWrite:
        """Convert this GraphQL format of Cognite asset to the writing format."""
        return CogniteAssetWrite.model_validate(as_write_args(self))


class CogniteAsset(CogniteVisualizable, CogniteDescribableNode, CogniteSourceableNode):
    """This represents the reading version of Cognite asset.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite asset.
        data_record: The data record of the Cognite asset node.
        activities: An automatically updated list of activities related to the asset.
        aliases: Alternative names for the node
        asset_class: Specifies the class of the asset. It's a direct relation to CogniteAssetClass.
        children: An automatically updated list of assets with this asset as their parent.
        description: Description of the instance
        equipment: An automatically updated list of equipment related to the asset.
        files: An automatically updated list of files related to the asset.
        name: Name of the instance
        object_3d: Direct relation to an Object3D instance representing the 3D resource
        parent: The parent of the asset.
        path: An automatically updated ordered list of this asset's ancestors, starting with the root asset. Enables
            subtree filtering to find all assets under a parent.
        path_last_updated_time: The last time the path was updated for this asset.
        root: An automatically updated reference to the top-level asset of the hierarchy.
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is
            not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This
            identifier is not guaranteed to match the user identifiers in CDF
        tags: Text based labels for generic use, limited to 1000
        time_series: An automatically updated list of time series related to the asset.
        type_: Specifies the type of the asset. It's a direct relation to CogniteAssetType.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteAsset", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    activities: Optional[list[CogniteActivity]] = Field(default=None, repr=False)
    asset_class: Union[CogniteAssetClass, str, dm.NodeId, None] = Field(default=None, repr=False, alias="assetClass")
    children: Optional[list[CogniteAsset]] = Field(default=None, repr=False)
    equipment: Optional[list[CogniteEquipment]] = Field(default=None, repr=False)
    files: Optional[list[CogniteFile]] = Field(default=None, repr=False)
    parent: Union[CogniteAsset, str, dm.NodeId, None] = Field(default=None, repr=False)
    path: Optional[list[Union[CogniteAsset, str, dm.NodeId]]] = Field(default=None, repr=False)
    path_last_updated_time: Optional[datetime.datetime] = Field(None, alias="pathLastUpdatedTime")
    root: Union[CogniteAsset, str, dm.NodeId, None] = Field(default=None, repr=False)
    time_series: Optional[list[CogniteTimeSeries]] = Field(default=None, repr=False, alias="timeSeries")
    type_: Union[CogniteAssetType, str, dm.NodeId, None] = Field(default=None, repr=False, alias="type")

    @field_validator("asset_class", "object_3d", "parent", "root", "source", "type_", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("activities", "children", "equipment", "files", "path", "time_series", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> CogniteAssetWrite:
        """Convert this read version of Cognite asset to the writing version."""
        return CogniteAssetWrite.model_validate(as_write_args(self))


class CogniteAssetWrite(CogniteVisualizableWrite, CogniteDescribableNodeWrite, CogniteSourceableNodeWrite):
    """This represents the writing version of Cognite asset.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite asset.
        data_record: The data record of the Cognite asset node.
        aliases: Alternative names for the node
        asset_class: Specifies the class of the asset. It's a direct relation to CogniteAssetClass.
        description: Description of the instance
        name: Name of the instance
        object_3d: Direct relation to an Object3D instance representing the 3D resource
        parent: The parent of the asset.
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is
            not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This
            identifier is not guaranteed to match the user identifiers in CDF
        tags: Text based labels for generic use, limited to 1000
        type_: Specifies the type of the asset. It's a direct relation to CogniteAssetType.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "asset_class",
        "description",
        "name",
        "object_3d",
        "parent",
        "source",
        "source_context",
        "source_created_time",
        "source_created_user",
        "source_id",
        "source_updated_time",
        "source_updated_user",
        "tags",
        "type_",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "asset_class",
        "object_3d",
        "parent",
        "path",
        "root",
        "source",
        "type_",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteAsset", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    asset_class: Union[CogniteAssetClassWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="assetClass"
    )
    parent: Union[CogniteAssetWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    type_: Union[CogniteAssetTypeWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="type")

    @field_validator("asset_class", "parent", "type_", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class CogniteAssetList(DomainModelList[CogniteAsset]):
    """List of Cognite assets in the read version."""

    _INSTANCE = CogniteAsset

    def as_write(self) -> CogniteAssetWriteList:
        """Convert these read versions of Cognite asset to the writing versions."""
        return CogniteAssetWriteList([node.as_write() for node in self.data])

    @property
    def activities(self) -> CogniteActivityList:
        from ._cognite_activity import CogniteActivity, CogniteActivityList

        return CogniteActivityList(
            [item for items in self.data for item in items.activities or [] if isinstance(item, CogniteActivity)]
        )

    @property
    def asset_class(self) -> CogniteAssetClassList:
        from ._cognite_asset_class import CogniteAssetClass, CogniteAssetClassList

        return CogniteAssetClassList(
            [item.asset_class for item in self.data if isinstance(item.asset_class, CogniteAssetClass)]
        )

    @property
    def children(self) -> CogniteAssetList:
        return CogniteAssetList(
            [item for items in self.data for item in items.children or [] if isinstance(item, CogniteAsset)]
        )

    @property
    def equipment(self) -> CogniteEquipmentList:
        from ._cognite_equipment import CogniteEquipment, CogniteEquipmentList

        return CogniteEquipmentList(
            [item for items in self.data for item in items.equipment or [] if isinstance(item, CogniteEquipment)]
        )

    @property
    def files(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList(
            [item for items in self.data for item in items.files or [] if isinstance(item, CogniteFile)]
        )

    @property
    def object_3d(self) -> Cognite3DObjectList:
        from ._cognite_3_d_object import Cognite3DObject, Cognite3DObjectList

        return Cognite3DObjectList(
            [item.object_3d for item in self.data if isinstance(item.object_3d, Cognite3DObject)]
        )

    @property
    def parent(self) -> CogniteAssetList:
        return CogniteAssetList([item.parent for item in self.data if isinstance(item.parent, CogniteAsset)])

    @property
    def path(self) -> CogniteAssetList:
        return CogniteAssetList(
            [item for items in self.data for item in items.path or [] if isinstance(item, CogniteAsset)]
        )

    @property
    def root(self) -> CogniteAssetList:
        return CogniteAssetList([item.root for item in self.data if isinstance(item.root, CogniteAsset)])

    @property
    def source(self) -> CogniteSourceSystemList:
        from ._cognite_source_system import CogniteSourceSystem, CogniteSourceSystemList

        return CogniteSourceSystemList(
            [item.source for item in self.data if isinstance(item.source, CogniteSourceSystem)]
        )

    @property
    def time_series(self) -> CogniteTimeSeriesList:
        from ._cognite_time_series import CogniteTimeSeries, CogniteTimeSeriesList

        return CogniteTimeSeriesList(
            [item for items in self.data for item in items.time_series or [] if isinstance(item, CogniteTimeSeries)]
        )

    @property
    def type_(self) -> CogniteAssetTypeList:
        from ._cognite_asset_type import CogniteAssetType, CogniteAssetTypeList

        return CogniteAssetTypeList([item.type_ for item in self.data if isinstance(item.type_, CogniteAssetType)])


class CogniteAssetWriteList(DomainModelWriteList[CogniteAssetWrite]):
    """List of Cognite assets in the writing version."""

    _INSTANCE = CogniteAssetWrite

    @property
    def asset_class(self) -> CogniteAssetClassWriteList:
        from ._cognite_asset_class import CogniteAssetClassWrite, CogniteAssetClassWriteList

        return CogniteAssetClassWriteList(
            [item.asset_class for item in self.data if isinstance(item.asset_class, CogniteAssetClassWrite)]
        )

    @property
    def object_3d(self) -> Cognite3DObjectWriteList:
        from ._cognite_3_d_object import Cognite3DObjectWrite, Cognite3DObjectWriteList

        return Cognite3DObjectWriteList(
            [item.object_3d for item in self.data if isinstance(item.object_3d, Cognite3DObjectWrite)]
        )

    @property
    def parent(self) -> CogniteAssetWriteList:
        return CogniteAssetWriteList([item.parent for item in self.data if isinstance(item.parent, CogniteAssetWrite)])

    @property
    def source(self) -> CogniteSourceSystemWriteList:
        from ._cognite_source_system import CogniteSourceSystemWrite, CogniteSourceSystemWriteList

        return CogniteSourceSystemWriteList(
            [item.source for item in self.data if isinstance(item.source, CogniteSourceSystemWrite)]
        )

    @property
    def type_(self) -> CogniteAssetTypeWriteList:
        from ._cognite_asset_type import CogniteAssetTypeWrite, CogniteAssetTypeWriteList

        return CogniteAssetTypeWriteList(
            [item.type_ for item in self.data if isinstance(item.type_, CogniteAssetTypeWrite)]
        )


def _create_cognite_asset_filter(
    view_id: dm.ViewId,
    asset_class: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    object_3d: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    parent: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    path: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    min_path_last_updated_time: datetime.datetime | None = None,
    max_path_last_updated_time: datetime.datetime | None = None,
    root: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    source: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    source_context: str | list[str] | None = None,
    source_context_prefix: str | None = None,
    min_source_created_time: datetime.datetime | None = None,
    max_source_created_time: datetime.datetime | None = None,
    source_created_user: str | list[str] | None = None,
    source_created_user_prefix: str | None = None,
    source_id: str | list[str] | None = None,
    source_id_prefix: str | None = None,
    min_source_updated_time: datetime.datetime | None = None,
    max_source_updated_time: datetime.datetime | None = None,
    source_updated_user: str | list[str] | None = None,
    source_updated_user_prefix: str | None = None,
    type_: (
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
    if isinstance(asset_class, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(asset_class):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetClass"), value=as_instance_dict_id(asset_class)))
    if (
        asset_class
        and isinstance(asset_class, Sequence)
        and not isinstance(asset_class, str)
        and not is_tuple_id(asset_class)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("assetClass"), values=[as_instance_dict_id(item) for item in asset_class]
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
    if isinstance(object_3d, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(object_3d):
        filters.append(dm.filters.Equals(view_id.as_property_ref("object3D"), value=as_instance_dict_id(object_3d)))
    if object_3d and isinstance(object_3d, Sequence) and not isinstance(object_3d, str) and not is_tuple_id(object_3d):
        filters.append(
            dm.filters.In(view_id.as_property_ref("object3D"), values=[as_instance_dict_id(item) for item in object_3d])
        )
    if isinstance(parent, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(parent):
        filters.append(dm.filters.Equals(view_id.as_property_ref("parent"), value=as_instance_dict_id(parent)))
    if parent and isinstance(parent, Sequence) and not isinstance(parent, str) and not is_tuple_id(parent):
        filters.append(
            dm.filters.In(view_id.as_property_ref("parent"), values=[as_instance_dict_id(item) for item in parent])
        )
    if isinstance(path, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(path):
        filters.append(dm.filters.Equals(view_id.as_property_ref("path"), value=as_instance_dict_id(path)))
    if path and isinstance(path, Sequence) and not isinstance(path, str) and not is_tuple_id(path):
        filters.append(
            dm.filters.In(view_id.as_property_ref("path"), values=[as_instance_dict_id(item) for item in path])
        )
    if min_path_last_updated_time is not None or max_path_last_updated_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("pathLastUpdatedTime"),
                gte=(
                    min_path_last_updated_time.isoformat(timespec="milliseconds")
                    if min_path_last_updated_time
                    else None
                ),
                lte=(
                    max_path_last_updated_time.isoformat(timespec="milliseconds")
                    if max_path_last_updated_time
                    else None
                ),
            )
        )
    if isinstance(root, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(root):
        filters.append(dm.filters.Equals(view_id.as_property_ref("root"), value=as_instance_dict_id(root)))
    if root and isinstance(root, Sequence) and not isinstance(root, str) and not is_tuple_id(root):
        filters.append(
            dm.filters.In(view_id.as_property_ref("root"), values=[as_instance_dict_id(item) for item in root])
        )
    if isinstance(source, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(source):
        filters.append(dm.filters.Equals(view_id.as_property_ref("source"), value=as_instance_dict_id(source)))
    if source and isinstance(source, Sequence) and not isinstance(source, str) and not is_tuple_id(source):
        filters.append(
            dm.filters.In(view_id.as_property_ref("source"), values=[as_instance_dict_id(item) for item in source])
        )
    if isinstance(source_context, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceContext"), value=source_context))
    if source_context and isinstance(source_context, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceContext"), values=source_context))
    if source_context_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("sourceContext"), value=source_context_prefix))
    if min_source_created_time is not None or max_source_created_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("sourceCreatedTime"),
                gte=min_source_created_time.isoformat(timespec="milliseconds") if min_source_created_time else None,
                lte=max_source_created_time.isoformat(timespec="milliseconds") if max_source_created_time else None,
            )
        )
    if isinstance(source_created_user, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceCreatedUser"), value=source_created_user))
    if source_created_user and isinstance(source_created_user, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceCreatedUser"), values=source_created_user))
    if source_created_user_prefix is not None:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("sourceCreatedUser"), value=source_created_user_prefix)
        )
    if isinstance(source_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceId"), value=source_id))
    if source_id and isinstance(source_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceId"), values=source_id))
    if source_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("sourceId"), value=source_id_prefix))
    if min_source_updated_time is not None or max_source_updated_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("sourceUpdatedTime"),
                gte=min_source_updated_time.isoformat(timespec="milliseconds") if min_source_updated_time else None,
                lte=max_source_updated_time.isoformat(timespec="milliseconds") if max_source_updated_time else None,
            )
        )
    if isinstance(source_updated_user, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceUpdatedUser"), value=source_updated_user))
    if source_updated_user and isinstance(source_updated_user, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceUpdatedUser"), values=source_updated_user))
    if source_updated_user_prefix is not None:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("sourceUpdatedUser"), value=source_updated_user_prefix)
        )
    if isinstance(type_, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(type_):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=as_instance_dict_id(type_)))
    if type_ and isinstance(type_, Sequence) and not isinstance(type_, str) and not is_tuple_id(type_):
        filters.append(
            dm.filters.In(view_id.as_property_ref("type"), values=[as_instance_dict_id(item) for item in type_])
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


class _CogniteAssetQuery(NodeQueryCore[T_DomainModelList, CogniteAssetList]):
    _view_id = CogniteAsset._view_id
    _result_cls = CogniteAsset
    _result_list_cls_end = CogniteAssetList

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
        from ._cognite_activity import _CogniteActivityQuery
        from ._cognite_asset_class import _CogniteAssetClassQuery
        from ._cognite_asset_type import _CogniteAssetTypeQuery
        from ._cognite_equipment import _CogniteEquipmentQuery
        from ._cognite_file import _CogniteFileQuery
        from ._cognite_source_system import _CogniteSourceSystemQuery
        from ._cognite_time_series import _CogniteTimeSeriesQuery

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

        if _CogniteActivityQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.activities = _CogniteActivityQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "CogniteActivity", "v1").as_property_ref("assets"),
                    direction="inwards",
                ),
                connection_name="activities",
                connection_property=ViewPropertyId(self._view_id, "activities"),
                connection_type="reverse-list",
            )

        if _CogniteAssetClassQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.asset_class = _CogniteAssetClassQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("assetClass"),
                    direction="outwards",
                ),
                connection_name="asset_class",
                connection_property=ViewPropertyId(self._view_id, "assetClass"),
            )

        if _CogniteAssetQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.children = _CogniteAssetQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "CogniteAsset", "v1").as_property_ref("parent"),
                    direction="inwards",
                ),
                connection_name="children",
                connection_property=ViewPropertyId(self._view_id, "children"),
            )

        if _CogniteEquipmentQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.equipment = _CogniteEquipmentQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "CogniteEquipment", "v1").as_property_ref("asset"),
                    direction="inwards",
                ),
                connection_name="equipment",
                connection_property=ViewPropertyId(self._view_id, "equipment"),
            )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.files = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "CogniteFile", "v1").as_property_ref("assets"),
                    direction="inwards",
                ),
                connection_name="files",
                connection_property=ViewPropertyId(self._view_id, "files"),
                connection_type="reverse-list",
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

        if _CogniteAssetQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.parent = _CogniteAssetQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("parent"),
                    direction="outwards",
                ),
                connection_name="parent",
                connection_property=ViewPropertyId(self._view_id, "parent"),
            )

        if _CogniteAssetQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.path = _CogniteAssetQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("path"),
                    direction="outwards",
                ),
                connection_name="path",
                connection_property=ViewPropertyId(self._view_id, "path"),
            )

        if _CogniteAssetQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.root = _CogniteAssetQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("root"),
                    direction="outwards",
                ),
                connection_name="root",
                connection_property=ViewPropertyId(self._view_id, "root"),
            )

        if _CogniteSourceSystemQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.source = _CogniteSourceSystemQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("source"),
                    direction="outwards",
                ),
                connection_name="source",
                connection_property=ViewPropertyId(self._view_id, "source"),
            )

        if _CogniteTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.time_series = _CogniteTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "CogniteTimeSeries", "v1").as_property_ref("assets"),
                    direction="inwards",
                ),
                connection_name="time_series",
                connection_property=ViewPropertyId(self._view_id, "timeSeries"),
                connection_type="reverse-list",
            )

        if _CogniteAssetTypeQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.type_ = _CogniteAssetTypeQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("type"),
                    direction="outwards",
                ),
                connection_name="type_",
                connection_property=ViewPropertyId(self._view_id, "type"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.asset_class_filter = DirectRelationFilter(self, self._view_id.as_property_ref("assetClass"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.object_3d_filter = DirectRelationFilter(self, self._view_id.as_property_ref("object3D"))
        self.parent_filter = DirectRelationFilter(self, self._view_id.as_property_ref("parent"))
        self.path_last_updated_time = TimestampFilter(self, self._view_id.as_property_ref("pathLastUpdatedTime"))
        self.root_filter = DirectRelationFilter(self, self._view_id.as_property_ref("root"))
        self.source_filter = DirectRelationFilter(self, self._view_id.as_property_ref("source"))
        self.source_context = StringFilter(self, self._view_id.as_property_ref("sourceContext"))
        self.source_created_time = TimestampFilter(self, self._view_id.as_property_ref("sourceCreatedTime"))
        self.source_created_user = StringFilter(self, self._view_id.as_property_ref("sourceCreatedUser"))
        self.source_id = StringFilter(self, self._view_id.as_property_ref("sourceId"))
        self.source_updated_time = TimestampFilter(self, self._view_id.as_property_ref("sourceUpdatedTime"))
        self.source_updated_user = StringFilter(self, self._view_id.as_property_ref("sourceUpdatedUser"))
        self.type__filter = DirectRelationFilter(self, self._view_id.as_property_ref("type"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.asset_class_filter,
                self.description,
                self.name,
                self.object_3d_filter,
                self.parent_filter,
                self.path_last_updated_time,
                self.root_filter,
                self.source_filter,
                self.source_context,
                self.source_created_time,
                self.source_created_user,
                self.source_id,
                self.source_updated_time,
                self.source_updated_user,
                self.type__filter,
            ]
        )

    def list_cognite_asset(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteAssetList:
        return self._list(limit=limit)


class CogniteAssetQuery(_CogniteAssetQuery[CogniteAssetList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteAssetList)
