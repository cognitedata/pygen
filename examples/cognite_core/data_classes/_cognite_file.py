from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union, no_type_check

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from pydantic import Field, field_validator, model_validator

from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite
from cognite_core.data_classes._cognite_sourceable_node import CogniteSourceableNode, CogniteSourceableNodeWrite
from cognite_core.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    BooleanFilter,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelList,
    DomainModelWrite,
    DomainModelWriteList,
    DomainRelation,
    FileContentAPI,
    GraphQLCore,
    NodeQueryCore,
    QueryCore,
    ResourcesWrite,
    StringFilter,
    T_DomainModelList,
    TimestampFilter,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_pygen_node_id,
    is_tuple_id,
)

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_asset import (
        CogniteAsset,
        CogniteAssetGraphQL,
        CogniteAssetList,
        CogniteAssetWrite,
        CogniteAssetWriteList,
    )
    from cognite_core.data_classes._cognite_equipment import (
        CogniteEquipment,
        CogniteEquipmentGraphQL,
        CogniteEquipmentList,
    )
    from cognite_core.data_classes._cognite_file_category import (
        CogniteFileCategory,
        CogniteFileCategoryGraphQL,
        CogniteFileCategoryList,
        CogniteFileCategoryWrite,
        CogniteFileCategoryWriteList,
    )
    from cognite_core.data_classes._cognite_source_system import (
        CogniteSourceSystemGraphQL,
        CogniteSourceSystemList,
        CogniteSourceSystemWriteList,
    )


__all__ = [
    "CogniteFile",
    "CogniteFileWrite",
    "CogniteFileApply",
    "CogniteFileList",
    "CogniteFileWriteList",
    "CogniteFileApplyList",
    "CogniteFileFields",
    "CogniteFileTextFields",
    "CogniteFileGraphQL",
]


CogniteFileTextFields = Literal[
    "external_id",
    "aliases",
    "description",
    "directory",
    "mime_type",
    "name",
    "source_context",
    "source_created_user",
    "source_id",
    "source_updated_user",
    "tags",
]
CogniteFileFields = Literal[
    "external_id",
    "aliases",
    "description",
    "directory",
    "is_uploaded",
    "mime_type",
    "name",
    "source_context",
    "source_created_time",
    "source_created_user",
    "source_id",
    "source_updated_time",
    "source_updated_user",
    "tags",
    "uploaded_time",
]

_COGNITEFILE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "directory": "directory",
    "is_uploaded": "isUploaded",
    "mime_type": "mimeType",
    "name": "name",
    "source_context": "sourceContext",
    "source_created_time": "sourceCreatedTime",
    "source_created_user": "sourceCreatedUser",
    "source_id": "sourceId",
    "source_updated_time": "sourceUpdatedTime",
    "source_updated_user": "sourceUpdatedUser",
    "tags": "tags",
    "uploaded_time": "uploadedTime",
}


class CogniteFileGraphQL(GraphQLCore):
    """This represents the reading version of Cognite file, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite file.
        data_record: The data record of the Cognite file node.
        aliases: Alternative names for the node
        assets: A list of assets this file is related to.
        category: Specifies the detected category the file belongs to. It's a direct relation to an instance of CogniteFileCategory.
        description: Description of the instance
        directory: Contains the path elements from the source (if the source system has a file system hierarchy or similar.)
        equipment: An automatically updated list of equipment this file is related to.
        is_uploaded: Specifies if the file content has been uploaded to Cognite Data Fusion or not.
        mime_type: The MIME type of the file.
        name: Name of the instance
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        tags: Text based labels for generic use, limited to 1000
        uploaded_time: The time the file upload completed.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteFile", "v1")
    aliases: Optional[list[str]] = None
    assets: Optional[list[CogniteAssetGraphQL]] = Field(default=None, repr=False)
    category: Optional[CogniteFileCategoryGraphQL] = Field(default=None, repr=False)
    description: Optional[str] = None
    directory: Optional[str] = None
    equipment: Optional[list[CogniteEquipmentGraphQL]] = Field(default=None, repr=False)
    is_uploaded: Optional[bool] = Field(None, alias="isUploaded")
    mime_type: Optional[str] = Field(None, alias="mimeType")
    name: Optional[str] = None
    source: Optional[CogniteSourceSystemGraphQL] = Field(default=None, repr=False)
    source_context: Optional[str] = Field(None, alias="sourceContext")
    source_created_time: Optional[datetime.datetime] = Field(None, alias="sourceCreatedTime")
    source_created_user: Optional[str] = Field(None, alias="sourceCreatedUser")
    source_id: Optional[str] = Field(None, alias="sourceId")
    source_updated_time: Optional[datetime.datetime] = Field(None, alias="sourceUpdatedTime")
    source_updated_user: Optional[str] = Field(None, alias="sourceUpdatedUser")
    tags: Optional[list[str]] = None
    uploaded_time: Optional[datetime.datetime] = Field(None, alias="uploadedTime")

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

    @field_validator("assets", "category", "equipment", "source", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CogniteFile:
        """Convert this GraphQL format of Cognite file to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteFile(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            aliases=self.aliases,
            assets=[asset.as_read() for asset in self.assets] if self.assets is not None else None,
            category=self.category.as_read() if isinstance(self.category, GraphQLCore) else self.category,
            description=self.description,
            directory=self.directory,
            equipment=[equipment.as_read() for equipment in self.equipment] if self.equipment is not None else None,
            is_uploaded=self.is_uploaded,
            mime_type=self.mime_type,
            name=self.name,
            source=self.source.as_read() if isinstance(self.source, GraphQLCore) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            tags=self.tags,
            uploaded_time=self.uploaded_time,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteFileWrite:
        """Convert this GraphQL format of Cognite file to the writing format."""
        return CogniteFileWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aliases=self.aliases,
            assets=[asset.as_write() for asset in self.assets] if self.assets is not None else None,
            category=self.category.as_write() if isinstance(self.category, GraphQLCore) else self.category,
            description=self.description,
            directory=self.directory,
            mime_type=self.mime_type,
            name=self.name,
            source=self.source.as_write() if isinstance(self.source, GraphQLCore) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            tags=self.tags,
        )


class CogniteFile(CogniteDescribableNode, CogniteSourceableNode):
    """This represents the reading version of Cognite file.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite file.
        data_record: The data record of the Cognite file node.
        aliases: Alternative names for the node
        assets: A list of assets this file is related to.
        category: Specifies the detected category the file belongs to. It's a direct relation to an instance of CogniteFileCategory.
        description: Description of the instance
        directory: Contains the path elements from the source (if the source system has a file system hierarchy or similar.)
        equipment: An automatically updated list of equipment this file is related to.
        is_uploaded: Specifies if the file content has been uploaded to Cognite Data Fusion or not.
        mime_type: The MIME type of the file.
        name: Name of the instance
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        tags: Text based labels for generic use, limited to 1000
        uploaded_time: The time the file upload completed.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteFile", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    assets: Optional[list[Union[CogniteAsset, str, dm.NodeId]]] = Field(default=None, repr=False)
    category: Union[CogniteFileCategory, str, dm.NodeId, None] = Field(default=None, repr=False)
    directory: Optional[str] = None
    equipment: Optional[list[CogniteEquipment]] = Field(default=None, repr=False)
    is_uploaded: Optional[bool] = Field(None, alias="isUploaded")
    mime_type: Optional[str] = Field(None, alias="mimeType")
    uploaded_time: Optional[datetime.datetime] = Field(None, alias="uploadedTime")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteFileWrite:
        """Convert this read version of Cognite file to the writing version."""
        return CogniteFileWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aliases=self.aliases,
            assets=(
                [asset.as_write() if isinstance(asset, DomainModel) else asset for asset in self.assets]
                if self.assets is not None
                else None
            ),
            category=self.category.as_write() if isinstance(self.category, DomainModel) else self.category,
            description=self.description,
            directory=self.directory,
            mime_type=self.mime_type,
            name=self.name,
            source=self.source.as_write() if isinstance(self.source, DomainModel) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            tags=self.tags,
        )

    def as_apply(self) -> CogniteFileWrite:
        """Convert this read version of Cognite file to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, CogniteFile],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._cognite_asset import CogniteAsset
        from ._cognite_equipment import CogniteEquipment
        from ._cognite_file_category import CogniteFileCategory
        from ._cognite_source_system import CogniteSourceSystem

        for instance in instances.values():
            if (
                isinstance(instance.category, (dm.NodeId, str))
                and (category := nodes_by_id.get(instance.category))
                and isinstance(category, CogniteFileCategory)
            ):
                instance.category = category
            if (
                isinstance(instance.source, (dm.NodeId, str))
                and (source := nodes_by_id.get(instance.source))
                and isinstance(source, CogniteSourceSystem)
            ):
                instance.source = source
            if instance.assets:
                new_assets: list[CogniteAsset | str | dm.NodeId] = []
                for asset in instance.assets:
                    if isinstance(asset, CogniteAsset):
                        new_assets.append(asset)
                    elif (other := nodes_by_id.get(asset)) and isinstance(other, CogniteAsset):
                        new_assets.append(other)
                    else:
                        new_assets.append(asset)
                instance.assets = new_assets
        for node in nodes_by_id.values():
            if isinstance(node, CogniteEquipment) and node.files is not None:
                for files in node.files:
                    if this_instance := instances.get(as_pygen_node_id(files)):
                        if this_instance.equipment is None:
                            this_instance.equipment = [node]
                        else:
                            this_instance.equipment.append(node)


class CogniteFileWrite(CogniteDescribableNodeWrite, CogniteSourceableNodeWrite):
    """This represents the writing version of Cognite file.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite file.
        data_record: The data record of the Cognite file node.
        aliases: Alternative names for the node
        assets: A list of assets this file is related to.
        category: Specifies the detected category the file belongs to. It's a direct relation to an instance of CogniteFileCategory.
        description: Description of the instance
        directory: Contains the path elements from the source (if the source system has a file system hierarchy or similar.)
        mime_type: The MIME type of the file.
        name: Name of the instance
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        tags: Text based labels for generic use, limited to 1000
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteFile", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    assets: Optional[list[Union[CogniteAssetWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    category: Union[CogniteFileCategoryWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    directory: Optional[str] = None
    mime_type: Optional[str] = Field(None, alias="mimeType")

    @field_validator("assets", "category", mode="before")
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

        if self.assets is not None:
            properties["assets"] = [
                {
                    "space": self.space if isinstance(asset, str) else asset.space,
                    "externalId": asset if isinstance(asset, str) else asset.external_id,
                }
                for asset in self.assets or []
            ]

        if self.category is not None:
            properties["category"] = {
                "space": self.space if isinstance(self.category, str) else self.category.space,
                "externalId": self.category if isinstance(self.category, str) else self.category.external_id,
            }

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.directory is not None or write_none:
            properties["directory"] = self.directory

        if self.mime_type is not None or write_none:
            properties["mimeType"] = self.mime_type

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.source is not None:
            properties["source"] = {
                "space": self.space if isinstance(self.source, str) else self.source.space,
                "externalId": self.source if isinstance(self.source, str) else self.source.external_id,
            }

        if self.source_context is not None or write_none:
            properties["sourceContext"] = self.source_context

        if self.source_created_time is not None or write_none:
            properties["sourceCreatedTime"] = (
                self.source_created_time.isoformat(timespec="milliseconds") if self.source_created_time else None
            )

        if self.source_created_user is not None or write_none:
            properties["sourceCreatedUser"] = self.source_created_user

        if self.source_id is not None or write_none:
            properties["sourceId"] = self.source_id

        if self.source_updated_time is not None or write_none:
            properties["sourceUpdatedTime"] = (
                self.source_updated_time.isoformat(timespec="milliseconds") if self.source_updated_time else None
            )

        if self.source_updated_user is not None or write_none:
            properties["sourceUpdatedUser"] = self.source_updated_user

        if self.tags is not None or write_none:
            properties["tags"] = self.tags

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

        if isinstance(self.category, DomainModelWrite):
            other_resources = self.category._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.source, DomainModelWrite):
            other_resources = self.source._to_instances_write(cache)
            resources.extend(other_resources)

        for asset in self.assets or []:
            if isinstance(asset, DomainModelWrite):
                other_resources = asset._to_instances_write(cache)
                resources.extend(other_resources)

        return resources


class CogniteFileApply(CogniteFileWrite):
    def __new__(cls, *args, **kwargs) -> CogniteFileApply:
        warnings.warn(
            "CogniteFileApply is deprecated and will be removed in v1.0. Use CogniteFileWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteFile.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteFileList(DomainModelList[CogniteFile]):
    """List of Cognite files in the read version."""

    _INSTANCE = CogniteFile

    def as_write(self) -> CogniteFileWriteList:
        """Convert these read versions of Cognite file to the writing versions."""
        return CogniteFileWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteFileWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def assets(self) -> CogniteAssetList:
        from ._cognite_asset import CogniteAsset, CogniteAssetList

        return CogniteAssetList(
            [item for items in self.data for item in items.assets or [] if isinstance(item, CogniteAsset)]
        )

    @property
    def category(self) -> CogniteFileCategoryList:
        from ._cognite_file_category import CogniteFileCategory, CogniteFileCategoryList

        return CogniteFileCategoryList(
            [item.category for item in self.data if isinstance(item.category, CogniteFileCategory)]
        )

    @property
    def equipment(self) -> CogniteEquipmentList:
        from ._cognite_equipment import CogniteEquipment, CogniteEquipmentList

        return CogniteEquipmentList(
            [item for items in self.data for item in items.equipment or [] if isinstance(item, CogniteEquipment)]
        )

    @property
    def source(self) -> CogniteSourceSystemList:
        from ._cognite_source_system import CogniteSourceSystem, CogniteSourceSystemList

        return CogniteSourceSystemList(
            [item.source for item in self.data if isinstance(item.source, CogniteSourceSystem)]
        )


class CogniteFileWriteList(DomainModelWriteList[CogniteFileWrite]):
    """List of Cognite files in the writing version."""

    _INSTANCE = CogniteFileWrite

    @property
    def assets(self) -> CogniteAssetWriteList:
        from ._cognite_asset import CogniteAssetWrite, CogniteAssetWriteList

        return CogniteAssetWriteList(
            [item for items in self.data for item in items.assets or [] if isinstance(item, CogniteAssetWrite)]
        )

    @property
    def category(self) -> CogniteFileCategoryWriteList:
        from ._cognite_file_category import CogniteFileCategoryWrite, CogniteFileCategoryWriteList

        return CogniteFileCategoryWriteList(
            [item.category for item in self.data if isinstance(item.category, CogniteFileCategoryWrite)]
        )

    @property
    def source(self) -> CogniteSourceSystemWriteList:
        from ._cognite_source_system import CogniteSourceSystemWrite, CogniteSourceSystemWriteList

        return CogniteSourceSystemWriteList(
            [item.source for item in self.data if isinstance(item.source, CogniteSourceSystemWrite)]
        )


class CogniteFileApplyList(CogniteFileWriteList): ...


def _create_cognite_file_filter(
    view_id: dm.ViewId,
    assets: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    category: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    directory: str | list[str] | None = None,
    directory_prefix: str | None = None,
    is_uploaded: bool | None = None,
    mime_type: str | list[str] | None = None,
    mime_type_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    min_uploaded_time: datetime.datetime | None = None,
    max_uploaded_time: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(assets, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(assets):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assets"), value=as_instance_dict_id(assets)))
    if assets and isinstance(assets, Sequence) and not isinstance(assets, str) and not is_tuple_id(assets):
        filters.append(
            dm.filters.In(view_id.as_property_ref("assets"), values=[as_instance_dict_id(item) for item in assets])
        )
    if isinstance(category, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(category):
        filters.append(dm.filters.Equals(view_id.as_property_ref("category"), value=as_instance_dict_id(category)))
    if category and isinstance(category, Sequence) and not isinstance(category, str) and not is_tuple_id(category):
        filters.append(
            dm.filters.In(view_id.as_property_ref("category"), values=[as_instance_dict_id(item) for item in category])
        )
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(directory, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("directory"), value=directory))
    if directory and isinstance(directory, list):
        filters.append(dm.filters.In(view_id.as_property_ref("directory"), values=directory))
    if directory_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("directory"), value=directory_prefix))
    if isinstance(is_uploaded, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isUploaded"), value=is_uploaded))
    if isinstance(mime_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("mimeType"), value=mime_type))
    if mime_type and isinstance(mime_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("mimeType"), values=mime_type))
    if mime_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("mimeType"), value=mime_type_prefix))
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
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
    if min_uploaded_time is not None or max_uploaded_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("uploadedTime"),
                gte=min_uploaded_time.isoformat(timespec="milliseconds") if min_uploaded_time else None,
                lte=max_uploaded_time.isoformat(timespec="milliseconds") if max_uploaded_time else None,
            )
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


class _CogniteFileQuery(NodeQueryCore[T_DomainModelList, CogniteFileList]):
    _view_id = CogniteFile._view_id
    _result_cls = CogniteFile
    _result_list_cls_end = CogniteFileList

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
        from ._cognite_asset import _CogniteAssetQuery
        from ._cognite_equipment import _CogniteEquipmentQuery
        from ._cognite_file_category import _CogniteFileCategoryQuery
        from ._cognite_source_system import _CogniteSourceSystemQuery

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

        if _CogniteAssetQuery not in created_types:
            self.assets = _CogniteAssetQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("assets"),
                    direction="outwards",
                ),
                connection_name="assets",
            )

        if _CogniteFileCategoryQuery not in created_types:
            self.category = _CogniteFileCategoryQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("category"),
                    direction="outwards",
                ),
                connection_name="category",
            )

        if _CogniteEquipmentQuery not in created_types:
            self.equipment = _CogniteEquipmentQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "CogniteEquipment", "v1").as_property_ref("files"),
                    direction="inwards",
                ),
                connection_name="equipment",
                connection_type="reverse-list",
            )

        if _CogniteSourceSystemQuery not in created_types:
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.directory = StringFilter(self, self._view_id.as_property_ref("directory"))
        self.is_uploaded = BooleanFilter(self, self._view_id.as_property_ref("isUploaded"))
        self.mime_type = StringFilter(self, self._view_id.as_property_ref("mimeType"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.source_context = StringFilter(self, self._view_id.as_property_ref("sourceContext"))
        self.source_created_time = TimestampFilter(self, self._view_id.as_property_ref("sourceCreatedTime"))
        self.source_created_user = StringFilter(self, self._view_id.as_property_ref("sourceCreatedUser"))
        self.source_id = StringFilter(self, self._view_id.as_property_ref("sourceId"))
        self.source_updated_time = TimestampFilter(self, self._view_id.as_property_ref("sourceUpdatedTime"))
        self.source_updated_user = StringFilter(self, self._view_id.as_property_ref("sourceUpdatedUser"))
        self.uploaded_time = TimestampFilter(self, self._view_id.as_property_ref("uploadedTime"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.directory,
                self.is_uploaded,
                self.mime_type,
                self.name,
                self.source_context,
                self.source_created_time,
                self.source_created_user,
                self.source_id,
                self.source_updated_time,
                self.source_updated_user,
                self.uploaded_time,
            ]
        )
        self.content = FileContentAPI(client, lambda limit: self._list(limit=limit).as_node_ids())

    def list_cognite_file(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteFileList:
        return self._list(limit=limit)


class CogniteFileQuery(_CogniteFileQuery[CogniteFileList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteFileList)
