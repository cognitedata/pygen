from __future__ import annotations

import datetime
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
    is_tuple_id,
    select_best_node,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    TimestampFilter,
)
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite
from cognite_core.data_classes._cognite_sourceable_node import CogniteSourceableNode, CogniteSourceableNodeWrite

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_activity import (
        CogniteActivity,
        CogniteActivityList,
        CogniteActivityGraphQL,
        CogniteActivityWrite,
        CogniteActivityWriteList,
    )
    from cognite_core.data_classes._cognite_asset import (
        CogniteAsset,
        CogniteAssetList,
        CogniteAssetGraphQL,
        CogniteAssetWrite,
        CogniteAssetWriteList,
    )
    from cognite_core.data_classes._cognite_equipment_type import (
        CogniteEquipmentType,
        CogniteEquipmentTypeList,
        CogniteEquipmentTypeGraphQL,
        CogniteEquipmentTypeWrite,
        CogniteEquipmentTypeWriteList,
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
    "CogniteEquipment",
    "CogniteEquipmentWrite",
    "CogniteEquipmentApply",
    "CogniteEquipmentList",
    "CogniteEquipmentWriteList",
    "CogniteEquipmentApplyList",
    "CogniteEquipmentFields",
    "CogniteEquipmentTextFields",
    "CogniteEquipmentGraphQL",
]


CogniteEquipmentTextFields = Literal[
    "external_id",
    "aliases",
    "description",
    "manufacturer",
    "name",
    "serial_number",
    "source_context",
    "source_created_user",
    "source_id",
    "source_updated_user",
    "tags",
]
CogniteEquipmentFields = Literal[
    "external_id",
    "aliases",
    "description",
    "manufacturer",
    "name",
    "serial_number",
    "source_context",
    "source_created_time",
    "source_created_user",
    "source_id",
    "source_updated_time",
    "source_updated_user",
    "tags",
]

_COGNITEEQUIPMENT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "manufacturer": "manufacturer",
    "name": "name",
    "serial_number": "serialNumber",
    "source_context": "sourceContext",
    "source_created_time": "sourceCreatedTime",
    "source_created_user": "sourceCreatedUser",
    "source_id": "sourceId",
    "source_updated_time": "sourceUpdatedTime",
    "source_updated_user": "sourceUpdatedUser",
    "tags": "tags",
}


class CogniteEquipmentGraphQL(GraphQLCore):
    """This represents the reading version of Cognite equipment, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite equipment.
        data_record: The data record of the Cognite equipment node.
        activities: An automatically updated list of activities related to the equipment.
        aliases: Alternative names for the node
        asset: The asset the equipment is related to.
        description: Description of the instance
        equipment_type: Specifies the type of the equipment. It's a direct relation to CogniteEquipmentType.
        files: A list of files the equipment relates to.
        manufacturer: The manufacturer of the equipment.
        name: Name of the instance
        serial_number: The serial number of the equipment.
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
        time_series: An automatically updated list of time series related to the equipment.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteEquipment", "v1")
    activities: Optional[list[CogniteActivityGraphQL]] = Field(default=None, repr=False)
    aliases: Optional[list[str]] = None
    asset: Optional[CogniteAssetGraphQL] = Field(default=None, repr=False)
    description: Optional[str] = None
    equipment_type: Optional[CogniteEquipmentTypeGraphQL] = Field(default=None, repr=False, alias="equipmentType")
    files: Optional[list[CogniteFileGraphQL]] = Field(default=None, repr=False)
    manufacturer: Optional[str] = None
    name: Optional[str] = None
    serial_number: Optional[str] = Field(None, alias="serialNumber")
    source: Optional[CogniteSourceSystemGraphQL] = Field(default=None, repr=False)
    source_context: Optional[str] = Field(None, alias="sourceContext")
    source_created_time: Optional[datetime.datetime] = Field(None, alias="sourceCreatedTime")
    source_created_user: Optional[str] = Field(None, alias="sourceCreatedUser")
    source_id: Optional[str] = Field(None, alias="sourceId")
    source_updated_time: Optional[datetime.datetime] = Field(None, alias="sourceUpdatedTime")
    source_updated_user: Optional[str] = Field(None, alias="sourceUpdatedUser")
    tags: Optional[list[str]] = None
    time_series: Optional[list[CogniteTimeSeriesGraphQL]] = Field(default=None, repr=False, alias="timeSeries")

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

    @field_validator("activities", "asset", "equipment_type", "files", "source", "time_series", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CogniteEquipment:
        """Convert this GraphQL format of Cognite equipment to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteEquipment(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            activities=[activity.as_read() for activity in self.activities] if self.activities is not None else None,
            aliases=self.aliases,
            asset=self.asset.as_read() if isinstance(self.asset, GraphQLCore) else self.asset,
            description=self.description,
            equipment_type=(
                self.equipment_type.as_read() if isinstance(self.equipment_type, GraphQLCore) else self.equipment_type
            ),
            files=[file.as_read() for file in self.files] if self.files is not None else None,
            manufacturer=self.manufacturer,
            name=self.name,
            serial_number=self.serial_number,
            source=self.source.as_read() if isinstance(self.source, GraphQLCore) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            tags=self.tags,
            time_series=(
                [time_series.as_read() for time_series in self.time_series] if self.time_series is not None else None
            ),
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteEquipmentWrite:
        """Convert this GraphQL format of Cognite equipment to the writing format."""
        return CogniteEquipmentWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aliases=self.aliases,
            asset=self.asset.as_write() if isinstance(self.asset, GraphQLCore) else self.asset,
            description=self.description,
            equipment_type=(
                self.equipment_type.as_write() if isinstance(self.equipment_type, GraphQLCore) else self.equipment_type
            ),
            files=[file.as_write() for file in self.files] if self.files is not None else None,
            manufacturer=self.manufacturer,
            name=self.name,
            serial_number=self.serial_number,
            source=self.source.as_write() if isinstance(self.source, GraphQLCore) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            tags=self.tags,
        )


class CogniteEquipment(CogniteDescribableNode, CogniteSourceableNode):
    """This represents the reading version of Cognite equipment.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite equipment.
        data_record: The data record of the Cognite equipment node.
        activities: An automatically updated list of activities related to the equipment.
        aliases: Alternative names for the node
        asset: The asset the equipment is related to.
        description: Description of the instance
        equipment_type: Specifies the type of the equipment. It's a direct relation to CogniteEquipmentType.
        files: A list of files the equipment relates to.
        manufacturer: The manufacturer of the equipment.
        name: Name of the instance
        serial_number: The serial number of the equipment.
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
        time_series: An automatically updated list of time series related to the equipment.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteEquipment", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    activities: Optional[list[CogniteActivity]] = Field(default=None, repr=False)
    asset: Union[CogniteAsset, str, dm.NodeId, None] = Field(default=None, repr=False)
    equipment_type: Union[CogniteEquipmentType, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="equipmentType"
    )
    files: Optional[list[Union[CogniteFile, str, dm.NodeId]]] = Field(default=None, repr=False)
    manufacturer: Optional[str] = None
    serial_number: Optional[str] = Field(None, alias="serialNumber")
    time_series: Optional[list[CogniteTimeSeries]] = Field(default=None, repr=False, alias="timeSeries")

    @field_validator("asset", "equipment_type", "source", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("activities", "files", "time_series", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteEquipmentWrite:
        """Convert this read version of Cognite equipment to the writing version."""
        return CogniteEquipmentWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aliases=self.aliases,
            asset=self.asset.as_write() if isinstance(self.asset, DomainModel) else self.asset,
            description=self.description,
            equipment_type=(
                self.equipment_type.as_write() if isinstance(self.equipment_type, DomainModel) else self.equipment_type
            ),
            files=(
                [file.as_write() if isinstance(file, DomainModel) else file for file in self.files]
                if self.files is not None
                else None
            ),
            manufacturer=self.manufacturer,
            name=self.name,
            serial_number=self.serial_number,
            source=self.source.as_write() if isinstance(self.source, DomainModel) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            tags=self.tags,
        )

    def as_apply(self) -> CogniteEquipmentWrite:
        """Convert this read version of Cognite equipment to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CogniteEquipmentWrite(CogniteDescribableNodeWrite, CogniteSourceableNodeWrite):
    """This represents the writing version of Cognite equipment.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite equipment.
        data_record: The data record of the Cognite equipment node.
        aliases: Alternative names for the node
        asset: The asset the equipment is related to.
        description: Description of the instance
        equipment_type: Specifies the type of the equipment. It's a direct relation to CogniteEquipmentType.
        files: A list of files the equipment relates to.
        manufacturer: The manufacturer of the equipment.
        name: Name of the instance
        serial_number: The serial number of the equipment.
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
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteEquipment", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    asset: Union[CogniteAssetWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    equipment_type: Union[CogniteEquipmentTypeWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="equipmentType"
    )
    files: Optional[list[Union[CogniteFileWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    manufacturer: Optional[str] = None
    serial_number: Optional[str] = Field(None, alias="serialNumber")

    @field_validator("asset", "equipment_type", "files", mode="before")
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

        if self.asset is not None:
            properties["asset"] = {
                "space": self.space if isinstance(self.asset, str) else self.asset.space,
                "externalId": self.asset if isinstance(self.asset, str) else self.asset.external_id,
            }

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.equipment_type is not None:
            properties["equipmentType"] = {
                "space": self.space if isinstance(self.equipment_type, str) else self.equipment_type.space,
                "externalId": (
                    self.equipment_type if isinstance(self.equipment_type, str) else self.equipment_type.external_id
                ),
            }

        if self.files is not None:
            properties["files"] = [
                {
                    "space": self.space if isinstance(file, str) else file.space,
                    "externalId": file if isinstance(file, str) else file.external_id,
                }
                for file in self.files or []
            ]

        if self.manufacturer is not None or write_none:
            properties["manufacturer"] = self.manufacturer

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.serial_number is not None or write_none:
            properties["serialNumber"] = self.serial_number

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

        if isinstance(self.asset, DomainModelWrite):
            other_resources = self.asset._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.equipment_type, DomainModelWrite):
            other_resources = self.equipment_type._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.source, DomainModelWrite):
            other_resources = self.source._to_instances_write(cache)
            resources.extend(other_resources)

        for file in self.files or []:
            if isinstance(file, DomainModelWrite):
                other_resources = file._to_instances_write(cache)
                resources.extend(other_resources)

        return resources


class CogniteEquipmentApply(CogniteEquipmentWrite):
    def __new__(cls, *args, **kwargs) -> CogniteEquipmentApply:
        warnings.warn(
            "CogniteEquipmentApply is deprecated and will be removed in v1.0. "
            "Use CogniteEquipmentWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteEquipment.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteEquipmentList(DomainModelList[CogniteEquipment]):
    """List of Cognite equipments in the read version."""

    _INSTANCE = CogniteEquipment

    def as_write(self) -> CogniteEquipmentWriteList:
        """Convert these read versions of Cognite equipment to the writing versions."""
        return CogniteEquipmentWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteEquipmentWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def activities(self) -> CogniteActivityList:
        from ._cognite_activity import CogniteActivity, CogniteActivityList

        return CogniteActivityList(
            [item for items in self.data for item in items.activities or [] if isinstance(item, CogniteActivity)]
        )

    @property
    def asset(self) -> CogniteAssetList:
        from ._cognite_asset import CogniteAsset, CogniteAssetList

        return CogniteAssetList([item.asset for item in self.data if isinstance(item.asset, CogniteAsset)])

    @property
    def equipment_type(self) -> CogniteEquipmentTypeList:
        from ._cognite_equipment_type import CogniteEquipmentType, CogniteEquipmentTypeList

        return CogniteEquipmentTypeList(
            [item.equipment_type for item in self.data if isinstance(item.equipment_type, CogniteEquipmentType)]
        )

    @property
    def files(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList(
            [item for items in self.data for item in items.files or [] if isinstance(item, CogniteFile)]
        )

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


class CogniteEquipmentWriteList(DomainModelWriteList[CogniteEquipmentWrite]):
    """List of Cognite equipments in the writing version."""

    _INSTANCE = CogniteEquipmentWrite

    @property
    def asset(self) -> CogniteAssetWriteList:
        from ._cognite_asset import CogniteAssetWrite, CogniteAssetWriteList

        return CogniteAssetWriteList([item.asset for item in self.data if isinstance(item.asset, CogniteAssetWrite)])

    @property
    def equipment_type(self) -> CogniteEquipmentTypeWriteList:
        from ._cognite_equipment_type import CogniteEquipmentTypeWrite, CogniteEquipmentTypeWriteList

        return CogniteEquipmentTypeWriteList(
            [item.equipment_type for item in self.data if isinstance(item.equipment_type, CogniteEquipmentTypeWrite)]
        )

    @property
    def files(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList(
            [item for items in self.data for item in items.files or [] if isinstance(item, CogniteFileWrite)]
        )

    @property
    def source(self) -> CogniteSourceSystemWriteList:
        from ._cognite_source_system import CogniteSourceSystemWrite, CogniteSourceSystemWriteList

        return CogniteSourceSystemWriteList(
            [item.source for item in self.data if isinstance(item.source, CogniteSourceSystemWrite)]
        )


class CogniteEquipmentApplyList(CogniteEquipmentWriteList): ...


def _create_cognite_equipment_filter(
    view_id: dm.ViewId,
    asset: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    equipment_type: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    files: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    manufacturer: str | list[str] | None = None,
    manufacturer_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    serial_number: str | list[str] | None = None,
    serial_number_prefix: str | None = None,
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
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(asset, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(asset):
        filters.append(dm.filters.Equals(view_id.as_property_ref("asset"), value=as_instance_dict_id(asset)))
    if asset and isinstance(asset, Sequence) and not isinstance(asset, str) and not is_tuple_id(asset):
        filters.append(
            dm.filters.In(view_id.as_property_ref("asset"), values=[as_instance_dict_id(item) for item in asset])
        )
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(equipment_type, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(equipment_type):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("equipmentType"), value=as_instance_dict_id(equipment_type))
        )
    if (
        equipment_type
        and isinstance(equipment_type, Sequence)
        and not isinstance(equipment_type, str)
        and not is_tuple_id(equipment_type)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("equipmentType"), values=[as_instance_dict_id(item) for item in equipment_type]
            )
        )
    if isinstance(files, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(files):
        filters.append(dm.filters.Equals(view_id.as_property_ref("files"), value=as_instance_dict_id(files)))
    if files and isinstance(files, Sequence) and not isinstance(files, str) and not is_tuple_id(files):
        filters.append(
            dm.filters.In(view_id.as_property_ref("files"), values=[as_instance_dict_id(item) for item in files])
        )
    if isinstance(manufacturer, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("manufacturer"), value=manufacturer))
    if manufacturer and isinstance(manufacturer, list):
        filters.append(dm.filters.In(view_id.as_property_ref("manufacturer"), values=manufacturer))
    if manufacturer_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("manufacturer"), value=manufacturer_prefix))
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(serial_number, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("serialNumber"), value=serial_number))
    if serial_number and isinstance(serial_number, list):
        filters.append(dm.filters.In(view_id.as_property_ref("serialNumber"), values=serial_number))
    if serial_number_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("serialNumber"), value=serial_number_prefix))
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _CogniteEquipmentQuery(NodeQueryCore[T_DomainModelList, CogniteEquipmentList]):
    _view_id = CogniteEquipment._view_id
    _result_cls = CogniteEquipment
    _result_list_cls_end = CogniteEquipmentList

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
        from ._cognite_activity import _CogniteActivityQuery
        from ._cognite_asset import _CogniteAssetQuery
        from ._cognite_equipment_type import _CogniteEquipmentTypeQuery
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

        if _CogniteActivityQuery not in created_types:
            self.activities = _CogniteActivityQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "CogniteActivity", "v1").as_property_ref("equipment"),
                    direction="inwards",
                ),
                connection_name="activities",
                connection_property=ViewPropertyId(self._view_id, "activities"),
                connection_type="reverse-list",
            )

        if _CogniteAssetQuery not in created_types:
            self.asset = _CogniteAssetQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("asset"),
                    direction="outwards",
                ),
                connection_name="asset",
                connection_property=ViewPropertyId(self._view_id, "asset"),
            )

        if _CogniteEquipmentTypeQuery not in created_types:
            self.equipment_type = _CogniteEquipmentTypeQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("equipmentType"),
                    direction="outwards",
                ),
                connection_name="equipment_type",
                connection_property=ViewPropertyId(self._view_id, "equipmentType"),
            )

        if _CogniteFileQuery not in created_types:
            self.files = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("files"),
                    direction="outwards",
                ),
                connection_name="files",
                connection_property=ViewPropertyId(self._view_id, "files"),
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
                connection_property=ViewPropertyId(self._view_id, "source"),
            )

        if _CogniteTimeSeriesQuery not in created_types:
            self.time_series = _CogniteTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "CogniteTimeSeries", "v1").as_property_ref("equipment"),
                    direction="inwards",
                ),
                connection_name="time_series",
                connection_property=ViewPropertyId(self._view_id, "timeSeries"),
                connection_type="reverse-list",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.manufacturer = StringFilter(self, self._view_id.as_property_ref("manufacturer"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.serial_number = StringFilter(self, self._view_id.as_property_ref("serialNumber"))
        self.source_context = StringFilter(self, self._view_id.as_property_ref("sourceContext"))
        self.source_created_time = TimestampFilter(self, self._view_id.as_property_ref("sourceCreatedTime"))
        self.source_created_user = StringFilter(self, self._view_id.as_property_ref("sourceCreatedUser"))
        self.source_id = StringFilter(self, self._view_id.as_property_ref("sourceId"))
        self.source_updated_time = TimestampFilter(self, self._view_id.as_property_ref("sourceUpdatedTime"))
        self.source_updated_user = StringFilter(self, self._view_id.as_property_ref("sourceUpdatedUser"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.manufacturer,
                self.name,
                self.serial_number,
                self.source_context,
                self.source_created_time,
                self.source_created_user,
                self.source_id,
                self.source_updated_time,
                self.source_updated_user,
            ]
        )

    def list_cognite_equipment(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteEquipmentList:
        return self._list(limit=limit)


class CogniteEquipmentQuery(_CogniteEquipmentQuery[CogniteEquipmentList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteEquipmentList)
