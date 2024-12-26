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
    DataPointsAPI,
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
    BooleanFilter,
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
    from cognite_core.data_classes._cognite_equipment import (
        CogniteEquipment,
        CogniteEquipmentList,
        CogniteEquipmentGraphQL,
        CogniteEquipmentWrite,
        CogniteEquipmentWriteList,
    )
    from cognite_core.data_classes._cognite_source_system import (
        CogniteSourceSystem,
        CogniteSourceSystemList,
        CogniteSourceSystemGraphQL,
        CogniteSourceSystemWrite,
        CogniteSourceSystemWriteList,
    )
    from cognite_core.data_classes._cognite_unit import (
        CogniteUnit,
        CogniteUnitList,
        CogniteUnitGraphQL,
        CogniteUnitWrite,
        CogniteUnitWriteList,
    )


__all__ = [
    "CogniteTimeSeries",
    "CogniteTimeSeriesWrite",
    "CogniteTimeSeriesApply",
    "CogniteTimeSeriesList",
    "CogniteTimeSeriesWriteList",
    "CogniteTimeSeriesApplyList",
    "CogniteTimeSeriesFields",
    "CogniteTimeSeriesTextFields",
    "CogniteTimeSeriesGraphQL",
]


CogniteTimeSeriesTextFields = Literal[
    "external_id",
    "aliases",
    "description",
    "name",
    "source_context",
    "source_created_user",
    "source_id",
    "source_unit",
    "source_updated_user",
    "tags",
]
CogniteTimeSeriesFields = Literal[
    "external_id",
    "aliases",
    "description",
    "is_step",
    "name",
    "source_context",
    "source_created_time",
    "source_created_user",
    "source_id",
    "source_unit",
    "source_updated_time",
    "source_updated_user",
    "tags",
    "type_",
]

_COGNITETIMESERIES_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "is_step": "isStep",
    "name": "name",
    "source_context": "sourceContext",
    "source_created_time": "sourceCreatedTime",
    "source_created_user": "sourceCreatedUser",
    "source_id": "sourceId",
    "source_unit": "sourceUnit",
    "source_updated_time": "sourceUpdatedTime",
    "source_updated_user": "sourceUpdatedUser",
    "tags": "tags",
    "type_": "type",
}


class CogniteTimeSeriesGraphQL(GraphQLCore):
    """This represents the reading version of Cognite time series, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite time series.
        data_record: The data record of the Cognite time series node.
        activities: An automatically updated list of activities the time series is related to.
        aliases: Alternative names for the node
        assets: A list of assets the time series is related to.
        description: Description of the instance
        equipment: A list of equipment the time series is related to.
        is_step: Specifies whether the time series is a step time series or not.
        name: Name of the instance
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is
            not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_unit: The unit specified in the source system.
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This
            identifier is not guaranteed to match the user identifiers in CDF
        tags: Text based labels for generic use, limited to 1000
        type_: Specifies the data type of the data points.
        unit: The unit of the time series.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteTimeSeries", "v1")
    activities: Optional[list[CogniteActivityGraphQL]] = Field(default=None, repr=False)
    aliases: Optional[list[str]] = None
    assets: Optional[list[CogniteAssetGraphQL]] = Field(default=None, repr=False)
    description: Optional[str] = None
    equipment: Optional[list[CogniteEquipmentGraphQL]] = Field(default=None, repr=False)
    is_step: Optional[bool] = Field(None, alias="isStep")
    name: Optional[str] = None
    source: Optional[CogniteSourceSystemGraphQL] = Field(default=None, repr=False)
    source_context: Optional[str] = Field(None, alias="sourceContext")
    source_created_time: Optional[datetime.datetime] = Field(None, alias="sourceCreatedTime")
    source_created_user: Optional[str] = Field(None, alias="sourceCreatedUser")
    source_id: Optional[str] = Field(None, alias="sourceId")
    source_unit: Optional[str] = Field(None, alias="sourceUnit")
    source_updated_time: Optional[datetime.datetime] = Field(None, alias="sourceUpdatedTime")
    source_updated_user: Optional[str] = Field(None, alias="sourceUpdatedUser")
    tags: Optional[list[str]] = None
    type_: Optional[Literal["numeric", "string"]] = Field(None, alias="type")
    unit: Optional[CogniteUnitGraphQL] = Field(default=None, repr=False)

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

    @field_validator("activities", "assets", "equipment", "source", "unit", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CogniteTimeSeries:
        """Convert this GraphQL format of Cognite time series to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteTimeSeries(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            activities=[activity.as_read() for activity in self.activities] if self.activities is not None else None,
            aliases=self.aliases,
            assets=[asset.as_read() for asset in self.assets] if self.assets is not None else None,
            description=self.description,
            equipment=[equipment.as_read() for equipment in self.equipment] if self.equipment is not None else None,
            is_step=self.is_step,
            name=self.name,
            source=self.source.as_read() if isinstance(self.source, GraphQLCore) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_unit=self.source_unit,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            tags=self.tags,
            type_=self.type_,
            unit=self.unit.as_read() if isinstance(self.unit, GraphQLCore) else self.unit,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteTimeSeriesWrite:
        """Convert this GraphQL format of Cognite time series to the writing format."""
        return CogniteTimeSeriesWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aliases=self.aliases,
            assets=[asset.as_write() for asset in self.assets] if self.assets is not None else None,
            description=self.description,
            equipment=[equipment.as_write() for equipment in self.equipment] if self.equipment is not None else None,
            is_step=self.is_step,
            name=self.name,
            source=self.source.as_write() if isinstance(self.source, GraphQLCore) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_unit=self.source_unit,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            tags=self.tags,
            type_=self.type_,
            unit=self.unit.as_write() if isinstance(self.unit, GraphQLCore) else self.unit,
        )


class CogniteTimeSeries(CogniteDescribableNode, CogniteSourceableNode):
    """This represents the reading version of Cognite time series.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite time series.
        data_record: The data record of the Cognite time series node.
        activities: An automatically updated list of activities the time series is related to.
        aliases: Alternative names for the node
        assets: A list of assets the time series is related to.
        description: Description of the instance
        equipment: A list of equipment the time series is related to.
        is_step: Specifies whether the time series is a step time series or not.
        name: Name of the instance
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is
            not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_unit: The unit specified in the source system.
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This
            identifier is not guaranteed to match the user identifiers in CDF
        tags: Text based labels for generic use, limited to 1000
        type_: Specifies the data type of the data points.
        unit: The unit of the time series.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteTimeSeries", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    activities: Optional[list[CogniteActivity]] = Field(default=None, repr=False)
    assets: Optional[list[Union[CogniteAsset, str, dm.NodeId]]] = Field(default=None, repr=False)
    equipment: Optional[list[Union[CogniteEquipment, str, dm.NodeId]]] = Field(default=None, repr=False)
    is_step: bool = Field(alias="isStep")
    source_unit: Optional[str] = Field(None, alias="sourceUnit")
    type_: Literal["numeric", "string"] = Field(alias="type")
    unit: Union[CogniteUnit, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("source", "unit", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteTimeSeriesWrite:
        """Convert this read version of Cognite time series to the writing version."""
        return CogniteTimeSeriesWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aliases=self.aliases,
            assets=(
                [asset.as_write() if isinstance(asset, DomainModel) else asset for asset in self.assets]
                if self.assets is not None
                else None
            ),
            description=self.description,
            equipment=(
                [
                    equipment.as_write() if isinstance(equipment, DomainModel) else equipment
                    for equipment in self.equipment
                ]
                if self.equipment is not None
                else None
            ),
            is_step=self.is_step,
            name=self.name,
            source=self.source.as_write() if isinstance(self.source, DomainModel) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_unit=self.source_unit,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            tags=self.tags,
            type_=self.type_,
            unit=self.unit.as_write() if isinstance(self.unit, DomainModel) else self.unit,
        )

    def as_apply(self) -> CogniteTimeSeriesWrite:
        """Convert this read version of Cognite time series to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, CogniteTimeSeries],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._cognite_activity import CogniteActivity
        from ._cognite_asset import CogniteAsset
        from ._cognite_equipment import CogniteEquipment
        from ._cognite_source_system import CogniteSourceSystem
        from ._cognite_unit import CogniteUnit

        for instance in instances.values():
            if (
                isinstance(instance.source, dm.NodeId | str)
                and (source := nodes_by_id.get(instance.source))
                and isinstance(source, CogniteSourceSystem)
            ):
                instance.source = source
            if (
                isinstance(instance.unit, dm.NodeId | str)
                and (unit := nodes_by_id.get(instance.unit))
                and isinstance(unit, CogniteUnit)
            ):
                instance.unit = unit
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
            if instance.equipment:
                new_equipment: list[CogniteEquipment | str | dm.NodeId] = []
                for equipment in instance.equipment:
                    if isinstance(equipment, CogniteEquipment):
                        new_equipment.append(equipment)
                    elif (other := nodes_by_id.get(equipment)) and isinstance(other, CogniteEquipment):
                        new_equipment.append(other)
                    else:
                        new_equipment.append(equipment)
                instance.equipment = new_equipment
        for node in nodes_by_id.values():
            if isinstance(node, CogniteActivity) and node.time_series is not None:
                for time_series in node.time_series:
                    if this_instance := instances.get(as_pygen_node_id(time_series)):
                        if this_instance.activities is None:
                            this_instance.activities = [node]
                        else:
                            this_instance.activities.append(node)


class CogniteTimeSeriesWrite(CogniteDescribableNodeWrite, CogniteSourceableNodeWrite):
    """This represents the writing version of Cognite time series.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite time series.
        data_record: The data record of the Cognite time series node.
        aliases: Alternative names for the node
        assets: A list of assets the time series is related to.
        description: Description of the instance
        equipment: A list of equipment the time series is related to.
        is_step: Specifies whether the time series is a step time series or not.
        name: Name of the instance
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is
            not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_unit: The unit specified in the source system.
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This
            identifier is not guaranteed to match the user identifiers in CDF
        tags: Text based labels for generic use, limited to 1000
        type_: Specifies the data type of the data points.
        unit: The unit of the time series.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteTimeSeries", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    assets: Optional[list[Union[CogniteAssetWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    equipment: Optional[list[Union[CogniteEquipmentWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    is_step: bool = Field(alias="isStep")
    source_unit: Optional[str] = Field(None, alias="sourceUnit")
    type_: Literal["numeric", "string"] = Field(alias="type")
    unit: Union[CogniteUnitWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("assets", "equipment", "unit", mode="before")
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

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.equipment is not None:
            properties["equipment"] = [
                {
                    "space": self.space if isinstance(equipment, str) else equipment.space,
                    "externalId": equipment if isinstance(equipment, str) else equipment.external_id,
                }
                for equipment in self.equipment or []
            ]

        if self.is_step is not None:
            properties["isStep"] = self.is_step

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

        if self.source_unit is not None or write_none:
            properties["sourceUnit"] = self.source_unit

        if self.source_updated_time is not None or write_none:
            properties["sourceUpdatedTime"] = (
                self.source_updated_time.isoformat(timespec="milliseconds") if self.source_updated_time else None
            )

        if self.source_updated_user is not None or write_none:
            properties["sourceUpdatedUser"] = self.source_updated_user

        if self.tags is not None or write_none:
            properties["tags"] = self.tags

        if self.type_ is not None:
            properties["type"] = self.type_

        if self.unit is not None:
            properties["unit"] = {
                "space": self.space if isinstance(self.unit, str) else self.unit.space,
                "externalId": self.unit if isinstance(self.unit, str) else self.unit.external_id,
            }

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

        if isinstance(self.source, DomainModelWrite):
            other_resources = self.source._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.unit, DomainModelWrite):
            other_resources = self.unit._to_instances_write(cache)
            resources.extend(other_resources)

        for asset in self.assets or []:
            if isinstance(asset, DomainModelWrite):
                other_resources = asset._to_instances_write(cache)
                resources.extend(other_resources)

        for equipment in self.equipment or []:
            if isinstance(equipment, DomainModelWrite):
                other_resources = equipment._to_instances_write(cache)
                resources.extend(other_resources)

        return resources


class CogniteTimeSeriesApply(CogniteTimeSeriesWrite):
    def __new__(cls, *args, **kwargs) -> CogniteTimeSeriesApply:
        warnings.warn(
            "CogniteTimeSeriesApply is deprecated and will be removed in v1.0. "
            "Use CogniteTimeSeriesWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteTimeSeries.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteTimeSeriesList(DomainModelList[CogniteTimeSeries]):
    """List of Cognite time series in the read version."""

    _INSTANCE = CogniteTimeSeries

    def as_write(self) -> CogniteTimeSeriesWriteList:
        """Convert these read versions of Cognite time series to the writing versions."""
        return CogniteTimeSeriesWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteTimeSeriesWriteList:
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
    def assets(self) -> CogniteAssetList:
        from ._cognite_asset import CogniteAsset, CogniteAssetList

        return CogniteAssetList(
            [item for items in self.data for item in items.assets or [] if isinstance(item, CogniteAsset)]
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

    @property
    def unit(self) -> CogniteUnitList:
        from ._cognite_unit import CogniteUnit, CogniteUnitList

        return CogniteUnitList([item.unit for item in self.data if isinstance(item.unit, CogniteUnit)])


class CogniteTimeSeriesWriteList(DomainModelWriteList[CogniteTimeSeriesWrite]):
    """List of Cognite time series in the writing version."""

    _INSTANCE = CogniteTimeSeriesWrite

    @property
    def assets(self) -> CogniteAssetWriteList:
        from ._cognite_asset import CogniteAssetWrite, CogniteAssetWriteList

        return CogniteAssetWriteList(
            [item for items in self.data for item in items.assets or [] if isinstance(item, CogniteAssetWrite)]
        )

    @property
    def equipment(self) -> CogniteEquipmentWriteList:
        from ._cognite_equipment import CogniteEquipmentWrite, CogniteEquipmentWriteList

        return CogniteEquipmentWriteList(
            [item for items in self.data for item in items.equipment or [] if isinstance(item, CogniteEquipmentWrite)]
        )

    @property
    def source(self) -> CogniteSourceSystemWriteList:
        from ._cognite_source_system import CogniteSourceSystemWrite, CogniteSourceSystemWriteList

        return CogniteSourceSystemWriteList(
            [item.source for item in self.data if isinstance(item.source, CogniteSourceSystemWrite)]
        )

    @property
    def unit(self) -> CogniteUnitWriteList:
        from ._cognite_unit import CogniteUnitWrite, CogniteUnitWriteList

        return CogniteUnitWriteList([item.unit for item in self.data if isinstance(item.unit, CogniteUnitWrite)])


class CogniteTimeSeriesApplyList(CogniteTimeSeriesWriteList): ...


def _create_cognite_time_series_filter(
    view_id: dm.ViewId,
    assets: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    equipment: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    is_step: bool | None = None,
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
    source_unit: str | list[str] | None = None,
    source_unit_prefix: str | None = None,
    min_source_updated_time: datetime.datetime | None = None,
    max_source_updated_time: datetime.datetime | None = None,
    source_updated_user: str | list[str] | None = None,
    source_updated_user_prefix: str | None = None,
    unit: (
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
    if isinstance(assets, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(assets):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assets"), value=as_instance_dict_id(assets)))
    if assets and isinstance(assets, Sequence) and not isinstance(assets, str) and not is_tuple_id(assets):
        filters.append(
            dm.filters.In(view_id.as_property_ref("assets"), values=[as_instance_dict_id(item) for item in assets])
        )
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(equipment, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(equipment):
        filters.append(dm.filters.Equals(view_id.as_property_ref("equipment"), value=as_instance_dict_id(equipment)))
    if equipment and isinstance(equipment, Sequence) and not isinstance(equipment, str) and not is_tuple_id(equipment):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("equipment"), values=[as_instance_dict_id(item) for item in equipment]
            )
        )
    if isinstance(is_step, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isStep"), value=is_step))
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
    if isinstance(source_unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceUnit"), value=source_unit))
    if source_unit and isinstance(source_unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceUnit"), values=source_unit))
    if source_unit_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("sourceUnit"), value=source_unit_prefix))
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
    if isinstance(unit, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(unit):
        filters.append(dm.filters.Equals(view_id.as_property_ref("unit"), value=as_instance_dict_id(unit)))
    if unit and isinstance(unit, Sequence) and not isinstance(unit, str) and not is_tuple_id(unit):
        filters.append(
            dm.filters.In(view_id.as_property_ref("unit"), values=[as_instance_dict_id(item) for item in unit])
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


class _CogniteTimeSeriesQuery(NodeQueryCore[T_DomainModelList, CogniteTimeSeriesList]):
    _view_id = CogniteTimeSeries._view_id
    _result_cls = CogniteTimeSeries
    _result_list_cls_end = CogniteTimeSeriesList

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
        from ._cognite_activity import _CogniteActivityQuery
        from ._cognite_asset import _CogniteAssetQuery
        from ._cognite_equipment import _CogniteEquipmentQuery
        from ._cognite_source_system import _CogniteSourceSystemQuery
        from ._cognite_unit import _CogniteUnitQuery

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

        if _CogniteActivityQuery not in created_types:
            self.activities = _CogniteActivityQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("cdf_cdm", "CogniteActivity", "v1").as_property_ref("timeSeries"),
                    direction="inwards",
                ),
                connection_name="activities",
                connection_type="reverse-list",
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

        if _CogniteEquipmentQuery not in created_types:
            self.equipment = _CogniteEquipmentQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("equipment"),
                    direction="outwards",
                ),
                connection_name="equipment",
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

        if _CogniteUnitQuery not in created_types:
            self.unit = _CogniteUnitQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("unit"),
                    direction="outwards",
                ),
                connection_name="unit",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.is_step = BooleanFilter(self, self._view_id.as_property_ref("isStep"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.source_context = StringFilter(self, self._view_id.as_property_ref("sourceContext"))
        self.source_created_time = TimestampFilter(self, self._view_id.as_property_ref("sourceCreatedTime"))
        self.source_created_user = StringFilter(self, self._view_id.as_property_ref("sourceCreatedUser"))
        self.source_id = StringFilter(self, self._view_id.as_property_ref("sourceId"))
        self.source_unit = StringFilter(self, self._view_id.as_property_ref("sourceUnit"))
        self.source_updated_time = TimestampFilter(self, self._view_id.as_property_ref("sourceUpdatedTime"))
        self.source_updated_user = StringFilter(self, self._view_id.as_property_ref("sourceUpdatedUser"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.is_step,
                self.name,
                self.source_context,
                self.source_created_time,
                self.source_created_user,
                self.source_id,
                self.source_unit,
                self.source_updated_time,
                self.source_updated_user,
            ]
        )
        self.data = DataPointsAPI(client, lambda limit: self._list(limit=limit).as_node_ids())

    def list_cognite_time_series(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteTimeSeriesList:
        return self._list(limit=limit)


class CogniteTimeSeriesQuery(_CogniteTimeSeriesQuery[CogniteTimeSeriesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteTimeSeriesList)
