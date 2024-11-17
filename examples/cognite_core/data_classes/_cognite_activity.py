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
    TimestampFilter,
)
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite
from cognite_core.data_classes._cognite_sourceable_node import CogniteSourceableNode, CogniteSourceableNodeWrite
from cognite_core.data_classes._cognite_schedulable import CogniteSchedulable, CogniteSchedulableWrite

if TYPE_CHECKING:
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
    from cognite_core.data_classes._cognite_time_series import (
        CogniteTimeSeries,
        CogniteTimeSeriesList,
        CogniteTimeSeriesGraphQL,
        CogniteTimeSeriesWrite,
        CogniteTimeSeriesWriteList,
    )


__all__ = [
    "CogniteActivity",
    "CogniteActivityWrite",
    "CogniteActivityApply",
    "CogniteActivityList",
    "CogniteActivityWriteList",
    "CogniteActivityApplyList",
    "CogniteActivityFields",
    "CogniteActivityTextFields",
    "CogniteActivityGraphQL",
]


CogniteActivityTextFields = Literal[
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
CogniteActivityFields = Literal[
    "external_id",
    "aliases",
    "description",
    "end_time",
    "name",
    "scheduled_end_time",
    "scheduled_start_time",
    "source_context",
    "source_created_time",
    "source_created_user",
    "source_id",
    "source_updated_time",
    "source_updated_user",
    "start_time",
    "tags",
]

_COGNITEACTIVITY_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "end_time": "endTime",
    "name": "name",
    "scheduled_end_time": "scheduledEndTime",
    "scheduled_start_time": "scheduledStartTime",
    "source_context": "sourceContext",
    "source_created_time": "sourceCreatedTime",
    "source_created_user": "sourceCreatedUser",
    "source_id": "sourceId",
    "source_updated_time": "sourceUpdatedTime",
    "source_updated_user": "sourceUpdatedUser",
    "start_time": "startTime",
    "tags": "tags",
}


class CogniteActivityGraphQL(GraphQLCore):
    """This represents the reading version of Cognite activity, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite activity.
        data_record: The data record of the Cognite activity node.
        aliases: Alternative names for the node
        assets: A list of assets the activity is related to.
        description: Description of the instance
        end_time: The actual end time of an activity (or similar that extends this)
        equipment: A list of equipment the activity is related to.
        name: Name of the instance
        scheduled_end_time: The planned end time of an activity (or similar that extends this)
        scheduled_start_time: The planned start time of an activity (or similar that extends this)
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        start_time: The actual start time of an activity (or similar that extends this)
        tags: Text based labels for generic use, limited to 1000
        time_series: A list of time series the activity is related to.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteActivity", "v1")
    aliases: Optional[list[str]] = None
    assets: Optional[list[CogniteAssetGraphQL]] = Field(default=None, repr=False)
    description: Optional[str] = None
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    equipment: Optional[list[CogniteEquipmentGraphQL]] = Field(default=None, repr=False)
    name: Optional[str] = None
    scheduled_end_time: Optional[datetime.datetime] = Field(None, alias="scheduledEndTime")
    scheduled_start_time: Optional[datetime.datetime] = Field(None, alias="scheduledStartTime")
    source: Optional[CogniteSourceSystemGraphQL] = Field(default=None, repr=False)
    source_context: Optional[str] = Field(None, alias="sourceContext")
    source_created_time: Optional[datetime.datetime] = Field(None, alias="sourceCreatedTime")
    source_created_user: Optional[str] = Field(None, alias="sourceCreatedUser")
    source_id: Optional[str] = Field(None, alias="sourceId")
    source_updated_time: Optional[datetime.datetime] = Field(None, alias="sourceUpdatedTime")
    source_updated_user: Optional[str] = Field(None, alias="sourceUpdatedUser")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
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

    @field_validator("assets", "equipment", "source", "time_series", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CogniteActivity:
        """Convert this GraphQL format of Cognite activity to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteActivity(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            aliases=self.aliases,
            assets=[asset.as_read() for asset in self.assets or []],
            description=self.description,
            end_time=self.end_time,
            equipment=[equipment.as_read() for equipment in self.equipment or []],
            name=self.name,
            scheduled_end_time=self.scheduled_end_time,
            scheduled_start_time=self.scheduled_start_time,
            source=self.source.as_read() if isinstance(self.source, GraphQLCore) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            start_time=self.start_time,
            tags=self.tags,
            time_series=[time_series.as_read() for time_series in self.time_series or []],
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteActivityWrite:
        """Convert this GraphQL format of Cognite activity to the writing format."""
        return CogniteActivityWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aliases=self.aliases,
            assets=[asset.as_write() for asset in self.assets or []],
            description=self.description,
            end_time=self.end_time,
            equipment=[equipment.as_write() for equipment in self.equipment or []],
            name=self.name,
            scheduled_end_time=self.scheduled_end_time,
            scheduled_start_time=self.scheduled_start_time,
            source=self.source.as_write() if isinstance(self.source, GraphQLCore) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            start_time=self.start_time,
            tags=self.tags,
            time_series=[time_series.as_write() for time_series in self.time_series or []],
        )


class CogniteActivity(CogniteDescribableNode, CogniteSourceableNode, CogniteSchedulable):
    """This represents the reading version of Cognite activity.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite activity.
        data_record: The data record of the Cognite activity node.
        aliases: Alternative names for the node
        assets: A list of assets the activity is related to.
        description: Description of the instance
        end_time: The actual end time of an activity (or similar that extends this)
        equipment: A list of equipment the activity is related to.
        name: Name of the instance
        scheduled_end_time: The planned end time of an activity (or similar that extends this)
        scheduled_start_time: The planned start time of an activity (or similar that extends this)
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        start_time: The actual start time of an activity (or similar that extends this)
        tags: Text based labels for generic use, limited to 1000
        time_series: A list of time series the activity is related to.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteActivity", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    assets: Optional[list[Union[CogniteAsset, str, dm.NodeId]]] = Field(default=None, repr=False)
    equipment: Optional[list[Union[CogniteEquipment, str, dm.NodeId]]] = Field(default=None, repr=False)
    time_series: Optional[list[Union[CogniteTimeSeries, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="timeSeries"
    )

    def as_write(self) -> CogniteActivityWrite:
        """Convert this read version of Cognite activity to the writing version."""
        return CogniteActivityWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aliases=self.aliases,
            assets=[asset.as_write() if isinstance(asset, DomainModel) else asset for asset in self.assets or []],
            description=self.description,
            end_time=self.end_time,
            equipment=[
                equipment.as_write() if isinstance(equipment, DomainModel) else equipment
                for equipment in self.equipment or []
            ],
            name=self.name,
            scheduled_end_time=self.scheduled_end_time,
            scheduled_start_time=self.scheduled_start_time,
            source=self.source.as_write() if isinstance(self.source, DomainModel) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
            start_time=self.start_time,
            tags=self.tags,
            time_series=[
                time_series.as_write() if isinstance(time_series, DomainModel) else time_series
                for time_series in self.time_series or []
            ],
        )

    def as_apply(self) -> CogniteActivityWrite:
        """Convert this read version of Cognite activity to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, CogniteActivity],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._cognite_asset import CogniteAsset
        from ._cognite_equipment import CogniteEquipment
        from ._cognite_source_system import CogniteSourceSystem
        from ._cognite_time_series import CogniteTimeSeries

        for instance in instances.values():
            if (
                isinstance(instance.source, (dm.NodeId, str))
                and (source := nodes_by_id.get(instance.source))
                and isinstance(source, CogniteSourceSystem)
            ):
                instance.source = source
            if instance.assets:
                new_assets: list[CogniteAsset | str | dm.NodeId] = []
                for relation in instance.assets:
                    if isinstance(relation, CogniteAsset):
                        new_assets.append(relation)
                    elif (other := nodes_by_id.get(relation)) and isinstance(other, CogniteAsset):
                        new_assets.append(other)
                    else:
                        new_assets.append(relation)
                instance.assets = new_assets
            if instance.equipment:
                new_equipment: list[CogniteEquipment | str | dm.NodeId] = []
                for relation in instance.equipment:
                    if isinstance(relation, CogniteEquipment):
                        new_equipment.append(relation)
                    elif (other := nodes_by_id.get(relation)) and isinstance(other, CogniteEquipment):
                        new_equipment.append(other)
                    else:
                        new_equipment.append(relation)
                instance.equipment = new_equipment
            if instance.time_series:
                new_time_series: list[CogniteTimeSeries | str | dm.NodeId] = []
                for relation in instance.time_series:
                    if isinstance(relation, CogniteTimeSeries):
                        new_time_series.append(relation)
                    elif (other := nodes_by_id.get(relation)) and isinstance(other, CogniteTimeSeries):
                        new_time_series.append(other)
                    else:
                        new_time_series.append(relation)
                instance.time_series = new_time_series


class CogniteActivityWrite(CogniteDescribableNodeWrite, CogniteSourceableNodeWrite, CogniteSchedulableWrite):
    """This represents the writing version of Cognite activity.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite activity.
        data_record: The data record of the Cognite activity node.
        aliases: Alternative names for the node
        assets: A list of assets the activity is related to.
        description: Description of the instance
        end_time: The actual end time of an activity (or similar that extends this)
        equipment: A list of equipment the activity is related to.
        name: Name of the instance
        scheduled_end_time: The planned end time of an activity (or similar that extends this)
        scheduled_start_time: The planned start time of an activity (or similar that extends this)
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
        start_time: The actual start time of an activity (or similar that extends this)
        tags: Text based labels for generic use, limited to 1000
        time_series: A list of time series the activity is related to.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteActivity", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    assets: Optional[list[Union[CogniteAssetWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    equipment: Optional[list[Union[CogniteEquipmentWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    time_series: Optional[list[Union[CogniteTimeSeriesWrite, str, dm.NodeId]]] = Field(
        default=None, repr=False, alias="timeSeries"
    )

    @field_validator("assets", "equipment", "time_series", mode="before")
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

        if self.end_time is not None or write_none:
            properties["endTime"] = self.end_time.isoformat(timespec="milliseconds") if self.end_time else None

        if self.equipment is not None:
            properties["equipment"] = [
                {
                    "space": self.space if isinstance(equipment, str) else equipment.space,
                    "externalId": equipment if isinstance(equipment, str) else equipment.external_id,
                }
                for equipment in self.equipment or []
            ]

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.scheduled_end_time is not None or write_none:
            properties["scheduledEndTime"] = (
                self.scheduled_end_time.isoformat(timespec="milliseconds") if self.scheduled_end_time else None
            )

        if self.scheduled_start_time is not None or write_none:
            properties["scheduledStartTime"] = (
                self.scheduled_start_time.isoformat(timespec="milliseconds") if self.scheduled_start_time else None
            )

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

        if self.start_time is not None or write_none:
            properties["startTime"] = self.start_time.isoformat(timespec="milliseconds") if self.start_time else None

        if self.tags is not None or write_none:
            properties["tags"] = self.tags

        if self.time_series is not None:
            properties["timeSeries"] = [
                {
                    "space": self.space if isinstance(time_series, str) else time_series.space,
                    "externalId": time_series if isinstance(time_series, str) else time_series.external_id,
                }
                for time_series in self.time_series or []
            ]

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

        for asset in self.assets or []:
            if isinstance(asset, DomainModelWrite):
                other_resources = asset._to_instances_write(cache)
                resources.extend(other_resources)

        for equipment in self.equipment or []:
            if isinstance(equipment, DomainModelWrite):
                other_resources = equipment._to_instances_write(cache)
                resources.extend(other_resources)

        for time_series in self.time_series or []:
            if isinstance(time_series, DomainModelWrite):
                other_resources = time_series._to_instances_write(cache)
                resources.extend(other_resources)

        return resources


class CogniteActivityApply(CogniteActivityWrite):
    def __new__(cls, *args, **kwargs) -> CogniteActivityApply:
        warnings.warn(
            "CogniteActivityApply is deprecated and will be removed in v1.0. Use CogniteActivityWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteActivity.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteActivityList(DomainModelList[CogniteActivity]):
    """List of Cognite activities in the read version."""

    _INSTANCE = CogniteActivity

    def as_write(self) -> CogniteActivityWriteList:
        """Convert these read versions of Cognite activity to the writing versions."""
        return CogniteActivityWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteActivityWriteList:
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
    def time_series(self) -> CogniteTimeSeriesList:
        from ._cognite_time_series import CogniteTimeSeries, CogniteTimeSeriesList

        return CogniteTimeSeriesList(
            [item for items in self.data for item in items.time_series or [] if isinstance(item, CogniteTimeSeries)]
        )


class CogniteActivityWriteList(DomainModelWriteList[CogniteActivityWrite]):
    """List of Cognite activities in the writing version."""

    _INSTANCE = CogniteActivityWrite

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
    def time_series(self) -> CogniteTimeSeriesWriteList:
        from ._cognite_time_series import CogniteTimeSeriesWrite, CogniteTimeSeriesWriteList

        return CogniteTimeSeriesWriteList(
            [
                item
                for items in self.data
                for item in items.time_series or []
                if isinstance(item, CogniteTimeSeriesWrite)
            ]
        )


class CogniteActivityApplyList(CogniteActivityWriteList): ...


def _create_cognite_activity_filter(
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
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    equipment: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_scheduled_end_time: datetime.datetime | None = None,
    max_scheduled_end_time: datetime.datetime | None = None,
    min_scheduled_start_time: datetime.datetime | None = None,
    max_scheduled_start_time: datetime.datetime | None = None,
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
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    time_series: (
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
    if min_end_time is not None or max_end_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("endTime"),
                gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None,
                lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None,
            )
        )
    if isinstance(equipment, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(equipment):
        filters.append(dm.filters.Equals(view_id.as_property_ref("equipment"), value=as_instance_dict_id(equipment)))
    if equipment and isinstance(equipment, Sequence) and not isinstance(equipment, str) and not is_tuple_id(equipment):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("equipment"), values=[as_instance_dict_id(item) for item in equipment]
            )
        )
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if min_scheduled_end_time is not None or max_scheduled_end_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("scheduledEndTime"),
                gte=min_scheduled_end_time.isoformat(timespec="milliseconds") if min_scheduled_end_time else None,
                lte=max_scheduled_end_time.isoformat(timespec="milliseconds") if max_scheduled_end_time else None,
            )
        )
    if min_scheduled_start_time is not None or max_scheduled_start_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("scheduledStartTime"),
                gte=min_scheduled_start_time.isoformat(timespec="milliseconds") if min_scheduled_start_time else None,
                lte=max_scheduled_start_time.isoformat(timespec="milliseconds") if max_scheduled_start_time else None,
            )
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
    if min_start_time is not None or max_start_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startTime"),
                gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None,
                lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None,
            )
        )
    if isinstance(time_series, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(time_series):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timeSeries"), value=as_instance_dict_id(time_series)))
    if (
        time_series
        and isinstance(time_series, Sequence)
        and not isinstance(time_series, str)
        and not is_tuple_id(time_series)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("timeSeries"), values=[as_instance_dict_id(item) for item in time_series]
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


class _CogniteActivityQuery(NodeQueryCore[T_DomainModelList, CogniteActivityList]):
    _view_id = CogniteActivity._view_id
    _result_cls = CogniteActivity
    _result_list_cls_end = CogniteActivityList

    def __init__(
        self,
        created_triples: set[type],
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
        from ._cognite_source_system import _CogniteSourceSystemQuery
        from ._cognite_time_series import _CogniteTimeSeriesQuery

        super().__init__(
            created_triples,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_type,
            reverse_expression,
        )

        if _CogniteAssetQuery not in created_triples:
            self.assets = _CogniteAssetQuery(
                created_triples.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("assets"),
                    direction="outwards",
                ),
                connection_name="assets",
            )

        if _CogniteEquipmentQuery not in created_triples:
            self.equipment = _CogniteEquipmentQuery(
                created_triples.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("equipment"),
                    direction="outwards",
                ),
                connection_name="equipment",
            )

        if _CogniteSourceSystemQuery not in created_triples:
            self.source = _CogniteSourceSystemQuery(
                created_triples.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("source"),
                    direction="outwards",
                ),
                connection_name="source",
            )

        if _CogniteTimeSeriesQuery not in created_triples:
            self.time_series = _CogniteTimeSeriesQuery(
                created_triples.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("timeSeries"),
                    direction="outwards",
                ),
                connection_name="time_series",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.end_time = TimestampFilter(self, self._view_id.as_property_ref("endTime"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.scheduled_end_time = TimestampFilter(self, self._view_id.as_property_ref("scheduledEndTime"))
        self.scheduled_start_time = TimestampFilter(self, self._view_id.as_property_ref("scheduledStartTime"))
        self.source_context = StringFilter(self, self._view_id.as_property_ref("sourceContext"))
        self.source_created_time = TimestampFilter(self, self._view_id.as_property_ref("sourceCreatedTime"))
        self.source_created_user = StringFilter(self, self._view_id.as_property_ref("sourceCreatedUser"))
        self.source_id = StringFilter(self, self._view_id.as_property_ref("sourceId"))
        self.source_updated_time = TimestampFilter(self, self._view_id.as_property_ref("sourceUpdatedTime"))
        self.source_updated_user = StringFilter(self, self._view_id.as_property_ref("sourceUpdatedUser"))
        self.start_time = TimestampFilter(self, self._view_id.as_property_ref("startTime"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.end_time,
                self.name,
                self.scheduled_end_time,
                self.scheduled_start_time,
                self.source_context,
                self.source_created_time,
                self.source_created_user,
                self.source_id,
                self.source_updated_time,
                self.source_updated_user,
                self.start_time,
            ]
        )

    def list_cognite_activity(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteActivityList:
        return self._list(limit=limit)


class CogniteActivityQuery(_CogniteActivityQuery[CogniteActivityList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteActivityList)
