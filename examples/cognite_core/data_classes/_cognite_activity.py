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
    "CogniteActivityList",
    "CogniteActivityWriteList",
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
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is
            not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This
            identifier is not guaranteed to match the user identifiers in CDF
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

    def as_read(self) -> CogniteActivity:
        """Convert this GraphQL format of Cognite activity to the reading format."""
        return CogniteActivity.model_validate(as_read_args(self))

    def as_write(self) -> CogniteActivityWrite:
        """Convert this GraphQL format of Cognite activity to the writing format."""
        return CogniteActivityWrite.model_validate(as_write_args(self))


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
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is
            not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This
            identifier is not guaranteed to match the user identifiers in CDF
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

    @field_validator("source", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("assets", "equipment", "time_series", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> CogniteActivityWrite:
        """Convert this read version of Cognite activity to the writing version."""
        return CogniteActivityWrite.model_validate(as_write_args(self))


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
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is
            not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This
            identifier is not guaranteed to match the user identifiers in CDF
        start_time: The actual start time of an activity (or similar that extends this)
        tags: Text based labels for generic use, limited to 1000
        time_series: A list of time series the activity is related to.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "assets",
        "description",
        "end_time",
        "equipment",
        "name",
        "scheduled_end_time",
        "scheduled_start_time",
        "source",
        "source_context",
        "source_created_time",
        "source_created_user",
        "source_id",
        "source_updated_time",
        "source_updated_user",
        "start_time",
        "tags",
        "time_series",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "assets",
        "equipment",
        "source",
        "time_series",
    )

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


class CogniteActivityList(DomainModelList[CogniteActivity]):
    """List of Cognite activities in the read version."""

    _INSTANCE = CogniteActivity

    def as_write(self) -> CogniteActivityWriteList:
        """Convert these read versions of Cognite activity to the writing versions."""
        return CogniteActivityWriteList([node.as_write() for node in self.data])

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
        from ._cognite_asset import _CogniteAssetQuery
        from ._cognite_equipment import _CogniteEquipmentQuery
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

        if _CogniteAssetQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "assets"),
            )

        if _CogniteEquipmentQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "equipment"),
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
                    through=self._view_id.as_property_ref("timeSeries"),
                    direction="outwards",
                ),
                connection_name="time_series",
                connection_property=ViewPropertyId(self._view_id, "timeSeries"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.end_time = TimestampFilter(self, self._view_id.as_property_ref("endTime"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.scheduled_end_time = TimestampFilter(self, self._view_id.as_property_ref("scheduledEndTime"))
        self.scheduled_start_time = TimestampFilter(self, self._view_id.as_property_ref("scheduledStartTime"))
        self.source_filter = DirectRelationFilter(self, self._view_id.as_property_ref("source"))
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
                self.source_filter,
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
