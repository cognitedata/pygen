from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

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
    TimestampFilter,
)


__all__ = [
    "CogniteSchedulable",
    "CogniteSchedulableWrite",
    "CogniteSchedulableList",
    "CogniteSchedulableWriteList",
    "CogniteSchedulableFields",
    "CogniteSchedulableGraphQL",
]


CogniteSchedulableTextFields = Literal["external_id",]
CogniteSchedulableFields = Literal[
    "external_id", "end_time", "scheduled_end_time", "scheduled_start_time", "start_time"
]

_COGNITESCHEDULABLE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "end_time": "endTime",
    "scheduled_end_time": "scheduledEndTime",
    "scheduled_start_time": "scheduledStartTime",
    "start_time": "startTime",
}


class CogniteSchedulableGraphQL(GraphQLCore):
    """This represents the reading version of Cognite schedulable, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite schedulable.
        data_record: The data record of the Cognite schedulable node.
        end_time: The actual end time of an activity (or similar that extends this)
        scheduled_end_time: The planned end time of an activity (or similar that extends this)
        scheduled_start_time: The planned start time of an activity (or similar that extends this)
        start_time: The actual start time of an activity (or similar that extends this)
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteSchedulable", "v1")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    scheduled_end_time: Optional[datetime.datetime] = Field(None, alias="scheduledEndTime")
    scheduled_start_time: Optional[datetime.datetime] = Field(None, alias="scheduledStartTime")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")

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

    def as_read(self) -> CogniteSchedulable:
        """Convert this GraphQL format of Cognite schedulable to the reading format."""
        return CogniteSchedulable.model_validate(as_read_args(self))

    def as_write(self) -> CogniteSchedulableWrite:
        """Convert this GraphQL format of Cognite schedulable to the writing format."""
        return CogniteSchedulableWrite.model_validate(as_write_args(self))


class CogniteSchedulable(DomainModel):
    """This represents the reading version of Cognite schedulable.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite schedulable.
        data_record: The data record of the Cognite schedulable node.
        end_time: The actual end time of an activity (or similar that extends this)
        scheduled_end_time: The planned end time of an activity (or similar that extends this)
        scheduled_start_time: The planned start time of an activity (or similar that extends this)
        start_time: The actual start time of an activity (or similar that extends this)
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteSchedulable", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    scheduled_end_time: Optional[datetime.datetime] = Field(None, alias="scheduledEndTime")
    scheduled_start_time: Optional[datetime.datetime] = Field(None, alias="scheduledStartTime")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")

    def as_write(self) -> CogniteSchedulableWrite:
        """Convert this read version of Cognite schedulable to the writing version."""
        return CogniteSchedulableWrite.model_validate(as_write_args(self))


class CogniteSchedulableWrite(DomainModelWrite):
    """This represents the writing version of Cognite schedulable.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite schedulable.
        data_record: The data record of the Cognite schedulable node.
        end_time: The actual end time of an activity (or similar that extends this)
        scheduled_end_time: The planned end time of an activity (or similar that extends this)
        scheduled_start_time: The planned start time of an activity (or similar that extends this)
        start_time: The actual start time of an activity (or similar that extends this)
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "end_time",
        "scheduled_end_time",
        "scheduled_start_time",
        "start_time",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteSchedulable", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    scheduled_end_time: Optional[datetime.datetime] = Field(None, alias="scheduledEndTime")
    scheduled_start_time: Optional[datetime.datetime] = Field(None, alias="scheduledStartTime")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")


class CogniteSchedulableList(DomainModelList[CogniteSchedulable]):
    """List of Cognite schedulables in the read version."""

    _INSTANCE = CogniteSchedulable

    def as_write(self) -> CogniteSchedulableWriteList:
        """Convert these read versions of Cognite schedulable to the writing versions."""
        return CogniteSchedulableWriteList([node.as_write() for node in self.data])


class CogniteSchedulableWriteList(DomainModelWriteList[CogniteSchedulableWrite]):
    """List of Cognite schedulables in the writing version."""

    _INSTANCE = CogniteSchedulableWrite


def _create_cognite_schedulable_filter(
    view_id: dm.ViewId,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    min_scheduled_end_time: datetime.datetime | None = None,
    max_scheduled_end_time: datetime.datetime | None = None,
    min_scheduled_start_time: datetime.datetime | None = None,
    max_scheduled_start_time: datetime.datetime | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if min_end_time is not None or max_end_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("endTime"),
                gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None,
                lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None,
            )
        )
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
    if min_start_time is not None or max_start_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startTime"),
                gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None,
                lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None,
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


class _CogniteSchedulableQuery(NodeQueryCore[T_DomainModelList, CogniteSchedulableList]):
    _view_id = CogniteSchedulable._view_id
    _result_cls = CogniteSchedulable
    _result_list_cls_end = CogniteSchedulableList

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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.end_time = TimestampFilter(self, self._view_id.as_property_ref("endTime"))
        self.scheduled_end_time = TimestampFilter(self, self._view_id.as_property_ref("scheduledEndTime"))
        self.scheduled_start_time = TimestampFilter(self, self._view_id.as_property_ref("scheduledStartTime"))
        self.start_time = TimestampFilter(self, self._view_id.as_property_ref("startTime"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.end_time,
                self.scheduled_end_time,
                self.scheduled_start_time,
                self.start_time,
            ]
        )

    def list_cognite_schedulable(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteSchedulableList:
        return self._list(limit=limit)


class CogniteSchedulableQuery(_CogniteSchedulableQuery[CogniteSchedulableList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteSchedulableList)
