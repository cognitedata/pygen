from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

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
    TimestampFilter,
)


__all__ = [
    "CogniteSchedulable",
    "CogniteSchedulableWrite",
    "CogniteSchedulableApply",
    "CogniteSchedulableList",
    "CogniteSchedulableWriteList",
    "CogniteSchedulableApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CogniteSchedulable:
        """Convert this GraphQL format of Cognite schedulable to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteSchedulable(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            end_time=self.end_time,
            scheduled_end_time=self.scheduled_end_time,
            scheduled_start_time=self.scheduled_start_time,
            start_time=self.start_time,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteSchedulableWrite:
        """Convert this GraphQL format of Cognite schedulable to the writing format."""
        return CogniteSchedulableWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            end_time=self.end_time,
            scheduled_end_time=self.scheduled_end_time,
            scheduled_start_time=self.scheduled_start_time,
            start_time=self.start_time,
        )


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

    def as_apply(self) -> CogniteSchedulableWrite:
        """Convert this read version of Cognite schedulable to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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

        if self.end_time is not None or write_none:
            properties["endTime"] = self.end_time.isoformat(timespec="milliseconds") if self.end_time else None

        if self.scheduled_end_time is not None or write_none:
            properties["scheduledEndTime"] = (
                self.scheduled_end_time.isoformat(timespec="milliseconds") if self.scheduled_end_time else None
            )

        if self.scheduled_start_time is not None or write_none:
            properties["scheduledStartTime"] = (
                self.scheduled_start_time.isoformat(timespec="milliseconds") if self.scheduled_start_time else None
            )

        if self.start_time is not None or write_none:
            properties["startTime"] = self.start_time.isoformat(timespec="milliseconds") if self.start_time else None

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

        return resources


class CogniteSchedulableApply(CogniteSchedulableWrite):
    def __new__(cls, *args, **kwargs) -> CogniteSchedulableApply:
        warnings.warn(
            "CogniteSchedulableApply is deprecated and will be removed in v1.0. "
            "Use CogniteSchedulableWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteSchedulable.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteSchedulableList(DomainModelList[CogniteSchedulable]):
    """List of Cognite schedulables in the read version."""

    _INSTANCE = CogniteSchedulable

    def as_write(self) -> CogniteSchedulableWriteList:
        """Convert these read versions of Cognite schedulable to the writing versions."""
        return CogniteSchedulableWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteSchedulableWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CogniteSchedulableWriteList(DomainModelWriteList[CogniteSchedulableWrite]):
    """List of Cognite schedulables in the writing version."""

    _INSTANCE = CogniteSchedulableWrite


class CogniteSchedulableApplyList(CogniteSchedulableWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
