from __future__ import annotations

import datetime
import warnings
from typing import Literal, Optional, Union

from cognite.client import data_modeling as dm

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainRelation,
    DomainRelationWrite,
    DomainRelationList,
    GraphQLCore,
    ResourcesWrite,
)
from ._unit_procedure import UnitProcedureWrite
from ._equipment_module import EquipmentModule, EquipmentModuleGraphQL, EquipmentModuleWrite
from ._work_order import WorkOrder, WorkOrderGraphQL, WorkOrderWrite

__all__ = [
    "StartEndTime",
    "StartEndTimeWrite",
    "StartEndTimeApply",
    "StartEndTimeList",
    "StartEndTimeWriteList",
    "StartEndTimeApplyList",
    "StartEndTimeFields",
]


StartEndTimeFields = Literal["end_time", "start_time"]
_STARTENDTIME_PROPERTIES_BY_FIELD = {
    "end_time": "end_time",
    "start_time": "start_time",
}


class StartEndTimeGraphQL(GraphQLCore):
    """This represents the reading version of start end time, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the start end time.
        data_record: The data record of the start end time node.
        end_node: The end node of this edge.
        end_time: The end time field.
        start_time: The start time field.
    """

    view_id = dm.ViewId("IntegrationTestsImmutable", "StartEndTime", "d416e0ed98186b")
    end_node: Union[EquipmentModuleGraphQL, WorkOrderGraphQL, None] = None
    end_time: Optional[datetime.datetime] = None
    start_time: Optional[datetime.datetime] = None

    def as_read(self) -> StartEndTime:
        """Convert this GraphQL format of start end time to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return StartEndTime(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            end_node=self.end_node.as_read() if isinstance(self.end_node, GraphQLCore) else self.end_node,
            end_time=self.end_time,
            start_time=self.start_time,
        )

    def as_write(self) -> StartEndTimeWrite:
        """Convert this GraphQL format of start end time to the writing format."""
        return StartEndTimeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            end_node=self.end_node.as_write() if isinstance(self.end_node, DomainModel) else self.end_node,
            end_time=self.end_time,
            start_time=self.start_time,
        )


class StartEndTime(DomainRelation):
    """This represents the reading version of start end time.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the start end time.
        data_record: The data record of the start end time edge.
        end_node: The end node of this edge.
        end_time: The end time field.
        start_time: The start time field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    end_node: Union[EquipmentModule, WorkOrder, str, dm.NodeId]
    end_time: Optional[datetime.datetime] = None
    start_time: Optional[datetime.datetime] = None

    def as_write(self) -> StartEndTimeWrite:
        """Convert this read version of start end time to the writing version."""
        return StartEndTimeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            end_node=self.end_node.as_write() if isinstance(self.end_node, DomainModel) else self.end_node,
            end_time=self.end_time,
            start_time=self.start_time,
        )

    def as_apply(self) -> StartEndTimeWrite:
        """Convert this read version of start end time to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class StartEndTimeWrite(DomainRelationWrite):
    """This represents the writing version of start end time.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the start end time.
        data_record: The data record of the start end time edge.
        end_node: The end node of this edge.
        end_time: The end time field.
        start_time: The start time field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    end_node: Union[EquipmentModuleWrite, WorkOrderWrite, str, dm.NodeId]
    end_time: Optional[datetime.datetime] = None
    start_time: Optional[datetime.datetime] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        start_node: DomainModelWrite,
        edge_type: dm.DirectRelationReference,
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.external_id and (self.space, self.external_id) in cache:
            return resources

        _validate_end_node(start_node, self.end_node)

        if isinstance(self.end_node, DomainModelWrite):
            end_node = self.end_node.as_direct_reference()
        elif isinstance(self.end_node, str):
            end_node = dm.DirectRelationReference(self.space, self.end_node)
        elif isinstance(self.end_node, dm.NodeId):
            end_node = dm.DirectRelationReference(self.end_node.space, self.end_node.external_id)
        else:
            raise ValueError(f"Invalid type for equipment_module: {type(self.end_node)}")

        external_id = self.external_id or DomainRelationWrite.external_id_factory(start_node, self.end_node, edge_type)

        write_view = (view_by_read_class or {}).get(
            StartEndTime, dm.ViewId("IntegrationTestsImmutable", "StartEndTime", "d416e0ed98186b")
        )

        properties = {}

        if self.end_time is not None or write_none:
            properties["end_time"] = self.end_time.isoformat(timespec="milliseconds") if self.end_time else None

        if self.start_time is not None or write_none:
            properties["start_time"] = self.start_time.isoformat(timespec="milliseconds") if self.start_time else None

        if properties:
            this_edge = dm.EdgeApply(
                space=self.space,
                external_id=external_id,
                type=edge_type,
                start_node=start_node.as_direct_reference(),
                end_node=end_node,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.edges.append(this_edge)
            cache.add((self.space, external_id))

        if isinstance(self.end_node, DomainModelWrite):
            other_resources = self.end_node._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class StartEndTimeApply(StartEndTimeWrite):
    def __new__(cls, *args, **kwargs) -> StartEndTimeApply:
        warnings.warn(
            "StartEndTimeApply is deprecated and will be removed in v1.0. Use StartEndTimeWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "StartEndTime.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class StartEndTimeList(DomainRelationList[StartEndTime]):
    """List of start end times in the reading version."""

    _INSTANCE = StartEndTime

    def as_write(self) -> StartEndTimeWriteList:
        """Convert this read version of start end time list to the writing version."""
        return StartEndTimeWriteList([edge.as_write() for edge in self])

    def as_apply(self) -> StartEndTimeWriteList:
        """Convert these read versions of start end time list to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class StartEndTimeWriteList(DomainRelationList[StartEndTimeWrite]):
    """List of start end times in the writing version."""

    _INSTANCE = StartEndTimeWrite


class StartEndTimeApplyList(StartEndTimeWriteList): ...


def _create_start_end_time_filter(
    edge_type: dm.DirectRelationReference,
    view_id: dm.ViewId,
    start_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    start_node_space: str = DEFAULT_INSTANCE_SPACE,
    end_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    space_end_node: str = DEFAULT_INSTANCE_SPACE,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter:
    filters: list[dm.Filter] = [
        dm.filters.Equals(
            ["edge", "type"],
            {"space": edge_type.space, "externalId": edge_type.external_id},
        )
    ]
    if start_node and isinstance(start_node, str):
        filters.append(
            dm.filters.Equals(["edge", "startNode"], value={"space": start_node_space, "externalId": start_node})
        )
    elif start_node and isinstance(start_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(
                ["edge", "startNode"], value=start_node.dump(camel_case=True, include_instance_type=False)
            )
        )
    if start_node and isinstance(start_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "startNode"],
                values=[
                    (
                        {"space": start_node_space, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in start_node
                ],
            )
        )
    if end_node and isinstance(end_node, str):
        filters.append(dm.filters.Equals(["edge", "endNode"], value={"space": space_end_node, "externalId": end_node}))
    elif end_node and isinstance(end_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(["edge", "endNode"], value=end_node.dump(camel_case=True, include_instance_type=False))
        )
    if end_node and isinstance(end_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "endNode"],
                values=[
                    (
                        {"space": space_end_node, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in end_node
                ],
            )
        )
    if min_end_time is not None or max_end_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("end_time"),
                gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None,
                lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None,
            )
        )
    if min_start_time is not None or max_start_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("start_time"),
                gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None,
                lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None,
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["edge", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters)


_EXPECTED_START_NODES_BY_END_NODE = {
    EquipmentModuleWrite: {UnitProcedureWrite},
    WorkOrderWrite: {UnitProcedureWrite},
}


def _validate_end_node(
    start_node: DomainModelWrite, end_node: Union[EquipmentModuleWrite, WorkOrderWrite, str, dm.NodeId]
) -> None:
    if isinstance(end_node, (str, dm.NodeId)):
        # Nothing to validate
        return
    if type(end_node) not in _EXPECTED_START_NODES_BY_END_NODE:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. Should be one of {[t.__name__ for t in _EXPECTED_START_NODES_BY_END_NODE.keys()]}"
        )
    if type(start_node) not in _EXPECTED_START_NODES_BY_END_NODE[type(end_node)]:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. Expected one of: {_EXPECTED_START_NODES_BY_END_NODE[type(end_node)]}"
        )
