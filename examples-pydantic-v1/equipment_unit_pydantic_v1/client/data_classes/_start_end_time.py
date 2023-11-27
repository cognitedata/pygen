from __future__ import annotations

import datetime
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field, root_validator

from ._core import DomainModelApply, DomainRelation, DomainRelationApply, DomainRelationList, ResourcesApply
from ._equipment_module import EquipmentModule, EquipmentModuleApply

__all__ = ["StartEndTime", "StartEndTimeApply", "StartEndTimeList", "StartEndTimeApplyList", "StartEndTimeFields"]
StartEndTimeFields = Literal["end_time", "start_time"]

_STARTENDTIME_PROPERTIES_BY_FIELD = {
    "end_time": "end_time",
    "start_time": "start_time",
}


class StartEndTime(DomainRelation):
    """This represents the reading version of start end time.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the start end time.
        equipment_module: The equipment module field.
        end_time: The end time field.
        start_time: The start time field.
        created_time: The created time of the start end time node.
        last_updated_time: The last updated time of the start end time node.
        deleted_time: If present, the deleted time of the start end time node.
        version: The version of the start end time node.
    """

    space: str = "IntegrationTestsImmutable"
    equipment_module: Union[EquipmentModule, str]
    end_time: Optional[datetime.datetime] = None
    start_time: Optional[datetime.datetime] = None

    @property
    def unit_procedure(self) -> str:
        return self.start_node.external_id

    @root_validator(pre=True)
    def set_equipment_module_if_missing(cls, data: Any):
        if isinstance(data, dict) and "equipment_module" not in data:
            data["equipment_module"] = data["end_node"]["external_id"]
        return data

    def as_apply(self) -> StartEndTimeApply:
        """Convert this read version of start end time to the writing version."""
        return StartEndTimeApply(
            space=self.space,
            external_id=self.external_id,
            equipment_module=self.equipment_module.as_apply()
            if isinstance(self.equipment_module, EquipmentModule)
            else self.equipment_module,
            end_time=self.end_time,
            start_time=self.start_time,
        )


class StartEndTimeApply(DomainRelationApply):
    """This represents the writing version of start end time.

    It is used to when data is sent to CDF.

    Args:
        edge_type: The edge type of the start end time.
        space: The space where the node is located.
        external_id: The external id of the start end time.
        equipment_module: The equipment module field.
        end_time: The end time field.
        start_time: The start time field.
        existing_version: Fail the ingestion request if the start end time version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    edge_type: dm.DirectRelationReference = dm.DirectRelationReference(
        "IntegrationTestsImmutable", "UnitProcedure.equipment_module"
    )
    space: str = "IntegrationTestsImmutable"
    equipment_module: Union[EquipmentModuleApply, str]
    end_time: Optional[datetime.datetime] = None
    start_time: Optional[datetime.datetime] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        start_node: DomainModelApply,
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.external_id and (self.space, self.external_id) in cache:
            return resources

        if isinstance(self.equipment_module, DomainModelApply):
            end_node = self.equipment_module.as_direct_reference()
        elif isinstance(self.equipment_module, str):
            end_node = dm.DirectRelationReference(self.space, self.equipment_module)
        else:
            raise ValueError(f"Invalid type for equipment_module: {type(self.equipment_module)}")

        self.external_id = external_id = DomainRelationApply.external_id_factory(start_node, end_node, self.edge_type)

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "StartEndTime", "d416e0ed98186b"
        )

        properties = {}
        if self.end_time is not None:
            properties["end_time"] = self.end_time.isoformat(timespec="milliseconds")
        if self.start_time is not None:
            properties["start_time"] = self.start_time.isoformat(timespec="milliseconds")

        if properties:
            this_edge = dm.EdgeApply(
                space=self.space,
                external_id=external_id,
                type=self.edge_type,
                start_node=start_node.as_direct_reference(),
                end_node=end_node,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.edges.append(this_edge)
            cache.add((self.space, external_id))

        if isinstance(self.equipment_module, DomainModelApply):
            other_resources = self.equipment_module._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class StartEndTimeList(DomainRelationList[StartEndTime]):
    """List of start end times in the reading version."""

    _INSTANCE = StartEndTime


class StartEndTimeApplyList(DomainRelationList[StartEndTimeApply]):
    """List of start end times in the writing version."""

    _INSTANCE = StartEndTimeApply


def _create_start_end_time_filter(
    edge_type: dm.DirectRelationReference,
    view_id: dm.ViewId,
    start_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    start_node_space: str = "IntegrationTestsImmutable",
    end_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    space_end_node: str = "IntegrationTestsImmutable",
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
                    {"space": start_node_space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
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
                    {"space": space_end_node, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in end_node
                ],
            )
        )
    if min_end_time or max_end_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("end_time"),
                gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None,
                lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None,
            )
        )
    if min_start_time or max_start_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("start_time"),
                gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None,
                lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["edge", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters)
