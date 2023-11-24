from __future__ import annotations

import datetime
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field, model_validator

from ._core import DomainModelApply, DomainRelation, DomainRelationApply, DomainRelationList, ResourcesApply
from ._equipment_module import EquipmentModule, EquipmentModuleApply

__all__ = ["StartEndTime", "StartEndTimeApply", "StartEndTimeList", "StartEndTimeFields"]
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

    @model_validator(mode="before")
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
