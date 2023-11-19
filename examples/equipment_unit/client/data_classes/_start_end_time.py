from __future__ import annotations

import datetime
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling import DirectRelationReference
from pydantic import Field, model_validator

from . import DomainModelApply
from ._core import DomainRelation, DomainRelationApply, DomainRelationList, ResourcesApply
from ._equipment_module import EquipmentModule, EquipmentModuleApply

__all__ = ["StartEndTime", "StartEndTimeList", "StartEndTimeFields"]
StartEndTimeFields = Literal["end_time", "start_time"]

_STARTENDTIME_PROPERTIES_BY_FIELD = {
    "end_time": "end_time",
    "start_time": "start_time",
}


class StartEndTime(DomainRelation):
    """This represents the read version of start end time.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the start end time.
        end_time: The end time field.
        start_time: The start time field.
        equipment_module: The equipment module field.
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
            end_time=self.end_time,
            start_time=self.start_time,
            equipment_module=self.equipment_module.as_apply()
            if isinstance(self.equipment_module, EquipmentModule)
            else self.equipment_module,
        )


class StartEndTimeApply(DomainRelationApply):
    type: ClassVar[DirectRelationReference] = DirectRelationReference(
        "IntegrationTestsImmutable", "UnitProcedure.equipment_module"
    )
    space: str = "IntegrationTestsImmutable"
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    equipment_module: Union[EquipmentModuleApply, str] = Field(alias="equipmentModule")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        start_node: dm.DirectRelationReference,
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        if self.external_id and (self.space, self.external_id) in cache:
            return ResourcesApply()

        if isinstance(self.equipment_module, DomainModelApply):
            end_node = dm.DirectRelationReference(self.equipment_module.space, self.equipment_module.external_id)
        elif isinstance(self.equipment_module, str):
            end_node = dm.DirectRelationReference(self.space, self.equipment_module)
        else:
            raise ValueError(f"Invalid type for equipment_module: {type(self.equipment_module)}")
        self.external_id = external_id = type(self).external_id_factory(type(self), start_node, end_node)

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "StartEndTime", "d416e0ed98186b"
        )

        this_instances = ResourcesApply()

        properties = {}
        if self.end_time is not None:
            properties["end_time"] = self.end_time.isoformat(timespec="milliseconds")
        if self.start_time is not None:
            properties["start_time"] = self.start_time.isoformat(timespec="milliseconds")

        if properties:
            this_edge = dm.EdgeApply(
                external_id=external_id,
                space=self.space,
                type=self.type,
                start_node=start_node,
                end_node=end_node,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            this_instances.edges.append(this_edge)

        if isinstance(self.equipment_module, DomainModelApply):
            instances = self.equipment_module._to_instances_apply(cache, view_by_write_class)
            this_instances.extend(instances)

        return this_instances


class StartEndTimeList(DomainRelationList[StartEndTime]):
    """List of start end time in read version."""

    _INSTANCE = StartEndTime
