from __future__ import annotations

import datetime
from typing import Literal, Optional, Union

from pydantic import Field
from ._core import DomainRelation, EdgeList, DomainRelationApply

from ._equipment_module import EquipmentModuleApply

__all__ = ["StartEndTime", "StartEndTimeList", "StartEndTimeFields"]
StartEndTimeFields = Literal["end_time", "start_time"]

_STARTENDTIME_PROPERTIES_BY_FIELD = {
    "end_time": "end_time",
    "start_time": "start_time",
}


class StartEndTime(DomainRelation):
    """This represent a read version of start end time.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the start end time.
        end_time: The end time field.
        start_time: The start time field.
        created_time: The created time of the start end time node.
        last_updated_time: The last updated time of the start end time node.
        deleted_time: If present, the deleted time of the start end time node.
        version: The version of the start end time node.
    """

    space: str = "IntegrationTestsImmutable"
    end_time: Optional[datetime.datetime] = None
    start_time: Optional[datetime.datetime] = None

    @property
    def unit_procedure(self) -> str:
        return self.start_node.external_id

    @property
    def equipment_module(self) -> str:
        return self.end_node.external_id


class StartEndTimeApply(DomainRelationApply):
    space: str = "IntegrationTestsImmutable"
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    equipment_module: Optional[Union[EquipmentModuleApply, str]] = Field(None, alias="equipmentModule")

    # @property
    # def unit_procedure(self) -> str:
    #     return self.start_node.external_id


class StartEndTimeList(EdgeList[StartEndTime]):
    """List of start end time in read version."""

    _INSTANCE = StartEndTime
