from __future__ import annotations

import datetime
from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainRelation, DomainRelationApply, EdgeList

__all__ = ["StartEndTime", "StartEndTimeApply", "StartEndTimeList", "StartEndTimeApplyList", "StartEndTimeFields"]
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

    def as_apply(self) -> StartEndTimeApply:
        """Convert this read version of start end time to a write version."""
        return StartEndTimeApply(
            space=self.space,
            external_id=self.external_id,
            end_time=self.end_time,
            start_time=self.start_time,
        )


class StartEndTimeApply(DomainRelationApply):
    """This represent a write version of start end time.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the start end time.
        end_time: The end time field.
        start_time: The start time field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
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
