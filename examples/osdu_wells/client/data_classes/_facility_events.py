from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "FacilityEvents",
    "FacilityEventsApply",
    "FacilityEventsList",
    "FacilityEventsApplyList",
    "FacilityEventsFields",
    "FacilityEventsTextFields",
]


FacilityEventsTextFields = Literal["effective_date_time", "facility_event_type_id", "remark", "termination_date_time"]
FacilityEventsFields = Literal["effective_date_time", "facility_event_type_id", "remark", "termination_date_time"]

_FACILITYEVENTS_PROPERTIES_BY_FIELD = {
    "effective_date_time": "EffectiveDateTime",
    "facility_event_type_id": "FacilityEventTypeID",
    "remark": "Remark",
    "termination_date_time": "TerminationDateTime",
}


class FacilityEvents(DomainModel):
    """This represent a read version of facility event.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the facility event.
        effective_date_time: The effective date time field.
        facility_event_type_id: The facility event type id field.
        remark: The remark field.
        termination_date_time: The termination date time field.
        created_time: The created time of the facility event node.
        last_updated_time: The last updated time of the facility event node.
        deleted_time: If present, the deleted time of the facility event node.
        version: The version of the facility event node.
    """

    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_event_type_id: Optional[str] = Field(None, alias="FacilityEventTypeID")
    remark: Optional[str] = Field(None, alias="Remark")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def as_apply(self) -> FacilityEventsApply:
        """Convert this read version of facility event to a write version."""
        return FacilityEventsApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            facility_event_type_id=self.facility_event_type_id,
            remark=self.remark,
            termination_date_time=self.termination_date_time,
        )


class FacilityEventsApply(DomainModelApply):
    """This represent a write version of facility event.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the facility event.
        effective_date_time: The effective date time field.
        facility_event_type_id: The facility event type id field.
        remark: The remark field.
        termination_date_time: The termination date time field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_event_type_id: Optional[str] = Field(None, alias="FacilityEventTypeID")
    remark: Optional[str] = Field(None, alias="Remark")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.facility_event_type_id is not None:
            properties["FacilityEventTypeID"] = self.facility_event_type_id
        if self.remark is not None:
            properties["Remark"] = self.remark
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "FacilityEvents", "1b7526673ad990"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class FacilityEventsList(TypeList[FacilityEvents]):
    """List of facility events in read version."""

    _NODE = FacilityEvents

    def as_apply(self) -> FacilityEventsApplyList:
        """Convert this read version of facility event to a write version."""
        return FacilityEventsApplyList([node.as_apply() for node in self.data])


class FacilityEventsApplyList(TypeApplyList[FacilityEventsApply]):
    """List of facility events in write version."""

    _NODE = FacilityEventsApply
