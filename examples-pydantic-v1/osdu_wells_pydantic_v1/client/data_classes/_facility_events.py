from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


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
    """This represents the reading version of facility event.

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

    space: str = DEFAULT_INSTANCE_SPACE
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_event_type_id: Optional[str] = Field(None, alias="FacilityEventTypeID")
    remark: Optional[str] = Field(None, alias="Remark")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def as_apply(self) -> FacilityEventsApply:
        """Convert this read version of facility event to the writing version."""
        return FacilityEventsApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            facility_event_type_id=self.facility_event_type_id,
            remark=self.remark,
            termination_date_time=self.termination_date_time,
        )


class FacilityEventsApply(DomainModelApply):
    """This represents the writing version of facility event.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the facility event.
        effective_date_time: The effective date time field.
        facility_event_type_id: The facility event type id field.
        remark: The remark field.
        termination_date_time: The termination date time field.
        existing_version: Fail the ingestion request if the facility event version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_event_type_id: Optional[str] = Field(None, alias="FacilityEventTypeID")
    remark: Optional[str] = Field(None, alias="Remark")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "FacilityEvents", "1b7526673ad990"
        )

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
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("IntegrationTestsImmutable", "FacilityEvents"),
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class FacilityEventsList(DomainModelList[FacilityEvents]):
    """List of facility events in the read version."""

    _INSTANCE = FacilityEvents

    def as_apply(self) -> FacilityEventsApplyList:
        """Convert these read versions of facility event to the writing versions."""
        return FacilityEventsApplyList([node.as_apply() for node in self.data])


class FacilityEventsApplyList(DomainModelApplyList[FacilityEventsApply]):
    """List of facility events in the writing version."""

    _INSTANCE = FacilityEventsApply


def _create_facility_event_filter(
    view_id: dm.ViewId,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    facility_event_type_id: str | list[str] | None = None,
    facility_event_type_id_prefix: str | None = None,
    remark: str | list[str] | None = None,
    remark_prefix: str | None = None,
    termination_date_time: str | list[str] | None = None,
    termination_date_time_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if effective_date_time is not None and isinstance(effective_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time))
    if effective_date_time and isinstance(effective_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EffectiveDateTime"), values=effective_date_time))
    if effective_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time_prefix)
        )
    if facility_event_type_id is not None and isinstance(facility_event_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityEventTypeID"), value=facility_event_type_id))
    if facility_event_type_id and isinstance(facility_event_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityEventTypeID"), values=facility_event_type_id))
    if facility_event_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("FacilityEventTypeID"), value=facility_event_type_id_prefix)
        )
    if remark is not None and isinstance(remark, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Remark"), value=remark))
    if remark and isinstance(remark, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Remark"), values=remark))
    if remark_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Remark"), value=remark_prefix))
    if termination_date_time is not None and isinstance(termination_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time))
    if termination_date_time and isinstance(termination_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TerminationDateTime"), values=termination_date_time))
    if termination_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time_prefix)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
