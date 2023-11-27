from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


__all__ = [
    "FacilityStates",
    "FacilityStatesApply",
    "FacilityStatesList",
    "FacilityStatesApplyList",
    "FacilityStatesFields",
    "FacilityStatesTextFields",
]


FacilityStatesTextFields = Literal["effective_date_time", "facility_state_type_id", "remark", "termination_date_time"]
FacilityStatesFields = Literal["effective_date_time", "facility_state_type_id", "remark", "termination_date_time"]

_FACILITYSTATES_PROPERTIES_BY_FIELD = {
    "effective_date_time": "EffectiveDateTime",
    "facility_state_type_id": "FacilityStateTypeID",
    "remark": "Remark",
    "termination_date_time": "TerminationDateTime",
}


class FacilityStates(DomainModel):
    """This represents the reading version of facility state.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the facility state.
        effective_date_time: The effective date time field.
        facility_state_type_id: The facility state type id field.
        remark: The remark field.
        termination_date_time: The termination date time field.
        created_time: The created time of the facility state node.
        last_updated_time: The last updated time of the facility state node.
        deleted_time: If present, the deleted time of the facility state node.
        version: The version of the facility state node.
    """

    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_state_type_id: Optional[str] = Field(None, alias="FacilityStateTypeID")
    remark: Optional[str] = Field(None, alias="Remark")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def as_apply(self) -> FacilityStatesApply:
        """Convert this read version of facility state to the writing version."""
        return FacilityStatesApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            facility_state_type_id=self.facility_state_type_id,
            remark=self.remark,
            termination_date_time=self.termination_date_time,
        )


class FacilityStatesApply(DomainModelApply):
    """This represents the writing version of facility state.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the facility state.
        effective_date_time: The effective date time field.
        facility_state_type_id: The facility state type id field.
        remark: The remark field.
        termination_date_time: The termination date time field.
        existing_version: Fail the ingestion request if the facility state version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_state_type_id: Optional[str] = Field(None, alias="FacilityStateTypeID")
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
            "IntegrationTestsImmutable", "FacilityStates", "a12316ff3d8033"
        )

        properties = {}
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.facility_state_type_id is not None:
            properties["FacilityStateTypeID"] = self.facility_state_type_id
        if self.remark is not None:
            properties["Remark"] = self.remark
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
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


class FacilityStatesList(DomainModelList[FacilityStates]):
    """List of facility states in the read version."""

    _INSTANCE = FacilityStates

    def as_apply(self) -> FacilityStatesApplyList:
        """Convert these read versions of facility state to the writing versions."""
        return FacilityStatesApplyList([node.as_apply() for node in self.data])


class FacilityStatesApplyList(DomainModelApplyList[FacilityStatesApply]):
    """List of facility states in the writing version."""

    _INSTANCE = FacilityStatesApply


def _create_facility_state_filter(
    view_id: dm.ViewId,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    facility_state_type_id: str | list[str] | None = None,
    facility_state_type_id_prefix: str | None = None,
    remark: str | list[str] | None = None,
    remark_prefix: str | None = None,
    termination_date_time: str | list[str] | None = None,
    termination_date_time_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if effective_date_time and isinstance(effective_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time))
    if effective_date_time and isinstance(effective_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EffectiveDateTime"), values=effective_date_time))
    if effective_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time_prefix)
        )
    if facility_state_type_id and isinstance(facility_state_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityStateTypeID"), value=facility_state_type_id))
    if facility_state_type_id and isinstance(facility_state_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityStateTypeID"), values=facility_state_type_id))
    if facility_state_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("FacilityStateTypeID"), value=facility_state_type_id_prefix)
        )
    if remark and isinstance(remark, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Remark"), value=remark))
    if remark and isinstance(remark, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Remark"), values=remark))
    if remark_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Remark"), value=remark_prefix))
    if termination_date_time and isinstance(termination_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time))
    if termination_date_time and isinstance(termination_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TerminationDateTime"), values=termination_date_time))
    if termination_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time_prefix)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
