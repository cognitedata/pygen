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
    "FacilityOperators",
    "FacilityOperatorsApply",
    "FacilityOperatorsList",
    "FacilityOperatorsApplyList",
    "FacilityOperatorsFields",
    "FacilityOperatorsTextFields",
]


FacilityOperatorsTextFields = Literal[
    "effective_date_time",
    "facility_operator_id",
    "facility_operator_organisation_id",
    "remark",
    "termination_date_time",
]
FacilityOperatorsFields = Literal[
    "effective_date_time",
    "facility_operator_id",
    "facility_operator_organisation_id",
    "remark",
    "termination_date_time",
]

_FACILITYOPERATORS_PROPERTIES_BY_FIELD = {
    "effective_date_time": "EffectiveDateTime",
    "facility_operator_id": "FacilityOperatorID",
    "facility_operator_organisation_id": "FacilityOperatorOrganisationID",
    "remark": "Remark",
    "termination_date_time": "TerminationDateTime",
}


class FacilityOperators(DomainModel):
    """This represents the reading version of facility operator.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the facility operator.
        effective_date_time: The effective date time field.
        facility_operator_id: The facility operator id field.
        facility_operator_organisation_id: The facility operator organisation id field.
        remark: The remark field.
        termination_date_time: The termination date time field.
        created_time: The created time of the facility operator node.
        last_updated_time: The last updated time of the facility operator node.
        deleted_time: If present, the deleted time of the facility operator node.
        version: The version of the facility operator node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_operator_id: Optional[str] = Field(None, alias="FacilityOperatorID")
    facility_operator_organisation_id: Optional[str] = Field(None, alias="FacilityOperatorOrganisationID")
    remark: Optional[str] = Field(None, alias="Remark")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def as_apply(self) -> FacilityOperatorsApply:
        """Convert this read version of facility operator to the writing version."""
        return FacilityOperatorsApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            facility_operator_id=self.facility_operator_id,
            facility_operator_organisation_id=self.facility_operator_organisation_id,
            remark=self.remark,
            termination_date_time=self.termination_date_time,
        )


class FacilityOperatorsApply(DomainModelApply):
    """This represents the writing version of facility operator.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the facility operator.
        effective_date_time: The effective date time field.
        facility_operator_id: The facility operator id field.
        facility_operator_organisation_id: The facility operator organisation id field.
        remark: The remark field.
        termination_date_time: The termination date time field.
        existing_version: Fail the ingestion request if the facility operator version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_operator_id: Optional[str] = Field(None, alias="FacilityOperatorID")
    facility_operator_organisation_id: Optional[str] = Field(None, alias="FacilityOperatorOrganisationID")
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
            "IntegrationTestsImmutable", "FacilityOperators", "935498861713d0"
        )

        properties = {}

        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time

        if self.facility_operator_id is not None:
            properties["FacilityOperatorID"] = self.facility_operator_id

        if self.facility_operator_organisation_id is not None:
            properties["FacilityOperatorOrganisationID"] = self.facility_operator_organisation_id

        if self.remark is not None:
            properties["Remark"] = self.remark

        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("IntegrationTestsImmutable", "FacilityOperators"),
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


class FacilityOperatorsList(DomainModelList[FacilityOperators]):
    """List of facility operators in the read version."""

    _INSTANCE = FacilityOperators

    def as_apply(self) -> FacilityOperatorsApplyList:
        """Convert these read versions of facility operator to the writing versions."""
        return FacilityOperatorsApplyList([node.as_apply() for node in self.data])


class FacilityOperatorsApplyList(DomainModelApplyList[FacilityOperatorsApply]):
    """List of facility operators in the writing version."""

    _INSTANCE = FacilityOperatorsApply


def _create_facility_operator_filter(
    view_id: dm.ViewId,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    facility_operator_id: str | list[str] | None = None,
    facility_operator_id_prefix: str | None = None,
    facility_operator_organisation_id: str | list[str] | None = None,
    facility_operator_organisation_id_prefix: str | None = None,
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
    if facility_operator_id is not None and isinstance(facility_operator_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityOperatorID"), value=facility_operator_id))
    if facility_operator_id and isinstance(facility_operator_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityOperatorID"), values=facility_operator_id))
    if facility_operator_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("FacilityOperatorID"), value=facility_operator_id_prefix)
        )
    if facility_operator_organisation_id is not None and isinstance(facility_operator_organisation_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("FacilityOperatorOrganisationID"), value=facility_operator_organisation_id
            )
        )
    if facility_operator_organisation_id and isinstance(facility_operator_organisation_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("FacilityOperatorOrganisationID"), values=facility_operator_organisation_id
            )
        )
    if facility_operator_organisation_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("FacilityOperatorOrganisationID"),
                value=facility_operator_organisation_id_prefix,
            )
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
