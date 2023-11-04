from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "FacilitySpecifications",
    "FacilitySpecificationsApply",
    "FacilitySpecificationsList",
    "FacilitySpecificationsApplyList",
    "FacilitySpecificationsFields",
    "FacilitySpecificationsTextFields",
]


FacilitySpecificationsTextFields = Literal[
    "effective_date_time",
    "facility_specification_date_time",
    "facility_specification_text",
    "parameter_type_id",
    "termination_date_time",
    "unit_of_measure_id",
]
FacilitySpecificationsFields = Literal[
    "effective_date_time",
    "facility_specification_date_time",
    "facility_specification_indicator",
    "facility_specification_quantity",
    "facility_specification_text",
    "parameter_type_id",
    "termination_date_time",
    "unit_of_measure_id",
]

_FACILITYSPECIFICATIONS_PROPERTIES_BY_FIELD = {
    "effective_date_time": "EffectiveDateTime",
    "facility_specification_date_time": "FacilitySpecificationDateTime",
    "facility_specification_indicator": "FacilitySpecificationIndicator",
    "facility_specification_quantity": "FacilitySpecificationQuantity",
    "facility_specification_text": "FacilitySpecificationText",
    "parameter_type_id": "ParameterTypeID",
    "termination_date_time": "TerminationDateTime",
    "unit_of_measure_id": "UnitOfMeasureID",
}


class FacilitySpecifications(DomainModel):
    """This represent a read version of facility specification.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the facility specification.
        effective_date_time: The effective date time field.
        facility_specification_date_time: The facility specification date time field.
        facility_specification_indicator: The facility specification indicator field.
        facility_specification_quantity: The facility specification quantity field.
        facility_specification_text: The facility specification text field.
        parameter_type_id: The parameter type id field.
        termination_date_time: The termination date time field.
        unit_of_measure_id: The unit of measure id field.
        created_time: The created time of the facility specification node.
        last_updated_time: The last updated time of the facility specification node.
        deleted_time: If present, the deleted time of the facility specification node.
        version: The version of the facility specification node.
    """

    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_specification_date_time: Optional[str] = Field(None, alias="FacilitySpecificationDateTime")
    facility_specification_indicator: Optional[bool] = Field(None, alias="FacilitySpecificationIndicator")
    facility_specification_quantity: Optional[float] = Field(None, alias="FacilitySpecificationQuantity")
    facility_specification_text: Optional[str] = Field(None, alias="FacilitySpecificationText")
    parameter_type_id: Optional[str] = Field(None, alias="ParameterTypeID")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")
    unit_of_measure_id: Optional[str] = Field(None, alias="UnitOfMeasureID")

    def as_apply(self) -> FacilitySpecificationsApply:
        """Convert this read version of facility specification to a write version."""
        return FacilitySpecificationsApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            facility_specification_date_time=self.facility_specification_date_time,
            facility_specification_indicator=self.facility_specification_indicator,
            facility_specification_quantity=self.facility_specification_quantity,
            facility_specification_text=self.facility_specification_text,
            parameter_type_id=self.parameter_type_id,
            termination_date_time=self.termination_date_time,
            unit_of_measure_id=self.unit_of_measure_id,
        )


class FacilitySpecificationsApply(DomainModelApply):
    """This represent a write version of facility specification.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the facility specification.
        effective_date_time: The effective date time field.
        facility_specification_date_time: The facility specification date time field.
        facility_specification_indicator: The facility specification indicator field.
        facility_specification_quantity: The facility specification quantity field.
        facility_specification_text: The facility specification text field.
        parameter_type_id: The parameter type id field.
        termination_date_time: The termination date time field.
        unit_of_measure_id: The unit of measure id field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_specification_date_time: Optional[str] = Field(None, alias="FacilitySpecificationDateTime")
    facility_specification_indicator: Optional[bool] = Field(None, alias="FacilitySpecificationIndicator")
    facility_specification_quantity: Optional[float] = Field(None, alias="FacilitySpecificationQuantity")
    facility_specification_text: Optional[str] = Field(None, alias="FacilitySpecificationText")
    parameter_type_id: Optional[str] = Field(None, alias="ParameterTypeID")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")
    unit_of_measure_id: Optional[str] = Field(None, alias="UnitOfMeasureID")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.facility_specification_date_time is not None:
            properties["FacilitySpecificationDateTime"] = self.facility_specification_date_time
        if self.facility_specification_indicator is not None:
            properties["FacilitySpecificationIndicator"] = self.facility_specification_indicator
        if self.facility_specification_quantity is not None:
            properties["FacilitySpecificationQuantity"] = self.facility_specification_quantity
        if self.facility_specification_text is not None:
            properties["FacilitySpecificationText"] = self.facility_specification_text
        if self.parameter_type_id is not None:
            properties["ParameterTypeID"] = self.parameter_type_id
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time
        if self.unit_of_measure_id is not None:
            properties["UnitOfMeasureID"] = self.unit_of_measure_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "FacilitySpecifications", "1b7ddbd5d36655"),
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


class FacilitySpecificationsList(TypeList[FacilitySpecifications]):
    """List of facility specifications in read version."""

    _NODE = FacilitySpecifications

    def as_apply(self) -> FacilitySpecificationsApplyList:
        """Convert this read version of facility specification to a write version."""
        return FacilitySpecificationsApplyList([node.as_apply() for node in self.data])


class FacilitySpecificationsApplyList(TypeApplyList[FacilitySpecificationsApply]):
    """List of facility specifications in write version."""

    _NODE = FacilitySpecificationsApply
