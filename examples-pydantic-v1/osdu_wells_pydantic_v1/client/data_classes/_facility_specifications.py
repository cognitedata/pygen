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
    """This represents the reading version of facility specification.

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

    space: str = DEFAULT_INSTANCE_SPACE
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_specification_date_time: Optional[str] = Field(None, alias="FacilitySpecificationDateTime")
    facility_specification_indicator: Optional[bool] = Field(None, alias="FacilitySpecificationIndicator")
    facility_specification_quantity: Optional[float] = Field(None, alias="FacilitySpecificationQuantity")
    facility_specification_text: Optional[str] = Field(None, alias="FacilitySpecificationText")
    parameter_type_id: Optional[str] = Field(None, alias="ParameterTypeID")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")
    unit_of_measure_id: Optional[str] = Field(None, alias="UnitOfMeasureID")

    def as_apply(self) -> FacilitySpecificationsApply:
        """Convert this read version of facility specification to the writing version."""
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
    """This represents the writing version of facility specification.

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
        existing_version: Fail the ingestion request if the facility specification version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    facility_specification_date_time: Optional[str] = Field(None, alias="FacilitySpecificationDateTime")
    facility_specification_indicator: Optional[bool] = Field(None, alias="FacilitySpecificationIndicator")
    facility_specification_quantity: Optional[float] = Field(None, alias="FacilitySpecificationQuantity")
    facility_specification_text: Optional[str] = Field(None, alias="FacilitySpecificationText")
    parameter_type_id: Optional[str] = Field(None, alias="ParameterTypeID")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")
    unit_of_measure_id: Optional[str] = Field(None, alias="UnitOfMeasureID")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "FacilitySpecifications", "1b7ddbd5d36655"
        )

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


class FacilitySpecificationsList(DomainModelList[FacilitySpecifications]):
    """List of facility specifications in the read version."""

    _INSTANCE = FacilitySpecifications

    def as_apply(self) -> FacilitySpecificationsApplyList:
        """Convert these read versions of facility specification to the writing versions."""
        return FacilitySpecificationsApplyList([node.as_apply() for node in self.data])


class FacilitySpecificationsApplyList(DomainModelApplyList[FacilitySpecificationsApply]):
    """List of facility specifications in the writing version."""

    _INSTANCE = FacilitySpecificationsApply


def _create_facility_specification_filter(
    view_id: dm.ViewId,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    facility_specification_date_time: str | list[str] | None = None,
    facility_specification_date_time_prefix: str | None = None,
    facility_specification_indicator: bool | None = None,
    min_facility_specification_quantity: float | None = None,
    max_facility_specification_quantity: float | None = None,
    facility_specification_text: str | list[str] | None = None,
    facility_specification_text_prefix: str | None = None,
    parameter_type_id: str | list[str] | None = None,
    parameter_type_id_prefix: str | None = None,
    termination_date_time: str | list[str] | None = None,
    termination_date_time_prefix: str | None = None,
    unit_of_measure_id: str | list[str] | None = None,
    unit_of_measure_id_prefix: str | None = None,
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
    if facility_specification_date_time is not None and isinstance(facility_specification_date_time, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("FacilitySpecificationDateTime"), value=facility_specification_date_time
            )
        )
    if facility_specification_date_time and isinstance(facility_specification_date_time, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("FacilitySpecificationDateTime"), values=facility_specification_date_time
            )
        )
    if facility_specification_date_time_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("FacilitySpecificationDateTime"), value=facility_specification_date_time_prefix
            )
        )
    if facility_specification_indicator is not None and isinstance(facility_specification_indicator, bool):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("FacilitySpecificationIndicator"), value=facility_specification_indicator
            )
        )
    if min_facility_specification_quantity or max_facility_specification_quantity:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("FacilitySpecificationQuantity"),
                gte=min_facility_specification_quantity,
                lte=max_facility_specification_quantity,
            )
        )
    if facility_specification_text is not None and isinstance(facility_specification_text, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("FacilitySpecificationText"), value=facility_specification_text)
        )
    if facility_specification_text and isinstance(facility_specification_text, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("FacilitySpecificationText"), values=facility_specification_text)
        )
    if facility_specification_text_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("FacilitySpecificationText"), value=facility_specification_text_prefix
            )
        )
    if parameter_type_id is not None and isinstance(parameter_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ParameterTypeID"), value=parameter_type_id))
    if parameter_type_id and isinstance(parameter_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ParameterTypeID"), values=parameter_type_id))
    if parameter_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ParameterTypeID"), value=parameter_type_id_prefix))
    if termination_date_time is not None and isinstance(termination_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time))
    if termination_date_time and isinstance(termination_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TerminationDateTime"), values=termination_date_time))
    if termination_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time_prefix)
        )
    if unit_of_measure_id is not None and isinstance(unit_of_measure_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("UnitOfMeasureID"), value=unit_of_measure_id))
    if unit_of_measure_id and isinstance(unit_of_measure_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("UnitOfMeasureID"), values=unit_of_measure_id))
    if unit_of_measure_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("UnitOfMeasureID"), value=unit_of_measure_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
