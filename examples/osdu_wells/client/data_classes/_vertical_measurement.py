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
    "VerticalMeasurement",
    "VerticalMeasurementApply",
    "VerticalMeasurementList",
    "VerticalMeasurementApplyList",
    "VerticalMeasurementFields",
    "VerticalMeasurementTextFields",
]


VerticalMeasurementTextFields = Literal[
    "effective_date_time",
    "termination_date_time",
    "vertical_crsid",
    "vertical_measurement_description",
    "vertical_measurement_path_id",
    "vertical_measurement_source_id",
    "vertical_measurement_type_id",
    "vertical_measurement_unit_of_measure_id",
    "vertical_reference_entity_id",
    "vertical_reference_id",
    "wellbore_tvd_trajectory_id",
]
VerticalMeasurementFields = Literal[
    "effective_date_time",
    "termination_date_time",
    "vertical_crsid",
    "vertical_measurement",
    "vertical_measurement_description",
    "vertical_measurement_path_id",
    "vertical_measurement_source_id",
    "vertical_measurement_type_id",
    "vertical_measurement_unit_of_measure_id",
    "vertical_reference_entity_id",
    "vertical_reference_id",
    "wellbore_tvd_trajectory_id",
]

_VERTICALMEASUREMENT_PROPERTIES_BY_FIELD = {
    "effective_date_time": "EffectiveDateTime",
    "termination_date_time": "TerminationDateTime",
    "vertical_crsid": "VerticalCRSID",
    "vertical_measurement": "VerticalMeasurement",
    "vertical_measurement_description": "VerticalMeasurementDescription",
    "vertical_measurement_path_id": "VerticalMeasurementPathID",
    "vertical_measurement_source_id": "VerticalMeasurementSourceID",
    "vertical_measurement_type_id": "VerticalMeasurementTypeID",
    "vertical_measurement_unit_of_measure_id": "VerticalMeasurementUnitOfMeasureID",
    "vertical_reference_entity_id": "VerticalReferenceEntityID",
    "vertical_reference_id": "VerticalReferenceID",
    "wellbore_tvd_trajectory_id": "WellboreTVDTrajectoryID",
}


class VerticalMeasurement(DomainModel):
    """This represents the reading version of vertical measurement.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the vertical measurement.
        effective_date_time: The effective date time field.
        termination_date_time: The termination date time field.
        vertical_crsid: The vertical crsid field.
        vertical_measurement: The vertical measurement field.
        vertical_measurement_description: The vertical measurement description field.
        vertical_measurement_path_id: The vertical measurement path id field.
        vertical_measurement_source_id: The vertical measurement source id field.
        vertical_measurement_type_id: The vertical measurement type id field.
        vertical_measurement_unit_of_measure_id: The vertical measurement unit of measure id field.
        vertical_reference_entity_id: The vertical reference entity id field.
        vertical_reference_id: The vertical reference id field.
        wellbore_tvd_trajectory_id: The wellbore tvd trajectory id field.
        created_time: The created time of the vertical measurement node.
        last_updated_time: The last updated time of the vertical measurement node.
        deleted_time: If present, the deleted time of the vertical measurement node.
        version: The version of the vertical measurement node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")
    vertical_crsid: Optional[str] = Field(None, alias="VerticalCRSID")
    vertical_measurement: Optional[float] = Field(None, alias="VerticalMeasurement")
    vertical_measurement_description: Optional[str] = Field(None, alias="VerticalMeasurementDescription")
    vertical_measurement_path_id: Optional[str] = Field(None, alias="VerticalMeasurementPathID")
    vertical_measurement_source_id: Optional[str] = Field(None, alias="VerticalMeasurementSourceID")
    vertical_measurement_type_id: Optional[str] = Field(None, alias="VerticalMeasurementTypeID")
    vertical_measurement_unit_of_measure_id: Optional[str] = Field(None, alias="VerticalMeasurementUnitOfMeasureID")
    vertical_reference_entity_id: Optional[str] = Field(None, alias="VerticalReferenceEntityID")
    vertical_reference_id: Optional[str] = Field(None, alias="VerticalReferenceID")
    wellbore_tvd_trajectory_id: Optional[str] = Field(None, alias="WellboreTVDTrajectoryID")

    def as_apply(self) -> VerticalMeasurementApply:
        """Convert this read version of vertical measurement to the writing version."""
        return VerticalMeasurementApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            termination_date_time=self.termination_date_time,
            vertical_crsid=self.vertical_crsid,
            vertical_measurement=self.vertical_measurement,
            vertical_measurement_description=self.vertical_measurement_description,
            vertical_measurement_path_id=self.vertical_measurement_path_id,
            vertical_measurement_source_id=self.vertical_measurement_source_id,
            vertical_measurement_type_id=self.vertical_measurement_type_id,
            vertical_measurement_unit_of_measure_id=self.vertical_measurement_unit_of_measure_id,
            vertical_reference_entity_id=self.vertical_reference_entity_id,
            vertical_reference_id=self.vertical_reference_id,
            wellbore_tvd_trajectory_id=self.wellbore_tvd_trajectory_id,
        )


class VerticalMeasurementApply(DomainModelApply):
    """This represents the writing version of vertical measurement.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the vertical measurement.
        effective_date_time: The effective date time field.
        termination_date_time: The termination date time field.
        vertical_crsid: The vertical crsid field.
        vertical_measurement: The vertical measurement field.
        vertical_measurement_description: The vertical measurement description field.
        vertical_measurement_path_id: The vertical measurement path id field.
        vertical_measurement_source_id: The vertical measurement source id field.
        vertical_measurement_type_id: The vertical measurement type id field.
        vertical_measurement_unit_of_measure_id: The vertical measurement unit of measure id field.
        vertical_reference_entity_id: The vertical reference entity id field.
        vertical_reference_id: The vertical reference id field.
        wellbore_tvd_trajectory_id: The wellbore tvd trajectory id field.
        existing_version: Fail the ingestion request if the vertical measurement version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")
    vertical_crsid: Optional[str] = Field(None, alias="VerticalCRSID")
    vertical_measurement: Optional[float] = Field(None, alias="VerticalMeasurement")
    vertical_measurement_description: Optional[str] = Field(None, alias="VerticalMeasurementDescription")
    vertical_measurement_path_id: Optional[str] = Field(None, alias="VerticalMeasurementPathID")
    vertical_measurement_source_id: Optional[str] = Field(None, alias="VerticalMeasurementSourceID")
    vertical_measurement_type_id: Optional[str] = Field(None, alias="VerticalMeasurementTypeID")
    vertical_measurement_unit_of_measure_id: Optional[str] = Field(None, alias="VerticalMeasurementUnitOfMeasureID")
    vertical_reference_entity_id: Optional[str] = Field(None, alias="VerticalReferenceEntityID")
    vertical_reference_id: Optional[str] = Field(None, alias="VerticalReferenceID")
    wellbore_tvd_trajectory_id: Optional[str] = Field(None, alias="WellboreTVDTrajectoryID")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "VerticalMeasurement", "fd63ec6e91292f"
        )

        properties = {}
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time
        if self.vertical_crsid is not None:
            properties["VerticalCRSID"] = self.vertical_crsid
        if self.vertical_measurement is not None:
            properties["VerticalMeasurement"] = self.vertical_measurement
        if self.vertical_measurement_description is not None:
            properties["VerticalMeasurementDescription"] = self.vertical_measurement_description
        if self.vertical_measurement_path_id is not None:
            properties["VerticalMeasurementPathID"] = self.vertical_measurement_path_id
        if self.vertical_measurement_source_id is not None:
            properties["VerticalMeasurementSourceID"] = self.vertical_measurement_source_id
        if self.vertical_measurement_type_id is not None:
            properties["VerticalMeasurementTypeID"] = self.vertical_measurement_type_id
        if self.vertical_measurement_unit_of_measure_id is not None:
            properties["VerticalMeasurementUnitOfMeasureID"] = self.vertical_measurement_unit_of_measure_id
        if self.vertical_reference_entity_id is not None:
            properties["VerticalReferenceEntityID"] = self.vertical_reference_entity_id
        if self.vertical_reference_id is not None:
            properties["VerticalReferenceID"] = self.vertical_reference_id
        if self.wellbore_tvd_trajectory_id is not None:
            properties["WellboreTVDTrajectoryID"] = self.wellbore_tvd_trajectory_id

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


class VerticalMeasurementList(DomainModelList[VerticalMeasurement]):
    """List of vertical measurements in the read version."""

    _INSTANCE = VerticalMeasurement

    def as_apply(self) -> VerticalMeasurementApplyList:
        """Convert these read versions of vertical measurement to the writing versions."""
        return VerticalMeasurementApplyList([node.as_apply() for node in self.data])


class VerticalMeasurementApplyList(DomainModelApplyList[VerticalMeasurementApply]):
    """List of vertical measurements in the writing version."""

    _INSTANCE = VerticalMeasurementApply


def _create_vertical_measurement_filter(
    view_id: dm.ViewId,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    termination_date_time: str | list[str] | None = None,
    termination_date_time_prefix: str | None = None,
    vertical_crsid: str | list[str] | None = None,
    vertical_crsid_prefix: str | None = None,
    min_vertical_measurement: float | None = None,
    max_vertical_measurement: float | None = None,
    vertical_measurement_description: str | list[str] | None = None,
    vertical_measurement_description_prefix: str | None = None,
    vertical_measurement_path_id: str | list[str] | None = None,
    vertical_measurement_path_id_prefix: str | None = None,
    vertical_measurement_source_id: str | list[str] | None = None,
    vertical_measurement_source_id_prefix: str | None = None,
    vertical_measurement_type_id: str | list[str] | None = None,
    vertical_measurement_type_id_prefix: str | None = None,
    vertical_measurement_unit_of_measure_id: str | list[str] | None = None,
    vertical_measurement_unit_of_measure_id_prefix: str | None = None,
    vertical_reference_entity_id: str | list[str] | None = None,
    vertical_reference_entity_id_prefix: str | None = None,
    vertical_reference_id: str | list[str] | None = None,
    vertical_reference_id_prefix: str | None = None,
    wellbore_tvd_trajectory_id: str | list[str] | None = None,
    wellbore_tvd_trajectory_id_prefix: str | None = None,
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
    if termination_date_time and isinstance(termination_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time))
    if termination_date_time and isinstance(termination_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TerminationDateTime"), values=termination_date_time))
    if termination_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time_prefix)
        )
    if vertical_crsid and isinstance(vertical_crsid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("VerticalCRSID"), value=vertical_crsid))
    if vertical_crsid and isinstance(vertical_crsid, list):
        filters.append(dm.filters.In(view_id.as_property_ref("VerticalCRSID"), values=vertical_crsid))
    if vertical_crsid_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("VerticalCRSID"), value=vertical_crsid_prefix))
    if min_vertical_measurement or max_vertical_measurement:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("VerticalMeasurement"),
                gte=min_vertical_measurement,
                lte=max_vertical_measurement,
            )
        )
    if vertical_measurement_description and isinstance(vertical_measurement_description, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalMeasurementDescription"), value=vertical_measurement_description
            )
        )
    if vertical_measurement_description and isinstance(vertical_measurement_description, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("VerticalMeasurementDescription"), values=vertical_measurement_description
            )
        )
    if vertical_measurement_description_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalMeasurementDescription"), value=vertical_measurement_description_prefix
            )
        )
    if vertical_measurement_path_id and isinstance(vertical_measurement_path_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("VerticalMeasurementPathID"), value=vertical_measurement_path_id)
        )
    if vertical_measurement_path_id and isinstance(vertical_measurement_path_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("VerticalMeasurementPathID"), values=vertical_measurement_path_id)
        )
    if vertical_measurement_path_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalMeasurementPathID"), value=vertical_measurement_path_id_prefix
            )
        )
    if vertical_measurement_source_id and isinstance(vertical_measurement_source_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalMeasurementSourceID"), value=vertical_measurement_source_id
            )
        )
    if vertical_measurement_source_id and isinstance(vertical_measurement_source_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("VerticalMeasurementSourceID"), values=vertical_measurement_source_id)
        )
    if vertical_measurement_source_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalMeasurementSourceID"), value=vertical_measurement_source_id_prefix
            )
        )
    if vertical_measurement_type_id and isinstance(vertical_measurement_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("VerticalMeasurementTypeID"), value=vertical_measurement_type_id)
        )
    if vertical_measurement_type_id and isinstance(vertical_measurement_type_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("VerticalMeasurementTypeID"), values=vertical_measurement_type_id)
        )
    if vertical_measurement_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalMeasurementTypeID"), value=vertical_measurement_type_id_prefix
            )
        )
    if vertical_measurement_unit_of_measure_id and isinstance(vertical_measurement_unit_of_measure_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalMeasurementUnitOfMeasureID"),
                value=vertical_measurement_unit_of_measure_id,
            )
        )
    if vertical_measurement_unit_of_measure_id and isinstance(vertical_measurement_unit_of_measure_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("VerticalMeasurementUnitOfMeasureID"),
                values=vertical_measurement_unit_of_measure_id,
            )
        )
    if vertical_measurement_unit_of_measure_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalMeasurementUnitOfMeasureID"),
                value=vertical_measurement_unit_of_measure_id_prefix,
            )
        )
    if vertical_reference_entity_id and isinstance(vertical_reference_entity_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("VerticalReferenceEntityID"), value=vertical_reference_entity_id)
        )
    if vertical_reference_entity_id and isinstance(vertical_reference_entity_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("VerticalReferenceEntityID"), values=vertical_reference_entity_id)
        )
    if vertical_reference_entity_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalReferenceEntityID"), value=vertical_reference_entity_id_prefix
            )
        )
    if vertical_reference_id and isinstance(vertical_reference_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("VerticalReferenceID"), value=vertical_reference_id))
    if vertical_reference_id and isinstance(vertical_reference_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("VerticalReferenceID"), values=vertical_reference_id))
    if vertical_reference_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("VerticalReferenceID"), value=vertical_reference_id_prefix)
        )
    if wellbore_tvd_trajectory_id and isinstance(wellbore_tvd_trajectory_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("WellboreTVDTrajectoryID"), value=wellbore_tvd_trajectory_id)
        )
    if wellbore_tvd_trajectory_id and isinstance(wellbore_tvd_trajectory_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("WellboreTVDTrajectoryID"), values=wellbore_tvd_trajectory_id)
        )
    if wellbore_tvd_trajectory_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("WellboreTVDTrajectoryID"), value=wellbore_tvd_trajectory_id_prefix
            )
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
