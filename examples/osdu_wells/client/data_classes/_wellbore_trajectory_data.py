from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._artefacts import ArtefactsApply
    from ._available_trajectory_station_properties import AvailableTrajectoryStationPropertiesApply
    from ._geo_contexts import GeoContextsApply
    from ._lineage_assertions import LineageAssertionsApply
    from ._name_aliases import NameAliasesApply
    from ._spatial_area import SpatialAreaApply
    from ._spatial_point import SpatialPointApply
    from ._technical_assurances import TechnicalAssurancesApply
    from ._vertical_measurement import VerticalMeasurementApply

__all__ = [
    "WellboreTrajectoryData",
    "WellboreTrajectoryDataApply",
    "WellboreTrajectoryDataList",
    "WellboreTrajectoryDataApplyList",
    "WellboreTrajectoryDataFields",
    "WellboreTrajectoryDataTextFields",
]


WellboreTrajectoryDataTextFields = Literal[
    "acquisition_date",
    "acquisition_remark",
    "applied_operations",
    "applied_operations_date_time",
    "applied_operations_remarks",
    "applied_operations_user",
    "author_i_ds",
    "azimuth_reference_type",
    "business_activities",
    "calculation_method_type",
    "company_id",
    "creation_date_time",
    "ddms_datasets",
    "datasets",
    "description",
    "end_date_time",
    "existence_kind",
    "extrapolated_measured_depth_remark",
    "geographic_crsid",
    "name",
    "projected_crsid",
    "resource_curation_status",
    "resource_home_region_id",
    "resource_host_region_i_ds",
    "resource_lifecycle_status",
    "resource_security_classification",
    "service_company_id",
    "source",
    "start_date_time",
    "submitter_name",
    "survey_reference_identifier",
    "survey_tool_type_id",
    "survey_type",
    "survey_version",
    "tags",
    "wellbore_id",
]
WellboreTrajectoryDataFields = Literal[
    "acquisition_date",
    "acquisition_remark",
    "active_indicator",
    "applied_operations",
    "applied_operations_date_time",
    "applied_operations_remarks",
    "applied_operations_user",
    "author_i_ds",
    "azimuth_reference_type",
    "base_depth_measured_depth",
    "business_activities",
    "calculation_method_type",
    "company_id",
    "creation_date_time",
    "ddms_datasets",
    "datasets",
    "description",
    "end_date_time",
    "existence_kind",
    "extrapolated_measured_depth",
    "extrapolated_measured_depth_remark",
    "geographic_crsid",
    "is_discoverable",
    "is_extended_load",
    "name",
    "projected_crsid",
    "resource_curation_status",
    "resource_home_region_id",
    "resource_host_region_i_ds",
    "resource_lifecycle_status",
    "resource_security_classification",
    "service_company_id",
    "source",
    "start_date_time",
    "submitter_name",
    "surface_grid_convergence",
    "surface_scale_factor",
    "survey_reference_identifier",
    "survey_tool_type_id",
    "survey_type",
    "survey_version",
    "tags",
    "tie_measured_depth",
    "tie_true_vertical_depth",
    "top_depth_measured_depth",
    "tortuosity",
    "wellbore_id",
]

_WELLBORETRAJECTORYDATA_PROPERTIES_BY_FIELD = {
    "acquisition_date": "AcquisitionDate",
    "acquisition_remark": "AcquisitionRemark",
    "active_indicator": "ActiveIndicator",
    "applied_operations": "AppliedOperations",
    "applied_operations_date_time": "AppliedOperationsDateTime",
    "applied_operations_remarks": "AppliedOperationsRemarks",
    "applied_operations_user": "AppliedOperationsUser",
    "author_i_ds": "AuthorIDs",
    "azimuth_reference_type": "AzimuthReferenceType",
    "base_depth_measured_depth": "BaseDepthMeasuredDepth",
    "business_activities": "BusinessActivities",
    "calculation_method_type": "CalculationMethodType",
    "company_id": "CompanyID",
    "creation_date_time": "CreationDateTime",
    "ddms_datasets": "DDMSDatasets",
    "datasets": "Datasets",
    "description": "Description",
    "end_date_time": "EndDateTime",
    "existence_kind": "ExistenceKind",
    "extrapolated_measured_depth": "ExtrapolatedMeasuredDepth",
    "extrapolated_measured_depth_remark": "ExtrapolatedMeasuredDepthRemark",
    "geographic_crsid": "GeographicCRSID",
    "is_discoverable": "IsDiscoverable",
    "is_extended_load": "IsExtendedLoad",
    "name": "Name",
    "projected_crsid": "ProjectedCRSID",
    "resource_curation_status": "ResourceCurationStatus",
    "resource_home_region_id": "ResourceHomeRegionID",
    "resource_host_region_i_ds": "ResourceHostRegionIDs",
    "resource_lifecycle_status": "ResourceLifecycleStatus",
    "resource_security_classification": "ResourceSecurityClassification",
    "service_company_id": "ServiceCompanyID",
    "source": "Source",
    "start_date_time": "StartDateTime",
    "submitter_name": "SubmitterName",
    "surface_grid_convergence": "SurfaceGridConvergence",
    "surface_scale_factor": "SurfaceScaleFactor",
    "survey_reference_identifier": "SurveyReferenceIdentifier",
    "survey_tool_type_id": "SurveyToolTypeID",
    "survey_type": "SurveyType",
    "survey_version": "SurveyVersion",
    "tags": "Tags",
    "tie_measured_depth": "TieMeasuredDepth",
    "tie_true_vertical_depth": "TieTrueVerticalDepth",
    "top_depth_measured_depth": "TopDepthMeasuredDepth",
    "tortuosity": "Tortuosity",
    "wellbore_id": "WellboreID",
}


class WellboreTrajectoryData(DomainModel):
    space: str = "IntegrationTestsImmutable"
    acquisition_date: Optional[str] = Field(None, alias="AcquisitionDate")
    acquisition_remark: Optional[str] = Field(None, alias="AcquisitionRemark")
    active_indicator: Optional[bool] = Field(None, alias="ActiveIndicator")
    applied_operations: Optional[list[str]] = Field(None, alias="AppliedOperations")
    applied_operations_date_time: Optional[str] = Field(None, alias="AppliedOperationsDateTime")
    applied_operations_remarks: Optional[str] = Field(None, alias="AppliedOperationsRemarks")
    applied_operations_user: Optional[str] = Field(None, alias="AppliedOperationsUser")
    artefacts: Optional[list[str]] = Field(None, alias="Artefacts")
    author_i_ds: Optional[list[str]] = Field(None, alias="AuthorIDs")
    available_trajectory_station_properties: Optional[list[str]] = Field(
        None, alias="AvailableTrajectoryStationProperties"
    )
    azimuth_reference_type: Optional[str] = Field(None, alias="AzimuthReferenceType")
    base_depth_measured_depth: Optional[int] = Field(None, alias="BaseDepthMeasuredDepth")
    business_activities: Optional[list[str]] = Field(None, alias="BusinessActivities")
    calculation_method_type: Optional[str] = Field(None, alias="CalculationMethodType")
    company_id: Optional[str] = Field(None, alias="CompanyID")
    creation_date_time: Optional[str] = Field(None, alias="CreationDateTime")
    ddms_datasets: Optional[list[str]] = Field(None, alias="DDMSDatasets")
    datasets: Optional[list[str]] = Field(None, alias="Datasets")
    description: Optional[str] = Field(None, alias="Description")
    end_date_time: Optional[str] = Field(None, alias="EndDateTime")
    existence_kind: Optional[str] = Field(None, alias="ExistenceKind")
    extrapolated_measured_depth: Optional[int] = Field(None, alias="ExtrapolatedMeasuredDepth")
    extrapolated_measured_depth_remark: Optional[str] = Field(None, alias="ExtrapolatedMeasuredDepthRemark")
    geo_contexts: Optional[list[str]] = Field(None, alias="GeoContexts")
    geographic_crsid: Optional[str] = Field(None, alias="GeographicCRSID")
    is_discoverable: Optional[bool] = Field(None, alias="IsDiscoverable")
    is_extended_load: Optional[bool] = Field(None, alias="IsExtendedLoad")
    lineage_assertions: Optional[list[str]] = Field(None, alias="LineageAssertions")
    name: Optional[str] = Field(None, alias="Name")
    name_aliases: Optional[list[str]] = Field(None, alias="NameAliases")
    projected_crsid: Optional[str] = Field(None, alias="ProjectedCRSID")
    resource_curation_status: Optional[str] = Field(None, alias="ResourceCurationStatus")
    resource_home_region_id: Optional[str] = Field(None, alias="ResourceHomeRegionID")
    resource_host_region_i_ds: Optional[list[str]] = Field(None, alias="ResourceHostRegionIDs")
    resource_lifecycle_status: Optional[str] = Field(None, alias="ResourceLifecycleStatus")
    resource_security_classification: Optional[str] = Field(None, alias="ResourceSecurityClassification")
    service_company_id: Optional[str] = Field(None, alias="ServiceCompanyID")
    source: Optional[str] = Field(None, alias="Source")
    spatial_area: Optional[str] = Field(None, alias="SpatialArea")
    spatial_point: Optional[str] = Field(None, alias="SpatialPoint")
    start_date_time: Optional[str] = Field(None, alias="StartDateTime")
    submitter_name: Optional[str] = Field(None, alias="SubmitterName")
    surface_grid_convergence: Optional[float] = Field(None, alias="SurfaceGridConvergence")
    surface_scale_factor: Optional[float] = Field(None, alias="SurfaceScaleFactor")
    survey_reference_identifier: Optional[str] = Field(None, alias="SurveyReferenceIdentifier")
    survey_tool_type_id: Optional[str] = Field(None, alias="SurveyToolTypeID")
    survey_type: Optional[str] = Field(None, alias="SurveyType")
    survey_version: Optional[str] = Field(None, alias="SurveyVersion")
    tags: Optional[list[str]] = Field(None, alias="Tags")
    technical_assurances: Optional[list[str]] = Field(None, alias="TechnicalAssurances")
    tie_measured_depth: Optional[int] = Field(None, alias="TieMeasuredDepth")
    tie_true_vertical_depth: Optional[int] = Field(None, alias="TieTrueVerticalDepth")
    top_depth_measured_depth: Optional[int] = Field(None, alias="TopDepthMeasuredDepth")
    tortuosity: Optional[float] = Field(None, alias="Tortuosity")
    vertical_measurement: Optional[str] = Field(None, alias="VerticalMeasurement")
    wellbore_id: Optional[str] = Field(None, alias="WellboreID")

    def as_apply(self) -> WellboreTrajectoryDataApply:
        return WellboreTrajectoryDataApply(
            space=self.space,
            external_id=self.external_id,
            acquisition_date=self.acquisition_date,
            acquisition_remark=self.acquisition_remark,
            active_indicator=self.active_indicator,
            applied_operations=self.applied_operations,
            applied_operations_date_time=self.applied_operations_date_time,
            applied_operations_remarks=self.applied_operations_remarks,
            applied_operations_user=self.applied_operations_user,
            artefacts=self.artefacts,
            author_i_ds=self.author_i_ds,
            available_trajectory_station_properties=self.available_trajectory_station_properties,
            azimuth_reference_type=self.azimuth_reference_type,
            base_depth_measured_depth=self.base_depth_measured_depth,
            business_activities=self.business_activities,
            calculation_method_type=self.calculation_method_type,
            company_id=self.company_id,
            creation_date_time=self.creation_date_time,
            ddms_datasets=self.ddms_datasets,
            datasets=self.datasets,
            description=self.description,
            end_date_time=self.end_date_time,
            existence_kind=self.existence_kind,
            extrapolated_measured_depth=self.extrapolated_measured_depth,
            extrapolated_measured_depth_remark=self.extrapolated_measured_depth_remark,
            geo_contexts=self.geo_contexts,
            geographic_crsid=self.geographic_crsid,
            is_discoverable=self.is_discoverable,
            is_extended_load=self.is_extended_load,
            lineage_assertions=self.lineage_assertions,
            name=self.name,
            name_aliases=self.name_aliases,
            projected_crsid=self.projected_crsid,
            resource_curation_status=self.resource_curation_status,
            resource_home_region_id=self.resource_home_region_id,
            resource_host_region_i_ds=self.resource_host_region_i_ds,
            resource_lifecycle_status=self.resource_lifecycle_status,
            resource_security_classification=self.resource_security_classification,
            service_company_id=self.service_company_id,
            source=self.source,
            spatial_area=self.spatial_area,
            spatial_point=self.spatial_point,
            start_date_time=self.start_date_time,
            submitter_name=self.submitter_name,
            surface_grid_convergence=self.surface_grid_convergence,
            surface_scale_factor=self.surface_scale_factor,
            survey_reference_identifier=self.survey_reference_identifier,
            survey_tool_type_id=self.survey_tool_type_id,
            survey_type=self.survey_type,
            survey_version=self.survey_version,
            tags=self.tags,
            technical_assurances=self.technical_assurances,
            tie_measured_depth=self.tie_measured_depth,
            tie_true_vertical_depth=self.tie_true_vertical_depth,
            top_depth_measured_depth=self.top_depth_measured_depth,
            tortuosity=self.tortuosity,
            vertical_measurement=self.vertical_measurement,
            wellbore_id=self.wellbore_id,
        )


class WellboreTrajectoryDataApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    acquisition_date: Optional[str] = Field(None, alias="AcquisitionDate")
    acquisition_remark: Optional[str] = Field(None, alias="AcquisitionRemark")
    active_indicator: Optional[bool] = Field(None, alias="ActiveIndicator")
    applied_operations: Optional[list[str]] = Field(None, alias="AppliedOperations")
    applied_operations_date_time: Optional[str] = Field(None, alias="AppliedOperationsDateTime")
    applied_operations_remarks: Optional[str] = Field(None, alias="AppliedOperationsRemarks")
    applied_operations_user: Optional[str] = Field(None, alias="AppliedOperationsUser")
    artefacts: Union[list[ArtefactsApply], list[str], None] = Field(default=None, repr=False, alias="Artefacts")
    author_i_ds: Optional[list[str]] = Field(None, alias="AuthorIDs")
    available_trajectory_station_properties: Union[
        list[AvailableTrajectoryStationPropertiesApply], list[str], None
    ] = Field(default=None, repr=False, alias="AvailableTrajectoryStationProperties")
    azimuth_reference_type: Optional[str] = Field(None, alias="AzimuthReferenceType")
    base_depth_measured_depth: Optional[int] = Field(None, alias="BaseDepthMeasuredDepth")
    business_activities: Optional[list[str]] = Field(None, alias="BusinessActivities")
    calculation_method_type: Optional[str] = Field(None, alias="CalculationMethodType")
    company_id: Optional[str] = Field(None, alias="CompanyID")
    creation_date_time: Optional[str] = Field(None, alias="CreationDateTime")
    ddms_datasets: Optional[list[str]] = Field(None, alias="DDMSDatasets")
    datasets: Optional[list[str]] = Field(None, alias="Datasets")
    description: Optional[str] = Field(None, alias="Description")
    end_date_time: Optional[str] = Field(None, alias="EndDateTime")
    existence_kind: Optional[str] = Field(None, alias="ExistenceKind")
    extrapolated_measured_depth: Optional[int] = Field(None, alias="ExtrapolatedMeasuredDepth")
    extrapolated_measured_depth_remark: Optional[str] = Field(None, alias="ExtrapolatedMeasuredDepthRemark")
    geo_contexts: Union[list[GeoContextsApply], list[str], None] = Field(default=None, repr=False, alias="GeoContexts")
    geographic_crsid: Optional[str] = Field(None, alias="GeographicCRSID")
    is_discoverable: Optional[bool] = Field(None, alias="IsDiscoverable")
    is_extended_load: Optional[bool] = Field(None, alias="IsExtendedLoad")
    lineage_assertions: Union[list[LineageAssertionsApply], list[str], None] = Field(
        default=None, repr=False, alias="LineageAssertions"
    )
    name: Optional[str] = Field(None, alias="Name")
    name_aliases: Union[list[NameAliasesApply], list[str], None] = Field(default=None, repr=False, alias="NameAliases")
    projected_crsid: Optional[str] = Field(None, alias="ProjectedCRSID")
    resource_curation_status: Optional[str] = Field(None, alias="ResourceCurationStatus")
    resource_home_region_id: Optional[str] = Field(None, alias="ResourceHomeRegionID")
    resource_host_region_i_ds: Optional[list[str]] = Field(None, alias="ResourceHostRegionIDs")
    resource_lifecycle_status: Optional[str] = Field(None, alias="ResourceLifecycleStatus")
    resource_security_classification: Optional[str] = Field(None, alias="ResourceSecurityClassification")
    service_company_id: Optional[str] = Field(None, alias="ServiceCompanyID")
    source: Optional[str] = Field(None, alias="Source")
    spatial_area: Union[SpatialAreaApply, str, None] = Field(None, repr=False, alias="SpatialArea")
    spatial_point: Union[SpatialPointApply, str, None] = Field(None, repr=False, alias="SpatialPoint")
    start_date_time: Optional[str] = Field(None, alias="StartDateTime")
    submitter_name: Optional[str] = Field(None, alias="SubmitterName")
    surface_grid_convergence: Optional[float] = Field(None, alias="SurfaceGridConvergence")
    surface_scale_factor: Optional[float] = Field(None, alias="SurfaceScaleFactor")
    survey_reference_identifier: Optional[str] = Field(None, alias="SurveyReferenceIdentifier")
    survey_tool_type_id: Optional[str] = Field(None, alias="SurveyToolTypeID")
    survey_type: Optional[str] = Field(None, alias="SurveyType")
    survey_version: Optional[str] = Field(None, alias="SurveyVersion")
    tags: Optional[list[str]] = Field(None, alias="Tags")
    technical_assurances: Union[list[TechnicalAssurancesApply], list[str], None] = Field(
        default=None, repr=False, alias="TechnicalAssurances"
    )
    tie_measured_depth: Optional[int] = Field(None, alias="TieMeasuredDepth")
    tie_true_vertical_depth: Optional[int] = Field(None, alias="TieTrueVerticalDepth")
    top_depth_measured_depth: Optional[int] = Field(None, alias="TopDepthMeasuredDepth")
    tortuosity: Optional[float] = Field(None, alias="Tortuosity")
    vertical_measurement: Union[VerticalMeasurementApply, str, None] = Field(
        None, repr=False, alias="VerticalMeasurement"
    )
    wellbore_id: Optional[str] = Field(None, alias="WellboreID")

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        properties = {}
        if self.acquisition_date is not None:
            properties["AcquisitionDate"] = self.acquisition_date
        if self.acquisition_remark is not None:
            properties["AcquisitionRemark"] = self.acquisition_remark
        if self.active_indicator is not None:
            properties["ActiveIndicator"] = self.active_indicator
        if self.applied_operations is not None:
            properties["AppliedOperations"] = self.applied_operations
        if self.applied_operations_date_time is not None:
            properties["AppliedOperationsDateTime"] = self.applied_operations_date_time
        if self.applied_operations_remarks is not None:
            properties["AppliedOperationsRemarks"] = self.applied_operations_remarks
        if self.applied_operations_user is not None:
            properties["AppliedOperationsUser"] = self.applied_operations_user
        if self.author_i_ds is not None:
            properties["AuthorIDs"] = self.author_i_ds
        if self.azimuth_reference_type is not None:
            properties["AzimuthReferenceType"] = self.azimuth_reference_type
        if self.base_depth_measured_depth is not None:
            properties["BaseDepthMeasuredDepth"] = self.base_depth_measured_depth
        if self.business_activities is not None:
            properties["BusinessActivities"] = self.business_activities
        if self.calculation_method_type is not None:
            properties["CalculationMethodType"] = self.calculation_method_type
        if self.company_id is not None:
            properties["CompanyID"] = self.company_id
        if self.creation_date_time is not None:
            properties["CreationDateTime"] = self.creation_date_time
        if self.ddms_datasets is not None:
            properties["DDMSDatasets"] = self.ddms_datasets
        if self.datasets is not None:
            properties["Datasets"] = self.datasets
        if self.description is not None:
            properties["Description"] = self.description
        if self.end_date_time is not None:
            properties["EndDateTime"] = self.end_date_time
        if self.existence_kind is not None:
            properties["ExistenceKind"] = self.existence_kind
        if self.extrapolated_measured_depth is not None:
            properties["ExtrapolatedMeasuredDepth"] = self.extrapolated_measured_depth
        if self.extrapolated_measured_depth_remark is not None:
            properties["ExtrapolatedMeasuredDepthRemark"] = self.extrapolated_measured_depth_remark
        if self.geographic_crsid is not None:
            properties["GeographicCRSID"] = self.geographic_crsid
        if self.is_discoverable is not None:
            properties["IsDiscoverable"] = self.is_discoverable
        if self.is_extended_load is not None:
            properties["IsExtendedLoad"] = self.is_extended_load
        if self.name is not None:
            properties["Name"] = self.name
        if self.projected_crsid is not None:
            properties["ProjectedCRSID"] = self.projected_crsid
        if self.resource_curation_status is not None:
            properties["ResourceCurationStatus"] = self.resource_curation_status
        if self.resource_home_region_id is not None:
            properties["ResourceHomeRegionID"] = self.resource_home_region_id
        if self.resource_host_region_i_ds is not None:
            properties["ResourceHostRegionIDs"] = self.resource_host_region_i_ds
        if self.resource_lifecycle_status is not None:
            properties["ResourceLifecycleStatus"] = self.resource_lifecycle_status
        if self.resource_security_classification is not None:
            properties["ResourceSecurityClassification"] = self.resource_security_classification
        if self.service_company_id is not None:
            properties["ServiceCompanyID"] = self.service_company_id
        if self.source is not None:
            properties["Source"] = self.source
        if self.spatial_area is not None:
            properties["SpatialArea"] = {
                "space": "IntegrationTestsImmutable",
                "externalId": self.spatial_area
                if isinstance(self.spatial_area, str)
                else self.spatial_area.external_id,
            }
        if self.spatial_point is not None:
            properties["SpatialPoint"] = {
                "space": "IntegrationTestsImmutable",
                "externalId": self.spatial_point
                if isinstance(self.spatial_point, str)
                else self.spatial_point.external_id,
            }
        if self.start_date_time is not None:
            properties["StartDateTime"] = self.start_date_time
        if self.submitter_name is not None:
            properties["SubmitterName"] = self.submitter_name
        if self.surface_grid_convergence is not None:
            properties["SurfaceGridConvergence"] = self.surface_grid_convergence
        if self.surface_scale_factor is not None:
            properties["SurfaceScaleFactor"] = self.surface_scale_factor
        if self.survey_reference_identifier is not None:
            properties["SurveyReferenceIdentifier"] = self.survey_reference_identifier
        if self.survey_tool_type_id is not None:
            properties["SurveyToolTypeID"] = self.survey_tool_type_id
        if self.survey_type is not None:
            properties["SurveyType"] = self.survey_type
        if self.survey_version is not None:
            properties["SurveyVersion"] = self.survey_version
        if self.tags is not None:
            properties["Tags"] = self.tags
        if self.tie_measured_depth is not None:
            properties["TieMeasuredDepth"] = self.tie_measured_depth
        if self.tie_true_vertical_depth is not None:
            properties["TieTrueVerticalDepth"] = self.tie_true_vertical_depth
        if self.top_depth_measured_depth is not None:
            properties["TopDepthMeasuredDepth"] = self.top_depth_measured_depth
        if self.tortuosity is not None:
            properties["Tortuosity"] = self.tortuosity
        if self.vertical_measurement is not None:
            properties["VerticalMeasurement"] = {
                "space": "IntegrationTestsImmutable",
                "externalId": self.vertical_measurement
                if isinstance(self.vertical_measurement, str)
                else self.vertical_measurement.external_id,
            }
        if self.wellbore_id is not None:
            properties["WellboreID"] = self.wellbore_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "WellboreTrajectoryData", "d35eace9691587"),
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

        for artefact in self.artefacts or []:
            edge = self._create_artefact_edge(artefact)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(artefact, DomainModelApply):
                instances = artefact._to_instances_apply(cache, write_view)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for available_trajectory_station_property in self.available_trajectory_station_properties or []:
            edge = self._create_available_trajectory_station_property_edge(available_trajectory_station_property)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(available_trajectory_station_property, DomainModelApply):
                instances = available_trajectory_station_property._to_instances_apply(cache, write_view)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for geo_context in self.geo_contexts or []:
            edge = self._create_geo_context_edge(geo_context)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(geo_context, DomainModelApply):
                instances = geo_context._to_instances_apply(cache, write_view)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for lineage_assertion in self.lineage_assertions or []:
            edge = self._create_lineage_assertion_edge(lineage_assertion)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(lineage_assertion, DomainModelApply):
                instances = lineage_assertion._to_instances_apply(cache, write_view)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for name_alias in self.name_aliases or []:
            edge = self._create_name_alias_edge(name_alias)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(name_alias, DomainModelApply):
                instances = name_alias._to_instances_apply(cache, write_view)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for technical_assurance in self.technical_assurances or []:
            edge = self._create_technical_assurance_edge(technical_assurance)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(technical_assurance, DomainModelApply):
                instances = technical_assurance._to_instances_apply(cache, write_view)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.spatial_area, DomainModelApply):
            instances = self.spatial_area._to_instances_apply(cache, write_view)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.spatial_point, DomainModelApply):
            instances = self.spatial_point._to_instances_apply(cache, write_view)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.vertical_measurement, DomainModelApply):
            instances = self.vertical_measurement._to_instances_apply(cache, write_view)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_artefact_edge(self, artefact: Union[str, ArtefactsApply]) -> dm.EdgeApply:
        if isinstance(artefact, str):
            end_node_ext_id = artefact
        elif isinstance(artefact, DomainModelApply):
            end_node_ext_id = artefact.external_id
        else:
            raise TypeError(f"Expected str or ArtefactsApply, got {type(artefact)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.Artefacts"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_available_trajectory_station_property_edge(
        self, available_trajectory_station_property: Union[str, AvailableTrajectoryStationPropertiesApply]
    ) -> dm.EdgeApply:
        if isinstance(available_trajectory_station_property, str):
            end_node_ext_id = available_trajectory_station_property
        elif isinstance(available_trajectory_station_property, DomainModelApply):
            end_node_ext_id = available_trajectory_station_property.external_id
        else:
            raise TypeError(
                f"Expected str or AvailableTrajectoryStationPropertiesApply, got {type(available_trajectory_station_property)}"
            )

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference(
                "IntegrationTestsImmutable", "WellboreTrajectoryData.AvailableTrajectoryStationProperties"
            ),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_geo_context_edge(self, geo_context: Union[str, GeoContextsApply]) -> dm.EdgeApply:
        if isinstance(geo_context, str):
            end_node_ext_id = geo_context
        elif isinstance(geo_context, DomainModelApply):
            end_node_ext_id = geo_context.external_id
        else:
            raise TypeError(f"Expected str or GeoContextsApply, got {type(geo_context)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.GeoContexts"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_lineage_assertion_edge(self, lineage_assertion: Union[str, LineageAssertionsApply]) -> dm.EdgeApply:
        if isinstance(lineage_assertion, str):
            end_node_ext_id = lineage_assertion
        elif isinstance(lineage_assertion, DomainModelApply):
            end_node_ext_id = lineage_assertion.external_id
        else:
            raise TypeError(f"Expected str or LineageAssertionsApply, got {type(lineage_assertion)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.LineageAssertions"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_name_alias_edge(self, name_alias: Union[str, NameAliasesApply]) -> dm.EdgeApply:
        if isinstance(name_alias, str):
            end_node_ext_id = name_alias
        elif isinstance(name_alias, DomainModelApply):
            end_node_ext_id = name_alias.external_id
        else:
            raise TypeError(f"Expected str or NameAliasesApply, got {type(name_alias)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.NameAliases"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_technical_assurance_edge(
        self, technical_assurance: Union[str, TechnicalAssurancesApply]
    ) -> dm.EdgeApply:
        if isinstance(technical_assurance, str):
            end_node_ext_id = technical_assurance
        elif isinstance(technical_assurance, DomainModelApply):
            end_node_ext_id = technical_assurance.external_id
        else:
            raise TypeError(f"Expected str or TechnicalAssurancesApply, got {type(technical_assurance)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.TechnicalAssurances"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )


class WellboreTrajectoryDataList(TypeList[WellboreTrajectoryData]):
    _NODE = WellboreTrajectoryData

    def as_apply(self) -> WellboreTrajectoryDataApplyList:
        return WellboreTrajectoryDataApplyList([node.as_apply() for node in self.data])


class WellboreTrajectoryDataApplyList(TypeApplyList[WellboreTrajectoryDataApply]):
    _NODE = WellboreTrajectoryDataApply
