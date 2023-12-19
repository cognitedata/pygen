from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

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

if TYPE_CHECKING:
    from ._artefacts import Artefacts, ArtefactsApply
    from ._available_trajectory_station_properties import (
        AvailableTrajectoryStationProperties,
        AvailableTrajectoryStationPropertiesApply,
    )
    from ._geo_contexts import GeoContexts, GeoContextsApply
    from ._lineage_assertions import LineageAssertions, LineageAssertionsApply
    from ._name_aliases import NameAliases, NameAliasesApply
    from ._spatial_area import SpatialArea, SpatialAreaApply
    from ._spatial_point import SpatialPoint, SpatialPointApply
    from ._technical_assurances import TechnicalAssurances, TechnicalAssurancesApply
    from ._vertical_measurement import VerticalMeasurement, VerticalMeasurementApply


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
    """This represents the reading version of wellbore trajectory datum.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wellbore trajectory datum.
        acquisition_date: The acquisition date field.
        acquisition_remark: The acquisition remark field.
        active_indicator: The active indicator field.
        applied_operations: The applied operation field.
        applied_operations_date_time: The applied operations date time field.
        applied_operations_remarks: The applied operations remark field.
        applied_operations_user: The applied operations user field.
        artefacts: The artefact field.
        author_i_ds: The author i d field.
        available_trajectory_station_properties: The available trajectory station property field.
        azimuth_reference_type: The azimuth reference type field.
        base_depth_measured_depth: The base depth measured depth field.
        business_activities: The business activity field.
        calculation_method_type: The calculation method type field.
        company_id: The company id field.
        creation_date_time: The creation date time field.
        ddms_datasets: The ddms dataset field.
        datasets: The dataset field.
        description: The description field.
        end_date_time: The end date time field.
        existence_kind: The existence kind field.
        extrapolated_measured_depth: The extrapolated measured depth field.
        extrapolated_measured_depth_remark: The extrapolated measured depth remark field.
        geo_contexts: The geo context field.
        geographic_crsid: The geographic crsid field.
        is_discoverable: The is discoverable field.
        is_extended_load: The is extended load field.
        lineage_assertions: The lineage assertion field.
        name: The name field.
        name_aliases: The name alias field.
        projected_crsid: The projected crsid field.
        resource_curation_status: The resource curation status field.
        resource_home_region_id: The resource home region id field.
        resource_host_region_i_ds: The resource host region i d field.
        resource_lifecycle_status: The resource lifecycle status field.
        resource_security_classification: The resource security classification field.
        service_company_id: The service company id field.
        source: The source field.
        spatial_area: The spatial area field.
        spatial_point: The spatial point field.
        start_date_time: The start date time field.
        submitter_name: The submitter name field.
        surface_grid_convergence: The surface grid convergence field.
        surface_scale_factor: The surface scale factor field.
        survey_reference_identifier: The survey reference identifier field.
        survey_tool_type_id: The survey tool type id field.
        survey_type: The survey type field.
        survey_version: The survey version field.
        tags: The tag field.
        technical_assurances: The technical assurance field.
        tie_measured_depth: The tie measured depth field.
        tie_true_vertical_depth: The tie true vertical depth field.
        top_depth_measured_depth: The top depth measured depth field.
        tortuosity: The tortuosity field.
        vertical_measurement: The vertical measurement field.
        wellbore_id: The wellbore id field.
        created_time: The created time of the wellbore trajectory datum node.
        last_updated_time: The last updated time of the wellbore trajectory datum node.
        deleted_time: If present, the deleted time of the wellbore trajectory datum node.
        version: The version of the wellbore trajectory datum node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    acquisition_date: Optional[str] = Field(None, alias="AcquisitionDate")
    acquisition_remark: Optional[str] = Field(None, alias="AcquisitionRemark")
    active_indicator: Optional[bool] = Field(None, alias="ActiveIndicator")
    applied_operations: Optional[list[str]] = Field(None, alias="AppliedOperations")
    applied_operations_date_time: Optional[str] = Field(None, alias="AppliedOperationsDateTime")
    applied_operations_remarks: Optional[str] = Field(None, alias="AppliedOperationsRemarks")
    applied_operations_user: Optional[str] = Field(None, alias="AppliedOperationsUser")
    artefacts: Union[list[Artefacts], list[str], None] = Field(default=None, repr=False, alias="Artefacts")
    author_i_ds: Optional[list[str]] = Field(None, alias="AuthorIDs")
    available_trajectory_station_properties: Union[list[AvailableTrajectoryStationProperties], list[str], None] = Field(
        default=None, repr=False, alias="AvailableTrajectoryStationProperties"
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
    geo_contexts: Union[list[GeoContexts], list[str], None] = Field(default=None, repr=False, alias="GeoContexts")
    geographic_crsid: Optional[str] = Field(None, alias="GeographicCRSID")
    is_discoverable: Optional[bool] = Field(None, alias="IsDiscoverable")
    is_extended_load: Optional[bool] = Field(None, alias="IsExtendedLoad")
    lineage_assertions: Union[list[LineageAssertions], list[str], None] = Field(
        default=None, repr=False, alias="LineageAssertions"
    )
    name: Optional[str] = Field(None, alias="Name")
    name_aliases: Union[list[NameAliases], list[str], None] = Field(default=None, repr=False, alias="NameAliases")
    projected_crsid: Optional[str] = Field(None, alias="ProjectedCRSID")
    resource_curation_status: Optional[str] = Field(None, alias="ResourceCurationStatus")
    resource_home_region_id: Optional[str] = Field(None, alias="ResourceHomeRegionID")
    resource_host_region_i_ds: Optional[list[str]] = Field(None, alias="ResourceHostRegionIDs")
    resource_lifecycle_status: Optional[str] = Field(None, alias="ResourceLifecycleStatus")
    resource_security_classification: Optional[str] = Field(None, alias="ResourceSecurityClassification")
    service_company_id: Optional[str] = Field(None, alias="ServiceCompanyID")
    source: Optional[str] = Field(None, alias="Source")
    spatial_area: Union[SpatialArea, str, dm.NodeId, None] = Field(None, repr=False, alias="SpatialArea")
    spatial_point: Union[SpatialPoint, str, dm.NodeId, None] = Field(None, repr=False, alias="SpatialPoint")
    start_date_time: Optional[str] = Field(None, alias="StartDateTime")
    submitter_name: Optional[str] = Field(None, alias="SubmitterName")
    surface_grid_convergence: Optional[float] = Field(None, alias="SurfaceGridConvergence")
    surface_scale_factor: Optional[float] = Field(None, alias="SurfaceScaleFactor")
    survey_reference_identifier: Optional[str] = Field(None, alias="SurveyReferenceIdentifier")
    survey_tool_type_id: Optional[str] = Field(None, alias="SurveyToolTypeID")
    survey_type: Optional[str] = Field(None, alias="SurveyType")
    survey_version: Optional[str] = Field(None, alias="SurveyVersion")
    tags: Optional[list[str]] = Field(None, alias="Tags")
    technical_assurances: Union[list[TechnicalAssurances], list[str], None] = Field(
        default=None, repr=False, alias="TechnicalAssurances"
    )
    tie_measured_depth: Optional[int] = Field(None, alias="TieMeasuredDepth")
    tie_true_vertical_depth: Optional[int] = Field(None, alias="TieTrueVerticalDepth")
    top_depth_measured_depth: Optional[int] = Field(None, alias="TopDepthMeasuredDepth")
    tortuosity: Optional[float] = Field(None, alias="Tortuosity")
    vertical_measurement: Union[VerticalMeasurement, str, dm.NodeId, None] = Field(
        None, repr=False, alias="VerticalMeasurement"
    )
    wellbore_id: Optional[str] = Field(None, alias="WellboreID")

    def as_apply(self) -> WellboreTrajectoryDataApply:
        """Convert this read version of wellbore trajectory datum to the writing version."""
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
            artefacts=[
                artefact.as_apply() if isinstance(artefact, DomainModel) else artefact
                for artefact in self.artefacts or []
            ],
            author_i_ds=self.author_i_ds,
            available_trajectory_station_properties=[
                available_trajectory_station_property.as_apply()
                if isinstance(available_trajectory_station_property, DomainModel)
                else available_trajectory_station_property
                for available_trajectory_station_property in self.available_trajectory_station_properties or []
            ],
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
            geo_contexts=[
                geo_context.as_apply() if isinstance(geo_context, DomainModel) else geo_context
                for geo_context in self.geo_contexts or []
            ],
            geographic_crsid=self.geographic_crsid,
            is_discoverable=self.is_discoverable,
            is_extended_load=self.is_extended_load,
            lineage_assertions=[
                lineage_assertion.as_apply() if isinstance(lineage_assertion, DomainModel) else lineage_assertion
                for lineage_assertion in self.lineage_assertions or []
            ],
            name=self.name,
            name_aliases=[
                name_alias.as_apply() if isinstance(name_alias, DomainModel) else name_alias
                for name_alias in self.name_aliases or []
            ],
            projected_crsid=self.projected_crsid,
            resource_curation_status=self.resource_curation_status,
            resource_home_region_id=self.resource_home_region_id,
            resource_host_region_i_ds=self.resource_host_region_i_ds,
            resource_lifecycle_status=self.resource_lifecycle_status,
            resource_security_classification=self.resource_security_classification,
            service_company_id=self.service_company_id,
            source=self.source,
            spatial_area=self.spatial_area.as_apply()
            if isinstance(self.spatial_area, DomainModel)
            else self.spatial_area,
            spatial_point=self.spatial_point.as_apply()
            if isinstance(self.spatial_point, DomainModel)
            else self.spatial_point,
            start_date_time=self.start_date_time,
            submitter_name=self.submitter_name,
            surface_grid_convergence=self.surface_grid_convergence,
            surface_scale_factor=self.surface_scale_factor,
            survey_reference_identifier=self.survey_reference_identifier,
            survey_tool_type_id=self.survey_tool_type_id,
            survey_type=self.survey_type,
            survey_version=self.survey_version,
            tags=self.tags,
            technical_assurances=[
                technical_assurance.as_apply() if isinstance(technical_assurance, DomainModel) else technical_assurance
                for technical_assurance in self.technical_assurances or []
            ],
            tie_measured_depth=self.tie_measured_depth,
            tie_true_vertical_depth=self.tie_true_vertical_depth,
            top_depth_measured_depth=self.top_depth_measured_depth,
            tortuosity=self.tortuosity,
            vertical_measurement=self.vertical_measurement.as_apply()
            if isinstance(self.vertical_measurement, DomainModel)
            else self.vertical_measurement,
            wellbore_id=self.wellbore_id,
        )


class WellboreTrajectoryDataApply(DomainModelApply):
    """This represents the writing version of wellbore trajectory datum.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wellbore trajectory datum.
        acquisition_date: The acquisition date field.
        acquisition_remark: The acquisition remark field.
        active_indicator: The active indicator field.
        applied_operations: The applied operation field.
        applied_operations_date_time: The applied operations date time field.
        applied_operations_remarks: The applied operations remark field.
        applied_operations_user: The applied operations user field.
        artefacts: The artefact field.
        author_i_ds: The author i d field.
        available_trajectory_station_properties: The available trajectory station property field.
        azimuth_reference_type: The azimuth reference type field.
        base_depth_measured_depth: The base depth measured depth field.
        business_activities: The business activity field.
        calculation_method_type: The calculation method type field.
        company_id: The company id field.
        creation_date_time: The creation date time field.
        ddms_datasets: The ddms dataset field.
        datasets: The dataset field.
        description: The description field.
        end_date_time: The end date time field.
        existence_kind: The existence kind field.
        extrapolated_measured_depth: The extrapolated measured depth field.
        extrapolated_measured_depth_remark: The extrapolated measured depth remark field.
        geo_contexts: The geo context field.
        geographic_crsid: The geographic crsid field.
        is_discoverable: The is discoverable field.
        is_extended_load: The is extended load field.
        lineage_assertions: The lineage assertion field.
        name: The name field.
        name_aliases: The name alias field.
        projected_crsid: The projected crsid field.
        resource_curation_status: The resource curation status field.
        resource_home_region_id: The resource home region id field.
        resource_host_region_i_ds: The resource host region i d field.
        resource_lifecycle_status: The resource lifecycle status field.
        resource_security_classification: The resource security classification field.
        service_company_id: The service company id field.
        source: The source field.
        spatial_area: The spatial area field.
        spatial_point: The spatial point field.
        start_date_time: The start date time field.
        submitter_name: The submitter name field.
        surface_grid_convergence: The surface grid convergence field.
        surface_scale_factor: The surface scale factor field.
        survey_reference_identifier: The survey reference identifier field.
        survey_tool_type_id: The survey tool type id field.
        survey_type: The survey type field.
        survey_version: The survey version field.
        tags: The tag field.
        technical_assurances: The technical assurance field.
        tie_measured_depth: The tie measured depth field.
        tie_true_vertical_depth: The tie true vertical depth field.
        top_depth_measured_depth: The top depth measured depth field.
        tortuosity: The tortuosity field.
        vertical_measurement: The vertical measurement field.
        wellbore_id: The wellbore id field.
        existing_version: Fail the ingestion request if the wellbore trajectory datum version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
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
    spatial_area: Union[SpatialAreaApply, str, dm.NodeId, None] = Field(None, repr=False, alias="SpatialArea")
    spatial_point: Union[SpatialPointApply, str, dm.NodeId, None] = Field(None, repr=False, alias="SpatialPoint")
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
    vertical_measurement: Union[VerticalMeasurementApply, str, dm.NodeId, None] = Field(
        None, repr=False, alias="VerticalMeasurement"
    )
    wellbore_id: Optional[str] = Field(None, alias="WellboreID")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "WellboreTrajectoryData", "d35eace9691587"
        )

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
                "space": self.space if isinstance(self.spatial_area, str) else self.spatial_area.space,
                "externalId": self.spatial_area
                if isinstance(self.spatial_area, str)
                else self.spatial_area.external_id,
            }
        if self.spatial_point is not None:
            properties["SpatialPoint"] = {
                "space": self.space if isinstance(self.spatial_point, str) else self.spatial_point.space,
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
                "space": self.space if isinstance(self.vertical_measurement, str) else self.vertical_measurement.space,
                "externalId": self.vertical_measurement
                if isinstance(self.vertical_measurement, str)
                else self.vertical_measurement.external_id,
            }
        if self.wellbore_id is not None:
            properties["WellboreID"] = self.wellbore_id

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData"),
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.Artefacts")
        for artefact in self.artefacts or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=artefact, edge_type=edge_type, view_by_write_class=view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference(
            "IntegrationTestsImmutable", "WellboreTrajectoryData.AvailableTrajectoryStationProperties"
        )
        for available_trajectory_station_property in self.available_trajectory_station_properties or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=available_trajectory_station_property,
                edge_type=edge_type,
                view_by_write_class=view_by_write_class,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.GeoContexts")
        for geo_context in self.geo_contexts or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=geo_context,
                edge_type=edge_type,
                view_by_write_class=view_by_write_class,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.LineageAssertions")
        for lineage_assertion in self.lineage_assertions or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=lineage_assertion,
                edge_type=edge_type,
                view_by_write_class=view_by_write_class,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.NameAliases")
        for name_alias in self.name_aliases or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=name_alias,
                edge_type=edge_type,
                view_by_write_class=view_by_write_class,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference(
            "IntegrationTestsImmutable", "WellboreTrajectoryData.TechnicalAssurances"
        )
        for technical_assurance in self.technical_assurances or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=technical_assurance,
                edge_type=edge_type,
                view_by_write_class=view_by_write_class,
            )
            resources.extend(other_resources)

        if isinstance(self.spatial_area, DomainModelApply):
            other_resources = self.spatial_area._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.spatial_point, DomainModelApply):
            other_resources = self.spatial_point._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.vertical_measurement, DomainModelApply):
            other_resources = self.vertical_measurement._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class WellboreTrajectoryDataList(DomainModelList[WellboreTrajectoryData]):
    """List of wellbore trajectory data in the read version."""

    _INSTANCE = WellboreTrajectoryData

    def as_apply(self) -> WellboreTrajectoryDataApplyList:
        """Convert these read versions of wellbore trajectory datum to the writing versions."""
        return WellboreTrajectoryDataApplyList([node.as_apply() for node in self.data])


class WellboreTrajectoryDataApplyList(DomainModelApplyList[WellboreTrajectoryDataApply]):
    """List of wellbore trajectory data in the writing version."""

    _INSTANCE = WellboreTrajectoryDataApply


def _create_wellbore_trajectory_datum_filter(
    view_id: dm.ViewId,
    acquisition_date: str | list[str] | None = None,
    acquisition_date_prefix: str | None = None,
    acquisition_remark: str | list[str] | None = None,
    acquisition_remark_prefix: str | None = None,
    active_indicator: bool | None = None,
    applied_operations_date_time: str | list[str] | None = None,
    applied_operations_date_time_prefix: str | None = None,
    applied_operations_remarks: str | list[str] | None = None,
    applied_operations_remarks_prefix: str | None = None,
    applied_operations_user: str | list[str] | None = None,
    applied_operations_user_prefix: str | None = None,
    azimuth_reference_type: str | list[str] | None = None,
    azimuth_reference_type_prefix: str | None = None,
    min_base_depth_measured_depth: int | None = None,
    max_base_depth_measured_depth: int | None = None,
    calculation_method_type: str | list[str] | None = None,
    calculation_method_type_prefix: str | None = None,
    company_id: str | list[str] | None = None,
    company_id_prefix: str | None = None,
    creation_date_time: str | list[str] | None = None,
    creation_date_time_prefix: str | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    end_date_time: str | list[str] | None = None,
    end_date_time_prefix: str | None = None,
    existence_kind: str | list[str] | None = None,
    existence_kind_prefix: str | None = None,
    min_extrapolated_measured_depth: int | None = None,
    max_extrapolated_measured_depth: int | None = None,
    extrapolated_measured_depth_remark: str | list[str] | None = None,
    extrapolated_measured_depth_remark_prefix: str | None = None,
    geographic_crsid: str | list[str] | None = None,
    geographic_crsid_prefix: str | None = None,
    is_discoverable: bool | None = None,
    is_extended_load: bool | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    projected_crsid: str | list[str] | None = None,
    projected_crsid_prefix: str | None = None,
    resource_curation_status: str | list[str] | None = None,
    resource_curation_status_prefix: str | None = None,
    resource_home_region_id: str | list[str] | None = None,
    resource_home_region_id_prefix: str | None = None,
    resource_lifecycle_status: str | list[str] | None = None,
    resource_lifecycle_status_prefix: str | None = None,
    resource_security_classification: str | list[str] | None = None,
    resource_security_classification_prefix: str | None = None,
    service_company_id: str | list[str] | None = None,
    service_company_id_prefix: str | None = None,
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
    spatial_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    spatial_point: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    start_date_time: str | list[str] | None = None,
    start_date_time_prefix: str | None = None,
    submitter_name: str | list[str] | None = None,
    submitter_name_prefix: str | None = None,
    min_surface_grid_convergence: float | None = None,
    max_surface_grid_convergence: float | None = None,
    min_surface_scale_factor: float | None = None,
    max_surface_scale_factor: float | None = None,
    survey_reference_identifier: str | list[str] | None = None,
    survey_reference_identifier_prefix: str | None = None,
    survey_tool_type_id: str | list[str] | None = None,
    survey_tool_type_id_prefix: str | None = None,
    survey_type: str | list[str] | None = None,
    survey_type_prefix: str | None = None,
    survey_version: str | list[str] | None = None,
    survey_version_prefix: str | None = None,
    min_tie_measured_depth: int | None = None,
    max_tie_measured_depth: int | None = None,
    min_tie_true_vertical_depth: int | None = None,
    max_tie_true_vertical_depth: int | None = None,
    min_top_depth_measured_depth: int | None = None,
    max_top_depth_measured_depth: int | None = None,
    min_tortuosity: float | None = None,
    max_tortuosity: float | None = None,
    vertical_measurement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    wellbore_id: str | list[str] | None = None,
    wellbore_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if acquisition_date is not None and isinstance(acquisition_date, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("AcquisitionDate"), value=acquisition_date))
    if acquisition_date and isinstance(acquisition_date, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AcquisitionDate"), values=acquisition_date))
    if acquisition_date_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("AcquisitionDate"), value=acquisition_date_prefix))
    if acquisition_remark is not None and isinstance(acquisition_remark, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("AcquisitionRemark"), value=acquisition_remark))
    if acquisition_remark and isinstance(acquisition_remark, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AcquisitionRemark"), values=acquisition_remark))
    if acquisition_remark_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("AcquisitionRemark"), value=acquisition_remark_prefix))
    if active_indicator is not None and isinstance(active_indicator, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ActiveIndicator"), value=active_indicator))
    if applied_operations_date_time is not None and isinstance(applied_operations_date_time, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("AppliedOperationsDateTime"), value=applied_operations_date_time)
        )
    if applied_operations_date_time and isinstance(applied_operations_date_time, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("AppliedOperationsDateTime"), values=applied_operations_date_time)
        )
    if applied_operations_date_time_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("AppliedOperationsDateTime"), value=applied_operations_date_time_prefix
            )
        )
    if applied_operations_remarks is not None and isinstance(applied_operations_remarks, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("AppliedOperationsRemarks"), value=applied_operations_remarks)
        )
    if applied_operations_remarks and isinstance(applied_operations_remarks, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("AppliedOperationsRemarks"), values=applied_operations_remarks)
        )
    if applied_operations_remarks_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("AppliedOperationsRemarks"), value=applied_operations_remarks_prefix
            )
        )
    if applied_operations_user is not None and isinstance(applied_operations_user, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("AppliedOperationsUser"), value=applied_operations_user)
        )
    if applied_operations_user and isinstance(applied_operations_user, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AppliedOperationsUser"), values=applied_operations_user))
    if applied_operations_user_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("AppliedOperationsUser"), value=applied_operations_user_prefix)
        )
    if azimuth_reference_type is not None and isinstance(azimuth_reference_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("AzimuthReferenceType"), value=azimuth_reference_type))
    if azimuth_reference_type and isinstance(azimuth_reference_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("AzimuthReferenceType"), values=azimuth_reference_type))
    if azimuth_reference_type_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("AzimuthReferenceType"), value=azimuth_reference_type_prefix)
        )
    if min_base_depth_measured_depth or max_base_depth_measured_depth:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("BaseDepthMeasuredDepth"),
                gte=min_base_depth_measured_depth,
                lte=max_base_depth_measured_depth,
            )
        )
    if calculation_method_type is not None and isinstance(calculation_method_type, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("CalculationMethodType"), value=calculation_method_type)
        )
    if calculation_method_type and isinstance(calculation_method_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("CalculationMethodType"), values=calculation_method_type))
    if calculation_method_type_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("CalculationMethodType"), value=calculation_method_type_prefix)
        )
    if company_id is not None and isinstance(company_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("CompanyID"), value=company_id))
    if company_id and isinstance(company_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("CompanyID"), values=company_id))
    if company_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("CompanyID"), value=company_id_prefix))
    if creation_date_time is not None and isinstance(creation_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("CreationDateTime"), value=creation_date_time))
    if creation_date_time and isinstance(creation_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("CreationDateTime"), values=creation_date_time))
    if creation_date_time_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("CreationDateTime"), value=creation_date_time_prefix))
    if description is not None and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Description"), value=description_prefix))
    if end_date_time is not None and isinstance(end_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EndDateTime"), value=end_date_time))
    if end_date_time and isinstance(end_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EndDateTime"), values=end_date_time))
    if end_date_time_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("EndDateTime"), value=end_date_time_prefix))
    if existence_kind is not None and isinstance(existence_kind, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ExistenceKind"), value=existence_kind))
    if existence_kind and isinstance(existence_kind, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ExistenceKind"), values=existence_kind))
    if existence_kind_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ExistenceKind"), value=existence_kind_prefix))
    if min_extrapolated_measured_depth or max_extrapolated_measured_depth:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("ExtrapolatedMeasuredDepth"),
                gte=min_extrapolated_measured_depth,
                lte=max_extrapolated_measured_depth,
            )
        )
    if extrapolated_measured_depth_remark is not None and isinstance(extrapolated_measured_depth_remark, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("ExtrapolatedMeasuredDepthRemark"), value=extrapolated_measured_depth_remark
            )
        )
    if extrapolated_measured_depth_remark and isinstance(extrapolated_measured_depth_remark, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("ExtrapolatedMeasuredDepthRemark"), values=extrapolated_measured_depth_remark
            )
        )
    if extrapolated_measured_depth_remark_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("ExtrapolatedMeasuredDepthRemark"),
                value=extrapolated_measured_depth_remark_prefix,
            )
        )
    if geographic_crsid is not None and isinstance(geographic_crsid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("GeographicCRSID"), value=geographic_crsid))
    if geographic_crsid and isinstance(geographic_crsid, list):
        filters.append(dm.filters.In(view_id.as_property_ref("GeographicCRSID"), values=geographic_crsid))
    if geographic_crsid_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("GeographicCRSID"), value=geographic_crsid_prefix))
    if is_discoverable is not None and isinstance(is_discoverable, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("IsDiscoverable"), value=is_discoverable))
    if is_extended_load is not None and isinstance(is_extended_load, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("IsExtendedLoad"), value=is_extended_load))
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Name"), value=name_prefix))
    if projected_crsid is not None and isinstance(projected_crsid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ProjectedCRSID"), value=projected_crsid))
    if projected_crsid and isinstance(projected_crsid, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ProjectedCRSID"), values=projected_crsid))
    if projected_crsid_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ProjectedCRSID"), value=projected_crsid_prefix))
    if resource_curation_status is not None and isinstance(resource_curation_status, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("ResourceCurationStatus"), value=resource_curation_status)
        )
    if resource_curation_status and isinstance(resource_curation_status, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("ResourceCurationStatus"), values=resource_curation_status)
        )
    if resource_curation_status_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("ResourceCurationStatus"), value=resource_curation_status_prefix)
        )
    if resource_home_region_id is not None and isinstance(resource_home_region_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("ResourceHomeRegionID"), value=resource_home_region_id)
        )
    if resource_home_region_id and isinstance(resource_home_region_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ResourceHomeRegionID"), values=resource_home_region_id))
    if resource_home_region_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("ResourceHomeRegionID"), value=resource_home_region_id_prefix)
        )
    if resource_lifecycle_status is not None and isinstance(resource_lifecycle_status, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("ResourceLifecycleStatus"), value=resource_lifecycle_status)
        )
    if resource_lifecycle_status and isinstance(resource_lifecycle_status, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("ResourceLifecycleStatus"), values=resource_lifecycle_status)
        )
    if resource_lifecycle_status_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("ResourceLifecycleStatus"), value=resource_lifecycle_status_prefix
            )
        )
    if resource_security_classification is not None and isinstance(resource_security_classification, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("ResourceSecurityClassification"), value=resource_security_classification
            )
        )
    if resource_security_classification and isinstance(resource_security_classification, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("ResourceSecurityClassification"), values=resource_security_classification
            )
        )
    if resource_security_classification_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("ResourceSecurityClassification"), value=resource_security_classification_prefix
            )
        )
    if service_company_id is not None and isinstance(service_company_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ServiceCompanyID"), value=service_company_id))
    if service_company_id and isinstance(service_company_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ServiceCompanyID"), values=service_company_id))
    if service_company_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ServiceCompanyID"), value=service_company_id_prefix))
    if source is not None and isinstance(source, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Source"), value=source))
    if source and isinstance(source, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Source"), values=source))
    if source_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Source"), value=source_prefix))
    if spatial_area and isinstance(spatial_area, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialArea"),
                value={"space": "IntegrationTestsImmutable", "externalId": spatial_area},
            )
        )
    if spatial_area and isinstance(spatial_area, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialArea"), value={"space": spatial_area[0], "externalId": spatial_area[1]}
            )
        )
    if spatial_area and isinstance(spatial_area, list) and isinstance(spatial_area[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialArea"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in spatial_area],
            )
        )
    if spatial_area and isinstance(spatial_area, list) and isinstance(spatial_area[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialArea"),
                values=[{"space": item[0], "externalId": item[1]} for item in spatial_area],
            )
        )
    if spatial_point and isinstance(spatial_point, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialPoint"),
                value={"space": "IntegrationTestsImmutable", "externalId": spatial_point},
            )
        )
    if spatial_point and isinstance(spatial_point, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialPoint"),
                value={"space": spatial_point[0], "externalId": spatial_point[1]},
            )
        )
    if spatial_point and isinstance(spatial_point, list) and isinstance(spatial_point[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialPoint"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in spatial_point],
            )
        )
    if spatial_point and isinstance(spatial_point, list) and isinstance(spatial_point[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialPoint"),
                values=[{"space": item[0], "externalId": item[1]} for item in spatial_point],
            )
        )
    if start_date_time is not None and isinstance(start_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("StartDateTime"), value=start_date_time))
    if start_date_time and isinstance(start_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("StartDateTime"), values=start_date_time))
    if start_date_time_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("StartDateTime"), value=start_date_time_prefix))
    if submitter_name is not None and isinstance(submitter_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("SubmitterName"), value=submitter_name))
    if submitter_name and isinstance(submitter_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("SubmitterName"), values=submitter_name))
    if submitter_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("SubmitterName"), value=submitter_name_prefix))
    if min_surface_grid_convergence or max_surface_grid_convergence:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("SurfaceGridConvergence"),
                gte=min_surface_grid_convergence,
                lte=max_surface_grid_convergence,
            )
        )
    if min_surface_scale_factor or max_surface_scale_factor:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("SurfaceScaleFactor"),
                gte=min_surface_scale_factor,
                lte=max_surface_scale_factor,
            )
        )
    if survey_reference_identifier is not None and isinstance(survey_reference_identifier, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("SurveyReferenceIdentifier"), value=survey_reference_identifier)
        )
    if survey_reference_identifier and isinstance(survey_reference_identifier, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("SurveyReferenceIdentifier"), values=survey_reference_identifier)
        )
    if survey_reference_identifier_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("SurveyReferenceIdentifier"), value=survey_reference_identifier_prefix
            )
        )
    if survey_tool_type_id is not None and isinstance(survey_tool_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("SurveyToolTypeID"), value=survey_tool_type_id))
    if survey_tool_type_id and isinstance(survey_tool_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("SurveyToolTypeID"), values=survey_tool_type_id))
    if survey_tool_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("SurveyToolTypeID"), value=survey_tool_type_id_prefix))
    if survey_type is not None and isinstance(survey_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("SurveyType"), value=survey_type))
    if survey_type and isinstance(survey_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("SurveyType"), values=survey_type))
    if survey_type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("SurveyType"), value=survey_type_prefix))
    if survey_version is not None and isinstance(survey_version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("SurveyVersion"), value=survey_version))
    if survey_version and isinstance(survey_version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("SurveyVersion"), values=survey_version))
    if survey_version_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("SurveyVersion"), value=survey_version_prefix))
    if min_tie_measured_depth or max_tie_measured_depth:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("TieMeasuredDepth"), gte=min_tie_measured_depth, lte=max_tie_measured_depth
            )
        )
    if min_tie_true_vertical_depth or max_tie_true_vertical_depth:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("TieTrueVerticalDepth"),
                gte=min_tie_true_vertical_depth,
                lte=max_tie_true_vertical_depth,
            )
        )
    if min_top_depth_measured_depth or max_top_depth_measured_depth:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("TopDepthMeasuredDepth"),
                gte=min_top_depth_measured_depth,
                lte=max_top_depth_measured_depth,
            )
        )
    if min_tortuosity or max_tortuosity:
        filters.append(dm.filters.Range(view_id.as_property_ref("Tortuosity"), gte=min_tortuosity, lte=max_tortuosity))
    if vertical_measurement and isinstance(vertical_measurement, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalMeasurement"),
                value={"space": "IntegrationTestsImmutable", "externalId": vertical_measurement},
            )
        )
    if vertical_measurement and isinstance(vertical_measurement, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalMeasurement"),
                value={"space": vertical_measurement[0], "externalId": vertical_measurement[1]},
            )
        )
    if vertical_measurement and isinstance(vertical_measurement, list) and isinstance(vertical_measurement[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("VerticalMeasurement"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in vertical_measurement],
            )
        )
    if vertical_measurement and isinstance(vertical_measurement, list) and isinstance(vertical_measurement[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("VerticalMeasurement"),
                values=[{"space": item[0], "externalId": item[1]} for item in vertical_measurement],
            )
        )
    if wellbore_id is not None and isinstance(wellbore_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("WellboreID"), value=wellbore_id))
    if wellbore_id and isinstance(wellbore_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("WellboreID"), values=wellbore_id))
    if wellbore_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("WellboreID"), value=wellbore_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
