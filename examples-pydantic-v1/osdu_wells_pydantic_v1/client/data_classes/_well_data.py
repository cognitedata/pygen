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
    from ._facility_events import FacilityEvents, FacilityEventsApply
    from ._facility_operators import FacilityOperators, FacilityOperatorsApply
    from ._facility_specifications import FacilitySpecifications, FacilitySpecificationsApply
    from ._facility_states import FacilityStates, FacilityStatesApply
    from ._geo_contexts import GeoContexts, GeoContextsApply
    from ._historical_interests import HistoricalInterests, HistoricalInterestsApply
    from ._name_aliases import NameAliases, NameAliasesApply
    from ._spatial_location import SpatialLocation, SpatialLocationApply
    from ._technical_assurances import TechnicalAssurances, TechnicalAssurancesApply
    from ._vertical_measurements import VerticalMeasurements, VerticalMeasurementsApply


__all__ = ["WellData", "WellDataApply", "WellDataList", "WellDataApplyList", "WellDataFields", "WellDataTextFields"]


WellDataTextFields = Literal[
    "business_intention_id",
    "condition_id",
    "current_operator_id",
    "data_source_organisation_id",
    "default_vertical_crsid",
    "default_vertical_measurement_id",
    "existence_kind",
    "facility_description",
    "facility_id",
    "facility_name",
    "facility_type_id",
    "initial_operator_id",
    "interest_type_id",
    "operating_environment_id",
    "outcome_id",
    "resource_curation_status",
    "resource_home_region_id",
    "resource_host_region_i_ds",
    "resource_lifecycle_status",
    "resource_security_classification",
    "role_id",
    "source",
    "status_summary_id",
    "technical_assurance_type_id",
    "version_creation_reason",
]
WellDataFields = Literal[
    "business_intention_id",
    "condition_id",
    "current_operator_id",
    "data_source_organisation_id",
    "default_vertical_crsid",
    "default_vertical_measurement_id",
    "existence_kind",
    "facility_description",
    "facility_id",
    "facility_name",
    "facility_type_id",
    "initial_operator_id",
    "interest_type_id",
    "operating_environment_id",
    "outcome_id",
    "resource_curation_status",
    "resource_home_region_id",
    "resource_host_region_i_ds",
    "resource_lifecycle_status",
    "resource_security_classification",
    "role_id",
    "source",
    "status_summary_id",
    "technical_assurance_type_id",
    "version_creation_reason",
    "was_business_interest_financial_non_operated",
    "was_business_interest_financial_operated",
    "was_business_interest_obligatory",
    "was_business_interest_technical",
]

_WELLDATA_PROPERTIES_BY_FIELD = {
    "business_intention_id": "BusinessIntentionID",
    "condition_id": "ConditionID",
    "current_operator_id": "CurrentOperatorID",
    "data_source_organisation_id": "DataSourceOrganisationID",
    "default_vertical_crsid": "DefaultVerticalCRSID",
    "default_vertical_measurement_id": "DefaultVerticalMeasurementID",
    "existence_kind": "ExistenceKind",
    "facility_description": "FacilityDescription",
    "facility_id": "FacilityID",
    "facility_name": "FacilityName",
    "facility_type_id": "FacilityTypeID",
    "initial_operator_id": "InitialOperatorID",
    "interest_type_id": "InterestTypeID",
    "operating_environment_id": "OperatingEnvironmentID",
    "outcome_id": "OutcomeID",
    "resource_curation_status": "ResourceCurationStatus",
    "resource_home_region_id": "ResourceHomeRegionID",
    "resource_host_region_i_ds": "ResourceHostRegionIDs",
    "resource_lifecycle_status": "ResourceLifecycleStatus",
    "resource_security_classification": "ResourceSecurityClassification",
    "role_id": "RoleID",
    "source": "Source",
    "status_summary_id": "StatusSummaryID",
    "technical_assurance_type_id": "TechnicalAssuranceTypeID",
    "version_creation_reason": "VersionCreationReason",
    "was_business_interest_financial_non_operated": "WasBusinessInterestFinancialNonOperated",
    "was_business_interest_financial_operated": "WasBusinessInterestFinancialOperated",
    "was_business_interest_obligatory": "WasBusinessInterestObligatory",
    "was_business_interest_technical": "WasBusinessInterestTechnical",
}


class WellData(DomainModel):
    """This represents the reading version of well datum.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the well datum.
        business_intention_id: The business intention id field.
        condition_id: The condition id field.
        current_operator_id: The current operator id field.
        data_source_organisation_id: The data source organisation id field.
        default_vertical_crsid: The default vertical crsid field.
        default_vertical_measurement_id: The default vertical measurement id field.
        existence_kind: The existence kind field.
        facility_description: The facility description field.
        facility_events: The facility event field.
        facility_id: The facility id field.
        facility_name: The facility name field.
        facility_operators: The facility operator field.
        facility_specifications: The facility specification field.
        facility_states: The facility state field.
        facility_type_id: The facility type id field.
        geo_contexts: The geo context field.
        historical_interests: The historical interest field.
        initial_operator_id: The initial operator id field.
        interest_type_id: The interest type id field.
        name_aliases: The name alias field.
        operating_environment_id: The operating environment id field.
        outcome_id: The outcome id field.
        resource_curation_status: The resource curation status field.
        resource_home_region_id: The resource home region id field.
        resource_host_region_i_ds: The resource host region i d field.
        resource_lifecycle_status: The resource lifecycle status field.
        resource_security_classification: The resource security classification field.
        role_id: The role id field.
        source: The source field.
        spatial_location: The spatial location field.
        status_summary_id: The status summary id field.
        technical_assurance_type_id: The technical assurance type id field.
        technical_assurances: The technical assurance field.
        version_creation_reason: The version creation reason field.
        vertical_measurements: The vertical measurement field.
        was_business_interest_financial_non_operated: The was business interest financial non operated field.
        was_business_interest_financial_operated: The was business interest financial operated field.
        was_business_interest_obligatory: The was business interest obligatory field.
        was_business_interest_technical: The was business interest technical field.
        created_time: The created time of the well datum node.
        last_updated_time: The last updated time of the well datum node.
        deleted_time: If present, the deleted time of the well datum node.
        version: The version of the well datum node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    business_intention_id: Optional[str] = Field(None, alias="BusinessIntentionID")
    condition_id: Optional[str] = Field(None, alias="ConditionID")
    current_operator_id: Optional[str] = Field(None, alias="CurrentOperatorID")
    data_source_organisation_id: Optional[str] = Field(None, alias="DataSourceOrganisationID")
    default_vertical_crsid: Optional[str] = Field(None, alias="DefaultVerticalCRSID")
    default_vertical_measurement_id: Optional[str] = Field(None, alias="DefaultVerticalMeasurementID")
    existence_kind: Optional[str] = Field(None, alias="ExistenceKind")
    facility_description: Optional[str] = Field(None, alias="FacilityDescription")
    facility_events: Union[list[FacilityEvents], list[str], None] = Field(
        default=None, repr=False, alias="FacilityEvents"
    )
    facility_id: Optional[str] = Field(None, alias="FacilityID")
    facility_name: Optional[str] = Field(None, alias="FacilityName")
    facility_operators: Union[list[FacilityOperators], list[str], None] = Field(
        default=None, repr=False, alias="FacilityOperators"
    )
    facility_specifications: Union[list[FacilitySpecifications], list[str], None] = Field(
        default=None, repr=False, alias="FacilitySpecifications"
    )
    facility_states: Union[list[FacilityStates], list[str], None] = Field(
        default=None, repr=False, alias="FacilityStates"
    )
    facility_type_id: Optional[str] = Field(None, alias="FacilityTypeID")
    geo_contexts: Union[list[GeoContexts], list[str], None] = Field(default=None, repr=False, alias="GeoContexts")
    historical_interests: Union[list[HistoricalInterests], list[str], None] = Field(
        default=None, repr=False, alias="HistoricalInterests"
    )
    initial_operator_id: Optional[str] = Field(None, alias="InitialOperatorID")
    interest_type_id: Optional[str] = Field(None, alias="InterestTypeID")
    name_aliases: Union[list[NameAliases], list[str], None] = Field(default=None, repr=False, alias="NameAliases")
    operating_environment_id: Optional[str] = Field(None, alias="OperatingEnvironmentID")
    outcome_id: Optional[str] = Field(None, alias="OutcomeID")
    resource_curation_status: Optional[str] = Field(None, alias="ResourceCurationStatus")
    resource_home_region_id: Optional[str] = Field(None, alias="ResourceHomeRegionID")
    resource_host_region_i_ds: Optional[list[str]] = Field(None, alias="ResourceHostRegionIDs")
    resource_lifecycle_status: Optional[str] = Field(None, alias="ResourceLifecycleStatus")
    resource_security_classification: Optional[str] = Field(None, alias="ResourceSecurityClassification")
    role_id: Optional[str] = Field(None, alias="RoleID")
    source: Optional[str] = Field(None, alias="Source")
    spatial_location: Union[SpatialLocation, str, dm.NodeId, None] = Field(None, repr=False, alias="SpatialLocation")
    status_summary_id: Optional[str] = Field(None, alias="StatusSummaryID")
    technical_assurance_type_id: Optional[str] = Field(None, alias="TechnicalAssuranceTypeID")
    technical_assurances: Union[list[TechnicalAssurances], list[str], None] = Field(
        default=None, repr=False, alias="TechnicalAssurances"
    )
    version_creation_reason: Optional[str] = Field(None, alias="VersionCreationReason")
    vertical_measurements: Union[list[VerticalMeasurements], list[str], None] = Field(
        default=None, repr=False, alias="VerticalMeasurements"
    )
    was_business_interest_financial_non_operated: Optional[bool] = Field(
        None, alias="WasBusinessInterestFinancialNonOperated"
    )
    was_business_interest_financial_operated: Optional[bool] = Field(None, alias="WasBusinessInterestFinancialOperated")
    was_business_interest_obligatory: Optional[bool] = Field(None, alias="WasBusinessInterestObligatory")
    was_business_interest_technical: Optional[bool] = Field(None, alias="WasBusinessInterestTechnical")

    def as_apply(self) -> WellDataApply:
        """Convert this read version of well datum to the writing version."""
        return WellDataApply(
            space=self.space,
            external_id=self.external_id,
            business_intention_id=self.business_intention_id,
            condition_id=self.condition_id,
            current_operator_id=self.current_operator_id,
            data_source_organisation_id=self.data_source_organisation_id,
            default_vertical_crsid=self.default_vertical_crsid,
            default_vertical_measurement_id=self.default_vertical_measurement_id,
            existence_kind=self.existence_kind,
            facility_description=self.facility_description,
            facility_events=[
                facility_event.as_apply() if isinstance(facility_event, DomainModel) else facility_event
                for facility_event in self.facility_events or []
            ],
            facility_id=self.facility_id,
            facility_name=self.facility_name,
            facility_operators=[
                facility_operator.as_apply() if isinstance(facility_operator, DomainModel) else facility_operator
                for facility_operator in self.facility_operators or []
            ],
            facility_specifications=[
                facility_specification.as_apply()
                if isinstance(facility_specification, DomainModel)
                else facility_specification
                for facility_specification in self.facility_specifications or []
            ],
            facility_states=[
                facility_state.as_apply() if isinstance(facility_state, DomainModel) else facility_state
                for facility_state in self.facility_states or []
            ],
            facility_type_id=self.facility_type_id,
            geo_contexts=[
                geo_context.as_apply() if isinstance(geo_context, DomainModel) else geo_context
                for geo_context in self.geo_contexts or []
            ],
            historical_interests=[
                historical_interest.as_apply() if isinstance(historical_interest, DomainModel) else historical_interest
                for historical_interest in self.historical_interests or []
            ],
            initial_operator_id=self.initial_operator_id,
            interest_type_id=self.interest_type_id,
            name_aliases=[
                name_alias.as_apply() if isinstance(name_alias, DomainModel) else name_alias
                for name_alias in self.name_aliases or []
            ],
            operating_environment_id=self.operating_environment_id,
            outcome_id=self.outcome_id,
            resource_curation_status=self.resource_curation_status,
            resource_home_region_id=self.resource_home_region_id,
            resource_host_region_i_ds=self.resource_host_region_i_ds,
            resource_lifecycle_status=self.resource_lifecycle_status,
            resource_security_classification=self.resource_security_classification,
            role_id=self.role_id,
            source=self.source,
            spatial_location=self.spatial_location.as_apply()
            if isinstance(self.spatial_location, DomainModel)
            else self.spatial_location,
            status_summary_id=self.status_summary_id,
            technical_assurance_type_id=self.technical_assurance_type_id,
            technical_assurances=[
                technical_assurance.as_apply() if isinstance(technical_assurance, DomainModel) else technical_assurance
                for technical_assurance in self.technical_assurances or []
            ],
            version_creation_reason=self.version_creation_reason,
            vertical_measurements=[
                vertical_measurement.as_apply()
                if isinstance(vertical_measurement, DomainModel)
                else vertical_measurement
                for vertical_measurement in self.vertical_measurements or []
            ],
            was_business_interest_financial_non_operated=self.was_business_interest_financial_non_operated,
            was_business_interest_financial_operated=self.was_business_interest_financial_operated,
            was_business_interest_obligatory=self.was_business_interest_obligatory,
            was_business_interest_technical=self.was_business_interest_technical,
        )


class WellDataApply(DomainModelApply):
    """This represents the writing version of well datum.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the well datum.
        business_intention_id: The business intention id field.
        condition_id: The condition id field.
        current_operator_id: The current operator id field.
        data_source_organisation_id: The data source organisation id field.
        default_vertical_crsid: The default vertical crsid field.
        default_vertical_measurement_id: The default vertical measurement id field.
        existence_kind: The existence kind field.
        facility_description: The facility description field.
        facility_events: The facility event field.
        facility_id: The facility id field.
        facility_name: The facility name field.
        facility_operators: The facility operator field.
        facility_specifications: The facility specification field.
        facility_states: The facility state field.
        facility_type_id: The facility type id field.
        geo_contexts: The geo context field.
        historical_interests: The historical interest field.
        initial_operator_id: The initial operator id field.
        interest_type_id: The interest type id field.
        name_aliases: The name alias field.
        operating_environment_id: The operating environment id field.
        outcome_id: The outcome id field.
        resource_curation_status: The resource curation status field.
        resource_home_region_id: The resource home region id field.
        resource_host_region_i_ds: The resource host region i d field.
        resource_lifecycle_status: The resource lifecycle status field.
        resource_security_classification: The resource security classification field.
        role_id: The role id field.
        source: The source field.
        spatial_location: The spatial location field.
        status_summary_id: The status summary id field.
        technical_assurance_type_id: The technical assurance type id field.
        technical_assurances: The technical assurance field.
        version_creation_reason: The version creation reason field.
        vertical_measurements: The vertical measurement field.
        was_business_interest_financial_non_operated: The was business interest financial non operated field.
        was_business_interest_financial_operated: The was business interest financial operated field.
        was_business_interest_obligatory: The was business interest obligatory field.
        was_business_interest_technical: The was business interest technical field.
        existing_version: Fail the ingestion request if the well datum version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    business_intention_id: Optional[str] = Field(None, alias="BusinessIntentionID")
    condition_id: Optional[str] = Field(None, alias="ConditionID")
    current_operator_id: Optional[str] = Field(None, alias="CurrentOperatorID")
    data_source_organisation_id: Optional[str] = Field(None, alias="DataSourceOrganisationID")
    default_vertical_crsid: Optional[str] = Field(None, alias="DefaultVerticalCRSID")
    default_vertical_measurement_id: Optional[str] = Field(None, alias="DefaultVerticalMeasurementID")
    existence_kind: Optional[str] = Field(None, alias="ExistenceKind")
    facility_description: Optional[str] = Field(None, alias="FacilityDescription")
    facility_events: Union[list[FacilityEventsApply], list[str], None] = Field(
        default=None, repr=False, alias="FacilityEvents"
    )
    facility_id: Optional[str] = Field(None, alias="FacilityID")
    facility_name: Optional[str] = Field(None, alias="FacilityName")
    facility_operators: Union[list[FacilityOperatorsApply], list[str], None] = Field(
        default=None, repr=False, alias="FacilityOperators"
    )
    facility_specifications: Union[list[FacilitySpecificationsApply], list[str], None] = Field(
        default=None, repr=False, alias="FacilitySpecifications"
    )
    facility_states: Union[list[FacilityStatesApply], list[str], None] = Field(
        default=None, repr=False, alias="FacilityStates"
    )
    facility_type_id: Optional[str] = Field(None, alias="FacilityTypeID")
    geo_contexts: Union[list[GeoContextsApply], list[str], None] = Field(default=None, repr=False, alias="GeoContexts")
    historical_interests: Union[list[HistoricalInterestsApply], list[str], None] = Field(
        default=None, repr=False, alias="HistoricalInterests"
    )
    initial_operator_id: Optional[str] = Field(None, alias="InitialOperatorID")
    interest_type_id: Optional[str] = Field(None, alias="InterestTypeID")
    name_aliases: Union[list[NameAliasesApply], list[str], None] = Field(default=None, repr=False, alias="NameAliases")
    operating_environment_id: Optional[str] = Field(None, alias="OperatingEnvironmentID")
    outcome_id: Optional[str] = Field(None, alias="OutcomeID")
    resource_curation_status: Optional[str] = Field(None, alias="ResourceCurationStatus")
    resource_home_region_id: Optional[str] = Field(None, alias="ResourceHomeRegionID")
    resource_host_region_i_ds: Optional[list[str]] = Field(None, alias="ResourceHostRegionIDs")
    resource_lifecycle_status: Optional[str] = Field(None, alias="ResourceLifecycleStatus")
    resource_security_classification: Optional[str] = Field(None, alias="ResourceSecurityClassification")
    role_id: Optional[str] = Field(None, alias="RoleID")
    source: Optional[str] = Field(None, alias="Source")
    spatial_location: Union[SpatialLocationApply, str, dm.NodeId, None] = Field(
        None, repr=False, alias="SpatialLocation"
    )
    status_summary_id: Optional[str] = Field(None, alias="StatusSummaryID")
    technical_assurance_type_id: Optional[str] = Field(None, alias="TechnicalAssuranceTypeID")
    technical_assurances: Union[list[TechnicalAssurancesApply], list[str], None] = Field(
        default=None, repr=False, alias="TechnicalAssurances"
    )
    version_creation_reason: Optional[str] = Field(None, alias="VersionCreationReason")
    vertical_measurements: Union[list[VerticalMeasurementsApply], list[str], None] = Field(
        default=None, repr=False, alias="VerticalMeasurements"
    )
    was_business_interest_financial_non_operated: Optional[bool] = Field(
        None, alias="WasBusinessInterestFinancialNonOperated"
    )
    was_business_interest_financial_operated: Optional[bool] = Field(None, alias="WasBusinessInterestFinancialOperated")
    was_business_interest_obligatory: Optional[bool] = Field(None, alias="WasBusinessInterestObligatory")
    was_business_interest_technical: Optional[bool] = Field(None, alias="WasBusinessInterestTechnical")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "WellData", "ed82310421bd56"
        )

        properties = {}
        if self.business_intention_id is not None:
            properties["BusinessIntentionID"] = self.business_intention_id
        if self.condition_id is not None:
            properties["ConditionID"] = self.condition_id
        if self.current_operator_id is not None:
            properties["CurrentOperatorID"] = self.current_operator_id
        if self.data_source_organisation_id is not None:
            properties["DataSourceOrganisationID"] = self.data_source_organisation_id
        if self.default_vertical_crsid is not None:
            properties["DefaultVerticalCRSID"] = self.default_vertical_crsid
        if self.default_vertical_measurement_id is not None:
            properties["DefaultVerticalMeasurementID"] = self.default_vertical_measurement_id
        if self.existence_kind is not None:
            properties["ExistenceKind"] = self.existence_kind
        if self.facility_description is not None:
            properties["FacilityDescription"] = self.facility_description
        if self.facility_id is not None:
            properties["FacilityID"] = self.facility_id
        if self.facility_name is not None:
            properties["FacilityName"] = self.facility_name
        if self.facility_type_id is not None:
            properties["FacilityTypeID"] = self.facility_type_id
        if self.initial_operator_id is not None:
            properties["InitialOperatorID"] = self.initial_operator_id
        if self.interest_type_id is not None:
            properties["InterestTypeID"] = self.interest_type_id
        if self.operating_environment_id is not None:
            properties["OperatingEnvironmentID"] = self.operating_environment_id
        if self.outcome_id is not None:
            properties["OutcomeID"] = self.outcome_id
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
        if self.role_id is not None:
            properties["RoleID"] = self.role_id
        if self.source is not None:
            properties["Source"] = self.source
        if self.spatial_location is not None:
            properties["SpatialLocation"] = {
                "space": self.space if isinstance(self.spatial_location, str) else self.spatial_location.space,
                "externalId": self.spatial_location
                if isinstance(self.spatial_location, str)
                else self.spatial_location.external_id,
            }
        if self.status_summary_id is not None:
            properties["StatusSummaryID"] = self.status_summary_id
        if self.technical_assurance_type_id is not None:
            properties["TechnicalAssuranceTypeID"] = self.technical_assurance_type_id
        if self.version_creation_reason is not None:
            properties["VersionCreationReason"] = self.version_creation_reason
        if self.was_business_interest_financial_non_operated is not None:
            properties["WasBusinessInterestFinancialNonOperated"] = self.was_business_interest_financial_non_operated
        if self.was_business_interest_financial_operated is not None:
            properties["WasBusinessInterestFinancialOperated"] = self.was_business_interest_financial_operated
        if self.was_business_interest_obligatory is not None:
            properties["WasBusinessInterestObligatory"] = self.was_business_interest_obligatory
        if self.was_business_interest_technical is not None:
            properties["WasBusinessInterestTechnical"] = self.was_business_interest_technical

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

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.FacilityEvents")
        for facility_event in self.facility_events or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, facility_event, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.FacilityOperators")
        for facility_operator in self.facility_operators or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, facility_operator, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.FacilitySpecifications")
        for facility_specification in self.facility_specifications or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, facility_specification, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.FacilityStates")
        for facility_state in self.facility_states or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, facility_state, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.GeoContexts")
        for geo_context in self.geo_contexts or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, geo_context, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.HistoricalInterests")
        for historical_interest in self.historical_interests or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, historical_interest, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.NameAliases")
        for name_alias in self.name_aliases or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, name_alias, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.TechnicalAssurances")
        for technical_assurance in self.technical_assurances or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, technical_assurance, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.VerticalMeasurements")
        for vertical_measurement in self.vertical_measurements or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, vertical_measurement, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        if isinstance(self.spatial_location, DomainModelApply):
            other_resources = self.spatial_location._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class WellDataList(DomainModelList[WellData]):
    """List of well data in the read version."""

    _INSTANCE = WellData

    def as_apply(self) -> WellDataApplyList:
        """Convert these read versions of well datum to the writing versions."""
        return WellDataApplyList([node.as_apply() for node in self.data])


class WellDataApplyList(DomainModelApplyList[WellDataApply]):
    """List of well data in the writing version."""

    _INSTANCE = WellDataApply


def _create_well_datum_filter(
    view_id: dm.ViewId,
    business_intention_id: str | list[str] | None = None,
    business_intention_id_prefix: str | None = None,
    condition_id: str | list[str] | None = None,
    condition_id_prefix: str | None = None,
    current_operator_id: str | list[str] | None = None,
    current_operator_id_prefix: str | None = None,
    data_source_organisation_id: str | list[str] | None = None,
    data_source_organisation_id_prefix: str | None = None,
    default_vertical_crsid: str | list[str] | None = None,
    default_vertical_crsid_prefix: str | None = None,
    default_vertical_measurement_id: str | list[str] | None = None,
    default_vertical_measurement_id_prefix: str | None = None,
    existence_kind: str | list[str] | None = None,
    existence_kind_prefix: str | None = None,
    facility_description: str | list[str] | None = None,
    facility_description_prefix: str | None = None,
    facility_id: str | list[str] | None = None,
    facility_id_prefix: str | None = None,
    facility_name: str | list[str] | None = None,
    facility_name_prefix: str | None = None,
    facility_type_id: str | list[str] | None = None,
    facility_type_id_prefix: str | None = None,
    initial_operator_id: str | list[str] | None = None,
    initial_operator_id_prefix: str | None = None,
    interest_type_id: str | list[str] | None = None,
    interest_type_id_prefix: str | None = None,
    operating_environment_id: str | list[str] | None = None,
    operating_environment_id_prefix: str | None = None,
    outcome_id: str | list[str] | None = None,
    outcome_id_prefix: str | None = None,
    resource_curation_status: str | list[str] | None = None,
    resource_curation_status_prefix: str | None = None,
    resource_home_region_id: str | list[str] | None = None,
    resource_home_region_id_prefix: str | None = None,
    resource_lifecycle_status: str | list[str] | None = None,
    resource_lifecycle_status_prefix: str | None = None,
    resource_security_classification: str | list[str] | None = None,
    resource_security_classification_prefix: str | None = None,
    role_id: str | list[str] | None = None,
    role_id_prefix: str | None = None,
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
    spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    status_summary_id: str | list[str] | None = None,
    status_summary_id_prefix: str | None = None,
    technical_assurance_type_id: str | list[str] | None = None,
    technical_assurance_type_id_prefix: str | None = None,
    version_creation_reason: str | list[str] | None = None,
    version_creation_reason_prefix: str | None = None,
    was_business_interest_financial_non_operated: bool | None = None,
    was_business_interest_financial_operated: bool | None = None,
    was_business_interest_obligatory: bool | None = None,
    was_business_interest_technical: bool | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if business_intention_id is not None and isinstance(business_intention_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("BusinessIntentionID"), value=business_intention_id))
    if business_intention_id and isinstance(business_intention_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("BusinessIntentionID"), values=business_intention_id))
    if business_intention_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("BusinessIntentionID"), value=business_intention_id_prefix)
        )
    if condition_id is not None and isinstance(condition_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ConditionID"), value=condition_id))
    if condition_id and isinstance(condition_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ConditionID"), values=condition_id))
    if condition_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ConditionID"), value=condition_id_prefix))
    if current_operator_id is not None and isinstance(current_operator_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("CurrentOperatorID"), value=current_operator_id))
    if current_operator_id and isinstance(current_operator_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("CurrentOperatorID"), values=current_operator_id))
    if current_operator_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("CurrentOperatorID"), value=current_operator_id_prefix)
        )
    if data_source_organisation_id is not None and isinstance(data_source_organisation_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("DataSourceOrganisationID"), value=data_source_organisation_id)
        )
    if data_source_organisation_id and isinstance(data_source_organisation_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("DataSourceOrganisationID"), values=data_source_organisation_id)
        )
    if data_source_organisation_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("DataSourceOrganisationID"), value=data_source_organisation_id_prefix
            )
        )
    if default_vertical_crsid is not None and isinstance(default_vertical_crsid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("DefaultVerticalCRSID"), value=default_vertical_crsid))
    if default_vertical_crsid and isinstance(default_vertical_crsid, list):
        filters.append(dm.filters.In(view_id.as_property_ref("DefaultVerticalCRSID"), values=default_vertical_crsid))
    if default_vertical_crsid_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("DefaultVerticalCRSID"), value=default_vertical_crsid_prefix)
        )
    if default_vertical_measurement_id is not None and isinstance(default_vertical_measurement_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("DefaultVerticalMeasurementID"), value=default_vertical_measurement_id
            )
        )
    if default_vertical_measurement_id and isinstance(default_vertical_measurement_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("DefaultVerticalMeasurementID"), values=default_vertical_measurement_id
            )
        )
    if default_vertical_measurement_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("DefaultVerticalMeasurementID"), value=default_vertical_measurement_id_prefix
            )
        )
    if existence_kind is not None and isinstance(existence_kind, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ExistenceKind"), value=existence_kind))
    if existence_kind and isinstance(existence_kind, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ExistenceKind"), values=existence_kind))
    if existence_kind_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ExistenceKind"), value=existence_kind_prefix))
    if facility_description is not None and isinstance(facility_description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityDescription"), value=facility_description))
    if facility_description and isinstance(facility_description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityDescription"), values=facility_description))
    if facility_description_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("FacilityDescription"), value=facility_description_prefix)
        )
    if facility_id is not None and isinstance(facility_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityID"), value=facility_id))
    if facility_id and isinstance(facility_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityID"), values=facility_id))
    if facility_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("FacilityID"), value=facility_id_prefix))
    if facility_name is not None and isinstance(facility_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityName"), value=facility_name))
    if facility_name and isinstance(facility_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityName"), values=facility_name))
    if facility_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("FacilityName"), value=facility_name_prefix))
    if facility_type_id is not None and isinstance(facility_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FacilityTypeID"), value=facility_type_id))
    if facility_type_id and isinstance(facility_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FacilityTypeID"), values=facility_type_id))
    if facility_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("FacilityTypeID"), value=facility_type_id_prefix))
    if initial_operator_id is not None and isinstance(initial_operator_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("InitialOperatorID"), value=initial_operator_id))
    if initial_operator_id and isinstance(initial_operator_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("InitialOperatorID"), values=initial_operator_id))
    if initial_operator_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("InitialOperatorID"), value=initial_operator_id_prefix)
        )
    if interest_type_id is not None and isinstance(interest_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("InterestTypeID"), value=interest_type_id))
    if interest_type_id and isinstance(interest_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("InterestTypeID"), values=interest_type_id))
    if interest_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("InterestTypeID"), value=interest_type_id_prefix))
    if operating_environment_id is not None and isinstance(operating_environment_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("OperatingEnvironmentID"), value=operating_environment_id)
        )
    if operating_environment_id and isinstance(operating_environment_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("OperatingEnvironmentID"), values=operating_environment_id)
        )
    if operating_environment_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("OperatingEnvironmentID"), value=operating_environment_id_prefix)
        )
    if outcome_id is not None and isinstance(outcome_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("OutcomeID"), value=outcome_id))
    if outcome_id and isinstance(outcome_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("OutcomeID"), values=outcome_id))
    if outcome_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("OutcomeID"), value=outcome_id_prefix))
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
    if role_id is not None and isinstance(role_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("RoleID"), value=role_id))
    if role_id and isinstance(role_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("RoleID"), values=role_id))
    if role_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("RoleID"), value=role_id_prefix))
    if source is not None and isinstance(source, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Source"), value=source))
    if source and isinstance(source, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Source"), values=source))
    if source_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Source"), value=source_prefix))
    if spatial_location and isinstance(spatial_location, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialLocation"),
                value={"space": "IntegrationTestsImmutable", "externalId": spatial_location},
            )
        )
    if spatial_location and isinstance(spatial_location, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("SpatialLocation"),
                value={"space": spatial_location[0], "externalId": spatial_location[1]},
            )
        )
    if spatial_location and isinstance(spatial_location, list) and isinstance(spatial_location[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialLocation"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in spatial_location],
            )
        )
    if spatial_location and isinstance(spatial_location, list) and isinstance(spatial_location[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("SpatialLocation"),
                values=[{"space": item[0], "externalId": item[1]} for item in spatial_location],
            )
        )
    if status_summary_id is not None and isinstance(status_summary_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("StatusSummaryID"), value=status_summary_id))
    if status_summary_id and isinstance(status_summary_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("StatusSummaryID"), values=status_summary_id))
    if status_summary_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("StatusSummaryID"), value=status_summary_id_prefix))
    if technical_assurance_type_id is not None and isinstance(technical_assurance_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("TechnicalAssuranceTypeID"), value=technical_assurance_type_id)
        )
    if technical_assurance_type_id and isinstance(technical_assurance_type_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("TechnicalAssuranceTypeID"), values=technical_assurance_type_id)
        )
    if technical_assurance_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("TechnicalAssuranceTypeID"), value=technical_assurance_type_id_prefix
            )
        )
    if version_creation_reason is not None and isinstance(version_creation_reason, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("VersionCreationReason"), value=version_creation_reason)
        )
    if version_creation_reason and isinstance(version_creation_reason, list):
        filters.append(dm.filters.In(view_id.as_property_ref("VersionCreationReason"), values=version_creation_reason))
    if version_creation_reason_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("VersionCreationReason"), value=version_creation_reason_prefix)
        )
    if was_business_interest_financial_non_operated is not None and isinstance(
        was_business_interest_financial_non_operated, bool
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("WasBusinessInterestFinancialNonOperated"),
                value=was_business_interest_financial_non_operated,
            )
        )
    if was_business_interest_financial_operated is not None and isinstance(
        was_business_interest_financial_operated, bool
    ):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("WasBusinessInterestFinancialOperated"),
                value=was_business_interest_financial_operated,
            )
        )
    if was_business_interest_obligatory is not None and isinstance(was_business_interest_obligatory, bool):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("WasBusinessInterestObligatory"), value=was_business_interest_obligatory
            )
        )
    if was_business_interest_technical is not None and isinstance(was_business_interest_technical, bool):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("WasBusinessInterestTechnical"), value=was_business_interest_technical
            )
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
