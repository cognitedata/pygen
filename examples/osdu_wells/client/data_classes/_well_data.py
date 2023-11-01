from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._facility_events import FacilityEventsApply
    from ._facility_operators import FacilityOperatorsApply
    from ._facility_specifications import FacilitySpecificationsApply
    from ._facility_states import FacilityStatesApply
    from ._geo_contexts import GeoContextsApply
    from ._historical_interests import HistoricalInterestsApply
    from ._name_aliases import NameAliasesApply
    from ._spatial_location import SpatialLocationApply
    from ._technical_assurances import TechnicalAssurancesApply
    from ._vertical_measurements import VerticalMeasurementsApply

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
    space: str = "IntegrationTestsImmutable"
    business_intention_id: Optional[str] = Field(None, alias="BusinessIntentionID")
    condition_id: Optional[str] = Field(None, alias="ConditionID")
    current_operator_id: Optional[str] = Field(None, alias="CurrentOperatorID")
    data_source_organisation_id: Optional[str] = Field(None, alias="DataSourceOrganisationID")
    default_vertical_crsid: Optional[str] = Field(None, alias="DefaultVerticalCRSID")
    default_vertical_measurement_id: Optional[str] = Field(None, alias="DefaultVerticalMeasurementID")
    existence_kind: Optional[str] = Field(None, alias="ExistenceKind")
    facility_description: Optional[str] = Field(None, alias="FacilityDescription")
    facility_events: Optional[list[str]] = Field(None, alias="FacilityEvents")
    facility_id: Optional[str] = Field(None, alias="FacilityID")
    facility_name: Optional[str] = Field(None, alias="FacilityName")
    facility_operators: Optional[list[str]] = Field(None, alias="FacilityOperators")
    facility_specifications: Optional[list[str]] = Field(None, alias="FacilitySpecifications")
    facility_states: Optional[list[str]] = Field(None, alias="FacilityStates")
    facility_type_id: Optional[str] = Field(None, alias="FacilityTypeID")
    geo_contexts: Optional[list[str]] = Field(None, alias="GeoContexts")
    historical_interests: Optional[list[str]] = Field(None, alias="HistoricalInterests")
    initial_operator_id: Optional[str] = Field(None, alias="InitialOperatorID")
    interest_type_id: Optional[str] = Field(None, alias="InterestTypeID")
    name_aliases: Optional[list[str]] = Field(None, alias="NameAliases")
    operating_environment_id: Optional[str] = Field(None, alias="OperatingEnvironmentID")
    outcome_id: Optional[str] = Field(None, alias="OutcomeID")
    resource_curation_status: Optional[str] = Field(None, alias="ResourceCurationStatus")
    resource_home_region_id: Optional[str] = Field(None, alias="ResourceHomeRegionID")
    resource_host_region_i_ds: Optional[list[str]] = Field(None, alias="ResourceHostRegionIDs")
    resource_lifecycle_status: Optional[str] = Field(None, alias="ResourceLifecycleStatus")
    resource_security_classification: Optional[str] = Field(None, alias="ResourceSecurityClassification")
    role_id: Optional[str] = Field(None, alias="RoleID")
    source: Optional[str] = Field(None, alias="Source")
    spatial_location: Optional[str] = Field(None, alias="SpatialLocation")
    status_summary_id: Optional[str] = Field(None, alias="StatusSummaryID")
    technical_assurance_type_id: Optional[str] = Field(None, alias="TechnicalAssuranceTypeID")
    technical_assurances: Optional[list[str]] = Field(None, alias="TechnicalAssurances")
    version_creation_reason: Optional[str] = Field(None, alias="VersionCreationReason")
    vertical_measurements: Optional[list[str]] = Field(None, alias="VerticalMeasurements")
    was_business_interest_financial_non_operated: Optional[bool] = Field(
        None, alias="WasBusinessInterestFinancialNonOperated"
    )
    was_business_interest_financial_operated: Optional[bool] = Field(None, alias="WasBusinessInterestFinancialOperated")
    was_business_interest_obligatory: Optional[bool] = Field(None, alias="WasBusinessInterestObligatory")
    was_business_interest_technical: Optional[bool] = Field(None, alias="WasBusinessInterestTechnical")

    def as_apply(self) -> WellDataApply:
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
            facility_events=self.facility_events,
            facility_id=self.facility_id,
            facility_name=self.facility_name,
            facility_operators=self.facility_operators,
            facility_specifications=self.facility_specifications,
            facility_states=self.facility_states,
            facility_type_id=self.facility_type_id,
            geo_contexts=self.geo_contexts,
            historical_interests=self.historical_interests,
            initial_operator_id=self.initial_operator_id,
            interest_type_id=self.interest_type_id,
            name_aliases=self.name_aliases,
            operating_environment_id=self.operating_environment_id,
            outcome_id=self.outcome_id,
            resource_curation_status=self.resource_curation_status,
            resource_home_region_id=self.resource_home_region_id,
            resource_host_region_i_ds=self.resource_host_region_i_ds,
            resource_lifecycle_status=self.resource_lifecycle_status,
            resource_security_classification=self.resource_security_classification,
            role_id=self.role_id,
            source=self.source,
            spatial_location=self.spatial_location,
            status_summary_id=self.status_summary_id,
            technical_assurance_type_id=self.technical_assurance_type_id,
            technical_assurances=self.technical_assurances,
            version_creation_reason=self.version_creation_reason,
            vertical_measurements=self.vertical_measurements,
            was_business_interest_financial_non_operated=self.was_business_interest_financial_non_operated,
            was_business_interest_financial_operated=self.was_business_interest_financial_operated,
            was_business_interest_obligatory=self.was_business_interest_obligatory,
            was_business_interest_technical=self.was_business_interest_technical,
        )


class WellDataApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
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
    spatial_location: Union[SpatialLocationApply, str, None] = Field(None, repr=False, alias="SpatialLocation")
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

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

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
                "space": "IntegrationTestsImmutable",
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
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "WellData", "ed82310421bd56"),
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

        for facility_event in self.facility_events or []:
            edge = self._create_facility_event_edge(facility_event)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(facility_event, DomainModelApply):
                instances = facility_event._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for facility_operator in self.facility_operators or []:
            edge = self._create_facility_operator_edge(facility_operator)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(facility_operator, DomainModelApply):
                instances = facility_operator._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for facility_specification in self.facility_specifications or []:
            edge = self._create_facility_specification_edge(facility_specification)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(facility_specification, DomainModelApply):
                instances = facility_specification._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for facility_state in self.facility_states or []:
            edge = self._create_facility_state_edge(facility_state)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(facility_state, DomainModelApply):
                instances = facility_state._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for geo_context in self.geo_contexts or []:
            edge = self._create_geo_context_edge(geo_context)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(geo_context, DomainModelApply):
                instances = geo_context._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for historical_interest in self.historical_interests or []:
            edge = self._create_historical_interest_edge(historical_interest)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(historical_interest, DomainModelApply):
                instances = historical_interest._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for name_alias in self.name_aliases or []:
            edge = self._create_name_alias_edge(name_alias)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(name_alias, DomainModelApply):
                instances = name_alias._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for technical_assurance in self.technical_assurances or []:
            edge = self._create_technical_assurance_edge(technical_assurance)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(technical_assurance, DomainModelApply):
                instances = technical_assurance._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for vertical_measurement in self.vertical_measurements or []:
            edge = self._create_vertical_measurement_edge(vertical_measurement)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(vertical_measurement, DomainModelApply):
                instances = vertical_measurement._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.spatial_location, DomainModelApply):
            instances = self.spatial_location._to_instances_apply(cache, write_view)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_facility_event_edge(self, facility_event: Union[str, FacilityEventsApply]) -> dm.EdgeApply:
        if isinstance(facility_event, str):
            end_node_ext_id = facility_event
        elif isinstance(facility_event, DomainModelApply):
            end_node_ext_id = facility_event.external_id
        else:
            raise TypeError(f"Expected str or FacilityEventsApply, got {type(facility_event)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.FacilityEvents"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_facility_operator_edge(self, facility_operator: Union[str, FacilityOperatorsApply]) -> dm.EdgeApply:
        if isinstance(facility_operator, str):
            end_node_ext_id = facility_operator
        elif isinstance(facility_operator, DomainModelApply):
            end_node_ext_id = facility_operator.external_id
        else:
            raise TypeError(f"Expected str or FacilityOperatorsApply, got {type(facility_operator)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.FacilityOperators"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_facility_specification_edge(
        self, facility_specification: Union[str, FacilitySpecificationsApply]
    ) -> dm.EdgeApply:
        if isinstance(facility_specification, str):
            end_node_ext_id = facility_specification
        elif isinstance(facility_specification, DomainModelApply):
            end_node_ext_id = facility_specification.external_id
        else:
            raise TypeError(f"Expected str or FacilitySpecificationsApply, got {type(facility_specification)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.FacilitySpecifications"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_facility_state_edge(self, facility_state: Union[str, FacilityStatesApply]) -> dm.EdgeApply:
        if isinstance(facility_state, str):
            end_node_ext_id = facility_state
        elif isinstance(facility_state, DomainModelApply):
            end_node_ext_id = facility_state.external_id
        else:
            raise TypeError(f"Expected str or FacilityStatesApply, got {type(facility_state)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.FacilityStates"),
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
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.GeoContexts"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_historical_interest_edge(
        self, historical_interest: Union[str, HistoricalInterestsApply]
    ) -> dm.EdgeApply:
        if isinstance(historical_interest, str):
            end_node_ext_id = historical_interest
        elif isinstance(historical_interest, DomainModelApply):
            end_node_ext_id = historical_interest.external_id
        else:
            raise TypeError(f"Expected str or HistoricalInterestsApply, got {type(historical_interest)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.HistoricalInterests"),
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
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.NameAliases"),
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
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.TechnicalAssurances"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_vertical_measurement_edge(
        self, vertical_measurement: Union[str, VerticalMeasurementsApply]
    ) -> dm.EdgeApply:
        if isinstance(vertical_measurement, str):
            end_node_ext_id = vertical_measurement
        elif isinstance(vertical_measurement, DomainModelApply):
            end_node_ext_id = vertical_measurement.external_id
        else:
            raise TypeError(f"Expected str or VerticalMeasurementsApply, got {type(vertical_measurement)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "WellData.VerticalMeasurements"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )


class WellDataList(TypeList[WellData]):
    _NODE = WellData

    def as_apply(self) -> WellDataApplyList:
        return WellDataApplyList([node.as_apply() for node in self.data])


class WellDataApplyList(TypeApplyList[WellDataApply]):
    _NODE = WellDataApply
