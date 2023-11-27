from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    WellboreData,
    WellboreDataApply,
    WellboreDataFields,
    WellboreDataList,
    WellboreDataApplyList,
    WellboreDataTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._wellbore_data import (
    _WELLBOREDATA_PROPERTIES_BY_FIELD,
    _create_wellbore_datum_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .wellbore_data_drilling_reasons import WellboreDataDrillingReasonsAPI
from .wellbore_data_facility_events import WellboreDataFacilityEventsAPI
from .wellbore_data_facility_operators import WellboreDataFacilityOperatorsAPI
from .wellbore_data_facility_specifications import WellboreDataFacilitySpecificationsAPI
from .wellbore_data_facility_states import WellboreDataFacilityStatesAPI
from .wellbore_data_geo_contexts import WellboreDataGeoContextsAPI
from .wellbore_data_historical_interests import WellboreDataHistoricalInterestsAPI
from .wellbore_data_name_aliases import WellboreDataNameAliasesAPI
from .wellbore_data_technical_assurances import WellboreDataTechnicalAssurancesAPI
from .wellbore_data_vertical_measurements import WellboreDataVerticalMeasurementsAPI
from .wellbore_data_wellbore_costs import WellboreDataWellboreCostsAPI
from .wellbore_data_query import WellboreDataQueryAPI


class WellboreDataAPI(NodeAPI[WellboreData, WellboreDataApply, WellboreDataList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WellboreDataApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WellboreData,
            class_apply_type=WellboreDataApply,
            class_list=WellboreDataList,
            class_apply_list=WellboreDataApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.drilling_reasons_edge = WellboreDataDrillingReasonsAPI(client)
        self.facility_events_edge = WellboreDataFacilityEventsAPI(client)
        self.facility_operators_edge = WellboreDataFacilityOperatorsAPI(client)
        self.facility_specifications_edge = WellboreDataFacilitySpecificationsAPI(client)
        self.facility_states_edge = WellboreDataFacilityStatesAPI(client)
        self.geo_contexts_edge = WellboreDataGeoContextsAPI(client)
        self.historical_interests_edge = WellboreDataHistoricalInterestsAPI(client)
        self.name_aliases_edge = WellboreDataNameAliasesAPI(client)
        self.technical_assurances_edge = WellboreDataTechnicalAssurancesAPI(client)
        self.vertical_measurements_edge = WellboreDataVerticalMeasurementsAPI(client)
        self.wellbore_costs_edge = WellboreDataWellboreCostsAPI(client)

    def __call__(
        self,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
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
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> WellboreDataQueryAPI[WellboreDataList]:
        """Query starting at wellbore data.

        Args:
            business_intention_id: The business intention id to filter on.
            business_intention_id_prefix: The prefix of the business intention id to filter on.
            condition_id: The condition id to filter on.
            condition_id_prefix: The prefix of the condition id to filter on.
            current_operator_id: The current operator id to filter on.
            current_operator_id_prefix: The prefix of the current operator id to filter on.
            data_source_organisation_id: The data source organisation id to filter on.
            data_source_organisation_id_prefix: The prefix of the data source organisation id to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
            definitive_trajectory_id: The definitive trajectory id to filter on.
            definitive_trajectory_id_prefix: The prefix of the definitive trajectory id to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            facility_description: The facility description to filter on.
            facility_description_prefix: The prefix of the facility description to filter on.
            facility_id: The facility id to filter on.
            facility_id_prefix: The prefix of the facility id to filter on.
            facility_name: The facility name to filter on.
            facility_name_prefix: The prefix of the facility name to filter on.
            facility_type_id: The facility type id to filter on.
            facility_type_id_prefix: The prefix of the facility type id to filter on.
            fluid_direction_id: The fluid direction id to filter on.
            fluid_direction_id_prefix: The prefix of the fluid direction id to filter on.
            formation_name_at_total_depth: The formation name at total depth to filter on.
            formation_name_at_total_depth_prefix: The prefix of the formation name at total depth to filter on.
            geographic_bottom_hole_location: The geographic bottom hole location to filter on.
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            kick_off_wellbore: The kick off wellbore to filter on.
            kick_off_wellbore_prefix: The prefix of the kick off wellbore to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
            primary_product_type_id: The primary product type id to filter on.
            primary_product_type_id_prefix: The prefix of the primary product type id to filter on.
            projected_bottom_hole_location: The projected bottom hole location to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            secondary_product_type_id: The secondary product type id to filter on.
            secondary_product_type_id_prefix: The prefix of the secondary product type id to filter on.
            min_sequence_number: The minimum value of the sequence number to filter on.
            max_sequence_number: The maximum value of the sequence number to filter on.
            show_product_type_id: The show product type id to filter on.
            show_product_type_id_prefix: The prefix of the show product type id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            target_formation: The target formation to filter on.
            target_formation_prefix: The prefix of the target formation to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            tertiary_product_type_id: The tertiary product type id to filter on.
            tertiary_product_type_id_prefix: The prefix of the tertiary product type id to filter on.
            trajectory_type_id: The trajectory type id to filter on.
            trajectory_type_id_prefix: The prefix of the trajectory type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            well_id: The well id to filter on.
            well_id_prefix: The prefix of the well id to filter on.
            wellbore_reason_id: The wellbore reason id to filter on.
            wellbore_reason_id_prefix: The prefix of the wellbore reason id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for wellbore data.

        """
        filter_ = _create_wellbore_datum_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
            definitive_trajectory_id,
            definitive_trajectory_id_prefix,
            existence_kind,
            existence_kind_prefix,
            facility_description,
            facility_description_prefix,
            facility_id,
            facility_id_prefix,
            facility_name,
            facility_name_prefix,
            facility_type_id,
            facility_type_id_prefix,
            fluid_direction_id,
            fluid_direction_id_prefix,
            formation_name_at_total_depth,
            formation_name_at_total_depth_prefix,
            geographic_bottom_hole_location,
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            kick_off_wellbore,
            kick_off_wellbore_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
            primary_product_type_id,
            primary_product_type_id_prefix,
            projected_bottom_hole_location,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            role_id,
            role_id_prefix,
            secondary_product_type_id,
            secondary_product_type_id_prefix,
            min_sequence_number,
            max_sequence_number,
            show_product_type_id,
            show_product_type_id_prefix,
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            target_formation,
            target_formation_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            tertiary_product_type_id,
            tertiary_product_type_id_prefix,
            trajectory_type_id,
            trajectory_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            well_id,
            well_id_prefix,
            wellbore_reason_id,
            wellbore_reason_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            WellboreDataList,
            [
                QueryStep(
                    name="wellbore_datum",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_WELLBOREDATA_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=WellboreData,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return WellboreDataQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, wellbore_datum: WellboreDataApply | Sequence[WellboreDataApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) wellbore data.

        Note: This method iterates through all nodes and timeseries linked to wellbore_datum and creates them including the edges
        between the nodes. For example, if any of `drilling_reasons`, `facility_events`, `facility_operators`, `facility_specifications`, `facility_states`, `geo_contexts`, `historical_interests`, `name_aliases`, `technical_assurances`, `vertical_measurements` or `wellbore_costs` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            wellbore_datum: Wellbore datum or sequence of wellbore data to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new wellbore_datum:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import WellboreDataApply
                >>> client = OSDUClient()
                >>> wellbore_datum = WellboreDataApply(external_id="my_wellbore_datum", ...)
                >>> result = client.wellbore_data.apply(wellbore_datum)

        """
        return self._apply(wellbore_datum, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more wellbore datum.

        Args:
            external_id: External id of the wellbore datum to delete.
            space: The space where all the wellbore datum are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete wellbore_datum by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.wellbore_data.delete("my_wellbore_datum")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> WellboreData:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> WellboreDataList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> WellboreData | WellboreDataList:
        """Retrieve one or more wellbore data by id(s).

        Args:
            external_id: External id or list of external ids of the wellbore data.
            space: The space where all the wellbore data are located.

        Returns:
            The requested wellbore data.

        Examples:

            Retrieve wellbore_datum by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_datum = client.wellbore_data.retrieve("my_wellbore_datum")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_triple=[
                (
                    self.drilling_reasons_edge,
                    "drilling_reasons",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.DrillingReasons"),
                ),
                (
                    self.facility_events_edge,
                    "facility_events",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.FacilityEvents"),
                ),
                (
                    self.facility_operators_edge,
                    "facility_operators",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.FacilityOperators"),
                ),
                (
                    self.facility_specifications_edge,
                    "facility_specifications",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.FacilitySpecifications"),
                ),
                (
                    self.facility_states_edge,
                    "facility_states",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.FacilityStates"),
                ),
                (
                    self.geo_contexts_edge,
                    "geo_contexts",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.GeoContexts"),
                ),
                (
                    self.historical_interests_edge,
                    "historical_interests",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.HistoricalInterests"),
                ),
                (
                    self.name_aliases_edge,
                    "name_aliases",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.NameAliases"),
                ),
                (
                    self.technical_assurances_edge,
                    "technical_assurances",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.TechnicalAssurances"),
                ),
                (
                    self.vertical_measurements_edge,
                    "vertical_measurements",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.VerticalMeasurements"),
                ),
                (
                    self.wellbore_costs_edge,
                    "wellbore_costs",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.WellboreCosts"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: WellboreDataTextFields | Sequence[WellboreDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
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
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellboreDataList:
        """Search wellbore data

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            business_intention_id: The business intention id to filter on.
            business_intention_id_prefix: The prefix of the business intention id to filter on.
            condition_id: The condition id to filter on.
            condition_id_prefix: The prefix of the condition id to filter on.
            current_operator_id: The current operator id to filter on.
            current_operator_id_prefix: The prefix of the current operator id to filter on.
            data_source_organisation_id: The data source organisation id to filter on.
            data_source_organisation_id_prefix: The prefix of the data source organisation id to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
            definitive_trajectory_id: The definitive trajectory id to filter on.
            definitive_trajectory_id_prefix: The prefix of the definitive trajectory id to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            facility_description: The facility description to filter on.
            facility_description_prefix: The prefix of the facility description to filter on.
            facility_id: The facility id to filter on.
            facility_id_prefix: The prefix of the facility id to filter on.
            facility_name: The facility name to filter on.
            facility_name_prefix: The prefix of the facility name to filter on.
            facility_type_id: The facility type id to filter on.
            facility_type_id_prefix: The prefix of the facility type id to filter on.
            fluid_direction_id: The fluid direction id to filter on.
            fluid_direction_id_prefix: The prefix of the fluid direction id to filter on.
            formation_name_at_total_depth: The formation name at total depth to filter on.
            formation_name_at_total_depth_prefix: The prefix of the formation name at total depth to filter on.
            geographic_bottom_hole_location: The geographic bottom hole location to filter on.
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            kick_off_wellbore: The kick off wellbore to filter on.
            kick_off_wellbore_prefix: The prefix of the kick off wellbore to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
            primary_product_type_id: The primary product type id to filter on.
            primary_product_type_id_prefix: The prefix of the primary product type id to filter on.
            projected_bottom_hole_location: The projected bottom hole location to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            secondary_product_type_id: The secondary product type id to filter on.
            secondary_product_type_id_prefix: The prefix of the secondary product type id to filter on.
            min_sequence_number: The minimum value of the sequence number to filter on.
            max_sequence_number: The maximum value of the sequence number to filter on.
            show_product_type_id: The show product type id to filter on.
            show_product_type_id_prefix: The prefix of the show product type id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            target_formation: The target formation to filter on.
            target_formation_prefix: The prefix of the target formation to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            tertiary_product_type_id: The tertiary product type id to filter on.
            tertiary_product_type_id_prefix: The prefix of the tertiary product type id to filter on.
            trajectory_type_id: The trajectory type id to filter on.
            trajectory_type_id_prefix: The prefix of the trajectory type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            well_id: The well id to filter on.
            well_id_prefix: The prefix of the well id to filter on.
            wellbore_reason_id: The wellbore reason id to filter on.
            wellbore_reason_id_prefix: The prefix of the wellbore reason id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results wellbore data matching the query.

        Examples:

           Search for 'my_wellbore_datum' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_data = client.wellbore_data.search('my_wellbore_datum')

        """
        filter_ = _create_wellbore_datum_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
            definitive_trajectory_id,
            definitive_trajectory_id_prefix,
            existence_kind,
            existence_kind_prefix,
            facility_description,
            facility_description_prefix,
            facility_id,
            facility_id_prefix,
            facility_name,
            facility_name_prefix,
            facility_type_id,
            facility_type_id_prefix,
            fluid_direction_id,
            fluid_direction_id_prefix,
            formation_name_at_total_depth,
            formation_name_at_total_depth_prefix,
            geographic_bottom_hole_location,
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            kick_off_wellbore,
            kick_off_wellbore_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
            primary_product_type_id,
            primary_product_type_id_prefix,
            projected_bottom_hole_location,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            role_id,
            role_id_prefix,
            secondary_product_type_id,
            secondary_product_type_id_prefix,
            min_sequence_number,
            max_sequence_number,
            show_product_type_id,
            show_product_type_id_prefix,
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            target_formation,
            target_formation_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            tertiary_product_type_id,
            tertiary_product_type_id_prefix,
            trajectory_type_id,
            trajectory_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            well_id,
            well_id_prefix,
            wellbore_reason_id,
            wellbore_reason_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WELLBOREDATA_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellboreDataFields | Sequence[WellboreDataFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WellboreDataTextFields | Sequence[WellboreDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
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
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellboreDataFields | Sequence[WellboreDataFields] | None = None,
        group_by: WellboreDataFields | Sequence[WellboreDataFields] = None,
        query: str | None = None,
        search_properties: WellboreDataTextFields | Sequence[WellboreDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
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
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellboreDataFields | Sequence[WellboreDataFields] | None = None,
        group_by: WellboreDataFields | Sequence[WellboreDataFields] | None = None,
        query: str | None = None,
        search_property: WellboreDataTextFields | Sequence[WellboreDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
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
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across wellbore data

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            business_intention_id: The business intention id to filter on.
            business_intention_id_prefix: The prefix of the business intention id to filter on.
            condition_id: The condition id to filter on.
            condition_id_prefix: The prefix of the condition id to filter on.
            current_operator_id: The current operator id to filter on.
            current_operator_id_prefix: The prefix of the current operator id to filter on.
            data_source_organisation_id: The data source organisation id to filter on.
            data_source_organisation_id_prefix: The prefix of the data source organisation id to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
            definitive_trajectory_id: The definitive trajectory id to filter on.
            definitive_trajectory_id_prefix: The prefix of the definitive trajectory id to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            facility_description: The facility description to filter on.
            facility_description_prefix: The prefix of the facility description to filter on.
            facility_id: The facility id to filter on.
            facility_id_prefix: The prefix of the facility id to filter on.
            facility_name: The facility name to filter on.
            facility_name_prefix: The prefix of the facility name to filter on.
            facility_type_id: The facility type id to filter on.
            facility_type_id_prefix: The prefix of the facility type id to filter on.
            fluid_direction_id: The fluid direction id to filter on.
            fluid_direction_id_prefix: The prefix of the fluid direction id to filter on.
            formation_name_at_total_depth: The formation name at total depth to filter on.
            formation_name_at_total_depth_prefix: The prefix of the formation name at total depth to filter on.
            geographic_bottom_hole_location: The geographic bottom hole location to filter on.
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            kick_off_wellbore: The kick off wellbore to filter on.
            kick_off_wellbore_prefix: The prefix of the kick off wellbore to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
            primary_product_type_id: The primary product type id to filter on.
            primary_product_type_id_prefix: The prefix of the primary product type id to filter on.
            projected_bottom_hole_location: The projected bottom hole location to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            secondary_product_type_id: The secondary product type id to filter on.
            secondary_product_type_id_prefix: The prefix of the secondary product type id to filter on.
            min_sequence_number: The minimum value of the sequence number to filter on.
            max_sequence_number: The maximum value of the sequence number to filter on.
            show_product_type_id: The show product type id to filter on.
            show_product_type_id_prefix: The prefix of the show product type id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            target_formation: The target formation to filter on.
            target_formation_prefix: The prefix of the target formation to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            tertiary_product_type_id: The tertiary product type id to filter on.
            tertiary_product_type_id_prefix: The prefix of the tertiary product type id to filter on.
            trajectory_type_id: The trajectory type id to filter on.
            trajectory_type_id_prefix: The prefix of the trajectory type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            well_id: The well id to filter on.
            well_id_prefix: The prefix of the well id to filter on.
            wellbore_reason_id: The wellbore reason id to filter on.
            wellbore_reason_id_prefix: The prefix of the wellbore reason id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count wellbore data in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.wellbore_data.aggregate("count", space="my_space")

        """

        filter_ = _create_wellbore_datum_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
            definitive_trajectory_id,
            definitive_trajectory_id_prefix,
            existence_kind,
            existence_kind_prefix,
            facility_description,
            facility_description_prefix,
            facility_id,
            facility_id_prefix,
            facility_name,
            facility_name_prefix,
            facility_type_id,
            facility_type_id_prefix,
            fluid_direction_id,
            fluid_direction_id_prefix,
            formation_name_at_total_depth,
            formation_name_at_total_depth_prefix,
            geographic_bottom_hole_location,
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            kick_off_wellbore,
            kick_off_wellbore_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
            primary_product_type_id,
            primary_product_type_id_prefix,
            projected_bottom_hole_location,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            role_id,
            role_id_prefix,
            secondary_product_type_id,
            secondary_product_type_id_prefix,
            min_sequence_number,
            max_sequence_number,
            show_product_type_id,
            show_product_type_id_prefix,
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            target_formation,
            target_formation_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            tertiary_product_type_id,
            tertiary_product_type_id_prefix,
            trajectory_type_id,
            trajectory_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            well_id,
            well_id_prefix,
            wellbore_reason_id,
            wellbore_reason_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WELLBOREDATA_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WellboreDataFields,
        interval: float,
        query: str | None = None,
        search_property: WellboreDataTextFields | Sequence[WellboreDataTextFields] | None = None,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
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
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for wellbore data

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            business_intention_id: The business intention id to filter on.
            business_intention_id_prefix: The prefix of the business intention id to filter on.
            condition_id: The condition id to filter on.
            condition_id_prefix: The prefix of the condition id to filter on.
            current_operator_id: The current operator id to filter on.
            current_operator_id_prefix: The prefix of the current operator id to filter on.
            data_source_organisation_id: The data source organisation id to filter on.
            data_source_organisation_id_prefix: The prefix of the data source organisation id to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
            definitive_trajectory_id: The definitive trajectory id to filter on.
            definitive_trajectory_id_prefix: The prefix of the definitive trajectory id to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            facility_description: The facility description to filter on.
            facility_description_prefix: The prefix of the facility description to filter on.
            facility_id: The facility id to filter on.
            facility_id_prefix: The prefix of the facility id to filter on.
            facility_name: The facility name to filter on.
            facility_name_prefix: The prefix of the facility name to filter on.
            facility_type_id: The facility type id to filter on.
            facility_type_id_prefix: The prefix of the facility type id to filter on.
            fluid_direction_id: The fluid direction id to filter on.
            fluid_direction_id_prefix: The prefix of the fluid direction id to filter on.
            formation_name_at_total_depth: The formation name at total depth to filter on.
            formation_name_at_total_depth_prefix: The prefix of the formation name at total depth to filter on.
            geographic_bottom_hole_location: The geographic bottom hole location to filter on.
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            kick_off_wellbore: The kick off wellbore to filter on.
            kick_off_wellbore_prefix: The prefix of the kick off wellbore to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
            primary_product_type_id: The primary product type id to filter on.
            primary_product_type_id_prefix: The prefix of the primary product type id to filter on.
            projected_bottom_hole_location: The projected bottom hole location to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            secondary_product_type_id: The secondary product type id to filter on.
            secondary_product_type_id_prefix: The prefix of the secondary product type id to filter on.
            min_sequence_number: The minimum value of the sequence number to filter on.
            max_sequence_number: The maximum value of the sequence number to filter on.
            show_product_type_id: The show product type id to filter on.
            show_product_type_id_prefix: The prefix of the show product type id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            target_formation: The target formation to filter on.
            target_formation_prefix: The prefix of the target formation to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            tertiary_product_type_id: The tertiary product type id to filter on.
            tertiary_product_type_id_prefix: The prefix of the tertiary product type id to filter on.
            trajectory_type_id: The trajectory type id to filter on.
            trajectory_type_id_prefix: The prefix of the trajectory type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            well_id: The well id to filter on.
            well_id_prefix: The prefix of the well id to filter on.
            wellbore_reason_id: The wellbore reason id to filter on.
            wellbore_reason_id_prefix: The prefix of the wellbore reason id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_wellbore_datum_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
            definitive_trajectory_id,
            definitive_trajectory_id_prefix,
            existence_kind,
            existence_kind_prefix,
            facility_description,
            facility_description_prefix,
            facility_id,
            facility_id_prefix,
            facility_name,
            facility_name_prefix,
            facility_type_id,
            facility_type_id_prefix,
            fluid_direction_id,
            fluid_direction_id_prefix,
            formation_name_at_total_depth,
            formation_name_at_total_depth_prefix,
            geographic_bottom_hole_location,
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            kick_off_wellbore,
            kick_off_wellbore_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
            primary_product_type_id,
            primary_product_type_id_prefix,
            projected_bottom_hole_location,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            role_id,
            role_id_prefix,
            secondary_product_type_id,
            secondary_product_type_id_prefix,
            min_sequence_number,
            max_sequence_number,
            show_product_type_id,
            show_product_type_id_prefix,
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            target_formation,
            target_formation_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            tertiary_product_type_id,
            tertiary_product_type_id_prefix,
            trajectory_type_id,
            trajectory_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            well_id,
            well_id_prefix,
            wellbore_reason_id,
            wellbore_reason_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WELLBOREDATA_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        business_intention_id: str | list[str] | None = None,
        business_intention_id_prefix: str | None = None,
        condition_id: str | list[str] | None = None,
        condition_id_prefix: str | None = None,
        current_operator_id: str | list[str] | None = None,
        current_operator_id_prefix: str | None = None,
        data_source_organisation_id: str | list[str] | None = None,
        data_source_organisation_id_prefix: str | None = None,
        default_vertical_measurement_id: str | list[str] | None = None,
        default_vertical_measurement_id_prefix: str | None = None,
        definitive_trajectory_id: str | list[str] | None = None,
        definitive_trajectory_id_prefix: str | None = None,
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
        fluid_direction_id: str | list[str] | None = None,
        fluid_direction_id_prefix: str | None = None,
        formation_name_at_total_depth: str | list[str] | None = None,
        formation_name_at_total_depth_prefix: str | None = None,
        geographic_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        initial_operator_id: str | list[str] | None = None,
        initial_operator_id_prefix: str | None = None,
        interest_type_id: str | list[str] | None = None,
        interest_type_id_prefix: str | None = None,
        kick_off_wellbore: str | list[str] | None = None,
        kick_off_wellbore_prefix: str | None = None,
        operating_environment_id: str | list[str] | None = None,
        operating_environment_id_prefix: str | None = None,
        outcome_id: str | list[str] | None = None,
        outcome_id_prefix: str | None = None,
        primary_product_type_id: str | list[str] | None = None,
        primary_product_type_id_prefix: str | None = None,
        projected_bottom_hole_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        secondary_product_type_id: str | list[str] | None = None,
        secondary_product_type_id_prefix: str | None = None,
        min_sequence_number: int | None = None,
        max_sequence_number: int | None = None,
        show_product_type_id: str | list[str] | None = None,
        show_product_type_id_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        spatial_location: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        status_summary_id: str | list[str] | None = None,
        status_summary_id_prefix: str | None = None,
        target_formation: str | list[str] | None = None,
        target_formation_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        tertiary_product_type_id: str | list[str] | None = None,
        tertiary_product_type_id_prefix: str | None = None,
        trajectory_type_id: str | list[str] | None = None,
        trajectory_type_id_prefix: str | None = None,
        version_creation_reason: str | list[str] | None = None,
        version_creation_reason_prefix: str | None = None,
        was_business_interest_financial_non_operated: bool | None = None,
        was_business_interest_financial_operated: bool | None = None,
        was_business_interest_obligatory: bool | None = None,
        was_business_interest_technical: bool | None = None,
        well_id: str | list[str] | None = None,
        well_id_prefix: str | None = None,
        wellbore_reason_id: str | list[str] | None = None,
        wellbore_reason_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WellboreDataList:
        """List/filter wellbore data

        Args:
            business_intention_id: The business intention id to filter on.
            business_intention_id_prefix: The prefix of the business intention id to filter on.
            condition_id: The condition id to filter on.
            condition_id_prefix: The prefix of the condition id to filter on.
            current_operator_id: The current operator id to filter on.
            current_operator_id_prefix: The prefix of the current operator id to filter on.
            data_source_organisation_id: The data source organisation id to filter on.
            data_source_organisation_id_prefix: The prefix of the data source organisation id to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
            definitive_trajectory_id: The definitive trajectory id to filter on.
            definitive_trajectory_id_prefix: The prefix of the definitive trajectory id to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            facility_description: The facility description to filter on.
            facility_description_prefix: The prefix of the facility description to filter on.
            facility_id: The facility id to filter on.
            facility_id_prefix: The prefix of the facility id to filter on.
            facility_name: The facility name to filter on.
            facility_name_prefix: The prefix of the facility name to filter on.
            facility_type_id: The facility type id to filter on.
            facility_type_id_prefix: The prefix of the facility type id to filter on.
            fluid_direction_id: The fluid direction id to filter on.
            fluid_direction_id_prefix: The prefix of the fluid direction id to filter on.
            formation_name_at_total_depth: The formation name at total depth to filter on.
            formation_name_at_total_depth_prefix: The prefix of the formation name at total depth to filter on.
            geographic_bottom_hole_location: The geographic bottom hole location to filter on.
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            kick_off_wellbore: The kick off wellbore to filter on.
            kick_off_wellbore_prefix: The prefix of the kick off wellbore to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
            primary_product_type_id: The primary product type id to filter on.
            primary_product_type_id_prefix: The prefix of the primary product type id to filter on.
            projected_bottom_hole_location: The projected bottom hole location to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            role_id: The role id to filter on.
            role_id_prefix: The prefix of the role id to filter on.
            secondary_product_type_id: The secondary product type id to filter on.
            secondary_product_type_id_prefix: The prefix of the secondary product type id to filter on.
            min_sequence_number: The minimum value of the sequence number to filter on.
            max_sequence_number: The maximum value of the sequence number to filter on.
            show_product_type_id: The show product type id to filter on.
            show_product_type_id_prefix: The prefix of the show product type id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            target_formation: The target formation to filter on.
            target_formation_prefix: The prefix of the target formation to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            tertiary_product_type_id: The tertiary product type id to filter on.
            tertiary_product_type_id_prefix: The prefix of the tertiary product type id to filter on.
            trajectory_type_id: The trajectory type id to filter on.
            trajectory_type_id_prefix: The prefix of the trajectory type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            well_id: The well id to filter on.
            well_id_prefix: The prefix of the well id to filter on.
            wellbore_reason_id: The wellbore reason id to filter on.
            wellbore_reason_id_prefix: The prefix of the wellbore reason id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `drilling_reasons`, `facility_events`, `facility_operators`, `facility_specifications`, `facility_states`, `geo_contexts`, `historical_interests`, `name_aliases`, `technical_assurances`, `vertical_measurements` or `wellbore_costs` external ids for the wellbore data. Defaults to True.

        Returns:
            List of requested wellbore data

        Examples:

            List wellbore data and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_data = client.wellbore_data.list(limit=5)

        """
        filter_ = _create_wellbore_datum_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
            definitive_trajectory_id,
            definitive_trajectory_id_prefix,
            existence_kind,
            existence_kind_prefix,
            facility_description,
            facility_description_prefix,
            facility_id,
            facility_id_prefix,
            facility_name,
            facility_name_prefix,
            facility_type_id,
            facility_type_id_prefix,
            fluid_direction_id,
            fluid_direction_id_prefix,
            formation_name_at_total_depth,
            formation_name_at_total_depth_prefix,
            geographic_bottom_hole_location,
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            kick_off_wellbore,
            kick_off_wellbore_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
            primary_product_type_id,
            primary_product_type_id_prefix,
            projected_bottom_hole_location,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            role_id,
            role_id_prefix,
            secondary_product_type_id,
            secondary_product_type_id_prefix,
            min_sequence_number,
            max_sequence_number,
            show_product_type_id,
            show_product_type_id_prefix,
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            target_formation,
            target_formation_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            tertiary_product_type_id,
            tertiary_product_type_id_prefix,
            trajectory_type_id,
            trajectory_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            well_id,
            well_id_prefix,
            wellbore_reason_id,
            wellbore_reason_id_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_triple=[
                (
                    self.drilling_reasons_edge,
                    "drilling_reasons",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.DrillingReasons"),
                ),
                (
                    self.facility_events_edge,
                    "facility_events",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.FacilityEvents"),
                ),
                (
                    self.facility_operators_edge,
                    "facility_operators",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.FacilityOperators"),
                ),
                (
                    self.facility_specifications_edge,
                    "facility_specifications",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.FacilitySpecifications"),
                ),
                (
                    self.facility_states_edge,
                    "facility_states",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.FacilityStates"),
                ),
                (
                    self.geo_contexts_edge,
                    "geo_contexts",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.GeoContexts"),
                ),
                (
                    self.historical_interests_edge,
                    "historical_interests",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.HistoricalInterests"),
                ),
                (
                    self.name_aliases_edge,
                    "name_aliases",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.NameAliases"),
                ),
                (
                    self.technical_assurances_edge,
                    "technical_assurances",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.TechnicalAssurances"),
                ),
                (
                    self.vertical_measurements_edge,
                    "vertical_measurements",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.VerticalMeasurements"),
                ),
                (
                    self.wellbore_costs_edge,
                    "wellbore_costs",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreData.WellboreCosts"),
                ),
            ],
        )
