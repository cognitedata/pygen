from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    WellData,
    WellDataApply,
    WellDataFields,
    WellDataList,
    WellDataApplyList,
    WellDataTextFields,
)
from osdu_wells.client.data_classes._well_data import (
    _WELLDATA_PROPERTIES_BY_FIELD,
    _create_well_datum_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .well_data_facility_events import WellDataFacilityEventsAPI
from .well_data_facility_operators import WellDataFacilityOperatorsAPI
from .well_data_facility_specifications import WellDataFacilitySpecificationsAPI
from .well_data_facility_states import WellDataFacilityStatesAPI
from .well_data_geo_contexts import WellDataGeoContextsAPI
from .well_data_historical_interests import WellDataHistoricalInterestsAPI
from .well_data_name_aliases import WellDataNameAliasesAPI
from .well_data_technical_assurances import WellDataTechnicalAssurancesAPI
from .well_data_vertical_measurements import WellDataVerticalMeasurementsAPI
from .well_data_query import WellDataQueryAPI


class WellDataAPI(NodeAPI[WellData, WellDataApply, WellDataList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WellDataApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WellData,
            class_apply_type=WellDataApply,
            class_list=WellDataList,
            class_apply_list=WellDataApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.facility_events_edge = WellDataFacilityEventsAPI(client)
        self.facility_operators_edge = WellDataFacilityOperatorsAPI(client)
        self.facility_specifications_edge = WellDataFacilitySpecificationsAPI(client)
        self.facility_states_edge = WellDataFacilityStatesAPI(client)
        self.geo_contexts_edge = WellDataGeoContextsAPI(client)
        self.historical_interests_edge = WellDataHistoricalInterestsAPI(client)
        self.name_aliases_edge = WellDataNameAliasesAPI(client)
        self.technical_assurances_edge = WellDataTechnicalAssurancesAPI(client)
        self.vertical_measurements_edge = WellDataVerticalMeasurementsAPI(client)

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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellDataQueryAPI[WellDataList]:
        """Query starting at well data.

        Args:
            business_intention_id: The business intention id to filter on.
            business_intention_id_prefix: The prefix of the business intention id to filter on.
            condition_id: The condition id to filter on.
            condition_id_prefix: The prefix of the condition id to filter on.
            current_operator_id: The current operator id to filter on.
            current_operator_id_prefix: The prefix of the current operator id to filter on.
            data_source_organisation_id: The data source organisation id to filter on.
            data_source_organisation_id_prefix: The prefix of the data source organisation id to filter on.
            default_vertical_crsid: The default vertical crsid to filter on.
            default_vertical_crsid_prefix: The prefix of the default vertical crsid to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
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
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
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
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of well data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for well data.

        """
        filter_ = _create_well_datum_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_crsid,
            default_vertical_crsid_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
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
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
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
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            WellDataList,
            [
                QueryStep(
                    name="well_datum",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_WELLDATA_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=WellData,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return WellDataQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, well_datum: WellDataApply | Sequence[WellDataApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) well data.

        Note: This method iterates through all nodes and timeseries linked to well_datum and creates them including the edges
        between the nodes. For example, if any of `facility_events`, `facility_operators`, `facility_specifications`, `facility_states`, `geo_contexts`, `historical_interests`, `name_aliases`, `technical_assurances` or `vertical_measurements` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            well_datum: Well datum or sequence of well data to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new well_datum:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import WellDataApply
                >>> client = OSDUClient()
                >>> well_datum = WellDataApply(external_id="my_well_datum", ...)
                >>> result = client.well_data.apply(well_datum)

        """
        return self._apply(well_datum, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more well datum.

        Args:
            external_id: External id of the well datum to delete.
            space: The space where all the well datum are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete well_datum by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.well_data.delete("my_well_datum")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> WellData:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> WellDataList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> WellData | WellDataList:
        """Retrieve one or more well data by id(s).

        Args:
            external_id: External id or list of external ids of the well data.
            space: The space where all the well data are located.

        Returns:
            The requested well data.

        Examples:

            Retrieve well_datum by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> well_datum = client.well_data.retrieve("my_well_datum")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_pairs=[
                (self.facility_events_edge, "facility_events"),
                (self.facility_operators_edge, "facility_operators"),
                (self.facility_specifications_edge, "facility_specifications"),
                (self.facility_states_edge, "facility_states"),
                (self.geo_contexts_edge, "geo_contexts"),
                (self.historical_interests_edge, "historical_interests"),
                (self.name_aliases_edge, "name_aliases"),
                (self.technical_assurances_edge, "technical_assurances"),
                (self.vertical_measurements_edge, "vertical_measurements"),
            ],
        )

    def search(
        self,
        query: str,
        properties: WellDataTextFields | Sequence[WellDataTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellDataList:
        """Search well data

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
            default_vertical_crsid: The default vertical crsid to filter on.
            default_vertical_crsid_prefix: The prefix of the default vertical crsid to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
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
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
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
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of well data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results well data matching the query.

        Examples:

           Search for 'my_well_datum' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> well_data = client.well_data.search('my_well_datum')

        """
        filter_ = _create_well_datum_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_crsid,
            default_vertical_crsid_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
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
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
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
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WELLDATA_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellDataFields | Sequence[WellDataFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WellDataTextFields | Sequence[WellDataTextFields] | None = None,
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
        property: WellDataFields | Sequence[WellDataFields] | None = None,
        group_by: WellDataFields | Sequence[WellDataFields] = None,
        query: str | None = None,
        search_properties: WellDataTextFields | Sequence[WellDataTextFields] | None = None,
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
        property: WellDataFields | Sequence[WellDataFields] | None = None,
        group_by: WellDataFields | Sequence[WellDataFields] | None = None,
        query: str | None = None,
        search_property: WellDataTextFields | Sequence[WellDataTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across well data

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
            default_vertical_crsid: The default vertical crsid to filter on.
            default_vertical_crsid_prefix: The prefix of the default vertical crsid to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
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
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
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
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of well data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count well data in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.well_data.aggregate("count", space="my_space")

        """

        filter_ = _create_well_datum_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_crsid,
            default_vertical_crsid_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
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
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
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
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WELLDATA_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WellDataFields,
        interval: float,
        query: str | None = None,
        search_property: WellDataTextFields | Sequence[WellDataTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for well data

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
            default_vertical_crsid: The default vertical crsid to filter on.
            default_vertical_crsid_prefix: The prefix of the default vertical crsid to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
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
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
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
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of well data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_well_datum_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_crsid,
            default_vertical_crsid_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
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
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
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
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WELLDATA_PROPERTIES_BY_FIELD,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WellDataList:
        """List/filter well data

        Args:
            business_intention_id: The business intention id to filter on.
            business_intention_id_prefix: The prefix of the business intention id to filter on.
            condition_id: The condition id to filter on.
            condition_id_prefix: The prefix of the condition id to filter on.
            current_operator_id: The current operator id to filter on.
            current_operator_id_prefix: The prefix of the current operator id to filter on.
            data_source_organisation_id: The data source organisation id to filter on.
            data_source_organisation_id_prefix: The prefix of the data source organisation id to filter on.
            default_vertical_crsid: The default vertical crsid to filter on.
            default_vertical_crsid_prefix: The prefix of the default vertical crsid to filter on.
            default_vertical_measurement_id: The default vertical measurement id to filter on.
            default_vertical_measurement_id_prefix: The prefix of the default vertical measurement id to filter on.
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
            initial_operator_id: The initial operator id to filter on.
            initial_operator_id_prefix: The prefix of the initial operator id to filter on.
            interest_type_id: The interest type id to filter on.
            interest_type_id_prefix: The prefix of the interest type id to filter on.
            operating_environment_id: The operating environment id to filter on.
            operating_environment_id_prefix: The prefix of the operating environment id to filter on.
            outcome_id: The outcome id to filter on.
            outcome_id_prefix: The prefix of the outcome id to filter on.
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
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_location: The spatial location to filter on.
            status_summary_id: The status summary id to filter on.
            status_summary_id_prefix: The prefix of the status summary id to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            version_creation_reason: The version creation reason to filter on.
            version_creation_reason_prefix: The prefix of the version creation reason to filter on.
            was_business_interest_financial_non_operated: The was business interest financial non operated to filter on.
            was_business_interest_financial_operated: The was business interest financial operated to filter on.
            was_business_interest_obligatory: The was business interest obligatory to filter on.
            was_business_interest_technical: The was business interest technical to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of well data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `facility_events`, `facility_operators`, `facility_specifications`, `facility_states`, `geo_contexts`, `historical_interests`, `name_aliases`, `technical_assurances` or `vertical_measurements` external ids for the well data. Defaults to True.

        Returns:
            List of requested well data

        Examples:

            List well data and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> well_data = client.well_data.list(limit=5)

        """
        filter_ = _create_well_datum_filter(
            self._view_id,
            business_intention_id,
            business_intention_id_prefix,
            condition_id,
            condition_id_prefix,
            current_operator_id,
            current_operator_id_prefix,
            data_source_organisation_id,
            data_source_organisation_id_prefix,
            default_vertical_crsid,
            default_vertical_crsid_prefix,
            default_vertical_measurement_id,
            default_vertical_measurement_id_prefix,
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
            initial_operator_id,
            initial_operator_id_prefix,
            interest_type_id,
            interest_type_id_prefix,
            operating_environment_id,
            operating_environment_id_prefix,
            outcome_id,
            outcome_id_prefix,
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
            source,
            source_prefix,
            spatial_location,
            status_summary_id,
            status_summary_id_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            version_creation_reason,
            version_creation_reason_prefix,
            was_business_interest_financial_non_operated,
            was_business_interest_financial_operated,
            was_business_interest_obligatory,
            was_business_interest_technical,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_pairs=[
                (self.facility_events_edge, "facility_events"),
                (self.facility_operators_edge, "facility_operators"),
                (self.facility_specifications_edge, "facility_specifications"),
                (self.facility_states_edge, "facility_states"),
                (self.geo_contexts_edge, "geo_contexts"),
                (self.historical_interests_edge, "historical_interests"),
                (self.name_aliases_edge, "name_aliases"),
                (self.technical_assurances_edge, "technical_assurances"),
                (self.vertical_measurements_edge, "vertical_measurements"),
            ],
        )
