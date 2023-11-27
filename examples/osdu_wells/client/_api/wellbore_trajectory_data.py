from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    WellboreTrajectoryData,
    WellboreTrajectoryDataApply,
    WellboreTrajectoryDataFields,
    WellboreTrajectoryDataList,
    WellboreTrajectoryDataApplyList,
    WellboreTrajectoryDataTextFields,
)
from osdu_wells.client.data_classes._wellbore_trajectory_data import (
    _WELLBORETRAJECTORYDATA_PROPERTIES_BY_FIELD,
    _create_wellbore_trajectory_datum_filter,
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
from .wellbore_trajectory_data_artefacts import WellboreTrajectoryDataArtefactsAPI
from .wellbore_trajectory_data_available_trajectory_station_properties import (
    WellboreTrajectoryDataAvailableTrajectoryStationPropertiesAPI,
)
from .wellbore_trajectory_data_geo_contexts import WellboreTrajectoryDataGeoContextsAPI
from .wellbore_trajectory_data_lineage_assertions import WellboreTrajectoryDataLineageAssertionsAPI
from .wellbore_trajectory_data_name_aliases import WellboreTrajectoryDataNameAliasesAPI
from .wellbore_trajectory_data_technical_assurances import WellboreTrajectoryDataTechnicalAssurancesAPI
from .wellbore_trajectory_data_query import WellboreTrajectoryDataQueryAPI


class WellboreTrajectoryDataAPI(
    NodeAPI[WellboreTrajectoryData, WellboreTrajectoryDataApply, WellboreTrajectoryDataList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WellboreTrajectoryDataApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WellboreTrajectoryData,
            class_apply_type=WellboreTrajectoryDataApply,
            class_list=WellboreTrajectoryDataList,
            class_apply_list=WellboreTrajectoryDataApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.artefacts_edge = WellboreTrajectoryDataArtefactsAPI(client)
        self.available_trajectory_station_properties_edge = (
            WellboreTrajectoryDataAvailableTrajectoryStationPropertiesAPI(client)
        )
        self.geo_contexts_edge = WellboreTrajectoryDataGeoContextsAPI(client)
        self.lineage_assertions_edge = WellboreTrajectoryDataLineageAssertionsAPI(client)
        self.name_aliases_edge = WellboreTrajectoryDataNameAliasesAPI(client)
        self.technical_assurances_edge = WellboreTrajectoryDataTechnicalAssurancesAPI(client)

    def __call__(
        self,
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
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> WellboreTrajectoryDataQueryAPI[WellboreTrajectoryDataList]:
        """Query starting at wellbore trajectory data.

        Args:
            acquisition_date: The acquisition date to filter on.
            acquisition_date_prefix: The prefix of the acquisition date to filter on.
            acquisition_remark: The acquisition remark to filter on.
            acquisition_remark_prefix: The prefix of the acquisition remark to filter on.
            active_indicator: The active indicator to filter on.
            applied_operations_date_time: The applied operations date time to filter on.
            applied_operations_date_time_prefix: The prefix of the applied operations date time to filter on.
            applied_operations_remarks: The applied operations remark to filter on.
            applied_operations_remarks_prefix: The prefix of the applied operations remark to filter on.
            applied_operations_user: The applied operations user to filter on.
            applied_operations_user_prefix: The prefix of the applied operations user to filter on.
            azimuth_reference_type: The azimuth reference type to filter on.
            azimuth_reference_type_prefix: The prefix of the azimuth reference type to filter on.
            min_base_depth_measured_depth: The minimum value of the base depth measured depth to filter on.
            max_base_depth_measured_depth: The maximum value of the base depth measured depth to filter on.
            calculation_method_type: The calculation method type to filter on.
            calculation_method_type_prefix: The prefix of the calculation method type to filter on.
            company_id: The company id to filter on.
            company_id_prefix: The prefix of the company id to filter on.
            creation_date_time: The creation date time to filter on.
            creation_date_time_prefix: The prefix of the creation date time to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            end_date_time: The end date time to filter on.
            end_date_time_prefix: The prefix of the end date time to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            min_extrapolated_measured_depth: The minimum value of the extrapolated measured depth to filter on.
            max_extrapolated_measured_depth: The maximum value of the extrapolated measured depth to filter on.
            extrapolated_measured_depth_remark: The extrapolated measured depth remark to filter on.
            extrapolated_measured_depth_remark_prefix: The prefix of the extrapolated measured depth remark to filter on.
            geographic_crsid: The geographic crsid to filter on.
            geographic_crsid_prefix: The prefix of the geographic crsid to filter on.
            is_discoverable: The is discoverable to filter on.
            is_extended_load: The is extended load to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            projected_crsid: The projected crsid to filter on.
            projected_crsid_prefix: The prefix of the projected crsid to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            service_company_id: The service company id to filter on.
            service_company_id_prefix: The prefix of the service company id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_area: The spatial area to filter on.
            spatial_point: The spatial point to filter on.
            start_date_time: The start date time to filter on.
            start_date_time_prefix: The prefix of the start date time to filter on.
            submitter_name: The submitter name to filter on.
            submitter_name_prefix: The prefix of the submitter name to filter on.
            min_surface_grid_convergence: The minimum value of the surface grid convergence to filter on.
            max_surface_grid_convergence: The maximum value of the surface grid convergence to filter on.
            min_surface_scale_factor: The minimum value of the surface scale factor to filter on.
            max_surface_scale_factor: The maximum value of the surface scale factor to filter on.
            survey_reference_identifier: The survey reference identifier to filter on.
            survey_reference_identifier_prefix: The prefix of the survey reference identifier to filter on.
            survey_tool_type_id: The survey tool type id to filter on.
            survey_tool_type_id_prefix: The prefix of the survey tool type id to filter on.
            survey_type: The survey type to filter on.
            survey_type_prefix: The prefix of the survey type to filter on.
            survey_version: The survey version to filter on.
            survey_version_prefix: The prefix of the survey version to filter on.
            min_tie_measured_depth: The minimum value of the tie measured depth to filter on.
            max_tie_measured_depth: The maximum value of the tie measured depth to filter on.
            min_tie_true_vertical_depth: The minimum value of the tie true vertical depth to filter on.
            max_tie_true_vertical_depth: The maximum value of the tie true vertical depth to filter on.
            min_top_depth_measured_depth: The minimum value of the top depth measured depth to filter on.
            max_top_depth_measured_depth: The maximum value of the top depth measured depth to filter on.
            min_tortuosity: The minimum value of the tortuosity to filter on.
            max_tortuosity: The maximum value of the tortuosity to filter on.
            vertical_measurement: The vertical measurement to filter on.
            wellbore_id: The wellbore id to filter on.
            wellbore_id_prefix: The prefix of the wellbore id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore trajectory data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for wellbore trajectory data.

        """
        filter_ = _create_wellbore_trajectory_datum_filter(
            self._view_id,
            acquisition_date,
            acquisition_date_prefix,
            acquisition_remark,
            acquisition_remark_prefix,
            active_indicator,
            applied_operations_date_time,
            applied_operations_date_time_prefix,
            applied_operations_remarks,
            applied_operations_remarks_prefix,
            applied_operations_user,
            applied_operations_user_prefix,
            azimuth_reference_type,
            azimuth_reference_type_prefix,
            min_base_depth_measured_depth,
            max_base_depth_measured_depth,
            calculation_method_type,
            calculation_method_type_prefix,
            company_id,
            company_id_prefix,
            creation_date_time,
            creation_date_time_prefix,
            description,
            description_prefix,
            end_date_time,
            end_date_time_prefix,
            existence_kind,
            existence_kind_prefix,
            min_extrapolated_measured_depth,
            max_extrapolated_measured_depth,
            extrapolated_measured_depth_remark,
            extrapolated_measured_depth_remark_prefix,
            geographic_crsid,
            geographic_crsid_prefix,
            is_discoverable,
            is_extended_load,
            name,
            name_prefix,
            projected_crsid,
            projected_crsid_prefix,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            service_company_id,
            service_company_id_prefix,
            source,
            source_prefix,
            spatial_area,
            spatial_point,
            start_date_time,
            start_date_time_prefix,
            submitter_name,
            submitter_name_prefix,
            min_surface_grid_convergence,
            max_surface_grid_convergence,
            min_surface_scale_factor,
            max_surface_scale_factor,
            survey_reference_identifier,
            survey_reference_identifier_prefix,
            survey_tool_type_id,
            survey_tool_type_id_prefix,
            survey_type,
            survey_type_prefix,
            survey_version,
            survey_version_prefix,
            min_tie_measured_depth,
            max_tie_measured_depth,
            min_tie_true_vertical_depth,
            max_tie_true_vertical_depth,
            min_top_depth_measured_depth,
            max_top_depth_measured_depth,
            min_tortuosity,
            max_tortuosity,
            vertical_measurement,
            wellbore_id,
            wellbore_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            WellboreTrajectoryDataList,
            [
                QueryStep(
                    name="wellbore_trajectory_datum",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_id, list(_WELLBORETRAJECTORYDATA_PROPERTIES_BY_FIELD.values())
                            )
                        ]
                    ),
                    result_cls=WellboreTrajectoryData,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return WellboreTrajectoryDataQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self,
        wellbore_trajectory_datum: WellboreTrajectoryDataApply | Sequence[WellboreTrajectoryDataApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) wellbore trajectory data.

        Note: This method iterates through all nodes and timeseries linked to wellbore_trajectory_datum and creates them including the edges
        between the nodes. For example, if any of `artefacts`, `available_trajectory_station_properties`, `geo_contexts`, `lineage_assertions`, `name_aliases` or `technical_assurances` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            wellbore_trajectory_datum: Wellbore trajectory datum or sequence of wellbore trajectory data to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new wellbore_trajectory_datum:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import WellboreTrajectoryDataApply
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = WellboreTrajectoryDataApply(external_id="my_wellbore_trajectory_datum", ...)
                >>> result = client.wellbore_trajectory_data.apply(wellbore_trajectory_datum)

        """
        return self._apply(wellbore_trajectory_datum, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more wellbore trajectory datum.

        Args:
            external_id: External id of the wellbore trajectory datum to delete.
            space: The space where all the wellbore trajectory datum are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete wellbore_trajectory_datum by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.wellbore_trajectory_data.delete("my_wellbore_trajectory_datum")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> WellboreTrajectoryData:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> WellboreTrajectoryDataList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> WellboreTrajectoryData | WellboreTrajectoryDataList:
        """Retrieve one or more wellbore trajectory data by id(s).

        Args:
            external_id: External id or list of external ids of the wellbore trajectory data.
            space: The space where all the wellbore trajectory data are located.

        Returns:
            The requested wellbore trajectory data.

        Examples:

            Retrieve wellbore_trajectory_datum by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_datum = client.wellbore_trajectory_data.retrieve("my_wellbore_trajectory_datum")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_triple=[
                (
                    self.artefacts_edge,
                    "artefacts",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.Artefacts"),
                ),
                (
                    self.available_trajectory_station_properties_edge,
                    "available_trajectory_station_properties",
                    dm.DirectRelationReference(
                        "IntegrationTestsImmutable", "WellboreTrajectoryData.AvailableTrajectoryStationProperties"
                    ),
                ),
                (
                    self.geo_contexts_edge,
                    "geo_contexts",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.GeoContexts"),
                ),
                (
                    self.lineage_assertions_edge,
                    "lineage_assertions",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.LineageAssertions"),
                ),
                (
                    self.name_aliases_edge,
                    "name_aliases",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.NameAliases"),
                ),
                (
                    self.technical_assurances_edge,
                    "technical_assurances",
                    dm.DirectRelationReference(
                        "IntegrationTestsImmutable", "WellboreTrajectoryData.TechnicalAssurances"
                    ),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: WellboreTrajectoryDataTextFields | Sequence[WellboreTrajectoryDataTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WellboreTrajectoryDataList:
        """Search wellbore trajectory data

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            acquisition_date: The acquisition date to filter on.
            acquisition_date_prefix: The prefix of the acquisition date to filter on.
            acquisition_remark: The acquisition remark to filter on.
            acquisition_remark_prefix: The prefix of the acquisition remark to filter on.
            active_indicator: The active indicator to filter on.
            applied_operations_date_time: The applied operations date time to filter on.
            applied_operations_date_time_prefix: The prefix of the applied operations date time to filter on.
            applied_operations_remarks: The applied operations remark to filter on.
            applied_operations_remarks_prefix: The prefix of the applied operations remark to filter on.
            applied_operations_user: The applied operations user to filter on.
            applied_operations_user_prefix: The prefix of the applied operations user to filter on.
            azimuth_reference_type: The azimuth reference type to filter on.
            azimuth_reference_type_prefix: The prefix of the azimuth reference type to filter on.
            min_base_depth_measured_depth: The minimum value of the base depth measured depth to filter on.
            max_base_depth_measured_depth: The maximum value of the base depth measured depth to filter on.
            calculation_method_type: The calculation method type to filter on.
            calculation_method_type_prefix: The prefix of the calculation method type to filter on.
            company_id: The company id to filter on.
            company_id_prefix: The prefix of the company id to filter on.
            creation_date_time: The creation date time to filter on.
            creation_date_time_prefix: The prefix of the creation date time to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            end_date_time: The end date time to filter on.
            end_date_time_prefix: The prefix of the end date time to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            min_extrapolated_measured_depth: The minimum value of the extrapolated measured depth to filter on.
            max_extrapolated_measured_depth: The maximum value of the extrapolated measured depth to filter on.
            extrapolated_measured_depth_remark: The extrapolated measured depth remark to filter on.
            extrapolated_measured_depth_remark_prefix: The prefix of the extrapolated measured depth remark to filter on.
            geographic_crsid: The geographic crsid to filter on.
            geographic_crsid_prefix: The prefix of the geographic crsid to filter on.
            is_discoverable: The is discoverable to filter on.
            is_extended_load: The is extended load to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            projected_crsid: The projected crsid to filter on.
            projected_crsid_prefix: The prefix of the projected crsid to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            service_company_id: The service company id to filter on.
            service_company_id_prefix: The prefix of the service company id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_area: The spatial area to filter on.
            spatial_point: The spatial point to filter on.
            start_date_time: The start date time to filter on.
            start_date_time_prefix: The prefix of the start date time to filter on.
            submitter_name: The submitter name to filter on.
            submitter_name_prefix: The prefix of the submitter name to filter on.
            min_surface_grid_convergence: The minimum value of the surface grid convergence to filter on.
            max_surface_grid_convergence: The maximum value of the surface grid convergence to filter on.
            min_surface_scale_factor: The minimum value of the surface scale factor to filter on.
            max_surface_scale_factor: The maximum value of the surface scale factor to filter on.
            survey_reference_identifier: The survey reference identifier to filter on.
            survey_reference_identifier_prefix: The prefix of the survey reference identifier to filter on.
            survey_tool_type_id: The survey tool type id to filter on.
            survey_tool_type_id_prefix: The prefix of the survey tool type id to filter on.
            survey_type: The survey type to filter on.
            survey_type_prefix: The prefix of the survey type to filter on.
            survey_version: The survey version to filter on.
            survey_version_prefix: The prefix of the survey version to filter on.
            min_tie_measured_depth: The minimum value of the tie measured depth to filter on.
            max_tie_measured_depth: The maximum value of the tie measured depth to filter on.
            min_tie_true_vertical_depth: The minimum value of the tie true vertical depth to filter on.
            max_tie_true_vertical_depth: The maximum value of the tie true vertical depth to filter on.
            min_top_depth_measured_depth: The minimum value of the top depth measured depth to filter on.
            max_top_depth_measured_depth: The maximum value of the top depth measured depth to filter on.
            min_tortuosity: The minimum value of the tortuosity to filter on.
            max_tortuosity: The maximum value of the tortuosity to filter on.
            vertical_measurement: The vertical measurement to filter on.
            wellbore_id: The wellbore id to filter on.
            wellbore_id_prefix: The prefix of the wellbore id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore trajectory data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results wellbore trajectory data matching the query.

        Examples:

           Search for 'my_wellbore_trajectory_datum' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_data = client.wellbore_trajectory_data.search('my_wellbore_trajectory_datum')

        """
        filter_ = _create_wellbore_trajectory_datum_filter(
            self._view_id,
            acquisition_date,
            acquisition_date_prefix,
            acquisition_remark,
            acquisition_remark_prefix,
            active_indicator,
            applied_operations_date_time,
            applied_operations_date_time_prefix,
            applied_operations_remarks,
            applied_operations_remarks_prefix,
            applied_operations_user,
            applied_operations_user_prefix,
            azimuth_reference_type,
            azimuth_reference_type_prefix,
            min_base_depth_measured_depth,
            max_base_depth_measured_depth,
            calculation_method_type,
            calculation_method_type_prefix,
            company_id,
            company_id_prefix,
            creation_date_time,
            creation_date_time_prefix,
            description,
            description_prefix,
            end_date_time,
            end_date_time_prefix,
            existence_kind,
            existence_kind_prefix,
            min_extrapolated_measured_depth,
            max_extrapolated_measured_depth,
            extrapolated_measured_depth_remark,
            extrapolated_measured_depth_remark_prefix,
            geographic_crsid,
            geographic_crsid_prefix,
            is_discoverable,
            is_extended_load,
            name,
            name_prefix,
            projected_crsid,
            projected_crsid_prefix,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            service_company_id,
            service_company_id_prefix,
            source,
            source_prefix,
            spatial_area,
            spatial_point,
            start_date_time,
            start_date_time_prefix,
            submitter_name,
            submitter_name_prefix,
            min_surface_grid_convergence,
            max_surface_grid_convergence,
            min_surface_scale_factor,
            max_surface_scale_factor,
            survey_reference_identifier,
            survey_reference_identifier_prefix,
            survey_tool_type_id,
            survey_tool_type_id_prefix,
            survey_type,
            survey_type_prefix,
            survey_version,
            survey_version_prefix,
            min_tie_measured_depth,
            max_tie_measured_depth,
            min_tie_true_vertical_depth,
            max_tie_true_vertical_depth,
            min_top_depth_measured_depth,
            max_top_depth_measured_depth,
            min_tortuosity,
            max_tortuosity,
            vertical_measurement,
            wellbore_id,
            wellbore_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _WELLBORETRAJECTORYDATA_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WellboreTrajectoryDataFields | Sequence[WellboreTrajectoryDataFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WellboreTrajectoryDataTextFields | Sequence[WellboreTrajectoryDataTextFields] | None = None,
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
        property: WellboreTrajectoryDataFields | Sequence[WellboreTrajectoryDataFields] | None = None,
        group_by: WellboreTrajectoryDataFields | Sequence[WellboreTrajectoryDataFields] = None,
        query: str | None = None,
        search_properties: WellboreTrajectoryDataTextFields | Sequence[WellboreTrajectoryDataTextFields] | None = None,
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
        property: WellboreTrajectoryDataFields | Sequence[WellboreTrajectoryDataFields] | None = None,
        group_by: WellboreTrajectoryDataFields | Sequence[WellboreTrajectoryDataFields] | None = None,
        query: str | None = None,
        search_property: WellboreTrajectoryDataTextFields | Sequence[WellboreTrajectoryDataTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across wellbore trajectory data

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            acquisition_date: The acquisition date to filter on.
            acquisition_date_prefix: The prefix of the acquisition date to filter on.
            acquisition_remark: The acquisition remark to filter on.
            acquisition_remark_prefix: The prefix of the acquisition remark to filter on.
            active_indicator: The active indicator to filter on.
            applied_operations_date_time: The applied operations date time to filter on.
            applied_operations_date_time_prefix: The prefix of the applied operations date time to filter on.
            applied_operations_remarks: The applied operations remark to filter on.
            applied_operations_remarks_prefix: The prefix of the applied operations remark to filter on.
            applied_operations_user: The applied operations user to filter on.
            applied_operations_user_prefix: The prefix of the applied operations user to filter on.
            azimuth_reference_type: The azimuth reference type to filter on.
            azimuth_reference_type_prefix: The prefix of the azimuth reference type to filter on.
            min_base_depth_measured_depth: The minimum value of the base depth measured depth to filter on.
            max_base_depth_measured_depth: The maximum value of the base depth measured depth to filter on.
            calculation_method_type: The calculation method type to filter on.
            calculation_method_type_prefix: The prefix of the calculation method type to filter on.
            company_id: The company id to filter on.
            company_id_prefix: The prefix of the company id to filter on.
            creation_date_time: The creation date time to filter on.
            creation_date_time_prefix: The prefix of the creation date time to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            end_date_time: The end date time to filter on.
            end_date_time_prefix: The prefix of the end date time to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            min_extrapolated_measured_depth: The minimum value of the extrapolated measured depth to filter on.
            max_extrapolated_measured_depth: The maximum value of the extrapolated measured depth to filter on.
            extrapolated_measured_depth_remark: The extrapolated measured depth remark to filter on.
            extrapolated_measured_depth_remark_prefix: The prefix of the extrapolated measured depth remark to filter on.
            geographic_crsid: The geographic crsid to filter on.
            geographic_crsid_prefix: The prefix of the geographic crsid to filter on.
            is_discoverable: The is discoverable to filter on.
            is_extended_load: The is extended load to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            projected_crsid: The projected crsid to filter on.
            projected_crsid_prefix: The prefix of the projected crsid to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            service_company_id: The service company id to filter on.
            service_company_id_prefix: The prefix of the service company id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_area: The spatial area to filter on.
            spatial_point: The spatial point to filter on.
            start_date_time: The start date time to filter on.
            start_date_time_prefix: The prefix of the start date time to filter on.
            submitter_name: The submitter name to filter on.
            submitter_name_prefix: The prefix of the submitter name to filter on.
            min_surface_grid_convergence: The minimum value of the surface grid convergence to filter on.
            max_surface_grid_convergence: The maximum value of the surface grid convergence to filter on.
            min_surface_scale_factor: The minimum value of the surface scale factor to filter on.
            max_surface_scale_factor: The maximum value of the surface scale factor to filter on.
            survey_reference_identifier: The survey reference identifier to filter on.
            survey_reference_identifier_prefix: The prefix of the survey reference identifier to filter on.
            survey_tool_type_id: The survey tool type id to filter on.
            survey_tool_type_id_prefix: The prefix of the survey tool type id to filter on.
            survey_type: The survey type to filter on.
            survey_type_prefix: The prefix of the survey type to filter on.
            survey_version: The survey version to filter on.
            survey_version_prefix: The prefix of the survey version to filter on.
            min_tie_measured_depth: The minimum value of the tie measured depth to filter on.
            max_tie_measured_depth: The maximum value of the tie measured depth to filter on.
            min_tie_true_vertical_depth: The minimum value of the tie true vertical depth to filter on.
            max_tie_true_vertical_depth: The maximum value of the tie true vertical depth to filter on.
            min_top_depth_measured_depth: The minimum value of the top depth measured depth to filter on.
            max_top_depth_measured_depth: The maximum value of the top depth measured depth to filter on.
            min_tortuosity: The minimum value of the tortuosity to filter on.
            max_tortuosity: The maximum value of the tortuosity to filter on.
            vertical_measurement: The vertical measurement to filter on.
            wellbore_id: The wellbore id to filter on.
            wellbore_id_prefix: The prefix of the wellbore id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore trajectory data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count wellbore trajectory data in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.wellbore_trajectory_data.aggregate("count", space="my_space")

        """

        filter_ = _create_wellbore_trajectory_datum_filter(
            self._view_id,
            acquisition_date,
            acquisition_date_prefix,
            acquisition_remark,
            acquisition_remark_prefix,
            active_indicator,
            applied_operations_date_time,
            applied_operations_date_time_prefix,
            applied_operations_remarks,
            applied_operations_remarks_prefix,
            applied_operations_user,
            applied_operations_user_prefix,
            azimuth_reference_type,
            azimuth_reference_type_prefix,
            min_base_depth_measured_depth,
            max_base_depth_measured_depth,
            calculation_method_type,
            calculation_method_type_prefix,
            company_id,
            company_id_prefix,
            creation_date_time,
            creation_date_time_prefix,
            description,
            description_prefix,
            end_date_time,
            end_date_time_prefix,
            existence_kind,
            existence_kind_prefix,
            min_extrapolated_measured_depth,
            max_extrapolated_measured_depth,
            extrapolated_measured_depth_remark,
            extrapolated_measured_depth_remark_prefix,
            geographic_crsid,
            geographic_crsid_prefix,
            is_discoverable,
            is_extended_load,
            name,
            name_prefix,
            projected_crsid,
            projected_crsid_prefix,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            service_company_id,
            service_company_id_prefix,
            source,
            source_prefix,
            spatial_area,
            spatial_point,
            start_date_time,
            start_date_time_prefix,
            submitter_name,
            submitter_name_prefix,
            min_surface_grid_convergence,
            max_surface_grid_convergence,
            min_surface_scale_factor,
            max_surface_scale_factor,
            survey_reference_identifier,
            survey_reference_identifier_prefix,
            survey_tool_type_id,
            survey_tool_type_id_prefix,
            survey_type,
            survey_type_prefix,
            survey_version,
            survey_version_prefix,
            min_tie_measured_depth,
            max_tie_measured_depth,
            min_tie_true_vertical_depth,
            max_tie_true_vertical_depth,
            min_top_depth_measured_depth,
            max_top_depth_measured_depth,
            min_tortuosity,
            max_tortuosity,
            vertical_measurement,
            wellbore_id,
            wellbore_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WELLBORETRAJECTORYDATA_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WellboreTrajectoryDataFields,
        interval: float,
        query: str | None = None,
        search_property: WellboreTrajectoryDataTextFields | Sequence[WellboreTrajectoryDataTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for wellbore trajectory data

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            acquisition_date: The acquisition date to filter on.
            acquisition_date_prefix: The prefix of the acquisition date to filter on.
            acquisition_remark: The acquisition remark to filter on.
            acquisition_remark_prefix: The prefix of the acquisition remark to filter on.
            active_indicator: The active indicator to filter on.
            applied_operations_date_time: The applied operations date time to filter on.
            applied_operations_date_time_prefix: The prefix of the applied operations date time to filter on.
            applied_operations_remarks: The applied operations remark to filter on.
            applied_operations_remarks_prefix: The prefix of the applied operations remark to filter on.
            applied_operations_user: The applied operations user to filter on.
            applied_operations_user_prefix: The prefix of the applied operations user to filter on.
            azimuth_reference_type: The azimuth reference type to filter on.
            azimuth_reference_type_prefix: The prefix of the azimuth reference type to filter on.
            min_base_depth_measured_depth: The minimum value of the base depth measured depth to filter on.
            max_base_depth_measured_depth: The maximum value of the base depth measured depth to filter on.
            calculation_method_type: The calculation method type to filter on.
            calculation_method_type_prefix: The prefix of the calculation method type to filter on.
            company_id: The company id to filter on.
            company_id_prefix: The prefix of the company id to filter on.
            creation_date_time: The creation date time to filter on.
            creation_date_time_prefix: The prefix of the creation date time to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            end_date_time: The end date time to filter on.
            end_date_time_prefix: The prefix of the end date time to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            min_extrapolated_measured_depth: The minimum value of the extrapolated measured depth to filter on.
            max_extrapolated_measured_depth: The maximum value of the extrapolated measured depth to filter on.
            extrapolated_measured_depth_remark: The extrapolated measured depth remark to filter on.
            extrapolated_measured_depth_remark_prefix: The prefix of the extrapolated measured depth remark to filter on.
            geographic_crsid: The geographic crsid to filter on.
            geographic_crsid_prefix: The prefix of the geographic crsid to filter on.
            is_discoverable: The is discoverable to filter on.
            is_extended_load: The is extended load to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            projected_crsid: The projected crsid to filter on.
            projected_crsid_prefix: The prefix of the projected crsid to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            service_company_id: The service company id to filter on.
            service_company_id_prefix: The prefix of the service company id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_area: The spatial area to filter on.
            spatial_point: The spatial point to filter on.
            start_date_time: The start date time to filter on.
            start_date_time_prefix: The prefix of the start date time to filter on.
            submitter_name: The submitter name to filter on.
            submitter_name_prefix: The prefix of the submitter name to filter on.
            min_surface_grid_convergence: The minimum value of the surface grid convergence to filter on.
            max_surface_grid_convergence: The maximum value of the surface grid convergence to filter on.
            min_surface_scale_factor: The minimum value of the surface scale factor to filter on.
            max_surface_scale_factor: The maximum value of the surface scale factor to filter on.
            survey_reference_identifier: The survey reference identifier to filter on.
            survey_reference_identifier_prefix: The prefix of the survey reference identifier to filter on.
            survey_tool_type_id: The survey tool type id to filter on.
            survey_tool_type_id_prefix: The prefix of the survey tool type id to filter on.
            survey_type: The survey type to filter on.
            survey_type_prefix: The prefix of the survey type to filter on.
            survey_version: The survey version to filter on.
            survey_version_prefix: The prefix of the survey version to filter on.
            min_tie_measured_depth: The minimum value of the tie measured depth to filter on.
            max_tie_measured_depth: The maximum value of the tie measured depth to filter on.
            min_tie_true_vertical_depth: The minimum value of the tie true vertical depth to filter on.
            max_tie_true_vertical_depth: The maximum value of the tie true vertical depth to filter on.
            min_top_depth_measured_depth: The minimum value of the top depth measured depth to filter on.
            max_top_depth_measured_depth: The maximum value of the top depth measured depth to filter on.
            min_tortuosity: The minimum value of the tortuosity to filter on.
            max_tortuosity: The maximum value of the tortuosity to filter on.
            vertical_measurement: The vertical measurement to filter on.
            wellbore_id: The wellbore id to filter on.
            wellbore_id_prefix: The prefix of the wellbore id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore trajectory data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_wellbore_trajectory_datum_filter(
            self._view_id,
            acquisition_date,
            acquisition_date_prefix,
            acquisition_remark,
            acquisition_remark_prefix,
            active_indicator,
            applied_operations_date_time,
            applied_operations_date_time_prefix,
            applied_operations_remarks,
            applied_operations_remarks_prefix,
            applied_operations_user,
            applied_operations_user_prefix,
            azimuth_reference_type,
            azimuth_reference_type_prefix,
            min_base_depth_measured_depth,
            max_base_depth_measured_depth,
            calculation_method_type,
            calculation_method_type_prefix,
            company_id,
            company_id_prefix,
            creation_date_time,
            creation_date_time_prefix,
            description,
            description_prefix,
            end_date_time,
            end_date_time_prefix,
            existence_kind,
            existence_kind_prefix,
            min_extrapolated_measured_depth,
            max_extrapolated_measured_depth,
            extrapolated_measured_depth_remark,
            extrapolated_measured_depth_remark_prefix,
            geographic_crsid,
            geographic_crsid_prefix,
            is_discoverable,
            is_extended_load,
            name,
            name_prefix,
            projected_crsid,
            projected_crsid_prefix,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            service_company_id,
            service_company_id_prefix,
            source,
            source_prefix,
            spatial_area,
            spatial_point,
            start_date_time,
            start_date_time_prefix,
            submitter_name,
            submitter_name_prefix,
            min_surface_grid_convergence,
            max_surface_grid_convergence,
            min_surface_scale_factor,
            max_surface_scale_factor,
            survey_reference_identifier,
            survey_reference_identifier_prefix,
            survey_tool_type_id,
            survey_tool_type_id_prefix,
            survey_type,
            survey_type_prefix,
            survey_version,
            survey_version_prefix,
            min_tie_measured_depth,
            max_tie_measured_depth,
            min_tie_true_vertical_depth,
            max_tie_true_vertical_depth,
            min_top_depth_measured_depth,
            max_top_depth_measured_depth,
            min_tortuosity,
            max_tortuosity,
            vertical_measurement,
            wellbore_id,
            wellbore_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WELLBORETRAJECTORYDATA_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WellboreTrajectoryDataList:
        """List/filter wellbore trajectory data

        Args:
            acquisition_date: The acquisition date to filter on.
            acquisition_date_prefix: The prefix of the acquisition date to filter on.
            acquisition_remark: The acquisition remark to filter on.
            acquisition_remark_prefix: The prefix of the acquisition remark to filter on.
            active_indicator: The active indicator to filter on.
            applied_operations_date_time: The applied operations date time to filter on.
            applied_operations_date_time_prefix: The prefix of the applied operations date time to filter on.
            applied_operations_remarks: The applied operations remark to filter on.
            applied_operations_remarks_prefix: The prefix of the applied operations remark to filter on.
            applied_operations_user: The applied operations user to filter on.
            applied_operations_user_prefix: The prefix of the applied operations user to filter on.
            azimuth_reference_type: The azimuth reference type to filter on.
            azimuth_reference_type_prefix: The prefix of the azimuth reference type to filter on.
            min_base_depth_measured_depth: The minimum value of the base depth measured depth to filter on.
            max_base_depth_measured_depth: The maximum value of the base depth measured depth to filter on.
            calculation_method_type: The calculation method type to filter on.
            calculation_method_type_prefix: The prefix of the calculation method type to filter on.
            company_id: The company id to filter on.
            company_id_prefix: The prefix of the company id to filter on.
            creation_date_time: The creation date time to filter on.
            creation_date_time_prefix: The prefix of the creation date time to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            end_date_time: The end date time to filter on.
            end_date_time_prefix: The prefix of the end date time to filter on.
            existence_kind: The existence kind to filter on.
            existence_kind_prefix: The prefix of the existence kind to filter on.
            min_extrapolated_measured_depth: The minimum value of the extrapolated measured depth to filter on.
            max_extrapolated_measured_depth: The maximum value of the extrapolated measured depth to filter on.
            extrapolated_measured_depth_remark: The extrapolated measured depth remark to filter on.
            extrapolated_measured_depth_remark_prefix: The prefix of the extrapolated measured depth remark to filter on.
            geographic_crsid: The geographic crsid to filter on.
            geographic_crsid_prefix: The prefix of the geographic crsid to filter on.
            is_discoverable: The is discoverable to filter on.
            is_extended_load: The is extended load to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            projected_crsid: The projected crsid to filter on.
            projected_crsid_prefix: The prefix of the projected crsid to filter on.
            resource_curation_status: The resource curation status to filter on.
            resource_curation_status_prefix: The prefix of the resource curation status to filter on.
            resource_home_region_id: The resource home region id to filter on.
            resource_home_region_id_prefix: The prefix of the resource home region id to filter on.
            resource_lifecycle_status: The resource lifecycle status to filter on.
            resource_lifecycle_status_prefix: The prefix of the resource lifecycle status to filter on.
            resource_security_classification: The resource security classification to filter on.
            resource_security_classification_prefix: The prefix of the resource security classification to filter on.
            service_company_id: The service company id to filter on.
            service_company_id_prefix: The prefix of the service company id to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            spatial_area: The spatial area to filter on.
            spatial_point: The spatial point to filter on.
            start_date_time: The start date time to filter on.
            start_date_time_prefix: The prefix of the start date time to filter on.
            submitter_name: The submitter name to filter on.
            submitter_name_prefix: The prefix of the submitter name to filter on.
            min_surface_grid_convergence: The minimum value of the surface grid convergence to filter on.
            max_surface_grid_convergence: The maximum value of the surface grid convergence to filter on.
            min_surface_scale_factor: The minimum value of the surface scale factor to filter on.
            max_surface_scale_factor: The maximum value of the surface scale factor to filter on.
            survey_reference_identifier: The survey reference identifier to filter on.
            survey_reference_identifier_prefix: The prefix of the survey reference identifier to filter on.
            survey_tool_type_id: The survey tool type id to filter on.
            survey_tool_type_id_prefix: The prefix of the survey tool type id to filter on.
            survey_type: The survey type to filter on.
            survey_type_prefix: The prefix of the survey type to filter on.
            survey_version: The survey version to filter on.
            survey_version_prefix: The prefix of the survey version to filter on.
            min_tie_measured_depth: The minimum value of the tie measured depth to filter on.
            max_tie_measured_depth: The maximum value of the tie measured depth to filter on.
            min_tie_true_vertical_depth: The minimum value of the tie true vertical depth to filter on.
            max_tie_true_vertical_depth: The maximum value of the tie true vertical depth to filter on.
            min_top_depth_measured_depth: The minimum value of the top depth measured depth to filter on.
            max_top_depth_measured_depth: The maximum value of the top depth measured depth to filter on.
            min_tortuosity: The minimum value of the tortuosity to filter on.
            max_tortuosity: The maximum value of the tortuosity to filter on.
            vertical_measurement: The vertical measurement to filter on.
            wellbore_id: The wellbore id to filter on.
            wellbore_id_prefix: The prefix of the wellbore id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of wellbore trajectory data to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `artefacts`, `available_trajectory_station_properties`, `geo_contexts`, `lineage_assertions`, `name_aliases` or `technical_assurances` external ids for the wellbore trajectory data. Defaults to True.

        Returns:
            List of requested wellbore trajectory data

        Examples:

            List wellbore trajectory data and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> wellbore_trajectory_data = client.wellbore_trajectory_data.list(limit=5)

        """
        filter_ = _create_wellbore_trajectory_datum_filter(
            self._view_id,
            acquisition_date,
            acquisition_date_prefix,
            acquisition_remark,
            acquisition_remark_prefix,
            active_indicator,
            applied_operations_date_time,
            applied_operations_date_time_prefix,
            applied_operations_remarks,
            applied_operations_remarks_prefix,
            applied_operations_user,
            applied_operations_user_prefix,
            azimuth_reference_type,
            azimuth_reference_type_prefix,
            min_base_depth_measured_depth,
            max_base_depth_measured_depth,
            calculation_method_type,
            calculation_method_type_prefix,
            company_id,
            company_id_prefix,
            creation_date_time,
            creation_date_time_prefix,
            description,
            description_prefix,
            end_date_time,
            end_date_time_prefix,
            existence_kind,
            existence_kind_prefix,
            min_extrapolated_measured_depth,
            max_extrapolated_measured_depth,
            extrapolated_measured_depth_remark,
            extrapolated_measured_depth_remark_prefix,
            geographic_crsid,
            geographic_crsid_prefix,
            is_discoverable,
            is_extended_load,
            name,
            name_prefix,
            projected_crsid,
            projected_crsid_prefix,
            resource_curation_status,
            resource_curation_status_prefix,
            resource_home_region_id,
            resource_home_region_id_prefix,
            resource_lifecycle_status,
            resource_lifecycle_status_prefix,
            resource_security_classification,
            resource_security_classification_prefix,
            service_company_id,
            service_company_id_prefix,
            source,
            source_prefix,
            spatial_area,
            spatial_point,
            start_date_time,
            start_date_time_prefix,
            submitter_name,
            submitter_name_prefix,
            min_surface_grid_convergence,
            max_surface_grid_convergence,
            min_surface_scale_factor,
            max_surface_scale_factor,
            survey_reference_identifier,
            survey_reference_identifier_prefix,
            survey_tool_type_id,
            survey_tool_type_id_prefix,
            survey_type,
            survey_type_prefix,
            survey_version,
            survey_version_prefix,
            min_tie_measured_depth,
            max_tie_measured_depth,
            min_tie_true_vertical_depth,
            max_tie_true_vertical_depth,
            min_top_depth_measured_depth,
            max_top_depth_measured_depth,
            min_tortuosity,
            max_tortuosity,
            vertical_measurement,
            wellbore_id,
            wellbore_id_prefix,
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
                    self.artefacts_edge,
                    "artefacts",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.Artefacts"),
                ),
                (
                    self.available_trajectory_station_properties_edge,
                    "available_trajectory_station_properties",
                    dm.DirectRelationReference(
                        "IntegrationTestsImmutable", "WellboreTrajectoryData.AvailableTrajectoryStationProperties"
                    ),
                ),
                (
                    self.geo_contexts_edge,
                    "geo_contexts",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.GeoContexts"),
                ),
                (
                    self.lineage_assertions_edge,
                    "lineage_assertions",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.LineageAssertions"),
                ),
                (
                    self.name_aliases_edge,
                    "name_aliases",
                    dm.DirectRelationReference("IntegrationTestsImmutable", "WellboreTrajectoryData.NameAliases"),
                ),
                (
                    self.technical_assurances_edge,
                    "technical_assurances",
                    dm.DirectRelationReference(
                        "IntegrationTestsImmutable", "WellboreTrajectoryData.TechnicalAssurances"
                    ),
                ),
            ],
        )
