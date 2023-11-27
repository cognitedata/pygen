from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    ProjectedBottomHoleLocation,
    ProjectedBottomHoleLocationApply,
    ProjectedBottomHoleLocationFields,
    ProjectedBottomHoleLocationList,
    ProjectedBottomHoleLocationTextFields,
)
from osdu_wells.client.data_classes._projected_bottom_hole_location import (
    _PROJECTEDBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD,
    _create_projected_bottom_hole_location_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .projected_bottom_hole_location_query import ProjectedBottomHoleLocationQueryAPI


class ProjectedBottomHoleLocationAPI(
    NodeAPI[ProjectedBottomHoleLocation, ProjectedBottomHoleLocationApply, ProjectedBottomHoleLocationList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[ProjectedBottomHoleLocationApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ProjectedBottomHoleLocation,
            class_apply_type=ProjectedBottomHoleLocationApply,
            class_list=ProjectedBottomHoleLocationList,
            class_apply_list=ProjectedBottomHoleLocationApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ProjectedBottomHoleLocationQueryAPI[ProjectedBottomHoleLocationList]:
        """Query starting at projected bottom hole locations.

        Args:
            as_ingested_coordinates: The as ingested coordinate to filter on.
            coordinate_quality_check_date_time: The coordinate quality check date time to filter on.
            coordinate_quality_check_date_time_prefix: The prefix of the coordinate quality check date time to filter on.
            coordinate_quality_check_performed_by: The coordinate quality check performed by to filter on.
            coordinate_quality_check_performed_by_prefix: The prefix of the coordinate quality check performed by to filter on.
            qualitative_spatial_accuracy_type_id: The qualitative spatial accuracy type id to filter on.
            qualitative_spatial_accuracy_type_id_prefix: The prefix of the qualitative spatial accuracy type id to filter on.
            quantitative_accuracy_band_id: The quantitative accuracy band id to filter on.
            quantitative_accuracy_band_id_prefix: The prefix of the quantitative accuracy band id to filter on.
            spatial_geometry_type_id: The spatial geometry type id to filter on.
            spatial_geometry_type_id_prefix: The prefix of the spatial geometry type id to filter on.
            spatial_location_coordinates_date: The spatial location coordinates date to filter on.
            spatial_location_coordinates_date_prefix: The prefix of the spatial location coordinates date to filter on.
            spatial_parameter_type_id: The spatial parameter type id to filter on.
            spatial_parameter_type_id_prefix: The prefix of the spatial parameter type id to filter on.
            wgs_84_coordinates: The wgs 84 coordinate to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of projected bottom hole locations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for projected bottom hole locations.

        """
        filter_ = _create_projected_bottom_hole_location_filter(
            self._view_id,
            as_ingested_coordinates,
            coordinate_quality_check_date_time,
            coordinate_quality_check_date_time_prefix,
            coordinate_quality_check_performed_by,
            coordinate_quality_check_performed_by_prefix,
            qualitative_spatial_accuracy_type_id,
            qualitative_spatial_accuracy_type_id_prefix,
            quantitative_accuracy_band_id,
            quantitative_accuracy_band_id_prefix,
            spatial_geometry_type_id,
            spatial_geometry_type_id_prefix,
            spatial_location_coordinates_date,
            spatial_location_coordinates_date_prefix,
            spatial_parameter_type_id,
            spatial_parameter_type_id_prefix,
            wgs_84_coordinates,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            ProjectedBottomHoleLocationList,
            [
                QueryStep(
                    name="projected_bottom_hole_location",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_id, list(_PROJECTEDBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD.values())
                            )
                        ]
                    ),
                    result_cls=ProjectedBottomHoleLocation,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return ProjectedBottomHoleLocationQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self,
        projected_bottom_hole_location: ProjectedBottomHoleLocationApply | Sequence[ProjectedBottomHoleLocationApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) projected bottom hole locations.

        Args:
            projected_bottom_hole_location: Projected bottom hole location or sequence of projected bottom hole locations to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new projected_bottom_hole_location:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import ProjectedBottomHoleLocationApply
                >>> client = OSDUClient()
                >>> projected_bottom_hole_location = ProjectedBottomHoleLocationApply(external_id="my_projected_bottom_hole_location", ...)
                >>> result = client.projected_bottom_hole_location.apply(projected_bottom_hole_location)

        """
        return self._apply(projected_bottom_hole_location, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more projected bottom hole location.

        Args:
            external_id: External id of the projected bottom hole location to delete.
            space: The space where all the projected bottom hole location are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete projected_bottom_hole_location by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.projected_bottom_hole_location.delete("my_projected_bottom_hole_location")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> ProjectedBottomHoleLocation:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> ProjectedBottomHoleLocationList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> ProjectedBottomHoleLocation | ProjectedBottomHoleLocationList:
        """Retrieve one or more projected bottom hole locations by id(s).

        Args:
            external_id: External id or list of external ids of the projected bottom hole locations.
            space: The space where all the projected bottom hole locations are located.

        Returns:
            The requested projected bottom hole locations.

        Examples:

            Retrieve projected_bottom_hole_location by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> projected_bottom_hole_location = client.projected_bottom_hole_location.retrieve("my_projected_bottom_hole_location")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: ProjectedBottomHoleLocationTextFields
        | Sequence[ProjectedBottomHoleLocationTextFields]
        | None = None,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ProjectedBottomHoleLocationList:
        """Search projected bottom hole locations

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            as_ingested_coordinates: The as ingested coordinate to filter on.
            coordinate_quality_check_date_time: The coordinate quality check date time to filter on.
            coordinate_quality_check_date_time_prefix: The prefix of the coordinate quality check date time to filter on.
            coordinate_quality_check_performed_by: The coordinate quality check performed by to filter on.
            coordinate_quality_check_performed_by_prefix: The prefix of the coordinate quality check performed by to filter on.
            qualitative_spatial_accuracy_type_id: The qualitative spatial accuracy type id to filter on.
            qualitative_spatial_accuracy_type_id_prefix: The prefix of the qualitative spatial accuracy type id to filter on.
            quantitative_accuracy_band_id: The quantitative accuracy band id to filter on.
            quantitative_accuracy_band_id_prefix: The prefix of the quantitative accuracy band id to filter on.
            spatial_geometry_type_id: The spatial geometry type id to filter on.
            spatial_geometry_type_id_prefix: The prefix of the spatial geometry type id to filter on.
            spatial_location_coordinates_date: The spatial location coordinates date to filter on.
            spatial_location_coordinates_date_prefix: The prefix of the spatial location coordinates date to filter on.
            spatial_parameter_type_id: The spatial parameter type id to filter on.
            spatial_parameter_type_id_prefix: The prefix of the spatial parameter type id to filter on.
            wgs_84_coordinates: The wgs 84 coordinate to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of projected bottom hole locations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results projected bottom hole locations matching the query.

        Examples:

           Search for 'my_projected_bottom_hole_location' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> projected_bottom_hole_locations = client.projected_bottom_hole_location.search('my_projected_bottom_hole_location')

        """
        filter_ = _create_projected_bottom_hole_location_filter(
            self._view_id,
            as_ingested_coordinates,
            coordinate_quality_check_date_time,
            coordinate_quality_check_date_time_prefix,
            coordinate_quality_check_performed_by,
            coordinate_quality_check_performed_by_prefix,
            qualitative_spatial_accuracy_type_id,
            qualitative_spatial_accuracy_type_id_prefix,
            quantitative_accuracy_band_id,
            quantitative_accuracy_band_id_prefix,
            spatial_geometry_type_id,
            spatial_geometry_type_id_prefix,
            spatial_location_coordinates_date,
            spatial_location_coordinates_date_prefix,
            spatial_parameter_type_id,
            spatial_parameter_type_id_prefix,
            wgs_84_coordinates,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _PROJECTEDBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ProjectedBottomHoleLocationFields | Sequence[ProjectedBottomHoleLocationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ProjectedBottomHoleLocationTextFields
        | Sequence[ProjectedBottomHoleLocationTextFields]
        | None = None,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: ProjectedBottomHoleLocationFields | Sequence[ProjectedBottomHoleLocationFields] | None = None,
        group_by: ProjectedBottomHoleLocationFields | Sequence[ProjectedBottomHoleLocationFields] = None,
        query: str | None = None,
        search_properties: ProjectedBottomHoleLocationTextFields
        | Sequence[ProjectedBottomHoleLocationTextFields]
        | None = None,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: ProjectedBottomHoleLocationFields | Sequence[ProjectedBottomHoleLocationFields] | None = None,
        group_by: ProjectedBottomHoleLocationFields | Sequence[ProjectedBottomHoleLocationFields] | None = None,
        query: str | None = None,
        search_property: ProjectedBottomHoleLocationTextFields
        | Sequence[ProjectedBottomHoleLocationTextFields]
        | None = None,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across projected bottom hole locations

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            as_ingested_coordinates: The as ingested coordinate to filter on.
            coordinate_quality_check_date_time: The coordinate quality check date time to filter on.
            coordinate_quality_check_date_time_prefix: The prefix of the coordinate quality check date time to filter on.
            coordinate_quality_check_performed_by: The coordinate quality check performed by to filter on.
            coordinate_quality_check_performed_by_prefix: The prefix of the coordinate quality check performed by to filter on.
            qualitative_spatial_accuracy_type_id: The qualitative spatial accuracy type id to filter on.
            qualitative_spatial_accuracy_type_id_prefix: The prefix of the qualitative spatial accuracy type id to filter on.
            quantitative_accuracy_band_id: The quantitative accuracy band id to filter on.
            quantitative_accuracy_band_id_prefix: The prefix of the quantitative accuracy band id to filter on.
            spatial_geometry_type_id: The spatial geometry type id to filter on.
            spatial_geometry_type_id_prefix: The prefix of the spatial geometry type id to filter on.
            spatial_location_coordinates_date: The spatial location coordinates date to filter on.
            spatial_location_coordinates_date_prefix: The prefix of the spatial location coordinates date to filter on.
            spatial_parameter_type_id: The spatial parameter type id to filter on.
            spatial_parameter_type_id_prefix: The prefix of the spatial parameter type id to filter on.
            wgs_84_coordinates: The wgs 84 coordinate to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of projected bottom hole locations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count projected bottom hole locations in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.projected_bottom_hole_location.aggregate("count", space="my_space")

        """

        filter_ = _create_projected_bottom_hole_location_filter(
            self._view_id,
            as_ingested_coordinates,
            coordinate_quality_check_date_time,
            coordinate_quality_check_date_time_prefix,
            coordinate_quality_check_performed_by,
            coordinate_quality_check_performed_by_prefix,
            qualitative_spatial_accuracy_type_id,
            qualitative_spatial_accuracy_type_id_prefix,
            quantitative_accuracy_band_id,
            quantitative_accuracy_band_id_prefix,
            spatial_geometry_type_id,
            spatial_geometry_type_id_prefix,
            spatial_location_coordinates_date,
            spatial_location_coordinates_date_prefix,
            spatial_parameter_type_id,
            spatial_parameter_type_id_prefix,
            wgs_84_coordinates,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PROJECTEDBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ProjectedBottomHoleLocationFields,
        interval: float,
        query: str | None = None,
        search_property: ProjectedBottomHoleLocationTextFields
        | Sequence[ProjectedBottomHoleLocationTextFields]
        | None = None,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for projected bottom hole locations

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            as_ingested_coordinates: The as ingested coordinate to filter on.
            coordinate_quality_check_date_time: The coordinate quality check date time to filter on.
            coordinate_quality_check_date_time_prefix: The prefix of the coordinate quality check date time to filter on.
            coordinate_quality_check_performed_by: The coordinate quality check performed by to filter on.
            coordinate_quality_check_performed_by_prefix: The prefix of the coordinate quality check performed by to filter on.
            qualitative_spatial_accuracy_type_id: The qualitative spatial accuracy type id to filter on.
            qualitative_spatial_accuracy_type_id_prefix: The prefix of the qualitative spatial accuracy type id to filter on.
            quantitative_accuracy_band_id: The quantitative accuracy band id to filter on.
            quantitative_accuracy_band_id_prefix: The prefix of the quantitative accuracy band id to filter on.
            spatial_geometry_type_id: The spatial geometry type id to filter on.
            spatial_geometry_type_id_prefix: The prefix of the spatial geometry type id to filter on.
            spatial_location_coordinates_date: The spatial location coordinates date to filter on.
            spatial_location_coordinates_date_prefix: The prefix of the spatial location coordinates date to filter on.
            spatial_parameter_type_id: The spatial parameter type id to filter on.
            spatial_parameter_type_id_prefix: The prefix of the spatial parameter type id to filter on.
            wgs_84_coordinates: The wgs 84 coordinate to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of projected bottom hole locations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_projected_bottom_hole_location_filter(
            self._view_id,
            as_ingested_coordinates,
            coordinate_quality_check_date_time,
            coordinate_quality_check_date_time_prefix,
            coordinate_quality_check_performed_by,
            coordinate_quality_check_performed_by_prefix,
            qualitative_spatial_accuracy_type_id,
            qualitative_spatial_accuracy_type_id_prefix,
            quantitative_accuracy_band_id,
            quantitative_accuracy_band_id_prefix,
            spatial_geometry_type_id,
            spatial_geometry_type_id_prefix,
            spatial_location_coordinates_date,
            spatial_location_coordinates_date_prefix,
            spatial_parameter_type_id,
            spatial_parameter_type_id_prefix,
            wgs_84_coordinates,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PROJECTEDBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        as_ingested_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        coordinate_quality_check_date_time: str | list[str] | None = None,
        coordinate_quality_check_date_time_prefix: str | None = None,
        coordinate_quality_check_performed_by: str | list[str] | None = None,
        coordinate_quality_check_performed_by_prefix: str | None = None,
        qualitative_spatial_accuracy_type_id: str | list[str] | None = None,
        qualitative_spatial_accuracy_type_id_prefix: str | None = None,
        quantitative_accuracy_band_id: str | list[str] | None = None,
        quantitative_accuracy_band_id_prefix: str | None = None,
        spatial_geometry_type_id: str | list[str] | None = None,
        spatial_geometry_type_id_prefix: str | None = None,
        spatial_location_coordinates_date: str | list[str] | None = None,
        spatial_location_coordinates_date_prefix: str | None = None,
        spatial_parameter_type_id: str | list[str] | None = None,
        spatial_parameter_type_id_prefix: str | None = None,
        wgs_84_coordinates: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ProjectedBottomHoleLocationList:
        """List/filter projected bottom hole locations

        Args:
            as_ingested_coordinates: The as ingested coordinate to filter on.
            coordinate_quality_check_date_time: The coordinate quality check date time to filter on.
            coordinate_quality_check_date_time_prefix: The prefix of the coordinate quality check date time to filter on.
            coordinate_quality_check_performed_by: The coordinate quality check performed by to filter on.
            coordinate_quality_check_performed_by_prefix: The prefix of the coordinate quality check performed by to filter on.
            qualitative_spatial_accuracy_type_id: The qualitative spatial accuracy type id to filter on.
            qualitative_spatial_accuracy_type_id_prefix: The prefix of the qualitative spatial accuracy type id to filter on.
            quantitative_accuracy_band_id: The quantitative accuracy band id to filter on.
            quantitative_accuracy_band_id_prefix: The prefix of the quantitative accuracy band id to filter on.
            spatial_geometry_type_id: The spatial geometry type id to filter on.
            spatial_geometry_type_id_prefix: The prefix of the spatial geometry type id to filter on.
            spatial_location_coordinates_date: The spatial location coordinates date to filter on.
            spatial_location_coordinates_date_prefix: The prefix of the spatial location coordinates date to filter on.
            spatial_parameter_type_id: The spatial parameter type id to filter on.
            spatial_parameter_type_id_prefix: The prefix of the spatial parameter type id to filter on.
            wgs_84_coordinates: The wgs 84 coordinate to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of projected bottom hole locations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested projected bottom hole locations

        Examples:

            List projected bottom hole locations and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> projected_bottom_hole_locations = client.projected_bottom_hole_location.list(limit=5)

        """
        filter_ = _create_projected_bottom_hole_location_filter(
            self._view_id,
            as_ingested_coordinates,
            coordinate_quality_check_date_time,
            coordinate_quality_check_date_time_prefix,
            coordinate_quality_check_performed_by,
            coordinate_quality_check_performed_by_prefix,
            qualitative_spatial_accuracy_type_id,
            qualitative_spatial_accuracy_type_id_prefix,
            quantitative_accuracy_band_id,
            quantitative_accuracy_band_id_prefix,
            spatial_geometry_type_id,
            spatial_geometry_type_id_prefix,
            spatial_location_coordinates_date,
            spatial_location_coordinates_date_prefix,
            spatial_parameter_type_id,
            spatial_parameter_type_id_prefix,
            wgs_84_coordinates,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
