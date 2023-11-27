from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    GeographicBottomHoleLocation,
    GeographicBottomHoleLocationApply,
    GeographicBottomHoleLocationFields,
    GeographicBottomHoleLocationList,
    GeographicBottomHoleLocationApplyList,
    GeographicBottomHoleLocationTextFields,
)
from osdu_wells.client.data_classes._geographic_bottom_hole_location import (
    _GEOGRAPHICBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD,
    _create_geographic_bottom_hole_location_filter,
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
from .geographic_bottom_hole_location_query import GeographicBottomHoleLocationQueryAPI


class GeographicBottomHoleLocationAPI(
    NodeAPI[GeographicBottomHoleLocation, GeographicBottomHoleLocationApply, GeographicBottomHoleLocationList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[GeographicBottomHoleLocationApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=GeographicBottomHoleLocation,
            class_apply_type=GeographicBottomHoleLocationApply,
            class_list=GeographicBottomHoleLocationList,
            class_apply_list=GeographicBottomHoleLocationApplyList,
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
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> GeographicBottomHoleLocationQueryAPI[GeographicBottomHoleLocationList]:
        """Query starting at geographic bottom hole locations.

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
            limit: Maximum number of geographic bottom hole locations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for geographic bottom hole locations.

        """
        filter_ = _create_geographic_bottom_hole_location_filter(
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
            GeographicBottomHoleLocationList,
            [
                QueryStep(
                    name="geographic_bottom_hole_location",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_id, list(_GEOGRAPHICBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD.values())
                            )
                        ]
                    ),
                    result_cls=GeographicBottomHoleLocation,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return GeographicBottomHoleLocationQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self,
        geographic_bottom_hole_location: GeographicBottomHoleLocationApply
        | Sequence[GeographicBottomHoleLocationApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) geographic bottom hole locations.

        Args:
            geographic_bottom_hole_location: Geographic bottom hole location or sequence of geographic bottom hole locations to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new geographic_bottom_hole_location:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import GeographicBottomHoleLocationApply
                >>> client = OSDUClient()
                >>> geographic_bottom_hole_location = GeographicBottomHoleLocationApply(external_id="my_geographic_bottom_hole_location", ...)
                >>> result = client.geographic_bottom_hole_location.apply(geographic_bottom_hole_location)

        """
        return self._apply(geographic_bottom_hole_location, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more geographic bottom hole location.

        Args:
            external_id: External id of the geographic bottom hole location to delete.
            space: The space where all the geographic bottom hole location are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete geographic_bottom_hole_location by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.geographic_bottom_hole_location.delete("my_geographic_bottom_hole_location")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> GeographicBottomHoleLocation:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> GeographicBottomHoleLocationList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> GeographicBottomHoleLocation | GeographicBottomHoleLocationList:
        """Retrieve one or more geographic bottom hole locations by id(s).

        Args:
            external_id: External id or list of external ids of the geographic bottom hole locations.
            space: The space where all the geographic bottom hole locations are located.

        Returns:
            The requested geographic bottom hole locations.

        Examples:

            Retrieve geographic_bottom_hole_location by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> geographic_bottom_hole_location = client.geographic_bottom_hole_location.retrieve("my_geographic_bottom_hole_location")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: GeographicBottomHoleLocationTextFields
        | Sequence[GeographicBottomHoleLocationTextFields]
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
    ) -> GeographicBottomHoleLocationList:
        """Search geographic bottom hole locations

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
            limit: Maximum number of geographic bottom hole locations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results geographic bottom hole locations matching the query.

        Examples:

           Search for 'my_geographic_bottom_hole_location' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> geographic_bottom_hole_locations = client.geographic_bottom_hole_location.search('my_geographic_bottom_hole_location')

        """
        filter_ = _create_geographic_bottom_hole_location_filter(
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
            self._view_id, query, _GEOGRAPHICBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: GeographicBottomHoleLocationFields | Sequence[GeographicBottomHoleLocationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: GeographicBottomHoleLocationTextFields
        | Sequence[GeographicBottomHoleLocationTextFields]
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
        property: GeographicBottomHoleLocationFields | Sequence[GeographicBottomHoleLocationFields] | None = None,
        group_by: GeographicBottomHoleLocationFields | Sequence[GeographicBottomHoleLocationFields] = None,
        query: str | None = None,
        search_properties: GeographicBottomHoleLocationTextFields
        | Sequence[GeographicBottomHoleLocationTextFields]
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
        property: GeographicBottomHoleLocationFields | Sequence[GeographicBottomHoleLocationFields] | None = None,
        group_by: GeographicBottomHoleLocationFields | Sequence[GeographicBottomHoleLocationFields] | None = None,
        query: str | None = None,
        search_property: GeographicBottomHoleLocationTextFields
        | Sequence[GeographicBottomHoleLocationTextFields]
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
        """Aggregate data across geographic bottom hole locations

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
            limit: Maximum number of geographic bottom hole locations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count geographic bottom hole locations in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.geographic_bottom_hole_location.aggregate("count", space="my_space")

        """

        filter_ = _create_geographic_bottom_hole_location_filter(
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
            _GEOGRAPHICBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: GeographicBottomHoleLocationFields,
        interval: float,
        query: str | None = None,
        search_property: GeographicBottomHoleLocationTextFields
        | Sequence[GeographicBottomHoleLocationTextFields]
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
        """Produces histograms for geographic bottom hole locations

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
            limit: Maximum number of geographic bottom hole locations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_geographic_bottom_hole_location_filter(
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
            _GEOGRAPHICBOTTOMHOLELOCATION_PROPERTIES_BY_FIELD,
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
    ) -> GeographicBottomHoleLocationList:
        """List/filter geographic bottom hole locations

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
            limit: Maximum number of geographic bottom hole locations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested geographic bottom hole locations

        Examples:

            List geographic bottom hole locations and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> geographic_bottom_hole_locations = client.geographic_bottom_hole_location.list(limit=5)

        """
        filter_ = _create_geographic_bottom_hole_location_filter(
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
