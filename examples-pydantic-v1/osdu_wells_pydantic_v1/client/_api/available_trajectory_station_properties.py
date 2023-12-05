from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    AvailableTrajectoryStationProperties,
    AvailableTrajectoryStationPropertiesApply,
    AvailableTrajectoryStationPropertiesFields,
    AvailableTrajectoryStationPropertiesList,
    AvailableTrajectoryStationPropertiesApplyList,
    AvailableTrajectoryStationPropertiesTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._available_trajectory_station_properties import (
    _AVAILABLETRAJECTORYSTATIONPROPERTIES_PROPERTIES_BY_FIELD,
    _create_available_trajectory_station_property_filter,
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
from .available_trajectory_station_properties_query import AvailableTrajectoryStationPropertiesQueryAPI


class AvailableTrajectoryStationPropertiesAPI(
    NodeAPI[
        AvailableTrajectoryStationProperties,
        AvailableTrajectoryStationPropertiesApply,
        AvailableTrajectoryStationPropertiesList,
    ]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[AvailableTrajectoryStationPropertiesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=AvailableTrajectoryStationProperties,
            class_apply_type=AvailableTrajectoryStationPropertiesApply,
            class_list=AvailableTrajectoryStationPropertiesList,
            class_apply_list=AvailableTrajectoryStationPropertiesApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        station_property_unit_id: str | list[str] | None = None,
        station_property_unit_id_prefix: str | None = None,
        trajectory_station_property_type_id: str | list[str] | None = None,
        trajectory_station_property_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> AvailableTrajectoryStationPropertiesQueryAPI[AvailableTrajectoryStationPropertiesList]:
        """Query starting at available trajectory station properties.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            station_property_unit_id: The station property unit id to filter on.
            station_property_unit_id_prefix: The prefix of the station property unit id to filter on.
            trajectory_station_property_type_id: The trajectory station property type id to filter on.
            trajectory_station_property_type_id_prefix: The prefix of the trajectory station property type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of available trajectory station properties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for available trajectory station properties.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_available_trajectory_station_property_filter(
            self._view_id,
            name,
            name_prefix,
            station_property_unit_id,
            station_property_unit_id_prefix,
            trajectory_station_property_type_id,
            trajectory_station_property_type_id_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(AvailableTrajectoryStationPropertiesList)
        return AvailableTrajectoryStationPropertiesQueryAPI(
            self._client, builder, self._view_by_write_class, filter_, limit
        )

    def apply(
        self,
        available_trajectory_station_property: AvailableTrajectoryStationPropertiesApply
        | Sequence[AvailableTrajectoryStationPropertiesApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) available trajectory station properties.

        Args:
            available_trajectory_station_property: Available trajectory station property or sequence of available trajectory station properties to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new available_trajectory_station_property:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import AvailableTrajectoryStationPropertiesApply
                >>> client = OSDUClient()
                >>> available_trajectory_station_property = AvailableTrajectoryStationPropertiesApply(external_id="my_available_trajectory_station_property", ...)
                >>> result = client.available_trajectory_station_properties.apply(available_trajectory_station_property)

        """
        return self._apply(available_trajectory_station_property, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more available trajectory station property.

        Args:
            external_id: External id of the available trajectory station property to delete.
            space: The space where all the available trajectory station property are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete available_trajectory_station_property by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.available_trajectory_station_properties.delete("my_available_trajectory_station_property")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> AvailableTrajectoryStationProperties | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> AvailableTrajectoryStationPropertiesList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> AvailableTrajectoryStationProperties | AvailableTrajectoryStationPropertiesList | None:
        """Retrieve one or more available trajectory station properties by id(s).

        Args:
            external_id: External id or list of external ids of the available trajectory station properties.
            space: The space where all the available trajectory station properties are located.

        Returns:
            The requested available trajectory station properties.

        Examples:

            Retrieve available_trajectory_station_property by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> available_trajectory_station_property = client.available_trajectory_station_properties.retrieve("my_available_trajectory_station_property")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: AvailableTrajectoryStationPropertiesTextFields
        | Sequence[AvailableTrajectoryStationPropertiesTextFields]
        | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        station_property_unit_id: str | list[str] | None = None,
        station_property_unit_id_prefix: str | None = None,
        trajectory_station_property_type_id: str | list[str] | None = None,
        trajectory_station_property_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AvailableTrajectoryStationPropertiesList:
        """Search available trajectory station properties

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            station_property_unit_id: The station property unit id to filter on.
            station_property_unit_id_prefix: The prefix of the station property unit id to filter on.
            trajectory_station_property_type_id: The trajectory station property type id to filter on.
            trajectory_station_property_type_id_prefix: The prefix of the trajectory station property type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of available trajectory station properties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results available trajectory station properties matching the query.

        Examples:

           Search for 'my_available_trajectory_station_property' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> available_trajectory_station_properties = client.available_trajectory_station_properties.search('my_available_trajectory_station_property')

        """
        filter_ = _create_available_trajectory_station_property_filter(
            self._view_id,
            name,
            name_prefix,
            station_property_unit_id,
            station_property_unit_id_prefix,
            trajectory_station_property_type_id,
            trajectory_station_property_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _AVAILABLETRAJECTORYSTATIONPROPERTIES_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AvailableTrajectoryStationPropertiesFields
        | Sequence[AvailableTrajectoryStationPropertiesFields]
        | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: AvailableTrajectoryStationPropertiesTextFields
        | Sequence[AvailableTrajectoryStationPropertiesTextFields]
        | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        station_property_unit_id: str | list[str] | None = None,
        station_property_unit_id_prefix: str | None = None,
        trajectory_station_property_type_id: str | list[str] | None = None,
        trajectory_station_property_type_id_prefix: str | None = None,
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
        property: AvailableTrajectoryStationPropertiesFields
        | Sequence[AvailableTrajectoryStationPropertiesFields]
        | None = None,
        group_by: AvailableTrajectoryStationPropertiesFields
        | Sequence[AvailableTrajectoryStationPropertiesFields] = None,
        query: str | None = None,
        search_properties: AvailableTrajectoryStationPropertiesTextFields
        | Sequence[AvailableTrajectoryStationPropertiesTextFields]
        | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        station_property_unit_id: str | list[str] | None = None,
        station_property_unit_id_prefix: str | None = None,
        trajectory_station_property_type_id: str | list[str] | None = None,
        trajectory_station_property_type_id_prefix: str | None = None,
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
        property: AvailableTrajectoryStationPropertiesFields
        | Sequence[AvailableTrajectoryStationPropertiesFields]
        | None = None,
        group_by: AvailableTrajectoryStationPropertiesFields
        | Sequence[AvailableTrajectoryStationPropertiesFields]
        | None = None,
        query: str | None = None,
        search_property: AvailableTrajectoryStationPropertiesTextFields
        | Sequence[AvailableTrajectoryStationPropertiesTextFields]
        | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        station_property_unit_id: str | list[str] | None = None,
        station_property_unit_id_prefix: str | None = None,
        trajectory_station_property_type_id: str | list[str] | None = None,
        trajectory_station_property_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across available trajectory station properties

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            station_property_unit_id: The station property unit id to filter on.
            station_property_unit_id_prefix: The prefix of the station property unit id to filter on.
            trajectory_station_property_type_id: The trajectory station property type id to filter on.
            trajectory_station_property_type_id_prefix: The prefix of the trajectory station property type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of available trajectory station properties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count available trajectory station properties in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.available_trajectory_station_properties.aggregate("count", space="my_space")

        """

        filter_ = _create_available_trajectory_station_property_filter(
            self._view_id,
            name,
            name_prefix,
            station_property_unit_id,
            station_property_unit_id_prefix,
            trajectory_station_property_type_id,
            trajectory_station_property_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _AVAILABLETRAJECTORYSTATIONPROPERTIES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: AvailableTrajectoryStationPropertiesFields,
        interval: float,
        query: str | None = None,
        search_property: AvailableTrajectoryStationPropertiesTextFields
        | Sequence[AvailableTrajectoryStationPropertiesTextFields]
        | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        station_property_unit_id: str | list[str] | None = None,
        station_property_unit_id_prefix: str | None = None,
        trajectory_station_property_type_id: str | list[str] | None = None,
        trajectory_station_property_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for available trajectory station properties

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            station_property_unit_id: The station property unit id to filter on.
            station_property_unit_id_prefix: The prefix of the station property unit id to filter on.
            trajectory_station_property_type_id: The trajectory station property type id to filter on.
            trajectory_station_property_type_id_prefix: The prefix of the trajectory station property type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of available trajectory station properties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_available_trajectory_station_property_filter(
            self._view_id,
            name,
            name_prefix,
            station_property_unit_id,
            station_property_unit_id_prefix,
            trajectory_station_property_type_id,
            trajectory_station_property_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _AVAILABLETRAJECTORYSTATIONPROPERTIES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        station_property_unit_id: str | list[str] | None = None,
        station_property_unit_id_prefix: str | None = None,
        trajectory_station_property_type_id: str | list[str] | None = None,
        trajectory_station_property_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AvailableTrajectoryStationPropertiesList:
        """List/filter available trajectory station properties

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            station_property_unit_id: The station property unit id to filter on.
            station_property_unit_id_prefix: The prefix of the station property unit id to filter on.
            trajectory_station_property_type_id: The trajectory station property type id to filter on.
            trajectory_station_property_type_id_prefix: The prefix of the trajectory station property type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of available trajectory station properties to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested available trajectory station properties

        Examples:

            List available trajectory station properties and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> available_trajectory_station_properties = client.available_trajectory_station_properties.list(limit=5)

        """
        filter_ = _create_available_trajectory_station_property_filter(
            self._view_id,
            name,
            name_prefix,
            station_property_unit_id,
            station_property_unit_id_prefix,
            trajectory_station_property_type_id,
            trajectory_station_property_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
