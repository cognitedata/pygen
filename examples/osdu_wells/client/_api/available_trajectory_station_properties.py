from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    AvailableTrajectoryStationProperties,
    AvailableTrajectoryStationPropertiesApply,
    AvailableTrajectoryStationPropertiesList,
    AvailableTrajectoryStationPropertiesApplyList,
    AvailableTrajectoryStationPropertiesFields,
    AvailableTrajectoryStationPropertiesTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._available_trajectory_station_properties import (
    _AVAILABLETRAJECTORYSTATIONPROPERTIES_PROPERTIES_BY_FIELD,
)


class AvailableTrajectoryStationPropertiesAPI(
    TypeAPI[
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
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(
        self,
        available_trajectory_station_property: AvailableTrajectoryStationPropertiesApply
        | Sequence[AvailableTrajectoryStationPropertiesApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) available trajectory station properties.

        Args:
            available_trajectory_station_property: Available trajectory station property or sequence of available trajectory station properties to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new available_trajectory_station_property:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import AvailableTrajectoryStationPropertiesApply
                >>> client = OSDUClient()
                >>> available_trajectory_station_property = AvailableTrajectoryStationPropertiesApply(external_id="my_available_trajectory_station_property", ...)
                >>> result = client.available_trajectory_station_properties.apply(available_trajectory_station_property)

        """
        if isinstance(available_trajectory_station_property, AvailableTrajectoryStationPropertiesApply):
            instances = available_trajectory_station_property.to_instances_apply(self._view_by_write_class)
        else:
            instances = AvailableTrajectoryStationPropertiesApplyList(
                available_trajectory_station_property
            ).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more available trajectory station property.

        Args:
            external_id: External id of the available trajectory station property to delete.
            space: The space where all the available trajectory station property are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete available_trajectory_station_property by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.available_trajectory_station_properties.delete("my_available_trajectory_station_property")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> AvailableTrajectoryStationProperties:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> AvailableTrajectoryStationPropertiesList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> AvailableTrajectoryStationProperties | AvailableTrajectoryStationPropertiesList:
        """Retrieve one or more available trajectory station properties by id(s).

        Args:
            external_id: External id or list of external ids of the available trajectory station properties.
            space: The space where all the available trajectory station properties are located.

        Returns:
            The requested available trajectory station properties.

        Examples:

            Retrieve available_trajectory_station_property by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> available_trajectory_station_property = client.available_trajectory_station_properties.retrieve("my_available_trajectory_station_property")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

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

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> available_trajectory_station_properties = client.available_trajectory_station_properties.search('my_available_trajectory_station_property')

        """
        filter_ = _create_filter(
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

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.available_trajectory_station_properties.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
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
        filter_ = _create_filter(
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

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> available_trajectory_station_properties = client.available_trajectory_station_properties.list(limit=5)

        """
        filter_ = _create_filter(
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


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    station_property_unit_id: str | list[str] | None = None,
    station_property_unit_id_prefix: str | None = None,
    trajectory_station_property_type_id: str | list[str] | None = None,
    trajectory_station_property_type_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Name"), value=name_prefix))
    if station_property_unit_id and isinstance(station_property_unit_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("StationPropertyUnitID"), value=station_property_unit_id)
        )
    if station_property_unit_id and isinstance(station_property_unit_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("StationPropertyUnitID"), values=station_property_unit_id))
    if station_property_unit_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("StationPropertyUnitID"), value=station_property_unit_id_prefix)
        )
    if trajectory_station_property_type_id and isinstance(trajectory_station_property_type_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("TrajectoryStationPropertyTypeID"), value=trajectory_station_property_type_id
            )
        )
    if trajectory_station_property_type_id and isinstance(trajectory_station_property_type_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("TrajectoryStationPropertyTypeID"), values=trajectory_station_property_type_id
            )
        )
    if trajectory_station_property_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("TrajectoryStationPropertyTypeID"),
                value=trajectory_station_property_type_id_prefix,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
