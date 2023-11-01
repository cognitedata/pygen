from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    AvailableTrajectoryStationProperties,
    AvailableTrajectoryStationPropertiesApply,
    AvailableTrajectoryStationPropertiesList,
    AvailableTrajectoryStationPropertiesApplyList,
    AvailableTrajectoryStationPropertiesFields,
    AvailableTrajectoryStationPropertiesTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._available_trajectory_station_properties import (
    _AVAILABLETRAJECTORYSTATIONPROPERTIES_PROPERTIES_BY_FIELD,
)


class AvailableTrajectoryStationPropertiesAPI(
    TypeAPI[
        AvailableTrajectoryStationProperties,
        AvailableTrajectoryStationPropertiesApply,
        AvailableTrajectoryStationPropertiesList,
    ]
):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=AvailableTrajectoryStationProperties,
            class_apply_type=AvailableTrajectoryStationPropertiesApply,
            class_list=AvailableTrajectoryStationPropertiesList,
        )
        self._view_id = view_id

    def apply(
        self,
        available_trajectory_station_property: AvailableTrajectoryStationPropertiesApply
        | Sequence[AvailableTrajectoryStationPropertiesApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        if isinstance(available_trajectory_station_property, AvailableTrajectoryStationPropertiesApply):
            instances = available_trajectory_station_property.to_instances_apply(self._view_id)
        else:
            instances = AvailableTrajectoryStationPropertiesApplyList(
                available_trajectory_station_property
            ).to_instances_apply(self._view_id)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.InstancesDeleteResult:
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
        self, external_id: str | Sequence[str]
    ) -> AvailableTrajectoryStationProperties | AvailableTrajectoryStationPropertiesList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AvailableTrajectoryStationPropertiesList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            station_property_unit_id,
            station_property_unit_id_prefix,
            trajectory_station_property_type_id,
            trajectory_station_property_type_id_prefix,
            external_id_prefix,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            station_property_unit_id,
            station_property_unit_id_prefix,
            trajectory_station_property_type_id,
            trajectory_station_property_type_id_prefix,
            external_id_prefix,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            station_property_unit_id,
            station_property_unit_id_prefix,
            trajectory_station_property_type_id,
            trajectory_station_property_type_id_prefix,
            external_id_prefix,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AvailableTrajectoryStationPropertiesList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            station_property_unit_id,
            station_property_unit_id_prefix,
            trajectory_station_property_type_id,
            trajectory_station_property_type_id_prefix,
            external_id_prefix,
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
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
