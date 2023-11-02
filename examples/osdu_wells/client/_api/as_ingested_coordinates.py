from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    AsIngestedCoordinates,
    AsIngestedCoordinatesApply,
    AsIngestedCoordinatesList,
    AsIngestedCoordinatesApplyList,
    AsIngestedCoordinatesFields,
    AsIngestedCoordinatesTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._as_ingested_coordinates import _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD


class AsIngestedCoordinatesFeaturesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "AsIngestedCoordinates.features"},
        )
        if isinstance(external_id, str):
            is_as_ingested_coordinate = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_as_ingested_coordinate)
            )

        else:
            is_as_ingested_coordinates = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_as_ingested_coordinates)
            )

    def list(
        self,
        as_ingested_coordinate_id: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space="IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "AsIngestedCoordinates.features"},
        )
        filters.append(is_edge_type)
        if as_ingested_coordinate_id:
            as_ingested_coordinate_ids = (
                [as_ingested_coordinate_id] if isinstance(as_ingested_coordinate_id, str) else as_ingested_coordinate_id
            )
            is_as_ingested_coordinates = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in as_ingested_coordinate_ids],
            )
            filters.append(is_as_ingested_coordinates)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class AsIngestedCoordinatesAPI(TypeAPI[AsIngestedCoordinates, AsIngestedCoordinatesApply, AsIngestedCoordinatesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[AsIngestedCoordinatesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=AsIngestedCoordinates,
            class_apply_type=AsIngestedCoordinatesApply,
            class_list=AsIngestedCoordinatesList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.features = AsIngestedCoordinatesFeaturesAPI(client)

    def apply(
        self,
        as_ingested_coordinate: AsIngestedCoordinatesApply | Sequence[AsIngestedCoordinatesApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        if isinstance(as_ingested_coordinate, AsIngestedCoordinatesApply):
            instances = as_ingested_coordinate.to_instances_apply(self._view_by_write_class)
        else:
            instances = AsIngestedCoordinatesApplyList(as_ingested_coordinate).to_instances_apply(
                self._view_by_write_class
            )
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
    def retrieve(self, external_id: str) -> AsIngestedCoordinates:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> AsIngestedCoordinatesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> AsIngestedCoordinates | AsIngestedCoordinatesList:
        if isinstance(external_id, str):
            as_ingested_coordinate = self._retrieve((self._sources.space, external_id))

            feature_edges = self.features.retrieve(external_id)
            as_ingested_coordinate.features = [edge.end_node.external_id for edge in feature_edges]

            return as_ingested_coordinate
        else:
            as_ingested_coordinates = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            feature_edges = self.features.retrieve(external_id)
            self._set_features(as_ingested_coordinates, feature_edges)

            return as_ingested_coordinates

    def search(
        self,
        query: str,
        properties: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AsIngestedCoordinatesList:
        filter_ = _create_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        property: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] | None = None,
        group_by: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] = None,
        query: str | None = None,
        search_properties: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        property: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] | None = None,
        group_by: AsIngestedCoordinatesFields | Sequence[AsIngestedCoordinatesFields] | None = None,
        query: str | None = None,
        search_property: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: AsIngestedCoordinatesFields,
        interval: float,
        query: str | None = None,
        search_property: AsIngestedCoordinatesTextFields | Sequence[AsIngestedCoordinatesTextFields] | None = None,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        coordinate_reference_system_id: str | list[str] | None = None,
        coordinate_reference_system_id_prefix: str | None = None,
        vertical_coordinate_reference_system_id: str | list[str] | None = None,
        vertical_coordinate_reference_system_id_prefix: str | None = None,
        vertical_unit_id: str | list[str] | None = None,
        vertical_unit_id_prefix: str | None = None,
        persistable_reference_crs: str | list[str] | None = None,
        persistable_reference_crs_prefix: str | None = None,
        persistable_reference_unit_z: str | list[str] | None = None,
        persistable_reference_unit_z_prefix: str | None = None,
        persistable_reference_vertical_crs: str | list[str] | None = None,
        persistable_reference_vertical_crs_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> AsIngestedCoordinatesList:
        filter_ = _create_filter(
            self._view_id,
            coordinate_reference_system_id,
            coordinate_reference_system_id_prefix,
            vertical_coordinate_reference_system_id,
            vertical_coordinate_reference_system_id_prefix,
            vertical_unit_id,
            vertical_unit_id_prefix,
            persistable_reference_crs,
            persistable_reference_crs_prefix,
            persistable_reference_unit_z,
            persistable_reference_unit_z_prefix,
            persistable_reference_vertical_crs,
            persistable_reference_vertical_crs_prefix,
            type,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )

        as_ingested_coordinates = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := as_ingested_coordinates.as_external_ids()) > IN_FILTER_LIMIT:
                feature_edges = self.features.list(limit=-1)
            else:
                feature_edges = self.features.list(external_ids, limit=-1)
            self._set_features(as_ingested_coordinates, feature_edges)

        return as_ingested_coordinates

    @staticmethod
    def _set_features(as_ingested_coordinates: Sequence[AsIngestedCoordinates], feature_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in feature_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for as_ingested_coordinate in as_ingested_coordinates:
            node_id = as_ingested_coordinate.id_tuple()
            if node_id in edges_by_start_node:
                as_ingested_coordinate.features = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    coordinate_reference_system_id: str | list[str] | None = None,
    coordinate_reference_system_id_prefix: str | None = None,
    vertical_coordinate_reference_system_id: str | list[str] | None = None,
    vertical_coordinate_reference_system_id_prefix: str | None = None,
    vertical_unit_id: str | list[str] | None = None,
    vertical_unit_id_prefix: str | None = None,
    persistable_reference_crs: str | list[str] | None = None,
    persistable_reference_crs_prefix: str | None = None,
    persistable_reference_unit_z: str | list[str] | None = None,
    persistable_reference_unit_z_prefix: str | None = None,
    persistable_reference_vertical_crs: str | list[str] | None = None,
    persistable_reference_vertical_crs_prefix: str | None = None,
    type: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if coordinate_reference_system_id and isinstance(coordinate_reference_system_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("CoordinateReferenceSystemID"), value=coordinate_reference_system_id
            )
        )
    if coordinate_reference_system_id and isinstance(coordinate_reference_system_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("CoordinateReferenceSystemID"), values=coordinate_reference_system_id)
        )
    if coordinate_reference_system_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("CoordinateReferenceSystemID"), value=coordinate_reference_system_id_prefix
            )
        )
    if vertical_coordinate_reference_system_id and isinstance(vertical_coordinate_reference_system_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalCoordinateReferenceSystemID"),
                value=vertical_coordinate_reference_system_id,
            )
        )
    if vertical_coordinate_reference_system_id and isinstance(vertical_coordinate_reference_system_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("VerticalCoordinateReferenceSystemID"),
                values=vertical_coordinate_reference_system_id,
            )
        )
    if vertical_coordinate_reference_system_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalCoordinateReferenceSystemID"),
                value=vertical_coordinate_reference_system_id_prefix,
            )
        )
    if vertical_unit_id and isinstance(vertical_unit_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("VerticalUnitID"), value=vertical_unit_id))
    if vertical_unit_id and isinstance(vertical_unit_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("VerticalUnitID"), values=vertical_unit_id))
    if vertical_unit_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("VerticalUnitID"), value=vertical_unit_id_prefix))
    if persistable_reference_crs and isinstance(persistable_reference_crs, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("persistableReferenceCrs"), value=persistable_reference_crs)
        )
    if persistable_reference_crs and isinstance(persistable_reference_crs, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("persistableReferenceCrs"), values=persistable_reference_crs)
        )
    if persistable_reference_crs_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("persistableReferenceCrs"), value=persistable_reference_crs_prefix
            )
        )
    if persistable_reference_unit_z and isinstance(persistable_reference_unit_z, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("persistableReferenceUnitZ"), value=persistable_reference_unit_z)
        )
    if persistable_reference_unit_z and isinstance(persistable_reference_unit_z, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("persistableReferenceUnitZ"), values=persistable_reference_unit_z)
        )
    if persistable_reference_unit_z_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("persistableReferenceUnitZ"), value=persistable_reference_unit_z_prefix
            )
        )
    if persistable_reference_vertical_crs and isinstance(persistable_reference_vertical_crs, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("persistableReferenceVerticalCrs"), value=persistable_reference_vertical_crs
            )
        )
    if persistable_reference_vertical_crs and isinstance(persistable_reference_vertical_crs, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("persistableReferenceVerticalCrs"), values=persistable_reference_vertical_crs
            )
        )
    if persistable_reference_vertical_crs_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("persistableReferenceVerticalCrs"),
                value=persistable_reference_vertical_crs_prefix,
            )
        )
    if type and isinstance(type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type))
    if type and isinstance(type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
