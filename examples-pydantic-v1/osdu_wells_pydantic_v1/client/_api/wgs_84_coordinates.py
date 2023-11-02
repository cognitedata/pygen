from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    WgsCoordinates,
    WgsCoordinatesApply,
    WgsCoordinatesList,
    WgsCoordinatesApplyList,
    WgsCoordinatesFields,
    WgsCoordinatesTextFields,
    DomainModelApply,
)
from osdu_wells_pydantic_v1.client.data_classes._wgs_84_coordinates import _WGSCOORDINATES_PROPERTIES_BY_FIELD


class WgsCoordinatesFeaturesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Wgs84Coordinates.features"},
        )
        if isinstance(external_id, str):
            is_wgs_84_coordinate = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_wgs_84_coordinate)
            )

        else:
            is_wgs_84_coordinates = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_wgs_84_coordinates)
            )

    def list(
        self,
        wgs_84_coordinate_id: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space="IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Wgs84Coordinates.features"},
        )
        filters.append(is_edge_type)
        if wgs_84_coordinate_id:
            wgs_84_coordinate_ids = (
                [wgs_84_coordinate_id] if isinstance(wgs_84_coordinate_id, str) else wgs_84_coordinate_id
            )
            is_wgs_84_coordinates = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in wgs_84_coordinate_ids],
            )
            filters.append(is_wgs_84_coordinates)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class WgsCoordinatesAPI(TypeAPI[WgsCoordinates, WgsCoordinatesApply, WgsCoordinatesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[WgsCoordinatesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WgsCoordinates,
            class_apply_type=WgsCoordinatesApply,
            class_list=WgsCoordinatesList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.features = WgsCoordinatesFeaturesAPI(client)

    def apply(
        self, wgs_84_coordinate: WgsCoordinatesApply | Sequence[WgsCoordinatesApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(wgs_84_coordinate, WgsCoordinatesApply):
            instances = wgs_84_coordinate.to_instances_apply(self._view_by_write_class)
        else:
            instances = WgsCoordinatesApplyList(wgs_84_coordinate).to_instances_apply(self._view_by_write_class)
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
    def retrieve(self, external_id: str) -> WgsCoordinates:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> WgsCoordinatesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> WgsCoordinates | WgsCoordinatesList:
        if isinstance(external_id, str):
            wgs_84_coordinate = self._retrieve((self._sources.space, external_id))

            feature_edges = self.features.retrieve(external_id)
            wgs_84_coordinate.features = [edge.end_node.external_id for edge in feature_edges]

            return wgs_84_coordinate
        else:
            wgs_84_coordinates = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            feature_edges = self.features.retrieve(external_id)
            self._set_features(wgs_84_coordinates, feature_edges)

            return wgs_84_coordinates

    def search(
        self,
        query: str,
        properties: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WgsCoordinatesList:
        filter_ = _create_filter(
            self._view_id,
            type,
            type_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _WGSCOORDINATES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        property: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] | None = None,
        group_by: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] = None,
        query: str | None = None,
        search_properties: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        property: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] | None = None,
        group_by: WgsCoordinatesFields | Sequence[WgsCoordinatesFields] | None = None,
        query: str | None = None,
        search_property: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            type,
            type_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WGSCOORDINATES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WgsCoordinatesFields,
        interval: float,
        query: str | None = None,
        search_property: WgsCoordinatesTextFields | Sequence[WgsCoordinatesTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            type,
            type_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WGSCOORDINATES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WgsCoordinatesList:
        filter_ = _create_filter(
            self._view_id,
            type,
            type_prefix,
            external_id_prefix,
            filter,
        )

        wgs_84_coordinates = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := wgs_84_coordinates.as_external_ids()) > IN_FILTER_LIMIT:
                feature_edges = self.features.list(limit=-1)
            else:
                feature_edges = self.features.list(external_ids, limit=-1)
            self._set_features(wgs_84_coordinates, feature_edges)

        return wgs_84_coordinates

    @staticmethod
    def _set_features(wgs_84_coordinates: Sequence[WgsCoordinates], feature_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in feature_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for wgs_84_coordinate in wgs_84_coordinates:
            node_id = wgs_84_coordinate.id_tuple()
            if node_id in edges_by_start_node:
                wgs_84_coordinate.features = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    type: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if type and isinstance(type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type))
    if type and isinstance(type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
