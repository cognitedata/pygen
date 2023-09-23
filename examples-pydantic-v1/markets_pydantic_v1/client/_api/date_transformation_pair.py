from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from markets_pydantic_v1.client.data_classes import (
    DateTransformationPair,
    DateTransformationPairApply,
    DateTransformationPairList,
    DateTransformationPairApplyList,
)


class DateTransformationPairEndAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "market", "externalId": "DateTransformationPair.end"},
        )
        if isinstance(external_id, str):
            is_date_transformation_pair = f.Equals(
                ["edge", "startNode"],
                {"space": "market", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_date_transformation_pair)
            )

        else:
            is_date_transformation_pairs = f.In(
                ["edge", "startNode"],
                [{"space": "market", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_date_transformation_pairs)
            )

    def list(self, date_transformation_pair_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "market", "externalId": "DateTransformationPair.end"},
        )
        filters.append(is_edge_type)
        if date_transformation_pair_id:
            date_transformation_pair_ids = (
                [date_transformation_pair_id]
                if isinstance(date_transformation_pair_id, str)
                else date_transformation_pair_id
            )
            is_date_transformation_pairs = f.In(
                ["edge", "startNode"],
                [{"space": "market", "externalId": ext_id} for ext_id in date_transformation_pair_ids],
            )
            filters.append(is_date_transformation_pairs)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DateTransformationPairStartAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "market", "externalId": "DateTransformationPair.start"},
        )
        if isinstance(external_id, str):
            is_date_transformation_pair = f.Equals(
                ["edge", "startNode"],
                {"space": "market", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_date_transformation_pair)
            )

        else:
            is_date_transformation_pairs = f.In(
                ["edge", "startNode"],
                [{"space": "market", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_date_transformation_pairs)
            )

    def list(self, date_transformation_pair_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "market", "externalId": "DateTransformationPair.start"},
        )
        filters.append(is_edge_type)
        if date_transformation_pair_id:
            date_transformation_pair_ids = (
                [date_transformation_pair_id]
                if isinstance(date_transformation_pair_id, str)
                else date_transformation_pair_id
            )
            is_date_transformation_pairs = f.In(
                ["edge", "startNode"],
                [{"space": "market", "externalId": ext_id} for ext_id in date_transformation_pair_ids],
            )
            filters.append(is_date_transformation_pairs)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DateTransformationPairAPI(
    TypeAPI[DateTransformationPair, DateTransformationPairApply, DateTransformationPairList]
):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=DateTransformationPair,
            class_apply_type=DateTransformationPairApply,
            class_list=DateTransformationPairList,
        )
        self.view_id = view_id
        self.end = DateTransformationPairEndAPI(client)
        self.start = DateTransformationPairStartAPI(client)

    def apply(
        self,
        date_transformation_pair: DateTransformationPairApply | Sequence[DateTransformationPairApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        if isinstance(date_transformation_pair, DateTransformationPairApply):
            instances = date_transformation_pair.to_instances_apply()
        else:
            instances = DateTransformationPairApplyList(date_transformation_pair).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(DateTransformationPairApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(DateTransformationPairApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DateTransformationPair:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DateTransformationPairList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DateTransformationPair | DateTransformationPairList:
        if isinstance(external_id, str):
            date_transformation_pair = self._retrieve((self.sources.space, external_id))

            end_edges = self.end.retrieve(external_id)
            date_transformation_pair.end = [edge.end_node.external_id for edge in end_edges]
            start_edges = self.start.retrieve(external_id)
            date_transformation_pair.start = [edge.end_node.external_id for edge in start_edges]

            return date_transformation_pair
        else:
            date_transformation_pairs = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            end_edges = self.end.retrieve(external_id)
            self._set_end(date_transformation_pairs, end_edges)
            start_edges = self.start.retrieve(external_id)
            self._set_start(date_transformation_pairs, start_edges)

            return date_transformation_pairs

    def list(
        self,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> DateTransformationPairList:
        filters = []
        if external_id_prefix:
            filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
        if filter:
            filters.append(filter)

        date_transformation_pairs = self._list(limit=limit, filter=dm.filters.And(*filters) if filters else None)

        if retrieve_edges:
            end_edges = self.end.list(date_transformation_pairs.as_external_ids(), limit=-1)
            self._set_end(date_transformation_pairs, end_edges)
            start_edges = self.start.list(date_transformation_pairs.as_external_ids(), limit=-1)
            self._set_start(date_transformation_pairs, start_edges)

        return date_transformation_pairs

    @staticmethod
    def _set_end(date_transformation_pairs: Sequence[DateTransformationPair], end_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in end_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for date_transformation_pair in date_transformation_pairs:
            node_id = date_transformation_pair.id_tuple()
            if node_id in edges_by_start_node:
                date_transformation_pair.end = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_start(date_transformation_pairs: Sequence[DateTransformationPair], start_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in start_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for date_transformation_pair in date_transformation_pairs:
            node_id = date_transformation_pair.id_tuple()
            if node_id in edges_by_start_node:
                date_transformation_pair.start = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
