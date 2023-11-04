from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from markets.client.data_classes import (
    DateTransformationPair,
    DateTransformationPairApply,
    DateTransformationPairList,
    DateTransformationPairApplyList,
)


class DateTransformationPairEndAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="market") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "DateTransformationPair.end"},
        )
        if isinstance(external_id, str):
            is_date_transformation_pair = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_date_transformation_pair)
            )

        else:
            is_date_transformation_pairs = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_date_transformation_pairs)
            )

    def list(
        self, date_transformation_pair_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="market"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "DateTransformationPair.end"},
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
                [{"space": space, "externalId": ext_id} for ext_id in date_transformation_pair_ids],
            )
            filters.append(is_date_transformation_pairs)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DateTransformationPairStartAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="market") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "DateTransformationPair.start"},
        )
        if isinstance(external_id, str):
            is_date_transformation_pair = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_date_transformation_pair)
            )

        else:
            is_date_transformation_pairs = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_date_transformation_pairs)
            )

    def list(
        self, date_transformation_pair_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="market"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "DateTransformationPair.start"},
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
                [{"space": space, "externalId": ext_id} for ext_id in date_transformation_pair_ids],
            )
            filters.append(is_date_transformation_pairs)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DateTransformationPairAPI(
    TypeAPI[DateTransformationPair, DateTransformationPairApply, DateTransformationPairList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[DateTransformationPairApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=DateTransformationPair,
            class_apply_type=DateTransformationPairApply,
            class_list=DateTransformationPairList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.end = DateTransformationPairEndAPI(client)
        self.start = DateTransformationPairStartAPI(client)

    def apply(
        self,
        date_transformation_pair: DateTransformationPairApply | Sequence[DateTransformationPairApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) date transformation pairs.

        Note: This method iterates through all nodes linked to date_transformation_pair and create them including the edges
        between the nodes. For example, if any of `end` or `start` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            date_transformation_pair: Date transformation pair or sequence of date transformation pairs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            InstancesApplyResult: Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new date_transformation_pair:

                >>> from markets.client import MarketClient
                >>> from markets.client.data_classes import DateTransformationPairApply
                >>> client = MarketClient()
                >>> date_transformation_pair = DateTransformationPairApply(external_id="my_date_transformation_pair", ...)
                >>> result = client.date_transformation_pair.apply(date_transformation_pair)

        """
        if isinstance(date_transformation_pair, DateTransformationPairApply):
            instances = date_transformation_pair.to_instances_apply(self._view_by_write_class)
        else:
            instances = DateTransformationPairApplyList(date_transformation_pair).to_instances_apply(
                self._view_by_write_class
            )
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="market") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DateTransformationPair:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DateTransformationPairList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DateTransformationPair | DateTransformationPairList:
        if isinstance(external_id, str):
            date_transformation_pair = self._retrieve((self._sources.space, external_id))

            end_edges = self.end.retrieve(external_id)
            date_transformation_pair.end = [edge.end_node.external_id for edge in end_edges]
            start_edges = self.start.retrieve(external_id)
            date_transformation_pair.start = [edge.end_node.external_id for edge in start_edges]

            return date_transformation_pair
        else:
            date_transformation_pairs = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            end_edges = self.end.retrieve(external_id)
            self._set_end(date_transformation_pairs, end_edges)
            start_edges = self.start.retrieve(external_id)
            self._set_start(date_transformation_pairs, start_edges)

            return date_transformation_pairs

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> DateTransformationPairList:
        filter_ = _create_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )

        date_transformation_pairs = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := date_transformation_pairs.as_external_ids()) > IN_FILTER_LIMIT:
                end_edges = self.end.list(limit=-1)
            else:
                end_edges = self.end.list(external_ids, limit=-1)
            self._set_end(date_transformation_pairs, end_edges)
            if len(external_ids := date_transformation_pairs.as_external_ids()) > IN_FILTER_LIMIT:
                start_edges = self.start.list(limit=-1)
            else:
                start_edges = self.start.list(external_ids, limit=-1)
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


def _create_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
