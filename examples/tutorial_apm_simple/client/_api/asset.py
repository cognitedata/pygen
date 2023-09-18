from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from tutorial_apm_simple.client.data_classes import Asset, AssetApply, AssetList


class AssetChildrenAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "Asset.children"},
        )
        if isinstance(external_id, str):
            is_asset = f.Equals(
                ["edge", "startNode"],
                {"space": "tutorial_apm_simple", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_asset))

        else:
            is_assets = f.In(
                ["edge", "startNode"],
                [{"space": "tutorial_apm_simple", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_assets))

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "Asset.children"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class AssetInmodel3dAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cdf_3d_schema", "externalId": "cdf3dEntityConnection"},
        )
        if isinstance(external_id, str):
            is_asset = f.Equals(
                ["edge", "startNode"],
                {"space": "cdf_3d_schema", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_asset))

        else:
            is_assets = f.In(
                ["edge", "startNode"],
                [{"space": "cdf_3d_schema", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_assets))

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cdf_3d_schema", "externalId": "cdf3dEntityConnection"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class AssetsAPI(TypeAPI[Asset, AssetApply, AssetList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("tutorial_apm_simple", "Asset", "beb2bebdcbb4ad"),
            class_type=Asset,
            class_apply_type=AssetApply,
            class_list=AssetList,
        )
        self.children = AssetChildrenAPI(client)
        self.in_model_3_d = AssetInmodel3dAPI(client)

    def apply(self, asset: AssetApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = asset.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(AssetApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(AssetApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Asset:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> AssetList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Asset | AssetList:
        if isinstance(external_id, str):
            asset = self._retrieve((self.sources.space, external_id))

            child_edges = self.children.retrieve(external_id)
            asset.children = [edge.end_node.external_id for edge in child_edges]
            in_model_3_d_edges = self.in_model_3_d.retrieve(external_id)
            asset.in_model_3_d = [edge.end_node.external_id for edge in in_model_3_d_edges]

            return asset
        else:
            assets = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            child_edges = self.children.retrieve(external_id)
            self._set_children(assets, child_edges)
            in_model_3_d_edges = self.in_model_3_d.retrieve(external_id)
            self._set_in_model_3_d(assets, in_model_3_d_edges)

            return assets

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> AssetList:
        assets = self._list(limit=limit)

        child_edges = self.children.list(limit=-1)
        self._set_children(assets, child_edges)
        in_model_3_d_edges = self.in_model_3_d.list(limit=-1)
        self._set_in_model_3_d(assets, in_model_3_d_edges)

        return assets

    @staticmethod
    def _set_children(assets: Sequence[Asset], child_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in child_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for asset in assets:
            node_id = asset.id_tuple()
            if node_id in edges_by_start_node:
                asset.children = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_in_model_3_d(assets: Sequence[Asset], in_model_3_d_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in in_model_3_d_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for asset in assets:
            node_id = asset.id_tuple()
            if node_id in edges_by_start_node:
                asset.in_model_3_d = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
