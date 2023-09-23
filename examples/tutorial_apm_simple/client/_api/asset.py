from __future__ import annotations

import datetime
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

    def list(self, asset_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "Asset.children"},
        )
        filters.append(is_edge_type)
        if asset_id:
            asset_ids = [asset_id] if isinstance(asset_id, str) else asset_id
            is_assets = f.In(
                ["edge", "startNode"],
                [{"space": "tutorial_apm_simple", "externalId": ext_id} for ext_id in asset_ids],
            )
            filters.append(is_assets)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


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

    def list(self, asset_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cdf_3d_schema", "externalId": "cdf3dEntityConnection"},
        )
        filters.append(is_edge_type)
        if asset_id:
            asset_ids = [asset_id] if isinstance(asset_id, str) else asset_id
            is_assets = f.In(
                ["edge", "startNode"],
                [{"space": "cdf_3d_schema", "externalId": ext_id} for ext_id in asset_ids],
            )
            filters.append(is_assets)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class AssetAPI(TypeAPI[Asset, AssetApply, AssetList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Asset,
            class_apply_type=AssetApply,
            class_list=AssetList,
        )
        self.view_id = view_id
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

    def list(
        self,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> AssetList:
        filters = []
        if min_area_id or max_area_id:
            filters.append(dm.filters.Range(self.view_id.as_property_ref("areaId"), gte=min_area_id, lte=max_area_id))
        if min_category_id or max_category_id:
            filters.append(
                dm.filters.Range(self.view_id.as_property_ref("categoryId"), gte=min_category_id, lte=max_category_id)
            )
        if min_created_date or max_created_date:
            filters.append(
                dm.filters.Range(
                    self.view_id.as_property_ref("createdDate"), gte=min_created_date, lte=max_created_date
                )
            )
        if description and isinstance(description, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("description"), value=description))
        if description and isinstance(description, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("description"), values=description))
        if description_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("description"), value=description_prefix))
        if is_active and isinstance(is_active, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("isActive"), value=is_active))
        if is_critical_line and isinstance(is_critical_line, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("isCriticalLine"), value=is_critical_line))
        if source_db and isinstance(source_db, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("sourceDb"), value=source_db))
        if source_db and isinstance(source_db, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("sourceDb"), values=source_db))
        if source_db_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("sourceDb"), value=source_db_prefix))
        if tag and isinstance(tag, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("tag"), value=tag))
        if tag and isinstance(tag, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("tag"), values=tag))
        if tag_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("tag"), value=tag_prefix))
        if min_updated_date or max_updated_date:
            filters.append(
                dm.filters.Range(
                    self.view_id.as_property_ref("updatedDate"), gte=min_updated_date, lte=max_updated_date
                )
            )
        if external_id_prefix:
            filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
        if filter:
            filters.append(filter)

        assets = self._list(limit=limit, filter=dm.filters.And(*filters) if filters else None)

        if retrieve_edges:
            child_edges = self.children.list(assets.as_external_ids(), limit=-1)
            self._set_children(assets, child_edges)
            in_model_3_d_edges = self.in_model_3_d.list(assets.as_external_ids(), limit=-1)
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
