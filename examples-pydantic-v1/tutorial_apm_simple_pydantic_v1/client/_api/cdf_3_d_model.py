from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from tutorial_apm_simple_pydantic_v1.client.data_classes import CdfModel, CdfModelApply, CdfModelList


class Cdf3dModelEntitiesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cdf_3d_schema", "externalId": "cdf3dEntityConnection"},
        )
        if isinstance(external_id, str):
            is_cdf_3_d_model = f.Equals(
                ["edge", "startNode"],
                {"space": "cdf_3d_schema", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_cdf_3_d_model)
            )

        else:
            is_cdf_3_d_models = f.In(
                ["edge", "startNode"],
                [{"space": "cdf_3d_schema", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_cdf_3_d_models)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cdf_3d_schema", "externalId": "cdf3dEntityConnection"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class CdfModelAPI(TypeAPI[CdfModel, CdfModelApply, CdfModelList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cdf_3d_schema", "Cdf3dModel", "1"),
            class_type=CdfModel,
            class_apply_type=CdfModelApply,
            class_list=CdfModelList,
        )
        self.entities = Cdf3dModelEntitiesAPI(client)

    def apply(self, cdf_3_d_model: CdfModelApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = cdf_3_d_model.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CdfModelApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CdfModelApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CdfModel:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CdfModelList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> CdfModel | CdfModelList:
        if isinstance(external_id, str):
            cdf_3_d_model = self._retrieve((self.sources.space, external_id))

            entity_edges = self.entities.retrieve(external_id)
            cdf_3_d_model.entities = [edge.end_node.external_id for edge in entity_edges]

            return cdf_3_d_model
        else:
            cdf_3_d_models = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            entity_edges = self.entities.retrieve(external_id)
            self._set_entities(cdf_3_d_models, entity_edges)

            return cdf_3_d_models

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> CdfModelList:
        cdf_3_d_models = self._list(limit=limit)

        entity_edges = self.entities.list(limit=-1)
        self._set_entities(cdf_3_d_models, entity_edges)

        return cdf_3_d_models

    @staticmethod
    def _set_entities(cdf_3_d_models: Sequence[CdfModel], entity_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in entity_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for cdf_3_d_model in cdf_3_d_models:
            node_id = cdf_3_d_model.id_tuple()
            if node_id in edges_by_start_node:
                cdf_3_d_model.entities = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
