from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from tutorial_apm_simple.client.data_classes import CdfEntity, CdfEntityApply, CdfEntityList


class Cdf3dEntityInmodel3dAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cdf_3d_schema", "externalId": "cdf3dEntityConnection"},
        )
        if isinstance(external_id, str):
            is_cdf_3_d_entity = f.Equals(
                ["edge", "startNode"],
                {"space": "cdf_3d_schema", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_cdf_3_d_entity)
            )

        else:
            is_cdf_3_d_entities = f.In(
                ["edge", "startNode"],
                [{"space": "cdf_3d_schema", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_cdf_3_d_entities)
            )

    def list(self, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cdf_3d_schema", "externalId": "cdf3dEntityConnection"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class CdfEntityAPI(TypeAPI[CdfEntity, CdfEntityApply, CdfEntityList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cdf_3d_schema", "Cdf3dEntity", "1"),
            class_type=CdfEntity,
            class_apply_type=CdfEntityApply,
            class_list=CdfEntityList,
        )
        self.in_model_3_d = Cdf3dEntityInmodel3dAPI(client)

    def apply(self, cdf_3_d_entity: CdfEntityApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = cdf_3_d_entity.to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CdfEntityApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CdfEntityApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CdfEntity:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CdfEntityList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> CdfEntity | CdfEntityList:
        if isinstance(external_id, str):
            cdf_3_d_entity = self._retrieve((self.sources.space, external_id))

            in_model_3_d_edges = self.in_model_3_d.retrieve(external_id)
            cdf_3_d_entity.in_model_3_d = [edge.end_node.external_id for edge in in_model_3_d_edges]

            return cdf_3_d_entity
        else:
            cdf_3_d_entities = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            in_model_3_d_edges = self.in_model_3_d.retrieve(external_id)
            self._set_in_model_3_d(cdf_3_d_entities, in_model_3_d_edges)

            return cdf_3_d_entities

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> CdfEntityList:
        cdf_3_d_entities = self._list(limit=limit)

        in_model_3_d_edges = self.in_model_3_d.list(limit=-1)
        self._set_in_model_3_d(cdf_3_d_entities, in_model_3_d_edges)

        return cdf_3_d_entities

    @staticmethod
    def _set_in_model_3_d(cdf_3_d_entities: Sequence[CdfEntity], in_model_3_d_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in in_model_3_d_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for cdf_3_d_entity in cdf_3_d_entities:
            node_id = cdf_3_d_entity.id_tuple()
            if node_id in edges_by_start_node:
                cdf_3_d_entity.in_model_3_d = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
