from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from tutorial_apm_simple.client.data_classes import CdfModel, CdfModelApply, CdfModelList, CdfModelApplyList


class CdfModelEntitiesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="cdf_3d_schema") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "cdf3dEntityConnection"},
        )
        if isinstance(external_id, str):
            is_cdf_3_d_model = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_cdf_3_d_model)
            )

        else:
            is_cdf_3_d_models = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_cdf_3_d_models)
            )

    def list(
        self, cdf_3_d_model_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="cdf_3d_schema"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "cdf3dEntityConnection"},
        )
        filters.append(is_edge_type)
        if cdf_3_d_model_id:
            cdf_3_d_model_ids = [cdf_3_d_model_id] if isinstance(cdf_3_d_model_id, str) else cdf_3_d_model_id
            is_cdf_3_d_models = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in cdf_3_d_model_ids],
            )
            filters.append(is_cdf_3_d_models)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class CdfModelAPI(TypeAPI[CdfModel, CdfModelApply, CdfModelList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CdfModel,
            class_apply_type=CdfModelApply,
            class_list=CdfModelList,
        )
        self._view_id = view_id
        self.entities = CdfModelEntitiesAPI(client)

    def apply(
        self, cdf_3_d_model: CdfModelApply | Sequence[CdfModelApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(cdf_3_d_model, CdfModelApply):
            instances = cdf_3_d_model.to_instances_apply()
        else:
            instances = CdfModelApplyList(cdf_3_d_model).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="cdf_3d_schema") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> CdfModel:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CdfModelList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> CdfModel | CdfModelList:
        if isinstance(external_id, str):
            cdf_3_d_model = self._retrieve((self._sources.space, external_id))

            entity_edges = self.entities.retrieve(external_id)
            cdf_3_d_model.entities = [edge.end_node.external_id for edge in entity_edges]

            return cdf_3_d_model
        else:
            cdf_3_d_models = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            entity_edges = self.entities.retrieve(external_id)
            self._set_entities(cdf_3_d_models, entity_edges)

            return cdf_3_d_models

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = False,
    ) -> CdfModelList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            filter,
        )

        cdf_3_d_models = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            entity_edges = self.entities.list(cdf_3_d_models.as_external_ids(), limit=-1)
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


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
