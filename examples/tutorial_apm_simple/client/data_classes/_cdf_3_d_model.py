from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._cdf_3_d_entity import CdfEntityApply

__all__ = ["CdfModel", "CdfModelApply", "CdfModelList", "CdfModelApplyList"]


class CdfModel(DomainModel):
    space: ClassVar[str] = "cdf_3d_schema"
    entities: Optional[list[str]] = None
    name: Optional[str] = None

    def as_apply(self) -> CdfModelApply:
        return CdfModelApply(
            external_id=self.external_id,
            entities=self.entities,
            name=self.name,
        )


class CdfModelApply(DomainModelApply):
    space: ClassVar[str] = "cdf_3d_schema"
    entities: Union[list[CdfEntityApply], list[str], None] = Field(default=None, repr=False)
    name: str

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("cdf_3d_schema", "Cdf3dModel"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        for entity in self.entities or []:
            edge = self._create_entity_edge(entity)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(entity, DomainModelApply):
                instances = entity._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_entity_edge(self, entity: Union[str, CdfEntityApply]) -> dm.EdgeApply:
        if isinstance(entity, str):
            end_node_ext_id = entity
        elif isinstance(entity, DomainModelApply):
            end_node_ext_id = entity.external_id
        else:
            raise TypeError(f"Expected str or CdfEntityApply, got {type(entity)}")

        return dm.EdgeApply(
            space="cdf_3d_schema",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cdf_3d_schema", end_node_ext_id),
        )


class CdfModelList(TypeList[CdfModel]):
    _NODE = CdfModel

    def as_apply(self) -> CdfModelApplyList:
        return CdfModelApplyList([node.as_apply() for node in self.data])


class CdfModelApplyList(TypeApplyList[CdfModelApply]):
    _NODE = CdfModelApply
