from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._cdf_3_d_model import CdfModelApply

__all__ = ["CdfEntity", "CdfEntityApply", "CdfEntityList", "CdfEntityApplyList"]


class CdfEntity(DomainModel):
    space: str = "cdf_3d_schema"
    in_model_3_d: Optional[list[str]] = Field(None, alias="inModel3d")

    def as_apply(self) -> CdfEntityApply:
        return CdfEntityApply(
            space=self.space,
            external_id=self.external_id,
            in_model_3_d=self.in_model_3_d,
        )


class CdfEntityApply(DomainModelApply):
    space: str = "cdf_3d_schema"
    in_model_3_d: Union[list[CdfModelApply], list[str], None] = Field(default=None, repr=False, alias="inModel3d")

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        nodes = []

        edges = []
        cache.add(self.external_id)

        for in_model_3_d in self.in_model_3_d or []:
            edge = self._create_in_model_3_d_edge(in_model_3_d)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(in_model_3_d, DomainModelApply):
                instances = in_model_3_d._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_in_model_3_d_edge(self, in_model_3_d: Union[str, CdfModelApply]) -> dm.EdgeApply:
        if isinstance(in_model_3_d, str):
            end_node_ext_id = in_model_3_d
        elif isinstance(in_model_3_d, DomainModelApply):
            end_node_ext_id = in_model_3_d.external_id
        else:
            raise TypeError(f"Expected str or CdfModelApply, got {type(in_model_3_d)}")

        return dm.EdgeApply(
            space="cdf_3d_schema",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cdf_3d_schema", end_node_ext_id),
        )


class CdfEntityList(TypeList[CdfEntity]):
    _NODE = CdfEntity

    def as_apply(self) -> CdfEntityApplyList:
        return CdfEntityApplyList([node.as_apply() for node in self.data])


class CdfEntityApplyList(TypeApplyList[CdfEntityApply]):
    _NODE = CdfEntityApply
