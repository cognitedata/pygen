from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union  # noqa: F401

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from ._date_transformations import DateTransformationApply

__all__ = ["DateTransformationPair", "DateTransformationPairApply", "DateTransformationPairList"]


class DateTransformationPair(DomainModel):
    space: ClassVar[str] = "market"
    end: list[str] = []
    start: list[str] = []


class DateTransformationPairApply(DomainModelApply):
    space: ClassVar[str] = "market"
    end: list[Union["DateTransformationApply", str]] = Field(default_factory=list, repr=False)
    start: list[Union["DateTransformationApply", str]] = Field(default_factory=list, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        nodes = []

        edges = []
        cache.add(self.external_id)

        for end in self.end:
            edge = self._create_end_edge(end)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(end, DomainModelApply):
                instances = end._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for start in self.start:
            edge = self._create_start_edge(start)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(start, DomainModelApply):
                instances = start._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_end_edge(self, end: Union[str, "DateTransformationApply"]) -> dm.EdgeApply:
        if isinstance(end, str):
            end_node_ext_id = end
        elif isinstance(end, DomainModelApply):
            end_node_ext_id = end.external_id
        else:
            raise TypeError(f"Expected str or DateTransformationApply, got {type(end)}")

        return dm.EdgeApply(
            space="market",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("market", "DateTransformationPair.end"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("market", end_node_ext_id),
        )

    def _create_start_edge(self, start: Union[str, "DateTransformationApply"]) -> dm.EdgeApply:
        if isinstance(start, str):
            end_node_ext_id = start
        elif isinstance(start, DomainModelApply):
            end_node_ext_id = start.external_id
        else:
            raise TypeError(f"Expected str or DateTransformationApply, got {type(start)}")

        return dm.EdgeApply(
            space="market",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("market", "DateTransformationPair.start"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("market", end_node_ext_id),
        )


class DateTransformationPairList(TypeList[DateTransformationPair]):
    _NODE = DateTransformationPair
