from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList

__all__ = ["DateTransformation", "DateTransformationApply", "DateTransformationList"]


class DateTransformation(DomainModel):
    space: ClassVar[str] = "market"
    arguments: Optional[dict] = None
    method: Optional[str] = None


class DateTransformationApply(DomainModelApply):
    space: ClassVar[str] = "market"
    arguments: Optional[dict] = None
    method: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.arguments is not None:
            properties["arguments"] = self.arguments
        if self.method is not None:
            properties["method"] = self.method
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("market", "DateTransformation"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class DateTransformationList(TypeList[DateTransformation]):
    _NODE = DateTransformation
