from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "ValueTransformation",
    "ValueTransformationApply",
    "ValueTransformationList",
    "ValueTransformationApplyList",
    "ValueTransformationFields",
    "ValueTransformationTextFields",
]


ValueTransformationTextFields = Literal["method"]
ValueTransformationFields = Literal["arguments", "method"]

_VALUETRANSFORMATION_PROPERTIES_BY_FIELD = {
    "arguments": "arguments",
    "method": "method",
}


class ValueTransformation(DomainModel):
    space: str = "market"
    arguments: Optional[dict] = None
    method: Optional[str] = None

    def as_apply(self) -> ValueTransformationApply:
        return ValueTransformationApply(
            space=self.space,
            external_id=self.external_id,
            arguments=self.arguments,
            method=self.method,
        )


class ValueTransformationApply(DomainModelApply):
    space: str = "market"
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
                source=dm.ContainerId("market", "ValueTransformation"),
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


class ValueTransformationList(TypeList[ValueTransformation]):
    _NODE = ValueTransformation

    def as_apply(self) -> ValueTransformationApplyList:
        return ValueTransformationApplyList([node.as_apply() for node in self.data])


class ValueTransformationApplyList(TypeApplyList[ValueTransformationApply]):
    _NODE = ValueTransformationApply
