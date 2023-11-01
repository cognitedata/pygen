from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "DateTransformation",
    "DateTransformationApply",
    "DateTransformationList",
    "DateTransformationApplyList",
    "DateTransformationFields",
    "DateTransformationTextFields",
]


DateTransformationTextFields = Literal["method"]
DateTransformationFields = Literal["arguments", "method"]

_DATETRANSFORMATION_PROPERTIES_BY_FIELD = {
    "arguments": "arguments",
    "method": "method",
}


class DateTransformation(DomainModel):
    space: str = "market"
    arguments: Optional[dict] = None
    method: Optional[str] = None

    def as_apply(self) -> DateTransformationApply:
        return DateTransformationApply(
            space=self.space,
            external_id=self.external_id,
            arguments=self.arguments,
            method=self.method,
        )


class DateTransformationApply(DomainModelApply):
    space: str = "market"
    arguments: Optional[dict] = None
    method: Optional[str] = None

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        properties = {}
        if self.arguments is not None:
            properties["arguments"] = self.arguments
        if self.method is not None:
            properties["method"] = self.method
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("market", "DateTransformation", "482866112eb911"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class DateTransformationList(TypeList[DateTransformation]):
    _NODE = DateTransformation

    def as_apply(self) -> DateTransformationApplyList:
        return DateTransformationApplyList([node.as_apply() for node in self.data])


class DateTransformationApplyList(TypeApplyList[DateTransformationApply]):
    _NODE = DateTransformationApply
