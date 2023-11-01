from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Meta", "MetaApply", "MetaList", "MetaApplyList", "MetaFields", "MetaTextFields"]


MetaTextFields = Literal["kind", "name", "persistable_reference", "property_names", "unit_of_measure_id"]
MetaFields = Literal["kind", "name", "persistable_reference", "property_names", "unit_of_measure_id"]

_META_PROPERTIES_BY_FIELD = {
    "kind": "kind",
    "name": "name",
    "persistable_reference": "persistableReference",
    "property_names": "propertyNames",
    "unit_of_measure_id": "unitOfMeasureID",
}


class Meta(DomainModel):
    space: str = "IntegrationTestsImmutable"
    kind: Optional[str] = None
    name: Optional[str] = None
    persistable_reference: Optional[str] = Field(None, alias="persistableReference")
    property_names: Optional[list[str]] = Field(None, alias="propertyNames")
    unit_of_measure_id: Optional[str] = Field(None, alias="unitOfMeasureID")

    def as_apply(self) -> MetaApply:
        return MetaApply(
            space=self.space,
            external_id=self.external_id,
            kind=self.kind,
            name=self.name,
            persistable_reference=self.persistable_reference,
            property_names=self.property_names,
            unit_of_measure_id=self.unit_of_measure_id,
        )


class MetaApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    kind: Optional[str] = None
    name: Optional[str] = None
    persistable_reference: Optional[str] = Field(None, alias="persistableReference")
    property_names: Optional[list[str]] = Field(None, alias="propertyNames")
    unit_of_measure_id: Optional[str] = Field(None, alias="unitOfMeasureID")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.kind is not None:
            properties["kind"] = self.kind
        if self.name is not None:
            properties["name"] = self.name
        if self.persistable_reference is not None:
            properties["persistableReference"] = self.persistable_reference
        if self.property_names is not None:
            properties["propertyNames"] = self.property_names
        if self.unit_of_measure_id is not None:
            properties["unitOfMeasureID"] = self.unit_of_measure_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "Meta"),
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


class MetaList(TypeList[Meta]):
    _NODE = Meta

    def as_apply(self) -> MetaApplyList:
        return MetaApplyList([node.as_apply() for node in self.data])


class MetaApplyList(TypeApplyList[MetaApply]):
    _NODE = MetaApply
