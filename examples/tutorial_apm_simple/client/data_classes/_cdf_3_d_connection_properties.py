from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "CdfConnectionProperties",
    "CdfConnectionPropertiesApply",
    "CdfConnectionPropertiesList",
    "CdfConnectionPropertiesApplyList",
    "CdfConnectionPropertiesFields",
]
CdfConnectionPropertiesFields = Literal["revision_id", "revision_node_id"]

_CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD = {
    "revision_id": "revisionId",
    "revision_node_id": "revisionNodeId",
}


class CdfConnectionProperties(DomainModel):
    space: str = "cdf_3d_schema"
    revision_id: Optional[int] = Field(None, alias="revisionId")
    revision_node_id: Optional[int] = Field(None, alias="revisionNodeId")

    def as_apply(self) -> CdfConnectionPropertiesApply:
        return CdfConnectionPropertiesApply(
            external_id=self.external_id,
            revision_id=self.revision_id,
            revision_node_id=self.revision_node_id,
        )


class CdfConnectionPropertiesApply(DomainModelApply):
    space: str = "cdf_3d_schema"
    revision_id: int
    revision_node_id: int

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.revision_id is not None:
            properties["revisionId"] = self.revision_id
        if self.revision_node_id is not None:
            properties["revisionNodeId"] = self.revision_node_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("cdf_3d_schema", "Cdf3dConnectionProperties"),
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


class CdfConnectionPropertiesList(TypeList[CdfConnectionProperties]):
    _NODE = CdfConnectionProperties

    def as_apply(self) -> CdfConnectionPropertiesApplyList:
        return CdfConnectionPropertiesApplyList([node.as_apply() for node in self.data])


class CdfConnectionPropertiesApplyList(TypeApplyList[CdfConnectionPropertiesApply]):
    _NODE = CdfConnectionPropertiesApply
