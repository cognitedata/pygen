from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList

__all__ = ["CdfConnectionProperties", "CdfConnectionPropertiesApply", "CdfConnectionPropertiesList"]


class CdfConnectionProperties(DomainModel):
    space: ClassVar[str] = "cdf_3d_schema"
    revision_id: Optional[int] = Field(None, alias="revisionId")
    revision_node_id: Optional[int] = Field(None, alias="revisionNodeId")


class CdfConnectionPropertiesApply(DomainModelApply):
    space: ClassVar[str] = "cdf_3d_schema"
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
