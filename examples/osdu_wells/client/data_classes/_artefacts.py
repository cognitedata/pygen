from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "Artefacts",
    "ArtefactsApply",
    "ArtefactsList",
    "ArtefactsApplyList",
    "ArtefactsFields",
    "ArtefactsTextFields",
]


ArtefactsTextFields = Literal["resource_id", "resource_kind", "role_id"]
ArtefactsFields = Literal["resource_id", "resource_kind", "role_id"]

_ARTEFACTS_PROPERTIES_BY_FIELD = {
    "resource_id": "ResourceID",
    "resource_kind": "ResourceKind",
    "role_id": "RoleID",
}


class Artefacts(DomainModel):
    space: str = "IntegrationTestsImmutable"
    resource_id: Optional[str] = Field(None, alias="ResourceID")
    resource_kind: Optional[str] = Field(None, alias="ResourceKind")
    role_id: Optional[str] = Field(None, alias="RoleID")

    def as_apply(self) -> ArtefactsApply:
        return ArtefactsApply(
            external_id=self.external_id,
            resource_id=self.resource_id,
            resource_kind=self.resource_kind,
            role_id=self.role_id,
        )


class ArtefactsApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    resource_id: Optional[str] = None
    resource_kind: Optional[str] = None
    role_id: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.resource_id is not None:
            properties["ResourceID"] = self.resource_id
        if self.resource_kind is not None:
            properties["ResourceKind"] = self.resource_kind
        if self.role_id is not None:
            properties["RoleID"] = self.role_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "Artefacts"),
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


class ArtefactsList(TypeList[Artefacts]):
    _NODE = Artefacts

    def as_apply(self) -> ArtefactsApplyList:
        return ArtefactsApplyList([node.as_apply() for node in self.data])


class ArtefactsApplyList(TypeApplyList[ArtefactsApply]):
    _NODE = ArtefactsApply
