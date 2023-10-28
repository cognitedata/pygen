from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Acl", "AclApply", "AclList", "AclApplyList", "AclFields", "AclTextFields"]


AclTextFields = Literal["owners", "viewers"]
AclFields = Literal["owners", "viewers"]

_ACL_PROPERTIES_BY_FIELD = {
    "owners": "owners",
    "viewers": "viewers",
}


class Acl(DomainModel):
    space: str = "IntegrationTestsImmutable"
    owners: Optional[list[str]] = None
    viewers: Optional[list[str]] = None

    def as_apply(self) -> AclApply:
        return AclApply(
            external_id=self.external_id,
            owners=self.owners,
            viewers=self.viewers,
        )


class AclApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    owners: Optional[list[str]] = None
    viewers: Optional[list[str]] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.owners is not None:
            properties["owners"] = self.owners
        if self.viewers is not None:
            properties["viewers"] = self.viewers
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("IntegrationTestsImmutable", "Acl"),
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


class AclList(TypeList[Acl]):
    _NODE = Acl

    def as_apply(self) -> AclApplyList:
        return AclApplyList([node.as_apply() for node in self.data])


class AclApplyList(TypeApplyList[AclApply]):
    _NODE = AclApply
