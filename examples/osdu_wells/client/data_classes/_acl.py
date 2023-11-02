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
            space=self.space,
            external_id=self.external_id,
            owners=self.owners,
            viewers=self.viewers,
        )


class AclApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    owners: Optional[list[str]] = None
    viewers: Optional[list[str]] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.owners is not None:
            properties["owners"] = self.owners
        if self.viewers is not None:
            properties["viewers"] = self.viewers
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Acl", "1c4f4a5942a9a8"),
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


class AclList(TypeList[Acl]):
    _NODE = Acl

    def as_apply(self) -> AclApplyList:
        return AclApplyList([node.as_apply() for node in self.data])


class AclApplyList(TypeApplyList[AclApply]):
    _NODE = AclApply
