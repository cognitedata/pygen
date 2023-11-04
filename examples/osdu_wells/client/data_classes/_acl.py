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
    """This represent a read version of acl.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the acl.
        owners: The owner field.
        viewers: The viewer field.
        created_time: The created time of the acl node.
        last_updated_time: The last updated time of the acl node.
        deleted_time: If present, the deleted time of the acl node.
        version: The version of the acl node.
    """

    space: str = "IntegrationTestsImmutable"
    owners: Optional[list[str]] = None
    viewers: Optional[list[str]] = None

    def as_apply(self) -> AclApply:
        """Convert this read version of acl to a write version."""
        return AclApply(
            space=self.space,
            external_id=self.external_id,
            owners=self.owners,
            viewers=self.viewers,
        )


class AclApply(DomainModelApply):
    """This represent a write version of acl.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the acl.
        owners: The owner field.
        viewers: The viewer field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

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
    """List of acls in read version."""

    _NODE = Acl

    def as_apply(self) -> AclApplyList:
        """Convert this read version of acl to a write version."""
        return AclApplyList([node.as_apply() for node in self.data])


class AclApplyList(TypeApplyList[AclApply]):
    """List of acls in write version."""

    _NODE = AclApply
