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
    """This represent a read version of artefact.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the artefact.
        resource_id: The resource id field.
        resource_kind: The resource kind field.
        role_id: The role id field.
        created_time: The created time of the artefact node.
        last_updated_time: The last updated time of the artefact node.
        deleted_time: If present, the deleted time of the artefact node.
        version: The version of the artefact node.
    """

    space: str = "IntegrationTestsImmutable"
    resource_id: Optional[str] = Field(None, alias="ResourceID")
    resource_kind: Optional[str] = Field(None, alias="ResourceKind")
    role_id: Optional[str] = Field(None, alias="RoleID")

    def as_apply(self) -> ArtefactsApply:
        """Convert this read version of artefact to a write version."""
        return ArtefactsApply(
            space=self.space,
            external_id=self.external_id,
            resource_id=self.resource_id,
            resource_kind=self.resource_kind,
            role_id=self.role_id,
        )


class ArtefactsApply(DomainModelApply):
    """This represent a write version of artefact.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the artefact.
        resource_id: The resource id field.
        resource_kind: The resource kind field.
        role_id: The role id field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    resource_id: Optional[str] = Field(None, alias="ResourceID")
    resource_kind: Optional[str] = Field(None, alias="ResourceKind")
    role_id: Optional[str] = Field(None, alias="RoleID")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.resource_id is not None:
            properties["ResourceID"] = self.resource_id
        if self.resource_kind is not None:
            properties["ResourceKind"] = self.resource_kind
        if self.role_id is not None:
            properties["RoleID"] = self.role_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Artefacts", "7a44a1f4dac367"),
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


class ArtefactsList(TypeList[Artefacts]):
    """List of artefacts in read version."""

    _NODE = Artefacts

    def as_apply(self) -> ArtefactsApplyList:
        """Convert this read version of artefact to a write version."""
        return ArtefactsApplyList([node.as_apply() for node in self.data])


class ArtefactsApplyList(TypeApplyList[ArtefactsApply]):
    """List of artefacts in write version."""

    _NODE = ArtefactsApply
