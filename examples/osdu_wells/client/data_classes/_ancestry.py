from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Ancestry", "AncestryApply", "AncestryList", "AncestryApplyList", "AncestryFields", "AncestryTextFields"]


AncestryTextFields = Literal["parents"]
AncestryFields = Literal["parents"]

_ANCESTRY_PROPERTIES_BY_FIELD = {
    "parents": "parents",
}


class Ancestry(DomainModel):
    """This represent a read version of ancestry.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the ancestry.
        parents: The parent field.
        created_time: The created time of the ancestry node.
        last_updated_time: The last updated time of the ancestry node.
        deleted_time: If present, the deleted time of the ancestry node.
        version: The version of the ancestry node.
    """

    space: str = "IntegrationTestsImmutable"
    parents: Optional[list[str]] = None

    def as_apply(self) -> AncestryApply:
        """Convert this read version of ancestry to a write version."""
        return AncestryApply(
            space=self.space,
            external_id=self.external_id,
            parents=self.parents,
        )


class AncestryApply(DomainModelApply):
    """This represent a write version of ancestry.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the ancestry.
        parents: The parent field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    parents: Optional[list[str]] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.parents is not None:
            properties["parents"] = self.parents
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Ancestry", "624b46e28cdd69"),
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


class AncestryList(TypeList[Ancestry]):
    """List of ancestries in read version."""

    _NODE = Ancestry

    def as_apply(self) -> AncestryApplyList:
        """Convert this read version of ancestry to a write version."""
        return AncestryApplyList([node.as_apply() for node in self.data])


class AncestryApplyList(TypeApplyList[AncestryApply]):
    """List of ancestries in write version."""

    _NODE = AncestryApply
