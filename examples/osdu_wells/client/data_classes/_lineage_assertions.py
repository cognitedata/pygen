from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "LineageAssertions",
    "LineageAssertionsApply",
    "LineageAssertionsList",
    "LineageAssertionsApplyList",
    "LineageAssertionsFields",
    "LineageAssertionsTextFields",
]


LineageAssertionsTextFields = Literal["id_", "lineage_relationship_type"]
LineageAssertionsFields = Literal["id_", "lineage_relationship_type"]

_LINEAGEASSERTIONS_PROPERTIES_BY_FIELD = {
    "id_": "ID",
    "lineage_relationship_type": "LineageRelationshipType",
}


class LineageAssertions(DomainModel):
    """This represent a read version of lineage assertion.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the lineage assertion.
        id_: The id field.
        lineage_relationship_type: The lineage relationship type field.
        created_time: The created time of the lineage assertion node.
        last_updated_time: The last updated time of the lineage assertion node.
        deleted_time: If present, the deleted time of the lineage assertion node.
        version: The version of the lineage assertion node.
    """

    space: str = "IntegrationTestsImmutable"
    id_: Optional[str] = Field(None, alias="ID")
    lineage_relationship_type: Optional[str] = Field(None, alias="LineageRelationshipType")

    def as_apply(self) -> LineageAssertionsApply:
        """Convert this read version of lineage assertion to a write version."""
        return LineageAssertionsApply(
            space=self.space,
            external_id=self.external_id,
            id_=self.id_,
            lineage_relationship_type=self.lineage_relationship_type,
        )


class LineageAssertionsApply(DomainModelApply):
    """This represent a write version of lineage assertion.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the lineage assertion.
        id_: The id field.
        lineage_relationship_type: The lineage relationship type field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    id_: Optional[str] = Field(None, alias="ID")
    lineage_relationship_type: Optional[str] = Field(None, alias="LineageRelationshipType")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.id_ is not None:
            properties["ID"] = self.id_
        if self.lineage_relationship_type is not None:
            properties["LineageRelationshipType"] = self.lineage_relationship_type
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "LineageAssertions", "ef344f6030d778"),
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


class LineageAssertionsList(TypeList[LineageAssertions]):
    """List of lineage assertions in read version."""

    _NODE = LineageAssertions

    def as_apply(self) -> LineageAssertionsApplyList:
        """Convert this read version of lineage assertion to a write version."""
        return LineageAssertionsApplyList([node.as_apply() for node in self.data])


class LineageAssertionsApplyList(TypeApplyList[LineageAssertionsApply]):
    """List of lineage assertions in write version."""

    _NODE = LineageAssertionsApply
