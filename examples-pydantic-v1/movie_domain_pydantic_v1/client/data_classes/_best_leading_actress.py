from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "BestLeadingActress",
    "BestLeadingActressApply",
    "BestLeadingActressList",
    "BestLeadingActressApplyList",
    "BestLeadingActressFields",
    "BestLeadingActressTextFields",
]


BestLeadingActressTextFields = Literal["name"]
BestLeadingActressFields = Literal["name", "year"]

_BESTLEADINGACTRESS_PROPERTIES_BY_FIELD = {
    "name": "name",
    "year": "year",
}


class BestLeadingActress(DomainModel):
    """This represent a read version of best leading actress.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the best leading actress.
        name: The name field.
        year: The year field.
        created_time: The created time of the best leading actress node.
        last_updated_time: The last updated time of the best leading actress node.
        deleted_time: If present, the deleted time of the best leading actress node.
        version: The version of the best leading actress node.
    """

    space: str = "IntegrationTestsImmutable"
    name: Optional[str] = None
    year: Optional[int] = None

    def as_apply(self) -> BestLeadingActressApply:
        """Convert this read version of best leading actress to a write version."""
        return BestLeadingActressApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            year=self.year,
        )


class BestLeadingActressApply(DomainModelApply):
    """This represent a write version of best leading actress.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the best leading actress.
        name: The name field.
        year: The year field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    name: str
    year: int

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.year is not None:
            properties["year"] = self.year
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "BestLeadingActress", "2"),
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


class BestLeadingActressList(TypeList[BestLeadingActress]):
    """List of best leading actresses in read version."""

    _NODE = BestLeadingActress

    def as_apply(self) -> BestLeadingActressApplyList:
        """Convert this read version of best leading actress to a write version."""
        return BestLeadingActressApplyList([node.as_apply() for node in self.data])


class BestLeadingActressApplyList(TypeApplyList[BestLeadingActressApply]):
    """List of best leading actresses in write version."""

    _NODE = BestLeadingActressApply
