from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "WellboreCosts",
    "WellboreCostsApply",
    "WellboreCostsList",
    "WellboreCostsApplyList",
    "WellboreCostsFields",
    "WellboreCostsTextFields",
]


WellboreCostsTextFields = Literal["activity_type_id"]
WellboreCostsFields = Literal["activity_type_id", "cost"]

_WELLBORECOSTS_PROPERTIES_BY_FIELD = {
    "activity_type_id": "ActivityTypeID",
    "cost": "Cost",
}


class WellboreCosts(DomainModel):
    """This represent a read version of wellbore cost.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wellbore cost.
        activity_type_id: The activity type id field.
        cost: The cost field.
        created_time: The created time of the wellbore cost node.
        last_updated_time: The last updated time of the wellbore cost node.
        deleted_time: If present, the deleted time of the wellbore cost node.
        version: The version of the wellbore cost node.
    """

    space: str = "IntegrationTestsImmutable"
    activity_type_id: Optional[str] = Field(None, alias="ActivityTypeID")
    cost: Optional[float] = Field(None, alias="Cost")

    def as_apply(self) -> WellboreCostsApply:
        """Convert this read version of wellbore cost to a write version."""
        return WellboreCostsApply(
            space=self.space,
            external_id=self.external_id,
            activity_type_id=self.activity_type_id,
            cost=self.cost,
        )


class WellboreCostsApply(DomainModelApply):
    """This represent a write version of wellbore cost.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wellbore cost.
        activity_type_id: The activity type id field.
        cost: The cost field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    activity_type_id: Optional[str] = Field(None, alias="ActivityTypeID")
    cost: Optional[float] = Field(None, alias="Cost")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.activity_type_id is not None:
            properties["ActivityTypeID"] = self.activity_type_id
        if self.cost is not None:
            properties["Cost"] = self.cost
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "WellboreCosts", "b4f71248f398a2"),
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


class WellboreCostsList(TypeList[WellboreCosts]):
    """List of wellbore costs in read version."""

    _NODE = WellboreCosts

    def as_apply(self) -> WellboreCostsApplyList:
        """Convert this read version of wellbore cost to a write version."""
        return WellboreCostsApplyList([node.as_apply() for node in self.data])


class WellboreCostsApplyList(TypeApplyList[WellboreCostsApply]):
    """List of wellbore costs in write version."""

    _NODE = WellboreCostsApply
