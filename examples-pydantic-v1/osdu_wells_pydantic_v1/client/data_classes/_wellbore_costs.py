from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


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
    """This represents the reading version of wellbore cost.

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
        """Convert this read version of wellbore cost to the writing version."""
        return WellboreCostsApply(
            space=self.space,
            external_id=self.external_id,
            activity_type_id=self.activity_type_id,
            cost=self.cost,
        )


class WellboreCostsApply(DomainModelApply):
    """This represents the writing version of wellbore cost.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wellbore cost.
        activity_type_id: The activity type id field.
        cost: The cost field.
        existing_version: Fail the ingestion request if the wellbore cost version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    activity_type_id: Optional[str] = Field(None, alias="ActivityTypeID")
    cost: Optional[float] = Field(None, alias="Cost")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "WellboreCosts", "b4f71248f398a2"
        )

        properties = {}
        if self.activity_type_id is not None:
            properties["ActivityTypeID"] = self.activity_type_id
        if self.cost is not None:
            properties["Cost"] = self.cost

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class WellboreCostsList(DomainModelList[WellboreCosts]):
    """List of wellbore costs in the read version."""

    _INSTANCE = WellboreCosts

    def as_apply(self) -> WellboreCostsApplyList:
        """Convert these read versions of wellbore cost to the writing versions."""
        return WellboreCostsApplyList([node.as_apply() for node in self.data])


class WellboreCostsApplyList(DomainModelApplyList[WellboreCostsApply]):
    """List of wellbore costs in the writing version."""

    _INSTANCE = WellboreCostsApply


def _create_wellbore_cost_filter(
    view_id: dm.ViewId,
    activity_type_id: str | list[str] | None = None,
    activity_type_id_prefix: str | None = None,
    min_cost: float | None = None,
    max_cost: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if activity_type_id and isinstance(activity_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ActivityTypeID"), value=activity_type_id))
    if activity_type_id and isinstance(activity_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ActivityTypeID"), values=activity_type_id))
    if activity_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ActivityTypeID"), value=activity_type_id_prefix))
    if min_cost or max_cost:
        filters.append(dm.filters.Range(view_id.as_property_ref("Cost"), gte=min_cost, lte=max_cost))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
