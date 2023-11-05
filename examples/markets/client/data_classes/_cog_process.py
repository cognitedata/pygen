from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._bid import BidApply
    from ._date_transformation_pair import DateTransformationPairApply
    from ._value_transformation import ValueTransformationApply

__all__ = [
    "CogProcess",
    "CogProcessApply",
    "CogProcessList",
    "CogProcessApplyList",
    "CogProcessFields",
    "CogProcessTextFields",
]


CogProcessTextFields = Literal["name"]
CogProcessFields = Literal["name"]

_COGPROCESS_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class CogProcess(DomainModel):
    """This represent a read version of cog proces.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cog proces.
        bid: The bid field.
        date_transformations: The date transformation field.
        name: The name field.
        transformation: The transformation field.
        created_time: The created time of the cog proces node.
        last_updated_time: The last updated time of the cog proces node.
        deleted_time: If present, the deleted time of the cog proces node.
        version: The version of the cog proces node.
    """

    space: str = "market"
    bid: Optional[str] = None
    date_transformations: Optional[str] = Field(None, alias="dateTransformations")
    name: Optional[str] = None
    transformation: Optional[str] = None

    def as_apply(self) -> CogProcessApply:
        """Convert this read version of cog proces to a write version."""
        return CogProcessApply(
            space=self.space,
            external_id=self.external_id,
            bid=self.bid,
            date_transformations=self.date_transformations,
            name=self.name,
            transformation=self.transformation,
        )


class CogProcessApply(DomainModelApply):
    """This represent a write version of cog proces.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cog proces.
        bid: The bid field.
        date_transformations: The date transformation field.
        name: The name field.
        transformation: The transformation field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "market"
    bid: Union[BidApply, str, None] = Field(None, repr=False)
    date_transformations: Union[DateTransformationPairApply, str, None] = Field(
        None, repr=False, alias="dateTransformations"
    )
    name: Optional[str] = None
    transformation: Union[ValueTransformationApply, str, None] = Field(None, repr=False)

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.bid is not None:
            properties["bid"] = {
                "space": self.space if isinstance(self.bid, str) else self.bid.space,
                "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
            }
        if self.date_transformations is not None:
            properties["dateTransformations"] = {
                "space": self.space if isinstance(self.date_transformations, str) else self.date_transformations.space,
                "externalId": self.date_transformations
                if isinstance(self.date_transformations, str)
                else self.date_transformations.external_id,
            }
        if self.name is not None:
            properties["name"] = self.name
        if self.transformation is not None:
            properties["transformation"] = {
                "space": self.space if isinstance(self.transformation, str) else self.transformation.space,
                "externalId": self.transformation
                if isinstance(self.transformation, str)
                else self.transformation.external_id,
            }
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("market", "CogProcess", "b5df5d19e08fd0"),
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

        if isinstance(self.bid, DomainModelApply):
            instances = self.bid._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.date_transformations, DomainModelApply):
            instances = self.date_transformations._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.transformation, DomainModelApply):
            instances = self.transformation._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class CogProcessList(TypeList[CogProcess]):
    """List of cog process in read version."""

    _NODE = CogProcess

    def as_apply(self) -> CogProcessApplyList:
        """Convert this read version of cog proces to a write version."""
        return CogProcessApplyList([node.as_apply() for node in self.data])


class CogProcessApplyList(TypeApplyList[CogProcessApply]):
    """List of cog process in write version."""

    _NODE = CogProcessApply
