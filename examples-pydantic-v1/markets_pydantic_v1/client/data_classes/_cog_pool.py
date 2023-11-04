from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["CogPool", "CogPoolApply", "CogPoolList", "CogPoolApplyList", "CogPoolFields", "CogPoolTextFields"]


CogPoolTextFields = Literal["name", "time_unit", "timezone"]
CogPoolFields = Literal["max_price", "min_price", "name", "time_unit", "timezone"]

_COGPOOL_PROPERTIES_BY_FIELD = {
    "max_price": "maxPrice",
    "min_price": "minPrice",
    "name": "name",
    "time_unit": "timeUnit",
    "timezone": "timezone",
}


class CogPool(DomainModel):
    """This represent a read version of cog pool.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cog pool.
        max_price: The max price field.
        min_price: The min price field.
        name: The name field.
        time_unit: The time unit field.
        timezone: The timezone field.
        created_time: The created time of the cog pool node.
        last_updated_time: The last updated time of the cog pool node.
        deleted_time: If present, the deleted time of the cog pool node.
        version: The version of the cog pool node.
    """

    space: str = "market"
    max_price: Optional[float] = Field(None, alias="maxPrice")
    min_price: Optional[float] = Field(None, alias="minPrice")
    name: Optional[str] = None
    time_unit: Optional[str] = Field(None, alias="timeUnit")
    timezone: Optional[str] = None

    def as_apply(self) -> CogPoolApply:
        """Convert this read version of cog pool to a write version."""
        return CogPoolApply(
            space=self.space,
            external_id=self.external_id,
            max_price=self.max_price,
            min_price=self.min_price,
            name=self.name,
            time_unit=self.time_unit,
            timezone=self.timezone,
        )


class CogPoolApply(DomainModelApply):
    """This represent a write version of cog pool.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cog pool.
        max_price: The max price field.
        min_price: The min price field.
        name: The name field.
        time_unit: The time unit field.
        timezone: The timezone field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "market"
    max_price: Optional[float] = Field(None, alias="maxPrice")
    min_price: Optional[float] = Field(None, alias="minPrice")
    name: Optional[str] = None
    time_unit: Optional[str] = Field(None, alias="timeUnit")
    timezone: Optional[str] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.max_price is not None:
            properties["maxPrice"] = self.max_price
        if self.min_price is not None:
            properties["minPrice"] = self.min_price
        if self.name is not None:
            properties["name"] = self.name
        if self.time_unit is not None:
            properties["timeUnit"] = self.time_unit
        if self.timezone is not None:
            properties["timezone"] = self.timezone
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("market", "CogPool", "28af312f1d7093"),
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


class CogPoolList(TypeList[CogPool]):
    """List of cog pools in read version."""

    _NODE = CogPool

    def as_apply(self) -> CogPoolApplyList:
        """Convert this read version of cog pool to a write version."""
        return CogPoolApplyList([node.as_apply() for node in self.data])


class CogPoolApplyList(TypeApplyList[CogPoolApply]):
    """List of cog pools in write version."""

    _NODE = CogPoolApply
