from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "PygenPool",
    "PygenPoolApply",
    "PygenPoolList",
    "PygenPoolApplyList",
    "PygenPoolFields",
    "PygenPoolTextFields",
]


PygenPoolTextFields = Literal["name", "timezone"]
PygenPoolFields = Literal["day_of_week", "name", "timezone"]

_PYGENPOOL_PROPERTIES_BY_FIELD = {
    "day_of_week": "dayOfWeek",
    "name": "name",
    "timezone": "timezone",
}


class PygenPool(DomainModel):
    """This represent a read version of pygen pool.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the pygen pool.
        day_of_week: The day of week field.
        name: The name field.
        timezone: The timezone field.
        created_time: The created time of the pygen pool node.
        last_updated_time: The last updated time of the pygen pool node.
        deleted_time: If present, the deleted time of the pygen pool node.
        version: The version of the pygen pool node.
    """

    space: str = "market"
    day_of_week: Optional[int] = Field(None, alias="dayOfWeek")
    name: Optional[str] = None
    timezone: Optional[str] = None

    def as_apply(self) -> PygenPoolApply:
        """Convert this read version of pygen pool to a write version."""
        return PygenPoolApply(
            space=self.space,
            external_id=self.external_id,
            day_of_week=self.day_of_week,
            name=self.name,
            timezone=self.timezone,
        )


class PygenPoolApply(DomainModelApply):
    """This represent a write version of pygen pool.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the pygen pool.
        day_of_week: The day of week field.
        name: The name field.
        timezone: The timezone field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "market"
    day_of_week: Optional[int] = Field(None, alias="dayOfWeek")
    name: Optional[str] = None
    timezone: Optional[str] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.day_of_week is not None:
            properties["dayOfWeek"] = self.day_of_week
        if self.name is not None:
            properties["name"] = self.name
        if self.timezone is not None:
            properties["timezone"] = self.timezone
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("market", "PygenPool", "23c71ba66bad9d"),
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


class PygenPoolList(TypeList[PygenPool]):
    """List of pygen pools in read version."""

    _NODE = PygenPool

    def as_apply(self) -> PygenPoolApplyList:
        """Convert this read version of pygen pool to a write version."""
        return PygenPoolApplyList([node.as_apply() for node in self.data])


class PygenPoolApplyList(TypeApplyList[PygenPoolApply]):
    """List of pygen pools in write version."""

    _NODE = PygenPoolApply
