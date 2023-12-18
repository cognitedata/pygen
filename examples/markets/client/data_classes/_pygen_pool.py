from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


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
    """This represents the reading version of pygen pool.

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

    space: str = DEFAULT_INSTANCE_SPACE
    day_of_week: Optional[int] = Field(None, alias="dayOfWeek")
    name: Optional[str] = None
    timezone: Optional[str] = None

    def as_apply(self) -> PygenPoolApply:
        """Convert this read version of pygen pool to the writing version."""
        return PygenPoolApply(
            space=self.space,
            external_id=self.external_id,
            day_of_week=self.day_of_week,
            name=self.name,
            timezone=self.timezone,
        )


class PygenPoolApply(DomainModelApply):
    """This represents the writing version of pygen pool.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the pygen pool.
        day_of_week: The day of week field.
        name: The name field.
        timezone: The timezone field.
        existing_version: Fail the ingestion request if the pygen pool version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    day_of_week: Optional[int] = Field(None, alias="dayOfWeek")
    name: Optional[str] = None
    timezone: Optional[str] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "market", "PygenPool", "23c71ba66bad9d"
        )

        properties = {}
        if self.day_of_week is not None:
            properties["dayOfWeek"] = self.day_of_week
        if self.name is not None:
            properties["name"] = self.name
        if self.timezone is not None:
            properties["timezone"] = self.timezone

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("market", "PygenPool"),
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


class PygenPoolList(DomainModelList[PygenPool]):
    """List of pygen pools in the read version."""

    _INSTANCE = PygenPool

    def as_apply(self) -> PygenPoolApplyList:
        """Convert these read versions of pygen pool to the writing versions."""
        return PygenPoolApplyList([node.as_apply() for node in self.data])


class PygenPoolApplyList(DomainModelApplyList[PygenPoolApply]):
    """List of pygen pools in the writing version."""

    _INSTANCE = PygenPoolApply


def _create_pygen_pool_filter(
    view_id: dm.ViewId,
    min_day_of_week: int | None = None,
    max_day_of_week: int | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_day_of_week or max_day_of_week:
        filters.append(dm.filters.Range(view_id.as_property_ref("dayOfWeek"), gte=min_day_of_week, lte=max_day_of_week))
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if timezone is not None and isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
