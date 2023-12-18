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
    """This represents the reading version of cog pool.

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

    space: str = DEFAULT_INSTANCE_SPACE
    max_price: Optional[float] = Field(None, alias="maxPrice")
    min_price: Optional[float] = Field(None, alias="minPrice")
    name: Optional[str] = None
    time_unit: Optional[str] = Field(None, alias="timeUnit")
    timezone: Optional[str] = None

    def as_apply(self) -> CogPoolApply:
        """Convert this read version of cog pool to the writing version."""
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
    """This represents the writing version of cog pool.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cog pool.
        max_price: The max price field.
        min_price: The min price field.
        name: The name field.
        time_unit: The time unit field.
        timezone: The timezone field.
        existing_version: Fail the ingestion request if the cog pool version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    max_price: Optional[float] = Field(None, alias="maxPrice")
    min_price: Optional[float] = Field(None, alias="minPrice")
    name: Optional[str] = None
    time_unit: Optional[str] = Field(None, alias="timeUnit")
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
            "market", "CogPool", "28af312f1d7093"
        )

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
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("market", "CogPool"),
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


class CogPoolList(DomainModelList[CogPool]):
    """List of cog pools in the read version."""

    _INSTANCE = CogPool

    def as_apply(self) -> CogPoolApplyList:
        """Convert these read versions of cog pool to the writing versions."""
        return CogPoolApplyList([node.as_apply() for node in self.data])


class CogPoolApplyList(DomainModelApplyList[CogPoolApply]):
    """List of cog pools in the writing version."""

    _INSTANCE = CogPoolApply


def _create_cog_pool_filter(
    view_id: dm.ViewId,
    min_max_price: float | None = None,
    max_max_price: float | None = None,
    min_min_price: float | None = None,
    max_min_price: float | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    time_unit: str | list[str] | None = None,
    time_unit_prefix: str | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_max_price or max_max_price:
        filters.append(dm.filters.Range(view_id.as_property_ref("maxPrice"), gte=min_max_price, lte=max_max_price))
    if min_min_price or max_min_price:
        filters.append(dm.filters.Range(view_id.as_property_ref("minPrice"), gte=min_min_price, lte=max_min_price))
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if time_unit is not None and isinstance(time_unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timeUnit"), value=time_unit))
    if time_unit and isinstance(time_unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timeUnit"), values=time_unit))
    if time_unit_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timeUnit"), value=time_unit_prefix))
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
