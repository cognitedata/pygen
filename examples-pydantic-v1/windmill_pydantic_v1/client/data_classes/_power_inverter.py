from __future__ import annotations

from typing import Literal, Optional, Union  # noqa: F401

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries

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
    "PowerInverter",
    "PowerInverterApply",
    "PowerInverterList",
    "PowerInverterApplyList",
    "PowerInverterFields",
    "PowerInverterTextFields",
]


PowerInverterTextFields = Literal["active_power_total", "apparent_power_total", "reactive_power_total"]
PowerInverterFields = Literal["active_power_total", "apparent_power_total", "reactive_power_total"]

_POWERINVERTER_PROPERTIES_BY_FIELD = {
    "active_power_total": "active_power_total",
    "apparent_power_total": "apparent_power_total",
    "reactive_power_total": "reactive_power_total",
}


class PowerInverter(DomainModel):
    """This represents the reading version of power inverter.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the power inverter.
        active_power_total: The active power total field.
        apparent_power_total: The apparent power total field.
        reactive_power_total: The reactive power total field.
        created_time: The created time of the power inverter node.
        last_updated_time: The last updated time of the power inverter node.
        deleted_time: If present, the deleted time of the power inverter node.
        version: The version of the power inverter node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    active_power_total: Union[TimeSeries, str, None] = None
    apparent_power_total: Union[TimeSeries, str, None] = None
    reactive_power_total: Union[TimeSeries, str, None] = None

    def as_apply(self) -> PowerInverterApply:
        """Convert this read version of power inverter to the writing version."""
        return PowerInverterApply(
            space=self.space,
            external_id=self.external_id,
            active_power_total=self.active_power_total,
            apparent_power_total=self.apparent_power_total,
            reactive_power_total=self.reactive_power_total,
        )


class PowerInverterApply(DomainModelApply):
    """This represents the writing version of power inverter.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the power inverter.
        active_power_total: The active power total field.
        apparent_power_total: The apparent power total field.
        reactive_power_total: The reactive power total field.
        existing_version: Fail the ingestion request if the power inverter version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    active_power_total: Union[TimeSeries, str, None] = None
    apparent_power_total: Union[TimeSeries, str, None] = None
    reactive_power_total: Union[TimeSeries, str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-models", "PowerInverter", "1"
        )

        properties = {}

        if self.active_power_total is not None:
            properties["active_power_total"] = (
                self.active_power_total
                if isinstance(self.active_power_total, str)
                else self.active_power_total.external_id
            )

        if self.apparent_power_total is not None:
            properties["apparent_power_total"] = (
                self.apparent_power_total
                if isinstance(self.apparent_power_total, str)
                else self.apparent_power_total.external_id
            )

        if self.reactive_power_total is not None:
            properties["reactive_power_total"] = (
                self.reactive_power_total
                if isinstance(self.reactive_power_total, str)
                else self.reactive_power_total.external_id
            )

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

        if isinstance(self.active_power_total, TimeSeries):
            resources.time_series.append(self.active_power_total)

        if isinstance(self.apparent_power_total, TimeSeries):
            resources.time_series.append(self.apparent_power_total)

        if isinstance(self.reactive_power_total, TimeSeries):
            resources.time_series.append(self.reactive_power_total)

        return resources


class PowerInverterList(DomainModelList[PowerInverter]):
    """List of power inverters in the read version."""

    _INSTANCE = PowerInverter

    def as_apply(self) -> PowerInverterApplyList:
        """Convert these read versions of power inverter to the writing versions."""
        return PowerInverterApplyList([node.as_apply() for node in self.data])


class PowerInverterApplyList(DomainModelApplyList[PowerInverterApply]):
    """List of power inverters in the writing version."""

    _INSTANCE = PowerInverterApply


def _create_power_inverter_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None