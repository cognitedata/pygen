from __future__ import annotations

from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
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
        data_record: The data record of the power inverter node.
        active_power_total: The active power total field.
        apparent_power_total: The apparent power total field.
        reactive_power_total: The reactive power total field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    active_power_total: Union[TimeSeries, str, None] = None
    apparent_power_total: Union[TimeSeries, str, None] = None
    reactive_power_total: Union[TimeSeries, str, None] = None

    def as_apply(self) -> PowerInverterApply:
        """Convert this read version of power inverter to the writing version."""
        return PowerInverterApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
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
        data_record: The data record of the power inverter node.
        active_power_total: The active power total field.
        apparent_power_total: The apparent power total field.
        reactive_power_total: The reactive power total field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    active_power_total: Union[TimeSeries, str, None] = None
    apparent_power_total: Union[TimeSeries, str, None] = None
    reactive_power_total: Union[TimeSeries, str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(PowerInverter, dm.ViewId("power-models", "PowerInverter", "1"))

        properties: dict[str, Any] = {}

        if self.active_power_total is not None or write_none:
            if isinstance(self.active_power_total, str) or self.active_power_total is None:
                properties["active_power_total"] = self.active_power_total
            else:
                properties["active_power_total"] = self.active_power_total.external_id

        if self.apparent_power_total is not None or write_none:
            if isinstance(self.apparent_power_total, str) or self.apparent_power_total is None:
                properties["apparent_power_total"] = self.apparent_power_total
            else:
                properties["apparent_power_total"] = self.apparent_power_total.external_id

        if self.reactive_power_total is not None or write_none:
            if isinstance(self.reactive_power_total, str) or self.reactive_power_total is None:
                properties["reactive_power_total"] = self.reactive_power_total
            else:
                properties["reactive_power_total"] = self.reactive_power_total.external_id

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.active_power_total, CogniteTimeSeries):
            resources.time_series.append(self.active_power_total)

        if isinstance(self.apparent_power_total, CogniteTimeSeries):
            resources.time_series.append(self.apparent_power_total)

        if isinstance(self.reactive_power_total, CogniteTimeSeries):
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
