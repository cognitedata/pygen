from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
)
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    FileMetadata,
    FileMetadataWrite,
    FileMetadataGraphQL,
    TimeSeries,
    TimeSeriesWrite,
    TimeSeriesGraphQL,
    T_DomainModelList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
)


__all__ = [
    "PowerInverter",
    "PowerInverterWrite",
    "PowerInverterApply",
    "PowerInverterList",
    "PowerInverterWriteList",
    "PowerInverterApplyList",
    "PowerInverterFields",
    "PowerInverterTextFields",
    "PowerInverterGraphQL",
]


PowerInverterTextFields = Literal["external_id", "active_power_total", "apparent_power_total", "reactive_power_total"]
PowerInverterFields = Literal["external_id", "active_power_total", "apparent_power_total", "reactive_power_total"]

_POWERINVERTER_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "active_power_total": "active_power_total",
    "apparent_power_total": "apparent_power_total",
    "reactive_power_total": "reactive_power_total",
}


class PowerInverterGraphQL(GraphQLCore):
    """This represents the reading version of power inverter, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the power inverter.
        data_record: The data record of the power inverter node.
        active_power_total: The active power total field.
        apparent_power_total: The apparent power total field.
        reactive_power_total: The reactive power total field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "PowerInverter", "1")
    active_power_total: Optional[TimeSeriesGraphQL] = None
    apparent_power_total: Optional[TimeSeriesGraphQL] = None
    reactive_power_total: Optional[TimeSeriesGraphQL] = None

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> PowerInverter:
        """Convert this GraphQL format of power inverter to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PowerInverter(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            active_power_total=self.active_power_total.as_read() if self.active_power_total else None,
            apparent_power_total=self.apparent_power_total.as_read() if self.apparent_power_total else None,
            reactive_power_total=self.reactive_power_total.as_read() if self.reactive_power_total else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PowerInverterWrite:
        """Convert this GraphQL format of power inverter to the writing format."""
        return PowerInverterWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            active_power_total=self.active_power_total.as_write() if self.active_power_total else None,
            apparent_power_total=self.apparent_power_total.as_write() if self.apparent_power_total else None,
            reactive_power_total=self.reactive_power_total.as_write() if self.reactive_power_total else None,
        )


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "PowerInverter", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    active_power_total: Union[TimeSeries, str, None] = None
    apparent_power_total: Union[TimeSeries, str, None] = None
    reactive_power_total: Union[TimeSeries, str, None] = None

    def as_write(self) -> PowerInverterWrite:
        """Convert this read version of power inverter to the writing version."""
        return PowerInverterWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            active_power_total=(
                self.active_power_total.as_write()
                if isinstance(self.active_power_total, CogniteTimeSeries)
                else self.active_power_total
            ),
            apparent_power_total=(
                self.apparent_power_total.as_write()
                if isinstance(self.apparent_power_total, CogniteTimeSeries)
                else self.apparent_power_total
            ),
            reactive_power_total=(
                self.reactive_power_total.as_write()
                if isinstance(self.reactive_power_total, CogniteTimeSeries)
                else self.reactive_power_total
            ),
        )

    def as_apply(self) -> PowerInverterWrite:
        """Convert this read version of power inverter to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PowerInverterWrite(DomainModelWrite):
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "PowerInverter", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    active_power_total: Union[TimeSeriesWrite, str, None] = None
    apparent_power_total: Union[TimeSeriesWrite, str, None] = None
    reactive_power_total: Union[TimeSeriesWrite, str, None] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.active_power_total is not None or write_none:
            properties["active_power_total"] = (
                self.active_power_total
                if isinstance(self.active_power_total, str) or self.active_power_total is None
                else self.active_power_total.external_id
            )

        if self.apparent_power_total is not None or write_none:
            properties["apparent_power_total"] = (
                self.apparent_power_total
                if isinstance(self.apparent_power_total, str) or self.apparent_power_total is None
                else self.apparent_power_total.external_id
            )

        if self.reactive_power_total is not None or write_none:
            properties["reactive_power_total"] = (
                self.reactive_power_total
                if isinstance(self.reactive_power_total, str) or self.reactive_power_total is None
                else self.reactive_power_total.external_id
            )

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=as_direct_relation_reference(self.node_type),
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.active_power_total, CogniteTimeSeriesWrite):
            resources.time_series.append(self.active_power_total)

        if isinstance(self.apparent_power_total, CogniteTimeSeriesWrite):
            resources.time_series.append(self.apparent_power_total)

        if isinstance(self.reactive_power_total, CogniteTimeSeriesWrite):
            resources.time_series.append(self.reactive_power_total)

        return resources


class PowerInverterApply(PowerInverterWrite):
    def __new__(cls, *args, **kwargs) -> PowerInverterApply:
        warnings.warn(
            "PowerInverterApply is deprecated and will be removed in v1.0. Use PowerInverterWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PowerInverter.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PowerInverterList(DomainModelList[PowerInverter]):
    """List of power inverters in the read version."""

    _INSTANCE = PowerInverter

    def as_write(self) -> PowerInverterWriteList:
        """Convert these read versions of power inverter to the writing versions."""
        return PowerInverterWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PowerInverterWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PowerInverterWriteList(DomainModelWriteList[PowerInverterWrite]):
    """List of power inverters in the writing version."""

    _INSTANCE = PowerInverterWrite


class PowerInverterApplyList(PowerInverterWriteList): ...


def _create_power_inverter_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _PowerInverterQuery(NodeQueryCore[T_DomainModelList, PowerInverterList]):
    _view_id = PowerInverter._view_id
    _result_cls = PowerInverter
    _result_list_cls_end = PowerInverterList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
    ):

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
        )

    def list_power_inverter(self, limit: int = DEFAULT_QUERY_LIMIT) -> PowerInverterList:
        return self._list(limit=limit)


class PowerInverterQuery(_PowerInverterQuery[PowerInverterList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PowerInverterList)
