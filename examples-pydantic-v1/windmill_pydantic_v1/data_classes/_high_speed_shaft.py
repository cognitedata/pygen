from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
)
from pydantic import validator, root_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
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
    FileMetadataGraphQL,
    TimeSeriesGraphQL,
    T_DomainModelList,
    as_direct_relation_reference,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    select_best_node,
    QueryCore,
    NodeQueryCore,
)


__all__ = [
    "HighSpeedShaft",
    "HighSpeedShaftWrite",
    "HighSpeedShaftApply",
    "HighSpeedShaftList",
    "HighSpeedShaftWriteList",
    "HighSpeedShaftApplyList",
    "HighSpeedShaftFields",
    "HighSpeedShaftTextFields",
    "HighSpeedShaftGraphQL",
]


HighSpeedShaftTextFields = Literal["external_id", "bending_moment_y", "bending_monent_x", "torque"]
HighSpeedShaftFields = Literal["external_id", "bending_moment_y", "bending_monent_x", "torque"]

_HIGHSPEEDSHAFT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "bending_moment_y": "bending_moment_y",
    "bending_monent_x": "bending_monent_x",
    "torque": "torque",
}


class HighSpeedShaftGraphQL(GraphQLCore):
    """This represents the reading version of high speed shaft, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the high speed shaft.
        data_record: The data record of the high speed shaft node.
        bending_moment_y: The bending moment y field.
        bending_monent_x: The bending monent x field.
        torque: The torque field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "HighSpeedShaft", "1")
    bending_moment_y: Optional[TimeSeriesGraphQL] = None
    bending_monent_x: Optional[TimeSeriesGraphQL] = None
    torque: Optional[TimeSeriesGraphQL] = None

    @root_validator(pre=True)
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
    def as_read(self) -> HighSpeedShaft:
        """Convert this GraphQL format of high speed shaft to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return HighSpeedShaft(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            bending_moment_y=self.bending_moment_y.as_read() if self.bending_moment_y else None,
            bending_monent_x=self.bending_monent_x.as_read() if self.bending_monent_x else None,
            torque=self.torque.as_read() if self.torque else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> HighSpeedShaftWrite:
        """Convert this GraphQL format of high speed shaft to the writing format."""
        return HighSpeedShaftWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            bending_moment_y=self.bending_moment_y.as_write() if self.bending_moment_y else None,
            bending_monent_x=self.bending_monent_x.as_write() if self.bending_monent_x else None,
            torque=self.torque.as_write() if self.torque else None,
        )


class HighSpeedShaft(DomainModel):
    """This represents the reading version of high speed shaft.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the high speed shaft.
        data_record: The data record of the high speed shaft node.
        bending_moment_y: The bending moment y field.
        bending_monent_x: The bending monent x field.
        torque: The torque field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "HighSpeedShaft", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    bending_moment_y: Union[TimeSeries, str, None] = None
    bending_monent_x: Union[TimeSeries, str, None] = None
    torque: Union[TimeSeries, str, None] = None

    def as_write(self) -> HighSpeedShaftWrite:
        """Convert this read version of high speed shaft to the writing version."""
        return HighSpeedShaftWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            bending_moment_y=(
                self.bending_moment_y.as_write()
                if isinstance(self.bending_moment_y, CogniteTimeSeries)
                else self.bending_moment_y
            ),
            bending_monent_x=(
                self.bending_monent_x.as_write()
                if isinstance(self.bending_monent_x, CogniteTimeSeries)
                else self.bending_monent_x
            ),
            torque=self.torque.as_write() if isinstance(self.torque, CogniteTimeSeries) else self.torque,
        )

    def as_apply(self) -> HighSpeedShaftWrite:
        """Convert this read version of high speed shaft to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class HighSpeedShaftWrite(DomainModelWrite):
    """This represents the writing version of high speed shaft.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the high speed shaft.
        data_record: The data record of the high speed shaft node.
        bending_moment_y: The bending moment y field.
        bending_monent_x: The bending monent x field.
        torque: The torque field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "HighSpeedShaft", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    bending_moment_y: Union[TimeSeriesWrite, str, None] = None
    bending_monent_x: Union[TimeSeriesWrite, str, None] = None
    torque: Union[TimeSeriesWrite, str, None] = None

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

        if self.bending_moment_y is not None or write_none:
            properties["bending_moment_y"] = (
                self.bending_moment_y
                if isinstance(self.bending_moment_y, str) or self.bending_moment_y is None
                else self.bending_moment_y.external_id
            )

        if self.bending_monent_x is not None or write_none:
            properties["bending_monent_x"] = (
                self.bending_monent_x
                if isinstance(self.bending_monent_x, str) or self.bending_monent_x is None
                else self.bending_monent_x.external_id
            )

        if self.torque is not None or write_none:
            properties["torque"] = (
                self.torque if isinstance(self.torque, str) or self.torque is None else self.torque.external_id
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

        if isinstance(self.bending_moment_y, CogniteTimeSeriesWrite):
            resources.time_series.append(self.bending_moment_y)

        if isinstance(self.bending_monent_x, CogniteTimeSeriesWrite):
            resources.time_series.append(self.bending_monent_x)

        if isinstance(self.torque, CogniteTimeSeriesWrite):
            resources.time_series.append(self.torque)

        return resources


class HighSpeedShaftApply(HighSpeedShaftWrite):
    def __new__(cls, *args, **kwargs) -> HighSpeedShaftApply:
        warnings.warn(
            "HighSpeedShaftApply is deprecated and will be removed in v1.0. Use HighSpeedShaftWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "HighSpeedShaft.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class HighSpeedShaftList(DomainModelList[HighSpeedShaft]):
    """List of high speed shafts in the read version."""

    _INSTANCE = HighSpeedShaft

    def as_write(self) -> HighSpeedShaftWriteList:
        """Convert these read versions of high speed shaft to the writing versions."""
        return HighSpeedShaftWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> HighSpeedShaftWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class HighSpeedShaftWriteList(DomainModelWriteList[HighSpeedShaftWrite]):
    """List of high speed shafts in the writing version."""

    _INSTANCE = HighSpeedShaftWrite


class HighSpeedShaftApplyList(HighSpeedShaftWriteList): ...


def _create_high_speed_shaft_filter(
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


class _HighSpeedShaftQuery(NodeQueryCore[T_DomainModelList, HighSpeedShaftList]):
    _view_id = HighSpeedShaft._view_id
    _result_cls = HighSpeedShaft
    _result_list_cls_end = HighSpeedShaftList

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


class HighSpeedShaftQuery(_HighSpeedShaftQuery[HighSpeedShaftList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, HighSpeedShaftList)
