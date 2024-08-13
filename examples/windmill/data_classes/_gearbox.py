from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import field_validator, model_validator

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
    TimeSeries,
    T_DomainModelList,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    select_best_node,
    QueryCore,
    NodeQueryCore,
)


__all__ = [
    "Gearbox",
    "GearboxWrite",
    "GearboxApply",
    "GearboxList",
    "GearboxWriteList",
    "GearboxApplyList",
    "GearboxFields",
    "GearboxTextFields",
    "GearboxGraphQL",
]


GearboxTextFields = Literal["displacement_x", "displacement_y", "displacement_z"]
GearboxFields = Literal["displacement_x", "displacement_y", "displacement_z"]

_GEARBOX_PROPERTIES_BY_FIELD = {
    "displacement_x": "displacement_x",
    "displacement_y": "displacement_y",
    "displacement_z": "displacement_z",
}


class GearboxGraphQL(GraphQLCore):
    """This represents the reading version of gearbox, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the gearbox.
        data_record: The data record of the gearbox node.
        displacement_x: The displacement x field.
        displacement_y: The displacement y field.
        displacement_z: The displacement z field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Gearbox", "1")
    displacement_x: Union[TimeSeries, dict, None] = None
    displacement_y: Union[TimeSeries, dict, None] = None
    displacement_z: Union[TimeSeries, dict, None] = None

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
    def as_read(self) -> Gearbox:
        """Convert this GraphQL format of gearbox to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Gearbox(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            displacement_x=self.displacement_x,
            displacement_y=self.displacement_y,
            displacement_z=self.displacement_z,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> GearboxWrite:
        """Convert this GraphQL format of gearbox to the writing format."""
        return GearboxWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            displacement_x=self.displacement_x,
            displacement_y=self.displacement_y,
            displacement_z=self.displacement_z,
        )


class Gearbox(DomainModel):
    """This represents the reading version of gearbox.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the gearbox.
        data_record: The data record of the gearbox node.
        displacement_x: The displacement x field.
        displacement_y: The displacement y field.
        displacement_z: The displacement z field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Gearbox", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    displacement_x: Union[TimeSeries, str, None] = None
    displacement_y: Union[TimeSeries, str, None] = None
    displacement_z: Union[TimeSeries, str, None] = None

    def as_write(self) -> GearboxWrite:
        """Convert this read version of gearbox to the writing version."""
        return GearboxWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            displacement_x=self.displacement_x,
            displacement_y=self.displacement_y,
            displacement_z=self.displacement_z,
        )

    def as_apply(self) -> GearboxWrite:
        """Convert this read version of gearbox to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class GearboxWrite(DomainModelWrite):
    """This represents the writing version of gearbox.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the gearbox.
        data_record: The data record of the gearbox node.
        displacement_x: The displacement x field.
        displacement_y: The displacement y field.
        displacement_z: The displacement z field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Gearbox", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    displacement_x: Union[TimeSeries, str, None] = None
    displacement_y: Union[TimeSeries, str, None] = None
    displacement_z: Union[TimeSeries, str, None] = None

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

        if self.displacement_x is not None or write_none:
            properties["displacement_x"] = (
                self.displacement_x
                if isinstance(self.displacement_x, str) or self.displacement_x is None
                else self.displacement_x.external_id
            )

        if self.displacement_y is not None or write_none:
            properties["displacement_y"] = (
                self.displacement_y
                if isinstance(self.displacement_y, str) or self.displacement_y is None
                else self.displacement_y.external_id
            )

        if self.displacement_z is not None or write_none:
            properties["displacement_z"] = (
                self.displacement_z
                if isinstance(self.displacement_z, str) or self.displacement_z is None
                else self.displacement_z.external_id
            )

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.displacement_x, CogniteTimeSeries):
            resources.time_series.append(self.displacement_x)

        if isinstance(self.displacement_y, CogniteTimeSeries):
            resources.time_series.append(self.displacement_y)

        if isinstance(self.displacement_z, CogniteTimeSeries):
            resources.time_series.append(self.displacement_z)

        return resources


class GearboxApply(GearboxWrite):
    def __new__(cls, *args, **kwargs) -> GearboxApply:
        warnings.warn(
            "GearboxApply is deprecated and will be removed in v1.0. Use GearboxWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Gearbox.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class GearboxList(DomainModelList[Gearbox]):
    """List of gearboxes in the read version."""

    _INSTANCE = Gearbox

    def as_write(self) -> GearboxWriteList:
        """Convert these read versions of gearbox to the writing versions."""
        return GearboxWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> GearboxWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class GearboxWriteList(DomainModelWriteList[GearboxWrite]):
    """List of gearboxes in the writing version."""

    _INSTANCE = GearboxWrite


class GearboxApplyList(GearboxWriteList): ...


def _create_gearbox_filter(
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


class _GearboxQuery(NodeQueryCore[T_DomainModelList, GearboxList]):
    _view_id = Gearbox._view_id
    _result_cls = Gearbox
    _result_list_cls_end = GearboxList

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


class GearboxQuery(_GearboxQuery[GearboxList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, GearboxList)
