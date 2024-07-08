from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
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
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._start_end_time import StartEndTime, StartEndTimeGraphQL, StartEndTimeWrite


__all__ = [
    "UnitProcedure",
    "UnitProcedureWrite",
    "UnitProcedureApply",
    "UnitProcedureList",
    "UnitProcedureWriteList",
    "UnitProcedureApplyList",
    "UnitProcedureFields",
    "UnitProcedureTextFields",
    "UnitProcedureGraphQL",
]


UnitProcedureTextFields = Literal["name", "type_"]
UnitProcedureFields = Literal["name", "type_"]

_UNITPROCEDURE_PROPERTIES_BY_FIELD = {
    "name": "name",
    "type_": "type",
}


class UnitProcedureGraphQL(GraphQLCore):
    """This represents the reading version of unit procedure, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the unit procedure.
        data_record: The data record of the unit procedure node.
        name: The name field.
        type_: The type field.
        work_orders: The work order field.
        work_units: The work unit field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("IntegrationTestsImmutable", "UnitProcedure", "a6e2fea1e1c664")
    name: Optional[str] = None
    type_: Optional[str] = Field(None, alias="type")
    work_orders: Optional[list[StartEndTimeGraphQL]] = Field(default=None, repr=False)
    work_units: Optional[list[StartEndTimeGraphQL]] = Field(default=None, repr=False)

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

    @validator("work_orders", "work_units", pre=True)
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> UnitProcedure:
        """Convert this GraphQL format of unit procedure to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return UnitProcedure(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            type_=self.type_,
            work_orders=[work_order.as_read() for work_order in self.work_orders or []],
            work_units=[work_unit.as_read() for work_unit in self.work_units or []],
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> UnitProcedureWrite:
        """Convert this GraphQL format of unit procedure to the writing format."""
        return UnitProcedureWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            type_=self.type_,
            work_orders=[work_order.as_write() for work_order in self.work_orders or []],
            work_units=[work_unit.as_write() for work_unit in self.work_units or []],
        )


class UnitProcedure(DomainModel):
    """This represents the reading version of unit procedure.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the unit procedure.
        data_record: The data record of the unit procedure node.
        name: The name field.
        type_: The type field.
        work_orders: The work order field.
        work_units: The work unit field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("IntegrationTestsImmutable", "UnitProcedure", "a6e2fea1e1c664")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: Optional[str] = None
    type_: Optional[str] = Field(None, alias="type")
    work_orders: Optional[list[StartEndTime]] = Field(default=None, repr=False)
    work_units: Optional[list[StartEndTime]] = Field(default=None, repr=False)

    def as_write(self) -> UnitProcedureWrite:
        """Convert this read version of unit procedure to the writing version."""
        return UnitProcedureWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            type_=self.type_,
            work_orders=[work_order.as_write() for work_order in self.work_orders or []],
            work_units=[work_unit.as_write() for work_unit in self.work_units or []],
        )

    def as_apply(self) -> UnitProcedureWrite:
        """Convert this read version of unit procedure to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class UnitProcedureWrite(DomainModelWrite):
    """This represents the writing version of unit procedure.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the unit procedure.
        data_record: The data record of the unit procedure node.
        name: The name field.
        type_: The type field.
        work_orders: The work order field.
        work_units: The work unit field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("IntegrationTestsImmutable", "UnitProcedure", "a6e2fea1e1c664")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: Optional[str] = None
    type_: Optional[str] = Field(None, alias="type")
    work_orders: Optional[list[StartEndTimeWrite]] = Field(default=None, repr=False)
    work_units: Optional[list[StartEndTimeWrite]] = Field(default=None, repr=False)

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

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.type_ is not None or write_none:
            properties["type"] = self.type_

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

        for work_order in self.work_orders or []:
            if isinstance(work_order, DomainRelationWrite):
                other_resources = work_order._to_instances_write(
                    cache,
                    self,
                    dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.work_order"),
                )
                resources.extend(other_resources)

        for work_unit in self.work_units or []:
            if isinstance(work_unit, DomainRelationWrite):
                other_resources = work_unit._to_instances_write(
                    cache,
                    self,
                    dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.equipment_module"),
                )
                resources.extend(other_resources)

        return resources


class UnitProcedureApply(UnitProcedureWrite):
    def __new__(cls, *args, **kwargs) -> UnitProcedureApply:
        warnings.warn(
            "UnitProcedureApply is deprecated and will be removed in v1.0. Use UnitProcedureWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "UnitProcedure.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class UnitProcedureList(DomainModelList[UnitProcedure]):
    """List of unit procedures in the read version."""

    _INSTANCE = UnitProcedure

    def as_write(self) -> UnitProcedureWriteList:
        """Convert these read versions of unit procedure to the writing versions."""
        return UnitProcedureWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> UnitProcedureWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class UnitProcedureWriteList(DomainModelWriteList[UnitProcedureWrite]):
    """List of unit procedures in the writing version."""

    _INSTANCE = UnitProcedureWrite


class UnitProcedureApplyList(UnitProcedureWriteList): ...


def _create_unit_procedure_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    type_: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(type_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type_))
    if type_ and isinstance(type_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type_))
    if type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
