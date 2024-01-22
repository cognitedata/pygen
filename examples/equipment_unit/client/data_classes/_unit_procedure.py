from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

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
)

if TYPE_CHECKING:
    from ._start_end_time import StartEndTime, StartEndTimeApply


__all__ = [
    "UnitProcedure",
    "UnitProcedureApply",
    "UnitProcedureList",
    "UnitProcedureApplyList",
    "UnitProcedureFields",
    "UnitProcedureTextFields",
]


UnitProcedureTextFields = Literal["name", "type_"]
UnitProcedureFields = Literal["name", "type_"]

_UNITPROCEDURE_PROPERTIES_BY_FIELD = {
    "name": "name",
    "type_": "type",
}


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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: Optional[str] = None
    type_: Optional[str] = Field(None, alias="type")
    work_orders: Optional[list[StartEndTime]] = Field(default=None, repr=False)
    work_units: Optional[list[StartEndTime]] = Field(default=None, repr=False)

    def as_apply(self) -> UnitProcedureApply:
        """Convert this read version of unit procedure to the writing version."""
        return UnitProcedureApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            type_=self.type_,
            work_orders=[work_order.as_apply() for work_order in self.work_orders or []],
            work_units=[work_unit.as_apply() for work_unit in self.work_units or []],
        )


class UnitProcedureApply(DomainModelApply):
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

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: Optional[str] = None
    type_: Optional[str] = Field(None, alias="type")
    work_orders: Optional[list[StartEndTimeApply]] = Field(default=None, repr=False)
    work_units: Optional[list[StartEndTimeApply]] = Field(default=None, repr=False)

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            UnitProcedure, dm.ViewId("IntegrationTestsImmutable", "UnitProcedure", "a6e2fea1e1c664")
        )

        properties: dict[str, Any] = {}

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.type_ is not None or write_none:
            properties["type"] = self.type_

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

        for work_order in self.work_orders or []:
            if isinstance(work_order, DomainRelationApply):
                other_resources = work_order._to_instances_apply(
                    cache,
                    self,
                    dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.work_order"),
                    view_by_read_class,
                )
                resources.extend(other_resources)

        for work_unit in self.work_units or []:
            if isinstance(work_unit, DomainRelationApply):
                other_resources = work_unit._to_instances_apply(
                    cache,
                    self,
                    dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.equipment_module"),
                    view_by_read_class,
                )
                resources.extend(other_resources)

        return resources


class UnitProcedureList(DomainModelList[UnitProcedure]):
    """List of unit procedures in the read version."""

    _INSTANCE = UnitProcedure

    def as_apply(self) -> UnitProcedureApplyList:
        """Convert these read versions of unit procedure to the writing versions."""
        return UnitProcedureApplyList([node.as_apply() for node in self.data])


class UnitProcedureApplyList(DomainModelApplyList[UnitProcedureApply]):
    """List of unit procedures in the writing version."""

    _INSTANCE = UnitProcedureApply


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
    filters = []
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if type_ is not None and isinstance(type_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type_))
    if type_ and isinstance(type_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type_))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
