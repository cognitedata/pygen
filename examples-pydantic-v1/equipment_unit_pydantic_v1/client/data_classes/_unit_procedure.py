from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DomainModel,
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
        name: The name field.
        type_: The type field.
        work_units: The work unit field.
        created_time: The created time of the unit procedure node.
        last_updated_time: The last updated time of the unit procedure node.
        deleted_time: If present, the deleted time of the unit procedure node.
        version: The version of the unit procedure node.
    """

    space: str = "IntegrationTestsImmutable"
    name: Optional[str] = None
    type_: Optional[str] = Field(None, alias="type")
    work_units: Optional[list[StartEndTime]] = Field(default=None, repr=False)

    def as_apply(self) -> UnitProcedureApply:
        """Convert this read version of unit procedure to the writing version."""
        return UnitProcedureApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            type_=self.type_,
            work_units=[work_unit.as_apply() for work_unit in self.work_units or []],
        )


class UnitProcedureApply(DomainModelApply):
    """This represents the writing version of unit procedure.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the unit procedure.
        name: The name field.
        type_: The type field.
        work_units: The work unit field.
        existing_version: Fail the ingestion request if the unit procedure version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    name: Optional[str] = None
    type_: Optional[str] = Field(None, alias="type")
    work_units: Optional[list[StartEndTimeApply]] = Field(default=None, repr=False)

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        from ._start_end_time import StartEndTimeApply

        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "UnitProcedure", "f16810a7105c44"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.type_ is not None:
            properties["type"] = self.type_

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

        for work_unit in self.work_units or []:
            if isinstance(work_unit, DomainRelationApply):
                other_resources = work_unit._to_instances_apply(cache, self, view_by_write_class)
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
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if type_ and isinstance(type_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type_))
    if type_ and isinstance(type_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type_))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None