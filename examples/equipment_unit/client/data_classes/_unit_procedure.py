from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, NodeList, TypeApplyList

if TYPE_CHECKING:
    from ._equipment_module import EquipmentModuleApply
    from ._start_end_time import StartEndTimeApply

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
    work_units: Optional[list[str]] = None

    def as_apply(self) -> UnitProcedureApply:
        """Convert this read version of unit procedure to the writing version."""
        return UnitProcedureApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            type_=self.type_,
            work_units=self.work_units,
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
        existing_version: Fail the ingestion request if the version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    name: Optional[str] = None
    type_: Optional[str] = Field(None, alias="type")
    work_units: Union[list[StartEndTimeApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.type_ is not None:
            properties["type"] = self.type_
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "UnitProcedure", "f16810a7105c44"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        for work_unit in self.work_units or []:
            edge = self._create_work_unit_edge(work_unit)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(work_unit, DomainModelApply):
                instances = work_unit._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_work_unit_edge(self, work_unit: Union[str, EquipmentModuleApply]) -> dm.EdgeApply:
        if isinstance(work_unit, str):
            end_space, end_node_ext_id = self.space, work_unit
        elif isinstance(work_unit, DomainModelApply):
            end_space, end_node_ext_id = work_unit.space, work_unit.external_id
        else:
            raise TypeError(f"Expected str or EquipmentModuleApply, got {type(work_unit)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "UnitProcedure.equipment_module"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )


class UnitProcedureList(NodeList[UnitProcedure]):
    """List of unit procedures in read version."""

    _INSTANCE = UnitProcedure

    def as_apply(self) -> UnitProcedureApplyList:
        """Convert this read version of unit procedure to a write version."""
        return UnitProcedureApplyList([node.as_apply() for node in self.data])


class UnitProcedureApplyList(TypeApplyList[UnitProcedureApply]):
    """List of unit procedures in write version."""

    _INSTANCE = UnitProcedureApply
