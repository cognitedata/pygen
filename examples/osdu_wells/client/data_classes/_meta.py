from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Meta", "MetaApply", "MetaList", "MetaApplyList", "MetaFields", "MetaTextFields"]


MetaTextFields = Literal["kind", "name", "persistable_reference", "property_names", "unit_of_measure_id"]
MetaFields = Literal["kind", "name", "persistable_reference", "property_names", "unit_of_measure_id"]

_META_PROPERTIES_BY_FIELD = {
    "kind": "kind",
    "name": "name",
    "persistable_reference": "persistableReference",
    "property_names": "propertyNames",
    "unit_of_measure_id": "unitOfMeasureID",
}


class Meta(DomainModel):
    """This represent a read version of meta.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the meta.
        kind: The kind field.
        name: The name field.
        persistable_reference: The persistable reference field.
        property_names: The property name field.
        unit_of_measure_id: The unit of measure id field.
        created_time: The created time of the meta node.
        last_updated_time: The last updated time of the meta node.
        deleted_time: If present, the deleted time of the meta node.
        version: The version of the meta node.
    """

    space: str = "IntegrationTestsImmutable"
    kind: Optional[str] = None
    name: Optional[str] = None
    persistable_reference: Optional[str] = Field(None, alias="persistableReference")
    property_names: Optional[list[str]] = Field(None, alias="propertyNames")
    unit_of_measure_id: Optional[str] = Field(None, alias="unitOfMeasureID")

    def as_apply(self) -> MetaApply:
        """Convert this read version of meta to a write version."""
        return MetaApply(
            space=self.space,
            external_id=self.external_id,
            kind=self.kind,
            name=self.name,
            persistable_reference=self.persistable_reference,
            property_names=self.property_names,
            unit_of_measure_id=self.unit_of_measure_id,
        )


class MetaApply(DomainModelApply):
    """This represent a write version of meta.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the meta.
        kind: The kind field.
        name: The name field.
        persistable_reference: The persistable reference field.
        property_names: The property name field.
        unit_of_measure_id: The unit of measure id field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    kind: Optional[str] = None
    name: Optional[str] = None
    persistable_reference: Optional[str] = Field(None, alias="persistableReference")
    property_names: Optional[list[str]] = Field(None, alias="propertyNames")
    unit_of_measure_id: Optional[str] = Field(None, alias="unitOfMeasureID")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.kind is not None:
            properties["kind"] = self.kind
        if self.name is not None:
            properties["name"] = self.name
        if self.persistable_reference is not None:
            properties["persistableReference"] = self.persistable_reference
        if self.property_names is not None:
            properties["propertyNames"] = self.property_names
        if self.unit_of_measure_id is not None:
            properties["unitOfMeasureID"] = self.unit_of_measure_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Meta", "bf181692a967b6"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class MetaList(TypeList[Meta]):
    """List of metas in read version."""

    _NODE = Meta

    def as_apply(self) -> MetaApplyList:
        """Convert this read version of meta to a write version."""
        return MetaApplyList([node.as_apply() for node in self.data])


class MetaApplyList(TypeApplyList[MetaApply]):
    """List of metas in write version."""

    _NODE = MetaApply
