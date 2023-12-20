from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


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
    """This represents the reading version of meta.

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

    space: str = DEFAULT_INSTANCE_SPACE
    kind: Optional[str] = None
    name: Optional[str] = None
    persistable_reference: Optional[str] = Field(None, alias="persistableReference")
    property_names: Optional[list[str]] = Field(None, alias="propertyNames")
    unit_of_measure_id: Optional[str] = Field(None, alias="unitOfMeasureID")

    def as_apply(self) -> MetaApply:
        """Convert this read version of meta to the writing version."""
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
    """This represents the writing version of meta.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the meta.
        kind: The kind field.
        name: The name field.
        persistable_reference: The persistable reference field.
        property_names: The property name field.
        unit_of_measure_id: The unit of measure id field.
        existing_version: Fail the ingestion request if the meta version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    kind: Optional[str] = None
    name: Optional[str] = None
    persistable_reference: Optional[str] = Field(None, alias="persistableReference")
    property_names: Optional[list[str]] = Field(None, alias="propertyNames")
    unit_of_measure_id: Optional[str] = Field(None, alias="unitOfMeasureID")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Meta", "bf181692a967b6"
        )

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
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("IntegrationTestsImmutable", "Meta"),
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class MetaList(DomainModelList[Meta]):
    """List of metas in the read version."""

    _INSTANCE = Meta

    def as_apply(self) -> MetaApplyList:
        """Convert these read versions of meta to the writing versions."""
        return MetaApplyList([node.as_apply() for node in self.data])


class MetaApplyList(DomainModelApplyList[MetaApply]):
    """List of metas in the writing version."""

    _INSTANCE = MetaApply


def _create_meta_filter(
    view_id: dm.ViewId,
    kind: str | list[str] | None = None,
    kind_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    persistable_reference: str | list[str] | None = None,
    persistable_reference_prefix: str | None = None,
    unit_of_measure_id: str | list[str] | None = None,
    unit_of_measure_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if kind is not None and isinstance(kind, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("kind"), value=kind))
    if kind and isinstance(kind, list):
        filters.append(dm.filters.In(view_id.as_property_ref("kind"), values=kind))
    if kind_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("kind"), value=kind_prefix))
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if persistable_reference is not None and isinstance(persistable_reference, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("persistableReference"), value=persistable_reference))
    if persistable_reference and isinstance(persistable_reference, list):
        filters.append(dm.filters.In(view_id.as_property_ref("persistableReference"), values=persistable_reference))
    if persistable_reference_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("persistableReference"), value=persistable_reference_prefix)
        )
    if unit_of_measure_id is not None and isinstance(unit_of_measure_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("unitOfMeasureID"), value=unit_of_measure_id))
    if unit_of_measure_id and isinstance(unit_of_measure_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("unitOfMeasureID"), values=unit_of_measure_id))
    if unit_of_measure_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("unitOfMeasureID"), value=unit_of_measure_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
