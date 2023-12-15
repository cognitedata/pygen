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


__all__ = [
    "LineageAssertions",
    "LineageAssertionsApply",
    "LineageAssertionsList",
    "LineageAssertionsApplyList",
    "LineageAssertionsFields",
    "LineageAssertionsTextFields",
]


LineageAssertionsTextFields = Literal["id_", "lineage_relationship_type"]
LineageAssertionsFields = Literal["id_", "lineage_relationship_type"]

_LINEAGEASSERTIONS_PROPERTIES_BY_FIELD = {
    "id_": "ID",
    "lineage_relationship_type": "LineageRelationshipType",
}


class LineageAssertions(DomainModel):
    """This represents the reading version of lineage assertion.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the lineage assertion.
        id_: The id field.
        lineage_relationship_type: The lineage relationship type field.
        created_time: The created time of the lineage assertion node.
        last_updated_time: The last updated time of the lineage assertion node.
        deleted_time: If present, the deleted time of the lineage assertion node.
        version: The version of the lineage assertion node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    id_: Optional[str] = Field(None, alias="ID")
    lineage_relationship_type: Optional[str] = Field(None, alias="LineageRelationshipType")

    def as_apply(self) -> LineageAssertionsApply:
        """Convert this read version of lineage assertion to the writing version."""
        return LineageAssertionsApply(
            space=self.space,
            external_id=self.external_id,
            id_=self.id_,
            lineage_relationship_type=self.lineage_relationship_type,
        )


class LineageAssertionsApply(DomainModelApply):
    """This represents the writing version of lineage assertion.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the lineage assertion.
        id_: The id field.
        lineage_relationship_type: The lineage relationship type field.
        existing_version: Fail the ingestion request if the lineage assertion version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    id_: Optional[str] = Field(None, alias="ID")
    lineage_relationship_type: Optional[str] = Field(None, alias="LineageRelationshipType")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "LineageAssertions", "ef344f6030d778"
        )

        properties = {}
        if self.id_ is not None:
            properties["ID"] = self.id_
        if self.lineage_relationship_type is not None:
            properties["LineageRelationshipType"] = self.lineage_relationship_type

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

        return resources


class LineageAssertionsList(DomainModelList[LineageAssertions]):
    """List of lineage assertions in the read version."""

    _INSTANCE = LineageAssertions

    def as_apply(self) -> LineageAssertionsApplyList:
        """Convert these read versions of lineage assertion to the writing versions."""
        return LineageAssertionsApplyList([node.as_apply() for node in self.data])


class LineageAssertionsApplyList(DomainModelApplyList[LineageAssertionsApply]):
    """List of lineage assertions in the writing version."""

    _INSTANCE = LineageAssertionsApply


def _create_lineage_assertion_filter(
    view_id: dm.ViewId,
    id_: str | list[str] | None = None,
    id_prefix: str | None = None,
    lineage_relationship_type: str | list[str] | None = None,
    lineage_relationship_type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if id_ is not None and isinstance(id_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ID"), value=id_))
    if id_ and isinstance(id_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ID"), values=id_))
    if id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ID"), value=id_prefix))
    if lineage_relationship_type is not None and isinstance(lineage_relationship_type, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("LineageRelationshipType"), value=lineage_relationship_type)
        )
    if lineage_relationship_type and isinstance(lineage_relationship_type, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("LineageRelationshipType"), values=lineage_relationship_type)
        )
    if lineage_relationship_type_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("LineageRelationshipType"), value=lineage_relationship_type_prefix
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
